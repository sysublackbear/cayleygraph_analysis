// Copyright 2017 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/proto"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/cayley/internal/lru"
	"github.com/cayleygraph/cayley/query/shape"
	"github.com/cayleygraph/quad"
	"github.com/cayleygraph/quad/pquads"
	"github.com/hidal-go/hidalgo/kv"
	boom "github.com/tylertreat/BoomFilters"
)

var (
	ErrNoBucket  = errors.New("kv: no bucket")
	ErrEmptyPath = errors.New("kv: path to the database must be specified")
)

type Registration struct {
	NewFunc      NewFunc
	InitFunc     InitFunc
	IsPersistent bool
}

type InitFunc func(string, graph.Options) (kv.KV, error)
type NewFunc func(string, graph.Options) (kv.KV, error)

func Register(name string, r Registration) {
	graph.RegisterQuadStore(name, graph.QuadStoreRegistration{
		InitFunc: func(addr string, opt graph.Options) error {
			if !r.IsPersistent { // 不需要持久化
				return nil
			}
			// 调用内层的初始化函数
			kv, err := r.InitFunc(addr, opt) // 返回kv.KV
			if err != nil {
				return err
			}
			defer kv.Close()
			if err = Init(kv, opt); err != nil {
				return err
			}
			return kv.Close()
		},
		NewFunc: func(addr string, opt graph.Options) (graph.QuadStore, error) {
			// 调用里层的New函数
			kv, err := r.NewFunc(addr, opt)
			if err != nil {
				return nil, err
			}
			if !r.IsPersistent {
				if err = Init(kv, opt); err != nil {
					kv.Close()
					return nil, err
				}
			}
			// 给KV.kv外面包装一层QuadStore
			return New(kv, opt)
		},
		IsPersistent: r.IsPersistent,
	})
}

const (
	latestDataVersion   = 2
	envKVDefaultIndexes = "CAYLEY_KV_INDEXES"
)

var (
	_ refs.BatchNamer = (*QuadStore)(nil)
	_ shape.Optimizer = (*QuadStore)(nil)
)

type QuadStore struct {
	db kv.KV

	indexes struct {
		sync.RWMutex
		all []QuadIndex
		// indexes used to detect duplicate quads
		exists []QuadIndex
	}

	valueLRU *lru.Cache

	writer    sync.Mutex
	mapBucket map[string]map[string][]uint64
	mapBloom  map[string]*boom.BloomFilter
	mapNodes  *boom.BloomFilter

	exists struct {
		disabled bool
		sync.Mutex
		buf []byte
		*boom.DeletableBloomFilter
	}
}

func newQuadStore(kv kv.KV) *QuadStore {
	return &QuadStore{db: kv}
}

func Init(kv kv.KV, opt graph.Options) error {
	ctx := context.TODO()
	qs := newQuadStore(kv)
	if data := os.Getenv(envKVDefaultIndexes); data != "" {
		qs.indexes.all = nil
		// 从环境变量取出Indexes的数据结构
		if err := json.Unmarshal([]byte(data), &qs.indexes); err != nil {
			return err
		}
	}
	if qs.indexes.all == nil {
		qs.indexes.all = DefaultQuadIndexes
	}
	// 取出Metadata
	// 取出meta version
	if _, err := qs.getMetadata(ctx); err == nil {
		return graph.ErrDatabaseExists
	} else if err != ErrNoBucket {
		return err
	}
	// 添加该opt Key
	upfront, err := opt.BoolKey("upfront", false)
	if err != nil {
		return err
	}
	if err := qs.createBuckets(ctx, upfront); err != nil {
		return err
	}
	// 更新版本号
	if err := setVersion(ctx, qs.db, latestDataVersion); err != nil {
		return err
	}
	// 往meta bucket写入index数据
	if err := qs.writeIndexesMeta(ctx); err != nil {
		return err
	}
	return nil
}

const (
	OptNoBloom = "no_bloom"
)

// New : Important!!! : 将kv的DB结构体转成graph.QuadStore
func New(kv kv.KV, opt graph.Options) (graph.QuadStore, error) {
	ctx := context.TODO()
	qs := newQuadStore(kv) // 将kv嵌入到QuadStore
	// MVCC取出版本号
	if vers, err := qs.getMetadata(ctx); err == ErrNoBucket {
		return nil, graph.ErrNotInitialized
	} else if err != nil {
		return nil, err
	} else if vers != latestDataVersion {
		return nil, errors.New("kv: data version is out of date. Run cayleyupgrade for your config to update the data")
	}
	// 取出索引
	list, err := qs.readIndexesMeta(ctx)
	if err != nil {
		return nil, err
	}
	qs.indexes.all = list
	// 初始化lru
	qs.valueLRU = lru.New(2000)
	// 是否开启布隆过滤器
	qs.exists.disabled, _ = opt.BoolKey(OptNoBloom, false)
	if err := qs.initBloomFilter(ctx); err != nil {
		return nil, err
	}
	if !qs.exists.disabled {
		if sz, err := qs.getSize(); err != nil {
			return nil, err
		} else if sz == 0 {
			qs.mapBloom = make(map[string]*boom.BloomFilter)
			qs.mapNodes = boom.NewBloomFilter(100*1000*1000, 0.05)
		}
	}
	return qs, nil
}

func setVersion(ctx context.Context, db kv.KV, version int64) error {
	return kv.Update(ctx, db, func(tx kv.Tx) error {
		var buf [8]byte
		// 小端序
		binary.LittleEndian.PutUint64(buf[:], uint64(version))
		if err := tx.Put(metaBucket.AppendBytes([]byte("version")), buf[:]); err != nil {
			return fmt.Errorf("couldn't write version: %v", err)
		}
		return nil
	})
}

func (qs *QuadStore) getMetaInt(ctx context.Context, key string) (int64, error) {
	var v int64
	err := kv.View(qs.db, func(tx kv.Tx) error {
		val, err := tx.Get(ctx, metaBucket.AppendBytes([]byte(key)))
		if err == kv.ErrNotFound {
			return ErrNoBucket
		} else if err != nil {
			return err
		}
		v, err = asInt64(val, 0)
		if err != nil {
			return err
		}
		return nil
	})
	return v, err
}

func (qs *QuadStore) getSize() (int64, error) {
	// 从metaBucket中取出size的key
	sz, err := qs.getMetaInt(context.TODO(), "size")
	if err == ErrNoBucket {
		return 0, nil
	}
	return sz, err
}

func (qs *QuadStore) Size() int64 {
	sz, _ := qs.getSize()
	return sz
}

func (qs *QuadStore) Stats(ctx context.Context, exact bool) (graph.Stats, error) {
	sz, err := qs.getMetaInt(ctx, "size")
	if err != nil {
		return graph.Stats{}, err
	}
	st := graph.Stats{
		Nodes: refs.Size{
			Value: sz / 3, // todo: 为啥除以3
			Exact: false,  // TODO(dennwc): store nodes count
		},
		Quads: refs.Size{
			Value: sz,
			Exact: true,
		},
	}
	if exact {
		// calculate the exact number of nodes
		st.Nodes.Value = 0
		it := qs.NodesAllIterator().Iterate()
		defer it.Close()
		for it.Next(ctx) {
			st.Nodes.Value++
		}
		if err := it.Err(); err != nil {
			return st, err
		}
		st.Nodes.Exact = true
	}
	return st, nil
}

func (qs *QuadStore) Close() error {
	return qs.db.Close()
}

func (qs *QuadStore) getMetadata(ctx context.Context) (int64, error) {
	var vers int64
	// 取出版本号
	/*
		// View is a helper to open a read-only transaction to read the database.
		func View(kv KV, view func(tx Tx) error) error {
			tx, err := kv.Tx(false)  // 不开读写锁
			if err != nil {
				return err
			}
			defer tx.Close()
			err = view(tx)
			if err == nil {
				err = tx.Close()
			}
			return err
		}
	*/
	err := kv.View(qs.db, func(tx kv.Tx) error {
		// 查询metaversion
		val, err := tx.Get(ctx, metaBucket.AppendBytes([]byte("version")))
		if err == kv.ErrNotFound {
			return ErrNoBucket
		} else if err != nil {
			return err
		}
		vers, err = asInt64(val, 0)
		if err != nil {
			return err
		}
		return nil
	})
	return vers, err
}

func asInt64(b []byte, empty int64) (int64, error) {
	if len(b) == 0 {
		return empty, nil
	} else if len(b) != 8 {
		return 0, fmt.Errorf("unexpected int size: %d", len(b))
	}
	v := int64(binary.LittleEndian.Uint64(b))
	return v, nil
}

func (qs *QuadStore) horizon(ctx context.Context) int64 {
	// 从meta取出horizon
	// 相当于一些数据从meta里面获取
	h, _ := qs.getMetaInt(ctx, "horizon")
	return h
}

// []graph.Ref -> []quad.Value
func (qs *QuadStore) ValuesOf(ctx context.Context, vals []graph.Ref) ([]quad.Value, error) {
	out := make([]quad.Value, len(vals))
	var (
		inds  []int
		irefs []uint64
	)
	for i, v := range vals {
		if v == nil {
			continue
		} else if pv, ok := v.(refs.PreFetchedValue); ok {
			out[i] = pv.NameOf()
			continue
		}
		switch v := v.(type) {
		case Int64Value: // 转成int
			if v == 0 {
				continue
			}
			inds = append(inds, i)
			irefs = append(irefs, uint64(v))
		default:
			return out, fmt.Errorf("unknown type of graph.Ref; not meant for this quadstore. apparently a %#v", v)
		}
	}
	if len(irefs) == 0 {
		return out, nil
	}
	prim, err := qs.getPrimitives(ctx, irefs) // 查询出这些值
	if err != nil {
		return out, err
	}
	var last error
	for i, p := range prim {
		if p == nil || !p.IsNode() {
			continue
		}
		// 取出Value,进行反序列化
		qv, err := pquads.UnmarshalValue(p.Value)
		if err != nil {
			last = err
			continue
		}
		out[inds[i]] = qv
	}
	return out, last
}

// todo: 待明确含义
func (qs *QuadStore) RefsOf(ctx context.Context, nodes []quad.Value) ([]graph.Ref, error) {
	values := make([]graph.Ref, len(nodes))
	err := kv.View(qs.db, func(tx kv.Tx) error {
		for i, node := range nodes {
			value, err := qs.resolveQuadValue(ctx, tx, node)
			if err != nil {
				return err
			}
			values[i] = Int64Value(value)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return values, nil
}

// graph.Ref -> quad.Value
func (qs *QuadStore) NameOf(v graph.Ref) (quad.Value, error) {
	ctx := context.TODO()
	vals, err := qs.ValuesOf(ctx, []graph.Ref{v})
	if err != nil {
		return nil, fmt.Errorf("error getting NameOf %d: %w", v, err)
	}
	// 取vals[0]
	return vals[0], nil
}

// graph.Ref转quad.Quad
func (qs *QuadStore) Quad(k graph.Ref) (quad.Quad, error) {
	key, ok := k.(*proto.Primitive)
	if !ok {
		return quad.Quad{}, fmt.Errorf("passed value was not a quad primitive: %T", k)
	}
	ctx := context.TODO()
	var v quad.Quad
	err := kv.View(qs.db, func(tx kv.Tx) error {
		var err error
		// 查询kv
		v, err = qs.primitiveToQuad(ctx, tx, key)
		return err
	})
	if err == kv.ErrNotFound {
		err = nil
	}
	if err != nil {
		err = fmt.Errorf("error fetching quad %#v: %w", key, err)
	}
	return v, err
}

func (qs *QuadStore) primitiveToQuad(ctx context.Context, tx kv.Tx, p *proto.Primitive) (quad.Quad, error) {
	q := &quad.Quad{}
	for _, dir := range quad.Directions {
		v := p.GetDirection(dir)
		// 取出多个方向的值
		val, err := qs.getValFromLog(ctx, tx, v)
		if err != nil {
			return *q, err
		}
		q.Set(dir, val)
	}
	return *q, nil
}

func (qs *QuadStore) getValFromLog(ctx context.Context, tx kv.Tx, k uint64) (quad.Value, error) {
	if k == 0 {
		return nil, nil
	}
	p, err := qs.getPrimitiveFromLog(ctx, tx, k)
	if err != nil {
		return nil, err
	}
	return pquads.UnmarshalValue(p.Value)
}

func (qs *QuadStore) ValueOf(s quad.Value) (graph.Ref, error) {
	ctx := context.TODO()
	var out Int64Value
	err := kv.View(qs.db, func(tx kv.Tx) error {
		v, err := qs.resolveQuadValue(ctx, tx, s)
		out = Int64Value(v)
		return err
	})
	if err != nil {
		return nil, err
	}
	if out == 0 {
		return nil, nil
	}
	return out, nil
}

func (qs *QuadStore) QuadDirection(val graph.Ref, d quad.Direction) (graph.Ref, error) {
	p, ok := val.(*proto.Primitive)
	if !ok {
		return nil, nil
	}
	switch d {
	case quad.Subject:
		return Int64Value(p.Subject), nil
	case quad.Predicate:
		return Int64Value(p.Predicate), nil
	case quad.Object:
		return Int64Value(p.Object), nil
	case quad.Label:
		if p.Label == 0 {
			return nil, nil
		}
		return Int64Value(p.Label), nil
	}
	return nil, nil
}

func (qs *QuadStore) getPrimitives(ctx context.Context, vals []uint64) ([]*proto.Primitive, error) {
	tx, err := qs.db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	tx = wrapTx(tx)
	return qs.getPrimitivesFromLog(ctx, tx, vals)
}

type Int64Value uint64

func (v Int64Value) Key() interface{} { return v }

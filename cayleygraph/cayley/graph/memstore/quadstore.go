// Copyright 2014 The Cayley Authors. All rights reserved.
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

package memstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/iterator"
	"github.com/cayleygraph/cayley/graph/refs"
	"github.com/cayleygraph/quad"
)

const QuadStoreType = "memstore"

func init() {
	graph.RegisterQuadStore(QuadStoreType, graph.QuadStoreRegistration{
		NewFunc: func(string, graph.Options) (graph.QuadStore, error) {
			return newQuadStore(), nil
		},
		UpgradeFunc:  nil,
		InitFunc:     nil,
		IsPersistent: false,
	})
}

type bnode int64

// 是否等于它自身
func (n bnode) Key() interface{} { return n }

type qprim struct {
	p *Primitive
}

func (n qprim) Key() interface{} { return n.p.ID }

var _ quad.Writer = (*QuadStore)(nil)

// 默认比较的优先级
func cmp(a, b int64) int {
	return int(a - b)
}

type QuadDirectionIndex struct {
	index [4]map[int64]*Tree
}

func NewQuadDirectionIndex() QuadDirectionIndex {
	return QuadDirectionIndex{[...]map[int64]*Tree{
		quad.Subject - 1:   make(map[int64]*Tree),
		quad.Predicate - 1: make(map[int64]*Tree),
		quad.Object - 1:    make(map[int64]*Tree),
		quad.Label - 1:     make(map[int64]*Tree),
	}}
}

// 查询特定id在特定方向的B+树
func (qdi QuadDirectionIndex) Tree(d quad.Direction, id int64) *Tree {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id] // 查询是否存在B+树
	if !ok {
		tree = TreeNew(cmp) //创建B+树，不存在，则创建
		qdi.index[d-1][id] = tree
	}
	return tree
}

func (qdi QuadDirectionIndex) Get(d quad.Direction, id int64) (*Tree, bool) {
	if d < quad.Subject || d > quad.Label {
		panic("illegal direction")
	}
	tree, ok := qdi.index[d-1][id] // 查询对应的B+树
	return tree, ok
}

type Primitive struct {
	ID    int64
	Quad  internalQuad
	Value quad.Value
	refs  int
}

type internalQuad struct {
	S, P, O, L int64
}

func (q internalQuad) Zero() bool {
	return q == (internalQuad{})
}

func (q *internalQuad) SetDir(d quad.Direction, n int64) {
	switch d {
	case quad.Subject: // 入度(点的属性)
		q.S = n
	case quad.Predicate: // 边(边的属性)
		q.P = n
	case quad.Object: // 出度(点的属性)
		q.O = n
	case quad.Label: // 边上的标签?
		q.L = n
	default:
		panic(fmt.Errorf("unknown dir: %v", d))
	}
}
func (q internalQuad) Dir(d quad.Direction) int64 {
	var n int64
	switch d {
	case quad.Subject:
		n = q.S
	case quad.Predicate:
		n = q.P
	case quad.Object:
		n = q.O
	case quad.Label:
		n = q.L
	}
	return n
}

type QuadStore struct {
	last int64 // 累加的id数?
	// TODO: string -> quad.Value once Raw -> typed resolution is unnecessary

	vals    map[string]int64
	quads   map[internalQuad]int64
	prim    map[int64]*Primitive
	all     []*Primitive // might not be sorted by id
	reading bool         // someone else might be reading "all" slice - next insert/delete should clone it
	index   QuadDirectionIndex
	horizon int64 // used only to assign ids to tx
	// vip_index map[string]map[int64]map[string]map[int64]*b.Tree
}

// New creates a new in-memory quad store and loads provided quads.
func New(quads ...quad.Quad) *QuadStore {
	qs := newQuadStore()
	for _, q := range quads {
		qs.AddQuad(q)
	}
	return qs
}

func newQuadStore() *QuadStore {
	return &QuadStore{
		vals:  make(map[string]int64),
		quads: make(map[internalQuad]int64),
		prim:  make(map[int64]*Primitive),
		index: NewQuadDirectionIndex(),
	}
}

func (qs *QuadStore) cloneAll() []*Primitive {
	qs.reading = true // 设置reading开关
	return qs.all
}

func (qs *QuadStore) addPrimitive(p *Primitive) int64 {
	qs.last++
	id := qs.last
	p.ID = id
	p.refs = 1
	qs.appendPrimitive(p)
	return id
}

func (qs *QuadStore) appendPrimitive(p *Primitive) {
	qs.prim[p.ID] = p
	if !qs.reading { // 没有人在读，直接修改
		qs.all = append(qs.all, p)
	} else {
		n := len(qs.all)
		qs.all = append(qs.all[:n:n], p) // reallocate slice，重新分配切片
		qs.reading = false               // this is a new slice
	}
}

const internalBNodePrefix = "memnode"

// add: 不存在则插入
func (qs *QuadStore) resolveVal(v quad.Value, add bool) (int64, bool) {
	if v == nil {
		return 0, false
	}
	//quad.Value -> quad.BNode,且以"memnode"开头
	if n, ok := v.(quad.BNode); ok && strings.HasPrefix(string(n), internalBNodePrefix) {
		n = n[len(internalBNodePrefix):] // 取出后半截
		id, err := strconv.ParseInt(string(n), 10, 64)
		if err == nil && id != 0 {
			if p, ok := qs.prim[id]; ok || !add {
				if add {
					p.refs++ // 引用计数+1
				}
				return id, ok
			}
			qs.appendPrimitive(&Primitive{ID: id, refs: 1})
			return id, true
		}
	}
	// 转换整型失败，当成字符串处理,但是需要在vals字典中记录映射
	vs := v.String()
	// 先从vals数组找出对应的id
	if id, exists := qs.vals[vs]; exists || !add {
		if exists && add {
			qs.prim[id].refs++
		}
		return id, exists
	}
	// id有last累加生成
	id := qs.addPrimitive(&Primitive{Value: v}) // id从qs.last取出来的
	qs.vals[vs] = id
	return id, true
}

func (qs *QuadStore) resolveQuad(q quad.Quad, add bool) (internalQuad, bool) {
	var p internalQuad
	for dir := quad.Subject; dir <= quad.Label; dir++ {
		v := q.Get(dir) //按照方向从quad.Quad中取出值
		if v == nil {
			continue
		}
		if vid, _ := qs.resolveVal(v, add); vid != 0 {
			p.SetDir(dir, vid)
		} else if !add {
			return internalQuad{}, false
		}
	}
	return p, true
}

// 查找qs.prim（通过id查找)
func (qs *QuadStore) lookupVal(id int64) quad.Value {
	pv := qs.prim[id]
	if pv == nil || pv.Value == nil {
		return quad.BNode(internalBNodePrefix + strconv.FormatInt(id, 10))
	}
	return pv.Value
}

func (qs *QuadStore) lookupQuadDirs(p internalQuad) quad.Quad {
	var q quad.Quad
	for dir := quad.Subject; dir <= quad.Label; dir++ {
		vid := p.Dir(dir)
		if vid == 0 {
			continue
		}
		// 四个id进行尝试查找
		v := qs.lookupVal(vid)
		q.Set(dir, v)
	}
	return q
}

// AddNode adds a blank node (with no value) to quad store. It returns an id of the node.
// 往图中添加一个空节点
func (qs *QuadStore) AddBNode() int64 {
	return qs.addPrimitive(&Primitive{})  // 插入空节点，qs.last++
}

// AddNode adds a value to quad store. It returns an id of the value.
// False is returned as a second parameter if value exists already.
// 往图中追加一个值（其实相当于插入一个点，但是如果这个点已经存在，则返回）
func (qs *QuadStore) AddValue(v quad.Value) (int64, bool) {
	id, exists := qs.resolveVal(v, true)
	return id, !exists
}

// 根据internalQuad的四个元素的值，在图中寻找这样的一棵B+树
func (qs *QuadStore) indexesForQuad(q internalQuad) []*Tree {
	trees := make([]*Tree, 0, 4)
	for dir := quad.Subject; dir <= quad.Label; dir++ {
		v := q.Dir(dir)
		if v == 0 {
			continue
		}
		trees = append(trees, qs.index.Tree(dir, v))
	}
	return trees // B+树的集合
}

// AddQuad adds a quad to quad store. It returns an id of the quad.
// False is returned as a second parameter if quad exists already.
// 把quad.Quad加入到QuadStore中，最多有可能往4棵B+树上添加节点
func (qs *QuadStore) AddQuad(q quad.Quad) (int64, bool) {
	// quad.Quad -> internalQuad
	p, _ := qs.resolveQuad(q, false)
	if id := qs.quads[p]; id != 0 {
		return id, false
	}
	p, _ = qs.resolveQuad(q, true)
	pr := &Primitive{Quad: p}
	id := qs.addPrimitive(pr)
	qs.quads[p] = id
	for _, t := range qs.indexesForQuad(p) {
		t.Set(id, pr) // B+树操作，把q这个点添加到B+树上
	}
	// TODO(barakmich): Add VIP indexing
	return id, true
}

// WriteQuad adds a quad to quad store.
//
// Deprecated: use AddQuad instead.
func (qs *QuadStore) WriteQuad(q quad.Quad) error {
	qs.AddQuad(q)
	return nil
}

// WriteQuads implements quad.Writer.
// 批量写入点
func (qs *QuadStore) WriteQuads(buf []quad.Quad) (int, error) {
	for _, q := range buf {
		qs.AddQuad(q)
	}
	return len(buf), nil
}

// wrapper装饰器模式
func (qs *QuadStore) NewQuadWriter() (quad.WriteCloser, error) {
	return &quadWriter{qs: qs}, nil
}

type quadWriter struct {
	qs *QuadStore
}

func (w *quadWriter) WriteQuad(q quad.Quad) error {
	w.qs.AddQuad(q)
	return nil
}

func (w *quadWriter) WriteQuads(buf []quad.Quad) (int, error) {
	for _, q := range buf {
		w.qs.AddQuad(q)
	}
	return len(buf), nil
}

func (w *quadWriter) Close() error {
	return nil
}

// 在图中删除节点
func (qs *QuadStore) deleteQuadNodes(q internalQuad) {
	for dir := quad.Subject; dir <= quad.Label; dir++ {
		id := q.Dir(dir)
		if id == 0 {
			continue
		}
		// 取出对应的Primitive
		if p := qs.prim[id]; p != nil {
			p.refs--
			if p.refs < 0 {
				panic("remove of deleted node")
			} else if p.refs == 0 {
				// 引用计数为0，删除该点
				qs.Delete(id)
			}
		}
	}
}
func (qs *QuadStore) Delete(id int64) bool {
	p := qs.prim[id]
	if p == nil {
		return false
	}
	// remove from value index
	if p.Value != nil {
		// 删除对应的vals字典
		delete(qs.vals, p.Value.String())
	}
	// remove from quad indexes
	// 在对应的B+树上面进行删除
	for _, t := range qs.indexesForQuad(p.Quad) {
		t.Delete(id)
	}
	// 在对应的字典上面进行删除
	delete(qs.quads, p.Quad)
	// remove primitive
	delete(qs.prim, id)
	di := -1
	// 在all列表中删除对应的节点
	for i, p2 := range qs.all {
		if p == p2 {
			di = i
			break
		}
	}
	if di >= 0 {
		// 没有人在读，直接append去挪动切片
		if !qs.reading {
			qs.all = append(qs.all[:di], qs.all[di+1:]...)
		} else {
			// 有的话，另外拷贝新的切片
			all := make([]*Primitive, 0, len(qs.all)-1)
			all = append(all, qs.all[:di]...)
			all = append(all, qs.all[di+1:]...)
			qs.all = all
			qs.reading = false // this is a new slice
		}
	}
	// 把它的对应关系进行删除（引用计数变为0）
	qs.deleteQuadNodes(p.Quad)
	return true
}

func (qs *QuadStore) findQuad(q quad.Quad) (int64, internalQuad, bool) {
	p, ok := qs.resolveQuad(q, false)
	if !ok {
		return 0, p, false
	}
	id := qs.quads[p]
	return id, p, id != 0
}

func (qs *QuadStore) ApplyDeltas(deltas []graph.Delta, ignoreOpts graph.IgnoreOpts) error {
	// Precheck the whole transaction (if required)
	// 不能接受重复或者不能接受丢失
	if !ignoreOpts.IgnoreDup || !ignoreOpts.IgnoreMissing {
		for _, d := range deltas {
			switch d.Action {
			case graph.Add: // 添加操作
				if !ignoreOpts.IgnoreDup {
					if _, _, ok := qs.findQuad(d.Quad); ok {
						return &graph.DeltaError{Delta: d, Err: graph.ErrQuadExists}
					}
				}
			case graph.Delete: // 删除操作
				if !ignoreOpts.IgnoreMissing {
					if _, _, ok := qs.findQuad(d.Quad); !ok {
						return &graph.DeltaError{Delta: d, Err: graph.ErrQuadNotExist}
					}
				}
			default:
				return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
			}
		}
	}

	// 事务操作（本质就是一个batch操作）-- start
	for _, d := range deltas {
		switch d.Action {
		case graph.Add:
			qs.AddQuad(d.Quad)
		case graph.Delete:
			if id, _, ok := qs.findQuad(d.Quad); ok {
				qs.Delete(id)
			}
		default:
			// TODO: ideally we should rollback it
			return &graph.DeltaError{Delta: d, Err: graph.ErrInvalidAction}
		}
	}
	// 事务操作（本质就是一个batch操作）-- end
	qs.horizon++ // used only to assign ids to tx
	return nil
}

func asID(v graph.Ref) (int64, bool) {
	switch v := v.(type) {
	case bnode:
		return int64(v), true
	case qprim:
		return v.p.ID, true
	default:
		return 0, false
	}
}

// graph.Ref转internalQuad
func (qs *QuadStore) quad(v graph.Ref) (q internalQuad, ok bool) {
	switch v := v.(type) {
	case bnode:
		p := qs.prim[int64(v)]
		if p == nil {
			return
		}
		q = p.Quad
	case qprim:
		q = v.p.Quad
	default:
		return internalQuad{}, false
	}
	return q, !q.Zero()
}

func (qs *QuadStore) Quad(index graph.Ref) (quad.Quad, error) {
	q, ok := qs.quad(index)
	if !ok {
		return quad.Quad{}, nil
	}
	return qs.lookupQuadDirs(q), nil
}

func (qs *QuadStore) QuadIterator(d quad.Direction, value graph.Ref) iterator.Shape {
	id, ok := asID(value)
	if !ok {
		return iterator.NewNull()
	}
	// 找对应的B+树--index
	index, ok := qs.index.Get(d, id)
	if ok && index.Len() != 0 {
		return qs.newIterator(index, d, id) //创建一个迭代器
	}
	return iterator.NewNull()
}

func (qs *QuadStore) QuadIteratorSize(ctx context.Context, d quad.Direction, v graph.Ref) (refs.Size, error) {
	id, ok := asID(v)
	if !ok {
		return refs.Size{Value: 0, Exact: true}, nil
	}
	index, ok := qs.index.Get(d, id)
	if !ok {
		return refs.Size{Value: 0, Exact: true}, nil
	}
	return refs.Size{Value: int64(index.Len()), Exact: true}, nil
}

func (qs *QuadStore) Stats(ctx context.Context, exact bool) (graph.Stats, error) {
	return graph.Stats{
		// 返回有多少个节点Node
		Nodes: refs.Size{
			Value: int64(len(qs.vals)),
			Exact: true,
		},
		// 多少个边关系
		Quads: refs.Size{
			Value: int64(len(qs.quads)),
			Exact: true,
		},
	}, nil
}

func (qs *QuadStore) ValueOf(name quad.Value) (graph.Ref, error) {
	if name == nil {
		return nil, nil
	}
	id := qs.vals[name.String()]
	if id == 0 {
		return nil, nil
	}
	return bnode(id), nil
}

func (qs *QuadStore) NameOf(v graph.Ref) (quad.Value, error) {
	if v == nil {
		return nil, nil
	} else if v, ok := v.(refs.PreFetchedValue); ok {
		return v.NameOf(), nil
	}
	n, ok := asID(v)
	if !ok {
		return nil, nil
	}
	if _, ok = qs.prim[n]; !ok {
		return nil, nil
	}
	return qs.lookupVal(n), nil
}

// all切片的迭代器
func (qs *QuadStore) QuadsAllIterator() iterator.Shape {
	return qs.newAllIterator(false, qs.last)
}

func (qs *QuadStore) QuadDirection(val graph.Ref, d quad.Direction) (graph.Ref, error) {
	q, ok := qs.quad(val)
	if !ok {
		return nil, nil
	}
	id := q.Dir(d)
	if id == 0 {
		return nil, nil
	}
	return bnode(id), nil
}

func (qs *QuadStore) NodesAllIterator() iterator.Shape {
	return qs.newAllIterator(true, qs.last)
}

func (qs *QuadStore) Close() error { return nil }

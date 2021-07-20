// Copyright 2014 The b Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package b implements a B+tree.
//
// Changelog
//
// 2014-06-26: Lower GC presure by recycling things.
//
// 2014-04-18: Added new method Put.
//
// Generic types
//
// Keys and their associated values are interface{} typed, similar to all of
// the containers in the standard library.
//
// Semiautomatic production of a type specific variant of this package is
// supported via
//
//	$ make generic
//
// This command will write to stdout a version of the btree.go file where
// every key type occurrence is replaced by the word 'key' (written in all
// CAPS) and every value type occurrence is replaced by the word 'value'
// (written in all CAPS). Then you have to replace these tokens with your
// desired type(s), using any technique you're comfortable with.
//
// This is how, for example, 'example/int.go' was created:
//
//	$ mkdir example
//	$
//	$ # Note: the command bellow must be actually written using the words
//	$ # 'key' and 'value' in all CAPS. The proper form is avoided in this
//	$ # documentation to not confuse any text replacement mechanism.
//	$
//	$ make generic | sed -e 's/key/int/g' -e 's/value/int/g' > example/int.go
//
// No other changes to int.go are necessary, it compiles just fine.
//
// Running the benchmarks for 1000 keys on a machine with Intel i5-4670 CPU @
// 3.4GHz, Go release 1.3.
//
//	$ go test -bench 1e3 example/all_test.go example/int.go
//	PASS
//	BenchmarkSetSeq1e3	   10000	    146740 ns/op
//	BenchmarkGetSeq1e3	   10000	    108261 ns/op
//	BenchmarkSetRnd1e3	   10000	    254359 ns/op
//	BenchmarkGetRnd1e3	   10000	    134621 ns/op
//	BenchmarkDelRnd1e3	   10000	    211864 ns/op
//	BenchmarkSeekSeq1e3	   10000	    148628 ns/op
//	BenchmarkSeekRnd1e3	   10000	    215166 ns/op
//	BenchmarkNext1e3	  200000	      9211 ns/op
//	BenchmarkPrev1e3	  200000	      8843 ns/op
//	ok  	command-line-arguments	25.071s
//	$
package memstore

import (
	"fmt"
	"io"
	"sync"
)

const (
	kx = 32 //TODO benchmark tune this number if using custom key/value type(s).
	kd = 32 //TODO benchmark tune this number if using custom key/value type(s).
)

func init() {
	if kd < 1 {
		panic(fmt.Errorf("kd %d: out of range", kd))
	}

	if kx < 2 {
		panic(fmt.Errorf("kx %d: out of range", kx))
	}
}

var (
	btDPool = sync.Pool{New: func() interface{} { return &d{} }}
	btEPool = btEpool{sync.Pool{New: func() interface{} { return &Enumerator{} }}}
	btTPool = btTpool{sync.Pool{New: func() interface{} { return &Tree{} }}}
	btXPool = sync.Pool{New: func() interface{} { return &x{} }}
)

type btTpool struct{ sync.Pool }

func (p *btTpool) get(cmp Cmp) *Tree {
	x := p.Get().(*Tree)
	x.cmp = cmp
	return x
}

type btEpool struct{ sync.Pool }

func (p *btEpool) get(err error, hit bool, i int, k int64, q *d, t *Tree, ver int64) *Enumerator {
	x := p.Get().(*Enumerator)
	x.err, x.hit, x.i, x.k, x.q, x.t, x.ver = err, hit, i, k, q, t, ver
	return x
}

type (
	// Cmp compares a and b. Return value is:
	//
	//	< 0 if a <  b
	//	  0 if a == b
	//	> 0 if a >  b
	//
	Cmp func(a, b int64) int

	d struct { // data page
		c int
		d [2*kd + 1]de
		n *d
		p *d
	}

	de struct { // d element
		k int64
		v *Primitive
	}

	// Enumerator captures the state of enumerating a tree. It is returned
	// from the Seek* methods. The enumerator is aware of any mutations
	// made to the tree in the process of enumerating it and automatically
	// resumes the enumeration at the proper key, if possible.
	//
	// However, once an Enumerator returns io.EOF to signal "no more
	// items", it does no more attempt to "resync" on tree mutation(s).  In
	// other words, io.EOF from an Enumaretor is "sticky" (idempotent).
	Enumerator struct {
		err error
		hit bool
		i   int
		k   int64
		q   *d
		t   *Tree
		ver int64
	}

	// Tree is a B+tree.
	Tree struct {
		c     int
		cmp   Cmp
		first *d
		last  *d
		r     interface{}  // 这个既有可能是index page,也有可能是data page
		ver   int64
	}

	xe struct { // x element
		ch interface{}
		k  int64
	}

	x struct { // index page
		c int
		x [2*kx + 2]xe
	}
)

var ( // R/O zero values
	zd  d
	zde de
	ze  Enumerator
	zk  int64
	zt  Tree
	zx  x
	zxe xe
)

func clr(q interface{}) {
	switch x := q.(type) {
	case *x:
		// todo: 这个for循环是干什么的
		for i := 0; i <= x.c; i++ { // Ch0 Sep0 ... Chn-1 Sepn-1 Chn
			clr(x.x[i].ch)
		}
		// 塞入空值
		*x = zx
		btXPool.Put(x)
	case *d:
		//塞入空值
		*x = zd
		btDPool.Put(x)
	}
}

// -------------------------------------------------------------------------- x

func newX(ch0 interface{}) *x {
	r := btXPool.Get().(*x)
	// 定长:2*kx+2
	r.x[0].ch = ch0 //给xe赋值
	return r
}

// 提取出第i个xe(覆盖第i位）
func (q *x) extract(i int) {
	q.c--
	if i < q.c {
		// [i+1,c+1)挪到[i,c)
		copy(q.x[i:], q.x[i+1:q.c+1])
		// c+1的ch值赋值给c
		q.x[q.c].ch = q.x[q.c+1].ch
		// 为啥要赋值-gc
		q.x[q.c].k = zk  // GC
		q.x[q.c+1] = zxe // GC
	}
}

/*
|k |k |......|k |
|ch|ch|......|ch|
*/
// 相当于第i位的k成员插入k值，第i+1位的ch成员插入ch值
func (q *x) insert(i int, k int64, ch interface{}) *x {
	c := q.c
	if i < c {
		// c的ch赋值到c+1
		q.x[c+1].ch = q.x[c].ch
		// [i+1,c)拷贝到[i+2, c+1)
		copy(q.x[i+2:], q.x[i+1:c])
		// i的k值赋值给i+1
		q.x[i+1].k = q.x[i].k //挪动数组
	}
	c++
	q.c = c          // c(count)++
	q.x[i].k = k     // 插入新的元素
	q.x[i+1].ch = ch // 多插入一个页
	return q
}

// ch的本质是*d(data page)
func (q *x) siblings(i int) (l, r *d) {
	if i >= 0 {
		if i > 0 {
			// 前一页的data page
			l = q.x[i-1].ch.(*d)
		}
		if i < q.c {
			// 后data page
			r = q.x[i+1].ch.(*d)
		}
	}
	return
}

// -------------------------------------------------------------------------- d
// 将r的若干个元素(c个元素)移动到l的右侧
func (l *d) mvL(r *d, c int) {
	// 将r的[0,c)拷贝到l的[c,+00)
	copy(l.d[l.c:], r.d[:c])
	copy(r.d[:], r.d[c:r.c])
	l.c += c
	r.c -= c
}

// 将l的若干个元素(c个元素)移动到r的右侧
func (l *d) mvR(r *d, c int) {
	copy(r.d[c:], r.d[:r.c])
	copy(r.d[:c], l.d[l.c-c:])
	r.c += c
	l.c -= c
}

// ----------------------------------------------------------------------- Tree

// TreeNew returns a newly created, empty Tree. The compare function is used
// for key collation.
// 创建一棵空树
func TreeNew(cmp Cmp) *Tree {
	return btTPool.get(cmp)
}

// Clear removes all K/V pairs from the tree.
// 清空空树
func (t *Tree) Clear() {
	if t.r == nil {
		return
	}

	clr(t.r)
	t.c, t.first, t.last, t.r = 0, nil, nil, nil
	t.ver++
}

// Close performs Clear and recycles t to a pool for possible later reuse. No
// references to t should exist or such references must not be used afterwards.
func (t *Tree) Close() {
	t.Clear()
	*t = zt
	// 放回sync.Pool
	btTPool.Put(t)
}

// 将r合并到q中，然后将q塞入到Tree t的r里面(会把pi位清空，放进q）
func (t *Tree) cat(p *x, q, r *d, pi int) {
	t.ver++ // 版本号+1?
	q.mvL(r, r.c)  // 将r的元素移动到q(相当于q和r合并在一起了)
	// n -- next
	if r.n != nil {
		r.n.p = q  // r的下一页的前继修改成q
	} else {
		// r没有下一页，证明r是最后一页
		t.last = q
	}
	q.n = r.n
	// 将r拼接到q上面，q取代了r的位置
	*r = zd  // r置成0值
	btDPool.Put(r)  // 将r进行回收。
	if p.c > 1 {
		p.extract(pi)  // 把pi这一位清空
		p.x[pi].ch = q  // indx page的pi下标塞下q这一页
	} else { // p.c <= 0
		switch x := t.r.(type) {
		case *x:
			*x = zx
			btXPool.Put(x)  // 回收
		case *d:
			*x = zd
			btDPool.Put(x)
		}
		t.r = q  // 空树，直接塞入data page，即叶子节点即可
	}
}

// 将r合并到q中，再塞入p的第pi位
func (t *Tree) catX(p, q, r *x, pi int) {
	t.ver++
	// 将p的第pi个x的k值赋值给q的最后一位(q.c)的k
	q.x[q.c].k = p.x[pi].k
	// 将r的x[0, c)赋值给q的[c+1, +00)
	copy(q.x[q.c+1:], r.x[:r.c])
	// q的c加上r.c和pi这一位
	q.c += r.c + 1
	// r的最后一位ch赋值给q的最后一位的ch
	q.x[q.c].ch = r.x[r.c].ch
	*r = zx
	btXPool.Put(r)  // 回收r
	if p.c > 1 {
		p.c--  // p这一位去掉
		pc := p.c
		if pi < pc {
			// p的pi+1位的数据覆写到p上的pi位上
			p.x[pi].k = p.x[pi+1].k
			// 将[pi+2, pc+1)拷贝到[pi+1, pc)上
			copy(p.x[pi+1:], p.x[pi+2:pc+1])
			p.x[pc].ch = p.x[pc+1].ch
			p.x[pc].k = zk     // GC
			p.x[pc+1].ch = nil // GC
		}
		return
	}
	// p.c <= 0
	// 回收r
	switch x := t.r.(type) {
	case *x:
		*x = zx
		btXPool.Put(x)
	case *d:
		*x = zd
		btDPool.Put(x)
	}
	t.r = q
}

// Delete removes the k's KV pair, if it exists, in which case Delete returns
// true.
func (t *Tree) Delete(k int64) (ok bool) {
	pi := -1
	var p *x
	q := t.r  // t.r means Tree::root
	// 树为空?
	if q == nil {
		return false
	}

	for {
		var i int
		// 在q上查找k
		i, ok = t.find(q, k)  // 在data page或者index page上进行二分
		if ok {  // 找到了
			switch x := q.(type) {
			case *x:  // q是一个index page
				// 单个页的容量小于kx,进行收缩
				if x.c < kx && q != t.r {
					x, i = t.underflowX(p, x, pi, i)
				}
				pi = i + 1
				p = x
				q = x.x[pi].ch
				ok = false
				continue
			case *d:  // 找到了，是一个data page
				t.extract(x, i)  // 在data page删除第i位
				if x.c >= kd {
					return true
				}
				// x.c < kd
				if q != t.r {
					t.underflow(p, x, pi)  // 把x放入p中进行合并
				} else if t.c == 0 {
					t.Clear()  // 整棵树进行清空
				}
				return true
			}
		}

		// 找不到
		switch x := q.(type) {
		case *x:
			if x.c < kx && q != t.r {
				x, i = t.underflowX(p, x, pi, i)
			}
			pi = i // 下一坐标点i
			p = x  // p是前继
			q = x.x[i].ch   // 递归到下一页进行查找
		case *d:  // data page，无法继续查找，返回false
			return false
		}
	}
}

// 将data page q的第i位下标的元素去掉
func (t *Tree) extract(q *d, i int) { // (r *primitive) {
	t.ver++
	//r = q.d[i].v // prepared for Extract
	q.c--
	if i < q.c {
		// [i+1, c+1)拷贝到[i, c)
		copy(q.d[i:], q.d[i+1:q.c+1])
	}
	q.d[q.c] = zde // GC
	t.c--
	return
}

// 在q中寻找k（q也有可能是个index page，也有可能是个data page）
func (t *Tree) find(q interface{}, k int64) (i int, ok bool) {
	var mk int64
	l := 0
	// 检查q的类型
	switch x := q.(type) {
	// 索引类型，直接在索引页进行二分
	case *x: // index page
		h := x.c - 1
		for l <= h {
			m := (l + h) >> 1 //二分
			mk = x.x[m].k     //mk:对应的值?
			switch cmp := t.cmp(k, mk); {
			case cmp > 0:
				l = m + 1
			case cmp == 0:
				return m, true
			default:
				h = m - 1
			}
		}
	case *d: // data page
		// data page:进行二分
		h := x.c - 1
		for l <= h {
			m := (l + h) >> 1
			mk = x.d[m].k
			switch cmp := t.cmp(k, mk); {
			case cmp > 0:
				l = m + 1
			case cmp == 0:
				return m, true
			default:
				h = m - 1
			}
		}
	}
	return l, false
}

// First returns the first item of the tree in the key collating order, or
// (zero-value, zero-value) if the tree is empty.
func (t *Tree) First() (k int64, v *Primitive) {
	// 第一个数据页的第一个元素的k和v
	if q := t.first; q != nil {
		q := &q.d[0]
		k, v = q.k, q.v
	}
	return
}

// Get returns the value associated with k and true if it exists. Otherwise Get
// returns (zero-value, false).
func (t *Tree) Get(k int64) (v *Primitive, ok bool) {
	q := t.r
	if q == nil {
		return
	}

	for {
		var i int
		if i, ok = t.find(q, k); ok {
			switch x := q.(type) {
			case *x:  // 还只是定位到index page，继续查找
				q = x.x[i+1].ch
				continue
			case *d:
				return x.d[i].v, true
			}
		}
		switch x := q.(type) {
		case *x:
			q = x.x[i].ch  // 继续递归查找下一页
		default:
			return
		}
	}
}

// 插入节点
func (t *Tree) insert(q *d, i int, k int64, v *Primitive) *d {
	t.ver++ //版本号？
	c := q.c
	if i < c {
		// [i: c) 移动到[i+1:c+1]
		copy(q.d[i+1:], q.d[i:c])
	}
	c++
	q.c = c                   // 追加到末尾(之所以要用这种方式，是因为需要预分配空间)
	q.d[i].k, q.d[i].v = k, v // 对kv分别赋值
	t.c++
	return q
}

// Last returns the last item of the tree in the key collating order, or
// (zero-value, zero-value) if the tree is empty.
// 返回B+树的最后一位元素
func (t *Tree) Last() (k int64, v *Primitive) {
	if q := t.last; q != nil {
		q := &q.d[q.c-1]
		k, v = q.k, q.v
	}
	return
}

// Len returns the number of items in the tree.
func (t *Tree) Len() int {
	return t.c  // 返回B+树上的节点个数
}

func (t *Tree) overflow(p *x, q *d, pi, i int, k int64, v *Primitive) {
	t.ver++
	// 在索引中分成两块: l和r(data page)
	l, r := p.siblings(pi)

	if l != nil && l.c < 2*kd {
		l.mvL(q, 1)  // 将q的一个元素移动到l的右侧
		t.insert(q, i-1, k, v)
		p.x[pi-1].k = q.d[0].k  // 不需要分裂
		return
	}

	if r != nil && r.c < 2*kd {
		if i < 2*kd {
			q.mvR(r, 1)  // 将q的一个元素移动到r的右侧
			t.insert(q, i, k, v)
			p.x[pi].k = r.d[0].k
		} else {  // i >= 2*kd
			t.insert(r, 0, k, v)  // 直接r前面追加
			p.x[pi].k = k
		}
		return
	}
	// 需要分裂插入了
	t.split(p, q, pi, i, k, v)
}

// Seek returns an Enumerator positioned on a an item such that k >= item's
// key. ok reports if k == item.key The Enumerator's position is possibly
// after the last item in the tree.
func (t *Tree) Seek(k int64) (e *Enumerator, ok bool) {
	q := t.r
	if q == nil {
		e = btEPool.get(nil, false, 0, k, nil, t, t.ver)
		return
	}

	for {
		var i int
		if i, ok = t.find(q, k); ok {
			switch x := q.(type) {
			case *x:
				q = x.x[i+1].ch
				continue
			case *d:
				return btEPool.get(nil, ok, i, k, x, t, t.ver), true
			}
		}

		switch x := q.(type) {
		case *x:
			q = x.x[i].ch
		case *d:
			return btEPool.get(nil, ok, i, k, x, t, t.ver), false
		}
	}
}

// SeekFirst returns an enumerator positioned on the first KV pair in the tree,
// if any. For an empty tree, err == io.EOF is returned and e will be nil.
func (t *Tree) SeekFirst() (e *Enumerator, err error) {
	q := t.first
	if q == nil {
		return nil, io.EOF
	}

	return btEPool.get(nil, true, 0, q.d[0].k, q, t, t.ver), nil
}

// SeekLast returns an enumerator positioned on the last KV pair in the tree,
// if any. For an empty tree, err == io.EOF is returned and e will be nil.
func (t *Tree) SeekLast() (e *Enumerator, err error) {
	q := t.last
	if q == nil {
		return nil, io.EOF
	}

	return btEPool.get(nil, true, q.c-1, q.d[q.c-1].k, q, t, t.ver), nil
}

// Set sets the value associated with k.
func (t *Tree) Set(k int64, v *Primitive) {
	//dbg("--- PRE Set(%v, %v)\n%s", k, v, t.dump())
	//defer func() {
	//	dbg("--- POST\n%s\n====\n", t.dump())
	//}()

	pi := -1
	var p *x
	q := t.r
	// 空的B+树
	if q == nil {
		// btDPool: data page的内存池
		// 插入新的data page
		z := t.insert(btDPool.Get().(*d), 0, k, v)
		t.r, t.first, t.last = z, z, z
		return
	}

	for {
		i, ok := t.find(q, k)
		if ok {
			switch x := q.(type) {
			case *x: // index page容量超过64,split
				if x.c > 2*kx {
					x, i = t.splitX(p, x, pi, i)
				}
				pi = i + 1
				p = x
				q = x.x[i+1].ch
				continue
			case *d:
				x.d[i].v = v
			}
			return
		}

		// 不ok，在原B+树上找不到
		switch x := q.(type) {
		case *x:
			if x.c > 2*kx {
				// 超过大小，直接分裂
				x, i = t.splitX(p, x, pi, i)
			}
			pi = i
			p = x
			q = x.x[i].ch
		case *d:
			switch {
			case x.c < 2*kd:
				t.insert(x, i, k, v) // 直接插入
			default:
				t.overflow(p, x, pi, i, k, v)
			}
			return
		}
	}
}

// Put combines Get and Set in a more efficient way where the tree is walked
// only once. The upd(ater) receives (old-value, true) if a KV pair for k
// exists or (zero-value, false) otherwise. It can then return a (new-value,
// true) to create or overwrite the existing value in the KV pair, or
// (whatever, false) if it decides not to create or not to update the value of
// the KV pair.
//
// 	tree.Set(k, v) call conceptually equals calling
//
// 	tree.Put(k, func(int64, bool){ return v, true })
//
// modulo the differing return values.
func (t *Tree) Put(k int64, upd func(oldV *Primitive, exists bool) (newV *Primitive, write bool)) (oldV *Primitive, written bool) {
	pi := -1
	var p *x
	q := t.r
	var newV *Primitive
	if q == nil {
		// new KV pair in empty tree
		newV, written = upd(newV, false)
		if !written {
			return
		}

		z := t.insert(btDPool.Get().(*d), 0, k, newV)
		t.r, t.first, t.last = z, z, z
		return
	}

	for {
		i, ok := t.find(q, k)
		if ok {
			switch x := q.(type) {
			case *x:
				if x.c > 2*kx {
					x, i = t.splitX(p, x, pi, i)
				}
				pi = i + 1
				p = x
				q = x.x[i+1].ch
				continue
			case *d:
				oldV = x.d[i].v
				newV, written = upd(oldV, true)
				if !written {
					return
				}

				x.d[i].v = newV
			}
			return
		}

		switch x := q.(type) {
		case *x:
			if x.c > 2*kx {
				x, i = t.splitX(p, x, pi, i)
			}
			pi = i
			p = x
			q = x.x[i].ch
		case *d: // new KV pair
			newV, written = upd(newV, false)
			if !written {
				return
			}

			switch {
			case x.c < 2*kd:
				t.insert(x, i, k, newV)
			default:
				t.overflow(p, x, pi, i, k, newV)
			}
			return
		}
	}
}

func (t *Tree) split(p *x, q *d, pi, i int, k int64, v *Primitive) {
	t.ver++
	r := btDPool.Get().(*d)  // 取出一个新的data page
	if q.n != nil {  // q.next 不为空
		r.n = q.n  // 新的页next指向q的next
		r.n.p = r  // 修改下一页的前继
	} else {
		t.last = r
	}
	q.n = r  // q -> r -> q.n(原)
	r.p = q

	// 把q中超出kd的部分赋值给r
	copy(r.d[:], q.d[kd:2*kd])
	for i := range q.d[kd:] {
		q.d[kd+i] = zde  // 将原q中超出kd的部分清0
	}
	// 调整两个页的数量
	q.c = kd
	r.c = kd
	var done bool
	if i > kd {
		done = true
		t.insert(r, i-kd, k, v)  // 超过了kd，应该写入到r的data page中
	}
	// i <= kd
	if pi >= 0 {
		p.insert(pi, r.d[0].k, r)  // 将新的一页r放入index page的pi下表中
	} else {
		// pi < 0空树
		t.r = newX(q).insert(0, r.d[0].k, r)
	}
	if done {
		return
	}
	// i <= kd, 直接在原来的页插入即可
	t.insert(q, i, k, v)
}

// 分裂页
func (t *Tree) splitX(p *x, q *x, pi int, i int) (*x, int) {
	t.ver++
	r := btXPool.Get().(*x)
	copy(r.x[:], q.x[kx+1:]) //拷贝到新的index page中
	q.c = kx
	r.c = kx
	if pi >= 0 {
		// 将k,r塞到p中
		p.insert(pi, q.x[kx].k, r)
		q.x[kx].k = zk
		for i := range q.x[kx+1:] {
			q.x[kx+i+1] = zxe
		}

		switch {
		case i < kx:
			return q, i
		case i == kx:
			return p, pi
		default: // i > kx
			return r, i - kx - 1
		}
	}
	// pi < 0
	nr := newX(q).insert(0, q.x[kx].k, r)
	t.r = nr
	q.x[kx].k = zk
	for i := range q.x[kx+1:] {
		q.x[kx+i+1] = zxe
	}

	switch {
	case i < kx:
		return q, i
	case i == kx:
		return nr, 0
	default: // i > kx
		return r, i - kx - 1
	}
}

// p是q的上继
func (t *Tree) underflow(p *x, q *d, pi int) {
	t.ver++
	l, r := p.siblings(pi)  // pi拆出左右两个页p.x[pi-1].ch, p.x[pi+1].ch

	if l != nil && l.c+q.c >= 2*kd {
		l.mvR(q, 1)  // 将l的一个元素移动到q的右侧
		p.x[pi-1].k = q.d[0].k
	} else if r != nil && q.c+r.c >= 2*kd {
		q.mvL(r, 1)  // 将r的一个元素移动到q的右侧
		p.x[pi].k = r.d[0].k
		r.d[r.c] = zde // GC
	} else if l != nil {
		t.cat(p, l, q, pi-1)  // 将q合并到l，因为l.c+q.c < 2*kd,p清空pi-1位，将l放入p，并且如果树为空作为树的第一个根
	} else {
		t.cat(p, q, r, pi)  // 否则，将r合并到q，因为l.c+q.c < 2*kd,p清空pi-1位，将q放入p
	}
}

// 合并B+树节点（将q塞到p的pi下标里面）
func (t *Tree) underflowX(p *x, q *x, pi int, i int) (*x, int) {
	t.ver++
	var l, r *x

	if pi >= 0 {
		if pi > 0 {
			l = p.x[pi-1].ch.(*x)  // 取出p的l和r两个页
		}
		if pi < p.c {
			r = p.x[pi+1].ch.(*x)
		}
	}

	if l != nil && l.c > kx {
		// q.x[c].ch赋值给q.x[c+1].ch
		q.x[q.c+1].ch = q.x[q.c].ch
		// 讲q的[0,c)赋值给q的[1,c+1)
		copy(q.x[1:], q.x[:q.c])
		// 把l拼接到q前面
		q.x[0].ch = l.x[l.c].ch
		// p的x[i-1]的k索引到q.x[0]的k
		q.x[0].k = p.x[pi-1].k  // 相当于将l和q桥接起来
		q.c++
		i++
		l.c--
		p.x[pi-1].k = l.x[l.c].k
		return q, i
	}

	if r != nil && r.c > kx {
		q.x[q.c].k = p.x[pi].k
		q.c++
		q.x[q.c].ch = r.x[0].ch
		p.x[pi].k = r.x[0].k
		copy(r.x[:], r.x[1:r.c])
		r.c--
		rc := r.c
		r.x[rc].ch = r.x[rc+1].ch
		r.x[rc].k = zk
		r.x[rc+1].ch = nil
		return q, i
	}

	// l.c <= kx
	if l != nil {
		i += l.c + 1
		t.catX(p, l, q, pi-1)  // 将l和q进行合并，塞入p的第pi-1位
		q = l
		return q, i
	}
	// r.c <= kx
	t.catX(p, q, r, pi)
	return q, i
}

// ----------------------------------------------------------------- Enumerator

// Close recycles e to a pool for possible later reuse. No references to e
// should exist or such references must not be used afterwards.
func (e *Enumerator) Close() {
	*e = ze
	btEPool.Put(e)
}

// Next returns the currently enumerated item, if it exists and moves to the
// next item in the key collation order. If there is no item to return, err ==
// io.EOF is returned.
func (e *Enumerator) Next() (k int64, v *Primitive, err error) {
	if err = e.err; err != nil {
		return
	}

	if e.ver != e.t.ver {
		f, hit := e.t.Seek(e.k)
		if !e.hit && hit {
			if err = f.next(); err != nil {
				return
			}
		}

		*e = *f
		f.Close()
	}
	if e.q == nil {
		e.err, err = io.EOF, io.EOF
		return
	}

	if e.i >= e.q.c {
		if err = e.next(); err != nil {
			return
		}
	}

	i := e.q.d[e.i]
	k, v = i.k, i.v
	e.k, e.hit = k, false
	e.next()
	return
}

func (e *Enumerator) next() error {
	if e.q == nil {
		e.err = io.EOF
		return io.EOF
	}

	switch {
	case e.i < e.q.c-1:
		e.i++
	default:
		if e.q, e.i = e.q.n, 0; e.q == nil {
			e.err = io.EOF
		}
	}
	return e.err
}

// Prev returns the currently enumerated item, if it exists and moves to the
// previous item in the key collation order. If there is no item to return, err
// == io.EOF is returned.
func (e *Enumerator) Prev() (k int64, v *Primitive, err error) {
	if err = e.err; err != nil {
		return
	}

	if e.ver != e.t.ver {
		f, hit := e.t.Seek(e.k)
		if !e.hit && hit {
			if err = f.prev(); err != nil {
				return
			}
		}

		*e = *f
		f.Close()
	}
	if e.q == nil {
		e.err, err = io.EOF, io.EOF
		return
	}

	if e.i >= e.q.c {
		if err = e.next(); err != nil {
			return
		}
	}

	i := e.q.d[e.i]
	k, v = i.k, i.v
	e.k, e.hit = k, false
	e.prev()
	return
}

func (e *Enumerator) prev() error {
	if e.q == nil {
		e.err = io.EOF
		return io.EOF
	}

	switch {
	case e.i > 0:
		e.i--
	default:
		if e.q = e.q.p; e.q == nil {
			e.err = io.EOF
			break
		}

		e.i = e.q.c - 1
	}
	return e.err
}

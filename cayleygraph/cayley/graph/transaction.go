// Copyright 2015 The Cayley Authors. All rights reserved.
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

package graph

import "github.com/cayleygraph/quad"

// Transaction stores a bunch of Deltas to apply together in an atomic step on the database.
type Transaction struct {
	// Deltas stores the deltas in the right order
	// 保证事务的操作在正确的顺序
	Deltas []Delta
	// deltas stores the deltas in a map to avoid duplications
	// 保证事务操作不重复
	deltas map[Delta]struct{}
}

// NewTransaction initialize a new transaction.
// 开启事务
func NewTransaction() *Transaction {
	return NewTransactionN(10)
}

// NewTransactionN initialize a new transaction with a predefined capacity.
// 初始化事务的个数
func NewTransactionN(n int) *Transaction {
	return &Transaction{Deltas: make([]Delta, 0, n), deltas: make(map[Delta]struct{}, n)}
}

// AddQuad adds a new quad to the transaction if it is not already present in it.
// If there is a 'remove' delta for that quad, it will remove that delta from
// the transaction instead of actually adding the quad.
// 加入边
func (t *Transaction) AddQuad(q quad.Quad) {
	// 创建指令
	ad, rd := createDeltas(q)

	if _, adExists := t.deltas[ad]; !adExists {
		// 看是创建还是删除
		if _, rdExists := t.deltas[rd]; rdExists {
			t.deleteDelta(rd)
		} else {
			t.addDelta(ad)
		}
	}
}

// RemoveQuad adds a quad to remove to the transaction.
// The quad will be removed from the database if it is not present in the
// transaction, otherwise it simply remove it from the transaction.
// 删除边
func (t *Transaction) RemoveQuad(q quad.Quad) {
	ad, rd := createDeltas(q)

	if _, adExists := t.deltas[ad]; adExists {
		t.deleteDelta(ad)
	} else {
		if _, rdExists := t.deltas[rd]; !rdExists {
			t.addDelta(rd)
		}
	}
}

// 生成delta指令
func createDeltas(q quad.Quad) (ad, rd Delta) {
	ad = Delta{
		Quad:   q,
		Action: Add,
	}
	rd = Delta{
		Quad:   q,
		Action: Delete,
	}
	return
}

// 增加指令
func (t *Transaction) addDelta(d Delta) {
	t.Deltas = append(t.Deltas, d)
	t.deltas[d] = struct{}{}
}

// 删除指令
func (t *Transaction) deleteDelta(d Delta) {
	delete(t.deltas, d)

	for i, id := range t.Deltas {
		if id == d {
			t.Deltas = append(t.Deltas[:i], t.Deltas[i+1:]...)
			break
		}
	}
}

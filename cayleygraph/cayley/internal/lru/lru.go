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

package lru

import (
	"container/list"
	"sync"
)

// TODO(kortschak) Reimplement without container/list.

// Cache implements an LRU cache.
type Cache struct {
	mu       sync.Mutex
	cache    map[string]*list.Element
	priority *list.List
	maxSize  int
}

type kv struct {
	key   string
	value interface{}
}

func New(size int) *Cache {
	return &Cache{
		maxSize:  size,
		priority: list.New(),
		cache:    make(map[string]*list.Element),
	}
}

func (lru *Cache) Put(key string, value interface{}) {
	if _, ok := lru.Get(key); ok {
		return
	}

	lru.mu.Lock()
	defer lru.mu.Unlock()
	if len(lru.cache) == lru.maxSize {
		// 删除最近最少使用的key
		last := lru.priority.Remove(lru.priority.Back())
		delete(lru.cache, last.(kv).key)
	}
	lru.priority.PushFront(kv{key: key, value: value})  // 把(key, value)都放进priority链表
	lru.cache[key] = lru.priority.Front()
}

func (lru *Cache) Del(key string) {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	e := lru.cache[key]  // 取出的那个地址
	if e == nil {
		return
	}
	delete(lru.cache, key)
	lru.priority.Remove(e)  // 链表删除指定元素
}

func (lru *Cache) Get(key string) (interface{}, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()
	if element, ok := lru.cache[key]; ok {
		lru.priority.MoveToFront(element)  // 移动到priority链表头部
		return element.Value.(kv).value, true
	}
	return nil, false
}

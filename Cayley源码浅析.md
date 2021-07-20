# Cayley源码浅析

## 1. Cayley概述

Cayley是谷歌的一个基于关联数据的开源图数据库，其灵感来自于谷歌和Freebase的知识图谱背后的图数据库。

### 1.1. 特点

- 内建查询编辑器和可视化界面；
- 支持多种图查询语言，包含Gizmo，GraphQL，MQL；
- 标准模块化，用户能够轻松对接你习惯的编程语言以及后端存储；
- 性能良好，测试覆盖率高，功能丰富强大。

### 1.2. 性能

粗略的性能测试统计，在普通的PC硬件和硬盘上，使用leveldb作为后端存储的条件下，建立134 million条边的基础下，进行多跳复杂查询没有别的问题，耗时约为150ms。（官方数据）

github地址：https://github.com/cayleygraph/cayley

## 2. 整体架构

![image-20210202125547919](https://github.com/sysublackbear/cayleygraph_analysis/blob/main/img/2.png?raw=true)

查询的语句进入Cayley之后，经过一些列的语句转换之后，变成了基于Iterator(Shape)相关的查询方法，然后进入到底层，会变成两个Handle，Handle由读（QuadStore）和写（QuadWriter）两块组成的，下层会注册多种存储，包含memstore, kv, sql和nosql。

## 3. 名词解释

### Triple

由subject-predicate-object（主-谓-宾）构成的数据三元组，诸如：“Bob35岁了”或者“Alice认识Bob”。

### Triplestore

专门为存储和访问Triple而建的数据库。

### Quad

1. 在Triple的基础上，一个quad拥有(subject, predicate, object, label)，其中subject, predicate和object是必填的，label属性选填。
2. label的目的在于更方便地定义出子图以及添加附加的属性。
3. 在cayley中，quad的定义如下：

```go
// Our quad struct, used throughout.
type Quad struct {
   Subject   Value `json:"subject"`
   Predicate Value `json:"predicate"`
   Object    Value `json:"object"`
   Label     Value `json:"label,omitempty"`
}
```

换句话而言，对于cayley而言，其实没有明确的对“点”的定义，所有的存储都归结为到一个基本四元组的数据定义(Quad），可以简单理解成是一条边。

### Link

1. triple的另一种命名，它看起来那个更像是连接任意两个点的边。
2. 给定triple元组`{A, knows, C}`，你可以看做是在图里面，A和C都作为“点”，然后knows作为“边”。当然，你可以从另一个角度去看：A，knows和C都是点，然后它们都被triple内置的虚拟顶点所连接着。

### IRI

1. IRI（国际化资源标识符）是RDF（资源描述框架）的一种标识符。
2. 一个RDF的图下的IRI是一个遵循RFC 3987转换规则的Unicode字符串。
3. IRI更是URI的广义化概念，一个URI或者URL一定是一个IRI，但并不是每个IRI都是URI。

### RDF

1. 资源描述框架，是各种quad集合的标准定义；
2. 一个RDF三元组由以下三个部分组成：
   1. subject，是一个IRI或者是一个空节点；
   2. predicate，是一个IRI；
   3. object，通常是一个IRI，固定值或者是一个空节点。

### Gizmo

一个受[Gremlin/TinkerPop](http://tinkerpop.apache.org/)所启发的图查询语言，为Cayley定制。看起来像JavaScript。

### g.V()

1. 对于Gremlin/TinkerPop，g.V()返回了图中的所有点的列表。
2. `.v()` 在Gizmo作为点的含义，用法诸如：`pathObject = graph.Vertex([nodeId], [nodeId]...)`

### Direction

Direction指定了在一条quad内部，一个node所在的位置，如下：

```go
const (
 Any Direction = iota
 Subject
 Predicate
 Object
 Label
)
```

### Path

1. Path是建立一个查询的工具，但是它们不支持建立更复杂的查询。你能够使用Shapes应付复杂查询的场景。
2. Path既可以代表一个morphism(一个提前定义的path对象，后面会使用），或者就是一个具体的path，它由一个morphism和一个潜在的QuadStore。
3. 相关的定义：

```go
type Path struct {
   stack       []morphism
   qs          graph.QuadStore
   baseContext pathContext
}
type morphism struct {
   IsTag    bool
   Reversal func(*pathContext) (morphism, *pathContext)
   Apply    applyMorphism
   tags     []string
}
type applyMorphism func(shape.Shape, *pathContext) (shape.Shape, *pathContext)
```

### Morphism

Morphism是不与具体的quadstore和具体的起点绑定在一起的路径。

### Iterator

一个图的查询可以被粗略代表为一棵树的迭代器，因此需要实现接口`graph.Iterator`的所有方法。迭代器可以粗略被认为关联到图里面一个具体的部分元素的集合。

### Subiterator

1. 由于一个图的查询可以被粗略代表为一棵树的迭代器，计算值的步骤是位于树的顶点重复调用`Next()`方法获取。Subiterators，相当于树的分支和叶子节点。

2. 举个例子，我们想要转换Cayley-Gremlin-Go-API查询 `g.V("B").In("follows").All()`成一个遍历树，步骤如下：

   1. **HasA**(subject) —— 在subject字段获取数据：

      1. **And**—— 如下若干个子查询的交集：

         1. **LinksTo**(predicate) 指向具备了predicate的所有连接：

            固定的包含"follows"的迭代器 —— 就是follows的节点。

         2. **LinksTo**(object) 指向具备了object的所有连接：

            固定的包含"B"的迭代器 —— 就是B的节点。

### LinkTo iterator

一个LinkTo的迭代器包含了传入一个子迭代器，然后包含了给定方向指向这些子迭代器节点的集合。

### HasA iterator

一个HasA的给定了direction下所有连接的迭代器集合。这个名字由“link HasA subject”灵感而来。

### Shape

1. Shape代表一个查询树的外形。如下：

```go
type Shape interface {
    BuildIterator(qs graph.QuadStore) graph.Iterator
    Optimize(r Optimizer) (Shape, bool)
}
```

2. Shape是查询系统里面最有趣的部分了——它确切描述了一个查询应该看起来是什么样子的。这个相当于一个查询的基本组成单元了，后面会进行详细介绍。

项目的核心代码在graph目录，下面围绕着graph目录进行详细介绍。



## 4. 核心类图

![2](/Users/bytedance/github/cayleygraph_analysis/img/2.png)

## 5. Shape

### 5.1. Shape的定义

```go
type Shape interface {
   // 迭代器的简单介绍
   String() string

   // 新建以scanning模式的迭代器,将会返回所有的结果，但不一定是强制有序的
   // 调用方使用完毕需要关闭迭代器
   Iterate() Scanner

   // 新建以索引查找模式的迭代器，取决于迭代器的类型，这个会跟数据库的scans有关
   // 这个能够检查索引是否包含了某个特定的值。调用方使用完毕需要关闭迭代器。
   Lookup() Index
   
   // 下面的几个方法与怎么选择合适的迭代器，或者优化迭代树的查询有关。

   // Stats返回使用特定的迭代器的遍历方法时所产生的“开销”，以及数据的总条数。
   // 粗略地计算，需要花费NextCost * Size的开销单元来获取本迭代器以外的数据。
   // 这是一个不精确的值，但是是一个实用的启发式的算法。
   Stats(ctx context.Context) (Costs, error)

   // Optimize会对迭代器进行表达优化，第二个参数代表经过优化后，是否替代了原来的迭代器
   Optimize(ctx context.Context) (Shape, bool)

   // SubIterators Return a slice of the subiterators for this iterator.
   SubIterators() []Shape
}

// Base是对于Scanner和Index的通用方法集
type Base interface {
   // String returns a short textual representation of an iterator.
   String() string

   // 填充一个tag-resultvalue的map
   TagResults(map[string]refs.Ref)

   // 返回当前的结果
   Result() refs.Ref

   // NextPath These methods are the heart and soul of the iterator, as they constitute
   // the iteration interface.
   // To get the full results of iteration, do the following:
   //
   //  for it.Next(ctx) {
   //     val := it.Result()
   //     ... do things with val.
   //     for it.NextPath(ctx) {
   //        ... find other paths to iterate
   //     }
   //  }
   //
   // All of them should set iterator.result to be the last returned value, to
   // make results work.
   //
   // NextPath() advances iterators that may have more than one valid result,
   // from the bottom up.
   NextPath(ctx context.Context) bool

   // Err returns any error that was encountered by the Iterator.
   Err() error

   // Close the iterator and do internal cleanup.
   Close() error
}

// Scanner is an iterator that lists all results sequentially, but not necessarily in a sorted order.
type Scanner interface {
   Base

   // Next advances the iterator to the next value, which will then be available through
   // the Result method. It returns false if no further advancement is possible, or if an
   // error was encountered during iteration.  Err should be consulted to distinguish
   // between the two cases.
   Next(ctx context.Context) bool
}

// Index is an index lookup iterator. It allows to check if an index contains a specific value.
type Index interface {
   Base

   // Contains returns whether the value is within the set held by the iterator.
   // It will set Result to the matching subtree. TagResults can be used to collect values from tree branches.
   Contains(ctx context.Context, v refs.Ref) bool
}
```

基本cayley内部，所有的算子都是继承`Shape`来实现的。

### 5.2. Shape类图

![3](/Users/bytedance/github/cayleygraph_analysis/img/3.png)

![4](/Users/bytedance/github/cayleygraph_analysis/img/4.png)

看到Cayley支持各种算子，然后每种算子去实现对应它们自身语义的一些逻辑，这个详见：graph/iterator 包。



## 6. Demo

我们可以从两个例子入手，看下常用的调用逻辑。代码详见examples包。

### 6.1. hello_world

```go
package main

import (
   "fmt"
   "log"

   "github.com/cayleygraph/cayley"
   "github.com/cayleygraph/quad"
)

func main() {
   // Create a brand new graph
   store, err := cayley.NewMemoryGraph()
   if err != nil {
      log.Fatalln(err)
   }
   // 往存储加入一条边
   store.AddQuad(quad.Make("phrase of the day", "is of course", "Hello World!", nil))

   // Now we create the path, to get to our data
   // Out -> outMorphism -> Apply -> Shape.Out -> buildOut -> NodesFrom
   p := cayley.StartPath(store, quad.String("phrase of the day")).Out(quad.String("is of course"))

   // Now we iterate over results. Arguments:
   // 1. Optional context used for cancellation.
   // 2. Quad store, but we can omit it because we have already built path with it.

   // Iterate -> p.Shape() -> p.ShapeFrom -> 把之前的叠加的每个算子逐个叠加Apply -> BuildIterator
   // EachValue -> Each -> c.next, c.it.Result, c.nextPath
   err = p.Iterate(nil).EachValue(nil, func(value quad.Value) error {
      nativeValue := quad.NativeOf(value) // this converts RDF values to normal Go types
      fmt.Println(nativeValue)
      return nil
   })
   if err != nil {
      log.Fatalln(err)
   }
}
```

这里每次的查询会新建一个Path对象，Path对象有个特殊的点是，会在链式调用的过程中，一直嵌套迭代器到它自身的stack中。比如调用Out方法，底层实现：

```go
type Path struct {
   stack       []morphism
   qs          graph.QuadStore // Optionally. A nil qs is equivalent to a morphism.
   baseContext pathContext
}

func (p *Path) Out(via ...interface{}) *Path {
   np := p.clone()
   np.stack = append(np.stack, outMorphism(nil, via...))  // 将新生成的morphism追加到stack中
   return np
}

// ...
```

然后在真正的Iterate的时候，会将之前延迟定义的各个morphism进行逐个Apply实施调用，然后建立迭代器，进行常规迭代即可，如下：

```go
// Iterate is an shortcut for graph.Iterate.
func (p *Path) Iterate(ctx context.Context) *iterator.Chain {
   return shape.Iterate(ctx, p.qs, p.Shape())
}
func (p *Path) Shape() shape.Shape {
   // 初始值是个AllNodes迭代器
   return p.ShapeFrom(shape.AllNodes{})
}
func (p *Path) ShapeFrom(from shape.Shape) shape.Shape {
   s := from
   ctx := &p.baseContext
   for _, m := range p.stack {
      s, ctx = m.Apply(s, ctx)   // 逐个调用
   }
   return s
}
```

### 6.2. hello_bolt

```go
package main

import (
   "context"
   "fmt"
   "io/ioutil"
   "log"
   "os"

   "github.com/cayleygraph/cayley"
   "github.com/cayleygraph/cayley/graph"
   _ "github.com/cayleygraph/cayley/graph/kv/bolt"
   "github.com/cayleygraph/quad"
)

func main() {
   // File for your new BoltDB. Use path to regular file and not temporary in the real world
   tmpdir, err := ioutil.TempDir("", "example")
   if err != nil {
      log.Fatal(err)
   }

   defer os.RemoveAll(tmpdir) // clean up

   // Initialize the database
   err = graph.InitQuadStore("bolt", tmpdir, nil)
   if err != nil {
      log.Fatal(err)
   }

   // Open and use the database
   store, err := cayley.NewGraph("bolt", tmpdir, nil)
   if err != nil {
      log.Fatalln(err)
   }

   store.AddQuad(quad.Make("phrase of the day", "is of course", "Hello BoltDB!", "demo graph"))

   // Now we create the path, to get to our data
   p := cayley.StartPath(store, quad.String("phrase of the day")).Out(quad.String("is of course"))

   // This is more advanced example of the query.
   // Simpler equivalent can be found in hello_world example.

   ctx := context.TODO()
   // Now we get an iterator for the path and optimize it.
   // The second return is if it was optimized, but we don't care for now.
   its, _ := p.BuildIterator(ctx).Optimize(ctx)
   it := its.Iterate()

   // remember to cleanup after yourself
   defer it.Close()

   // While we have items
   for it.Next(ctx) {
      token := it.Result()              // get a ref to a node (backend-specific)
      // 把值给查询出来
      value, err := store.NameOf(token) // get the value in the node (RDF)
      if err != nil {
         log.Fatalln(err)
      }
      nativeValue := quad.NativeOf(value) // convert value to normal Go type

      fmt.Println(nativeValue) // print it!
   }
   if err := it.Err(); err != nil {
      log.Fatalln(err)
   }
}
```

代码逻辑类似，不过在遍历的时候没有使用Chain的调用，是直接通过遍历迭代器来实现的。

### 6.3. 事务的使用 —— transaction

```go
package main

import (
   "fmt"
   "log"

   "github.com/cayleygraph/cayley"
   "github.com/cayleygraph/quad"
)

func main() {
   // To see how most of this works, see hello_world -- this just add in a transaction
   store, err := cayley.NewMemoryGraph()
   if err != nil {
      log.Fatalln(err)
   }

   // Create a transaction of work to do
   // NOTE: the transaction is independent of the storage type, so comes from cayley rather than store
   t := cayley.NewTransaction()
   t.AddQuad(quad.Make("food", "is", "good", nil))
   t.AddQuad(quad.Make("phrase of the day", "is of course", "Hello World!", nil))
   t.AddQuad(quad.Make("cats", "are", "awesome", nil))
   t.AddQuad(quad.Make("cats", "are", "scary", nil))
   t.AddQuad(quad.Make("cats", "want to", "kill you", nil))

   // Apply the transaction
   err = store.ApplyTransaction(t)
   if err != nil {
      log.Fatalln(err)
   }

   p := cayley.StartPath(store, quad.String("cats")).Out(quad.String("are"))

   err = p.Iterate(nil).EachValue(nil, func(v quad.Value) error {
      fmt.Println("cats are", v.Native())
      return nil
   })
   if err != nil {
      log.Fatalln(err)
   }
}
```

调用`ApplyTransaction`达到执行事务的目的。

下面分别介绍下cayley的对接几种基于不同存储组件的实现。

## 7. memstore

```go
type QuadStore struct {
   // 当前生成的id号，保证单调递增，任何的属性(subject, predicate, object, label)都会
   // 具有属于它的独一无二的id
   last int64 
   
   vals    map[string]int64
   quads   map[internalQuad]int64
   prim    map[int64]*Primitive
   all     []*Primitive // might not be sorted by id
   // 是否有别的请求在读
   reading bool         // someone else might be reading "all" slice - next insert/delete should clone it
   // 当前的迭代器对象
   index   QuadDirectionIndex
   // 没执行一次事务，计数累加1
   horizon int64 // used only to assign ids to tx
}

type QuadDirectionIndex struct {
   index [4]map[int64]*Tree  // B+树
}

type Primitive struct {
   ID    int64
   Quad  internalQuad
   Value quad.Value
   refs  int
}

// 四元组
type internalQuad struct {
   S, P, O, L int64
}
```

### 7.1. 数据结构说明

![5](/Users/bytedance/github/cayleygraph_analysis/img/5.png)

![6](/Users/bytedance/github/cayleygraph_analysis/img/6.png)

我们可以看到B+树维度的话，每个方向{subject, predicate, object, label}都记录了一棵B+树，B+树的叶子节点都是prim里面的id项。遍历和增删改查围绕上面几个关键要素进行。



## 8. kv

Kv包这边统一使用了hidalgo包的通用接口来统一适配，屏蔽掉下层接入的kv数据库(badgerDB, boltDB, levelDB )的实现细节。

关于hidalgo包，详见：https://github.com/hidal-go/hidalgo/tree/master/kv

![7](/Users/bytedance/github/cayleygraph_analysis/img/7.png)

### 8.1. 核心结构

```go
type QuadStore struct {
   db kv.KV
   
   // 索引结构
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

type QuadIndex struct {
   Dirs   []quad.Direction `json:"dirs"`
   Unique bool             `json:"unique"`
}
```

默认的索引结构：

```go
// DefaultQuadIndexes 两种默认的索引结构
DefaultQuadIndexes = []QuadIndex{
   // First index optimizes forward traversals. Getting all relations for a node should
   // also be reasonably fast (prefix scan).
   {Dirs: []quad.Direction{quad.Subject, quad.Predicate}},

   // Second index helps with reverse traversals as well as full quad lookups.
   // It also prevents issues with super-nodes, since most of those are values
   // with a high in-degree.
   {Dirs: []quad.Direction{quad.Object, quad.Predicate, quad.Subject}},
}
```

Quad的统一结构体：

```go
type Primitive struct {
   ID        uint64 `protobuf:"varint,1,opt,name=ID,json=iD,proto3" json:"ID,omitempty"`
   Subject   uint64 `protobuf:"varint,2,opt,name=Subject,json=subject,proto3" json:"Subject,omitempty"`
   Predicate uint64 `protobuf:"varint,3,opt,name=Predicate,json=predicate,proto3" json:"Predicate,omitempty"`
   Object    uint64 `protobuf:"varint,4,opt,name=Object,json=object,proto3" json:"Object,omitempty"`
   Label     uint64 `protobuf:"varint,5,opt,name=Label,json=label,proto3" json:"Label,omitempty"`
   Replaces  uint64 `protobuf:"varint,6,opt,name=Replaces,json=replaces,proto3" json:"Replaces,omitempty"`
   Timestamp int64  `protobuf:"varint,7,opt,name=Timestamp,json=timestamp,proto3" json:"Timestamp,omitempty"`
   Value     []byte `protobuf:"bytes,8,opt,name=Value,json=value,proto3" json:"Value,omitempty"`
   Deleted   bool   `protobuf:"varint,9,opt,name=Deleted,json=deleted,proto3" json:"Deleted,omitempty"`
}
```

### 8.2. KV结构

![8](/Users/bytedance/github/cayleygraph_analysis/img/8.png)

- meta数据包含version，size， horizon。
- 遍历的时候，会扫描log bucket。

### 8.3. 边的写入步骤

1. 尝试查询是否已经存在了部分的点/边；
2. 对于未曾添加的部分，预分配生成id（对meta-horizon进行累加）；
3. 把每个id和数据结构，打包成一个`Primitive`结构体；
4. 对`Primitive.Value`计算一个hash值；
5. 往value bucket写入这个`Primitive`的ID值，key为hash值的头两个byte；
6. 更新LRU Cache和BloomFilter；
7. 往log bucket写入key为ID，value为`Primitive`的json字符串。
8. 从number bucket中取出对应id的key-value，value记录的是该`Primitive`结构体的引用计数值，更新累加1。

删除流程与增加流程类似，这里不再赘述。

## 9. sql

sql包也统一定义了`Registration`结构体，来适配下层的不同的关系型数据库（cockroach，mysql，postgres，sqlite）。如下：

![9](/Users/bytedance/github/cayleygraph_analysis/img/9.png)

### 9.1. 数据库表设计

#### nodes表（以mysql为例）

| 字段名       | 数据类型    | 备注        |
| ------------ | ----------- | ----------- |
| hash         | BINARY(20)  | Primary key |
| refs         | INT         | Not null    |
| value        | BLOB        |             |
| value_string | TEXT        |             |
| datatype     | TEXT        |             |
| language     | TEXT        |             |
| iri          | BOOLEAN     |             |
| bnode        | BOOLEAN     |             |
| value_int    | BIGINT      |             |
| value_bool   | BOOLEAN     |             |
| value_float  | double      |             |
| value_time   | DATETIME(6) |             |

#### quads表（以mysql为例）

| 字段名         | 数据类型  | 备注        |
| -------------- | --------- | ----------- |
| horizon        | SERIAL    | PRIMARY KEY |
| subject_hash   | SERIAL    | NOT NULL    |
| predicate_hash | SERIAL    | NOT NULL    |
| object_hash    | SERIAL    | NOT NULL    |
| label_hash     | SERIAL    |             |
| ts             | timestamp |             |

> SERIAL is an alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.

### 9.2. 数据结构说明

```go
type QuadStore struct {
   db      *sql.DB
   opt     *Optimizer
   flavor  Registration
   ids     *lru.Cache
   sizes   *lru.Cache
   noSizes bool

   mu    sync.RWMutex
   nodes int64
   quads int64
}

type Value interface {
   SQLValue() interface{}
}

type Shape interface {
   SQL(b *Builder) string
   Args() []Value
   Columns() []string
}

type Iterator struct {
   qs    *QuadStore
   query Select
   err   error
}

// 特定的Select查询结构体
type Select struct {
   Fields []Field
   From   []Source
   Where  []Where
   Params []Value
   Limit  int64
   Offset int64

   // TODO(dennwc): this field in unexported because we don't want it to a be a part of the API
   //               however, it's necessary to make NodesFrom optimizations to work with SQL
   nextPath bool
}

type NodeUpdate struct {
   Hash   refs.ValueHash
   Val    quad.Value
   RefInc int
}

type QuadUpdate struct {
   Ind  int
   Quad refs.QuadHash
   Del  bool
}
```

### 9.3. 增加/删除Quad操作

1. 计算Quad的hash值（每个direction），分别打包成`IncNode`, `DecNode`, `QuadAdd`, `QuadDel`四种结构体，`NodeUpdate`的结构体记录的是某个属性的val和对应的hash，而QuadUpdate是一个Quad对应的四个属性的hash值的集合。（1QuadUpdate -> 4NodeUpdate{subject, predicate, )
2. 修改Nodes表（存在则修改引用计数），SQL如下：

```go
stmt, err = tx.Prepare(`INSERT INTO nodes(refs, hash, ` +
   strings.Join(nodeKey.Columns(), ", ") +
   `) VALUES (` + strings.Join(ph, ", ") +
   `) ON DUPLICATE KEY UPDATE refs = refs + ?;`)
```

3. 修改Quads表（覆盖写入Quad），SQL如下：

```go
insertQuad, err = tx.Prepare(`INSERT` + ignore + ` INTO quads(subject_hash, predicate_hash, object_hash, label_hash, ts) VALUES (?, ?, ?, ?, now());`)
```

4. 对Quads表进行删除Quad操作，SQL如下：

```go
if deleteQuad == nil {
   // 删除边（开启事务）
   deleteQuad, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=` + p[0] + ` and predicate_hash=` + p[1] + ` and object_hash=` + p[2] + ` and label_hash=` + p[3] + `;`)
   if err != nil {
      return err
   }
   deleteTriple, err = tx.Prepare(`DELETE FROM quads WHERE subject_hash=` + p[0] + ` and predicate_hash=` + p[1] + ` and object_hash=` + p[2] + ` and label_hash is null;`)
   if err != nil {
      return err
   }
}
```

5. 修改Nodes表的引用计数，SQL如下：

```go
updateNode, err := tx.Prepare(`UPDATE nodes SET refs = refs + ` + p[0] + ` WHERE hash = ` + p[1] + `;`)
```

6. 清理引用计数为0的点，进行真正的删除，如下：

```go
_, err = tx.Exec(`DELETE FROM nodes WHERE refs <= 0;`)
if err != nil {
   clog.Errorf("couldn't exec DELETE nodes statement: %v", err)
   return err
}
```



## 10. nosql

nosql包这边统一使用了hidalgo包的通用接口来统一适配，屏蔽掉下层接入的nosql数据库(elastic, mongo, ouch )的实现细节，详见：github.com/hidal-go/hidalgo/legacy/nosql

nosql.Database细节：

```go
// Database is a minimal interface for NoSQL database implementations.
type Database interface {
   // Insert creates a document with a given key in a given collection.
   // Key can be nil meaning that implementation should generate a unique key for the item.
   // It returns the key that was generated, or the same key that was passed to it.
   Insert(ctx context.Context, col string, key Key, d Document) (Key, error)
   // FindByKey finds a document by it's Key. It returns ErrNotFound if document not exists.
   FindByKey(ctx context.Context, col string, key Key) (Document, error)
   // Query starts construction of a new query for a specified collection.
   Query(col string) Query
   // Update starts construction of document update request for a specified document and collection.
   Update(col string, key Key) Update
   // Delete starts construction of document delete request.
   Delete(col string) Delete
   // 创建索引。索引通常能够极大的提高查询的效率，如果没有索引，nosql在读取数据时必须扫描集合中
   // 的每个文件并选取那些符合查询条件的记录。这种扫描全集合的查询效率是非常低的，特别在处理
   // EnsureIndex creates or updates indexes on the collection to match it's arguments.
   // It should create collection if it not exists. Primary index is guaranteed to be of StringExact type.
   EnsureIndex(ctx context.Context, col string, primary Index, secondary []Index) error
   // Close closes the database connection.
   Close() error
}
```

![10](/Users/bytedance/github/cayleygraph_analysis/img/10.png)

nosql的层级关系：

- **Databases：**databases保存文档（Documents）的集合（Collections）。
- **Collections：**nosql在collections中存储文档（Documents）。Collections类似于关系型数据库中的表(tables)。
- **Documents：**nosql的Documents是由field和value对的结构组成，类似以下结构：

```yaml
{
   field1: value1,
   field2: value2,
   field3: value3,
   ...
   fieldN: valueN
}
```



### 10.1. QuadStore数据结构

```go
type QuadStore struct {
   db    nosql.Database
   ids   *lru.Cache
   sizes *lru.Cache
   opt   Traits
}
```

### 10.2. 索引结构

| 索引名 | 索引结构                            | 类型   |
| ------ | ----------------------------------- | ------ |
| log    | id                                  | string |
| nodes  | hash                                | string |
| quads  | {subject, predicate, object, label} | string |

### 10.3. 增加/删除Quad操作

1. 将操作记录插入到日志索引(graph.Delta -> log's Document)；
2. 将每个graph.Delta的操作，拆分成对每个属性(subject, predicate, object, label)的引用计数进行累加或者累减；
3. 计算每个属性(subject, predicate, object, label)的hash值，更新nodes索引对应的每个属性的引用计数值；
4. 清理所有引用计数的为0的属性key-value；
5. 再逐条边进行added字段和deleted字段的计数更新（换句话来说，当added > deleted时，quad在图中存在；当added <= deleted时，quad在图中不存在）。

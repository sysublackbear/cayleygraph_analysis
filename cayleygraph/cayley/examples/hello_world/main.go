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
	// 查询语句?
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

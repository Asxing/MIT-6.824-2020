package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one master.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"../mr"
	"fmt"
	"os"
)
import "plugin"
import "log"

func main() {
	fmt.Printf("os.Args[1]:%s\n", os.Args[1])
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
	fmt.Printf("os.Args[1]:%s\n", os.Args[1])
	mapFun, reduceFun := loadPlugin(os.Args[1])

	mr.Worker(mapFun, reduceFun)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}

	xMapFun, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapFun := xMapFun.(func(string, string) []mr.KeyValue)

	xReduceFun, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reduceFun := xReduceFun.(func(string, []string) string)

	return mapFun, reduceFun
}

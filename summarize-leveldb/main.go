package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer-diagnostics"
	"github.com/sburnett/transformer/store"
)

func main() {
	go cube.Run("transformer_diagnostics_summarize")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s <leveldb path>:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}
	dbPath := flag.Arg(0)

	stor := store.NewLevelDbStore(dbPath, store.LevelDbReadOnly)
	pipeline := diagnostics.SummarizeStorePipeline(stor)

	transformer.RunPipeline(pipeline)
}

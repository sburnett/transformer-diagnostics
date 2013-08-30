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
	go cube.Run("transformer_diagnostics_print")

	keyFormat := flag.String("key_format", "", "Format keys using this format string")
	valueFormat := flag.String("value_format", "", "Format values using this format string")
	keyPrefix := flag.String("key_prefix", "", "Only print keys with this prefix")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s [options] <leveldb path>:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}
	dbPath := flag.Arg(0)

	stor := store.NewLevelDbStore(dbPath, store.LevelDbReadOnly)
	pipeline := diagnostics.RecordPrinterPipeline(stor, *keyFormat, *valueFormat, *keyPrefix)

	transformer.RunPipeline(pipeline)
}

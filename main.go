package main

import (
	"flag"
	"fmt"
	"path/filepath"

	"github.com/sburnett/cube"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer-diagnostics/diagnostics"
	"github.com/sburnett/transformer/store"
)

func pipelineSummarize(dbRoot string, workers int) []transformer.PipelineStage {
	flagset := flag.NewFlagSet("print", flag.ExitOnError)
	storePath := flagset.String("leveldb", "", "Print the contents of this LevelDB")
	flagset.Parse(flag.Args()[2:])
	if len(*storePath) == 0 {
		panic(fmt.Errorf("Invalid leveldb name. Must specify --leveldb."))
	}
	store := store.NewLevelDbStore(filepath.Join(dbRoot, *storePath))
	return diagnostics.SummarizeStorePipeline(store)
}

func pipelinePrint(dbRoot string, workers int) []transformer.PipelineStage {
	flagset := flag.NewFlagSet("print", flag.ExitOnError)
	storePath := flagset.String("leveldb", "", "Print the contents of this LevelDB")
	keyFormat := flagset.String("key_format", "", "Format keys using this format string")
	valueFormat := flagset.String("value_format", "", "Format values using this format string")
	flagset.Parse(flag.Args()[2:])
	if len(*storePath) == 0 {
		panic(fmt.Errorf("Invalid leveldb name. Must specify --leveldb."))
	}
	store := store.NewLevelDbStore(filepath.Join(dbRoot, *storePath))
	return diagnostics.RecordPrinterPipeline(store, *keyFormat, *valueFormat)
}

func main() {
	pipelineFuncs := map[string]transformer.PipelineFunc{
		"print":     pipelinePrint,
		"summarize": pipelineSummarize,
	}
	name, pipeline := transformer.ParsePipelineChoice(pipelineFuncs)

	go cube.Run(fmt.Sprintf("transformer_diagnostics_%s", name))

	transformer.RunPipeline(pipeline)
}

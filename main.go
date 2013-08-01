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

func getPipelineStages(dbRoot string, workers int) map[string]func() []transformer.PipelineStage {
	dbPath := func(filename string) string {
		return filepath.Join(dbRoot, filename)
	}
	pipelineFuncs := make(map[string]func() []transformer.PipelineStage)
	pipelineFuncs["print"] = func() []transformer.PipelineStage {
		flagset := flag.NewFlagSet("print", flag.ExitOnError)
		storePath := flagset.String("leveldb", "", "Print the contents of this LevelDB")
		keyFormat := flagset.String("key_format", "", "Format keys using this format string")
		valueFormat := flagset.String("value_format", "", "Format values using this format string")
		flagset.Parse(flag.Args()[2:])
		if len(*storePath) == 0 {
			panic(fmt.Errorf("Invalid leveldb name. Must specify --leveldb."))
		}
		store := store.NewLevelDbStore(dbPath(*storePath))
		return diagnostics.RecordPrinterPipeline(store, *keyFormat, *valueFormat)
	}
	return pipelineFuncs
}

func main() {
	name, pipeline := transformer.ParsePipelineChoice(getPipelineStages)

	go cube.Run(fmt.Sprintf("transformer_diagnostics_%s", name))

	transformer.RunPipeline(pipeline)
}

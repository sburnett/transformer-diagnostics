package diagnostics

import (
	"fmt"

	"github.com/dustin/go-humanize"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

func SummarizeStorePipeline(store store.Reader) []transformer.PipelineStage {
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "Summarize",
			Reader:      store,
			Transformer: transformer.TransformFunc(summarizeStore),
		},
	}
}

func summarizeStore(inputChan, outputChan chan *store.Record) {
	var count, keyBytes, valueBytes int64
	for record := range inputChan {
		count++
		keyBytes += int64(len(record.Key))
		valueBytes += int64(len(record.Value))
	}
	fmt.Println("Records:", humanize.Comma(count))
	fmt.Printf("Size: %s (%s for keys and %s for values)\n", humanize.Bytes(uint64(keyBytes+valueBytes)), humanize.Bytes(uint64(keyBytes)), humanize.Bytes(uint64(valueBytes)))
	fmt.Println("Average key size:", humanize.Bytes(uint64(keyBytes/count)))
	fmt.Println("Average value size:", humanize.Bytes(uint64(valueBytes/count)))
}

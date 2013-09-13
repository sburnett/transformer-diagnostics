package diagnostics

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/sburnett/lexicographic-tuples"
	"github.com/sburnett/transformer"
	"github.com/sburnett/transformer/store"
)

type RawFormat int

const (
	NoRaw RawFormat = iota
	RawBytes
	RawString
)

func RecordPrinterPipeline(stor store.Seeker, keyFormat, valueFormat, keyPrefix string) []transformer.PipelineStage {
	printer, err := newRecordPrinter(keyFormat, valueFormat, keyPrefix)
	if err != nil {
		panic(err)
	}
	keyPrefixStore := makeKeyPrefixStore(printer.keyPrefix)
	return []transformer.PipelineStage{
		transformer.PipelineStage{
			Name:        "Print",
			Reader:      store.NewPrefixIncludingReader(stor, keyPrefixStore),
			Transformer: printer,
		},
	}
}

func makeKeyPrefixStore(keyPrefix []byte) *store.SliceStore {
	prefixStore := store.SliceStore{}
	prefixStore.BeginWriting()
	prefixStore.WriteRecord(&store.Record{Key: keyPrefix})
	prefixStore.EndWriting()
	return &prefixStore
}

type recordPrinter struct {
	keys, values               []interface{}
	keysIgnored, valuesIgnored []bool
	keysRaw, valuesRaw         RawFormat
	keyPrefix                  []byte
}

func parsePrintFormatString(format string) (values []interface{}, ignored []bool, raw RawFormat, err error) {
	for _, format := range strings.Split(format, ",") {
		if format[0] == '-' {
			ignored = append(ignored, true)
			format = format[1:]
		} else {
			ignored = append(ignored, false)
		}
		switch format {
		case "[]byte":
			values = append(values, new([]byte))
		case "[][]byte":
			values = append(values, new([][]byte))
		case "string":
			values = append(values, new(string))
		case "[]string":
			values = append(values, new([]string))
		case "bool":
			values = append(values, new(bool))
		case "int8":
			values = append(values, new(int8))
		case "[]int8":
			values = append(values, new([]int8))
		case "uint8":
			values = append(values, new(uint8))
		case "[]uint8":
			values = append(values, new([]uint8))
		case "int16":
			values = append(values, new(int16))
		case "[]int16":
			values = append(values, new([]int16))
		case "uint32":
			values = append(values, new(uint32))
		case "[]uint32":
			values = append(values, new([]uint32))
		case "int32":
			values = append(values, new(int32))
		case "[]int32":
			values = append(values, new([]int32))
		case "uint64":
			values = append(values, new(uint64))
		case "[]uint64":
			values = append(values, new([]uint64))
		case "int64":
			values = append(values, new(int64))
		case "[]int64":
			values = append(values, new([]int64))
		case "raw":
			raw = RawBytes
		case "raw_string":
			raw = RawString
		default:
			err = fmt.Errorf("Invalid format specifier: %v", format)
		}
	}
	return
}

func parseKeyPrefix(keys []interface{}, keyPrefixString string) ([]byte, error) {
	if keyPrefixString == "" {
		return nil, nil
	}
	valueStrings := strings.Split(keyPrefixString, ",")
	numValues := len(valueStrings)
	formatString := strings.TrimSuffix(strings.Repeat("%v,", numValues), ",")
	keyPrefix := keys[:numValues]
	if _, err := fmt.Sscanf(keyPrefixString, formatString, keyPrefix...); err != nil {
		return nil, err
	}
	var keyPrefixDereferenced []interface{}
	for _, k := range keyPrefix {
		dereferencedValue := reflect.ValueOf(k).Elem().Interface()
		keyPrefixDereferenced = append(keyPrefixDereferenced, dereferencedValue)
	}
	return lex.EncodeOrDie(keyPrefixDereferenced...), nil
}

func newRecordPrinter(keyFormat, valueFormat, keyPrefix string) (*recordPrinter, error) {
	var printer recordPrinter
	if len(keyFormat) > 0 {
		if keys, ignored, raw, err := parsePrintFormatString(keyFormat); err != nil {
			return nil, err
		} else {
			printer.keys = keys
			printer.keysIgnored = ignored
			printer.keysRaw = raw
		}
		prefix, err := parseKeyPrefix(printer.keys, keyPrefix)
		if err != nil {
			return nil, err
		}
		printer.keyPrefix = prefix
	}
	if len(valueFormat) > 0 {
		if values, ignored, raw, err := parsePrintFormatString(valueFormat); err != nil {
			return nil, err
		} else {
			printer.values = values
			printer.valuesIgnored = ignored
			printer.valuesRaw = raw
		}
	}
	return &printer, nil
}

func (printer *recordPrinter) Do(inputChan, outputChan chan *store.Record) {
	for record := range inputChan {
		if printer.keys != nil {
			remainder := lex.DecodeOrDie(record.Key, printer.keys...)
			printed := 0
			for idx, k := range printer.keys {
				if printer.keysIgnored[idx] {
					continue
				}
				v := reflect.ValueOf(k)
				if printed > 0 {
					fmt.Print(",")
				}
				fmt.Print(v.Elem().Interface())
				printed++
			}
			if len(remainder) > 0 {
				fmt.Print(",")
				switch printer.keysRaw {
				case RawBytes:
					fmt.Print(remainder)
				case RawString:
					fmt.Print(string(remainder))
				}
			}
			fmt.Printf(": ")
		} else {
			switch printer.keysRaw {
			case RawBytes:
				fmt.Printf("%v: ", record.Key)
			case RawString:
				fmt.Printf("%v: ", string(record.Key))
			}
		}
		if printer.values != nil {
			remainder := lex.DecodeOrDie(record.Value, printer.values...)
			printed := 0
			for idx, value := range printer.values {
				if printer.valuesIgnored[idx] {
					continue
				}
				v := reflect.ValueOf(value)
				if printed > 0 {
					fmt.Print(",")
				}
				fmt.Print(v.Elem().Interface())
				printed++
			}
			if len(remainder) > 0 {
				fmt.Print(",")
				switch printer.valuesRaw {
				case RawBytes:
					fmt.Print(remainder)
				case RawString:
					fmt.Print(string(remainder))
				}
			}
		} else {
			switch printer.valuesRaw {
			case RawBytes:
				fmt.Print(record.Value)
			case RawString:
				fmt.Print(string(record.Value))
			}
		}
		fmt.Printf("\n")
	}
}

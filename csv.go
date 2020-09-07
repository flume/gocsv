// Copyright 2014 Jonathan Picques. All rights reserved.
// Use of this source code is governed by a MIT license
// The license can be found in the LICENSE file.

// The GoCSV package aims to provide easy CSV serialization and deserialization to the golang programming language

package gocsv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
)

// Normalizer is a function that takes and returns a string. It is applied to
// struct and header field values before they are compared. It can be used to alter
// names for comparison. For instance, you could allow case insensitive matching
// or convert '-' to '_'.
type Normalizer func(string) string

// DefaultNameNormalizer is a nop Normalizer.
func DefaultNameNormalizer() Normalizer { return func(s string) string { return s } }

type ErrorHandler func(*csv.ParseError) bool

type CSV struct {
	failIfUnmatchedStructTags                       bool
	failIfDoubleHeaderNames                         bool
	shouldAlignDuplicateHeadersWithStructFieldOrder bool
	tagName                                         string
	tagSeparator                                    string
	comma                                           rune
	normalizeName                                   Normalizer
	selfCSVWriter                                   func(out io.Writer, tagSeparator string) *SafeCSVWriter
	selfCSVReader                                   func(in io.Reader) CSVReader
}

func WithShouldAlign() func(*CSV) {
	return func(c *CSV) {
		c.shouldAlignDuplicateHeadersWithStructFieldOrder = true
	}
}

func WithFailDoubleHeader() func(*CSV) {
	return func(c *CSV) {
		c.failIfDoubleHeaderNames = true
	}
}

// WithTagSeparator sets the default reflection tag separator to use
func WithTagSeparator(tag string) func(*CSV) {
	return func(c *CSV) {
		c.tagSeparator = tag
	}
}

// WithTagName sets the default reflection tag name to use
func WithTagName(name string) func(*CSV) {
	return func(c *CSV) {
		c.tagName = name
	}
}

// WithComma sets the comma to use inside the file
func WithComma(comma string) func(*CSV) {
	return func(c *CSV) {
		c.comma = commaRuneFromString(comma)
		c.SetCSVWriter(c.selfCSVWriter, comma)
		c.selfCSVReader = func(in io.Reader) CSVReader {
			iRead := csv.NewReader(in)
			iRead.Comma = c.comma
			return iRead
		}
	}
}

func commaRuneFromString(in string) rune {
	if runes := []rune(strings.TrimSpace(in)); len(runes) > 0 {
		return runes[0]
	}
	return DefaultComma
}

const DefaultTagSeparator = ","
const DefaultTagName = "csv"
const DefaultComma = ','

func New(opts ...func(*CSV)) *CSV {
	c := &CSV{
		tagName:       DefaultTagName,
		tagSeparator:  DefaultTagSeparator,
		comma:         DefaultComma,
		normalizeName: DefaultNameNormalizer(),
		selfCSVWriter: DefaultCSVWriter,
		selfCSVReader: DefaultCSVReader,
	}

	for _, o := range opts {
		o(c)
	}
	return c
}

// SetHeaderNormalizer sets the normalizer used to normalize struct and header field names.
func (c *CSV) SetHeaderNormalizer(f Normalizer) {
	c.normalizeName = f
	// Need to clear the cache hen the header normalizer changes.
	structInfoCache = sync.Map{}
}

// DefaultCSVWriter is the default SafeCSVWriter used to format CSV (cf. csv.NewWriter)
func DefaultCSVWriter(out io.Writer, comma string) *SafeCSVWriter {
	writer := NewSafeCSVWriter(csv.NewWriter(out))

	// Only one rune can be defined as a CSV separator. Trim comma and use first rune.
	writer.Comma = commaRuneFromString(comma)
	return writer
}

// SetCSVWriter sets the SafeCSVWriter used to format CSV.
func (c *CSV) SetCSVWriter(csvWriter func(io.Writer, string) *SafeCSVWriter, comma string) {
	c.selfCSVWriter = csvWriter
	c.comma = commaRuneFromString(comma)
}

func (c CSV) getCSVWriter(out io.Writer) *SafeCSVWriter {
	return c.selfCSVWriter(out, string(c.comma))
}

// DefaultCSVReader is the default CSV reader used to parse CSV (cf. csv.NewReader)
func DefaultCSVReader(in io.Reader) CSVReader {
	r := csv.NewReader(in)
	r.Comma = DefaultComma
	return r
}

// LazyCSVReader returns a lazy CSV reader, with LazyQuotes and TrimLeadingSpace.
func LazyCSVReader(in io.Reader) CSVReader {
	csvReader := csv.NewReader(in)
	csvReader.LazyQuotes = true
	csvReader.TrimLeadingSpace = true
	return csvReader
}

// SetCSVReader sets the CSV reader used to parse CSV.
func (c *CSV) SetCSVReader(csvReader func(io.Reader) CSVReader) {
	c.selfCSVReader = csvReader
}

func (c CSV) getCSVReader(in io.Reader) CSVReader {
	return c.selfCSVReader(in)
}

// --------------------------------------------------------------------------
// Marshal functions

// MarshalFile saves the interface as CSV in the file.
func (c CSV) MarshalFile(in interface{}, file *os.File) (err error) {
	return c.Marshal(in, file)
}

// MarshalString returns the CSV string from the interface.
func (c CSV) MarshalString(in interface{}) (out string, err error) {
	bufferString := bytes.NewBufferString(out)
	if err := c.Marshal(in, bufferString); err != nil {
		return "", err
	}
	return bufferString.String(), nil
}

// MarshalBytes returns the CSV bytes from the interface.
func (c CSV) MarshalBytes(in interface{}) (out []byte, err error) {
	bufferString := bytes.NewBuffer(out)
	if err := c.Marshal(in, bufferString); err != nil {
		return nil, err
	}
	return bufferString.Bytes(), nil
}

// Marshal returns the CSV in writer from the interface.
func (c CSV) Marshal(in interface{}, out io.Writer) (err error) {
	writer := c.getCSVWriter(out)
	return c.writeTo(writer, in, false)
}

// MarshalWithoutHeaders returns the CSV in writer from the interface.
func (c CSV) MarshalWithoutHeaders(in interface{}, out io.Writer) (err error) {
	writer := c.getCSVWriter(out)
	return c.writeTo(writer, in, true)
}

// MarshalChan returns the CSV read from the channel.
func (c CSV) MarshalChan(o <-chan interface{}, out *SafeCSVWriter) error {
	return c.writeFromChan(out, o)
}

// MarshalCSV returns the CSV in writer from the interface.
func (c CSV) MarshalCSV(in interface{}, out *SafeCSVWriter) (err error) {
	return c.writeTo(out, in, false)
}

// MarshalCSVWithoutHeaders returns the CSV in writer from the interface.
func (c CSV) MarshalCSVWithoutHeaders(in interface{}, out *SafeCSVWriter) (err error) {
	return c.writeTo(out, in, true)
}

// --------------------------------------------------------------------------
// Unmarshal functions

// UnmarshalFile parses the CSV from the file in the interface.
func (c CSV) UnmarshalFile(in *os.File, out interface{}) error {
	return c.Unmarshal(in, out)
}

// UnmarshalFile parses the CSV from the file in the interface.
func (c CSV) UnmarshalFileWithErrorHandler(in *os.File, errHandler ErrorHandler, out interface{}) error {
	return c.UnmarshalWithErrorHandler(in, errHandler, out)
}

// UnmarshalString parses the CSV from the string in the interface.
func (c CSV) UnmarshalString(in string, out interface{}) error {
	return c.Unmarshal(strings.NewReader(in), out)
}

// UnmarshalBytes parses the CSV from the bytes in the interface.
func (c CSV) UnmarshalBytes(in []byte, out interface{}) error {
	return c.Unmarshal(bytes.NewReader(in), out)
}

// Unmarshal parses the CSV from the reader in the interface.
func (c CSV) Unmarshal(in io.Reader, out interface{}) error {
	return c.readTo(c.newSimpleDecoderFromReader(in), out)
}

// Unmarshal parses the CSV from the reader in the interface.
func (c CSV) UnmarshalWithErrorHandler(in io.Reader, errHandle ErrorHandler, out interface{}) error {
	return c.readToWithErrorHandler(c.newSimpleDecoderFromReader(in), errHandle, out)
}

// UnmarshalWithoutHeaders parses the CSV from the reader in the interface.
func (c CSV) UnmarshalWithoutHeaders(in io.Reader, out interface{}) error {
	return c.readToWithoutHeaders(c.newSimpleDecoderFromReader(in), out)
}

// UnmarshalCSVWithoutHeaders parses a headerless CSV with passed in CSV reader
func (c CSV) UnmarshalCSVWithoutHeaders(in CSVReader, out interface{}) error {
	return c.readToWithoutHeaders(csvDecoder{in}, out)
}

// UnmarshalDecoder parses the CSV from the decoder in the interface
func (c CSV) UnmarshalDecoder(in Decoder, out interface{}) error {
	return c.readTo(in, out)
}

// UnmarshalCSV parses the CSV from the reader in the interface.
func (c CSV) UnmarshalCSV(in CSVReader, out interface{}) error {
	return c.readTo(csvDecoder{in}, out)
}

// UnmarshalToChan parses the CSV from the reader and send each value in the chan c.
// The channel must have a concrete type.
func (c CSV) UnmarshalToChan(in io.Reader, o interface{}) error {
	if o == nil {
		return fmt.Errorf("goscv: channel is %v", o)
	}
	return c.readEach(c.newSimpleDecoderFromReader(in), o)
}

// UnmarshalToChanWithoutHeaders parses the CSV from the reader and send each value in the chan c.
// The channel must have a concrete type.
func (c CSV) UnmarshalToChanWithoutHeaders(in io.Reader, o interface{}) error {
	if o == nil {
		return fmt.Errorf("goscv: channel is %v", o)
	}
	return c.readEachWithoutHeaders(c.newSimpleDecoderFromReader(in), o)
}

// UnmarshalDecoderToChan parses the CSV from the decoder and send each value in the chan c.
// The channel must have a concrete type.
func (c CSV) UnmarshalDecoderToChan(in SimpleDecoder, o interface{}) error {
	if o == nil {
		return fmt.Errorf("goscv: channel is %v", o)
	}
	return c.readEach(in, o)
}

// UnmarshalStringToChan parses the CSV from the string and send each value in the chan c.
// The channel must have a concrete type.
func (c CSV) UnmarshalStringToChan(in string, o interface{}) error {
	return c.UnmarshalToChan(strings.NewReader(in), o)
}

// UnmarshalBytesToChan parses the CSV from the bytes and send each value in the chan c.
// The channel must have a concrete type.
func (c CSV) UnmarshalBytesToChan(in []byte, o interface{}) error {
	return c.UnmarshalToChan(bytes.NewReader(in), o)
}

// UnmarshalToCallback parses the CSV from the reader and send each value to the given func f.
// The func must look like func(Struct).
func (c CSV) UnmarshalToCallback(in io.Reader, f interface{}) error {
	valueFunc := reflect.ValueOf(f)
	t := reflect.TypeOf(f)
	if t.NumIn() != 1 {
		return fmt.Errorf("the given function must have exactly one parameter")
	}
	cerr := make(chan error)
	o := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, t.In(0)), 0)
	go func() {
		cerr <- c.UnmarshalToChan(in, o.Interface())
	}()
	for {
		select {
		case err := <-cerr:
			return err
		default:
		}
		v, notClosed := o.Recv()
		if !notClosed || v.Interface() == nil {
			break
		}
		valueFunc.Call([]reflect.Value{v})
	}
	return nil
}

// UnmarshalDecoderToCallback parses the CSV from the decoder and send each value to the given func f.
// The func must look like func(Struct).
func (c CSV) UnmarshalDecoderToCallback(in SimpleDecoder, f interface{}) error {
	valueFunc := reflect.ValueOf(f)
	t := reflect.TypeOf(f)
	if t.NumIn() != 1 {
		return fmt.Errorf("the given function must have exactly one parameter")
	}
	cerr := make(chan error)
	o := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, t.In(0)), 0)
	go func() {
		cerr <- c.UnmarshalDecoderToChan(in, o.Interface())
	}()
	for {
		select {
		case err := <-cerr:
			return err
		default:
		}
		v, notClosed := o.Recv()
		if !notClosed || v.Interface() == nil {
			break
		}
		valueFunc.Call([]reflect.Value{v})
	}
	return nil
}

// UnmarshalBytesToCallback parses the CSV from the bytes and send each value to the given func f.
// The func must look like func(Struct).
func (c CSV) UnmarshalBytesToCallback(in []byte, f interface{}) error {
	return c.UnmarshalToCallback(bytes.NewReader(in), f)
}

// UnmarshalStringToCallback parses the CSV from the string and send each value to the given func f.
// The func must look like func(Struct).
func (c CSV) UnmarshalStringToCallback(in string, o interface{}) (err error) {
	return c.UnmarshalToCallback(strings.NewReader(in), o)
}

// UnmarshalToCallbackWithError parses the CSV from the reader and
// send each value to the given func f.
//
// If func returns error, it will stop processing, drain the
// parser and propagate the error to caller.
//
// The func must look like func(Struct) error.
func (c CSV) UnmarshalToCallbackWithError(in io.Reader, f interface{}) error {
	valueFunc := reflect.ValueOf(f)
	t := reflect.TypeOf(f)
	if t.NumIn() != 1 {
		return fmt.Errorf("the given function must have exactly one parameter")
	}
	if t.NumOut() != 1 {
		return fmt.Errorf("the given function must have exactly one return value")
	}
	if !isErrorType(t.Out(0)) {
		return fmt.Errorf("the given function must only return error.")
	}

	cerr := make(chan error)
	o := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, t.In(0)), 0)
	go func() {
		cerr <- c.UnmarshalToChan(in, o.Interface())
	}()

	var fErr error
	for {
		select {
		case err := <-cerr:
			if err != nil {
				return err
			}
			return fErr
		default:
		}
		v, notClosed := o.Recv()
		if !notClosed || v.Interface() == nil {
			break
		}

		// callback f has already returned an error, stop processing but keep draining the chan c
		if fErr != nil {
			continue
		}

		results := valueFunc.Call([]reflect.Value{v})

		// If the callback f returns an error, stores it and returns it in future.
		errValue := results[0].Interface()
		if errValue != nil {
			fErr = errValue.(error)
		}
	}
	return fErr
}

// UnmarshalBytesToCallbackWithError parses the CSV from the bytes and
// send each value to the given func f.
//
// If func returns error, it will stop processing, drain the
// parser and propagate the error to caller.
//
// The func must look like func(Struct) error.
func (c CSV) UnmarshalBytesToCallbackWithError(in []byte, f interface{}) error {
	return c.UnmarshalToCallbackWithError(bytes.NewReader(in), f)
}

// UnmarshalStringToCallbackWithError parses the CSV from the string and
// send each value to the given func f.
//
// If func returns error, it will stop processing, drain the
// parser and propagate the error to caller.
//
// The func must look like func(Struct) error.
func (c CSV) UnmarshalStringToCallbackWithError(in string, o interface{}) (err error) {
	return c.UnmarshalToCallbackWithError(strings.NewReader(in), o)
}

// CSVToMap creates a simple map from a CSV of 2 columns.
func (c CSV) CSVToMap(in io.Reader) (map[string]string, error) {
	decoder := c.newSimpleDecoderFromReader(in)
	header, err := decoder.getCSVRow()
	if err != nil {
		return nil, err
	}
	if len(header) != 2 {
		return nil, fmt.Errorf("maps can only be created for csv of two columns")
	}
	m := make(map[string]string)
	for {
		line, err := decoder.getCSVRow()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		m[line[0]] = line[1]
	}
	return m, nil
}

// CSVToMaps takes a reader and returns an array of dictionaries, using the header row as the keys
func CSVToMaps(reader io.Reader) ([]map[string]string, error) {
	r := csv.NewReader(reader)
	rows := []map[string]string{}
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return rows, nil
}

// CSVToChanMaps parses the CSV from the reader and send a dictionary in the chan c, using the header row as the keys.
func CSVToChanMaps(reader io.Reader, c chan<- map[string]string) error {
	r := csv.NewReader(reader)
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header == nil {
			header = record
		} else {
			dict := map[string]string{}
			for i := range header {
				dict[header[i]] = record[i]
			}
			c <- dict
		}
	}
	return nil
}

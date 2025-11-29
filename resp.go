package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP prefix bytes for different data types
const (
	RespPrefixString  = '+' // simple string
	RespPrefixError   = '-'
	RespPrefixInteger = ':'
	RespPrefixBulk    = '$' // bulk string
	RespPrefixArray   = '*'
)

// Data type for Value
const (
	RespTypeString = "string"
	RespTypeBulk   = "bulk"
	RespTypeArray  = "array"
	RespTypeError  = "error"
	RespTypeNull   = "null"
)

// Represents a deserialized RESP value
type Value struct {
	typ   string
	str   string
	num   int
	bulk  string
	array []Value
}

// ========== READER ==========
// ============================
type Resp struct {
	reader *bufio.Reader
}

func NewResp(reader io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(reader)}
}

// Read from reader and deserialize RESP value
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case RespPrefixArray:
		return r.readArray()
	case RespPrefixBulk:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

// Read bytes until CRLF. Return the line without CRLF
func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}

		n++
		line = append(line, b)

		// Detect '\r\n'
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	line = line[:len(line)-2] // Remove '\r\n'
	return line, n, nil
}

// Parse an integer
func (r *Resp) readInteger() (x int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}

	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, 0, err
	}

	return int(i64), n, nil
}

// Parse an array: "*<len>\r\n<value...>"
func (r *Resp) readArray() (Value, error) {
	v := Value{}
	v.typ = "array"

	// Skip the first byte ($) since we already read it in Read

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	v.array = make([]Value, length)

	// Parse each element with Read
	for i := range length {
		val, err := r.Read()
		if err != nil {
			return v, err
		}

		v.array[i] = val
	}

	return v, nil
}

// Parse a bulk string: "$<len>\r\n<value>\r\n"
func (r *Resp) readBulk() (Value, error) {
	v := Value{}
	v.typ = "bulk"

	// Skip the first byte ($) since we already read it in Read

	length, _, err := r.readInteger()
	if err != nil {
		return v, err
	}

	buffer := make([]byte, length)
	r.reader.Read(buffer)
	v.bulk = string(buffer)

	// Consume the trailing CRLF
	r.readLine()

	return v, nil
}

// ========== WRITER ==========
// ============================
type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

// Serialize RESP value and write to writer
func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()
	_, err := w.writer.Write(bytes)
	return err
}

// Serialize RESP value
func (v Value) Marshal() []byte {
	switch v.typ {
	case RespTypeArray:
		return v.marshalArray()
	case RespTypeBulk:
		return v.marshalBulk()
	case RespTypeString:
		return v.marshalString()
	case RespTypeNull:
		return v.marshalNull()
	case RespTypeError:
		return v.marshalError()
	default:
		return []byte{}
	}
}

// Serialize simple string: "+<value>\r\n"
func (v Value) marshalString() []byte {
	var bytes []byte
	bytes = append(bytes, RespPrefixString)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// Serialize bulk string: "$<len>\r\n<value>\r\n"
func (v Value) marshalBulk() []byte {
	var bytes []byte
	bytes = append(bytes, RespPrefixBulk)
	bytes = append(bytes, strconv.Itoa(len(v.bulk))...)
	bytes = append(bytes, '\r', '\n')
	bytes = append(bytes, v.bulk...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// Serialize array: "*<len>\r\n<value...>"
func (v Value) marshalArray() []byte {
	length := len(v.array)
	var bytes []byte
	bytes = append(bytes, RespPrefixArray)
	bytes = append(bytes, strconv.Itoa(length)...)
	bytes = append(bytes, '\r', '\n')

	for i := range length {
		bytes = append(bytes, v.array[i].Marshal()...)
	}

	return bytes
}

// Serialize error: "-<error>\r\n"
func (v Value) marshalError() []byte {
	var bytes []byte
	bytes = append(bytes, RespPrefixError)
	bytes = append(bytes, v.str...)
	bytes = append(bytes, '\r', '\n')
	return bytes
}

// Serialize null (data not found)
func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

// RESP prefix bytes for different types
const (
	STRING  = '+' // simple string
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$' // bulk string
	ARRAY   = '*'
)

// Represents a deserialized RESP value
type Value struct {
	typ   string  // data type
	str   string  // simple string value
	num   int     // integer value
	bulk  string  // bulk string value
	array []Value // array values
}

// Wraps a buffered reader for parsing RESP
type Resp struct {
	reader *bufio.Reader
}

// Creates a RESP parser from any io.Reader
func NewResp(reader io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(reader)}
}

// Reads bytes until CRLF, returns the line without CRLF
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

// Parses an integer
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

// Reads 1 RESP value, dispatches based on type prefix
func (r *Resp) Read() (Value, error) {
	_type, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch _type {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(_type))
		return Value{}, nil
	}
}

// Parses an array: "*<len>\r\n<value...>""
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

// Parses a bulk string: "$<len>\r\n<value>\r\n"
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

// Package resp implements the Redis Serialization Protocol (RESP2).
//
// Clients send every command as an array of bulk strings. SET key value is:
//
//	*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
//
// Every value is prefixed with its length, so the reader always knows exactly
// how many bytes come next. It never scans a value for a delimiter, which is
// what makes values binary-safe: a value may itself contain "\r\n".
package resp

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
)

const (
	maxArgs     = 1024 * 1024
	maxBulkSize = 512 * 1024 * 1024 // the same limit real Redis uses
)

// ProtocolError means the client sent bytes that are not valid RESP. There is
// no way to find the start of the next command after one, so the server
// replies with an error and closes the connection.
type ProtocolError string

func (e ProtocolError) Error() string { return "Protocol error: " + string(e) }

type Reader struct {
	br *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, 64*1024)}
}

// Buffered returns how many bytes have been received but not parsed yet. If
// it is non-zero the client has pipelined more commands, so the server can
// hold its replies back and send them all in one write.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadCommand reads one command. It returns (nil, nil) for an empty command,
// which callers skip. A stream that ends in the middle of a command returns
// io.ErrUnexpectedEOF; one that ends cleanly between commands returns io.EOF.
func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != '*' {
		return parseInline(line), nil
	}

	n, err := strconv.Atoi(string(line[1:]))
	if err != nil || n > maxArgs {
		return nil, ProtocolError("invalid multibulk length")
	}
	if n <= 0 {
		return nil, nil
	}
	args := make([][]byte, n)
	for i := range args {
		if args[i], err = r.readBulk(); err != nil {
			return nil, unexpected(err)
		}
	}
	return args, nil
}

func (r *Reader) readBulk() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '$' {
		return nil, ProtocolError("expected '$'")
	}
	size, err := strconv.Atoi(string(line[1:]))
	if err != nil || size < 0 || size > maxBulkSize {
		return nil, ProtocolError("invalid bulk length")
	}
	// Read the value and its trailing \r\n in one go.
	buf := make([]byte, size+2)
	if _, err := io.ReadFull(r.br, buf); err != nil {
		return nil, err
	}
	if buf[size] != '\r' || buf[size+1] != '\n' {
		return nil, ProtocolError("bulk string not terminated by CRLF")
	}
	return buf[:size:size], nil
}

// readLine returns the next line without its line ending. The slice points
// into the bufio buffer, so it is only valid until the next read.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		return nil, ProtocolError("line too long")
	}
	if err != nil {
		if len(line) > 0 {
			return nil, unexpected(err)
		}
		return nil, err
	}
	line = line[:len(line)-1]
	return bytes.TrimSuffix(line, []byte{'\r'}), nil
}

// parseInline handles the "inline" form, plain space-separated words, which
// is what you type when talking to the server with nc or telnet.
func parseInline(line []byte) [][]byte {
	fields := bytes.Fields(line)
	args := make([][]byte, len(fields))
	for i, f := range fields {
		args[i] = append([]byte(nil), f...) // line is reused by the next read
	}
	return args
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

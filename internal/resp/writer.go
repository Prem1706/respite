package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Writer encodes replies into a buffer. Nothing reaches the socket until
// Flush, which lets the server answer a batch of pipelined commands with a
// single write system call.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 64*1024)}
}

func (w *Writer) SimpleString(s string) {
	w.bw.WriteByte('+')
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Error writes an error reply. By convention msg starts with an error code
// such as "ERR". Line breaks would end the reply early, so they are replaced.
func (w *Writer) Error(msg string) {
	w.bw.WriteByte('-')
	w.bw.WriteString(strings.NewReplacer("\r", " ", "\n", " ").Replace(msg))
	w.bw.WriteString("\r\n")
}

func (w *Writer) Integer(n int64) { w.header(':', n) }

func (w *Writer) Bulk(b []byte) {
	w.header('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

func (w *Writer) BulkString(s string) {
	w.header('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// Null writes the null bulk string, which is how GET reports a missing key.
func (w *Writer) Null() { w.bw.WriteString("$-1\r\n") }

// Array writes an array header. The caller then writes n elements.
func (w *Writer) Array(n int) { w.header('*', int64(n)) }

// Raw writes bytes that are already RESP-encoded.
func (w *Writer) Raw(b []byte) { w.bw.Write(b) }

// Flush sends everything buffered so far. Write errors are sticky inside
// bufio.Writer, so this is the one place they need to be checked.
func (w *Writer) Flush() error { return w.bw.Flush() }

func (w *Writer) header(prefix byte, n int64) {
	w.scratch = append(w.scratch[:0], prefix)
	w.scratch = strconv.AppendInt(w.scratch, n, 10)
	w.scratch = append(w.scratch, '\r', '\n')
	w.bw.Write(w.scratch)
}

// AppendCommand appends args to dst as a RESP array of bulk strings, the same
// format clients send. The append-only file stores commands this way.
func AppendCommand(dst []byte, args [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

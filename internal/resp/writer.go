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
//
// Replies are encoded in RESP2 unless the client switched to RESP3 with
// HELLO 3. RESP3 adds types RESP2 lacks: a real null, maps, doubles and
// "push" messages that aren't replies to a command (used for pub/sub). The
// methods for those types fall back to their RESP2 encoding, so command
// handlers don't need to care which version a client speaks.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte
	Proto   int // 2 or 3; zero means 2
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

// Null writes a null: RESP3's "_", or RESP2's null bulk string.
func (w *Writer) Null() {
	if w.Proto == 3 {
		w.bw.WriteString("_\r\n")
	} else {
		w.bw.WriteString("$-1\r\n")
	}
}

// Map writes a map header for n key-value pairs. The caller then writes 2n
// elements. RESP2 has no maps, so there it is a flat array.
func (w *Writer) Map(n int) {
	if w.Proto == 3 {
		w.header('%', int64(n))
	} else {
		w.header('*', int64(2*n))
	}
}

// Double writes a floating-point number, already formatted by the caller.
// RESP2 has no doubles, so there it is a bulk string.
func (w *Writer) Double(formatted string) {
	if w.Proto == 3 {
		w.bw.WriteByte(',')
		w.bw.WriteString(formatted)
		w.bw.WriteString("\r\n")
	} else {
		w.BulkString(formatted)
	}
}

// Push writes the header of an out-of-band message, such as a pub/sub
// message. In RESP2 it is an ordinary array.
func (w *Writer) Push(n int) {
	if w.Proto == 3 {
		w.header('>', int64(n))
	} else {
		w.header('*', int64(n))
	}
}

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

package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadArray(t *testing.T) {
	r := NewReader(strings.NewReader("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	assertArgs(t, args, "SET", "key", "value")
}

func TestReadIsBinarySafe(t *testing.T) {
	// The value contains \r\n. Because of the length prefix it is still read as data.
	r := NewReader(strings.NewReader("*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n"))
	args, err := r.ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	assertArgs(t, args, "ECHO", "a\r\nb")
}

func TestReadInline(t *testing.T) {
	r := NewReader(strings.NewReader("SET  greeting hello\r\n\r\nPING\n"))
	args, _ := r.ReadCommand()
	assertArgs(t, args, "SET", "greeting", "hello")
	args, _ = r.ReadCommand()
	if args != nil {
		t.Fatalf("blank line: got %q, want nil", args)
	}
	args, _ = r.ReadCommand()
	assertArgs(t, args, "PING")
}

func TestReadPipelined(t *testing.T) {
	r := NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"))
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if r.Buffered() == 0 {
		t.Fatal("second command should already be buffered")
	}
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("got %v, want io.EOF", err)
	}
}

func TestReadTruncated(t *testing.T) {
	for _, in := range []string{"*2\r\n$3\r\nGET\r\n", "*2\r\n$3\r\nGET\r\n$3\r\nke", "*2\r"} {
		_, err := NewReader(strings.NewReader(in)).ReadCommand()
		if err != io.ErrUnexpectedEOF {
			t.Errorf("%q: got %v, want io.ErrUnexpectedEOF", in, err)
		}
	}
}

func TestReadProtocolErrors(t *testing.T) {
	for _, in := range []string{"*x\r\n", "*1\r\n+PING\r\n", "*1\r\n$-5\r\n", "*1\r\n$4\r\nPINGxx"} {
		_, err := NewReader(strings.NewReader(in)).ReadCommand()
		var pe ProtocolError
		if !errors.As(err, &pe) {
			t.Errorf("%q: got %v, want a ProtocolError", in, err)
		}
	}
}

func TestAppendCommandRoundTrip(t *testing.T) {
	in := [][]byte{[]byte("SET"), []byte("k"), []byte("line1\r\nline2"), {}}
	args, err := NewReader(bytes.NewReader(AppendCommand(nil, in))).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	assertArgs(t, args, "SET", "k", "line1\r\nline2", "")
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.SimpleString("OK")
	w.Error("ERR bad\r\nthing")
	w.Integer(-42)
	w.Array(2)
	w.BulkString("hi")
	w.Null()
	w.Flush()
	want := "+OK\r\n-ERR bad  thing\r\n:-42\r\n*2\r\n$2\r\nhi\r\n$-1\r\n"
	if buf.String() != want {
		t.Fatalf("got %q\nwant %q", buf.String(), want)
	}
}

func assertArgs(t *testing.T, got [][]byte, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range want {
		if string(got[i]) != want[i] {
			t.Fatalf("arg %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestOffset(t *testing.T) {
	cmd1 := "*1\r\n$4\r\nPING\r\n"
	cmd2 := "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	r := NewReader(strings.NewReader(cmd1 + cmd2))
	r.ReadCommand()
	if r.Offset() != int64(len(cmd1)) {
		t.Fatalf("offset after one command = %d, want %d", r.Offset(), len(cmd1))
	}
	r.ReadCommand()
	if r.Offset() != int64(len(cmd1+cmd2)) {
		t.Fatalf("offset after two commands = %d", r.Offset())
	}
}

func TestReadBulk(t *testing.T) {
	r := NewReader(strings.NewReader("$5\r\nhello\r\n$3\r\nab"))
	if b, err := r.ReadBulk(); err != nil || string(b) != "hello" {
		t.Fatalf("got %q, %v", b, err)
	}
	if _, err := r.ReadBulk(); err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated bulk: got %v", err)
	}
}

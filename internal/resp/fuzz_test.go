package resp

import (
	"bytes"
	"runtime"
	"slices"
	"testing"
)

// FuzzReadCommand feeds the parser arbitrary bytes. It must never panic or
// hang, and every command it does accept must survive being encoded and
// parsed again unchanged.
//
//	go test -fuzz FuzzReadCommand ./internal/resp
func FuzzReadCommand(f *testing.F) {
	for _, seed := range []string{
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
		"*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n",
		"SET greeting hello\r\n",
		"*0\r\n",
		"*-1\r\n",
		"*1\r\n$-1\r\n",
		"*1\r\n$536870912\r\n",
		"*1048576\r\n",
		"\r\n\r\n*1\r\n$4\r\nPING\r\n",
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for range 100 {
			args, err := r.ReadCommand()
			if err != nil {
				return
			}
			if args == nil {
				continue
			}
			again, err := NewReader(bytes.NewReader(AppendCommand(nil, args))).ReadCommand()
			if err != nil || !slices.EqualFunc(args, again, bytes.Equal) {
				t.Fatalf("round trip changed %q into %q (err %v)", args, again, err)
			}
		}
	})
}

// A length prefix alone must not make the reader reserve that much memory.
func TestHugeLengthPrefixesDontAllocate(t *testing.T) {
	for _, in := range []string{"*1\r\n$536870912\r\nabc", "*1048576\r\n$1\r\na\r\n"} {
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		NewReader(bytes.NewReader([]byte(in))).ReadCommand()
		runtime.ReadMemStats(&after)
		if n := after.TotalAlloc - before.TotalAlloc; n > 1<<20 {
			t.Errorf("%q: allocated %d bytes", in, n)
		}
	}
}

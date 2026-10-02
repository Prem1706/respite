package aof

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Prem1706/respite/internal/resp"
)

func cmd(s string) []byte {
	var args [][]byte
	for _, f := range strings.Fields(s) {
		args = append(args, []byte(f))
	}
	return resp.AppendCommand(nil, args)
}

func replayAll(t *testing.T, path string) ([]string, int64) {
	t.Helper()
	var got []string
	_, truncated, err := Replay(path, func(args [][]byte) error {
		parts := make([]string, len(args))
		for i, a := range args {
			parts[i] = string(a)
		}
		got = append(got, strings.Join(parts, " "))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return got, truncated
}

func TestAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	a, err := Open(path, FsyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	a.Append(cmd("SET a 1"))
	a.Append(cmd("INCR a"))
	a.Flush()
	a.Close()

	got, _ := replayAll(t, path)
	if strings.Join(got, "|") != "SET a 1|INCR a" {
		t.Fatalf("got %q", got)
	}
}

func TestReplayMissingFile(t *testing.T) {
	got, _ := replayAll(t, filepath.Join(t.TempDir(), "nope.aof"))
	if len(got) != 0 {
		t.Fatal("expected nothing")
	}
}

func TestReplayTruncatesPartialTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	whole := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
	partial := "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$5\r\nhel" // crash mid-write
	os.WriteFile(path, []byte(whole+partial), 0o644)

	got, truncated := replayAll(t, path)
	if len(got) != 1 || truncated != int64(len(partial)) {
		t.Fatalf("got %q, truncated %d", got, truncated)
	}
	data, _ := os.ReadFile(path)
	if string(data) != whole {
		t.Fatalf("file not truncated to last complete command: %q", data)
	}

	// Appending after recovery gives a file that replays cleanly.
	a, _ := Open(path, FsyncNo)
	a.Append(cmd("SET c 3"))
	a.Close()
	if got, _ := replayAll(t, path); len(got) != 2 {
		t.Fatalf("got %q", got)
	}
}

func TestReplayRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.aof")
	os.WriteFile(path, []byte("*1\r\n$4\r\nPING\r\n*1\r\n+oops\r\n*1\r\n$4\r\nPING\r\n"), 0o644)
	if _, _, err := Replay(path, func([][]byte) error { return nil }); err == nil {
		t.Fatal("expected an error for corrupt data mid-file")
	}
}

// Package aof implements the append-only file. Every command that changes
// data is appended to a log in RESP format, and on startup the log is
// replayed to rebuild the keyspace.
//
// Writing a file and making it durable are separate steps. A write only
// hands the bytes to the operating system; they aren't safe from a power cut
// until fsync. The fsync policy chooses the trade-off, as in Redis:
//
//	always    fsync before every reply. Nothing acknowledged is lost, but it is slow.
//	everysec  fsync once a second in the background. At most ~1s is lost on power failure.
//	no        never fsync; the OS flushes when it chooses.
package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"

	"github.com/Prem1706/respite/internal/resp"
)

type FsyncPolicy int

const (
	FsyncEverySec FsyncPolicy = iota
	FsyncAlways
	FsyncNo
)

func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch s {
	case "everysec":
		return FsyncEverySec, nil
	case "always":
		return FsyncAlways, nil
	case "no":
		return FsyncNo, nil
	}
	return 0, fmt.Errorf("unknown fsync policy %q (want always, everysec or no)", s)
}

type AOF struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	policy FsyncPolicy

	stop chan struct{}
	done chan struct{}
}

func Open(path string, policy FsyncPolicy) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	a := &AOF{
		f:      f,
		w:      bufio.NewWriterSize(f, 64*1024),
		policy: policy,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go a.background()
	return a, nil
}

// Append adds RESP-encoded commands to the in-memory buffer. They are not
// written to the file until Flush.
func (a *AOF) Append(cmds []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.w.Write(cmds) // errors are sticky and reported by Flush
}

// Flush writes buffered commands to the file, and fsyncs too under the
// "always" policy. The server calls it before sending replies, so a client is
// never told "OK" for a write that hasn't at least reached the OS.
//
// This is a group commit. The server calls Flush once per batch of pipelined
// commands, and whichever client flushes first also writes out commands
// buffered by other clients, so one system call can cover many writes.
func (a *AOF) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.w.Buffered() == 0 {
		return nil
	}
	if err := a.w.Flush(); err != nil {
		return err
	}
	if a.policy == FsyncAlways {
		return a.f.Sync()
	}
	return nil
}

// background handles "everysec". It runs for every policy anyway as a safety
// net that flushes anything still sitting in the buffer.
func (a *AOF) background() {
	defer close(a.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
			a.Flush()
			if a.policy == FsyncEverySec {
				a.f.Sync() // outside the lock, so appends carry on during the slow fsync
			}
		}
	}
}

// Close flushes, fsyncs and closes the file.
func (a *AOF) Close() error {
	close(a.stop)
	<-a.done
	a.mu.Lock()
	defer a.mu.Unlock()
	err := errors.Join(a.w.Flush(), a.f.Sync())
	return errors.Join(err, a.f.Close())
}

// Replay calls apply for every command in the file at path. A missing file
// is not an error.
//
// If the process crashed halfway through writing a command, the file ends in
// a partial command. Replay cuts that tail off, as Redis does with
// aof-load-truncated, so new appends start on a clean boundary. It returns
// the number of bytes removed. Corruption anywhere else is an error: starting
// with silently missing data would be worse than not starting.
func Replay(path string, apply func(args [][]byte) error) (commands int, truncated int64, err error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	rd := resp.NewReader(f)
	var good int64 // file offset just after the last complete command
	for {
		args, err := rd.ReadCommand()
		switch {
		case err == io.EOF:
			return commands, 0, nil
		case err == io.ErrUnexpectedEOF:
			info, err := f.Stat()
			if err != nil {
				return commands, 0, err
			}
			if err := os.Truncate(path, good); err != nil {
				return commands, 0, err
			}
			return commands, info.Size() - good, nil
		case err != nil:
			return commands, 0, fmt.Errorf("aof: bad data at byte %d: %w", good, err)
		}
		if args != nil {
			if err := apply(args); err != nil {
				return commands, 0, fmt.Errorf("aof: command at byte %d: %w", good, err)
			}
			commands++
		}
		good = rd.Offset()
	}
}

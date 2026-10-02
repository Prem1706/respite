// Package server accepts TCP connections and runs Redis commands against the
// store.
//
// Concurrency model: one goroutine per connection, all sharing one Store
// behind a read/write mutex. Real Redis runs every command on a single
// thread instead. Here, reads (GET, EXISTS, TTL) can run in parallel across
// CPU cores, while writes take turns.
//
// Write commands also hold Server.writeMu from the moment they touch the
// store until their entry is in the log. Without it, two clients setting the
// same key at once could be applied in one order and logged in the other,
// and a restart would bring back the wrong value.
package server

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/Prem1706/respite/internal/aof"
	"github.com/Prem1706/respite/internal/resp"
	"github.com/Prem1706/respite/internal/store"
)

type Config struct {
	AOFPath string // empty disables persistence
	Fsync   aof.FsyncPolicy
	Clock   func() int64 // Unix milliseconds; tests use a fake one. nil means the real clock.
	Logger  *slog.Logger
}

type Server struct {
	store   *store.Store
	aof     *aof.AOF // nil when persistence is off, and while the AOF is replaying
	broker  *broker
	log     *slog.Logger
	started time.Time

	// writeMu makes "apply to the store, then append to the log" one step.
	writeMu sync.Mutex

	mu      sync.Mutex
	ln      net.Listener
	clients map[*client]struct{}
	closed  bool

	wg        sync.WaitGroup
	stop      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// New creates a server, replaying the AOF first if there is one.
func New(cfg Config) (*Server, error) {
	s := &Server{
		store:   store.New(),
		broker:  newBroker(),
		log:     cfg.Logger,
		started: time.Now(),
		clients: make(map[*client]struct{}),
		stop:    make(chan struct{}),
	}
	if cfg.Clock != nil {
		s.store = store.NewWithClock(cfg.Clock)
	}
	if s.log == nil {
		s.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	// When the store deletes an expired key, log a DEL so that replay (and
	// followers) delete it at the same point instead of deciding for themselves.
	s.store.OnExpire(func(key string) { s.propagate(nil, []byte("DEL"), []byte(key)) })

	if cfg.AOFPath != "" {
		start := time.Now()
		s.store.SetPassive(true) // replay must not expire keys on its own; see package store
		n, truncated, err := aof.Replay(cfg.AOFPath, s.replay)
		s.store.SetPassive(false)
		if err != nil {
			return nil, err
		}
		if truncated > 0 {
			s.log.Warn("AOF ended with a partial command; truncated it", "bytes", truncated)
		}
		s.log.Info("AOF loaded", "commands", n, "keys", s.store.Len(), "took", time.Since(start).Round(time.Millisecond))
		if s.aof, err = aof.Open(cfg.AOFPath, cfg.Fsync); err != nil {
			return nil, err
		}
	}

	go s.expiryLoop()
	return s, nil
}

// replay runs one command from the AOF through the normal command code. The
// replies go nowhere, and s.aof is still nil, so nothing is logged twice.
func (s *Server) replay(args [][]byte) error {
	c := &client{w: resp.NewWriter(io.Discard)}
	if cmd, ok := lookupCommand(args[0]); !ok || !cmd.write {
		return fmt.Errorf("unexpected command %q", args[0])
	}
	s.dispatch(c, args)
	return nil
}

// Serve accepts connections until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		c := newClient(s, conn)
		if !s.track(c) {
			conn.Close()
			return nil
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			c.serve()
		}()
	}
}

// Close stops accepting connections, disconnects clients, and flushes the
// AOF to disk. It can be called more than once; later calls wait for the
// first to finish.
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		if s.ln != nil {
			s.ln.Close()
		}
		for c := range s.clients {
			c.conn.Close()
		}
		s.mu.Unlock()

		s.wg.Wait()
		close(s.stop)
		if s.aof != nil {
			s.closeErr = s.aof.Close()
		}
	})
	return s.closeErr
}

func (s *Server) track(c *client) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.clients[c] = struct{}{}
	return true
}

func (s *Server) untrack(c *client) {
	s.mu.Lock()
	delete(s.clients, c)
	s.mu.Unlock()
}

func (s *Server) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

// expiryLoop runs active expiry ten times a second, as Redis does by default.
func (s *Server) expiryLoop() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			for more := true; more; {
				s.writeMu.Lock() // the DELs this logs must be ordered with other writes
				_, more = s.store.ExpireRound()
				s.writeMu.Unlock()
			}
		}
	}
}

// testHookBeforeLog, if set, runs between a write being applied and logged.
// Tests use it to widen that gap and make ordering bugs show up reliably.
var testHookBeforeLog func()

// propagate appends a write command to the AOF. Callers hold s.writeMu.
// c is the client whose command caused the write, or nil.
func (s *Server) propagate(c *client, args ...[]byte) {
	if testHookBeforeLog != nil {
		testHookBeforeLog()
	}
	if s.aof != nil {
		s.aof.Append(args)
		if c != nil {
			c.dirty = true
		}
	}
}

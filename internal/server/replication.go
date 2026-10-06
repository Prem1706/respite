package server

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Prem1706/respite/internal/resp"
)

// Leader–follower replication, modelled on Redis.
//
// The leader turns every write into a stream of RESP commands, the same
// bytes it appends to the AOF. A follower connects and sends
// PSYNC <replication id> <offset>, saying which stream it has and how far
// into it it got. The leader then does one of two things:
//
//   - Full resync: "+FULLRESYNC <id> <offset>", followed by a snapshot of the
//     whole dataset (as commands, starting with FLUSHALL), then the live
//     stream from <offset>. Used for a new follower, or one that is too far
//     behind.
//   - Partial resync: "+CONTINUE", then the stream from where the follower
//     left off. The leader keeps the most recent bytes of the stream in a
//     backlog, so a follower that loses its connection briefly can catch up
//     without copying everything again.
//
// Followers apply commands in order through the normal command code, reject
// writes from clients, and send REPLCONF ACK <offset> every second so the
// leader can report how far behind each one is.
//
// Replication is asynchronous: the leader replies to a client before its
// followers have the write, so a follower can briefly serve stale reads,
// and a write acknowledged just before the leader dies can be lost.

const defaultBacklogSize = 1 << 20 // 1 MiB

// backlog is a ring buffer of the most recent bytes of the stream.
type backlog struct {
	buf    []byte
	offset int64 // total bytes ever written: the stream position after the newest byte
}

func (b *backlog) write(p []byte) {
	for len(p) > 0 {
		i := int(b.offset % int64(len(b.buf)))
		n := copy(b.buf[i:], p)
		p = p[n:]
		b.offset += int64(n)
	}
}

// readFrom returns everything from stream position off to the newest byte.
// It reports false if those bytes have already been overwritten.
func (b *backlog) readFrom(off int64) ([]byte, bool) {
	if off < 0 || off > b.offset || b.offset-off > int64(len(b.buf)) {
		return nil, false
	}
	out := make([]byte, b.offset-off)
	for n := 0; n < len(out); {
		i := int((off + int64(n)) % int64(len(b.buf)))
		n += copy(out[n:], b.buf[i:])
	}
	return out, true
}

// leader is the replication state every server has, in case followers connect.
type leader struct {
	id          string
	backlogSize int

	// active is set by the first PSYNC. Until then there is nobody to
	// replicate to, so writes skip the backlog.
	active atomic.Bool

	mu       sync.Mutex
	cond     *sync.Cond // signalled when the stream grows or a follower leaves
	log      backlog
	replicas map[*replica]struct{}
	closed   bool

	fullSyncs, partialSyncs atomic.Int64
}

// replica is a connected follower, as seen from the leader.
type replica struct {
	c     *client
	acked atomic.Int64 // stream offset the follower last reported
	gone  bool         // guarded by leader.mu
}

func newLeader(backlogSize int) *leader {
	id := make([]byte, 20)
	rand.Read(id)
	l := &leader{
		id:          hex.EncodeToString(id),
		backlogSize: backlogSize,
		replicas:    make(map[*replica]struct{}),
	}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// feed appends encoded commands to the stream. Callers hold Server.writeMu.
func (l *leader) feed(p []byte) {
	l.mu.Lock()
	l.log.write(p)
	l.mu.Unlock()
	l.cond.Broadcast()
}

func (l *leader) offset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.log.offset
}

func (l *leader) remove(r *replica) {
	l.mu.Lock()
	r.gone = true
	delete(l.replicas, r)
	l.mu.Unlock()
	l.cond.Broadcast()
}

func (l *leader) close() {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	l.cond.Broadcast()
}

// PSYNC <replication id> <offset>, sent by a follower.
func cmdPSync(s *Server, c *client, args [][]byte) {
	if s.follower != nil {
		c.w.Error("ERR this server is a follower; connect to the leader instead")
		return
	}
	if c.replica != nil {
		c.w.Error("ERR already syncing")
		return
	}
	l := s.leader
	wantID := string(args[1])
	wantOff, _ := strconv.ParseInt(string(args[2]), 10, 64)

	// Pause writes while choosing the starting point. For a full resync the
	// snapshot must contain exactly the writes before that offset: none
	// missing and none repeated in the stream that follows.
	s.writeMu.Lock()
	l.mu.Lock()
	if l.log.buf == nil {
		l.log = backlog{buf: make([]byte, l.backlogSize)}
		l.active.Store(true)
	}
	start := l.log.offset
	_, partial := l.log.readFrom(wantOff)
	partial = partial && wantID == l.id
	l.mu.Unlock()

	if partial {
		start = wantOff
		l.partialSyncs.Add(1)
		c.w.SimpleString("CONTINUE")
	} else {
		// Real Redis forks a child process to write the snapshot while the
		// parent keeps serving writes (copy-on-write). Go can't fork, so
		// writes wait while the snapshot is built in memory. Reads carry on.
		snapshot := resp.AppendCommand(nil, [][]byte{[]byte("FLUSHALL")})
		s.store.Dump(func(args ...[]byte) { snapshot = resp.AppendCommand(snapshot, args) })
		l.fullSyncs.Add(1)
		c.w.SimpleString(fmt.Sprintf("FULLRESYNC %s %d", l.id, start))
		c.w.Bulk(snapshot)
	}

	r := &replica{c: c}
	r.acked.Store(start)
	l.mu.Lock()
	l.replicas[r] = struct{}{}
	l.mu.Unlock()
	s.writeMu.Unlock()

	c.replica = r
	go s.streamTo(r, start)
}

// streamTo sends the stream to one follower from offset off onwards.
func (s *Server) streamTo(r *replica, off int64) {
	l := s.leader
	for {
		l.mu.Lock()
		for l.log.offset == off && !l.closed && !r.gone {
			l.cond.Wait()
		}
		if l.closed || r.gone {
			l.mu.Unlock()
			return
		}
		data, ok := l.log.readFrom(off)
		l.mu.Unlock()
		if !ok {
			// The follower fell so far behind that the bytes it needs were
			// overwritten. Drop it; it will reconnect and do a full resync.
			s.log.Warn("follower fell behind the backlog; disconnecting it", "addr", r.c.conn.RemoteAddr())
			r.c.conn.Close()
			return
		}
		// c.wmu also orders this after the PSYNC reply, which the client's
		// own goroutine is still flushing when streamTo starts.
		r.c.wmu.Lock()
		r.c.w.Raw(data)
		err := r.c.w.Flush()
		r.c.wmu.Unlock()
		if err != nil {
			r.c.conn.Close()
			return
		}
		off += int64(len(data))
	}
}

// REPLCONF ACK <offset> from a follower. It gets no reply.
func cmdReplConf(s *Server, c *client, args [][]byte) {
	if c.replica != nil && len(args) == 3 && strings.EqualFold(string(args[1]), "ACK") {
		if off, err := strconv.ParseInt(string(args[2]), 10, 64); err == nil {
			c.replica.acked.Store(off)
		}
		return
	}
	c.w.SimpleString("OK")
}

// follower is the state of a server started with -replicaof.
type follower struct {
	addr string

	mu       sync.Mutex
	conn     net.Conn // current connection to the leader, if any
	id       string   // the leader's replication id
	linkUp   bool
	lastSync time.Time

	offset atomic.Int64 // how far into the leader's stream we've applied
}

// followLoop keeps a connection to the leader, reconnecting after failures.
func (s *Server) followLoop() {
	defer s.bg.Done()
	f := s.follower
	f.offset.Store(-1)
	for {
		err := s.follow()
		f.mu.Lock()
		f.linkUp = false
		f.mu.Unlock()
		select {
		case <-s.stop:
			return
		default:
		}
		s.log.Warn("lost connection to leader; retrying in 1s", "leader", f.addr, "err", err)
		select {
		case <-s.stop:
			return
		case <-time.After(time.Second):
		}
	}
}

// follow runs one connection to the leader until it fails.
func (s *Server) follow() error {
	f := s.follower
	conn, err := net.DialTimeout("tcp", f.addr, 2*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if !s.setLeaderConn(conn) {
		return errors.New("server is shutting down")
	}

	f.mu.Lock()
	id := f.id
	f.mu.Unlock()
	if id == "" {
		id = "?"
	}
	psync := [][]byte{[]byte("PSYNC"), []byte(id), strconv.AppendInt(nil, f.offset.Load(), 10)}
	if _, err := conn.Write(resp.AppendCommand(nil, psync)); err != nil {
		return err
	}

	r := resp.NewReader(conn)
	reply, err := r.ReadCommand() // a +status line, parsed as inline words
	if err != nil {
		return err
	}
	switch {
	case len(reply) == 3 && string(reply[0]) == "+FULLRESYNC":
		off, err := strconv.ParseInt(string(reply[2]), 10, 64)
		if err != nil {
			return fmt.Errorf("bad FULLRESYNC offset %q", reply[2])
		}
		snapshot, err := r.ReadBulk()
		if err != nil {
			return err
		}
		if err := s.applySnapshot(snapshot); err != nil {
			return err
		}
		f.mu.Lock()
		f.id = string(reply[1])
		f.mu.Unlock()
		f.offset.Store(off)
		s.log.Info("full resync with leader", "leader", f.addr, "bytes", len(snapshot), "offset", off)
	case len(reply) == 1 && string(reply[0]) == "+CONTINUE":
		s.log.Info("partial resync with leader", "leader", f.addr, "offset", f.offset.Load())
	default:
		return fmt.Errorf("unexpected PSYNC reply %q", bytes.Join(reply, []byte(" ")))
	}

	f.mu.Lock()
	f.linkUp = true
	f.lastSync = time.Now()
	f.mu.Unlock()

	done := make(chan struct{})
	defer close(done)
	go s.sendAcks(conn, done)

	base, start := r.Offset(), f.offset.Load()
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return err
		}
		if args != nil {
			s.applyFromLeader(args)
		}
		f.offset.Store(start + r.Offset() - base)
	}
}

// sendAcks reports the follower's offset to the leader once a second. It
// is the only goroutine writing to the connection after the handshake.
func (s *Server) sendAcks(conn net.Conn, done chan struct{}) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			ack := [][]byte{[]byte("REPLCONF"), []byte("ACK"), strconv.AppendInt(nil, s.follower.offset.Load(), 10)}
			if _, err := conn.Write(resp.AppendCommand(nil, ack)); err != nil {
				return
			}
		}
	}
}

func (s *Server) applySnapshot(snapshot []byte) error {
	r := resp.NewReader(bytes.NewReader(snapshot))
	for {
		args, err := r.ReadCommand()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("bad snapshot: %w", err)
		}
		if args != nil {
			s.applyFromLeader(args)
		}
	}
}

// applyFromLeader runs a replicated command. It goes through the normal
// command code, so it also reaches this server's own AOF.
func (s *Server) applyFromLeader(args [][]byte) {
	cmd, ok := lookupCommand(args[0])
	if !ok || !cmd.write {
		s.log.Warn("ignoring unexpected command from leader", "cmd", string(args[0]))
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	cmd.fn(s, s.leaderClient, args)
}

func (s *Server) setLeaderConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.follower.mu.Lock()
	s.follower.conn = conn
	s.follower.mu.Unlock()
	return true
}

// replicationInfo writes the "# Replication" section of INFO.
func (s *Server) replicationInfo(b *strings.Builder) {
	b.WriteString("# Replication\r\n")
	if f := s.follower; f != nil {
		f.mu.Lock()
		status, id := "down", f.id
		if f.linkUp {
			status = "up"
		}
		f.mu.Unlock()
		host, port, _ := net.SplitHostPort(f.addr)
		fmt.Fprintf(b, "role:slave\r\nmaster_host:%s\r\nmaster_port:%s\r\nmaster_link_status:%s\r\n", host, port, status)
		fmt.Fprintf(b, "master_replid:%s\r\nslave_repl_offset:%d\r\n", id, f.offset.Load())
		return
	}
	l := s.leader
	l.mu.Lock()
	offset := l.log.offset
	replicas := make([]*replica, 0, len(l.replicas))
	for r := range l.replicas {
		replicas = append(replicas, r)
	}
	l.mu.Unlock()
	fmt.Fprintf(b, "role:master\r\nconnected_slaves:%d\r\n", len(replicas))
	for i, r := range replicas {
		acked := r.acked.Load()
		fmt.Fprintf(b, "slave%d:addr=%s,offset=%d,lag_bytes=%d\r\n", i, r.c.conn.RemoteAddr(), acked, offset-acked)
	}
	fmt.Fprintf(b, "master_replid:%s\r\nmaster_repl_offset:%d\r\n", l.id, offset)
	fmt.Fprintf(b, "sync_full:%d\r\nsync_partial_ok:%d\r\n", l.fullSyncs.Load(), l.partialSyncs.Load())
}

package server

import (
	"bufio"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Prem1706/respite/internal/aof"
	"github.com/Prem1706/respite/internal/resp"
)

// These are end-to-end tests. Each starts a real server on a random port and
// talks to it over TCP, as redis-cli would.

type fakeClock struct{ ms atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ms.Store(1_700_000_000_000)
	return c
}
func (c *fakeClock) now() int64              { return c.ms.Load() }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

type testServer struct {
	*Server
	addr string
}

func start(t *testing.T, cfg Config) *testServer {
	t.Helper()
	return startOn(t, "127.0.0.1:0", cfg)
}

func startOn(t *testing.T, addr string, cfg Config) *testServer {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ln)
	t.Cleanup(func() { s.Close() })
	return &testServer{s, ln.Addr().String()}
}

type conn struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func dial(t *testing.T, s *testServer) *conn {
	t.Helper()
	c, err := net.Dial("tcp", s.addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(5 * time.Second))
	return &conn{t: t, c: c, r: bufio.NewReader(c)}
}

func (c *conn) send(args ...string) {
	c.t.Helper()
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	if _, err := c.c.Write(resp.AppendCommand(nil, b)); err != nil {
		c.t.Fatal(err)
	}
}

// do sends a command and returns its reply formatted as a string, so tests
// can compare against things like "OK", "(nil)", "(error) ERR ..." or "[a b]".
func (c *conn) do(args ...string) string {
	c.t.Helper()
	c.send(args...)
	return c.read()
}

func (c *conn) read() string {
	c.t.Helper()
	line, err := c.r.ReadString('\n')
	if err != nil {
		c.t.Fatalf("reading reply: %v", err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '+', ':':
		return line[1:]
	case '-':
		return "(error) " + line[1:]
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			c.t.Fatal(err)
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		parts := make([]string, n)
		for i := range parts {
			parts[i] = c.read()
		}
		return "[" + strings.Join(parts, " ") + "]"
	}
	c.t.Fatalf("unexpected reply %q", line)
	return ""
}

func expect(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestBasicCommands(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("PING"), "PONG")
	expect(t, c.do("ping", "hi"), "hi")
	expect(t, c.do("SET", "k", "v"), "OK")
	expect(t, c.do("GET", "k"), "v")
	expect(t, c.do("GET", "missing"), "(nil)")
	expect(t, c.do("EXISTS", "k", "k", "missing"), "2")
	expect(t, c.do("MSET", "a", "1", "b", "2"), "OK")
	expect(t, c.do("MGET", "a", "missing", "b"), "[1 (nil) 2]")
	expect(t, c.do("DEL", "a", "b", "missing"), "2")
	expect(t, c.do("DBSIZE"), "1")
	expect(t, c.do("FLUSHALL"), "OK")
	expect(t, c.do("DBSIZE"), "0")
}

func TestSetOptions(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("SET", "k", "1", "XX"), "(nil)")
	expect(t, c.do("SET", "k", "1", "NX"), "OK")
	expect(t, c.do("SET", "k", "2", "NX"), "(nil)")
	expect(t, c.do("SET", "k", "1", "NX", "XX"), "(error) ERR syntax error")
	expect(t, c.do("SET", "k", "1", "EX", "0"), "(error) ERR invalid expire time in 'set' command")
	expect(t, c.do("SET", "k", "1", "EX", "ten"), "(error) ERR value is not an integer or out of range")
	expect(t, c.do("SET", "k", "1", "EX", "9223372036854775807"), "(error) ERR invalid expire time in 'set' command")
	expect(t, c.do("SET", "k", "1", "BOGUS"), "(error) ERR syntax error")
}

func TestErrors(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("NOPE"), "(error) ERR unknown command 'NOPE'")
	expect(t, c.do("GET"), "(error) ERR wrong number of arguments for 'get' command")
	expect(t, c.do("SET", "s", "abc"), "OK")
	expect(t, c.do("INCR", "s"), "(error) ERR value is not an integer or out of range")
	// The connection still works after an error reply.
	expect(t, c.do("PING"), "PONG")
}

func TestIncr(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("INCR", "n"), "1")
	expect(t, c.do("INCRBY", "n", "10"), "11")
	expect(t, c.do("DECR", "n"), "10")
	expect(t, c.do("DECRBY", "n", "15"), "-5")
}

func TestExpiry(t *testing.T) {
	clock := newFakeClock()
	c := dial(t, start(t, Config{Clock: clock.now}))
	expect(t, c.do("SET", "k", "v", "EX", "10"), "OK")
	expect(t, c.do("TTL", "k"), "10")
	expect(t, c.do("PTTL", "k"), "10000")
	clock.advance(9 * time.Second)
	expect(t, c.do("GET", "k"), "v")
	clock.advance(time.Second)
	expect(t, c.do("GET", "k"), "(nil)")
	expect(t, c.do("TTL", "k"), "-2")

	expect(t, c.do("SET", "k", "v"), "OK")
	expect(t, c.do("TTL", "k"), "-1")
	expect(t, c.do("EXPIRE", "k", "5"), "1")
	expect(t, c.do("PERSIST", "k"), "1")
	expect(t, c.do("TTL", "k"), "-1")
	expect(t, c.do("EXPIRE", "k", "-1"), "1") // negative expiry deletes
	expect(t, c.do("EXISTS", "k"), "0")
	expect(t, c.do("EXPIRE", "missing", "5"), "0")
}

func TestKeys(t *testing.T) {
	c := dial(t, start(t, Config{}))
	c.do("MSET", "user:1", "a", "user:2", "b", "order:1", "c")
	got := c.do("KEYS", "user:*")
	if got != "[user:1 user:2]" && got != "[user:2 user:1]" {
		t.Fatalf("got %q", got)
	}
}

func TestPipelining(t *testing.T) {
	c := dial(t, start(t, Config{}))
	// Send 1000 commands before reading any reply.
	var batch []byte
	for range 1000 {
		batch = resp.AppendCommand(batch, [][]byte{[]byte("INCR"), []byte("n")})
	}
	c.c.Write(batch)
	for i := 1; i <= 1000; i++ {
		expect(t, c.read(), strconv.Itoa(i))
	}
}

func TestInlineCommands(t *testing.T) {
	c := dial(t, start(t, Config{}))
	fmt.Fprint(c.c, "SET greeting hello\r\nGET greeting\r\n")
	expect(t, c.read(), "OK")
	expect(t, c.read(), "hello")
}

func TestProtocolErrorClosesConnection(t *testing.T) {
	c := dial(t, start(t, Config{}))
	fmt.Fprint(c.c, "*1\r\n+PING\r\n")
	expect(t, c.read(), "(error) ERR Protocol error: expected '$'")
	if _, err := c.r.ReadByte(); err == nil {
		t.Fatal("connection should be closed")
	}
}

func TestConcurrentClients(t *testing.T) {
	s := start(t, Config{})
	var wg sync.WaitGroup
	for range 20 {
		c := dial(t, s)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				c.do("INCR", "n")
			}
		}()
	}
	wg.Wait()
	expect(t, dial(t, s).do("GET", "n"), "2000")
}

func TestPubSub(t *testing.T) {
	s := start(t, Config{})
	sub, pub := dial(t, s), dial(t, s)

	expect(t, sub.do("SUBSCRIBE", "news", "sport"), "[subscribe news 1]")
	expect(t, sub.read(), "[subscribe sport 2]")
	expect(t, sub.do("GET", "k"), "(error) ERR Can't execute 'get': only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are allowed in this context")
	expect(t, sub.do("PING"), "[pong ]")

	expect(t, pub.do("PUBLISH", "news", "hello"), "1")
	expect(t, pub.do("PUBLISH", "weather", "rain"), "0")
	expect(t, sub.read(), "[message news hello]")

	// With no arguments the order of channels is unspecified (as in Redis).
	first, second := sub.do("UNSUBSCRIBE"), sub.read()
	if !strings.HasSuffix(first, " 1]") || !strings.HasSuffix(second, " 0]") || first[:17] == second[:17] {
		t.Fatalf("got %q then %q", first, second)
	}
	expect(t, pub.do("PUBLISH", "news", "again"), "0")
	expect(t, sub.do("GET", "k"), "(nil)") // back to normal mode
}

func TestSlowSubscriberIsDisconnected(t *testing.T) {
	s := start(t, Config{})
	sub, pub := dial(t, s), dial(t, s)
	expect(t, sub.do("SUBSCRIBE", "ch"), "[subscribe ch 1]")

	// sub never reads, so its socket buffers fill up and then its queue does.
	// The publisher must never block.
	big := strings.Repeat("x", 64*1024)
	deadline := time.Now().Add(5 * time.Second)
	for pub.do("PUBLISH", "ch", big) != "0" {
		if time.Now().After(deadline) {
			t.Fatal("slow subscriber was never disconnected")
		}
	}
}

func TestDisconnectUnsubscribes(t *testing.T) {
	s := start(t, Config{})
	sub, pub := dial(t, s), dial(t, s)
	expect(t, sub.do("SUBSCRIBE", "ch"), "[subscribe ch 1]")
	sub.c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for pub.do("PUBLISH", "ch", "x") != "0" {
		if time.Now().After(deadline) {
			t.Fatal("closed subscriber still receiving")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAOFRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	clock := newFakeClock()
	cfg := Config{AOFPath: path, Fsync: aof.FsyncAlways, Clock: clock.now}

	s := start(t, cfg)
	c := dial(t, s)
	c.do("SET", "name", "respite")
	c.do("INCRBY", "counter", "41")
	c.do("INCR", "counter")
	c.do("SET", "session", "abc", "EX", "10")
	c.do("SET", "temp", "x")
	c.do("DEL", "temp")
	c.do("SET", "once", "1", "NX")
	c.do("SET", "once", "2", "NX") // refused, so must not be logged
	s.Close()

	// Restart 4 seconds later. The session had a 10s TTL, so 6s should be
	// left, not a fresh 10s: the AOF stores the expiry as an absolute time.
	clock.advance(4 * time.Second)
	c = dial(t, start(t, cfg))
	expect(t, c.do("GET", "name"), "respite")
	expect(t, c.do("GET", "counter"), "42")
	expect(t, c.do("TTL", "session"), "6")
	expect(t, c.do("EXISTS", "temp"), "0")
	expect(t, c.do("GET", "once"), "1")

	clock.advance(6 * time.Second)
	expect(t, c.do("GET", "session"), "(nil)")
}

// Before writes held writeMu, two clients setting the same key at once could
// be applied in one order and logged in the other, so a restart brought back
// the wrong value. Here 20 clients race on each of 50 keys.
func TestAOFOrderMatchesConcurrentWrites(t *testing.T) {
	testHookBeforeLog = func() { time.Sleep(time.Duration(rand.IntN(100)) * time.Microsecond) }
	t.Cleanup(func() { testHookBeforeLog = nil })
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	cfg := Config{AOFPath: path, Fsync: aof.FsyncNo}
	s := start(t, cfg)

	const keys = 50
	var wg sync.WaitGroup
	for i := range 20 {
		c := dial(t, s)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keys {
				c.do("SET", fmt.Sprint("k", k), fmt.Sprint(i))
			}
		}()
	}
	wg.Wait()
	c := dial(t, s)
	want := make([]string, keys)
	for k := range keys {
		want[k] = c.do("GET", fmt.Sprint("k", k))
	}
	s.Close()

	c = dial(t, start(t, cfg))
	for k := range keys {
		expect(t, c.do("GET", fmt.Sprint("k", k)), want[k])
	}
}

// Before the store became passive during replay, this key came back as 1
// with no expiry and never went away.
func TestAOFReplayDoesNotResurrectExpiredKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	clock := newFakeClock()
	cfg := Config{AOFPath: path, Fsync: aof.FsyncAlways, Clock: clock.now}

	s := start(t, cfg)
	c := dial(t, s)
	c.do("SET", "k", "5", "PX", "1000")
	expect(t, c.do("INCR", "k"), "6")
	s.Close()

	clock.advance(2 * time.Second) // k has expired by the time we restart
	s = start(t, cfg)
	c = dial(t, s)
	expect(t, c.do("GET", "k"), "(nil)")
	expect(t, c.do("INCR", "k"), "1") // a write sees it as gone, and logs a DEL first
	expect(t, c.do("TTL", "k"), "-1")
	s.Close()

	c = dial(t, start(t, cfg))
	expect(t, c.do("GET", "k"), "1")
	expect(t, c.do("TTL", "k"), "-1")
}

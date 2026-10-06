package server

import (
	"io"
	"testing"
)

// expectRaw sends a command and checks the exact bytes of the reply, for
// tests that care about the encoding rather than the value.
func (c *conn) expectRaw(want string, args ...string) {
	c.t.Helper()
	c.send(args...)
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(c.r, buf); err != nil {
		c.t.Fatalf("%v: reading %q: %v", args, want, err)
	}
	if string(buf) != want {
		c.t.Fatalf("%v: got %q, want %q", args, buf, want)
	}
}

func TestHello(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("HELLO"), "[server respite version 7.0.0 proto 2 id 1 mode standalone role master modules []]")
	expect(t, c.do("HELLO", "3", "SETNAME", "app"), "[server respite version 7.0.0 proto 3 id 1 mode standalone role master modules []]")
	expect(t, c.do("CLIENT", "GETNAME"), "app")
	expect(t, c.do("HELLO", "4"), "(error) NOPROTO unsupported protocol version")
	expect(t, c.do("HELLO", "3", "AUTH", "user", "pass"), "(error) ERR AUTH is not supported: this server has no passwords")
	expect(t, c.do("CLIENT", "SETINFO", "LIB-NAME", "redis-py"), "OK")
	expect(t, c.do("CLIENT", "ID"), "1")
	expect(t, c.do("CLIENT", "KILL"), "(error) ERR unknown subcommand or wrong number of arguments for 'client|kill'")
}

func TestRESP3Encoding(t *testing.T) {
	c := dial(t, start(t, Config{}))
	c.do("ZADD", "z", "1.5", "a", "2", "b")

	// RESP2: null bulk string, flat WITHSCORES, scores as bulk strings.
	c.expectRaw("$-1\r\n", "GET", "missing")
	c.expectRaw("$3\r\n1.5\r\n", "ZSCORE", "z", "a")
	c.expectRaw("*4\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$1\r\n2\r\n", "ZRANGE", "z", "0", "-1", "WITHSCORES")

	c.do("HELLO", "3")
	// RESP3: real null, doubles, and [member score] pairs.
	c.expectRaw("_\r\n", "GET", "missing")
	c.expectRaw(",1.5\r\n", "ZSCORE", "z", "a")
	c.expectRaw("*2\r\n*2\r\n$1\r\na\r\n,1.5\r\n*2\r\n$1\r\nb\r\n,2\r\n", "ZRANGE", "z", "0", "-1", "WITHSCORES")
	c.expectRaw("%1\r\n$4\r\nsave\r\n$0\r\n\r\n", "CONFIG", "GET", "save")
}

func TestRESP3PubSubUsesPushMessages(t *testing.T) {
	s := start(t, Config{})
	sub, pub := dial(t, s), dial(t, s)
	sub.do("HELLO", "3")
	sub.expectRaw(">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n", "SUBSCRIBE", "news")
	expect(t, sub.do("PING"), "PONG") // a normal reply in RESP3, not an array
	pub.do("PUBLISH", "news", "hi")
	want := ">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n"
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(sub.r, buf); err != nil {
		t.Fatal(err)
	}
	expect(t, string(buf), want)
}

package server

import (
	"errors"
	"net"
	"sync"

	"github.com/Prem1706/respite/internal/resp"
)

type client struct {
	s    *Server
	conn net.Conn
	r    *resp.Reader

	// wmu guards w. Normally only this client's own goroutine writes, but
	// once subscribed, pub/sub messages arrive from a second goroutine (pump).
	wmu sync.Mutex
	w   *resp.Writer

	dirty bool // wrote to the AOF since it was last flushed
	quit  bool

	// Pub/sub. subs is only touched by this client's goroutine. Publishers
	// don't write to the socket themselves; they queue messages on msgs.
	subs map[string]struct{}
	msgs chan message
	done chan struct{}
}

func newClient(s *Server, conn net.Conn) *client {
	return &client{
		s:    s,
		conn: conn,
		r:    resp.NewReader(conn),
		w:    resp.NewWriter(conn),
		done: make(chan struct{}),
	}
}

// serve is the per-connection loop: read a command, run it, reply.
func (c *client) serve() {
	defer c.close()
	for {
		args, err := c.r.ReadCommand()
		if err != nil {
			var pe resp.ProtocolError
			if errors.As(err, &pe) {
				c.wmu.Lock()
				c.w.Error("ERR " + pe.Error())
				c.w.Flush()
				c.wmu.Unlock()
			}
			return // client disconnected or sent garbage
		}
		if args == nil {
			continue
		}

		c.wmu.Lock()
		c.s.dispatch(c, args)
		// Pipelining: if the client has already sent more commands, run
		// them first and send all the replies in one write. Before replies
		// go out, make sure the writes they acknowledge are in the AOF.
		if c.r.Buffered() == 0 {
			if c.dirty {
				if err := c.s.aof.Flush(); err != nil {
					c.s.log.Error("AOF write failed", "err", err)
				}
				c.dirty = false
			}
			err = c.w.Flush()
		}
		c.wmu.Unlock()

		if err != nil || c.quit {
			return
		}
	}
}

// pump delivers pub/sub messages. It is started on the first SUBSCRIBE.
func (c *client) pump() {
	for {
		select {
		case <-c.done:
			return
		case m := <-c.msgs:
			c.wmu.Lock()
			c.w.Array(3)
			c.w.BulkString("message")
			c.w.BulkString(m.channel)
			c.w.Bulk(m.payload)
			var err error
			if len(c.msgs) == 0 { // batch the write if more messages are queued
				err = c.w.Flush()
			}
			c.wmu.Unlock()
			if err != nil {
				c.conn.Close()
				return
			}
		}
	}
}

func (c *client) close() {
	close(c.done)
	c.conn.Close()
	for ch := range c.subs {
		c.s.broker.unsubscribe(c, ch)
	}
	c.s.untrack(c)
}

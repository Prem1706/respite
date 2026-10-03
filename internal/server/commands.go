package server

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/Prem1706/respite/internal/store"
)

type command struct {
	// Arity follows the Redis convention and counts the command name:
	// n means exactly n arguments, -n means at least n.
	arity  int
	fn     func(s *Server, c *client, args [][]byte)
	write  bool // changes data, so may appear in the AOF
	pubsub bool // allowed while the client is subscribed
}

var commands map[string]command

func init() {
	commands = map[string]command{
		"ping":          {arity: -1, fn: cmdPing, pubsub: true},
		"echo":          {arity: 2, fn: cmdEcho},
		"get":           {arity: 2, fn: cmdGet},
		"set":           {arity: -3, fn: cmdSet, write: true},
		"mget":          {arity: -2, fn: cmdMGet},
		"mset":          {arity: -3, fn: cmdMSet, write: true},
		"del":           {arity: -2, fn: cmdDel, write: true},
		"exists":        {arity: -2, fn: cmdExists},
		"incr":          {arity: 2, fn: incrBy(1), write: true},
		"decr":          {arity: 2, fn: incrBy(-1), write: true},
		"incrby":        {arity: 3, fn: incrBy(0), write: true},
		"decrby":        {arity: 3, fn: incrBy(0), write: true},
		"expire":        {arity: 3, fn: expire(time.Second, false), write: true},
		"pexpire":       {arity: 3, fn: expire(time.Millisecond, false), write: true},
		"expireat":      {arity: 3, fn: expire(time.Second, true), write: true},
		"pexpireat":     {arity: 3, fn: expire(time.Millisecond, true), write: true},
		"ttl":           {arity: 2, fn: ttl(time.Second)},
		"pttl":          {arity: 2, fn: ttl(time.Millisecond)},
		"persist":       {arity: 2, fn: cmdPersist, write: true},
		"keys":          {arity: 2, fn: cmdKeys},
		"type":          {arity: 2, fn: cmdType},
		"dbsize":        {arity: 1, fn: cmdDBSize},
		"flushall":      {arity: -1, fn: cmdFlushAll, write: true},
		"zadd":          {arity: -4, fn: cmdZAdd, write: true},
		"zincrby":       {arity: 4, fn: cmdZIncrBy, write: true},
		"zrem":          {arity: -3, fn: cmdZRem, write: true},
		"zscore":        {arity: 3, fn: cmdZScore},
		"zcard":         {arity: 2, fn: cmdZCard},
		"zrank":         {arity: 3, fn: zrank(false)},
		"zrevrank":      {arity: 3, fn: zrank(true)},
		"zrange":        {arity: -4, fn: zrange(false)},
		"zrevrange":     {arity: -4, fn: zrange(true)},
		"zrangebyscore": {arity: -4, fn: cmdZRangeByScore},
		"zcount":        {arity: 4, fn: cmdZCount},
		"publish":       {arity: 3, fn: cmdPublish},
		"subscribe":     {arity: -2, fn: cmdSubscribe, pubsub: true},
		"unsubscribe":   {arity: -1, fn: cmdUnsubscribe, pubsub: true},
		"info":          {arity: -1, fn: cmdInfo},
		"psync":         {arity: 3, fn: cmdPSync},
		"replconf":      {arity: -1, fn: cmdReplConf},
		"quit":          {arity: -1, fn: cmdQuit, pubsub: true},
		// redis-cli and redis-benchmark send these on connect. Empty replies
		// are enough to keep them happy.
		"command": {arity: -1, fn: emptyArray},
		"config":  {arity: -2, fn: cmdConfig},
	}
}

func lookupCommand(name []byte) (command, bool) {
	cmd, ok := commands[strings.ToLower(string(name))]
	return cmd, ok
}

func (s *Server) dispatch(c *client, args [][]byte) {
	name := strings.ToLower(string(args[0]))
	cmd, ok := commands[name]
	switch {
	case !ok:
		s.metrics.rejected.Add(1)
		c.w.Error(fmt.Sprintf("ERR unknown command '%.64s'", args[0]))
	case (cmd.arity > 0 && len(args) != cmd.arity) || len(args) < -cmd.arity:
		s.metrics.rejected.Add(1)
		c.w.Error("ERR wrong number of arguments for '" + name + "' command")
	case len(c.subs) > 0 && !cmd.pubsub:
		s.metrics.rejected.Add(1)
		c.w.Error("ERR Can't execute '" + name + "': only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are allowed in this context")
	case cmd.write && s.follower != nil:
		s.metrics.rejected.Add(1)
		c.w.Error("READONLY You can't write against a read only replica.")
	default:
		start := time.Now()
		if cmd.write {
			s.writeMu.Lock()
			cmd.fn(s, c, args)
			s.writeMu.Unlock()
		} else {
			cmd.fn(s, c, args)
		}
		s.metrics.observe(name, time.Since(start))
	}
}

const (
	errSyntax        = "ERR syntax error"
	errNotInteger    = "ERR value is not an integer or out of range"
	errInvalidExpire = "ERR invalid expire time in '%s' command"
)

func cmdPing(s *Server, c *client, args [][]byte) {
	switch {
	case len(args) > 2:
		c.w.Error("ERR wrong number of arguments for 'ping' command")
	case len(c.subs) > 0: // subscribed clients get PONG as a push-style array
		c.w.Array(2)
		c.w.BulkString("pong")
		if len(args) == 2 {
			c.w.Bulk(args[1])
		} else {
			c.w.BulkString("")
		}
	case len(args) == 2:
		c.w.Bulk(args[1])
	default:
		c.w.SimpleString("PONG")
	}
}

func cmdEcho(s *Server, c *client, args [][]byte) { c.w.Bulk(args[1]) }

func cmdGet(s *Server, c *client, args [][]byte) {
	v, ok, err := s.store.Get(string(args[1]))
	switch {
	case err != nil:
		replyErr(c, err)
	case ok:
		c.w.Bulk(v)
	default:
		c.w.Null()
	}
}

// SET key value [NX|XX] [EX s|PX ms|EXAT unix-s|PXAT unix-ms|KEEPTTL]
func cmdSet(s *Server, c *client, args [][]byte) {
	var opt store.SetOptions
	hasExpiry := false
	for i := 3; i < len(args); i++ {
		switch flag := strings.ToUpper(string(args[i])); flag {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "KEEPTTL":
			opt.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpiry || i+1 == len(args) {
				c.w.Error(errSyntax)
				return
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				c.w.Error(errNotInteger)
				return
			}
			unit := time.Millisecond
			if flag == "EX" || flag == "EXAT" {
				unit = time.Second
			}
			at, ok := toUnixMs(n, unit, strings.HasSuffix(flag, "AT"), s.store.Now())
			if n <= 0 || !ok {
				c.w.Error(fmt.Sprintf(errInvalidExpire, "set"))
				return
			}
			opt.ExpireAt, hasExpiry = at, true
			i++
		default:
			c.w.Error(errSyntax)
			return
		}
	}
	if (opt.NX && opt.XX) || (opt.KeepTTL && hasExpiry) {
		c.w.Error(errSyntax)
		return
	}
	if !s.store.Set(string(args[1]), args[2], opt) {
		c.w.Null()
		return
	}

	// Log the effect, not the exact command. The expiry is logged as an
	// absolute time: "EX 60" replayed tomorrow would give the key a fresh 60
	// seconds. NX/XX already did their job, so they are dropped.
	logged := [][]byte{[]byte("SET"), args[1], args[2]}
	if hasExpiry {
		logged = append(logged, []byte("PXAT"), strconv.AppendInt(nil, opt.ExpireAt, 10))
	} else if opt.KeepTTL {
		logged = append(logged, []byte("KEEPTTL"))
	}
	s.propagate(c, logged...)
	c.w.SimpleString("OK")
}

// toUnixMs turns an expiry of n units (relative to now, or absolute) into
// Unix milliseconds. It reports false if the result would overflow int64.
func toUnixMs(n int64, unit time.Duration, absolute bool, now int64) (int64, bool) {
	scale := int64(unit / time.Millisecond)
	if n > math.MaxInt64/scale || n < math.MinInt64/scale {
		return 0, false
	}
	ms := n * scale
	if absolute {
		return ms, true
	}
	if ms > math.MaxInt64-now {
		return 0, false
	}
	return now + ms, true
}

func cmdMGet(s *Server, c *client, args [][]byte) {
	c.w.Array(len(args) - 1)
	for _, k := range args[1:] {
		if v, ok, _ := s.store.Get(string(k)); ok { // other types read as nil, as in Redis
			c.w.Bulk(v)
		} else {
			c.w.Null()
		}
	}
}

func cmdMSet(s *Server, c *client, args [][]byte) {
	if len(args)%2 != 1 {
		c.w.Error("ERR wrong number of arguments for 'mset' command")
		return
	}
	s.store.MSet(args[1:])
	s.propagate(c, args...)
	c.w.SimpleString("OK")
}

func cmdDel(s *Server, c *client, args [][]byte) {
	n := s.store.Del(keys(args[1:])...)
	if n > 0 {
		s.propagate(c, args...)
	}
	c.w.Integer(int64(n))
}

func cmdExists(s *Server, c *client, args [][]byte) {
	c.w.Integer(int64(s.store.Exists(keys(args[1:])...)))
}

// incrBy builds INCR/DECR (fixed delta) and INCRBY/DECRBY (delta argument).
func incrBy(delta int64) func(*Server, *client, [][]byte) {
	return func(s *Server, c *client, args [][]byte) {
		d := delta
		if d == 0 {
			var err error
			if d, err = strconv.ParseInt(string(args[2]), 10, 64); err != nil {
				c.w.Error(errNotInteger)
				return
			}
			if strings.EqualFold(string(args[0]), "decrby") {
				if d == math.MinInt64 {
					c.w.Error("ERR decrement would overflow")
					return
				}
				d = -d
			}
		}
		n, err := s.store.IncrBy(string(args[1]), d)
		if err != nil {
			replyErr(c, err)
			return
		}
		s.propagate(c, args...) // deterministic, so safe to log as sent
		c.w.Integer(n)
	}
}

// expire builds EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT. All of them are
// logged as PEXPIREAT, an absolute time, for the same reason as SET.
func expire(unit time.Duration, absolute bool) func(*Server, *client, [][]byte) {
	return func(s *Server, c *client, args [][]byte) {
		n, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			c.w.Error(errNotInteger)
			return
		}
		at, ok := toUnixMs(n, unit, absolute, s.store.Now())
		if !ok {
			c.w.Error(fmt.Sprintf(errInvalidExpire, strings.ToLower(string(args[0]))))
			return
		}
		if !s.store.ExpireAt(string(args[1]), at) {
			c.w.Integer(0)
			return
		}
		s.propagate(c, []byte("PEXPIREAT"), args[1], strconv.AppendInt(nil, at, 10))
		c.w.Integer(1)
	}
}

func ttl(unit time.Duration) func(*Server, *client, [][]byte) {
	return func(s *Server, c *client, args [][]byte) {
		ms := s.store.TTL(string(args[1]))
		if ms >= 0 && unit == time.Second {
			ms = (ms + 500) / 1000 // Redis rounds to the nearest second
		}
		c.w.Integer(ms)
	}
}

func cmdPersist(s *Server, c *client, args [][]byte) {
	if !s.store.Persist(string(args[1])) {
		c.w.Integer(0)
		return
	}
	s.propagate(c, args...)
	c.w.Integer(1)
}

func cmdKeys(s *Server, c *client, args [][]byte) {
	ks := s.store.Keys(string(args[1]))
	c.w.Array(len(ks))
	for _, k := range ks {
		c.w.BulkString(k)
	}
}

func cmdType(s *Server, c *client, args [][]byte) {
	c.w.SimpleString(s.store.Type(string(args[1])))
}

func cmdDBSize(s *Server, c *client, args [][]byte) { c.w.Integer(int64(s.store.Len())) }

func cmdFlushAll(s *Server, c *client, args [][]byte) {
	s.store.Flush()
	s.propagate(c, []byte("FLUSHALL"))
	c.w.SimpleString("OK")
}

func cmdPublish(s *Server, c *client, args [][]byte) {
	c.w.Integer(int64(s.broker.publish(string(args[1]), args[2])))
}

func cmdSubscribe(s *Server, c *client, args [][]byte) {
	if c.msgs == nil {
		c.subs = make(map[string]struct{})
		c.msgs = make(chan message, subscriberQueue)
		go c.pump()
	}
	for _, ch := range args[1:] {
		name := string(ch)
		if _, ok := c.subs[name]; !ok {
			c.subs[name] = struct{}{}
			s.broker.subscribe(c, name)
		}
		c.w.Array(3)
		c.w.BulkString("subscribe")
		c.w.Bulk(ch)
		c.w.Integer(int64(len(c.subs)))
	}
}

func cmdUnsubscribe(s *Server, c *client, args [][]byte) {
	channels := keys(args[1:])
	if len(channels) == 0 { // no arguments: unsubscribe from everything
		for ch := range c.subs {
			channels = append(channels, ch)
		}
	}
	if len(channels) == 0 {
		c.w.Array(3)
		c.w.BulkString("unsubscribe")
		c.w.Null()
		c.w.Integer(0)
		return
	}
	for _, ch := range channels {
		if _, ok := c.subs[ch]; ok {
			delete(c.subs, ch)
			s.broker.unsubscribe(c, ch)
		}
		c.w.Array(3)
		c.w.BulkString("unsubscribe")
		c.w.BulkString(ch)
		c.w.Integer(int64(len(c.subs)))
	}
}

func cmdInfo(s *Server, c *client, args [][]byte) {
	var b strings.Builder
	fmt.Fprintf(&b, "# Server\r\nredis_version:7.0.0\r\nrespite:1\r\nuptime_in_seconds:%d\r\n",
		int(time.Since(s.started).Seconds()))
	fmt.Fprintf(&b, "\r\n# Clients\r\nconnected_clients:%d\r\n", s.clientCount())
	fmt.Fprintf(&b, "\r\n# Persistence\r\naof_enabled:%d\r\n", boolInt(s.aof != nil))
	s.replicationInfo(&b)
	fmt.Fprintf(&b, "\r\n# Keyspace\r\ndb0:keys=%d\r\n", s.store.Len())
	c.w.BulkString(b.String())
}

func cmdQuit(s *Server, c *client, args [][]byte) {
	c.w.SimpleString("OK")
	c.quit = true
}

func emptyArray(s *Server, c *client, args [][]byte) { c.w.Array(0) }

// cmdConfig answers CONFIG GET for the two settings redis-benchmark asks
// about. Settings can't be changed at runtime.
func cmdConfig(s *Server, c *client, args [][]byte) {
	if !strings.EqualFold(string(args[1]), "get") || len(args) != 3 {
		c.w.Error("ERR only CONFIG GET <parameter> is supported")
		return
	}
	appendonly := "no"
	if s.aof != nil {
		appendonly = "yes"
	}
	settings := map[string]string{"save": "", "appendonly": appendonly}
	param := strings.ToLower(string(args[2]))
	v, ok := settings[param]
	if !ok {
		c.w.Array(0)
		return
	}
	c.w.Array(2)
	c.w.BulkString(param)
	c.w.BulkString(v)
}

// replyErr writes a store error. Errors that already carry a Redis error
// code, like WRONGTYPE, are sent as they are; the rest get "ERR".
func replyErr(c *client, err error) {
	if errors.Is(err, store.ErrWrongType) {
		c.w.Error(err.Error())
		return
	}
	c.w.Error("ERR " + err.Error())
}

func keys(args [][]byte) []string {
	ks := make([]string, len(args))
	for i, a := range args {
		ks[i] = string(a)
	}
	return ks
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

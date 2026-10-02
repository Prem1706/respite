package server

import (
	"path/filepath"
	"testing"

	"github.com/Prem1706/respite/internal/aof"
)

func TestSortedSets(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("ZADD", "board", "300", "carol", "100", "alice", "200", "bob"), "3")
	expect(t, c.do("ZADD", "board", "150", "alice", "50", "dave"), "1") // alice updated, dave added
	expect(t, c.do("ZRANGE", "board", "0", "-1"), "[dave alice bob carol]")
	expect(t, c.do("ZRANGE", "board", "0", "1", "WITHSCORES"), "[dave 50 alice 150]")
	expect(t, c.do("ZREVRANGE", "board", "0", "1"), "[carol bob]")
	expect(t, c.do("ZRANK", "board", "bob"), "2")
	expect(t, c.do("ZREVRANK", "board", "bob"), "1")
	expect(t, c.do("ZRANK", "board", "nobody"), "(nil)")
	expect(t, c.do("ZSCORE", "board", "alice"), "150")
	expect(t, c.do("ZINCRBY", "board", "0.5", "alice"), "150.5")
	expect(t, c.do("ZCARD", "board"), "4")
	expect(t, c.do("ZCOUNT", "board", "(50", "+inf"), "3")
	expect(t, c.do("ZRANGEBYSCORE", "board", "-inf", "200", "WITHSCORES", "LIMIT", "1", "2"), "[alice 150.5 bob 200]")
	expect(t, c.do("ZREM", "board", "dave", "nobody"), "1")
	expect(t, c.do("TYPE", "board"), "zset")
}

func TestZAddFlags(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("ZADD", "z", "XX", "1", "a"), "0")
	expect(t, c.do("EXISTS", "z"), "0") // XX on a missing key creates nothing
	expect(t, c.do("ZADD", "z", "NX", "1", "a"), "1")
	expect(t, c.do("ZADD", "z", "NX", "5", "a"), "0")
	expect(t, c.do("ZSCORE", "z", "a"), "1")
	expect(t, c.do("ZADD", "z", "CH", "2", "a", "3", "b"), "2") // CH counts changes too
	expect(t, c.do("ZADD", "z", "NX", "XX", "1", "a"), "(error) ERR syntax error")
	expect(t, c.do("ZADD", "z", "1", "a", "nope", "b"), "(error) ERR value is not a valid float")
	expect(t, c.do("ZSCORE", "z", "b"), "3") // the bad command changed nothing
	expect(t, c.do("ZADD", "z", "1"), "(error) ERR wrong number of arguments for 'zadd' command")
}

func TestZSetEdgeCases(t *testing.T) {
	c := dial(t, start(t, Config{}))
	expect(t, c.do("ZADD", "z", "inf", "top", "-inf", "bottom"), "2")
	expect(t, c.do("ZRANGE", "z", "0", "-1", "WITHSCORES"), "[bottom -inf top inf]")
	expect(t, c.do("ZINCRBY", "z", "-inf", "top"), "(error) ERR resulting score is not a number (NaN)")
	expect(t, c.do("ZADD", "z", "nan", "x"), "(error) ERR value is not a valid float")
	expect(t, c.do("ZRANGEBYSCORE", "z", "abc", "1"), "(error) ERR min or max is not a float")
	expect(t, c.do("ZRANGE", "z", "5", "10"), "[]")
	expect(t, c.do("ZREM", "z", "top", "bottom"), "2")
	expect(t, c.do("EXISTS", "z"), "0") // emptied sets are deleted
	expect(t, c.do("ZCARD", "z"), "0")
}

func TestWrongType(t *testing.T) {
	c := dial(t, start(t, Config{}))
	c.do("SET", "s", "1")
	c.do("ZADD", "z", "1", "a")
	wrongType := "(error) WRONGTYPE Operation against a key holding the wrong kind of value"
	expect(t, c.do("GET", "z"), wrongType)
	expect(t, c.do("INCR", "z"), wrongType)
	expect(t, c.do("ZADD", "s", "1", "a"), wrongType)
	expect(t, c.do("ZRANGE", "s", "0", "-1"), wrongType)
	expect(t, c.do("MGET", "s", "z"), "[1 (nil)]")
	expect(t, c.do("SET", "z", "x"), "OK") // SET replaces any type
	expect(t, c.do("TYPE", "z"), "string")
}

func TestZSetSurvivesRestart(t *testing.T) {
	cfg := Config{AOFPath: filepath.Join(t.TempDir(), "appendonly.aof"), Fsync: aof.FsyncAlways}
	s := start(t, cfg)
	c := dial(t, s)
	c.do("ZADD", "board", "10", "a", "20", "b", "30", "c")
	c.do("ZINCRBY", "board", "0.1", "a")
	c.do("ZADD", "board", "NX", "99", "a") // refused, so not logged
	c.do("ZREM", "board", "b")
	c.do("EXPIRE", "board", "100")
	s.Close()

	c = dial(t, start(t, cfg))
	expect(t, c.do("ZRANGE", "board", "0", "-1", "WITHSCORES"), "[a 10.1 c 30]")
	expect(t, c.do("TTL", "board"), "100")
}

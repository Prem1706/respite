package server

import (
	"strings"
	"testing"
	"time"
)

// waitFor retries check until it returns "" or five seconds pass.
func waitFor(t *testing.T, what string, check func() string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		problem := check()
		if problem == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %s", what, problem)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitForReply waits until a command on c returns want.
func waitForReply(t *testing.T, c *conn, want string, args ...string) {
	t.Helper()
	waitFor(t, strings.Join(args, " ")+" = "+want, func() string {
		if got := c.do(args...); got != want {
			return "got " + got
		}
		return ""
	})
}

func info(c *conn, field string) string {
	for _, line := range strings.Split(c.do("INFO"), "\r\n") {
		if v, ok := strings.CutPrefix(line, field+":"); ok {
			return v
		}
	}
	return ""
}

func TestFullSyncThenLiveStream(t *testing.T) {
	leader := start(t, Config{})
	lc := dial(t, leader)
	lc.do("SET", "name", "respite")
	lc.do("SET", "session", "abc", "EX", "100")
	lc.do("ZADD", "board", "1", "a", "2", "b")
	lc.do("INCRBY", "n", "41")

	follower := start(t, Config{ReplicaOf: leader.addr})
	fc := dial(t, follower)

	// The initial snapshot.
	waitForReply(t, fc, "respite", "GET", "name")
	expect(t, fc.do("ZRANGE", "board", "0", "-1", "WITHSCORES"), "[a 1 b 2]")
	expect(t, fc.do("TTL", "session"), "100")
	expect(t, fc.do("GET", "n"), "41")
	expect(t, info(fc, "master_link_status"), "up")

	// Then the live stream.
	lc.do("INCR", "n")
	lc.do("ZINCRBY", "board", "10", "a")
	lc.do("DEL", "name")
	waitForReply(t, fc, "42", "GET", "n")
	expect(t, fc.do("ZRANGE", "board", "0", "-1"), "[b a]")
	expect(t, fc.do("EXISTS", "name"), "0")
}

func TestFollowerIsReadOnly(t *testing.T) {
	leader := start(t, Config{})
	fc := dial(t, start(t, Config{ReplicaOf: leader.addr}))
	expect(t, fc.do("SET", "k", "v"), "(error) READONLY You can't write against a read only replica.")
	expect(t, fc.do("ZADD", "z", "1", "a"), "(error) READONLY You can't write against a read only replica.")
	expect(t, fc.do("GET", "k"), "(nil)") // reads are fine
}

// dropFollowers cuts every follower's connection from the leader's side,
// as a network blip would.
func dropFollowers(s *testServer) {
	s.leader.mu.Lock()
	for r := range s.leader.replicas {
		r.c.conn.Close()
	}
	s.leader.mu.Unlock()
}

func TestPartialResyncAfterDisconnect(t *testing.T) {
	leader := start(t, Config{})
	lc := dial(t, leader)
	follower := start(t, Config{ReplicaOf: leader.addr})
	fc := dial(t, follower)
	lc.do("SET", "a", "1")
	waitForReply(t, fc, "1", "GET", "a")

	dropFollowers(leader)
	lc.do("SET", "b", "2") // written while the follower is disconnected

	waitForReply(t, fc, "2", "GET", "b")
	expect(t, info(lc, "sync_full"), "1")
	expect(t, info(lc, "sync_partial_ok"), "1") // caught up from the backlog
}

func TestFullResyncWhenBacklogIsOverwritten(t *testing.T) {
	leader := start(t, Config{BacklogSize: 1024})
	lc := dial(t, leader)
	follower := start(t, Config{ReplicaOf: leader.addr})
	fc := dial(t, follower)
	lc.do("SET", "a", "1")
	waitForReply(t, fc, "1", "GET", "a")

	dropFollowers(leader)
	big := strings.Repeat("x", 2000) // more than the whole backlog
	lc.do("SET", "big", big)
	lc.do("DEL", "a")

	waitForReply(t, fc, big, "GET", "big")
	expect(t, fc.do("EXISTS", "a"), "0")
	expect(t, info(lc, "sync_full"), "2")
	expect(t, info(lc, "sync_partial_ok"), "0")
}

func TestFollowerReportsOffset(t *testing.T) {
	leader := start(t, Config{})
	lc := dial(t, leader)
	dial(t, start(t, Config{ReplicaOf: leader.addr}))
	waitFor(t, "a follower to connect", func() string {
		if info(lc, "connected_slaves") != "1" {
			return "not connected"
		}
		return ""
	})
	lc.do("SET", "k", "v")
	// The follower acks once a second; wait until it has caught up.
	waitFor(t, "lag to reach 0", func() string {
		if line := info(lc, "slave0"); !strings.HasSuffix(line, ",lag_bytes=0") || info(lc, "master_repl_offset") == "0" {
			return line
		}
		return ""
	})
}

// The leader decides when keys expire. Here the follower's clock is 10s
// behind, so on its own it would still think the key is live.
func TestExpiryFollowsTheLeader(t *testing.T) {
	leaderClock, followerClock := newFakeClock(), newFakeClock()
	followerClock.advance(-10 * time.Second)
	leader := start(t, Config{Clock: leaderClock.now})
	lc := dial(t, leader)
	fc := dial(t, start(t, Config{ReplicaOf: leader.addr, Clock: followerClock.now}))

	lc.do("SET", "k", "5", "PX", "1000")
	waitForReply(t, fc, "5", "GET", "k")

	leaderClock.advance(2 * time.Second) // expired on the leader only
	// The leader's write finds the key expired, logs DEL, then INCR. Without
	// that DEL, the follower would compute 6 and keep the old TTL.
	expect(t, lc.do("INCR", "k"), "1")
	waitForReply(t, fc, "-1", "TTL", "k")
	expect(t, fc.do("GET", "k"), "1")

	// Active expiry on the leader also reaches the follower.
	lc.do("SET", "temp", "x", "PX", "500")
	waitForReply(t, fc, "x", "GET", "temp")
	leaderClock.advance(time.Second)
	waitForReply(t, fc, "0", "EXISTS", "temp")
}

func TestFollowerReconnectsAfterLeaderRestart(t *testing.T) {
	leader := start(t, Config{})
	addr := leader.addr
	lc := dial(t, leader)
	lc.do("SET", "a", "1")
	fc := dial(t, start(t, Config{ReplicaOf: addr}))
	waitForReply(t, fc, "1", "GET", "a")

	// A new leader on the same address has a new replication id and no
	// data, so the follower must do a full resync and end up empty too.
	leader.Close()
	restarted := startOn(t, addr, Config{})
	dial(t, restarted).do("SET", "b", "2")
	waitForReply(t, fc, "2", "GET", "b")
	expect(t, fc.do("EXISTS", "a"), "0")
	expect(t, info(fc, "master_link_status"), "up")
}

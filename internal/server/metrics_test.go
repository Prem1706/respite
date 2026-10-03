package server

import (
	"strings"
	"testing"
	"time"
)

func scrape(s *testServer) string {
	var b strings.Builder
	s.writeMetrics(&b)
	return b.String()
}

func expectMetric(t *testing.T, out, line string) {
	t.Helper()
	if !strings.Contains(out, "\n"+line+"\n") {
		t.Fatalf("metrics missing %q:\n%s", line, out)
	}
}

func TestMetrics(t *testing.T) {
	clock := newFakeClock()
	s := start(t, Config{Clock: clock.now})
	c := dial(t, s)
	c.do("SET", "a", "1")
	c.do("GET", "a")
	c.do("GET", "a")
	c.do("NOPE")
	c.do("SET", "temp", "x", "PX", "10")
	clock.advance(time.Second)
	c.do("INCR", "temp") // finds temp expired

	out := scrape(s)
	expectMetric(t, out, `respite_commands_total{cmd="get"} 2`)
	expectMetric(t, out, `respite_commands_total{cmd="set"} 2`)
	expectMetric(t, out, `respite_commands_rejected_total 1`)
	expectMetric(t, out, `respite_command_duration_seconds_count 5`)
	expectMetric(t, out, `respite_connected_clients 1`)
	expectMetric(t, out, `respite_expired_keys_total 1`)
	expectMetric(t, out, `respite_is_leader 1`)
	if strings.Contains(out, `cmd="zadd"`) {
		t.Fatal("commands never run should be left out")
	}
}

func TestMetricsShowFollowers(t *testing.T) {
	leader := start(t, Config{})
	follower := start(t, Config{ReplicaOf: leader.addr})
	waitFor(t, "follower link", func() string {
		if !strings.Contains(scrape(follower), "\nrespite_leader_link_up 1\n") {
			return "link down"
		}
		return ""
	})
	expectMetric(t, scrape(follower), "respite_is_leader 0")
	expectMetric(t, scrape(leader), "respite_connected_followers 1")
}

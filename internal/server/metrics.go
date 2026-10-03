package server

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"sync/atomic"
	"time"
)

// Metrics in the Prometheus text format, served over HTTP for Prometheus to
// scrape. Counters only ever go up; Prometheus works out rates from them.
//
// Latency is measured around the command itself, from dispatch to the reply
// being buffered. It doesn't include network time or time spent queued
// behind other clients' pipelined commands.

// latencyBuckets are the upper bounds of the histogram buckets.
var latencyBuckets = []time.Duration{
	time.Microsecond, 5 * time.Microsecond, 10 * time.Microsecond, 25 * time.Microsecond,
	50 * time.Microsecond, 100 * time.Microsecond, 250 * time.Microsecond, 500 * time.Microsecond,
	time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
}

type metrics struct {
	calls     map[string]*atomic.Uint64 // per command; the map itself never changes after init
	rejected  atomic.Uint64             // unknown commands, wrong arity, READONLY
	buckets   []atomic.Uint64           // one per latency bucket, plus one for +Inf
	latencyNs atomic.Uint64
	conns     atomic.Uint64
}

func newMetrics() *metrics {
	m := &metrics{
		calls:   make(map[string]*atomic.Uint64, len(commands)),
		buckets: make([]atomic.Uint64, len(latencyBuckets)+1),
	}
	for name := range commands {
		m.calls[name] = new(atomic.Uint64)
	}
	return m
}

func (m *metrics) observe(name string, d time.Duration) {
	m.calls[name].Add(1)
	m.latencyNs.Add(uint64(d))
	i, _ := slices.BinarySearch(latencyBuckets, d)
	m.buckets[i].Add(1)
}

// MetricsHandler serves /metrics.
func (s *Server) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.writeMetrics(w)
	})
}

func (s *Server) writeMetrics(w io.Writer) {
	m := s.metrics
	metric := func(name, typ, help string) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, typ)
	}

	metric("respite_commands_total", "counter", "Commands processed, by command.")
	names := make([]string, 0, len(m.calls))
	for name := range m.calls {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if n := m.calls[name].Load(); n > 0 {
			fmt.Fprintf(w, "respite_commands_total{cmd=%q} %d\n", name, n)
		}
	}
	metric("respite_commands_rejected_total", "counter", "Commands refused before running: unknown, wrong arity or READONLY.")
	fmt.Fprintf(w, "respite_commands_rejected_total %d\n", m.rejected.Load())

	metric("respite_command_duration_seconds", "histogram", "Time spent executing commands.")
	var cumulative uint64
	for i, bound := range latencyBuckets {
		cumulative += m.buckets[i].Load()
		fmt.Fprintf(w, "respite_command_duration_seconds_bucket{le=\"%g\"} %d\n", bound.Seconds(), cumulative)
	}
	cumulative += m.buckets[len(latencyBuckets)].Load()
	fmt.Fprintf(w, "respite_command_duration_seconds_bucket{le=\"+Inf\"} %d\n", cumulative)
	fmt.Fprintf(w, "respite_command_duration_seconds_sum %g\n", time.Duration(m.latencyNs.Load()).Seconds())
	fmt.Fprintf(w, "respite_command_duration_seconds_count %d\n", cumulative)

	metric("respite_connected_clients", "gauge", "Open client connections.")
	fmt.Fprintf(w, "respite_connected_clients %d\n", s.clientCount())
	metric("respite_connections_total", "counter", "Client connections accepted.")
	fmt.Fprintf(w, "respite_connections_total %d\n", m.conns.Load())
	metric("respite_keys", "gauge", "Keys in the keyspace, including expired keys not yet deleted.")
	fmt.Fprintf(w, "respite_keys %d\n", s.store.Len())
	metric("respite_expired_keys_total", "counter", "Keys deleted because their TTL passed.")
	fmt.Fprintf(w, "respite_expired_keys_total %d\n", s.store.ExpiredKeys())

	if f := s.follower; f != nil {
		f.mu.Lock()
		up := 0
		if f.linkUp {
			up = 1
		}
		f.mu.Unlock()
		metric("respite_is_leader", "gauge", "1 on a leader, 0 on a follower.")
		fmt.Fprintln(w, "respite_is_leader 0")
		metric("respite_leader_link_up", "gauge", "1 while connected to the leader.")
		fmt.Fprintf(w, "respite_leader_link_up %d\n", up)
		metric("respite_replication_offset_bytes", "counter", "How far into the leader's replication stream this follower has applied.")
		fmt.Fprintf(w, "respite_replication_offset_bytes %d\n", f.offset.Load())
	} else {
		l := s.leader
		l.mu.Lock()
		offset := l.log.offset
		type lag struct {
			addr  string
			bytes int64
		}
		lags := make([]lag, 0, len(l.replicas))
		for r := range l.replicas {
			lags = append(lags, lag{r.c.conn.RemoteAddr().String(), offset - r.acked.Load()})
		}
		l.mu.Unlock()
		metric("respite_is_leader", "gauge", "1 on a leader, 0 on a follower.")
		fmt.Fprintln(w, "respite_is_leader 1")
		metric("respite_replication_offset_bytes", "counter", "Bytes written to the replication stream.")
		fmt.Fprintf(w, "respite_replication_offset_bytes %d\n", offset)
		metric("respite_connected_followers", "gauge", "Followers currently connected.")
		fmt.Fprintf(w, "respite_connected_followers %d\n", len(lags))
		metric("respite_follower_lag_bytes", "gauge", "How far each follower's last acknowledgement is behind the stream.")
		for _, x := range lags {
			fmt.Fprintf(w, "respite_follower_lag_bytes{follower=%q} %d\n", x.addr, x.bytes)
		}
		metric("respite_full_syncs_total", "counter", "Followers sent a full snapshot.")
		fmt.Fprintf(w, "respite_full_syncs_total %d\n", l.fullSyncs.Load())
		metric("respite_partial_syncs_total", "counter", "Followers that caught up from the backlog.")
		fmt.Fprintf(w, "respite_partial_syncs_total %d\n", l.partialSyncs.Load())
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	metric("respite_heap_bytes", "gauge", "Bytes of allocated heap objects.")
	fmt.Fprintf(w, "respite_heap_bytes %d\n", mem.HeapAlloc)
	metric("respite_goroutines", "gauge", "Running goroutines.")
	fmt.Fprintf(w, "respite_goroutines %d\n", runtime.NumGoroutine())
	metric("respite_uptime_seconds", "gauge", "Seconds since the server started.")
	fmt.Fprintf(w, "respite_uptime_seconds %d\n", int(time.Since(s.started).Seconds()))
}

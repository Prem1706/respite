package store

import (
	"sort"
	"strconv"
	"sync"
	"testing"
)

// clock is a fake time source so expiry tests run instantly and deterministically.
type clock struct{ ms int64 }

func (c *clock) now() int64 { return c.ms }

func newTestStore() (*Store, *clock) {
	c := &clock{ms: 1_000_000}
	return NewWithClock(c.now), c
}

func TestSetGet(t *testing.T) {
	s, _ := newTestStore()
	if _, ok := s.Get("k"); ok {
		t.Fatal("empty store returned a value")
	}
	s.Set("k", []byte("v"), SetOptions{})
	if v, ok := s.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("got %q, %v", v, ok)
	}
}

func TestSetNXXX(t *testing.T) {
	s, _ := newTestStore()
	if s.Set("k", []byte("1"), SetOptions{XX: true}) {
		t.Fatal("XX set a missing key")
	}
	if !s.Set("k", []byte("1"), SetOptions{NX: true}) {
		t.Fatal("NX refused a missing key")
	}
	if s.Set("k", []byte("2"), SetOptions{NX: true}) {
		t.Fatal("NX overwrote an existing key")
	}
	if v, _ := s.Get("k"); string(v) != "1" {
		t.Fatalf("got %q", v)
	}
}

func TestExpiry(t *testing.T) {
	s, c := newTestStore()
	s.Set("k", []byte("v"), SetOptions{ExpireAt: c.ms + 100})
	if ttl := s.TTL("k"); ttl != 100 {
		t.Fatalf("TTL = %d, want 100", ttl)
	}
	c.ms += 99
	if _, ok := s.Get("k"); !ok {
		t.Fatal("key expired early")
	}
	c.ms++
	if _, ok := s.Get("k"); ok {
		t.Fatal("expired key is still visible")
	}
	if ttl := s.TTL("k"); ttl != -2 {
		t.Fatalf("TTL of expired key = %d, want -2", ttl)
	}
	// Not deleted yet (lazy expiry): still counted by Len until swept.
	if s.Len() != 1 || s.ActiveExpire() != 1 || s.Len() != 0 {
		t.Fatal("ActiveExpire did not remove the expired key")
	}
}

func TestSetClearsTTLUnlessKeepTTL(t *testing.T) {
	s, c := newTestStore()
	s.Set("k", []byte("v"), SetOptions{ExpireAt: c.ms + 100})
	s.Set("k", []byte("v2"), SetOptions{KeepTTL: true})
	if s.TTL("k") != 100 {
		t.Fatal("KEEPTTL lost the TTL")
	}
	s.Set("k", []byte("v3"), SetOptions{})
	if s.TTL("k") != -1 {
		t.Fatal("plain SET should clear the TTL")
	}
}

func TestExpireAtPastDeletes(t *testing.T) {
	s, c := newTestStore()
	s.Set("k", []byte("v"), SetOptions{})
	if !s.ExpireAt("k", c.ms-1) {
		t.Fatal("ExpireAt reported a missing key")
	}
	if s.Exists("k") != 0 {
		t.Fatal("key with a past expiry still exists")
	}
	if s.ExpireAt("missing", c.ms+10) {
		t.Fatal("ExpireAt on a missing key returned true")
	}
}

func TestPersist(t *testing.T) {
	s, c := newTestStore()
	s.Set("k", []byte("v"), SetOptions{ExpireAt: c.ms + 10})
	if !s.Persist("k") || s.TTL("k") != -1 || s.Persist("k") {
		t.Fatal("Persist did not remove the TTL exactly once")
	}
}

func TestIncrBy(t *testing.T) {
	s, c := newTestStore()
	if n, _ := s.IncrBy("n", 5); n != 5 {
		t.Fatalf("got %d, want 5", n)
	}
	if n, _ := s.IncrBy("n", -7); n != -2 {
		t.Fatalf("got %d, want -2", n)
	}
	s.Set("s", []byte("abc"), SetOptions{})
	if _, err := s.IncrBy("s", 1); err != ErrNotInteger {
		t.Fatalf("got %v, want ErrNotInteger", err)
	}
	s.Set("big", []byte("9223372036854775807"), SetOptions{})
	if _, err := s.IncrBy("big", 1); err != ErrOverflow {
		t.Fatalf("got %v, want ErrOverflow", err)
	}
	s.ExpireAt("n", c.ms+50)
	s.IncrBy("n", 1)
	if s.TTL("n") != 50 {
		t.Fatal("INCR should keep the TTL")
	}
}

func TestConcurrentIncr(t *testing.T) {
	s, _ := newTestStore()
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				s.IncrBy("n", 1)
			}
		}()
	}
	wg.Wait()
	if v, _ := s.Get("n"); string(v) != "10000" {
		t.Fatalf("lost updates: got %s, want 10000", v)
	}
}

func TestActiveExpireSweepsLargeBacklog(t *testing.T) {
	s, c := newTestStore()
	for i := range 1000 {
		s.Set(strconv.Itoa(i), []byte("v"), SetOptions{ExpireAt: c.ms + 1})
	}
	for i := range 10 {
		s.Set("keep"+strconv.Itoa(i), []byte("v"), SetOptions{ExpireAt: c.ms + 1000})
	}
	c.ms += 2
	s.ActiveExpire()
	// It keeps sampling while over 25% of a sample is expired, so nearly
	// everything goes in one call.
	if s.Len() > 100 {
		t.Fatalf("%d keys left after ActiveExpire, want most of the 1000 gone", s.Len())
	}
	if s.Exists("keep0", "keep9") != 2 {
		t.Fatal("ActiveExpire removed live keys")
	}
}

func TestKeys(t *testing.T) {
	s, _ := newTestStore()
	for _, k := range []string{"user:1", "user:2", "user/3", "session:1"} {
		s.Set(k, []byte("v"), SetOptions{})
	}
	got := s.Keys("user*")
	sort.Strings(got)
	if len(got) != 3 || got[2] != "user:2" {
		t.Fatalf("got %v", got)
	}
}

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true},
		{"*", "a/b", true},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "heeeello", true},
		{"h*llo", "helloo", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{`h\*llo`, "h*llo", true},
		{`h\*llo`, "hello", false},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxbyy", false},
		{"[abc", "a", false},
	}
	for _, tc := range cases {
		if got := Match(tc.pattern, tc.s); got != tc.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.s, got, tc.want)
		}
	}
}

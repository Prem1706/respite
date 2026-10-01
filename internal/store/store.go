// Package store is the in-memory keyspace: a Go map guarded by a
// read/write mutex, with Redis-style key expiry.
//
// Expiry works the same two ways as in Redis:
//
//   - Lazily. Reads treat an expired key as missing, so a client can never
//     see one, even before it has been deleted.
//   - Actively. ActiveExpire, run ten times a second, samples keys that have
//     a TTL and deletes the expired ones, so memory is reclaimed for keys
//     that nobody reads again.
package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrOverflow   = errors.New("increment or decrement would overflow")
)

type entry struct {
	val      []byte
	expireAt int64 // Unix milliseconds; 0 means the key never expires
}

func (e entry) expired(now int64) bool { return e.expireAt != 0 && e.expireAt <= now }

// Store is safe for concurrent use. Values are never modified in place; a
// write always stores a new slice. That means a []byte handed out by Get
// stays valid after the lock is released, even if the key is overwritten.
type Store struct {
	mu      sync.RWMutex
	data    map[string]entry
	expires map[string]struct{} // the keys that have a TTL, for ActiveExpire
	now     func() int64
}

func New() *Store {
	return NewWithClock(func() int64 { return time.Now().UnixMilli() })
}

// NewWithClock lets tests control time instead of sleeping.
func NewWithClock(now func() int64) *Store {
	return &Store{
		data:    make(map[string]entry),
		expires: make(map[string]struct{}),
		now:     now,
	}
}

// Now returns the store's current time in Unix milliseconds.
func (s *Store) Now() int64 { return s.now() }

// lookup returns the entry for key if it exists and has not expired.
// Deleting an expired entry needs the write lock, which readers don't hold,
// so that is left to writers and ActiveExpire.
func (s *Store) lookup(key string, now int64) (entry, bool) {
	e, ok := s.data[key]
	if !ok || e.expired(now) {
		return entry{}, false
	}
	return e, true
}

func (s *Store) put(key string, e entry) {
	s.data[key] = e
	if e.expireAt != 0 {
		s.expires[key] = struct{}{}
	} else {
		delete(s.expires, key)
	}
}

func (s *Store) remove(key string) {
	delete(s.data, key)
	delete(s.expires, key)
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, s.now())
	return e.val, ok
}

type SetOptions struct {
	ExpireAt int64 // absolute Unix milliseconds; 0 means no expiry
	KeepTTL  bool  // keep the key's existing expiry
	NX       bool  // only set the key if it does not exist
	XX       bool  // only set the key if it already exists
}

// Set stores val under key and reports whether it did (NX and XX can stop it).
func (s *Store) Set(key string, val []byte, opt SetOptions) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, exists := s.lookup(key, s.now())
	if (opt.NX && exists) || (opt.XX && !exists) {
		return false
	}
	e := entry{val: val, expireAt: opt.ExpireAt}
	if opt.KeepTTL {
		e.expireAt = old.expireAt
	}
	s.put(key, e)
	return true
}

// MSet sets several keys at once. All of them change under one lock, so no
// reader can see some of the new values without the others.
func (s *Store) MSet(pairs [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i+1 < len(pairs); i += 2 {
		s.put(string(pairs[i]), entry{val: pairs[i+1]})
	}
}

// Del deletes keys and returns how many existed.
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now, n := s.now(), 0
	for _, k := range keys {
		if _, ok := s.lookup(k, now); ok {
			n++
		}
		s.remove(k)
	}
	return n
}

// Exists returns how many of keys exist. A key named twice is counted twice.
func (s *Store) Exists(keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now, n := s.now(), 0
	for _, k := range keys {
		if _, ok := s.lookup(k, now); ok {
			n++
		}
	}
	return n
}

// IncrBy adds delta to the integer stored at key, treating a missing key as
// 0. The key keeps its TTL, as in Redis. The read, add and write all happen
// under one lock, so concurrent increments can't lose updates.
func (s *Store) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, s.now())
	var n int64
	if ok {
		var err error
		if n, err = strconv.ParseInt(string(e.val), 10, 64); err != nil {
			return 0, ErrNotInteger
		}
	}
	if (delta > 0 && n > math.MaxInt64-delta) || (delta < 0 && n < math.MinInt64-delta) {
		return 0, ErrOverflow
	}
	n += delta
	e.val = strconv.AppendInt(nil, n, 10)
	s.put(key, e)
	return n, nil
}

// ExpireAt sets key to expire at the given Unix millisecond time and reports
// whether the key existed. A time in the past deletes the key.
func (s *Store) ExpireAt(key string, at int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	e, ok := s.lookup(key, now)
	if !ok {
		return false
	}
	if at <= now {
		s.remove(key)
		return true
	}
	e.expireAt = at
	s.put(key, e)
	return true
}

// Persist removes key's expiry and reports whether it had one.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookup(key, s.now())
	if !ok || e.expireAt == 0 {
		return false
	}
	e.expireAt = 0
	s.put(key, e)
	return true
}

// TTL returns the milliseconds until key expires. Like Redis it returns -2
// if the key does not exist and -1 if it has no expiry.
func (s *Store) TTL(key string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	e, ok := s.lookup(key, now)
	switch {
	case !ok:
		return -2
	case e.expireAt == 0:
		return -1
	default:
		return e.expireAt - now
	}
}

// Keys returns every live key that matches a glob pattern. It walks the
// whole keyspace, so like Redis' KEYS it is O(n).
func (s *Store) Keys(pattern string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	var keys []string
	for k, e := range s.data {
		if !e.expired(now) && Match(pattern, k) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Len returns the number of keys. It can include expired keys that haven't
// been cleaned up yet, as Redis' DBSIZE does.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *Store) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]entry)
	s.expires = make(map[string]struct{})
}

const expireSample = 20

// ActiveExpire uses Redis' sampling approach. It checks 20 keys that have a
// TTL, deletes the expired ones, and goes again if more than a quarter of the
// sample had expired, since that suggests many more are waiting. Each round
// holds the lock only briefly, so clients are never blocked for long, but the
// share of expired keys still left in memory stays small.
//
// Go randomises the starting point of map iteration, so the first 20 keys of
// a range loop are a cheap random-ish sample.
func (s *Store) ActiveExpire() int {
	total := 0
	for {
		s.mu.Lock()
		now, sampled, expired := s.now(), 0, 0
		for k := range s.expires {
			if sampled == expireSample {
				break
			}
			sampled++
			if s.data[k].expired(now) {
				s.remove(k) // deleting during range is allowed in Go
				expired++
			}
		}
		s.mu.Unlock()
		total += expired
		if sampled < expireSample || expired*4 <= sampled {
			return total
		}
	}
}

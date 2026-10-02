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
//
// Only one place decides that a key has expired: the leader's own store.
// Whenever it deletes an expired key it calls the OnExpire hook, and the
// server logs a DEL. While replaying the AOF, or running as a follower, the
// store is passive: it never deletes expired keys itself and waits for those
// DELs instead. Reads still hide expired keys.
//
// Without this, replaying "INCR k" after k's TTL has passed would treat k as
// missing and recreate it as 1 with no expiry: a key that should be gone
// would live forever. A follower whose clock is slightly ahead would make
// the same mistake.
package store

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/Prem1706/respite/internal/zset"
)

var (
	ErrNotInteger = errors.New("value is not an integer or out of range")
	ErrOverflow   = errors.New("increment or decrement would overflow")
	ErrWrongType  = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type entry struct {
	val      any   // []byte for a string, *zset.ZSet for a sorted set
	expireAt int64 // Unix milliseconds; 0 means the key never expires
}

func (e entry) expired(now int64) bool { return e.expireAt != 0 && e.expireAt <= now }

// Store is safe for concurrent use. String values are never modified in
// place; a write always stores a new slice. That means a []byte handed out by
// Get stays valid after the lock is released, even if the key is
// overwritten. Sorted sets are modified in place, so they are only ever
// touched inside UpdateZSet and ReadZSet, with the lock held.
type Store struct {
	mu       sync.RWMutex
	data     map[string]entry
	expires  map[string]struct{} // the keys that have a TTL, for ActiveExpire
	now      func() int64
	passive  bool
	onExpire func(key string)
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

// SetPassive turns passive expiry on or off. See the package comment.
func (s *Store) SetPassive(passive bool) {
	s.mu.Lock()
	s.passive = passive
	s.mu.Unlock()
}

// OnExpire sets a function to call whenever an expired key is deleted. It is
// called with the store's lock held, so it must not call back into the store.
func (s *Store) OnExpire(fn func(key string)) {
	s.mu.Lock()
	s.onExpire = fn
	s.mu.Unlock()
}

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

// lookupWrite is lookup for code that is about to modify key. An expired
// entry is deleted (and reported to onExpire) so the write starts from a
// missing key. In passive mode the entry is returned as if still live,
// because only the leader's DEL may remove it.
func (s *Store) lookupWrite(key string, now int64) (entry, bool) {
	e, ok := s.data[key]
	if !ok {
		return entry{}, false
	}
	if e.expired(now) && !s.passive {
		s.expire(key)
		return entry{}, false
	}
	return e, true
}

func (s *Store) expire(key string) {
	s.remove(key)
	if s.onExpire != nil {
		s.onExpire(key)
	}
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

// Get returns the string stored at key. It returns ErrWrongType if the key
// holds another type.
func (s *Store) Get(key string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, s.now())
	if !ok {
		return nil, false, nil
	}
	b, isString := e.val.([]byte)
	if !isString {
		return nil, false, ErrWrongType
	}
	return b, true, nil
}

// Type returns "string", "zset", or "none" if the key doesn't exist.
func (s *Store) Type(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, s.now())
	switch {
	case !ok:
		return "none"
	case isZSet(e.val):
		return "zset"
	default:
		return "string"
	}
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
	old, exists := s.lookupWrite(key, s.now())
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
		if _, ok := s.lookupWrite(k, now); ok {
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
	e, ok := s.lookupWrite(key, s.now())
	var n int64
	if ok {
		b, isString := e.val.([]byte)
		if !isString {
			return 0, ErrWrongType
		}
		var err error
		if n, err = strconv.ParseInt(string(b), 10, 64); err != nil {
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
	e, ok := s.lookupWrite(key, now)
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

func isZSet(v any) bool {
	_, ok := v.(*zset.ZSet)
	return ok
}

// UpdateZSet runs fn on the sorted set at key with the write lock held. If
// the key doesn't exist, fn is only called when create is true, with a new
// empty set. A set left empty afterwards is deleted, as Redis does.
//
// fn must not keep z, or anything that refers into it, after it returns.
func (s *Store) UpdateZSet(key string, create bool, fn func(z *zset.ZSet)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookupWrite(key, s.now())
	if ok && !isZSet(e.val) {
		return ErrWrongType
	}
	if !ok {
		if !create {
			return nil
		}
		e = entry{val: zset.New()}
	}
	z := e.val.(*zset.ZSet)
	fn(z)
	if z.Len() == 0 {
		s.remove(key)
	} else {
		s.put(key, e)
	}
	return nil
}

// ReadZSet runs fn on the sorted set at key with the read lock held. fn is
// not called if the key doesn't exist. Other readers can run at the same
// time, so fn must not modify z.
func (s *Store) ReadZSet(key string, fn func(z *zset.ZSet)) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.lookup(key, s.now())
	if !ok {
		return nil
	}
	z, isZ := e.val.(*zset.ZSet)
	if !isZ {
		return ErrWrongType
	}
	fn(z)
	return nil
}

// Persist removes key's expiry and reports whether it had one.
func (s *Store) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.lookupWrite(key, s.now())
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

// Dump calls emit with commands that rebuild the current keyspace from
// empty: a SET or ZADD per key, then PEXPIREAT for keys with a TTL. A new
// follower receives this as its starting snapshot. Expired keys are skipped.
func (s *Store) Dump(emit func(args ...[]byte)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	for k, e := range s.data {
		if e.expired(now) {
			continue
		}
		key := []byte(k)
		switch v := e.val.(type) {
		case []byte:
			emit([]byte("SET"), key, v)
		case *zset.ZSet:
			args := [][]byte{[]byte("ZADD"), key}
			for _, m := range v.All() {
				args = append(args, []byte(zset.FormatScore(m.Score)), []byte(m.Name))
			}
			emit(args...)
		}
		if e.expireAt != 0 {
			emit([]byte("PEXPIREAT"), key, strconv.AppendInt(nil, e.expireAt, 10))
		}
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
func (s *Store) ActiveExpire() int {
	total := 0
	for {
		expired, more := s.ExpireRound()
		total += expired
		if !more {
			return total
		}
	}
}

// ExpireRound runs one sampling round of ActiveExpire. It reports how many
// keys it deleted and whether another round is worthwhile. It does nothing
// in passive mode.
//
// Go randomises the starting point of map iteration, so the first 20 keys of
// a range loop are a cheap random-ish sample.
func (s *Store) ExpireRound() (expired int, more bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.passive {
		return 0, false
	}
	now, sampled := s.now(), 0
	for k := range s.expires {
		if sampled == expireSample {
			break
		}
		sampled++
		if s.data[k].expired(now) {
			s.expire(k) // deleting during range is allowed in Go
			expired++
		}
	}
	return expired, sampled == expireSample && expired*4 > sampled
}

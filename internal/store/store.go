// Package store is the in-memory key-value store: a map guarded by a
// sync.RWMutex, lazy expiry on read, and a background sweeper for keys
// that are never read again after they expire.
package store

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"
)

// ErrNotInteger is returned by Incr when the existing value at a key
// can't be parsed as a base-10 int64.
var ErrNotInteger = errors.New("value is not an integer or out of range")

type entry struct {
	value     []byte
	expiresAt time.Time // zero value means "no expiry"
}

func (e entry) expired(now time.Time) bool {
	return !e.expiresAt.IsZero() && now.After(e.expiresAt)
}

// Store is a concurrency-safe in-memory key-value store with optional
// per-key expiry.
type Store struct {
	mu   sync.RWMutex
	data map[string]entry
}

// New creates an empty Store.
func New() *Store {
	return &Store{data: make(map[string]entry)}
}

// Set stores value under key. If ttl > 0, the key expires ttl after now;
// ttl <= 0 means no expiry.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	s.data[key] = entry{value: value, expiresAt: exp}
}

// Get returns the value for key and whether it was present (and not
// expired). An expired key reads back as absent and is lazily removed.
func (s *Store) Get(key string) ([]byte, bool) {
	e, ok := s.getLive(key)
	if !ok {
		return nil, false
	}
	return e.value, true
}

// Del removes keys and returns how many were actually present (and not
// already expired).
func (s *Store) Del(keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	n := 0
	for _, k := range keys {
		e, ok := s.data[k]
		if !ok {
			continue
		}
		delete(s.data, k)
		if !e.expired(now) {
			n++
		}
	}
	return n
}

// Exists returns how many of keys are present and not expired.
func (s *Store) Exists(keys ...string) int {
	n := 0
	for _, k := range keys {
		if _, ok := s.getLive(k); ok {
			n++
		}
	}
	return n
}

// Expire sets key to expire after ttl from now and reports whether key
// existed. A ttl <= 0 is honored exactly as given - the key becomes
// immediately expired (and will read back as absent), matching Redis's
// EXPIRE-with-non-positive-seconds behavior, but Expire itself still
// reports true because the key existed at the time of the call.
func (s *Store) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	if !ok || e.expired(time.Now()) {
		return false
	}
	e.expiresAt = time.Now().Add(ttl)
	s.data[key] = e
	return true
}

// TTLNoExpiry and TTLNoKey are the sentinel seconds values TTL returns for
// "key exists but has no expiry" and "key does not exist", mirroring real
// Redis's TTL command (-1 and -2 respectively).
const (
	TTLNoExpiry int64 = -1
	TTLNoKey    int64 = -2
)

// TTL returns the remaining time to live for key in whole seconds
// (rounded up), or TTLNoExpiry / TTLNoKey per the sentinels above.
func (s *Store) TTL(key string) int64 {
	e, ok := s.getLive(key)
	if !ok {
		return TTLNoKey
	}
	if e.expiresAt.IsZero() {
		return TTLNoExpiry
	}
	remaining := e.expiresAt.Sub(time.Now())
	if remaining <= 0 {
		// Expired between getLive's check and here; treat as gone.
		return TTLNoKey
	}
	secs := int64(remaining / time.Second)
	if remaining%time.Second != 0 {
		secs++ // round up, so a key doesn't read TTL 0 while still technically alive
	}
	return secs
}

// Incr parses the value at key as a base-10 integer (treating a missing
// key as 0), adds one, stores the result back as a string, and returns
// the new value. An existing TTL on the key is preserved.
func (s *Store) Incr(key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.data[key]
	now := time.Now()
	if ok && e.expired(now) {
		ok = false
	}

	var n int64
	if ok {
		parsed, err := strconv.ParseInt(string(e.value), 10, 64)
		if err != nil {
			return 0, ErrNotInteger
		}
		n = parsed
	}
	n++

	next := entry{value: []byte(strconv.FormatInt(n, 10))}
	if ok {
		next.expiresAt = e.expiresAt
	}
	s.data[key] = next
	return n, nil
}

// Len reports the number of keys currently stored, including any that
// have expired but not yet been swept - mainly useful for tests and
// metrics, not for correctness-sensitive code.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// getLive returns key's entry if present and not expired. If it's present
// but expired, it is lazily deleted and reported absent.
func (s *Store) getLive(key string) (entry, bool) {
	s.mu.RLock()
	e, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return entry{}, false
	}
	if !e.expired(time.Now()) {
		return e, true
	}

	// Expired: drop it under a write lock, re-checking in case a
	// concurrent Set already replaced it since the RLock above.
	s.mu.Lock()
	if cur, ok := s.data[key]; ok && cur.expired(time.Now()) {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return entry{}, false
}

// RunSweeper periodically scans the whole store removing expired keys
// that lazy expiry alone would otherwise leave sitting in memory forever
// (a key nobody ever reads again). It blocks until ctx is canceled, so
// callers run it in its own goroutine and cancel ctx for shutdown.
func (s *Store) RunSweeper(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, e := range s.data {
		if e.expired(now) {
			delete(s.data, k)
		}
	}
}

package store

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"
)

func TestSetGet(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  []byte
	}{
		{"simple value", "k1", []byte("hello")},
		{"empty value", "k2", []byte("")},
		{"binary-ish value", "k3", []byte{0x00, 0x01, 0xff}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			s.Set(tt.key, tt.val, 0)
			got, ok := s.Get(tt.key)
			if !ok {
				t.Fatalf("Get(%q) ok = false, want true", tt.key)
			}
			if !bytes.Equal(got, tt.val) {
				t.Errorf("Get(%q) = %q, want %q", tt.key, got, tt.val)
			}
		})
	}
}

func TestGetMissingKey(t *testing.T) {
	s := New()
	if _, ok := s.Get("nope"); ok {
		t.Error("Get() on missing key: ok = true, want false")
	}
}

func TestSetOverwrites(t *testing.T) {
	s := New()
	s.Set("k", []byte("first"), 0)
	s.Set("k", []byte("second"), 0)
	got, ok := s.Get("k")
	if !ok || string(got) != "second" {
		t.Errorf("Get() = %q, %v, want %q, true", got, ok, "second")
	}
}

func TestExpiryPastTTLMakesKeyUnreadable(t *testing.T) {
	s := New()
	s.Set("k", []byte("v"), 10*time.Millisecond)
	if _, ok := s.Get("k"); !ok {
		t.Fatal("Get() immediately after Set with TTL: expected key present")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("k"); ok {
		t.Error("Get() after TTL elapsed: expected key absent")
	}
}

func TestZeroTTLMeansNoExpiry(t *testing.T) {
	s := New()
	s.Set("k", []byte("v"), 0)
	time.Sleep(10 * time.Millisecond)
	if _, ok := s.Get("k"); !ok {
		t.Error("Get() with ttl=0: expected key to never expire")
	}
}

func TestDel(t *testing.T) {
	tests := []struct {
		name    string
		seed    []string
		delKeys []string
		wantN   int
	}{
		{"delete one present key", []string{"a"}, []string{"a"}, 1},
		{"delete one missing key", []string{"a"}, []string{"b"}, 0},
		{"delete mixed present and missing", []string{"a", "b"}, []string{"a", "b", "c"}, 2},
		{"delete none", []string{"a"}, []string{}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			for _, k := range tt.seed {
				s.Set(k, []byte("v"), 0)
			}
			got := s.Del(tt.delKeys...)
			if got != tt.wantN {
				t.Errorf("Del(%v) = %d, want %d", tt.delKeys, got, tt.wantN)
			}
			for _, k := range tt.delKeys {
				if _, ok := s.Get(k); ok {
					t.Errorf("Get(%q) after Del: expected absent", k)
				}
			}
		})
	}
}

func TestDelDoesNotCountExpiredKeys(t *testing.T) {
	s := New()
	s.Set("k", []byte("v"), 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if got := s.Del("k"); got != 0 {
		t.Errorf("Del() on an already-expired key = %d, want 0", got)
	}
}

func TestExists(t *testing.T) {
	tests := []struct {
		name  string
		seed  []string
		query []string
		want  int
	}{
		{"all present", []string{"a", "b"}, []string{"a", "b"}, 2},
		{"none present", []string{"a"}, []string{"x", "y"}, 0},
		{"duplicate query key counts twice", []string{"a"}, []string{"a", "a"}, 2},
		{"mixed", []string{"a", "b"}, []string{"a", "x"}, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			for _, k := range tt.seed {
				s.Set(k, []byte("v"), 0)
			}
			if got := s.Exists(tt.query...); got != tt.want {
				t.Errorf("Exists(%v) = %d, want %d", tt.query, got, tt.want)
			}
		})
	}
}

func TestExpire(t *testing.T) {
	s := New()
	s.Set("k", []byte("v"), 0)

	if ok := s.Expire("missing", time.Second); ok {
		t.Error("Expire() on missing key: ok = true, want false")
	}

	if ok := s.Expire("k", 10*time.Millisecond); !ok {
		t.Fatal("Expire() on present key: ok = false, want true")
	}
	if _, ok := s.Get("k"); !ok {
		t.Fatal("Get() immediately after Expire: expected key still present")
	}
	time.Sleep(30 * time.Millisecond)
	if _, ok := s.Get("k"); ok {
		t.Error("Get() after Expire's TTL elapsed: expected key absent")
	}
}

func TestExpireWithNonPositiveTTLExpiresImmediately(t *testing.T) {
	s := New()
	s.Set("k", []byte("v"), 0)
	if ok := s.Expire("k", -1*time.Second); !ok {
		t.Fatal("Expire() with negative ttl on present key: ok = false, want true")
	}
	if _, ok := s.Get("k"); ok {
		t.Error("Get() after Expire with negative ttl: expected key immediately absent")
	}
}

func TestTTL(t *testing.T) {
	s := New()
	s.Set("no-expiry", []byte("v"), 0)
	s.Set("with-expiry", []byte("v"), time.Minute)

	if got := s.TTL("missing"); got != TTLNoKey {
		t.Errorf("TTL(missing) = %d, want %d", got, TTLNoKey)
	}
	if got := s.TTL("no-expiry"); got != TTLNoExpiry {
		t.Errorf("TTL(no-expiry) = %d, want %d", got, TTLNoExpiry)
	}
	if got := s.TTL("with-expiry"); got <= 0 || got > 60 {
		t.Errorf("TTL(with-expiry) = %d, want a value in (0, 60]", got)
	}
}

func TestTTLOnExpiredKeyIsNoKey(t *testing.T) {
	s := New()
	s.Set("k", []byte("v"), 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if got := s.TTL("k"); got != TTLNoKey {
		t.Errorf("TTL() on expired key = %d, want %d", got, TTLNoKey)
	}
}

func TestIncr(t *testing.T) {
	tests := []struct {
		name    string
		seed    []byte
		seeded  bool
		want    int64
		wantErr bool
	}{
		{"missing key starts at 1", nil, false, 1, false},
		{"increments existing integer", []byte("41"), true, 42, false},
		{"increments negative integer", []byte("-5"), true, -4, false},
		{"non-integer value errors", []byte("not-a-number"), true, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.seeded {
				s.Set("k", tt.seed, 0)
			}
			got, err := s.Incr("k")
			if tt.wantErr {
				if err == nil {
					t.Fatal("Incr() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Incr() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("Incr() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestIncrPreservesExistingTTL(t *testing.T) {
	s := New()
	s.Set("k", []byte("1"), time.Minute)
	if _, err := s.Incr("k"); err != nil {
		t.Fatalf("Incr() error = %v", err)
	}
	if ttl := s.TTL("k"); ttl <= 0 {
		t.Errorf("TTL() after Incr = %d, want a positive remaining TTL", ttl)
	}
}

func TestIncrOnExpiredKeyStartsFresh(t *testing.T) {
	s := New()
	s.Set("k", []byte("100"), 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	got, err := s.Incr("k")
	if err != nil {
		t.Fatalf("Incr() error = %v", err)
	}
	if got != 1 {
		t.Errorf("Incr() on expired key = %d, want 1", got)
	}
}

func TestSweeperRemovesExpiredKeys(t *testing.T) {
	s := New()
	s.Set("k", []byte("v"), 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.RunSweeper(ctx, 5*time.Millisecond)

	deadline := time.Now().Add(time.Second)
	for s.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := s.Len(); got != 0 {
		t.Errorf("Len() after sweeper had time to run = %d, want 0", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	const goroutines = 50
	const opsPerGoroutine = 200

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "k"
			for j := 0; j < opsPerGoroutine; j++ {
				s.Set(key, []byte("v"), time.Hour)
				s.Get(key)
				s.Exists(key)
				s.TTL(key)
				s.Expire(key, time.Hour)
				if _, err := s.Incr("counter"); err != nil {
					t.Errorf("Incr() error = %v", err)
				}
				s.Del("throwaway")
			}
		}(i)
	}
	wg.Wait()

	got, err := s.Incr("counter")
	if err != nil {
		t.Fatalf("final Incr() error = %v", err)
	}
	want := int64(goroutines*opsPerGoroutine + 1)
	if got != want {
		t.Errorf("final counter = %d, want %d", got, want)
	}
}

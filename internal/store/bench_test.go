package store

import (
	"fmt"
	"testing"
)

// BenchmarkSet measures Set throughput for a single, fixed key - the
// worst case for lock contention under -cpu with multiple goroutines,
// since every goroutine fights over the same map bucket.
func BenchmarkSet(b *testing.B) {
	s := New()
	val := []byte("benchmark-value")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Set("benchkey", val, 0)
	}
}

// BenchmarkSetParallel measures Set throughput under concurrent access,
// which is the shape that matters for a server handling many connections.
func BenchmarkSetParallel(b *testing.B) {
	s := New()
	val := []byte("benchmark-value")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Set("benchkey", val, 0)
		}
	})
}

// BenchmarkGet measures Get throughput for a key that is always present.
func BenchmarkGet(b *testing.B) {
	s := New()
	s.Set("benchkey", []byte("benchmark-value"), 0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Get("benchkey")
	}
}

// BenchmarkGetParallel measures Get throughput under concurrent access -
// Get only takes the RWMutex's read lock in the common (non-expired) path,
// so this should scale far better than BenchmarkSetParallel.
func BenchmarkGetParallel(b *testing.B) {
	s := New()
	s.Set("benchkey", []byte("benchmark-value"), 0)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Get("benchkey")
		}
	})
}

// BenchmarkGetMiss measures Get throughput for a key that's never present,
// the other common real-world path alongside a hit.
func BenchmarkGetMiss(b *testing.B) {
	s := New()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Get("nosuchkey")
	}
}

// BenchmarkSetManyKeys exercises Set across a spread of keys rather than
// one hot key, which is closer to a real workload's map growth pattern.
func BenchmarkSetManyKeys(b *testing.B) {
	s := New()
	val := []byte("benchmark-value")
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%d", i)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Set(keys[i%len(keys)], val, 0)
	}
}

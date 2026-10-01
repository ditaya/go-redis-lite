package aof

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"go-redis-lite/internal/command"
	"go-redis-lite/internal/store"
)

func tempAOFPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.aof")
}

func TestAppendAndReplay(t *testing.T) {
	path := tempAOFPath(t)

	a, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	commands := [][][]byte{
		{[]byte("SET"), []byte("a"), []byte("1")},
		{[]byte("SET"), []byte("b"), []byte("2")},
		{[]byte("DEL"), []byte("a")},
		{[]byte("INCR"), []byte("counter")},
		{[]byte("INCR"), []byte("counter")},
	}
	for _, c := range commands {
		if err := a.Append(c); err != nil {
			t.Fatalf("Append(%v) error = %v", c, err)
		}
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	s := store.New()
	err = Replay(path, func(args [][]byte) error {
		command.Dispatch(s, args)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}

	if _, ok := s.Get("a"); ok {
		t.Error(`Get("a") after replay: expected absent (was deleted)`)
	}
	if v, ok := s.Get("b"); !ok || string(v) != "2" {
		t.Errorf(`Get("b") after replay = %q, %v, want "2", true`, v, ok)
	}
	if v, ok := s.Get("counter"); !ok || string(v) != "2" {
		t.Errorf(`Get("counter") after replay = %q, %v, want "2", true`, v, ok)
	}
}

func TestReplayMissingFileIsNotAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "does-not-exist.aof")
	called := false
	err := Replay(path, func(args [][]byte) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("Replay() on missing file: error = %v, want nil", err)
	}
	if called {
		t.Error("Replay() on missing file: apply was called, want not called")
	}
}

func TestReplayPropagatesApplyError(t *testing.T) {
	path := tempAOFPath(t)
	a, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := a.Append([][]byte{[]byte("SET"), []byte("a"), []byte("1")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wantErr := errors.New("boom")
	err = Replay(path, func(args [][]byte) error { return wantErr })
	if err == nil || !errors.Is(err, wantErr) {
		t.Errorf("Replay() error = %v, want it to wrap %v", err, wantErr)
	}
}

func TestReplayRejectsMalformedEntry(t *testing.T) {
	path := tempAOFPath(t)
	// Write something that isn't a valid RESP array of bulk strings - a
	// bare integer, which StringArgs rejects.
	if err := os.WriteFile(path, []byte(":1\r\n"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	err := Replay(path, func(args [][]byte) error { return nil })
	if err == nil {
		t.Error("Replay() on malformed entry: expected error, got nil")
	}
}

func TestAppendIsPersistedAcrossReopen(t *testing.T) {
	path := tempAOFPath(t)

	a1, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := a1.Append([][]byte{[]byte("SET"), []byte("k"), []byte("v1")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := a1.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	a2, err := Open(path) // reopen in append mode, like a server restart
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	if err := a2.Append([][]byte{[]byte("SET"), []byte("k"), []byte("v2")}); err != nil {
		t.Fatalf("second Append() error = %v", err)
	}
	if err := a2.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	s := store.New()
	if err := Replay(path, func(args [][]byte) error {
		command.Dispatch(s, args)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if v, ok := s.Get("k"); !ok || string(v) != "v2" {
		t.Errorf(`Get("k") after two Open/Append/Close cycles = %q, %v, want "v2", true`, v, ok)
	}
}

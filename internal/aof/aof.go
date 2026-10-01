// Package aof implements append-only-file persistence: every write command
// is logged to disk in RESP wire format (the same format a client sends it
// in), and the file is replayed command-by-command on startup to rebuild
// the store's state.
package aof

import (
	"fmt"
	"io"
	"os"
	"sync"

	"go-redis-lite/internal/resp"
)

// AOF appends write commands to a file on disk, one RESP command array per
// call, flushing (and optionally fsyncing) each one so a command is
// durable before its reply goes back to the client.
type AOF struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens (creating if necessary) the AOF file at path for appending.
func Open(path string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("aof: open %s: %w", path, err)
	}
	return &AOF{file: f}, nil
}

// Append writes one command (its name followed by its arguments) to the
// log as a RESP array of bulk strings, then fsyncs so the write survives a
// crash immediately after the call returns.
func (a *AOF) Append(args [][]byte) error {
	vals := make([]resp.Value, len(args))
	for i, arg := range args {
		vals[i] = resp.BulkString(arg)
	}
	encoded := resp.Array(vals...).Encode(nil)

	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.file.Write(encoded); err != nil {
		return fmt.Errorf("aof: write: %w", err)
	}
	return a.file.Sync()
}

// Close closes the underlying file.
func (a *AOF) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}

// Replay reads every command logged at path, in order, and calls apply
// with its args (command name followed by arguments, as resp.Value.StringArgs
// would return it). apply's errors are not fatal to the replay - command.Dispatch
// already turns bad input into a RESP error rather than an error return,
// so apply is expected to only ever report truly unexpected failures;
// Replay stops and returns on the first one.
//
// A missing file is not an error - it just means there's nothing to
// replay yet (first run).
func Replay(path string, apply func(args [][]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("aof: open %s for replay: %w", path, err)
	}
	defer f.Close()

	r := resp.NewReader(f)
	for {
		v, err := r.ReadValue()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("aof: replay %s: %w", path, err)
		}
		args, err := v.StringArgs()
		if err != nil {
			return fmt.Errorf("aof: replay %s: malformed entry: %w", path, err)
		}
		if err := apply(args); err != nil {
			return fmt.Errorf("aof: replay %s: %w", path, err)
		}
	}
}

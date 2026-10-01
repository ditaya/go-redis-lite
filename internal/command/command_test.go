package command

import (
	"strings"
	"testing"
	"time"

	"go-redis-lite/internal/resp"
	"go-redis-lite/internal/store"
)

func args(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

// wantValue describes the subset of a resp.Value this test cares about,
// since none of our commands produce arrays.
type wantValue struct {
	typ      resp.Type
	str      string // for SimpleString/Error (Error is matched as a substring via strings.Contains)
	int      int64
	bulk     string
	nullBulk bool
}

func check(t *testing.T, got resp.Value, want wantValue) {
	t.Helper()
	if got.Type != want.typ {
		t.Fatalf("Type = %q, want %q (value: %+v)", got.Type, want.typ, got)
	}
	switch want.typ {
	case resp.TypeSimpleString:
		if got.Str != want.str {
			t.Errorf("Str = %q, want %q", got.Str, want.str)
		}
	case resp.TypeError:
		if !strings.Contains(got.Str, want.str) {
			t.Errorf("Error = %q, want it to contain %q", got.Str, want.str)
		}
	case resp.TypeInteger:
		if got.Int != want.int {
			t.Errorf("Int = %d, want %d", got.Int, want.int)
		}
	case resp.TypeBulkString:
		if got.Null != want.nullBulk {
			t.Errorf("Null = %v, want %v", got.Null, want.nullBulk)
		}
		if !want.nullBulk && string(got.Bulk) != want.bulk {
			t.Errorf("Bulk = %q, want %q", got.Bulk, want.bulk)
		}
	}
}

func TestDispatchPing(t *testing.T) {
	s := store.New()
	tests := []struct {
		name string
		args [][]byte
		want wantValue
	}{
		{"no message", args(), wantValue{typ: resp.TypeSimpleString, str: "PONG"}},
		{"with message", args("hello"), wantValue{typ: resp.TypeBulkString, bulk: "hello"}},
		{"too many args", args("a", "b"), wantValue{typ: resp.TypeError, str: "wrong number of arguments"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			check(t, Dispatch(s, append([][]byte{[]byte("PING")}, tt.args...)), tt.want)
		})
	}
}

func TestDispatchSetGet(t *testing.T) {
	tests := []struct {
		name    string
		setArgs [][]byte
		wantSet wantValue
		getKey  string
		wantGet wantValue
	}{
		{
			name:    "plain set and get",
			setArgs: args("k", "v"),
			wantSet: wantValue{typ: resp.TypeSimpleString, str: "OK"},
			getKey:  "k",
			wantGet: wantValue{typ: resp.TypeBulkString, bulk: "v"},
		},
		{
			name:    "set with EX",
			setArgs: args("k", "v", "EX", "100"),
			wantSet: wantValue{typ: resp.TypeSimpleString, str: "OK"},
			getKey:  "k",
			wantGet: wantValue{typ: resp.TypeBulkString, bulk: "v"},
		},
		{
			name:    "set with lowercase ex",
			setArgs: args("k", "v", "ex", "100"),
			wantSet: wantValue{typ: resp.TypeSimpleString, str: "OK"},
			getKey:  "k",
			wantGet: wantValue{typ: resp.TypeBulkString, bulk: "v"},
		},
		{
			name:    "set with invalid EX seconds errors",
			setArgs: args("k", "v", "EX", "notanumber"),
			wantSet: wantValue{typ: resp.TypeError, str: "invalid expire time"},
			getKey:  "k",
			wantGet: wantValue{typ: resp.TypeBulkString, nullBulk: true},
		},
		{
			name:    "set with zero EX errors",
			setArgs: args("k", "v", "EX", "0"),
			wantSet: wantValue{typ: resp.TypeError, str: "invalid expire time"},
			getKey:  "k",
			wantGet: wantValue{typ: resp.TypeBulkString, nullBulk: true},
		},
		{
			name:    "set with unknown option errors",
			setArgs: args("k", "v", "NX"),
			wantSet: wantValue{typ: resp.TypeError, str: "syntax error"},
			getKey:  "k",
			wantGet: wantValue{typ: resp.TypeBulkString, nullBulk: true},
		},
		{
			name:    "too few args",
			setArgs: args("k"),
			wantSet: wantValue{typ: resp.TypeError, str: "wrong number of arguments"},
			getKey:  "k",
			wantGet: wantValue{typ: resp.TypeBulkString, nullBulk: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := store.New()
			check(t, Dispatch(s, append([][]byte{[]byte("SET")}, tt.setArgs...)), tt.wantSet)
			check(t, Dispatch(s, [][]byte{[]byte("GET"), []byte(tt.getKey)}), tt.wantGet)
		})
	}
}

func TestDispatchGetMissingKey(t *testing.T) {
	s := store.New()
	check(t, Dispatch(s, args("GET", "nope")), wantValue{typ: resp.TypeBulkString, nullBulk: true})
}

func TestDispatchSetExpiresKey(t *testing.T) {
	s := store.New()
	Dispatch(s, args("SET", "k", "v", "EX", "1"))
	// Simulate elapsed time by setting a very short TTL instead of sleeping
	// a full second: set again with a TTL we can wait out quickly.
	s.Set("k2", []byte("v"), 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	check(t, Dispatch(s, args("GET", "k2")), wantValue{typ: resp.TypeBulkString, nullBulk: true})
}

func TestDispatchDel(t *testing.T) {
	s := store.New()
	Dispatch(s, args("SET", "a", "1"))
	Dispatch(s, args("SET", "b", "1"))

	check(t, Dispatch(s, args("DEL", "a", "b", "c")), wantValue{typ: resp.TypeInteger, int: 2})
	check(t, Dispatch(s, args("DEL")), wantValue{typ: resp.TypeError, str: "wrong number of arguments"})
}

func TestDispatchExists(t *testing.T) {
	s := store.New()
	Dispatch(s, args("SET", "a", "1"))

	check(t, Dispatch(s, args("EXISTS", "a", "a", "x")), wantValue{typ: resp.TypeInteger, int: 2})
	check(t, Dispatch(s, args("EXISTS")), wantValue{typ: resp.TypeError, str: "wrong number of arguments"})
}

func TestDispatchExpireAndTTL(t *testing.T) {
	s := store.New()
	Dispatch(s, args("SET", "a", "1"))

	check(t, Dispatch(s, args("EXPIRE", "missing", "10")), wantValue{typ: resp.TypeInteger, int: 0})
	check(t, Dispatch(s, args("EXPIRE", "a", "100")), wantValue{typ: resp.TypeInteger, int: 1})

	ttlReply := Dispatch(s, args("TTL", "a"))
	if ttlReply.Type != resp.TypeInteger || ttlReply.Int <= 0 || ttlReply.Int > 100 {
		t.Errorf("TTL(a) = %+v, want an integer in (0, 100]", ttlReply)
	}

	check(t, Dispatch(s, args("TTL", "missing")), wantValue{typ: resp.TypeInteger, int: store.TTLNoKey})

	Dispatch(s, args("SET", "no-ttl", "1"))
	check(t, Dispatch(s, args("TTL", "no-ttl")), wantValue{typ: resp.TypeInteger, int: store.TTLNoExpiry})

	check(t, Dispatch(s, args("EXPIRE", "a", "not-a-number")), wantValue{typ: resp.TypeError, str: "not an integer"})
}

func TestDispatchIncr(t *testing.T) {
	s := store.New()
	check(t, Dispatch(s, args("INCR", "counter")), wantValue{typ: resp.TypeInteger, int: 1})
	check(t, Dispatch(s, args("INCR", "counter")), wantValue{typ: resp.TypeInteger, int: 2})

	Dispatch(s, args("SET", "notanumber", "abc"))
	check(t, Dispatch(s, args("INCR", "notanumber")), wantValue{typ: resp.TypeError, str: "not an integer"})
}

func TestDispatchUnknownCommand(t *testing.T) {
	s := store.New()
	check(t, Dispatch(s, args("FROBNICATE", "x")), wantValue{typ: resp.TypeError, str: "unknown command"})
}

func TestDispatchEmptyCommand(t *testing.T) {
	s := store.New()
	check(t, Dispatch(s, nil), wantValue{typ: resp.TypeError, str: "empty command"})
}

func TestDispatchCommandNameIsCaseInsensitive(t *testing.T) {
	s := store.New()
	check(t, Dispatch(s, args("set", "k", "v")), wantValue{typ: resp.TypeSimpleString, str: "OK"})
	check(t, Dispatch(s, args("get", "k")), wantValue{typ: resp.TypeBulkString, bulk: "v"})
}

func TestIsWrite(t *testing.T) {
	tests := []struct {
		cmd  string
		want bool
	}{
		{"SET", true}, {"set", true}, {"DEL", true}, {"EXPIRE", true}, {"INCR", true},
		{"GET", false}, {"EXISTS", false}, {"TTL", false}, {"PING", false}, {"UNKNOWN", false},
	}
	for _, tt := range tests {
		if got := IsWrite(tt.cmd); got != tt.want {
			t.Errorf("IsWrite(%q) = %v, want %v", tt.cmd, got, tt.want)
		}
	}
}

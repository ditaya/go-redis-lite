// Package command implements the Redis command set this server supports -
// PING, SET (with EX), GET, DEL, EXISTS, EXPIRE, TTL, and INCR - as a
// single Dispatch function over a *store.Store. Dispatch is the one place
// that understands both the wire-level args and the store's API, so the
// TCP server, the AOF replay path, and tests all go through it instead of
// duplicating command logic.
package command

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go-redis-lite/internal/resp"
	"go-redis-lite/internal/store"
)

// IsWrite reports whether cmd mutates the store and therefore needs to be
// logged to the AOF. Matching is case-insensitive, like Redis commands.
func IsWrite(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "SET", "DEL", "EXPIRE", "INCR":
		return true
	default:
		return false
	}
}

// Dispatch executes one command (args[0] is the command name, args[1:] its
// arguments - exactly what resp.Value.StringArgs returns for a client's
// command array) against s and returns the RESP reply to send back.
//
// Dispatch never panics on malformed input; every error path is reported
// as a RESP error value, matching how a real Redis server degrades on a
// single bad command without dropping the connection.
func Dispatch(s *store.Store, args [][]byte) resp.Value {
	if len(args) == 0 {
		return resp.Error("ERR empty command")
	}
	name := strings.ToUpper(string(args[0]))
	rest := args[1:]

	switch name {
	case "PING":
		return doPing(rest)
	case "SET":
		return doSet(s, rest)
	case "GET":
		return doGet(s, rest)
	case "DEL":
		return doDel(s, rest)
	case "EXISTS":
		return doExists(s, rest)
	case "EXPIRE":
		return doExpire(s, rest)
	case "TTL":
		return doTTL(s, rest)
	case "INCR":
		return doIncr(s, rest)
	default:
		return resp.Errorf("ERR unknown command %q", name)
	}
}

func wrongArgs(cmd string) resp.Value {
	return resp.Errorf("ERR wrong number of arguments for '%s' command", strings.ToLower(cmd))
}

func doPing(args [][]byte) resp.Value {
	switch len(args) {
	case 0:
		return resp.SimpleString("PONG")
	case 1:
		return resp.BulkString(args[0])
	default:
		return wrongArgs("PING")
	}
}

func doSet(s *store.Store, args [][]byte) resp.Value {
	if len(args) < 2 {
		return wrongArgs("SET")
	}
	key, value := string(args[0]), args[1]
	var ttl time.Duration

	rest := args[2:]
	for len(rest) > 0 {
		opt := strings.ToUpper(string(rest[0]))
		switch opt {
		case "EX":
			if len(rest) < 2 {
				return resp.Error("ERR syntax error")
			}
			seconds, err := strconv.ParseInt(string(rest[1]), 10, 64)
			if err != nil || seconds <= 0 {
				return resp.Error("ERR invalid expire time in 'set' command")
			}
			ttl = time.Duration(seconds) * time.Second
			rest = rest[2:]
		default:
			return resp.Error("ERR syntax error")
		}
	}

	s.Set(key, append([]byte(nil), value...), ttl)
	return resp.SimpleString("OK")
}

func doGet(s *store.Store, args [][]byte) resp.Value {
	if len(args) != 1 {
		return wrongArgs("GET")
	}
	val, ok := s.Get(string(args[0]))
	if !ok {
		return resp.NullBulk()
	}
	return resp.BulkString(val)
}

func doDel(s *store.Store, args [][]byte) resp.Value {
	if len(args) < 1 {
		return wrongArgs("DEL")
	}
	return resp.Integer(int64(s.Del(toStrings(args)...)))
}

func doExists(s *store.Store, args [][]byte) resp.Value {
	if len(args) < 1 {
		return wrongArgs("EXISTS")
	}
	return resp.Integer(int64(s.Exists(toStrings(args)...)))
}

func doExpire(s *store.Store, args [][]byte) resp.Value {
	if len(args) != 2 {
		return wrongArgs("EXPIRE")
	}
	seconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return resp.Error("ERR value is not an integer or out of range")
	}
	ttl := time.Duration(seconds) * time.Second
	if s.Expire(string(args[0]), ttl) {
		return resp.Integer(1)
	}
	return resp.Integer(0)
}

func doTTL(s *store.Store, args [][]byte) resp.Value {
	if len(args) != 1 {
		return wrongArgs("TTL")
	}
	return resp.Integer(s.TTL(string(args[0])))
}

func doIncr(s *store.Store, args [][]byte) resp.Value {
	if len(args) != 1 {
		return wrongArgs("INCR")
	}
	n, err := s.Incr(string(args[0]))
	if err != nil {
		return resp.Error(fmt.Sprintf("ERR %s", err))
	}
	return resp.Integer(n)
}

func toStrings(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

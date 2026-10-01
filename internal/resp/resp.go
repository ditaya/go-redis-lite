// Package resp implements the subset of the Redis Serialization Protocol
// (RESP2) this server needs: simple strings, errors, integers, bulk
// strings (including null), and arrays (including null) - enough to read
// a client's command and write any reply our commands produce.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// Type is the one-byte RESP type tag that starts every value on the wire.
type Type byte

const (
	TypeSimpleString Type = '+'
	TypeError        Type = '-'
	TypeInteger      Type = ':'
	TypeBulkString   Type = '$'
	TypeArray        Type = '*'
)

// Value is a RESP value. Which fields are meaningful depends on Type:
//   - TypeSimpleString, TypeError: Str
//   - TypeInteger:                 Int
//   - TypeBulkString:              Bulk (or Null for a null bulk string, "$-1\r\n")
//   - TypeArray:                   Array (or Null for a null array, "*-1\r\n")
type Value struct {
	Type  Type
	Str   string
	Int   int64
	Bulk  []byte
	Array []Value
	Null  bool
}

func SimpleString(s string) Value { return Value{Type: TypeSimpleString, Str: s} }
func Error(s string) Value        { return Value{Type: TypeError, Str: s} }
func Errorf(format string, a ...any) Value {
	return Error(fmt.Sprintf(format, a...))
}
func Integer(n int64) Value         { return Value{Type: TypeInteger, Int: n} }
func BulkString(b []byte) Value     { return Value{Type: TypeBulkString, Bulk: b} }
func BulkStringFrom(s string) Value { return BulkString([]byte(s)) }
func NullBulk() Value               { return Value{Type: TypeBulkString, Null: true} }
func Array(vals ...Value) Value     { return Value{Type: TypeArray, Array: vals} }
func NullArray() Value              { return Value{Type: TypeArray, Null: true} }

// CommandArray builds the RESP array-of-bulk-strings a client uses to send
// a command, e.g. CommandArray("SET", "k", "v") - used both by any test
// client and by the AOF writer, which logs commands in the same wire
// format they were received in.
func CommandArray(args ...string) Value {
	vals := make([]Value, len(args))
	for i, a := range args {
		vals[i] = BulkStringFrom(a)
	}
	return Array(vals...)
}

// Encode appends the RESP wire encoding of v to buf and returns the
// extended slice, so callers can build up a reply (or a whole pipeline of
// replies) without an intermediate allocation per value.
func (v Value) Encode(buf []byte) []byte {
	switch v.Type {
	case TypeSimpleString:
		buf = append(buf, '+')
		buf = append(buf, v.Str...)
		return append(buf, '\r', '\n')
	case TypeError:
		buf = append(buf, '-')
		buf = append(buf, v.Str...)
		return append(buf, '\r', '\n')
	case TypeInteger:
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, v.Int, 10)
		return append(buf, '\r', '\n')
	case TypeBulkString:
		if v.Null {
			return append(buf, '$', '-', '1', '\r', '\n')
		}
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(v.Bulk)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, v.Bulk...)
		return append(buf, '\r', '\n')
	case TypeArray:
		if v.Null {
			return append(buf, '*', '-', '1', '\r', '\n')
		}
		buf = append(buf, '*')
		buf = strconv.AppendInt(buf, int64(len(v.Array)), 10)
		buf = append(buf, '\r', '\n')
		for _, e := range v.Array {
			buf = e.Encode(buf)
		}
		return buf
	default:
		return append(buf, '-', 'E', 'R', 'R', ' ', 'i', 'n', 't', 'e', 'r', 'n', 'a', 'l', ':', ' ',
			'u', 'n', 'k', 'n', 'o', 'w', 'n', ' ', 'R', 'E', 'S', 'P', ' ', 't', 'y', 'p', 'e', '\r', '\n')
	}
}

// Write encodes v and writes it to w.
func Write(w io.Writer, v Value) error {
	_, err := w.Write(v.Encode(nil))
	return err
}

// StringArgs extracts a command's arguments from a RESP array of bulk
// strings - the shape every real client (redis-cli, redis-benchmark,
// go-redis) sends a command as. It's an error for v not to be an array of
// (non-null) bulk strings.
func (v Value) StringArgs() ([][]byte, error) {
	if v.Type != TypeArray || v.Null {
		return nil, fmt.Errorf("resp: expected an array, got %v", v.Type)
	}
	args := make([][]byte, len(v.Array))
	for i, e := range v.Array {
		if e.Type != TypeBulkString || e.Null {
			return nil, fmt.Errorf("resp: array element %d is not a bulk string", i)
		}
		args[i] = e.Bulk
	}
	return args, nil
}

// ErrProtocol is wrapped by parse errors caused by malformed input (as
// opposed to a plain I/O error like EOF or a closed connection).
var ErrProtocol = errors.New("resp: protocol error")

// Reader parses a stream of RESP values from an underlying io.Reader.
type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReader(r)}
}

// ReadValue parses and returns the next RESP value from the stream. Arrays
// are parsed recursively (each element may itself be any RESP value,
// though in practice every command is a flat array of bulk strings).
func (r *Reader) ReadValue() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, fmt.Errorf("%w: empty line", ErrProtocol)
	}

	typ := Type(line[0])
	payload := line[1:]

	switch typ {
	case TypeSimpleString:
		return SimpleString(string(payload)), nil

	case TypeError:
		return Error(string(payload)), nil

	case TypeInteger:
		n, err := parseInt(payload)
		if err != nil {
			return Value{}, fmt.Errorf("%w: invalid integer: %v", ErrProtocol, err)
		}
		return Integer(n), nil

	case TypeBulkString:
		n, err := parseInt(payload)
		if err != nil {
			return Value{}, fmt.Errorf("%w: invalid bulk string length: %v", ErrProtocol, err)
		}
		if n < 0 {
			return NullBulk(), nil
		}
		buf := make([]byte, n+2) // payload plus the trailing CRLF
		if _, err := io.ReadFull(r.r, buf); err != nil {
			return Value{}, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return Value{}, fmt.Errorf("%w: bulk string missing CRLF terminator", ErrProtocol)
		}
		return BulkString(buf[:n:n]), nil

	case TypeArray:
		n, err := parseInt(payload)
		if err != nil {
			return Value{}, fmt.Errorf("%w: invalid array length: %v", ErrProtocol, err)
		}
		if n < 0 {
			return NullArray(), nil
		}
		vals := make([]Value, n)
		for i := range vals {
			v, err := r.ReadValue()
			if err != nil {
				return Value{}, err
			}
			vals[i] = v
		}
		return Array(vals...), nil

	default:
		return Value{}, fmt.Errorf("%w: unknown type byte %q", ErrProtocol, line[0])
	}
}

// readLine reads up to and including a CRLF (or a bare LF) and returns the
// line with the terminator stripped.
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = line[:len(line)-1] // drop '\n'
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	return []byte(line), nil
}

func parseInt(b []byte) (int64, error) {
	return strconv.ParseInt(string(b), 10, 64)
}

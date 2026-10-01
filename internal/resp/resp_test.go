package resp

import (
	"bytes"
	"strings"
	"testing"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		name string
		v    Value
		want string
	}{
		{"simple string", SimpleString("OK"), "+OK\r\n"},
		{"error", Error("ERR boom"), "-ERR boom\r\n"},
		{"positive integer", Integer(42), ":42\r\n"},
		{"negative integer", Integer(-2), ":-2\r\n"},
		{"zero integer", Integer(0), ":0\r\n"},
		{"bulk string", BulkStringFrom("hello"), "$5\r\nhello\r\n"},
		{"empty bulk string", BulkStringFrom(""), "$0\r\n\r\n"},
		{"null bulk string", NullBulk(), "$-1\r\n"},
		{"empty array", Array(), "*0\r\n"},
		{"array of bulk strings", CommandArray("SET", "k", "v"), "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		{"null array", NullArray(), "*-1\r\n"},
		{"nested array", Array(Integer(1), Array(Integer(2), Integer(3))), "*2\r\n:1\r\n*2\r\n:2\r\n:3\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(tt.v.Encode(nil))
			if got != tt.want {
				t.Errorf("Encode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, SimpleString("PONG")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got, want := buf.String(), "+PONG\r\n"; got != want {
		t.Errorf("Write() wrote %q, want %q", got, want)
	}
}

func TestReadValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  Value
	}{
		{"simple string", "+OK\r\n", SimpleString("OK")},
		{"error", "-ERR boom\r\n", Error("ERR boom")},
		{"integer", ":1000\r\n", Integer(1000)},
		{"negative integer", ":-7\r\n", Integer(-7)},
		{"bulk string", "$5\r\nhello\r\n", BulkStringFrom("hello")},
		{"empty bulk string", "$0\r\n\r\n", BulkStringFrom("")},
		{"null bulk string", "$-1\r\n", NullBulk()},
		{"empty array", "*0\r\n", Array()},
		{"null array", "*-1\r\n", NullArray()},
		{
			"command array",
			"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
			CommandArray("SET", "k", "v"),
		},
		{
			"nested array",
			"*2\r\n:1\r\n*2\r\n:2\r\n:3\r\n",
			Array(Integer(1), Array(Integer(2), Integer(3))),
		},
		{
			"bare LF line ending is tolerated",
			"+OK\n",
			SimpleString("OK"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			got, err := r.ReadValue()
			if err != nil {
				t.Fatalf("ReadValue() error = %v", err)
			}
			if !valuesEqual(got, tt.want) {
				t.Errorf("ReadValue() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestReadValueErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"unknown type byte", "!nope\r\n"},
		{"bad integer payload", ":notanumber\r\n"},
		{"bulk string missing terminator", "$3\r\nabcXX"},
		{"bulk string truncated", "$5\r\nhi\r\n"},
		{"array truncated", "*2\r\n:1\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			if _, err := r.ReadValue(); err == nil {
				t.Error("ReadValue() expected an error, got nil")
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	values := []Value{
		SimpleString("PONG"),
		Error("ERR wrong number of arguments"),
		Integer(123456789),
		Integer(-1),
		BulkStringFrom("value with spaces and \"quotes\""),
		BulkStringFrom(""),
		NullBulk(),
		CommandArray("SET", "mykey", "myvalue", "EX", "10"),
		Array(),
	}

	for _, v := range values {
		encoded := v.Encode(nil)
		r := NewReader(bytes.NewReader(encoded))
		got, err := r.ReadValue()
		if err != nil {
			t.Fatalf("round trip for %+v: ReadValue() error = %v", v, err)
		}
		if !valuesEqual(got, v) {
			t.Errorf("round trip: got %+v, want %+v", got, v)
		}
	}
}

func TestStringArgs(t *testing.T) {
	v := CommandArray("SET", "k", "v")
	args, err := v.StringArgs()
	if err != nil {
		t.Fatalf("StringArgs() error = %v", err)
	}
	want := [][]byte{[]byte("SET"), []byte("k"), []byte("v")}
	if len(args) != len(want) {
		t.Fatalf("len(args) = %d, want %d", len(args), len(want))
	}
	for i := range want {
		if !bytes.Equal(args[i], want[i]) {
			t.Errorf("args[%d] = %q, want %q", i, args[i], want[i])
		}
	}
}

func TestStringArgsRejectsNonArray(t *testing.T) {
	if _, err := Integer(1).StringArgs(); err == nil {
		t.Error("StringArgs() on a non-array value: expected error, got nil")
	}
}

func TestStringArgsRejectsNestedArray(t *testing.T) {
	v := Array(Array(Integer(1)))
	if _, err := v.StringArgs(); err == nil {
		t.Error("StringArgs() with a nested array element: expected error, got nil")
	}
}

// valuesEqual compares two Values for the purposes of this test file. It
// treats nil and empty slices/arrays as equal, which reflect.DeepEqual
// would not.
func valuesEqual(a, b Value) bool {
	if a.Type != b.Type || a.Str != b.Str || a.Int != b.Int || a.Null != b.Null {
		return false
	}
	if !bytes.Equal(a.Bulk, b.Bulk) {
		return false
	}
	if len(a.Array) != len(b.Array) {
		return false
	}
	for i := range a.Array {
		if !valuesEqual(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}

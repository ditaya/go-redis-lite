package server

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go-redis-lite/internal/resp"
	"go-redis-lite/internal/store"
)

// fakePersister records every Append call instead of touching disk, so
// tests can assert which commands the server chose to log without needing
// a real AOF package test (that's covered separately in internal/aof).
type fakePersister struct {
	mu    sync.Mutex
	calls [][][]byte
}

func (f *fakePersister) Append(args [][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([][]byte, len(args))
	copy(cp, args)
	f.calls = append(f.calls, cp)
	return nil
}

func (f *fakePersister) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// testClient is a tiny RESP client over a real TCP connection, used to
// drive the server exactly like redis-cli or a real client library would.
type testClient struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
}

func newTestClient(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("Dial(%s) error = %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{t: t, conn: conn, r: resp.NewReader(conn)}
}

func (c *testClient) do(args ...string) resp.Value {
	c.t.Helper()
	if err := resp.Write(c.conn, resp.CommandArray(args...)); err != nil {
		c.t.Fatalf("write command %v: %v", args, err)
	}
	v, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatalf("read reply to %v: %v", args, err)
	}
	return v
}

func startTestServer(t *testing.T, aof Persister) (addr string, done chan struct{}) {
	t.Helper()
	srv := New(store.New(), aof)
	if err := srv.Listen(":0"); err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	done = make(chan struct{})
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			close(done)
		}
		if err := <-serveErr; err != nil {
			t.Errorf("Serve() returned error = %v", err)
		}
	})
	return srv.Addr(), done
}

func TestIntegrationCommandsOverTCP(t *testing.T) {
	addr, _ := startTestServer(t, nil)
	c := newTestClient(t, addr)

	tests := []struct {
		name     string
		args     []string
		wantType resp.Type
		wantStr  string
		wantInt  int64
		wantBulk string
		wantNull bool
	}{
		{"ping", []string{"PING"}, resp.TypeSimpleString, "PONG", 0, "", false},
		{"set", []string{"SET", "k", "hello"}, resp.TypeSimpleString, "OK", 0, "", false},
		{"get hit", []string{"GET", "k"}, resp.TypeBulkString, "", 0, "hello", false},
		{"get miss", []string{"GET", "nope"}, resp.TypeBulkString, "", 0, "", true},
		{"set with ex", []string{"SET", "k2", "v2", "EX", "100"}, resp.TypeSimpleString, "OK", 0, "", false},
		{"incr new", []string{"INCR", "counter"}, resp.TypeInteger, "", 1, "", false},
		{"incr again", []string{"INCR", "counter"}, resp.TypeInteger, "", 2, "", false},
		{"exists", []string{"EXISTS", "k", "k2", "nope"}, resp.TypeInteger, "", 2, "", false},
		{"expire missing", []string{"EXPIRE", "nope", "10"}, resp.TypeInteger, "", 0, "", false},
		{"expire present", []string{"EXPIRE", "k", "10"}, resp.TypeInteger, "", 1, "", false},
		{"del", []string{"DEL", "k", "k2", "counter"}, resp.TypeInteger, "", 3, "", false},
		{"get after del", []string{"GET", "k"}, resp.TypeBulkString, "", 0, "", true},
		{"unknown command", []string{"FROBNICATE"}, resp.TypeError, "", 0, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.do(tt.args...)
			if got.Type != tt.wantType {
				t.Fatalf("%v: Type = %q, want %q (value: %+v)", tt.args, got.Type, tt.wantType, got)
			}
			switch tt.wantType {
			case resp.TypeSimpleString:
				if got.Str != tt.wantStr {
					t.Errorf("%v: Str = %q, want %q", tt.args, got.Str, tt.wantStr)
				}
			case resp.TypeInteger:
				if got.Int != tt.wantInt {
					t.Errorf("%v: Int = %d, want %d", tt.args, got.Int, tt.wantInt)
				}
			case resp.TypeBulkString:
				if got.Null != tt.wantNull {
					t.Errorf("%v: Null = %v, want %v", tt.args, got.Null, tt.wantNull)
				}
				if !tt.wantNull && string(got.Bulk) != tt.wantBulk {
					t.Errorf("%v: Bulk = %q, want %q", tt.args, got.Bulk, tt.wantBulk)
				}
			}
		})
	}
}

func TestIntegrationTTLReflectsExpiry(t *testing.T) {
	addr, _ := startTestServer(t, nil)
	c := newTestClient(t, addr)

	if got := c.do("SET", "k", "v", "EX", "100"); got.Type != resp.TypeSimpleString {
		t.Fatalf("SET: unexpected reply %+v", got)
	}
	ttl := c.do("TTL", "k")
	if ttl.Type != resp.TypeInteger || ttl.Int <= 0 || ttl.Int > 100 {
		t.Errorf("TTL(k) = %+v, want an integer in (0, 100]", ttl)
	}

	noExpiry := c.do("TTL", "nonexistent")
	if noExpiry.Type != resp.TypeInteger || noExpiry.Int != -2 {
		t.Errorf("TTL(nonexistent) = %+v, want -2", noExpiry)
	}
}

func TestIntegrationAOFLogsOnlySuccessfulWrites(t *testing.T) {
	fake := &fakePersister{}
	addr, _ := startTestServer(t, fake)
	c := newTestClient(t, addr)

	c.do("PING")                        // read-only, not logged
	c.do("SET", "k", "v")               // write, logged
	c.do("GET", "k")                    // read-only, not logged
	c.do("SET", "k", "v", "EX", "oops") // write that errors, not logged
	c.do("INCR", "k")                   // write that errors (k is not an int), not logged
	c.do("DEL", "k")                    // write, logged

	if got, want := fake.callCount(), 2; got != want {
		t.Fatalf("AOF Append call count = %d, want %d (calls: %v)", got, want, fake.calls)
	}
	if string(fake.calls[0][0]) != "SET" {
		t.Errorf("first logged command = %q, want SET", fake.calls[0][0])
	}
	if string(fake.calls[1][0]) != "DEL" {
		t.Errorf("second logged command = %q, want DEL", fake.calls[1][0])
	}
}

func TestIntegrationConcurrentClients(t *testing.T) {
	addr, _ := startTestServer(t, nil)

	const clients = 20
	const opsPerClient = 50
	var wg sync.WaitGroup
	errCh := make(chan error, clients)

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := newTestClient(t, addr)
			key := fmt.Sprintf("key:%d", id)
			for j := 0; j < opsPerClient; j++ {
				if reply := c.do("INCR", key); reply.Type != resp.TypeInteger {
					errCh <- fmt.Errorf("client %d: INCR reply = %+v, want integer", id, reply)
					return
				}
			}
			final := c.do("GET", key)
			if final.Type != resp.TypeBulkString || string(final.Bulk) != fmt.Sprintf("%d", opsPerClient) {
				errCh <- fmt.Errorf("client %d: final GET = %+v, want %q", id, final, fmt.Sprintf("%d", opsPerClient))
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

func TestIntegrationGracefulShutdown(t *testing.T) {
	srv := New(store.New(), nil)
	if err := srv.Listen(":0"); err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	addr := srv.Addr()
	done := make(chan struct{})
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(done) }()

	// A connection made before shutdown should still complete its request.
	c := newTestClient(t, addr)
	if got := c.do("PING"); got.Type != resp.TypeSimpleString {
		t.Fatalf("PING before shutdown: unexpected reply %+v", got)
	}

	close(done)

	select {
	case err := <-serveErr:
		if err != nil {
			t.Errorf("Serve() after shutdown returned error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not return within 2s of done being closed")
	}

	if _, err := net.DialTimeout("tcp", addr, 500*time.Millisecond); err == nil {
		t.Error("Dial() after shutdown: expected connection refused, got a successful connection")
	}
}

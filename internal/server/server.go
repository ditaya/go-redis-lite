// Package server implements the TCP front end: one goroutine per
// connection, each reading RESP commands off the wire, dispatching them
// against the shared store, optionally logging write commands to the AOF,
// and writing the RESP reply back.
package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"go-redis-lite/internal/command"
	"go-redis-lite/internal/resp"
	"go-redis-lite/internal/store"
)

// Persister is the one method Server needs from the AOF writer - narrowed
// to an interface so tests can run a Server with persistence disabled (a
// nil Persister) or swap in a fake.
type Persister interface {
	Append(args [][]byte) error
}

// Server is a Redis-compatible TCP server. The zero value is not usable;
// construct one with New.
type Server struct {
	store *store.Store
	aof   Persister // nil disables AOF logging

	listener net.Listener
	wg       sync.WaitGroup

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

// New builds a Server over s. aof may be nil to disable persistence.
func New(s *store.Store, aof Persister) *Server {
	return &Server{store: s, aof: aof, conns: make(map[net.Conn]struct{})}
}

// Listen binds addr (e.g. ":6380", or ":0" to let the OS pick a free
// port). Call Addr afterward to find out what port was actually bound.
func (srv *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("server: listen on %s: %w", addr, err)
	}
	srv.listener = ln
	return nil
}

// Addr returns the address Listen actually bound to. Valid only after a
// successful Listen call.
func (srv *Server) Addr() string {
	return srv.listener.Addr().String()
}

// Serve accepts connections until done is closed, spawning one goroutine
// per connection, and returns once every in-flight connection has
// finished - the caller can rely on Serve's return as the signal that
// shutdown is complete. Must be called after a successful Listen.
//
// Shutdown stops accepting new connections and force-closes every
// currently open one, rather than waiting for them to disconnect on their
// own: a connection can sit idle indefinitely (nothing wrong with that
// during normal operation), and graceful shutdown should complete in
// bounded time regardless of what clients are doing when it's triggered.
// Any command genuinely in flight still gets its reply written first -
// handleConn only observes the close on its next blocking read.
func (srv *Server) Serve(done <-chan struct{}) error {
	go func() {
		<-done
		srv.listener.Close()
		srv.closeAllConns()
	}()

	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			select {
			case <-done:
				srv.wg.Wait()
				return nil
			default:
				return fmt.Errorf("server: accept: %w", err)
			}
		}
		srv.trackConn(conn)
		srv.wg.Add(1)
		go func() {
			defer srv.wg.Done()
			defer srv.untrackConn(conn)
			srv.handleConn(conn)
		}()
	}
}

func (srv *Server) trackConn(conn net.Conn) {
	srv.connsMu.Lock()
	srv.conns[conn] = struct{}{}
	srv.connsMu.Unlock()
}

func (srv *Server) untrackConn(conn net.Conn) {
	srv.connsMu.Lock()
	delete(srv.conns, conn)
	srv.connsMu.Unlock()
}

func (srv *Server) closeAllConns() {
	srv.connsMu.Lock()
	defer srv.connsMu.Unlock()
	for conn := range srv.conns {
		conn.Close()
	}
}

func (srv *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	r := resp.NewReader(conn)

	for {
		v, err := r.ReadValue()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				// Malformed input, as opposed to a plain disconnect: tell
				// the client why before closing, same as real Redis does
				// for a protocol error.
				_ = resp.Write(conn, resp.Errorf("ERR Protocol error: %v", err))
			}
			return
		}

		args, err := v.StringArgs()
		if err != nil {
			if err := resp.Write(conn, resp.Errorf("ERR Protocol error: %v", err)); err != nil {
				return
			}
			continue
		}

		reply := command.Dispatch(srv.store, args)

		if srv.aof != nil && len(args) > 0 && reply.Type != resp.TypeError && command.IsWrite(string(args[0])) {
			if err := srv.aof.Append(args); err != nil {
				log.Printf("server: failed to persist command: %v", err)
				reply = resp.Errorf("ERR failed to persist command: %v", err)
			}
		}

		if err := resp.Write(conn, reply); err != nil {
			return
		}
	}
}

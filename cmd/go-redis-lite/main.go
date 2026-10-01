// Command go-redis-lite runs a Redis-compatible TCP server implementing
// PING, SET (with EX), GET, DEL, EXISTS, EXPIRE, TTL, and INCR.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go-redis-lite/internal/aof"
	"go-redis-lite/internal/command"
	"go-redis-lite/internal/server"
	"go-redis-lite/internal/store"
)

func main() {
	addr := flag.String("addr", ":6380", "address to listen on")
	aofPath := flag.String("aof", "go-redis-lite.aof", "append-only file path (empty disables persistence)")
	sweepInterval := flag.Duration("sweep-interval", time.Second, "background expired-key sweep interval")
	flag.Parse()

	s := store.New()

	var persister server.Persister
	if *aofPath != "" {
		log.Printf("replaying AOF from %s", *aofPath)
		if err := aof.Replay(*aofPath, func(args [][]byte) error {
			command.Dispatch(s, args)
			return nil
		}); err != nil {
			log.Fatalf("failed to replay AOF: %v", err)
		}

		a, err := aof.Open(*aofPath)
		if err != nil {
			log.Fatalf("failed to open AOF: %v", err)
		}
		defer a.Close()
		persister = a
	} else {
		log.Printf("AOF persistence disabled (-aof=\"\")")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go s.RunSweeper(ctx, *sweepInterval)

	srv := server.New(s, persister)
	if err := srv.Listen(*addr); err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("go-redis-lite listening on %s", srv.Addr())

	if err := srv.Serve(ctx.Done()); err != nil {
		log.Fatalf("server error: %v", err)
	}
	log.Println("shutdown complete")
}

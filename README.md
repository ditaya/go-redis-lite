# go-redis-lite

A Redis-compatible TCP server in Go, standard library only - no third-party packages at
all (`go.mod` has no `require` block). It speaks real RESP over real TCP, so `redis-cli`,
`redis-benchmark`, and any RESP client library can talk to it directly.

Supported commands: `PING`, `SET` (with `EX`), `GET`, `DEL`, `EXISTS`, `EXPIRE`, `TTL`,
`INCR`.

## Approach

Each piece is its own package with a narrow job, built and tested bottom-up:

1. **`internal/resp`** - encode/decode RESP2 values (simple strings, errors, integers,
   bulk strings, arrays, including the null variants). No knowledge of commands.
2. **`internal/store`** - the map + `sync.RWMutex`, lazy expiry, background sweeper. No
   knowledge of RESP or the network.
3. **`internal/command`** - `Dispatch(store, args) resp.Value`, the one place that knows
   both the wire args and the store API. No knowledge of TCP.
4. **`internal/aof`** - append each write command to disk in RESP wire format; replay it
   on startup through the same `Dispatch` function.
5. **`internal/server`** - the TCP loop: one goroutine per connection, wiring `resp` +
   `command` + `aof` together.
6. **`cmd/go-redis-lite`** - flags, signal handling, and wiring the whole thing up.

Each layer was built and `go test -race ./...`'d before the next one was started, so a
failure always pointed at the package just added - see "A bug the tests actually caught"
below for a case where this method paid off.

## Architecture & why

```
cmd/go-redis-lite/        flags, SIGINT/SIGTERM handling, wires everything together
internal/
  resp/                    RESP2 Reader (parser) and Value.Encode (writer)
  store/                   map[string]entry + sync.RWMutex, lazy expiry, sweeper
  command/                 Dispatch: the 8 commands, as pure functions over *store.Store
  aof/                     Append (log a write) and Replay (rebuild state on startup)
  server/                  net.Listener, one goroutine per conn, graceful shutdown
```

**Lazy expiry + a background sweeper, not either alone.** A `Get` on an expired key
deletes it on the spot (double-checked: `RLock` to read, promote to `Lock` only if it's
actually expired, re-checking under the write lock in case a concurrent `Set` already
replaced it). That keeps the hot read path cheap. But a key that's set with a TTL and
never read again would sit in the map forever under lazy-only expiry - so a background
goroutine (`Store.RunSweeper`) walks the whole map on a timer and deletes anything expired
nobody happened to touch. Same technique real Redis uses, for the same reason.

**`sync.RWMutex` over sharding, for now.** A single mutex is the simplest thing that's
provably correct, and `Get`'s common path only takes the read lock, which scales well
across readers. It does **not** scale well for `Set`-heavy concurrent workloads on a
single hot key - see the parallel benchmark numbers below, and "What I'd add for
production" for the sharding fix.

**AOF entries are literally RESP command arrays.** `aof.Append` encodes a write command
exactly the way a client would have sent it, and `aof.Replay` reads it back with the same
`resp.Reader` used for live connections, feeding each parsed command through
`command.Dispatch` - the identical path a live client's command takes. There's no second
parser to keep in sync with the first, and no risk of a replay format drifting from the
live format, because they're the same format.

**Why only `SET`/`DEL`/`EXPIRE`/`INCR` are logged:** `command.IsWrite` classifies which
commands mutate state. `PING`/`GET`/`EXISTS`/`TTL` are read-only and would be pure noise
in the log. A write command is only logged if it actually succeeded (`Dispatch`'s reply
isn't a RESP error) - e.g. `SET k v EX 0` (invalid) or `INCR` on a non-integer value never
touch the store, so they're correctly never written to the AOF either.

**Graceful shutdown force-closes in-flight connections, it doesn't wait for them.**
The first version of `Serve` just stopped accepting and waited (`wg.Wait()`) for existing
connections to finish on their own - which deadlocks forever against an idle client that
never sends another command, since that connection's goroutine is parked in a blocking
`Read` with nothing to wake it. The integration test (`TestIntegrationGracefulShutdown`)
caught this directly: it leaves one connection open across shutdown and asserts `Serve`
returns within 2 seconds. The fix tracks every open `net.Conn` in a set and force-closes
all of them the moment shutdown starts, in addition to closing the listener - any reply
already in flight still gets written (the connection is only closed once its handler loop
goes back to blocking on the next read), but an idle connection no longer blocks shutdown.

**`net/http`-style interfaces at the boundaries.** `server.Persister` is a one-method
interface (`Append([]byte) error`) so tests can run the server with a fake and assert
exactly which commands got logged, without touching a real file - see
`TestIntegrationAOFLogsOnlySuccessfulWrites`. This mirrors the `claude.Client` interface
pattern from an earlier project of mine for the same reason: keep the thing with I/O
behind one narrow interface, and the real implementation becomes a thin adapter.

## Assumptions

- **Clients send commands as RESP arrays of bulk strings** - what every real client
  (`redis-cli`, `redis-benchmark`, every client library) actually sends. The legacy
  "inline command" text protocol (bare `PING\r\n` over telnet) is not supported; a plain
  telnet session won't work, but `redis-cli` and `redis-benchmark` will, since they speak
  RESP natively.
- **Only `EX` is supported on `SET`** - no `PX`/`NX`/`XX`/`KEEPTTL`. An unrecognized
  option is a RESP error, not a silent no-op.
- **`EXPIRE` with a non-positive TTL expires the key immediately** but still returns `1`
  (matching Redis's own behavior) rather than erroring.
- **The AOF is replayed in full on every startup** - there's no snapshotting/compaction,
  so a long-lived server's AOF only grows. Fine for a demo; not for production (see
  below).
- **One `sync.RWMutex` for the whole keyspace** - correctness over throughput for a single
  hot key under concurrent writers. See the benchmark numbers and "What I'd add for
  production."

## Running it

```bash
go build ./...
go run ./cmd/go-redis-lite -addr :6380 -aof go-redis-lite.aof
```

Then from another terminal, with `redis-cli` (or `nc`, or any RESP client):

```bash
redis-cli -p 6380 SET foo bar EX 30
redis-cli -p 6380 GET foo
redis-cli -p 6380 TTL foo
redis-cli -p 6380 INCR counter
```

Flags: `-addr` (default `:6380`), `-aof` (default `go-redis-lite.aof`; pass `-aof=""` to
disable persistence), `-sweep-interval` (default `1s`).

Stop it with Ctrl+C (SIGINT) or `kill` (SIGTERM) - it force-closes connections, stops the
sweeper, closes the AOF file, and exits cleanly; see "Graceful shutdown" above.

### Docker

```bash
docker build -t go-redis-lite .
docker run --rm -p 6380:6380 -v grl-data:/data go-redis-lite
```

(Not verified in this environment - no Docker daemon was available while building this -
but it's a standard two-stage Alpine build; flag it if it doesn't come up clean.)

## Testing

```bash
go test -race ./...
```

Every package has table-driven unit tests; `internal/server` additionally has an
integration suite that dials real TCP connections against a real listener (no mocking the
network), covering all 8 commands, AOF call wiring, 20 concurrent clients hammering INCR
on distinct keys, and the graceful-shutdown deadlock above. Everything in this repo runs
clean under `-race`.

## Benchmarks

```bash
go test -run=^$ -bench=. -benchmem ./internal/store/...
```

Measured on this machine (Apple M3, `GOMAXPROCS=8`, non-`-race` build - `-race`
instruments every memory access and roughly triples latency, so it's excluded here):

```
BenchmarkSet-8            161534950   14.88 ns/op    0 B/op   0 allocs/op
BenchmarkSetParallel-8     28760626   82.75 ns/op    0 B/op   0 allocs/op
BenchmarkGet-8             59398650   40.00 ns/op    0 B/op   0 allocs/op
BenchmarkGetParallel-8     29642559   81.26 ns/op    0 B/op   0 allocs/op
BenchmarkGetMiss-8        383619781    6.253 ns/op   0 B/op   0 allocs/op
BenchmarkSetManyKeys-8    132293769   17.98 ns/op    0 B/op   0 allocs/op
```

These are `Store.Get`/`Store.Set` calls directly - no network, no RESP encoding, no
goroutine scheduling across connections. That isolates the data structure's own cost,
which is the number worth optimizing first.

**Read the parallel numbers carefully**: `BenchmarkSetParallel` and `BenchmarkGetParallel`
hammer a *single* hot key from every core at once - the worst case for a single-mutex
design - and both land around 80ns/op, noticeably worse than their single-goroutine
counterparts (15ns and 40ns). That's lock contention, not algorithmic cost: every core is
fighting over the same `sync.RWMutex`. `BenchmarkSetManyKeys`, spread across 1,000 keys
instead of one, is a closer proxy for a real workload and keeps `Set` near its
single-goroutine cost. This is exactly the contention a sharded store (see below) would
relieve.

**No `redis-benchmark` comparison is included.** `redis-benchmark`/`redis-server` aren't
installed in the environment this was built in, and I didn't want to put numbers in this
README I hadn't actually measured. Since this server speaks real RESP/TCP, the honest
comparison is one command away on a machine with Redis installed:

```bash
# in one terminal
go run ./cmd/go-redis-lite -addr :6380

# in another, against go-redis-lite:
redis-benchmark -p 6380 -t set,get -n 100000 -q

# and against real Redis, for comparison:
redis-server --port 6381 --daemonize yes
redis-benchmark -p 6381 -t set,get -n 100000 -q
```

Expect real Redis to win on raw throughput - it's a single-threaded event loop with no
locking at all and years of micro-optimization - but the gap, and whether it's a goroutine
scheduling cost or a lock contention cost, is worth actually measuring rather than
guessing at.

## What I'd add for production

- **Shard the store** (e.g. 16 or 32 `sync.RWMutex`-guarded maps, key hashed to a shard) to
  relieve the single-hot-key contention the parallel benchmarks show. This is the
  highest-leverage change for write-heavy concurrent throughput.
- **AOF rewrite/compaction.** Right now the AOF only grows - every `SET` on the same key
  stays in the log forever, and startup replay time grows unboundedly with it. Real Redis
  periodically rewrites the AOF to the minimal set of commands that reproduce current
  state (or snapshots instead, RDB-style).
- **`fsync` policy as a tradeoff, not a given.** `aof.Append` fsyncs on every write for
  maximum durability, which caps throughput at disk fsync latency. Production would want
  this configurable (`always`/`everysec`/`no`, as real Redis offers) so the operator picks
  the durability/throughput tradeoff instead of me picking it for them.
- **More commands and data types** - `PX`/`NX`/`XX` on `SET`, `MGET`/`MSET`, `APPEND`,
  and non-string types (lists, hashes, sets) are the obvious next layer once the
  string-command foundation is solid.
- **Replication and/or clustering** for availability - this is a single process with a
  single AOF file on local disk; losing that disk loses everything since the last fsync'd
  write.
- **Connection limits and timeouts.** There's currently no cap on concurrent connections
  and no idle-connection timeout - a slow-loris-style client (or just too many legitimate
  ones) can hold a goroutine and a file descriptor open indefinitely. A read deadline per
  command and a configurable max-connections would close that gap.
- **Observability**: command counts and latency histograms per command, current key
  count, AOF size and last-fsync time, exposed over `/metrics` or similar - right now the
  only visibility is the startup/shutdown log lines.
- **Protection against pathological pipelines** - a client pipelining thousands of
  commands before reading any replies is legal RESP but would be worth bounding (e.g. a
  max in-flight command count) to avoid unbounded server-side buffering of replies behind
  a slow reader.

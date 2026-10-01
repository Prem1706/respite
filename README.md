# Respite

A Redis-compatible in-memory key-value server written from scratch in Go, with no dependencies beyond the standard library.

It speaks Redis' wire protocol (RESP), so the official `redis-cli`, `redis-benchmark` and existing Redis client libraries work against it unchanged. It supports strings, counters, key expiry, pub/sub and append-only-file persistence.

```console
$ go run ./cmd/respite
level=INFO msg="ready to accept connections" addr=[::]:6379

$ redis-cli SET session abc EX 60
OK
$ redis-cli TTL session
(integer) 60
$ redis-cli INCR hits
(integer) 1
```

## What it supports

| Area | Commands |
|---|---|
| Strings | `GET` `SET` (with `NX` `XX` `EX` `PX` `EXAT` `PXAT` `KEEPTTL`) `MGET` `MSET` `DEL` `EXISTS` |
| Counters | `INCR` `DECR` `INCRBY` `DECRBY` (overflow-checked) |
| Expiry | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Keyspace | `KEYS` (Redis glob patterns) `DBSIZE` `FLUSHALL` |
| Pub/sub | `SUBSCRIBE` `UNSUBSCRIBE` `PUBLISH` |
| Server | `PING` `ECHO` `INFO` `QUIT` `CONFIG GET` |

## Architecture

```
            TCP connection (one goroutine each)
                          │
   resp.Reader ──► dispatch ──► command handler ──► store.Store (map + RWMutex)
                                        │
                                        ├──► aof.AOF (append-only log, fsync policy)
                                        └──► broker (pub/sub channels)
                          │
   resp.Writer ◄── replies buffered, flushed once per pipelined batch
```

| Package | Responsibility |
|---|---|
| [`internal/resp`](internal/resp) | Parses and writes the RESP2 protocol, plus the inline form used by `nc`/`telnet` |
| [`internal/store`](internal/store) | The keyspace: a map behind a read/write lock, with lazy and active expiry and a glob matcher |
| [`internal/aof`](internal/aof) | Append-only-file persistence: group commit, fsync policies, crash-safe replay |
| [`internal/server`](internal/server) | Connections, command table, pipelining, pub/sub broker, graceful shutdown |

## Design decisions

**Goroutine per connection, shared store behind a `sync.RWMutex`.** Real Redis runs every command on one thread. Here, reads can run in parallel on every CPU core, while writes take turns. The benchmarks below show the effect of that trade-off.

**Pipelining.** A client can send many commands without waiting for replies. The server keeps executing while the reader still has buffered input, and only then flushes all the replies in one `write` system call. At 16 commands per batch, throughput goes up roughly 8–12× (see the benchmarks).

**Expiry, done the Redis way.** A key with an expiry time in the past is hidden immediately on read (*lazy* expiry). Ten times a second a background sweep samples 20 keys that have a TTL and deletes the expired ones. It repeats while more than 25% of a sample had expired (*active* expiry). Memory is reclaimed without scanning the whole keyspace or holding the lock for long.

**Persistence with an append-only file.** Every write is appended to a log in RESP format and replayed on startup.
- *Absolute expiry.* `SET k v EX 60` is logged as `SET k v PXAT <unix-ms>`. If it were logged as written, restarting a day later would give the key a fresh 60 seconds.
- *The effect is logged, not the request.* A `SET … NX` that didn't set anything isn't logged, and one that did is logged without `NX`.
- *Group commit.* Commands are buffered and written out before replies are sent. A client is never told `OK` for a write that hasn't reached the OS, and one `write` call covers the whole batch.
- *fsync policy* (`always` / `everysec` / `no`) trades durability against speed, as in Redis.
- *Crash-safe replay.* A crash in the middle of a write leaves a half-written command at the end of the file. On startup that tail is detected and truncated. Corruption anywhere else refuses to start, because starting with silently missing data would be worse.

**Pub/sub without letting slow subscribers block publishers.** Each subscriber has a bounded message queue drained by its own goroutine. `PUBLISH` never waits: if a subscriber's queue is full, that subscriber is disconnected, as Redis does with `client-output-buffer-limit`.

## Benchmarks

`redis-benchmark` against Redis 8.10 and Respite on the same machine (Apple M2): 500,000 requests, 50 clients, 100,000 random keys. Run with `make bench`; full output in [bench/results.md](bench/results.md).

**No persistence**

| Command | Pipeline | Redis ops/s | Respite ops/s | Respite vs Redis |
|---|---|---|---|---|
| SET | 1 | 103,369 | 121,951 | 118% |
| GET | 1 | 103,928 | 121,802 | 117% |
| INCR | 1 | 133,191 | 124,038 | 93% |
| SET | 16 | 724,637 | 1,052,631 | 145% |
| GET | 16 | 840,336 | 1,408,450 | 168% |
| INCR | 16 | 1,369,863 | 970,873 | 71% |

**With AOF, fsync every second**

| Command | Pipeline | Redis ops/s | Respite ops/s | Respite vs Redis |
|---|---|---|---|---|
| SET | 16 | 636,132 | 608,272 | 96% |
| GET | 16 | 954,198 | 1,628,664 | 171% |
| INCR | 16 | 625,782 | 533,049 | 85% |

**What the numbers mean.**
- Reads are faster than Redis because `GET`s take a shared read lock and run on every core at once.
- Pipelined writes are slower because they all queue behind the one write lock, so extra cores stop helping. Real Redis does a similar amount of work on a single core with no lock at all.
- Throughput per CPU core, Redis is far more efficient. Respite uses several cores to reach these numbers, while Redis uses one.
- p99 latency is also worse for Respite on writes, because some requests wait on the lock.

## Running it

```bash
make test     # all tests with the race detector
make run      # listens on :6379, persists to appendonly.aof
make bench    # needs redis-server and redis-benchmark (brew install redis)
```

Flags: `-addr :6379`, `-aof appendonly.aof` (pass `-aof ""` to disable persistence), `-appendfsync always|everysec|no`.

## Testing

- **Unit tests** cover the parser (binary-safe values, truncated input, malformed input), the store (expiry with a fake clock, overflow, 50 goroutines × 200 concurrent `INCR`s losing no updates) and the AOF (replay, truncated-tail recovery, refusing corruption).
- **End-to-end tests** start a real server on a random port and talk to it over TCP. They cover pipelining 1,000 commands, pub/sub (including disconnecting a slow subscriber), and restarting from the AOF with TTLs preserved.
- Everything runs under `go test -race` in CI.

## Limitations and next steps

- **Sharded locking.** Split the keyspace into N shards by key hash, each with its own lock, so writes to different keys stop contending. This targets the pipelined-write gap above.
- **AOF rewrite.** The log grows forever. `BGREWRITEAOF` would rewrite it as the smallest set of commands that rebuilds the current data.
- Only the string type exists. Lists, hashes and sets are not implemented, and there is one database, no auth, no transactions and no replication.

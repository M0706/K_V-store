# K/V Store

A Redis-compatible in-memory key-value store written in Python from scratch. It speaks the [RESP protocol](https://redis.io/docs/reference/protocol-spec/) (Redis Serialization Protocol), which means any standard Redis client — `redis-cli`, `redis-py`, `redis-benchmark`, etc. — can connect to it without modification.

---

## Architecture Overview

The code is layered so the same core can be used **two ways**: imported as an
in-process library, or run as a network server. Each layer depends only on the
one below it.

```
Layer 3  async_tcp.py / sync_tcp.py  — TCP servers (RESP over sockets)
Layer 2  protocol.py                 — RESP <-> Store command translation
Layer 1  store.py                    — Store: the pure in-memory KV core
         resp.py                     — RESP parser/encoder (used by Layer 2)
```

- **`store.py` (`Store`)** — the actual key-value engine: the keyspace, TTL, and
  expiry. Pure Python, no sockets, no RESP. This is what you `import` to use the
  store in-process.
- **`protocol.py`** — a *stateless* adapter that parses a RESP command, calls the
  matching `Store` method, and encodes the reply. Holds no data itself.
- **`async_tcp.py` / `sync_tcp.py`** — TCP servers that own a `Store` and feed
  client bytes through `protocol.py`. The async server uses Python's `selectors`
  module (`kqueue` on macOS, `epoll` on Linux) to multiplex many clients on a
  single thread — no threads, no async/await, just an event loop.

### Key Design Points

- **Library or server, one core** — `store.py` is usable on its own; the servers
  are thin shells on top. See [Two Ways to Use It](#two-ways-to-use-it).
- **Two-level expiry** — passive expiry on read (an expired key is dropped when
  accessed) + active expiry via `active_expire_cycle()`, which mirrors Redis's
  `serverCron`. The active cycle samples from a separate TTL index (`expires`
  dict) so its cost scales with the number of volatile keys, not total keyspace
  size.
- **Concurrency by construction** — the library is thread-safe by default; the
  server is single-threaded and opts out of locking. See
  [Concurrency Model](#concurrency-model) for the full intent.
- **RESP pipelining** — the read buffer is drained in a loop, so multiple
  commands sent in one `recv()` are all handled before yielding back to the
  selector.
- **Inline command support** — plain text commands (e.g. `PING\r\n`) are accepted
  alongside full RESP arrays.

---

## Two Ways to Use It

**As a library (in-process).** Import `Store` and call it directly — no sockets,
no protocol, no separate process:

```python
from store import Store

s = Store()
s.set("session", "abc123", ex=60)   # TTL in seconds (px=... for milliseconds)
s.get("session")                     # "abc123"
s.delete("session")                  # 1
```

This is the right choice for local testing or a single-process app that just
wants a dict with Redis-style TTL semantics and nothing to install or run.

**As a server (over the network).** Run it and point any Redis client at it:

```python
from async_tcp import serve
serve(port=6379)
```

```bash
redis-cli -p 6379 set name Manav
```

Same engine underneath — the server is just `protocol.py` + a socket loop
wrapped around the same `Store`.

---

## Concurrency Model

The intent is simple to state:

> **The library is thread-safe by default — a caller never has to manage locks.
> The server is single-threaded, so it needs none.**

### The library: safe by default

A plain `Store()` guards every operation with an internal lock, so you can call
it from any number of threads and each call is atomic. You write **zero**
synchronization code:

```python
s = Store()              # thread-safe by default
# call s.set / s.get / s.delete from as many threads as you like — each
# individual call is atomic; the lock is entirely internal.
```

This is a deliberate **safe-by-default, fast-by-opt-out** design. There is
nothing exotic here — it is an ordinary `threading.Lock`, a decades-old pattern.
(If anything, it matters *more* under free-threaded "no-GIL" Python (3.13+),
where you can no longer accidentally rely on the GIL to serialize access.)

A caller that *knows* it is single-threaded can drop the lock for a
contention-free hot path:

```python
s = Store(thread_safe=False)   # opt out — no locking overhead
```

### The server: single-threaded, so locking is moot

The server runs everything on **one** event-loop thread. There are no concurrent
accessors to coordinate, so thread-safety simply does not arise — it is not that
the event loop *makes* the store safe, it is that there is only one thread
touching it. For that reason the server constructs its store with
`Store(thread_safe=False)` and pays no locking cost.

> ⚠️ The flip side: because the server's store has no lock, you must **not** share
> that instance across threads yourself. Single-threaded by design.

### What "atomic" covers (and what it doesn't)

The internal lock makes each **individual command** atomic. It does **not** make
a *sequence* of commands atomic, because only the caller knows where a sequence
begins and ends:

```python
n = s.get("counter")        # another thread can run between these two lines
s.set("counter", str(int(n or 0) + 1))   # -> classic lost-update race
```

This is the same reason Redis provides `MULTI`/`EXEC` despite being
single-threaded. Multi-command atomicity requires an explicit boundary; per-
command atomicity is automatic.

### Self-cleaning store: background active expiry

A normal in-memory cache only frees an expired key when you *touch* it again
(passive expiry). That means a key you set with a TTL and then **never read
again** sits in memory until you happen to access it — a slow leak for
write-heavy or fire-and-forget workloads.

`Store` can clean itself. Pass `active_expiry=True` and it runs a background
sweeper that proactively reclaims expired keys on its own — no event loop, no
cron, no work from you:

```python
from store import Store

with Store(active_expiry=True) as s:
    s.set("temp", "x", ex=5)
    # ... 5 seconds later, even if nobody ever reads "temp" again,
    # the background sweeper has already removed it. No leak.
```

The `with` block is the recommended form: the sweeper thread is started on entry
and **stopped automatically on exit**, so you never leak a thread.

#### Without a context manager

If a `with` block doesn't fit your code (e.g. the store lives for the whole
process), start and stop the sweeper explicitly:

```python
s = Store(active_expiry=True)   # or: s = Store(); s.start_expiry()
# ... use s for the lifetime of your app ...
s.stop()                         # stop the sweeper when shutting down
```

Both `start_expiry()` and `stop()` are idempotent and safe to call more than
once. The thread is a **daemon**, so even if you forget to stop it, it won't
block your process from exiting.

#### Tuning the sweeper

Two knobs control the cost/freshness trade-off:

```python
Store(
    active_expiry=True,
    expiry_interval=0.1,    # seconds between sweeps (default 0.1 = 10x/sec, like Redis)
    expiry_budget_ms=1.0,   # max time one sweep may run before yielding (default 1ms)
)
```

- **`expiry_interval`** — how often the sweeper wakes up. Smaller = fresher
  reclamation, more CPU wakeups.
- **`expiry_budget_ms`** — a hard time cap per sweep, so a keyspace full of
  expired keys can never freeze the thread for long; leftovers are picked up on
  the next tick.

#### How it works (and why it needs the lock)

Each sweep calls `active_expire_cycle()`, which mirrors Redis's `serverCron`:
it samples ~20 keys from the TTL index, deletes the expired ones, and — if more
than 25% of the sample was expired — loops to clean more aggressively, always
bounded by the time budget. Sampling (rather than scanning) keeps the cost
proportional to the number of volatile keys, not the whole keyspace.

Because the sweeper runs on a **separate daemon thread**, it mutates the
keyspace concurrently with your calls — so enabling `active_expiry` automatically
turns the internal lock on (even if you passed `thread_safe=False`). You still
write no synchronization code; it's handled for you.

> The server doesn't use this thread: it drives `active_expire_cycle()` from its
> own event loop instead, staying single-threaded. The background sweeper exists
> specifically for **library** users, who have no loop of their own.

---

## Requirements

- Python 3.10+ (uses `X | Y` union type hints)
- No external dependencies for the server itself

For tests:
```
pip install pytest
```

---

## Running the Server

```bash
python main.py
```

The server listens on `127.0.0.1:65432` by default.

```
Listening on 127.0.0.1:65432
```

To switch to the synchronous single-client server (useful for debugging), edit `main.py`:

```python
# Change this:
AsyncTCPServer().run()

# To this:
run_sync_tcp_server()
```

---

## Connecting Clients

### Option 1 — redis-cli

The easiest way. Connect directly:

```bash
redis-cli -p 65432
```

You'll get an interactive shell:

```
127.0.0.1:65432> PING
PONG
127.0.0.1:65432> SET name "Manav"
OK
127.0.0.1:65432> GET name
"Manav"
127.0.0.1:65432> SET session_token "abc123" EX 60
OK
127.0.0.1:65432> GET session_token
"abc123"
127.0.0.1:65432> DEL name
(integer) 1
127.0.0.1:65432> GET name
(nil)
```

### Option 2 — netcat (raw RESP)

Send raw RESP frames directly to verify protocol correctness:

```bash
# PING
printf "*1\r\n\$4\r\nPING\r\n" | nc 127.0.0.1 65432

# SET foo bar
printf "*3\r\n\$3\r\nSET\r\n\$3\r\nfoo\r\n\$3\r\nbar\r\n" | nc 127.0.0.1 65432

# Inline command
printf "PING\r\n" | nc 127.0.0.1 65432
```

### Option 3 — Python (redis-py)

```bash
pip install redis
```

```python
import redis

r = redis.Redis(host="127.0.0.1", port=65432, decode_responses=True)

r.ping()                         # True
r.set("name", "Manav")          # True
r.get("name")                    # 'Manav'
r.set("token", "abc", ex=30)    # True  — expires in 30 seconds
r.get("token")                   # 'abc'
r.delete("name")                 # 1
r.get("name")                    # None
```

### Option 4 — redis-benchmark

The server handles the `CLIENT SETNAME` and `CONFIG GET` handshake that `redis-benchmark` sends, so you can run benchmarks directly:

```bash
redis-benchmark -p 65432 -t set,get -n 10000
```

---

## Supported Commands

| Command | Syntax | Description |
|---|---|---|
| `PING` | `PING [message]` | Returns `PONG`, or echoes the message if provided |
| `ECHO` | `ECHO message` | Returns the message as a bulk string |
| `SET` | `SET key value [EX seconds] [PX milliseconds]` | Set a key. Optional `EX`/`PX` sets a TTL |
| `GET` | `GET key` | Get the value of a key. Returns `nil` if missing or expired |
| `DEL` | `DEL key [key ...]` | Delete one or more keys. Returns count of keys actually deleted |

### TTL Behaviour

- `EX` — time-to-live in **seconds**
- `PX` — time-to-live in **milliseconds**
- A `SET` on an existing key with no TTL **clears** any previous expiry (matches Redis behaviour)
- Zero or negative TTL values are rejected with `-ERR`

```bash
SET counter 100 EX 10    # expires in 10 seconds
SET flag 1 PX 500        # expires in 500 milliseconds
SET key val              # no expiry — overwrites key and clears any prior TTL
```

### Expiry Implementation

Keys are expired via two mechanisms:

1. **Passive** — on every `GET` or `DEL`, the key's expiry is checked and the key is deleted if it has elapsed. No background work needed for keys that are regularly accessed.

2. **Active** — every 100ms, `active_expire_cycle()` runs. It randomly samples up to 20 keys from the TTL index and deletes the expired ones. If more than 25% of the sample is expired, it loops immediately to clean up aggressively. This prevents memory leaks from keys that are never read again.

---

## Running Tests

```bash
pytest
```

Configuration lives in `pytest.ini`, which also enforces a **90% coverage gate**
(`--cov-fail-under=90`). The test suite covers:

- `Store` library API — set/get/delete, TTL, isolation between instances
- Thread-safety — concurrent writers don't lose updates; opt-out drops the lock
- Background active expiry — reclaims untouched keys, lifecycle, concurrent sweeps
- `PING` / `ECHO` — argument handling, case-insensitivity
- `SET`/`GET` — basic, overwrite, missing key, spaces in values, multiple keys
- TTL — `EX`, `PX`, passive deletion, index sync, overwrite clears TTL
- `DEL` — single, multiple, missing, expired key not counted, idempotent
- Active expiry cycle — reclaims untouched keys, leaves live keys alone, time budget
- Server framing — pipelining, partial reads, split commands, disconnect, broken pipe
- Protocol edge cases — inline commands, garbled input, unknown commands

---

## Contributing

`master` and `staging` are protected — **nobody pushes to them directly**. All changes go through pull requests, and the merge path is one-directional: feature branch → `staging` → `master`.

### Workflow

1. **Branch off `staging`:**
   ```bash
   git checkout staging
   git pull
   git checkout -b your-feature-branch
   ```
2. **Make your changes** and commit them.
3. **Run the test suite locally** before pushing — CI enforces the same gate:
   ```bash
   pytest
   ```
4. **Push your branch and open a PR into `staging`:**
   ```bash
   git push -u origin your-feature-branch
   ```
   Open the PR with **`staging`** as the base branch (never `master`).
5. **Wait for review and merge.** The repository owner reviews and merges the PR into `staging`.
6. **Promotion to `master`** is handled separately by the owner via a `staging` → `master` PR.

### Merge requirements

Every PR into `staging` and `master` must satisfy:

- ✅ **CI passes** — the `test` GitHub Actions check runs `pytest` on every PR.
- ✅ **Coverage ≥ 90%** — enforced by `--cov-fail-under=90` in `pytest.ini`. The build fails if application coverage drops below this.
- ✅ **Code-owner review** — see [`.github/CODEOWNERS`](.github/CODEOWNERS).

> Direct pushes, force-pushes, and branch deletions are blocked on `master` and `staging`. Only the repository owner can bypass these rules.

---

## Project Structure

```
.
├── main.py                  # Entry point — starts the async server
├── store.py                 # Store: in-memory KV core (TTL, expiry, thread-safety)
├── protocol.py              # RESP <-> Store command translation (stateless)
├── async_tcp.py             # Non-blocking selector-based TCP server
├── sync_tcp.py              # Blocking single-client TCP server (reference)
├── resp.py                  # RESP protocol parser and encoder
└── tests/                   # pytest test suite
    ├── test_store.py        # Store library API + thread-safety + background expiry
    ├── test_protocol.py     # RESP command layer over a Store
    ├── test_async_tcp.py    # Server framing logic
    └── test_sync_tcp.py     # Legacy server integration smoke test
```

---

## Configuration

The host and port are hardcoded at the top of `async_tcp.py` and `sync_tcp.py`:

```python
HOST = "127.0.0.1"
PORT = 65432
```

Change these directly if you need the server to bind on a different interface or port (e.g. `HOST = "0.0.0.0"` to accept connections from other machines).

The active expiry interval is controlled in `async_tcp.py`:

```python
CRON_INTERVAL = 0.1  # seconds — how often the active-expire cycle runs
```

And the sampling parameters in `store.py`:

```python
KEYS_PER_LOOP = 20       # keys sampled per cycle
ACCEPTABLE_STALE = 0.25  # re-run if more than 25% of the sample was expired
```

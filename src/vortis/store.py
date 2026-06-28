"""In-memory key-value store with Redis-style TTL and active expiration.

This is the pure, importable core of the project — no sockets, no RESP, no
bytes. It can be used directly as a library:

    from store import Store

    s = Store()
    s.set("session", "abc123", ex=60)   # expires in 60 seconds
    s.get("session")                     # "abc123"
    s.delete("session")                  # 1

By default the store is passive: an expired key is reclaimed when it is next
accessed. To also reclaim keys that are *never* read again, enable background
active expiration, which runs a sweep on its own daemon thread:

    with Store(active_expiry=True) as s:   # thread stopped automatically on exit
        s.set("temp", "x", ex=5)
        ...

The TCP server (async_tcp.py) and the RESP command layer (protocol.py)
are thin adapters built on top of this class. The server leaves
``active_expiry`` off and drives ``active_expire_cycle()`` from its own event
loop instead — so it stays single-threaded and lock-free.
"""
import itertools
import random
import threading
import time
from contextlib import nullcontext

from vortis.eviction import EVICTION_SAMPLES, KeyCountSizer, make_policy
from vortis.sweeper import BackgroundSweeper

# Active-expire tuning (mirrors Redis's activeExpireCycle constants), used by
# active_expire_cycle below.
KEYS_PER_LOOP = 20       # ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP
ACCEPTABLE_STALE = 0.25  # continue while >25% of the sample is expired


class Store:
    """A single in-memory keyspace with passive + active TTL expiration.

    Two independent ``Store`` instances share no state, so they can be used
    side by side (e.g. one per test, or several logical databases in one
    process).

    Thread-safety (``thread_safe``, default True): every public operation is
    guarded by a lock, so a client may call any method from any number of
    threads and each call is atomic — the client never has to think about
    synchronization. Note this is a *per-command* guarantee only: a *sequence*
    of commands (e.g. a read-modify-write like get-then-set) is not atomic, as
    another thread may run between the calls. If you need a multi-command
    sequence to be atomic, guard it with your own lock around the calls.

    A caller that knows it is single-threaded (e.g. the event-loop server) can
    pass ``thread_safe=False`` to drop the lock for a faster, contention-free
    hot path. Enabling ``active_expiry`` forces the lock on regardless, because
    its background daemon thread mutates the keyspace concurrently.

    Bounding size (``max_size``): by default the store is unbounded and grows
    until you run out of memory. Pass ``max_size`` to cap it; once full, a new
    write first evicts existing keys according to ``eviction`` (Redis's
    evict-then-write). The limit is a **key count** (a future byte-based limiter
    is pluggable via the Sizer strategy). When ``max_size`` is None there is no
    eviction machinery at all, so the unbounded hot path is unchanged.
    """

    def __init__(self, thread_safe: bool = True, active_expiry: bool = False,
                 expiry_interval: float = 0.1,
                 expiry_budget_ms: float = 1.0,
                 max_size: int | None = None,
                 eviction: str = "random") -> None:
        # key -> (value, expires_at_monotonic_seconds | None)
        self.data: dict[str, tuple[str, float | None]] = {}
        # Secondary index: key -> expires_at, for keys that have a TTL only.
        # Mirrors Redis's redisDb.expires dict. The active-expire cycle samples
        # from THIS dict so its cost scales with the number of volatile keys,
        # not the total keyspace. Kept in sync with `data` on every mutation.
        self.expires: dict[str, float] = {}

        # Background active-expiry runs the cycle periodically on a daemon
        # thread. The sweeper owns all the threading; Store just hands it the
        # task to run.
        self._expiry_budget_ms = expiry_budget_ms
        self._sweeper = BackgroundSweeper(
            expiry_interval,
            lambda: self.active_expire_cycle(self._expiry_budget_ms),
            name="store-active-expiry")

        # Size bounding (off unless max_size is set). When off, none of the
        # eviction machinery is consulted, so the hot path is untouched.
        if max_size is not None and max_size <= 0:
            raise ValueError("max_size must be a positive integer or None")
        self._max_size = max_size
        self._bounded = max_size is not None
        if self._bounded:
            self._sizer = KeyCountSizer()
            self._policy = make_policy(eviction)

        # Thread-safety is the default guarantee. A background-expiry daemon
        # always needs the lock; a caller can otherwise opt out when it can
        # prove it is single-threaded (the server), paying zero locking cost.
        self._locking = thread_safe or active_expiry
        self._lock = threading.Lock() if self._locking else nullcontext()

        if active_expiry:
            self.start_expiry()

    # -- internal, lock-free (callers must already hold the lock) -----------

    def _del_key(self, key: str) -> None:
        """Remove a key from both the keyspace and the TTL index.

        This is the single removal chokepoint — delete, passive expiry, the
        active sweep, and eviction all route through it — so notifying the
        eviction policy here keeps any policy bookkeeping consistent no matter
        how a key leaves.
        """
        existed = key in self.data
        self.data.pop(key, None)
        self.expires.pop(key, None)
        if self._bounded and existed:
            self._policy.note_remove(key)

    def _get_nolock(self, key: str) -> str | None:
        """Lookup with passive expiry, assuming the lock is already held."""
        entry = self.data.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and time.monotonic() >= expires_at:
            self._del_key(key)
            return None
        if self._bounded and self._policy.tracks_access:
            self._policy.note_access(key)
        return value

    # -- core operations (public, lock-guarded) -----------------------------

    def set(self, key: str, value: str, *, ex: int | None = None,
            px: int | None = None) -> None:
        """Set ``key`` to ``value`` with an optional TTL.

        ``ex`` is a time-to-live in seconds, ``px`` in milliseconds; pass at
        most one. A plain ``set`` (no TTL) clears any previous expiry on the
        key, matching Redis behaviour.
        """
        if ex is not None and px is not None:
            raise ValueError("set: specify at most one of 'ex' and 'px'")

        expires_at: float | None = None
        if ex is not None:
            expires_at = time.monotonic() + ex
        elif px is not None:
            expires_at = time.monotonic() + px / 1000

        with self._lock:
            if self._bounded:
                self._make_room_for(key, value)
            self.data[key] = (value, expires_at)
            if expires_at is not None:
                self.expires[key] = expires_at
            else:
                self.expires.pop(key, None)
            if self._bounded:
                self._policy.note_write(key)

    def get(self, key: str) -> str | None:
        """Return the value for ``key``, or None if missing or expired.

        Performs passive expiration: an expired key is deleted on access.
        """
        with self._lock:
            return self._get_nolock(key)

    def delete(self, *keys: str) -> int:
        """Delete one or more keys; return how many were actually present.

        Honours passive expiry: an already-expired key is not counted.
        """
        with self._lock:
            deleted = 0
            for key in keys:
                if self._get_nolock(key) is not None:
                    self._del_key(key)
                    deleted += 1
            return deleted

    def __contains__(self, key: str) -> bool:
        """Support ``key in store`` with passive-expiry semantics."""
        with self._lock:
            return self._get_nolock(key) is not None

    def __len__(self) -> int:
        """Number of keys currently stored (without sweeping for expiry)."""
        with self._lock:
            return len(self.data)

    # -- active expiration --------------------------------------------------

    def _sample(self, pool: dict, n: int) -> list[str]:
        """Return up to ``n`` keys from ``pool``, approximately at random.

        Used both for active-expiry sampling (``pool=self.expires``) and for
        eviction victim selection (``pool=self.data``).

        The naive approach — ``random.sample(list(pool), n)`` — copies the
        *entire* mapping into a new list on every call. With a large keyspace
        and a looping cycle, that O(N) allocation per iteration dwarfs the O(n)
        useful work and defeats the whole point of sampling.

        Instead we take a contiguous window of ``n`` keys starting at a random
        offset, advancing the dict iterator with ``itertools.islice`` (the skip
        happens in C, with no intermediate list). This is O(offset + n) time and
        O(n) memory — no full copy.

        Trade-off: a contiguous slice is only *approximately* uniform (the keys
        are adjacent in insertion order, not independently drawn). That matches
        the spirit of Redis's sampling (``maxmemory-samples``), which is also
        approximate — we need decent coverage over time, not perfect per-call
        uniformity.

        Assumes the lock is already held.
        """
        size = len(pool)
        if size <= n:
            return list(pool)
        # Random starting offset; wrap by chaining the iterator with itself so a
        # window near the end still yields n keys.
        start = random.randint(0, size - 1)
        window = itertools.islice(itertools.chain(pool, pool), start, start + n)
        return list(window)

    def _make_room_for(self, key: str, value: str) -> None:
        """Evict keys until ``key`` of given ``value`` fits within max_size.

        Assumes the lock is held. Inserting over an existing key does not grow
        the store, so nothing is evicted in that case. Stops early if the store
        cannot be shrunk further (e.g. a policy that declines to evict).
        """
        if key in self.data:
            return  # overwrite — size unchanged, no eviction needed
        incoming = self._sizer.cost(key, value)
        while self._sizer.current(self.data) + incoming > self._max_size:
            if self._evict_one() is None:
                break  # nothing evictable — let the write proceed

    def _evict_one(self) -> str | None:
        """Evict a single key per the policy. Returns the victim, or None.

        Assumes the lock is held. Samples a small random set of keys and lets
        the policy choose the victim — never scans the whole keyspace.
        """
        sample = self._sample(self.data, EVICTION_SAMPLES)
        victim = self._policy.evict(sample)
        if victim is None:
            return None
        self._del_key(victim)
        return victim

    def active_expire_cycle(self, time_budget_ms: float = 1.0) -> int:
        """Reclaim expired keys cooperatively. Returns the number deleted.

        Passive expiry only deletes keys when someone touches them, so a key
        nobody reads again would leak forever. This cycle proactively reclaims
        them WITHOUT scanning the whole keyspace and WITHOUT running unbounded:

          1. Sample up to KEYS_PER_LOOP random keys from the TTL index.
          2. Delete the expired ones.
          3. If more than ACCEPTABLE_STALE of the sample was expired, the
             keyspace is probably dirty -> loop again. Otherwise stop.
          4. Regardless, bail the moment we exceed the per-call time budget so
             the caller (e.g. the event loop) gets control back.

        The lock is acquired and released *per batch*, not for the whole cycle,
        so a concurrent caller's get/set only ever waits microseconds even while
        a large sweep is in progress.
        """
        deadline = time.monotonic() + time_budget_ms / 1000
        total_deleted = 0

        while self.expires:
            now = time.monotonic()
            if now >= deadline:
                break  # out of time — leftover keys wait for the next tick

            with self._lock:
                sample = self._sample(self.expires, KEYS_PER_LOOP)
                sample_size = len(sample)

                expired = 0
                for key in sample:
                    if now >= self.expires.get(key, float("inf")):
                        self._del_key(key)
                        expired += 1
                total_deleted += expired

            # If the sample was mostly fresh, the rest of the keyspace probably
            # is too — stop and let passive expiry handle the stragglers.
            if expired <= sample_size * ACCEPTABLE_STALE:
                break

        return total_deleted

    # -- background expiry lifecycle ----------------------------------------

    def _ensure_locking(self) -> None:
        """Promote to a real lock before a second thread can touch the store.

        A ``thread_safe=False`` store starts with a no-op lock; if background
        expiry is later started, a concurrent thread appears and real locking
        is required. Safe to call before the sweeper thread starts (no race).
        """
        if not self._locking:
            self._locking = True
            self._lock = threading.Lock()

    def start_expiry(self) -> None:
        """Start the background daemon that runs active expiration (idempotent)."""
        self._ensure_locking()
        self._sweeper.start()

    def stop(self) -> None:
        """Stop the background expiry daemon and wait for it (idempotent)."""
        self._sweeper.stop()

    # -- context manager: ensure the thread is cleaned up -------------------

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

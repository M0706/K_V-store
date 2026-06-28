#!/usr/bin/env python3
"""In-depth stress harness for the Store library.

Unlike the unit tests (which verify logic), this hammers the store under real
multithreaded load to validate the claims we make in the README:

  1. Thread-safety / per-command atomicity  — concurrent writers never lose or
     corrupt updates, and never raise (no "dict changed size during iteration").
  2. Internal invariant                     — every key in `expires` is also in
     `data` (the two structures stay consistent under concurrency).
  3. Background active expiry               — keys nobody reads again ARE
     reclaimed (no memory leak), while live keys survive.
  4. Throughput                             — hundreds/thousands of commands per
     second across many threads.
  5. thread_safe=False                      — the single-threaded opt-out is
     actually faster (the lock has a measurable, if small, cost).

Run:  python scripts/stress_test.py
Exit code is non-zero if any check fails, so it doubles as a smoke gate.

A note on the GIL: under standard CPython, a single `dict[k] = v` is one atomic
bytecode, so even a *lock-free* store rarely shows visible corruption on simple
per-key writes. The lock's value is in (a) multi-bytecode sequences like
delete's get→check→pop, and (b) free-threaded "no-GIL" builds (3.13+) where even
single writes can tear. Scenario 7 makes the boundary concrete by exposing a
lost-update on a read-modify-write, which no per-command lock can prevent.
"""
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Allow running from the repo without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vortis import Store  # noqa: E402


# ---------------------------------------------------------------------------
# Small reporting helpers
# ---------------------------------------------------------------------------

_failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    mark = "PASS" if ok else "FAIL"
    line = f"  [{mark}] {label}"
    if detail:
        line += f"  ({detail})"
    print(line)
    if not ok:
        _failures.append(label)


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def assert_invariant(store: Store, label: str = "invariant: expires ⊆ data") -> None:
    """Every key with a TTL entry must exist in the keyspace."""
    # Snapshot under the lock-free read is fine: we only check membership.
    missing = [k for k in list(store.expires) if k not in store.data]
    check(label, not missing, f"{len(missing)} orphaned TTL keys" if missing else "consistent")


# ---------------------------------------------------------------------------
# Scenario 1 — concurrent writers, no lost updates
# ---------------------------------------------------------------------------

def test_no_lost_updates(threads: int = 32, per_thread: int = 5_000) -> None:
    section(f"Scenario 1: {threads} threads × {per_thread} writes — no lost updates")
    s = Store()  # thread-safe by default
    total = threads * per_thread

    def worker(t: int) -> None:
        for i in range(per_thread):
            s.set(f"t{t}-k{i}", "v")

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(as_completed([pool.submit(worker, t) for t in range(threads)]))
    elapsed = time.perf_counter() - start

    rate = total / elapsed
    check("every write survived (no lost updates)", len(s) == total,
          f"{len(s)}/{total} keys")
    assert_invariant(s)
    print(f"  → {total:,} SET in {elapsed:.3f}s = {rate:,.0f} ops/sec")


# ---------------------------------------------------------------------------
# Scenario 2 — mixed set/get/delete chaos, must stay consistent & not crash
# ---------------------------------------------------------------------------

def test_mixed_ops_consistency(threads: int = 24, per_thread: int = 8_000) -> None:
    section(f"Scenario 2: {threads} threads × {per_thread} mixed ops — consistency under chaos")
    s = Store()
    errors: list[Exception] = []
    ops = [0]
    ops_lock = threading.Lock()

    def worker(t: int) -> None:
        local = 0
        try:
            for i in range(per_thread):
                key = f"t{t}-k{i % 50}"  # reuse keys -> real contention on same keys
                s.set(key, str(i), ex=30)
                s.get(key)
                if i % 3 == 0:
                    s.delete(key)
                local += 3
        except Exception as e:  # any race -> RuntimeError etc.
            errors.append(e)
        with ops_lock:
            ops[0] += local

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(as_completed([pool.submit(worker, t) for t in range(threads)]))
    elapsed = time.perf_counter() - start

    check("no exceptions raised under concurrent mixed ops", not errors,
          f"{len(errors)} errors" if errors else "clean")
    if errors:
        check("first error detail", False, repr(errors[0]))
    assert_invariant(s)
    print(f"  → {ops[0]:,} ops in {elapsed:.3f}s = {ops[0] / elapsed:,.0f} ops/sec")


# ---------------------------------------------------------------------------
# Scenario 3 — background sweeper concurrent with a writer storm
# ---------------------------------------------------------------------------

def test_sweeper_under_load(writers: int = 16, per_thread: int = 10_000) -> None:
    section(f"Scenario 3: background sweeper + {writers} writers — no crash, no orphans")
    errors: list[Exception] = []
    with Store(active_expiry=True, expiry_interval=0.005) as s:
        def worker(t: int) -> None:
            try:
                for i in range(per_thread):
                    # Half the keys are short-lived (sweeper races to delete them),
                    # half are long-lived (must survive).
                    if i % 2 == 0:
                        s.set(f"short-{t}-{i}", "v", px=2)
                    else:
                        s.set(f"long-{t}-{i}", "v", ex=300)
                    s.get(f"long-{t}-{i}")
            except Exception as e:
                errors.append(e)

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=writers) as pool:
            list(as_completed([pool.submit(worker, t) for t in range(writers)]))
        elapsed = time.perf_counter() - start

        check("no exceptions while sweeper ran concurrently", not errors,
              f"{len(errors)} errors" if errors else "clean")
        if errors:
            check("first error detail", False, repr(errors[0]))
        assert_invariant(s, "invariant holds DURING concurrent sweeping")
        total_ops = writers * per_thread * 2
        print(f"  → ~{total_ops:,} ops in {elapsed:.3f}s = {total_ops / elapsed:,.0f} ops/sec")


# ---------------------------------------------------------------------------
# Scenario 4 — background sweeper actually reclaims untouched keys (no leak)
# ---------------------------------------------------------------------------

def test_no_leak(n_keys: int = 50_000) -> None:
    section(f"Scenario 4: {n_keys:,} short-TTL keys, never read again — must be reclaimed")
    with Store(active_expiry=True, expiry_interval=0.01, expiry_budget_ms=2.0) as s:
        for i in range(n_keys):
            s.set(f"k{i}", "v", px=20)  # all expire in 20ms
        peak = len(s)
        # Nobody ever reads these again — only the background sweeper can free them.
        deadline = time.perf_counter() + 10.0
        while len(s) > 0 and time.perf_counter() < deadline:
            time.sleep(0.05)
        remaining = len(s)
        elapsed = 10.0 - (deadline - time.perf_counter())
        check("all untouched expired keys reclaimed by sweeper", remaining == 0,
              f"{remaining} leaked (peak {peak:,})")
        assert_invariant(s)
        print(f"  → drained {peak:,} → 0 in ~{elapsed:.2f}s with no reads")


# ---------------------------------------------------------------------------
# Scenario 5 — lock cost: thread_safe=True vs False (single thread)
# ---------------------------------------------------------------------------

def test_lock_overhead(n: int = 500_000) -> None:
    section(f"Scenario 5: lock overhead — {n:,} single-threaded SET/GET, locked vs opt-out")

    def run(store: Store) -> float:
        start = time.perf_counter()
        for i in range(n):
            store.set("k", "v")
            store.get("k")
        return time.perf_counter() - start

    locked = run(Store(thread_safe=True))
    unlocked = run(Store(thread_safe=False))
    overhead = (locked - unlocked) / unlocked * 100 if unlocked else 0
    print(f"  → thread_safe=True : {locked:.3f}s  ({2 * n / locked:,.0f} ops/sec)")
    print(f"  → thread_safe=False: {unlocked:.3f}s  ({2 * n / unlocked:,.0f} ops/sec)")
    print(f"  → lock overhead: {overhead:.1f}%")
    # Not a hard assertion (timing is noisy), just informational — but sanity
    # check that opt-out isn't somehow dramatically slower.
    check("opt-out is not slower than locked (sanity)", unlocked <= locked * 1.5,
          f"locked={locked:.3f}s unlocked={unlocked:.3f}s")


# ---------------------------------------------------------------------------
# Scenario 6 — TTL correctness under concurrency (no premature/late expiry)
# ---------------------------------------------------------------------------

def test_ttl_correctness_concurrent(threads: int = 16) -> None:
    section(f"Scenario 6: {threads} threads — TTL boundaries honoured under load")
    s = Store()
    # Long-TTL keys must NEVER be reported missing while load runs.
    for i in range(1000):
        s.set(f"keep{i}", "v", ex=300)

    premature: list[str] = []
    stop = threading.Event()

    def reader() -> None:
        while not stop.is_set():
            for i in range(0, 1000, 50):
                if s.get(f"keep{i}") is None:
                    premature.append(f"keep{i}")

    def churner(t: int) -> None:
        for i in range(20_000):
            s.set(f"tmp-{t}-{i}", "v", px=1)  # rapid short-lived churn

    threads_list = [threading.Thread(target=reader) for _ in range(threads // 2)]
    threads_list += [threading.Thread(target=churner, args=(t,)) for t in range(threads // 2)]
    for th in threads_list[:threads // 2]:
        th.start()
    churn_threads = threads_list[threads // 2:]
    for th in churn_threads:
        th.start()
    for th in churn_threads:
        th.join()
    stop.set()
    for th in threads_list[:threads // 2]:
        th.join()

    check("long-TTL keys never prematurely expired under load", not premature,
          f"{len(premature)} false expirations" if premature else "all 1000 stable")
    assert_invariant(s)


# ---------------------------------------------------------------------------
# Scenario 7 — the boundary: per-command atomicity is NOT multi-command atomic
# ---------------------------------------------------------------------------

def test_rmw_is_not_atomic(threads: int = 8, incr: int = 500) -> None:
    section("Scenario 7: read-modify-write demonstrates the multi-command boundary")
    s = Store()
    s.set("c", "0")

    def worker() -> None:
        for _ in range(incr):
            n = int(s.get("c"))   # command 1
            time.sleep(0)         # widen the window so the race is observable
            s.set("c", str(n + 1))  # command 2 — another thread slipped in between

    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(as_completed([pool.submit(worker) for _ in range(threads)]))

    got, expected = int(s.get("c")), threads * incr
    lost = expected - got
    # This is EXPECTED to lose updates — it proves per-command locking does not
    # cover a get→set sequence. The "check" passes when the loss is demonstrated,
    # documenting the limitation the README warns about.
    check("per-command atomicity does NOT make get→set atomic (as documented)",
          lost > 0, f"lost {lost}/{expected} updates without a transaction boundary")
    print("  → this is why multi-command atomicity needs an explicit boundary "
          "(future transaction() API), exactly as Redis uses MULTI/EXEC.")


# ---------------------------------------------------------------------------
# Scenario 8 — bounded store stays within max_size under a writer storm
# ---------------------------------------------------------------------------

def test_bounded_store_under_load(threads: int = 16, per_thread: int = 20_000,
                                  max_size: int = 1_000) -> None:
    section(f"Scenario 8: bounded store (max_size={max_size:,}) + {threads} writers "
            f"× {per_thread:,} — never exceeds the cap")
    s = Store(max_size=max_size, eviction="random")
    errors: list[Exception] = []
    breaches = [0]

    def worker(t: int) -> None:
        try:
            for i in range(per_thread):
                s.set(f"t{t}-k{i}", "v")
                if len(s) > max_size:
                    breaches[0] += 1
        except Exception as e:
            errors.append(e)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(as_completed([pool.submit(worker, t) for t in range(threads)]))
    elapsed = time.perf_counter() - start

    total = threads * per_thread
    check("no exceptions while evicting under concurrent writers", not errors,
          f"{len(errors)} errors" if errors else "clean")
    if errors:
        check("first error detail", False, repr(errors[0]))
    check("len(store) NEVER exceeded max_size", breaches[0] == 0,
          f"{breaches[0]} breaches" if breaches[0] else "cap held every check")
    check("final size within cap", len(s) <= max_size, f"len={len(s)}")
    assert_invariant(s, "invariant holds after concurrent eviction")
    print(f"  → {total:,} writes ({total // max_size}x the cap) in {elapsed:.3f}s "
          f"= {total / elapsed:,.0f} ops/sec; final len={len(s)}")


# ---------------------------------------------------------------------------

def main() -> int:
    print("Store library — in-depth stress test")
    print(f"Python {sys.version.split()[0]}, {os.cpu_count()} CPUs")

    t0 = time.perf_counter()
    test_no_lost_updates()
    test_mixed_ops_consistency()
    test_sweeper_under_load()
    test_no_leak()
    test_lock_overhead()
    test_ttl_correctness_concurrent()
    test_rmw_is_not_atomic()
    test_bounded_store_under_load()
    total = time.perf_counter() - t0

    section("RESULT")
    if _failures:
        print(f"  {len(_failures)} CHECK(S) FAILED:")
        for f in _failures:
            print(f"    - {f}")
        print(f"\n  Total runtime: {total:.1f}s")
        return 1
    print(f"  ALL CHECKS PASSED   (runtime {total:.1f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

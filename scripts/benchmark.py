#!/usr/bin/env python3
"""Benchmark the Store *library* (in-process, no sockets).

Measures three things people actually ask about for an in-memory KV store:
  1. Throughput  — ops/sec for set and get, single- and multi-threaded.
  2. Latency     — per-op time and tail percentiles.
  3. Memory      — bytes of overhead per key (measured with tracemalloc, which
                   tracks real Python allocations, not the shallow getsizeof).

Run:  python scripts/benchmark.py
The numbers are machine-specific; the script prints the host spec so they're
interpretable.
"""
import gc
import os
import statistics
import sys
import threading
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor

# Allow running from the repo without installing the package.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from vortis import Store  # noqa: E402


def _fmt(n: float) -> str:
    return f"{n:,.0f}"


def header() -> None:
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    print("=" * 68)
    print("Store library benchmark")
    print(f"Python {sys.version.split()[0]} | {os.cpu_count()} CPUs | "
          f"GIL {'on' if gil else 'off'}")
    print("=" * 68)


# ---------------------------------------------------------------------------
# 1. Single-threaded throughput (the raw per-core ceiling)
# ---------------------------------------------------------------------------

def bench_single_thread(n: int = 1_000_000) -> None:
    print(f"\n[1] Single-threaded throughput ({_fmt(n)} ops each)")

    # writes
    s = Store()
    start = time.perf_counter()
    for i in range(n):
        s.set(f"key:{i}", "value")
    set_dt = time.perf_counter() - start

    # reads (hit the keys we just wrote)
    start = time.perf_counter()
    for i in range(n):
        s.get(f"key:{i}")
    get_dt = time.perf_counter() - start

    print(f"    SET : {_fmt(n / set_dt):>12} ops/sec   ({set_dt * 1e9 / n:6.1f} ns/op)")
    print(f"    GET : {_fmt(n / get_dt):>12} ops/sec   ({get_dt * 1e9 / n:6.1f} ns/op)")

    # thread_safe=False (the server's mode) — shows the lock cost
    s2 = Store(thread_safe=False)
    start = time.perf_counter()
    for i in range(n):
        s2.set(f"key:{i}", "value")
    nolock_dt = time.perf_counter() - start
    print(f"    SET (thread_safe=False): {_fmt(n / nolock_dt):>12} ops/sec   "
          f"({nolock_dt * 1e9 / n:6.1f} ns/op)  -- server's lock-free mode")


# ---------------------------------------------------------------------------
# 2. Multi-threaded throughput (aggregate, under the GIL)
# ---------------------------------------------------------------------------

def bench_multi_thread(threads: int = 8, per_thread: int = 200_000) -> None:
    total = threads * per_thread
    print(f"\n[2] Multi-threaded throughput ({threads} threads x "
          f"{_fmt(per_thread)} = {_fmt(total)} ops)")
    s = Store()

    def writer(t: int) -> None:
        for i in range(per_thread):
            s.set(f"t{t}:k{i}", "v")

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(writer, range(threads)))
    dt = time.perf_counter() - start
    print(f"    SET (contended): {_fmt(total / dt):>12} ops/sec aggregate "
          f"({dt * 1e9 / total:6.1f} ns/op)")
    print(f"    note: with the GIL on, threads serialize CPU-bound work; this is")
    print(f"          aggregate throughput across {threads} threads, not {threads}x speedup.")


# ---------------------------------------------------------------------------
# 3. Latency distribution (per-op, with tail percentiles)
# ---------------------------------------------------------------------------

def bench_latency(samples: int = 100_000) -> None:
    print(f"\n[3] Latency distribution ({_fmt(samples)} samples)")
    s = Store()
    for i in range(samples):
        s.set(f"key:{i}", "value")

    # time individual GETs
    times = []
    for i in range(samples):
        t0 = time.perf_counter_ns()
        s.get(f"key:{i}")
        times.append(time.perf_counter_ns() - t0)
    times.sort()

    def pct(p: float) -> float:
        return times[int(len(times) * p)]

    print(f"    GET p50 : {pct(0.50):6.0f} ns")
    print(f"    GET p90 : {pct(0.90):6.0f} ns")
    print(f"    GET p99 : {pct(0.99):6.0f} ns")
    print(f"    GET mean: {statistics.mean(times):6.0f} ns")


# ---------------------------------------------------------------------------
# 4. Memory overhead per key (real allocations via tracemalloc)
# ---------------------------------------------------------------------------

def bench_memory(n: int = 500_000) -> None:
    print(f"\n[4] Memory overhead per key ({_fmt(n)} keys)")
    print("    (tracemalloc measures real allocations, not shallow getsizeof)")

    # Use short, fixed keys/values so we isolate STORE overhead from payload.
    # We subtract the payload's own size to report the store's bookkeeping cost.
    gc.collect()
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]

    s = Store()
    for i in range(n):
        s.set(str(i), "v")     # tiny payload
    peak_after = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()

    total = peak_after - base
    print(f"    total tracked: {total / 1e6:8.1f} MB for {_fmt(n)} keys")
    print(f"    per key      : {total / n:8.0f} bytes "
          f"(incl. the key string, the (value, None) tuple, and dict slots)")

    # Same, but for keys WITH a TTL — they also live in the `expires` index.
    gc.collect()
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    s2 = Store()
    for i in range(n):
        s2.set(str(i), "v", ex=3600)
    peak2 = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    total2 = peak2 - base
    print(f"    per key w/TTL: {total2 / n:8.0f} bytes "
          f"(adds the expires-index entry: key + float)")
    print(f"    TTL index adds ~{(total2 - total) / n:.0f} bytes/key")


def main() -> int:
    header()
    bench_single_thread()
    bench_multi_thread()
    bench_latency()
    bench_memory()
    print("\n" + "=" * 68)
    print("Done. Numbers are for THIS machine; re-run to compare environments.")
    print("=" * 68)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

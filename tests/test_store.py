"""Unit tests for the Store library API (Layer 1) — used directly, no RESP.

This is the in-process / embeddable usage: `from vortis.store import Store`.
"""
import threading
import time

import pytest

from vortis.store import Store


@pytest.fixture
def store():
    return Store()


# ---------------------------------------------------------------------------
# Basic set / get / delete
# ---------------------------------------------------------------------------

class TestBasics:
    def test_set_and_get(self, store):
        store.set("k", "v")
        assert store.get("k") == "v"

    def test_get_missing_returns_none(self, store):
        assert store.get("nope") is None

    def test_overwrite(self, store):
        store.set("k", "first")
        store.set("k", "second")
        assert store.get("k") == "second"

    def test_delete_returns_count(self, store):
        store.set("a", "1")
        store.set("b", "2")
        assert store.delete("a", "b", "missing") == 2
        assert store.get("a") is None

    def test_delete_missing_returns_zero(self, store):
        assert store.delete("ghost") == 0

    def test_contains(self, store):
        store.set("k", "v")
        assert "k" in store
        assert "nope" not in store

    def test_len(self, store):
        store.set("a", "1")
        store.set("b", "2")
        assert len(store) == 2


# ---------------------------------------------------------------------------
# TTL semantics
# ---------------------------------------------------------------------------

class TestTTL:
    def test_ex_alive_before_expiry(self, store):
        store.set("k", "v", ex=10)
        assert store.get("k") == "v"

    def test_ex_expired(self, store, monkeypatch):
        store.set("k", "v", ex=5)
        real = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real() + 10)
        assert store.get("k") is None

    def test_px_expired(self, store, monkeypatch):
        store.set("k", "v", px=100)
        real = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real() + 1)
        assert store.get("k") is None

    def test_ttl_set_indexes_key(self, store):
        store.set("k", "v", ex=5)
        assert "k" in store.expires

    def test_plain_set_clears_ttl(self, store, monkeypatch):
        store.set("k", "v1", ex=5)
        store.set("k", "v2")  # no TTL -> clears expiry
        assert "k" not in store.expires
        real = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real() + 10)
        assert store.get("k") == "v2"

    def test_ex_and_px_together_raises(self, store):
        with pytest.raises(ValueError):
            store.set("k", "v", ex=5, px=5000)


# ---------------------------------------------------------------------------
# Active expiration
# ---------------------------------------------------------------------------

class TestActiveExpire:
    def test_reclaims_expired(self, store, monkeypatch):
        for i in range(50):
            store.set(f"k{i}", "v", ex=5)
        real = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real() + 10)
        deleted = 0
        while store.expires:
            n = store.active_expire_cycle(time_budget_ms=100)
            if n == 0:
                break
            deleted += n
        assert deleted == 50
        assert len(store) == 0

    def test_leaves_live_keys(self, store):
        for i in range(50):
            store.set(f"k{i}", "v", ex=1000)
        store.active_expire_cycle(time_budget_ms=100)
        assert len(store) == 50

    def test_empty_noop(self, store):
        assert store.active_expire_cycle(time_budget_ms=100) == 0


# ---------------------------------------------------------------------------
# Isolation: two stores share no state
# ---------------------------------------------------------------------------

def test_two_stores_are_independent():
    a, b = Store(), Store()
    a.set("k", "in-a")
    assert b.get("k") is None
    assert a.get("k") == "in-a"


# ---------------------------------------------------------------------------
# Background active expiry (the library's own daemon thread)
# ---------------------------------------------------------------------------

class TestBackgroundExpiry:
    def test_reclaims_untouched_key(self):
        # The core problem: a key nobody reads again must still be reclaimed,
        # with no caller-driven cycle and no event loop.
        with Store(active_expiry=True, expiry_interval=0.02) as s:
            s.set("temp", "x", px=50)
            assert len(s) == 1
            deadline = time.monotonic() + 2.0
            while len(s) > 0 and time.monotonic() < deadline:
                time.sleep(0.02)
            assert len(s) == 0

    def test_default_has_no_thread(self):
        s = Store()
        assert s._sweeper.is_running() is False  # passive-only by default

    def test_start_is_idempotent(self):
        s = Store(active_expiry=True, expiry_interval=0.05)
        try:
            s.start_expiry()  # second call must not spawn another thread
            assert s._sweeper.is_running() is True
        finally:
            s.stop()

    def test_stop_is_idempotent_and_safe_without_start(self):
        s = Store()
        s.stop()  # never started — must not raise
        s2 = Store(active_expiry=True, expiry_interval=0.05)
        s2.stop()
        s2.stop()  # double stop is fine
        assert s2._sweeper.is_running() is False

    def test_context_manager_stops_thread(self):
        with Store(active_expiry=True, expiry_interval=0.05) as s:
            assert s._sweeper.is_running() is True
        assert s._sweeper.is_running() is False  # exited on __exit__

    def test_live_keys_survive_background_sweeps(self):
        with Store(active_expiry=True, expiry_interval=0.02) as s:
            s.set("keep", "v", ex=1000)
            time.sleep(0.1)  # several sweeps happen
            assert s.get("keep") == "v"

    def test_concurrent_writes_during_sweeps_do_not_crash(self):
        # Exercises the lock: caller mutates while the daemon iterates/deletes.
        with Store(active_expiry=True, expiry_interval=0.005) as s:
            for i in range(2000):
                s.set(f"k{i}", "v", px=1)  # all short-lived
            # Hammer the store while the sweep thread runs concurrently.
            for i in range(2000):
                s.set(f"live{i}", "v")
                s.get(f"k{i}")
            # No RuntimeError ("dict changed size during iteration") = pass.


# ---------------------------------------------------------------------------
# Thread-safety: the client never manages locks; the library guarantees
# per-command atomicity by default.
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_thread_safe_by_default(self):
        # A plain Store() must be safe to share across threads.
        s = Store()
        assert not isinstance(s._lock, __import__("contextlib").nullcontext)

    def test_opt_out_drops_the_lock(self):
        # A caller that knows it's single-threaded (the server) can opt out.
        s = Store(thread_safe=False)
        assert isinstance(s._lock, __import__("contextlib").nullcontext)

    def test_active_expiry_forces_lock_even_if_opted_out(self):
        # The background daemon needs the lock regardless of thread_safe.
        with Store(thread_safe=False, active_expiry=True, expiry_interval=0.05) as s:
            assert not isinstance(s._lock, __import__("contextlib").nullcontext)

    def test_start_expiry_later_upgrades_the_lock(self):
        # Opt out of the lock, then start expiry afterwards: start_expiry must
        # upgrade the no-op lock to a real one before spawning the thread.
        from contextlib import nullcontext
        s = Store(thread_safe=False)
        assert isinstance(s._lock, nullcontext)
        s.start_expiry()
        try:
            assert not isinstance(s._lock, nullcontext)
        finally:
            s.stop()

    def test_concurrent_writers_do_not_lose_updates(self):
        # 20 threads each write 500 distinct keys to one shared default Store.
        # With per-command atomicity, all 10_000 keys must survive — no torn
        # writes, no dropped keys from interleaving.
        s = Store()
        n_threads, per_thread = 20, 500

        def worker(t: int):
            for i in range(per_thread):
                s.set(f"t{t}-k{i}", "v")

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert len(s) == n_threads * per_thread

    def test_concurrent_mixed_ops_stay_consistent(self):
        # Mixed set/get/delete from many threads must never raise and must leave
        # the two internal dicts consistent (every TTL'd key is in `data`).
        s = Store()

        def worker(t: int):
            for i in range(300):
                key = f"t{t}-k{i}"
                s.set(key, "v", ex=1000)
                s.get(key)
                if i % 2 == 0:
                    s.delete(key)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(10)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        # Invariant: every key with a TTL entry still exists in the keyspace.
        for key in list(s.expires):
            assert key in s.data

"""Tests for size bounding + eviction (Strategy pattern).

Covers the eviction.py strategies directly and their integration with Store via
``max_size`` / ``eviction``. This phase ships Random eviction + key-count sizing.
"""
import threading

import pytest

from vortis.eviction import (
    EVICTION_SAMPLES,
    KeyCountSizer,
    NoEvictionPolicy,
    RandomPolicy,
    make_policy,
)
from vortis.store import Store


# ---------------------------------------------------------------------------
# Sizer
# ---------------------------------------------------------------------------

class TestKeyCountSizer:
    def test_current_is_len(self):
        sizer = KeyCountSizer()
        assert sizer.current({}) == 0
        assert sizer.current({"a": 1, "b": 2}) == 2

    def test_cost_is_one(self):
        sizer = KeyCountSizer()
        assert sizer.cost("k", "some long value") == 1


# ---------------------------------------------------------------------------
# Policy factory + policies in isolation
# ---------------------------------------------------------------------------

class TestPolicyFactory:
    def test_make_random(self):
        assert isinstance(make_policy("random"), RandomPolicy)

    def test_make_noeviction(self):
        assert isinstance(make_policy("noeviction"), NoEvictionPolicy)

    def test_unknown_policy_raises(self):
        with pytest.raises(ValueError, match="unknown eviction policy"):
            make_policy("bogus")

    def test_unknown_policy_lists_supported(self):
        with pytest.raises(ValueError, match="random"):
            make_policy("bogus")


class TestPolicies:
    def test_random_picks_from_sample(self):
        p = RandomPolicy()
        assert p.evict(["a", "b", "c"]) in {"a", "b", "c"}

    def test_random_empty_sample_returns_none(self):
        assert RandomPolicy().evict([]) is None

    def test_noeviction_never_evicts(self):
        assert NoEvictionPolicy().evict(["a", "b"]) is None

    def test_random_is_stateless_hooks_are_noops(self):
        p = RandomPolicy()
        assert p.tracks_access is False
        # hooks must exist and do nothing harmful
        p.note_write("k")
        p.note_access("k")
        p.note_remove("k")


# ---------------------------------------------------------------------------
# Store integration — bounding
# ---------------------------------------------------------------------------

class TestBoundedStore:
    def test_limit_strictly_enforced(self):
        s = Store(max_size=5, eviction="random")
        for i in range(100):
            s.set(f"k{i}", str(i))
            assert len(s) <= 5
        assert len(s) == 5

    def test_eviction_keeps_invariant_expires_subset_data(self):
        s = Store(max_size=3)
        for i in range(50):
            s.set(f"k{i}", "v", ex=100)  # all volatile
        # every TTL-indexed key must still be a real key
        assert set(s.expires) <= set(s.data)
        assert len(s.data) == 3
        assert len(s.expires) == 3

    def test_overwrite_does_not_evict(self):
        s = Store(max_size=2)
        s.set("a", "1")
        s.set("b", "2")
        s.set("a", "99")  # overwrite — must not push out "b"
        assert len(s) == 2
        assert s.get("a") == "99"
        assert s.get("b") == "2"

    def test_mixed_ttl_and_plain_keys(self):
        s = Store(max_size=4)
        s.set("p1", "v")            # plain
        s.set("t1", "v", ex=100)    # volatile
        for i in range(10):
            s.set(f"k{i}", "v")
        assert len(s) == 4
        assert set(s.expires) <= set(s.data)

    def test_recently_set_key_is_retrievable(self):
        # The just-inserted key should survive its own eviction round.
        s = Store(max_size=3, eviction="random")
        for i in range(20):
            s.set(f"k{i}", str(i))
            assert s.get(f"k{i}") == str(i)  # the key we just set is present

    def test_default_eviction_is_random(self):
        s = Store(max_size=2)
        assert isinstance(s._policy, RandomPolicy)


class TestNoEviction:
    def test_noeviction_grows_past_limit(self):
        # Null-object policy: never evicts, so the store grows (documented).
        s = Store(max_size=2, eviction="noeviction")
        for i in range(5):
            s.set(f"k{i}", "v")
        assert len(s) == 5


# ---------------------------------------------------------------------------
# Zero overhead when unbounded
# ---------------------------------------------------------------------------

class TestUnboundedUnchanged:
    def test_default_store_is_unbounded(self):
        s = Store()
        assert s._bounded is False
        assert s._max_size is None

    def test_unbounded_store_has_no_policy(self):
        s = Store()
        assert not hasattr(s, "_policy")
        assert not hasattr(s, "_sizer")

    def test_unbounded_store_grows_freely(self):
        s = Store()
        for i in range(1000):
            s.set(f"k{i}", "v")
        assert len(s) == 1000


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_zero_max_size_rejected(self):
        with pytest.raises(ValueError, match="positive"):
            Store(max_size=0)

    def test_negative_max_size_rejected(self):
        with pytest.raises(ValueError, match="positive"):
            Store(max_size=-5)

    def test_unknown_eviction_rejected_at_construction(self):
        with pytest.raises(ValueError, match="unknown eviction policy"):
            Store(max_size=5, eviction="bogus")


# ---------------------------------------------------------------------------
# Concurrency — bounded store stays bounded and never raises
# ---------------------------------------------------------------------------

class TestBoundedConcurrency:
    def test_stays_bounded_under_concurrent_writers(self):
        s = Store(max_size=100)
        errors: list[Exception] = []

        def worker(t: int) -> None:
            try:
                for i in range(2000):
                    s.set(f"t{t}-k{i}", "v")
                    assert len(s) <= 100
            except Exception as e:  # includes the assert above
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert not errors, f"errors under load: {errors[:3]}"
        assert len(s) <= 100
        assert set(s.expires) <= set(s.data)


def test_eviction_samples_constant_sane():
    # Guard against an accidental zero/negative that would break sampling.
    assert EVICTION_SAMPLES >= 1


# ---------------------------------------------------------------------------
# Lifecycle hooks: a stateful (access-tracking) policy is notified correctly.
# No shipped policy sets tracks_access yet, so this also documents the seam a
# future LRU/LFU will use — and exercises Store's note_access call path.
# ---------------------------------------------------------------------------

class _RecordingPolicy(NoEvictionPolicy):
    """Test double: records lifecycle events, opts into access tracking."""

    tracks_access = True

    def __init__(self):
        self.writes: list[str] = []
        self.accesses: list[str] = []
        self.removes: list[str] = []

    def note_write(self, key):
        self.writes.append(key)

    def note_access(self, key):
        self.accesses.append(key)

    def note_remove(self, key):
        self.removes.append(key)


class TestLifecycleHooks:
    def _store_with(self, policy):
        # Build a bounded store, then swap in the recording policy so we can
        # assert on the hook calls Store makes.
        s = Store(max_size=10)
        s._policy = policy
        return s

    def test_note_write_on_set(self):
        p = _RecordingPolicy()
        s = self._store_with(p)
        s.set("a", "1")
        assert p.writes == ["a"]

    def test_note_access_on_get_hit_when_tracking(self):
        p = _RecordingPolicy()
        s = self._store_with(p)
        s.set("a", "1")
        s.get("a")            # hit -> note_access
        s.get("missing")      # miss -> no note_access
        assert p.accesses == ["a"]

    def test_note_remove_on_delete(self):
        p = _RecordingPolicy()
        s = self._store_with(p)
        s.set("a", "1")
        s.delete("a")
        assert p.removes == ["a"]

    def test_stateless_policy_skips_note_access(self):
        # Random has tracks_access False, so Store must not call note_access.
        s = Store(max_size=10, eviction="random")
        assert s._policy.tracks_access is False

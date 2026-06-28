"""The EvictionPolicy strategy interface.

An eviction policy decides which key a bounded Store should drop once it is
full. Policies are interchangeable (Strategy pattern): adding a new one means
adding a new module under ``eviction/policies/`` and registering it in the
factory — never editing Store (Open/Closed).
"""
from abc import ABC, abstractmethod

# Number of keys sampled per eviction decision. Mirrors Redis's
# `maxmemory-samples` (default 5): evict the best victim from a small random
# sample rather than scanning the whole keyspace, for bounded latency.
EVICTION_SAMPLES = 5


class EvictionPolicy(ABC):
    """Strategy for choosing which key to evict when the store is full.

    Lifecycle hooks (``note_*``) default to no-ops so stateless policies need
    not implement them; a stateful policy (e.g. a future LFU counter) overrides
    them to maintain its own metadata. Store calls these only while holding its
    lock, so policies never need their own synchronization.
    """

    #: If False, Store skips note_access entirely on the read hot path.
    tracks_access: bool = False

    def note_write(self, key: str) -> None:
        """A key was inserted or overwritten."""

    def note_access(self, key: str) -> None:
        """A key was read (hit). Only called when ``tracks_access`` is True."""

    def note_remove(self, key: str) -> None:
        """A key was removed (by delete, expiry, or eviction)."""

    @abstractmethod
    def evict(self, sample: list[str]) -> str | None:
        """Choose a victim key from a random ``sample``, or None if empty.

        Store passes a pre-drawn random sample (never the whole keyspace), so a
        policy never scans or copies all keys — matching Redis's sampling.
        """

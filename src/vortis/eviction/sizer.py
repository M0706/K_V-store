"""Sizer strategy — how a bounded Store measures its own size.

Abstracting "size" lets the limit be a key count today and an approximate byte
total tomorrow (a future ``BytesSizer``) without changing Store or its public
API — only the cost function differs.
"""
from abc import ABC, abstractmethod


class Sizer(ABC):
    """Strategy for measuring store size and the cost of a single entry."""

    @abstractmethod
    def current(self, data: dict) -> int:
        """Current size of the store, in this sizer's units."""

    @abstractmethod
    def cost(self, key: str, value: str) -> int:
        """Size contribution of a single key/value, in this sizer's units."""


class KeyCountSizer(Sizer):
    """Measure size as the number of keys. Exact and O(1) (just ``len``)."""

    def current(self, data: dict) -> int:
        return len(data)

    def cost(self, key: str, value: str) -> int:
        return 1


# A future BytesSizer would estimate len(key)+len(value)+overhead and maintain a
# running total — addable here with no change to Store or its public API.

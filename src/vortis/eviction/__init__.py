"""Eviction strategies for a size-bounded Store.

When a Store is created with a ``max_size``, it must decide what to drop once
the limit is reached. That decision is a classic Strategy pattern: a family of
interchangeable algorithms (Random now; LRU/LFU/etc. later) behind one
interface, so new policies can be added without touching Store (Open/Closed).

Layout (one policy per module):

    eviction/
        base.py                  EvictionPolicy ABC + EVICTION_SAMPLES
        sizer.py                 Sizer ABC + KeyCountSizer
        policies/
            noeviction.py        NoEvictionPolicy
            random_policy.py     RandomPolicy

This package's public API is the ABCs, the shipped concrete strategies, and the
``make_policy`` factory.
"""
from vortis.eviction.base import EVICTION_SAMPLES, EvictionPolicy
from vortis.eviction.policies import NoEvictionPolicy, RandomPolicy
from vortis.eviction.sizer import KeyCountSizer, Sizer

__all__ = [
    "EVICTION_SAMPLES",
    "EvictionPolicy",
    "Sizer",
    "KeyCountSizer",
    "NoEvictionPolicy",
    "RandomPolicy",
    "make_policy",
]

# Registry maps policy name -> class. Add a new policy by importing its class
# and adding one entry here; nothing else changes (Open/Closed).
_POLICIES: dict[str, type[EvictionPolicy]] = {
    "random": RandomPolicy,
    "noeviction": NoEvictionPolicy,
}


def make_policy(name: str) -> EvictionPolicy:
    """Resolve an eviction-policy name to a fresh policy instance.

    Raises ValueError on an unknown name, listing the supported policies.
    """
    try:
        return _POLICIES[name]()
    except KeyError:
        supported = ", ".join(sorted(_POLICIES))
        raise ValueError(
            f"unknown eviction policy {name!r}; supported: {supported}") from None

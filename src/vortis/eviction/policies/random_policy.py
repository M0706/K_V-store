"""RandomPolicy — evict a random key (Redis's ``allkeys-random``).

Named ``random_policy`` rather than ``random`` to avoid shadowing the standard
library ``random`` module from within this package.
"""
from vortis.eviction.base import EvictionPolicy


class RandomPolicy(EvictionPolicy):
    """Evict a random key.

    Stateless: no per-key memory, no per-access work. The sample handed in by
    Store is already drawn at random, so the first element is itself a uniformly
    random victim.
    """

    def evict(self, sample: list[str]) -> str | None:
        return sample[0] if sample else None

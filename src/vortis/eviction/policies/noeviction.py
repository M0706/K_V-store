"""NoEvictionPolicy — the null-object policy that never evicts."""
from vortis.eviction.base import EvictionPolicy


class NoEvictionPolicy(EvictionPolicy):
    """Never evicts (Redis's ``noeviction``).

    Used when a store is bounded but configured not to drop keys; the store
    then simply grows past its limit. A null object, so Store needs no special
    casing.
    """

    def evict(self, sample: list[str]) -> str | None:
        return None

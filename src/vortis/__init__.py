"""vortis — a fast in-memory key-value store with TTL, active expiry, and eviction.

Use it two ways:

    # As a library (in-process):
    from vortis import Store
    s = Store(max_size=10_000, eviction="random")
    s.set("session", "abc", ex=60)

    # As a server (RESP over TCP, any Redis client can connect):
    from vortis import serve
    serve(port=6379)
"""
from vortis.async_tcp import AsyncTCPServer, serve
from vortis.store import Store

__all__ = ["Store", "serve", "AsyncTCPServer"]

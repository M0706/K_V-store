import random
import time
from dataclasses import dataclass, field

from resp import encode, parse_resp

# (value, expires_at_monotonic_seconds | None)
store: dict[str, tuple[str, float | None]] = {}

# Secondary index: key -> expires_at, for keys that have a TTL only.
# Mirrors Redis's redisDb.expires dict. The active-expire cycle samples from
# THIS dict so its cost scales with the number of volatile keys, not the
# total keyspace. Must be kept in sync with `store` on every mutation.
expires: dict[str, float] = {}


def _del_key(key: str) -> None:
    """Remove a key from both the keyspace and the TTL index."""
    store.pop(key, None)
    expires.pop(key, None)


@dataclass
class RedisCmd:
    cmd: str
    args: list[str] = field(default_factory=list)


def eval_ping(args: list[str]) -> bytes:
    if len(args) >= 2:
        return b"-ERR wrong number of arguments for 'ping' command\r\n"
    if len(args) == 0:
        return encode("PONG", is_simple=True)
    return encode(args[0], is_simple=False)


def eval_echo(args: list[str]) -> bytes:
    if len(args) != 1:
        return b"-ERR wrong number of arguments for 'echo' command\r\n"
    return encode(args[0], is_simple=False)


def _lookup(key: str) -> str | None:
    """Return the value for key, or None if missing or expired (passive deletion)."""
    entry = store.get(key)
    if entry is None:
        return None
    value, expires_at = entry
    if expires_at is not None and time.monotonic() >= expires_at:
        _del_key(key)
        return None
    return value


def eval_set(args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'set' command\r\n"
    key, value = args[0], args[1]
    expires_at: float | None = None
    i = 2
    while i < len(args):
        opt = args[i].upper()
        if opt in ("EX", "PX"):
            if i + 1 >= len(args):
                return b"-ERR syntax error\r\n"
            try:
                ttl = int(args[i + 1])
            except ValueError:
                return b"-ERR value is not an integer or out of range\r\n"
            if ttl <= 0:
                return b"-ERR invalid expire time in 'set' command\r\n"
            # EX is seconds; PX is milliseconds — both stored as monotonic seconds
            expires_at = time.monotonic() + (ttl if opt == "EX" else ttl / 1000)
            i += 2
        else:
            return b"-ERR syntax error\r\n"
    store[key] = (value, expires_at)
    # Keep the TTL index in sync: a SET with EX/PX records the expiry, while a
    # plain SET over an existing key must DROP any previous TTL (Redis behaviour).
    if expires_at is not None:
        expires[key] = expires_at
    else:
        expires.pop(key, None)
    return b"+OK\r\n"


def eval_get(args: list[str]) -> bytes:
    if len(args) != 1:
        return b"-ERR wrong number of arguments for 'get' command\r\n"
    val = _lookup(args[0])
    if val is None:
        return b"$-1\r\n"
    return encode(val, is_simple=False)


def eval_del(args: list[str]) -> bytes:
    if len(args) < 1:
        return b"-ERR wrong number of arguments for 'del' command\r\n"
    deleted = 0
    for key in args:
        # Use _lookup to honour passive expiry: an expired key is not "present"
        if _lookup(key) is not None:
            _del_key(key)
            deleted += 1
    return f":{deleted}\r\n".encode()


# ---------------------------------------------------------------------------
# Active expiration (Redis's activeExpireCycle)
#
# Passive expiry in _lookup only deletes keys when someone touches them, so a
# key nobody reads again would leak forever. This cycle proactively reclaims
# them WITHOUT scanning the whole keyspace and WITHOUT blocking the event loop:
#
#   1. Sample up to KEYS_PER_LOOP random keys from the TTL index.
#   2. Delete the expired ones.
#   3. If more than ACCEPTABLE_STALE of the sample was expired, the keyspace is
#      probably dirty -> loop again immediately. Otherwise stop.
#   4. Regardless, bail the moment we exceed the per-call time budget so the
#      main loop gets the thread back.
# ---------------------------------------------------------------------------

KEYS_PER_LOOP = 20      # ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP
ACCEPTABLE_STALE = 0.25  # continue while >25% of the sample is expired


def active_expire_cycle(time_budget_ms: float = 1.0) -> int:
    """Reclaim expired keys cooperatively. Returns the number deleted."""
    deadline = time.monotonic() + time_budget_ms / 1000
    total_deleted = 0

    while expires:
        now = time.monotonic()
        if now >= deadline:
            break  # out of time — leftover keys wait for the next tick

        keys = list(expires.keys())
        sample_size = min(KEYS_PER_LOOP, len(keys))
        sample = random.sample(keys, sample_size)

        expired = 0
        for key in sample:
            if now >= expires.get(key, float("inf")):
                _del_key(key)
                expired += 1
        total_deleted += expired

        # If the sample was mostly fresh, the rest of the keyspace probably is
        # too — stop and let passive expiry handle the stragglers.
        if expired <= sample_size * ACCEPTABLE_STALE:
            break

    return total_deleted


def eval_and_respond(cmd: RedisCmd) -> bytes:
    print(f"Command: {cmd.cmd} args: {cmd.args}")
    if cmd.cmd == "PING":
        return eval_ping(cmd.args)
    elif cmd.cmd == "ECHO":
        return eval_echo(cmd.args)
    elif cmd.cmd == "SET":
        return eval_set(cmd.args)
    elif cmd.cmd == "GET":
        return eval_get(cmd.args)
    elif cmd.cmd == "DEL":
        return eval_del(cmd.args)
    elif cmd.cmd == "CLIENT":
        # redis-benchmark sends CLIENT SETNAME during handshake
        return b"+OK\r\n"
    elif cmd.cmd == "CONFIG":
        # redis-benchmark expects a 2-element bulk array for CONFIG GET
        if len(cmd.args) >= 2 and cmd.args[0].upper() == "GET":
            key = cmd.args[1]
            return f"*2\r\n${len(key)}\r\n{key}\r\n$0\r\n\r\n".encode()
        return b"*0\r\n"
    elif cmd.cmd == "COMMAND":
        # redis-cli sends this on connect to introspect the server — return empty array
        return b"*0\r\n"
    else:
        return f"-ERR unknown command '{cmd.cmd}'\r\n".encode()


def process_input(data: bytes) -> bytes:
    print(f"Raw: {data}")
    tokens = parse_resp(data)
    if tokens is None or len(tokens) == 0:
        return b"-ERR Protocol error: expected RESP array\r\n"
    cmd = RedisCmd(cmd=tokens[0].upper(), args=tokens[1:])
    return eval_and_respond(cmd)

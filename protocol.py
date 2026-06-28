"""RESP command layer: translate parsed commands into operations on a Store.

This is the bytes <-> core adapter. Each ``eval_*`` function takes the target
``Store`` plus the command arguments and returns a raw RESP-encoded reply.
The store holds all state; this module is stateless.
"""
from dataclasses import dataclass, field

from resp import encode, parse_resp
from store import Store


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


def eval_set(store: Store, args: list[str]) -> bytes:
    if len(args) < 2:
        return b"-ERR wrong number of arguments for 'set' command\r\n"
    key, value = args[0], args[1]
    ex: int | None = None
    px: int | None = None
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
            if opt == "EX":
                ex = ttl
            else:
                px = ttl
            i += 2
        else:
            return b"-ERR syntax error\r\n"
    store.set(key, value, ex=ex, px=px)
    return b"+OK\r\n"


def eval_get(store: Store, args: list[str]) -> bytes:
    if len(args) != 1:
        return b"-ERR wrong number of arguments for 'get' command\r\n"
    val = store.get(args[0])
    if val is None:
        return b"$-1\r\n"
    return encode(val, is_simple=False)


def eval_del(store: Store, args: list[str]) -> bytes:
    if len(args) < 1:
        return b"-ERR wrong number of arguments for 'del' command\r\n"
    return f":{store.delete(*args)}\r\n".encode()


def eval_and_respond(store: Store, cmd: RedisCmd) -> bytes:
    if cmd.cmd == "PING":
        return eval_ping(cmd.args)
    elif cmd.cmd == "ECHO":
        return eval_echo(cmd.args)
    elif cmd.cmd == "SET":
        return eval_set(store, cmd.args)
    elif cmd.cmd == "GET":
        return eval_get(store, cmd.args)
    elif cmd.cmd == "DEL":
        return eval_del(store, cmd.args)
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


def process_input(store: Store, data: bytes) -> bytes:
    tokens = parse_resp(data)
    if tokens is None or len(tokens) == 0:
        return b"-ERR Protocol error: expected RESP array\r\n"
    cmd = RedisCmd(cmd=tokens[0].upper(), args=tokens[1:])
    return eval_and_respond(store, cmd)

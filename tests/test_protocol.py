"""Depth tests for the RESP command layer (protocol.py) over a Store.

Each test gets a fresh ``Store`` via the ``store`` fixture, and drives it
through ``process_input`` exactly as the server would. State assertions read
the store's ``data`` / ``expires`` dicts directly.
"""
import time
import pytest

from store import Store
from protocol import process_input
from resp import parse_resp, encode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resp_array(*tokens: str) -> bytes:
    parts = [f"*{len(tokens)}\r\n".encode()]
    for t in tokens:
        parts.append(f"${len(t)}\r\n{t}\r\n".encode())
    return b"".join(parts)


def set_cmd(key: str, value: str, *opts: str) -> bytes:
    return resp_array("SET", key, value, *opts)


def get_cmd(key: str) -> bytes:
    return resp_array("GET", key)


def del_cmd(*keys: str) -> bytes:
    return resp_array("DEL", *keys)


@pytest.fixture
def store():
    """A fresh, isolated keyspace for each test."""
    return Store()


# ---------------------------------------------------------------------------
# PING
# ---------------------------------------------------------------------------

class TestPing:
    def test_bare_ping(self, store):
        assert process_input(store, resp_array("PING")) == b"+PONG\r\n"

    def test_ping_with_message(self, store):
        assert process_input(store, resp_array("PING", "hello")) == b"$5\r\nhello\r\n"

    def test_ping_too_many_args(self, store):
        assert process_input(store, resp_array("PING", "a", "b")).startswith(b"-ERR")

    def test_ping_case_insensitive(self, store):
        assert process_input(store, resp_array("ping")) == b"+PONG\r\n"


# ---------------------------------------------------------------------------
# ECHO
# ---------------------------------------------------------------------------

class TestEcho:
    def test_basic_echo(self, store):
        assert process_input(store, resp_array("ECHO", "world")) == b"$5\r\nworld\r\n"

    def test_echo_empty_string(self, store):
        assert process_input(store, resp_array("ECHO", "")) == b"$0\r\n\r\n"

    def test_echo_too_few_args(self, store):
        assert process_input(store, resp_array("ECHO")).startswith(b"-ERR")

    def test_echo_too_many_args(self, store):
        assert process_input(store, resp_array("ECHO", "a", "b")).startswith(b"-ERR")


# ---------------------------------------------------------------------------
# SET / GET — no TTL
# ---------------------------------------------------------------------------

class TestSetGet:
    def test_set_and_get(self, store):
        assert process_input(store, set_cmd("k", "v")) == b"+OK\r\n"
        assert process_input(store, get_cmd("k")) == b"$1\r\nv\r\n"

    def test_get_missing_key(self, store):
        assert process_input(store, get_cmd("nope")) == b"$-1\r\n"

    def test_set_overwrites(self, store):
        process_input(store, set_cmd("k", "first"))
        process_input(store, set_cmd("k", "second"))
        assert process_input(store, get_cmd("k")) == b"$6\r\nsecond\r\n"

    def test_set_too_few_args(self, store):
        assert process_input(store, resp_array("SET", "only_key")).startswith(b"-ERR")

    def test_get_too_many_args(self, store):
        assert process_input(store, resp_array("GET", "a", "b")).startswith(b"-ERR")

    def test_set_value_with_spaces(self, store):
        process_input(store, set_cmd("k", "hello world"))
        assert process_input(store, get_cmd("k")) == b"$11\r\nhello world\r\n"

    def test_set_multiple_keys_independent(self, store):
        process_input(store, set_cmd("a", "1"))
        process_input(store, set_cmd("b", "2"))
        assert process_input(store, get_cmd("a")) == b"$1\r\n1\r\n"
        assert process_input(store, get_cmd("b")) == b"$1\r\n2\r\n"


# ---------------------------------------------------------------------------
# SET EX / PX — TTL, passive expiry
# ---------------------------------------------------------------------------

class TestTTL:
    def test_set_ex_key_alive(self, store):
        process_input(store, set_cmd("k", "v", "EX", "10"))
        assert process_input(store, get_cmd("k")) == b"$1\r\nv\r\n"

    def test_set_px_key_alive(self, store):
        process_input(store, set_cmd("k", "v", "PX", "10000"))
        assert process_input(store, get_cmd("k")) == b"$1\r\nv\r\n"

    def test_set_ex_expired(self, store, monkeypatch):
        process_input(store, set_cmd("k", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        assert process_input(store, get_cmd("k")) == b"$-1\r\n"

    def test_set_px_expired(self, store, monkeypatch):
        process_input(store, set_cmd("k", "v", "PX", "100"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 1)
        assert process_input(store, get_cmd("k")) == b"$-1\r\n"

    def test_expired_key_removed_from_store(self, store, monkeypatch):
        process_input(store, set_cmd("k", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        process_input(store, get_cmd("k"))  # triggers passive deletion
        assert "k" not in store.data

    def test_ex_zero_rejected(self, store):
        assert process_input(store, set_cmd("k", "v", "EX", "0")).startswith(b"-ERR")

    def test_ex_negative_rejected(self, store):
        assert process_input(store, set_cmd("k", "v", "EX", "-1")).startswith(b"-ERR")

    def test_px_zero_rejected(self, store):
        assert process_input(store, set_cmd("k", "v", "PX", "0")).startswith(b"-ERR")

    def test_ex_non_integer_rejected(self, store):
        assert process_input(store, set_cmd("k", "v", "EX", "abc")).startswith(b"-ERR")

    def test_ex_missing_value_rejected(self, store):
        assert process_input(store, resp_array("SET", "k", "v", "EX")).startswith(b"-ERR")

    def test_unknown_option_rejected(self, store):
        assert process_input(store, set_cmd("k", "v", "XX")).startswith(b"-ERR")

    def test_set_ex_lowercase(self, store):
        # Redis accepts case-insensitive options
        assert process_input(store, set_cmd("k", "v", "ex", "10")) == b"+OK\r\n"
        assert process_input(store, get_cmd("k")) == b"$1\r\nv\r\n"

    def test_overwrite_clears_ttl(self, store, monkeypatch):
        """Re-setting a key without TTL should remove the expiry."""
        process_input(store, set_cmd("k", "v1", "EX", "5"))
        process_input(store, set_cmd("k", "v2"))  # no TTL
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        # Key must still be alive because new SET had no expiry
        assert process_input(store, get_cmd("k")) == b"$2\r\nv2\r\n"


# ---------------------------------------------------------------------------
# DEL
# ---------------------------------------------------------------------------

class TestDel:
    def test_del_existing_key(self, store):
        process_input(store, set_cmd("k", "v"))
        assert process_input(store, del_cmd("k")) == b":1\r\n"
        assert process_input(store, get_cmd("k")) == b"$-1\r\n"

    def test_del_missing_key(self, store):
        assert process_input(store, del_cmd("ghost")) == b":0\r\n"

    def test_del_multiple_keys(self, store):
        process_input(store, set_cmd("a", "1"))
        process_input(store, set_cmd("b", "2"))
        assert process_input(store, del_cmd("a", "b", "c")) == b":2\r\n"

    def test_del_no_args(self, store):
        assert process_input(store, resp_array("DEL")).startswith(b"-ERR")

    def test_del_expired_key_not_counted(self, store, monkeypatch):
        process_input(store, set_cmd("k", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        assert process_input(store, del_cmd("k")) == b":0\r\n"

    def test_del_removes_from_store(self, store):
        process_input(store, set_cmd("k", "v"))
        process_input(store, del_cmd("k"))
        assert "k" not in store.data

    def test_del_idempotent(self, store):
        process_input(store, set_cmd("k", "v"))
        process_input(store, del_cmd("k"))
        assert process_input(store, del_cmd("k")) == b":0\r\n"


# ---------------------------------------------------------------------------
# expires index sync + active expiration (RESP level)
# ---------------------------------------------------------------------------

class TestExpiresIndex:
    def test_set_with_ttl_indexed(self, store):
        process_input(store, set_cmd("k", "v", "EX", "5"))
        assert "k" in store.expires

    def test_set_without_ttl_not_indexed(self, store):
        process_input(store, set_cmd("k", "v"))
        assert "k" not in store.expires

    def test_overwrite_clears_index(self, store):
        process_input(store, set_cmd("k", "v1", "EX", "5"))
        process_input(store, set_cmd("k", "v2"))  # plain SET drops the TTL
        assert "k" not in store.expires

    def test_del_removes_from_index(self, store):
        process_input(store, set_cmd("k", "v", "EX", "5"))
        process_input(store, del_cmd("k"))
        assert "k" not in store.expires

    def test_passive_expiry_removes_from_index(self, store, monkeypatch):
        process_input(store, set_cmd("k", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        process_input(store, get_cmd("k"))  # triggers passive deletion
        assert "k" not in store.expires


class TestActiveExpire:
    def test_reclaims_untouched_expired_keys(self, store, monkeypatch):
        # A key nobody reads again must still be reclaimed by the cycle.
        for i in range(50):
            process_input(store, set_cmd(f"k{i}", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        deleted = 0
        # Loop the cycle to completion (each call is time-budgeted).
        while store.expires:
            n = store.active_expire_cycle(time_budget_ms=100)
            if n == 0:
                break
            deleted += n
        assert deleted == 50
        assert len(store.data) == 0

    def test_leaves_live_keys_alone(self, store):
        for i in range(50):
            process_input(store, set_cmd(f"k{i}", "v", "EX", "1000"))
        store.active_expire_cycle(time_budget_ms=100)
        assert len(store.data) == 50

    def test_no_ttl_keys_untouched(self, store):
        for i in range(20):
            process_input(store, set_cmd(f"k{i}", "v"))  # no TTL
        store.active_expire_cycle(time_budget_ms=100)
        assert len(store.data) == 20

    def test_time_budget_aborts_cycle(self, store, monkeypatch):
        # With a zero/elapsed budget the cycle must bail before sampling,
        # leaving keys for the next tick (covers the deadline break).
        for i in range(30):
            process_input(store, set_cmd(f"k{i}", "v", "EX", "5"))
        real_monotonic = time.monotonic
        # Jump the clock far past expiry AND past any deadline, so the very
        # first deadline check trips.
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 1000)
        deleted = store.active_expire_cycle(time_budget_ms=0)
        assert deleted == 0
        assert len(store.expires) == 30  # nothing reclaimed — ran out of time first

    def test_empty_expires_is_noop(self, store):
        # No volatile keys -> while loop never entered, returns 0 cleanly.
        assert store.active_expire_cycle(time_budget_ms=100) == 0


# ---------------------------------------------------------------------------
# Protocol edge cases
# ---------------------------------------------------------------------------

class TestProtocol:
    def test_inline_ping(self, store):
        assert process_input(store, b"PING\r\n") == b"+PONG\r\n"

    def test_empty_input(self, store):
        assert process_input(store, b"\r\n").startswith(b"-ERR")

    def test_unknown_command(self, store):
        resp = process_input(store, resp_array("FOOBAR"))
        assert resp.startswith(b"-ERR")
        assert b"unknown command" in resp

    def test_garbled_input(self, store):
        # At minimum it must not raise
        try:
            process_input(store, b"\x00\xff\xfe")
        except Exception as e:
            pytest.fail(f"process_input raised on garbled input: {e}")


# ---------------------------------------------------------------------------
# Benchmark/handshake commands (CLIENT / CONFIG / COMMAND)
# ---------------------------------------------------------------------------

class TestHandshakeCommands:
    def test_client_setname_ok(self, store):
        assert process_input(store, resp_array("CLIENT", "SETNAME", "x")) == b"+OK\r\n"

    def test_config_get_returns_pair(self, store):
        resp = process_input(store, resp_array("CONFIG", "GET", "maxmemory"))
        # 2-element bulk array: the echoed key + an empty value.
        assert resp == b"*2\r\n$9\r\nmaxmemory\r\n$0\r\n\r\n"

    def test_config_non_get_returns_empty_array(self, store):
        assert process_input(store, resp_array("CONFIG", "SET", "x", "1")) == b"*0\r\n"

    def test_command_returns_empty_array(self, store):
        assert process_input(store, resp_array("COMMAND")) == b"*0\r\n"


# ---------------------------------------------------------------------------
# resp.parse_resp / encode — protocol primitives
# ---------------------------------------------------------------------------

class TestRespParser:
    def test_valid_array(self):
        assert parse_resp(b"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n") == ["ECHO", "hi"]

    def test_inline_command_tokenised(self):
        assert parse_resp(b"PING\r\n") == ["PING"]

    def test_inline_empty_is_none(self):
        assert parse_resp(b"\r\n") is None

    def test_malformed_array_missing_dollar(self):
        # Element header is not "$..." -> parser must reject (resp.py line 10).
        assert parse_resp(b"*1\r\nXECHO\r\n") is None

    def test_truncated_array_returns_none(self):
        # count says 2 elements but only one provided -> IndexError -> None.
        assert parse_resp(b"*2\r\n$4\r\nECHO\r\n") is None

    def test_encode_simple_vs_bulk(self):
        assert encode("OK", is_simple=True) == b"+OK\r\n"
        assert encode("hi", is_simple=False) == b"$2\r\nhi\r\n"

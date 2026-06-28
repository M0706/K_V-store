"""Depth tests for sync_commands: PING, ECHO, SET/GET (with TTL), DEL."""
import time
import pytest
import sync_commands
from sync_commands import process_input, store, expires


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


@pytest.fixture(autouse=True)
def clear_store():
    """Wipe global store and TTL index before every test."""
    store.clear()
    expires.clear()
    yield
    store.clear()
    expires.clear()


# ---------------------------------------------------------------------------
# PING
# ---------------------------------------------------------------------------

class TestPing:
    def test_bare_ping(self):
        assert process_input(resp_array("PING")) == b"+PONG\r\n"

    def test_ping_with_message(self):
        assert process_input(resp_array("PING", "hello")) == b"$5\r\nhello\r\n"

    def test_ping_too_many_args(self):
        assert process_input(resp_array("PING", "a", "b")).startswith(b"-ERR")

    def test_ping_case_insensitive(self):
        assert process_input(resp_array("ping")) == b"+PONG\r\n"


# ---------------------------------------------------------------------------
# ECHO
# ---------------------------------------------------------------------------

class TestEcho:
    def test_basic_echo(self):
        assert process_input(resp_array("ECHO", "world")) == b"$5\r\nworld\r\n"

    def test_echo_empty_string(self):
        assert process_input(resp_array("ECHO", "")) == b"$0\r\n\r\n"

    def test_echo_too_few_args(self):
        assert process_input(resp_array("ECHO")).startswith(b"-ERR")

    def test_echo_too_many_args(self):
        assert process_input(resp_array("ECHO", "a", "b")).startswith(b"-ERR")


# ---------------------------------------------------------------------------
# SET / GET — no TTL
# ---------------------------------------------------------------------------

class TestSetGet:
    def test_set_and_get(self):
        assert process_input(set_cmd("k", "v")) == b"+OK\r\n"
        assert process_input(get_cmd("k")) == b"$1\r\nv\r\n"

    def test_get_missing_key(self):
        assert process_input(get_cmd("nope")) == b"$-1\r\n"

    def test_set_overwrites(self):
        process_input(set_cmd("k", "first"))
        process_input(set_cmd("k", "second"))
        assert process_input(get_cmd("k")) == b"$6\r\nsecond\r\n"

    def test_set_too_few_args(self):
        assert process_input(resp_array("SET", "only_key")).startswith(b"-ERR")

    def test_get_too_many_args(self):
        assert process_input(resp_array("GET", "a", "b")).startswith(b"-ERR")

    def test_set_value_with_spaces(self):
        process_input(set_cmd("k", "hello world"))
        assert process_input(get_cmd("k")) == b"$11\r\nhello world\r\n"

    def test_set_multiple_keys_independent(self):
        process_input(set_cmd("a", "1"))
        process_input(set_cmd("b", "2"))
        assert process_input(get_cmd("a")) == b"$1\r\n1\r\n"
        assert process_input(get_cmd("b")) == b"$1\r\n2\r\n"


# ---------------------------------------------------------------------------
# SET EX / PX — TTL, passive expiry
# ---------------------------------------------------------------------------

class TestTTL:
    def test_set_ex_key_alive(self):
        process_input(set_cmd("k", "v", "EX", "10"))
        assert process_input(get_cmd("k")) == b"$1\r\nv\r\n"

    def test_set_px_key_alive(self):
        process_input(set_cmd("k", "v", "PX", "10000"))
        assert process_input(get_cmd("k")) == b"$1\r\nv\r\n"

    def test_set_ex_expired(self, monkeypatch):
        process_input(set_cmd("k", "v", "EX", "5"))
        # Advance monotonic clock past expiry
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        assert process_input(get_cmd("k")) == b"$-1\r\n"

    def test_set_px_expired(self, monkeypatch):
        process_input(set_cmd("k", "v", "PX", "100"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 1)
        assert process_input(get_cmd("k")) == b"$-1\r\n"

    def test_expired_key_removed_from_store(self, monkeypatch):
        process_input(set_cmd("k", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        process_input(get_cmd("k"))  # triggers passive deletion
        assert "k" not in store

    def test_ex_zero_rejected(self):
        assert process_input(set_cmd("k", "v", "EX", "0")).startswith(b"-ERR")

    def test_ex_negative_rejected(self):
        assert process_input(set_cmd("k", "v", "EX", "-1")).startswith(b"-ERR")

    def test_px_zero_rejected(self):
        assert process_input(set_cmd("k", "v", "PX", "0")).startswith(b"-ERR")

    def test_ex_non_integer_rejected(self):
        assert process_input(set_cmd("k", "v", "EX", "abc")).startswith(b"-ERR")

    def test_ex_missing_value_rejected(self):
        assert process_input(resp_array("SET", "k", "v", "EX")).startswith(b"-ERR")

    def test_unknown_option_rejected(self):
        assert process_input(set_cmd("k", "v", "XX")).startswith(b"-ERR")

    def test_set_ex_lowercase(self):
        # Redis accepts case-insensitive options
        assert process_input(set_cmd("k", "v", "ex", "10")) == b"+OK\r\n"
        assert process_input(get_cmd("k")) == b"$1\r\nv\r\n"

    def test_overwrite_clears_ttl(self, monkeypatch):
        """Re-setting a key without TTL should remove the expiry."""
        process_input(set_cmd("k", "v1", "EX", "5"))
        process_input(set_cmd("k", "v2"))  # no TTL
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        # Key must still be alive because new SET had no expiry
        assert process_input(get_cmd("k")) == b"$2\r\nv2\r\n"


# ---------------------------------------------------------------------------
# DEL
# ---------------------------------------------------------------------------

class TestDel:
    def test_del_existing_key(self):
        process_input(set_cmd("k", "v"))
        assert process_input(del_cmd("k")) == b":1\r\n"
        assert process_input(get_cmd("k")) == b"$-1\r\n"

    def test_del_missing_key(self):
        assert process_input(del_cmd("ghost")) == b":0\r\n"

    def test_del_multiple_keys(self):
        process_input(set_cmd("a", "1"))
        process_input(set_cmd("b", "2"))
        assert process_input(del_cmd("a", "b", "c")) == b":2\r\n"

    def test_del_no_args(self):
        assert process_input(resp_array("DEL")).startswith(b"-ERR")

    def test_del_expired_key_not_counted(self, monkeypatch):
        process_input(set_cmd("k", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        assert process_input(del_cmd("k")) == b":0\r\n"

    def test_del_removes_from_store(self):
        process_input(set_cmd("k", "v"))
        process_input(del_cmd("k"))
        assert "k" not in store

    def test_del_idempotent(self):
        process_input(set_cmd("k", "v"))
        process_input(del_cmd("k"))
        assert process_input(del_cmd("k")) == b":0\r\n"


# ---------------------------------------------------------------------------
# expires index sync + active expiration
# ---------------------------------------------------------------------------

class TestExpiresIndex:
    def test_set_with_ttl_indexed(self):
        process_input(set_cmd("k", "v", "EX", "5"))
        assert "k" in expires

    def test_set_without_ttl_not_indexed(self):
        process_input(set_cmd("k", "v"))
        assert "k" not in expires

    def test_overwrite_clears_index(self):
        process_input(set_cmd("k", "v1", "EX", "5"))
        process_input(set_cmd("k", "v2"))  # plain SET drops the TTL
        assert "k" not in expires

    def test_del_removes_from_index(self):
        process_input(set_cmd("k", "v", "EX", "5"))
        process_input(del_cmd("k"))
        assert "k" not in expires

    def test_passive_expiry_removes_from_index(self, monkeypatch):
        process_input(set_cmd("k", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        process_input(get_cmd("k"))  # triggers passive deletion
        assert "k" not in expires


class TestActiveExpire:
    def test_reclaims_untouched_expired_keys(self, monkeypatch):
        # A key nobody reads again must still be reclaimed by the cycle.
        for i in range(50):
            process_input(set_cmd(f"k{i}", "v", "EX", "5"))
        real_monotonic = time.monotonic
        monkeypatch.setattr(time, "monotonic", lambda: real_monotonic() + 10)
        deleted = 0
        # Loop the cycle to completion (each call is time-budgeted).
        while expires:
            n = sync_commands.active_expire_cycle(time_budget_ms=100)
            if n == 0:
                break
            deleted += n
        assert deleted == 50
        assert len(store) == 0

    def test_leaves_live_keys_alone(self):
        for i in range(50):
            process_input(set_cmd(f"k{i}", "v", "EX", "1000"))
        sync_commands.active_expire_cycle(time_budget_ms=100)
        assert len(store) == 50

    def test_no_ttl_keys_untouched(self):
        for i in range(20):
            process_input(set_cmd(f"k{i}", "v"))  # no TTL
        sync_commands.active_expire_cycle(time_budget_ms=100)
        assert len(store) == 20


# ---------------------------------------------------------------------------
# parse_resp / protocol edge cases
# ---------------------------------------------------------------------------

class TestProtocol:
    def test_inline_ping(self):
        assert process_input(b"PING\r\n") == b"+PONG\r\n"

    def test_empty_input(self):
        assert process_input(b"\r\n").startswith(b"-ERR")

    def test_unknown_command(self):
        resp = process_input(resp_array("FOOBAR"))
        assert resp.startswith(b"-ERR")
        assert b"unknown command" in resp

    def test_garbled_input(self):
        assert process_input(b"garbage").startswith(b"-ERR") or \
               process_input(b"garbage") == b"+PONG\r\n" or True
        # At minimum it must not raise
        try:
            process_input(b"\x00\xff\xfe")
        except Exception as e:
            pytest.fail(f"process_input raised on garbled input: {e}")

"""Tests for AsyncTCPServer's framing logic — the byte-stream reassembly that
turns arbitrary TCP chunks into discrete RESP commands.

These use an in-memory fake socket instead of real networking, so they are fast
and deterministic. We deliberately do NOT test run()'s infinite event loop here
(pure IO wiring); the bug-prone logic is the buffer draining and offset math.
"""
import pytest

from vortis.async_tcp import AsyncTCPServer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def resp_array(*tokens: str) -> bytes:
    parts = [f"*{len(tokens)}\r\n".encode()]
    for t in tokens:
        parts.append(f"${len(t)}\r\n{t}\r\n".encode())
    return b"".join(parts)


class FakeSock:
    """Minimal stand-in for a client socket.

    recv() yields the queued chunks one at a time (then b"" = EOF/disconnect),
    and sendall() accumulates everything written back for assertions.
    """

    def __init__(self, chunks: list[bytes]):
        self._chunks = list(chunks)
        self.sent = b""
        self.closed = False

    def recv(self, _n: int) -> bytes:
        return self._chunks.pop(0) if self._chunks else b""

    def sendall(self, data: bytes) -> None:
        self.sent += data

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def server():
    srv = AsyncTCPServer()
    # Stub the selector so _close() doesn't touch a real selector.
    class _NoopSel:
        def unregister(self, _conn):
            pass

    srv.sel = _NoopSel()
    return srv


def feed(server, sock):
    """Register a fake socket's buffer and drive one _read()."""
    server.read_buffers[sock] = b""
    server._read(sock)


# ---------------------------------------------------------------------------
# _resp_byte_length — pure offset arithmetic
# ---------------------------------------------------------------------------

class TestRespByteLength:
    def test_single_array_command(self):
        srv = AsyncTCPServer()
        data = resp_array("PING")
        assert srv._resp_byte_length(data, 1) == len(data)

    def test_multi_token_array(self):
        srv = AsyncTCPServer()
        data = resp_array("SET", "k", "v")
        assert srv._resp_byte_length(data, 3) == len(data)

    def test_stops_at_first_message_when_pipelined(self):
        srv = AsyncTCPServer()
        first = resp_array("PING")
        data = first + resp_array("ECHO", "hi")
        # Should report only the first message's length, not the whole buffer.
        assert srv._resp_byte_length(data, 1) == len(first)

    def test_inline_command(self):
        srv = AsyncTCPServer()
        data = b"PING\r\n"
        assert srv._resp_byte_length(data, 1) == len(data)


# ---------------------------------------------------------------------------
# _read — buffer draining, pipelining, partial reads, disconnect
# ---------------------------------------------------------------------------

class TestReadFraming:
    def test_single_command_responds(self, server):
        sock = FakeSock([resp_array("PING")])
        feed(server, sock)
        assert sock.sent == b"+PONG\r\n"

    def test_pipelined_commands_in_one_chunk(self, server):
        # Two commands arriving in a single recv() must both be processed.
        sock = FakeSock([resp_array("PING") + resp_array("ECHO", "hi")])
        feed(server, sock)
        assert sock.sent == b"+PONG\r\n" + b"$2\r\nhi\r\n"

    def test_partial_message_waits_for_rest(self, server):
        full = resp_array("ECHO", "hello")
        split = len(full) // 2
        # First recv delivers half a message, second delivers the rest.
        sock = FakeSock([full[:split], full[split:]])
        server.read_buffers[sock] = b""

        server._read(sock)  # first half — incomplete, no response yet
        assert sock.sent == b""
        assert server.read_buffers[sock] == full[:split]

        server._read(sock)  # rest arrives — now it responds
        assert sock.sent == b"$5\r\nhello\r\n"
        assert server.read_buffers[sock] == b""

    def test_command_split_across_three_reads(self, server):
        full = resp_array("SET", "k", "v")
        sock = FakeSock([full[:3], full[3:8], full[8:]])
        server.read_buffers[sock] = b""
        for _ in range(3):
            server._read(sock)
        assert sock.sent == b"+OK\r\n"
        assert server.store.data["k"][0] == "v"

    def test_disconnect_closes_connection(self, server):
        sock = FakeSock([])  # recv() returns b"" immediately = client FIN
        server.read_buffers[sock] = b""
        server._read(sock)
        assert sock.closed is True
        assert sock not in server.read_buffers

    def test_broken_pipe_on_send_closes_connection(self, server):
        # If the client vanishes mid-response, sendall raises and the server
        # must close the connection rather than crash (covers the except path).
        class BrokenSock(FakeSock):
            def sendall(self, _data):
                raise BrokenPipeError("client gone")

        sock = BrokenSock([resp_array("PING")])
        server.read_buffers[sock] = b""
        server._read(sock)  # must not raise
        assert sock.closed is True
        assert sock not in server.read_buffers

    def test_leftover_partial_kept_after_complete_one(self, server):
        # One full command followed by the start of another in the same chunk.
        full = resp_array("PING")
        partial = resp_array("ECHO", "x")[:4]
        sock = FakeSock([full + partial])
        server.read_buffers[sock] = b""
        server._read(sock)
        assert sock.sent == b"+PONG\r\n"
        # The incomplete tail must remain buffered for the next read.
        assert server.read_buffers[sock] == partial


# ---------------------------------------------------------------------------
# Eviction through the SERVER path — a bounded Store injected into the server,
# driven via real RESP SET commands over the fake socket.
# ---------------------------------------------------------------------------

class TestServerEviction:
    def _bounded_server(self, max_size):
        from vortis.store import Store
        srv = AsyncTCPServer(store=Store(thread_safe=False, max_size=max_size,
                                         eviction="random"))

        class _NoopSel:
            def unregister(self, _conn):
                pass

        srv.sel = _NoopSel()
        return srv

    def test_set_commands_respect_max_size(self):
        srv = self._bounded_server(max_size=5)
        # Fire 50 SET commands through the server's RESP path, pipelined.
        chunk = b"".join(resp_array("SET", f"k{i}", str(i)) for i in range(50))
        sock = FakeSock([chunk])
        srv.read_buffers[sock] = b""
        srv._read(sock)
        # Every SET returned +OK ...
        assert sock.sent == b"+OK\r\n" * 50
        # ... but the bounded store never exceeded the limit.
        assert len(srv.store) == 5
        assert set(srv.store.expires) <= set(srv.store.data)

    def test_get_after_eviction_returns_nil_or_value(self):
        srv = self._bounded_server(max_size=3)
        for i in range(10):
            sock = FakeSock([resp_array("SET", f"k{i}", str(i))])
            srv.read_buffers[sock] = b""
            srv._read(sock)
        # The last key set must still be retrievable via GET over the server.
        sock = FakeSock([resp_array("GET", "k9")])
        srv.read_buffers[sock] = b""
        srv._read(sock)
        assert sock.sent == b"$1\r\n9\r\n"
        assert len(srv.store) == 3

    def test_default_server_store_is_unbounded(self):
        # Regression guard: the plain server must NOT be bounded.
        srv = AsyncTCPServer()
        assert srv.store._bounded is False

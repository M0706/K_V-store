import selectors
import socket
import time
from resp import parse_resp
from store import Store
from protocol import RedisCmd, eval_and_respond

HOST = "127.0.0.1"
PORT = 65432

# How often the housekeeping (active-expire) cron runs, in seconds.
# Mirrors Redis's server.hz=10 -> a tick every 100ms.
CRON_INTERVAL = 0.1


class AsyncTCPServer:
    def __init__(self, store: Store | None = None) -> None:
        # The keyspace this server serves. A fresh one is created if not given,
        # so callers can share or inspect the store if they want to.
        # The server is single-threaded (one event loop), so it opts out of the
        # store's default lock for a contention-free hot path. It drives active
        # expiry itself from the loop, so it doesn't need the background daemon.
        self.store = store if store is not None else Store(thread_safe=False)
        # kqueue (macOS) or epoll (Linux) instance
        self.sel = selectors.DefaultSelector()
        # Per-connection read buffer — TCP is a byte stream, not a message stream
        self.read_buffers: dict[socket.socket, bytes] = {}

    def _close(self, conn: socket.socket) -> None:
        self.sel.unregister(conn)
        del self.read_buffers[conn]
        conn.close()

    def _accept(self, server: socket.socket) -> None:  # pragma: no cover - real socket IO
        conn, addr = server.accept()
        print(f"Connection from {addr}")
        conn.setblocking(False)
        self.read_buffers[conn] = b""
        # Watch this client fd for incoming data
        self.sel.register(conn, selectors.EVENT_READ, data=self._read)

    def _read(self, conn: socket.socket) -> None:
        chunk = conn.recv(4096)

        if not chunk:
            # recv() == 0: client sent FIN — clean disconnect
            print("Client disconnected")
            self._close(conn)
            return

        self.read_buffers[conn] += chunk

        # Drain all complete RESP messages from the buffer.
        # Loop handles pipelining — multiple commands in one recv().
        while self.read_buffers[conn]:
            tokens = parse_resp(self.read_buffers[conn])
            if tokens is None:
                # Incomplete message — wait for more data
                break

            consumed = self._resp_byte_length(self.read_buffers[conn], len(tokens))
            self.read_buffers[conn] = self.read_buffers[conn][consumed:]

            response = eval_and_respond(self.store, RedisCmd(cmd=tokens[0].upper(), args=tokens[1:]))
            try:
                conn.sendall(response)
            except (BrokenPipeError, ConnectionResetError):
                self._close(conn)
                return

    def _resp_byte_length(self, data: bytes, token_count: int) -> int:
        """Return byte length of the first complete message in data."""
        if data[0:1] == b"*":
            # RESP array: *N\r\n + N * ($len\r\n + value\r\n)
            pos = data.index(b"\r\n") + 2
            for _ in range(token_count):
                pos = data.index(b"\r\n", pos) + 2  # skip $len\r\n
                pos = data.index(b"\r\n", pos) + 2  # skip value\r\n
            return pos
        else:
            # Inline command: everything up to and including the first \r\n
            return data.index(b"\r\n") + 2

    def run(self, host: str = HOST, port: int = PORT) -> None:  # pragma: no cover - event loop / real socket IO
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((host, port))
        server.listen()
        server.setblocking(False)
        print(f"Listening on {host}:{port}")

        # Register server socket — readable means a new connection is waiting
        self.sel.register(server, selectors.EVENT_READ, data=self._accept)

        last_cron = time.monotonic()

        while True:
            # Block until an fd is ready OR the cron interval elapses — this IS
            # the kevent/epoll_wait call. The finite timeout is what guarantees
            # the active-expire cycle gets a turn even when no client sends data.
            for key, _ in self.sel.select(timeout=CRON_INTERVAL):
                try:
                    key.data(key.fileobj)
                except Exception as e:
                    print(f"Error on fd {key.fd}: {e}")
                    try:
                        self._close(key.fileobj)
                    except Exception:
                        pass

            # Housekeeping (Redis's serverCron). Runs cooperatively on this same
            # thread between socket events, so no locks are needed on the store.
            now = time.monotonic()
            if now - last_cron >= CRON_INTERVAL:
                self.store.active_expire_cycle(time_budget_ms=1.0)
                last_cron = now


def serve(host: str = HOST, port: int = PORT, store: Store | None = None) -> None:  # pragma: no cover - real socket IO
    """Run the key-value store as a RESP server (blocks forever).

    Convenience entry point so the package can be used as a server with one
    call::

        from async_tcp import serve
        serve(port=6379)
    """
    AsyncTCPServer(store=store).run(host=host, port=port)



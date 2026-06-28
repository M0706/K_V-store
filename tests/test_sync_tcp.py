"""Light integration coverage for the legacy blocking server (sync_tcp.py).

It is superseded by AsyncTCPServer (sync_tcp is commented out in main.py), so we
keep this to a single end-to-end smoke test: real socket, real client, one
round-trip. Uses an ephemeral port to avoid clashing with the hard-coded PORT.
"""
import socket
import threading

from vortis import sync_tcp
from vortis.store import Store
from vortis.protocol import process_input


def resp_array(*tokens: str) -> bytes:
    parts = [f"*{len(tokens)}\r\n".encode()]
    for t in tokens:
        parts.append(f"${len(t)}\r\n{t}\r\n".encode())
    return b"".join(parts)


def _serve_one(server_sock, store):
    """Accept a single connection and echo RESP responses until it closes.

    A trimmed copy of run_sync_tcp_server()'s per-connection loop that exits
    after the client disconnects, so the test thread can join cleanly.
    """
    conn, _addr = server_sock.accept()
    with conn:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(process_input(store, data))


def test_set_get_roundtrip_over_real_socket():
    store = Store()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))  # ephemeral port
    server_sock.listen()
    port = server_sock.getsockname()[1]

    t = threading.Thread(target=_serve_one, args=(server_sock, store), daemon=True)
    t.start()

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
            client.sendall(resp_array("SET", "k", "v"))
            assert client.recv(4096) == b"+OK\r\n"
            client.sendall(resp_array("GET", "k"))
            assert client.recv(4096) == b"$1\r\nv\r\n"
    finally:
        t.join(timeout=2)
        server_sock.close()

    # State went through the real process_input path.
    assert store.data["k"][0] == "v"


def test_run_sync_tcp_server_is_importable():
    # Guard against import-time regressions in the legacy module.
    assert callable(sync_tcp.run_sync_tcp_server)

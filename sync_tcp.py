import socket
from store import Store
from protocol import process_input

HOST = "127.0.0.1"
PORT = 65432


def run_sync_tcp_server() -> None:  # pragma: no cover - blocking accept loop / real socket IO
    store = Store(thread_safe=False)  # single-threaded accept loop
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen()
        print(f"Listening on {HOST}:{PORT}")

        while True:
            conn, addr = server.accept()
            print(f"Connection from {addr}")

            with conn:
                while True:
                    try:
                        data = conn.recv(4096)
                        if not data:
                            print(f"Client {addr} disconnected")
                            break
                        response = process_input(store, data)
                        conn.sendall(response)
                    except (ConnectionResetError, BrokenPipeError):
                        print(f"Connection with {addr} lost")
                        break

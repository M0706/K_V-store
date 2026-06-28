"""A small helper for running a task periodically on a background thread.

Extracted from Store: "run a callable every N seconds on a daemon thread until
told to stop" is a generic concern with nothing to do with being a key-value
store, so it lives on its own and is testable in isolation.
"""
import threading
from typing import Callable


class BackgroundSweeper:
    """Runs ``task`` every ``interval`` seconds on a daemon thread.

    The thread is a daemon, so a forgotten sweeper never blocks process exit.
    ``start`` and ``stop`` are both idempotent.
    """

    def __init__(self, interval: float, task: Callable[[], None],
                 name: str = "background-sweeper") -> None:
        self._interval = interval
        self._task = task
        self._name = name
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        """Start the sweep loop. No-op if already running."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name=self._name, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Signal the loop to stop and wait for the thread to exit.

        Idempotent and safe to call even if never started.
        """
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval + 1.0)
            self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def _loop(self) -> None:  # pragma: no cover - runs on a background thread
        # Event.wait() sleeps for the interval but returns immediately when
        # stop() is signalled, so shutdown is prompt rather than waiting out a
        # full sleep.
        while not self._stop.wait(self._interval):
            try:
                self._task()
            except Exception:
                # A single bad run must not kill the sweeper permanently — the
                # next tick tries again. (Swallowed deliberately; the task owns
                # its own logging if it needs any.)
                pass

    def __enter__(self) -> "BackgroundSweeper":
        self.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop()

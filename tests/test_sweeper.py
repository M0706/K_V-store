"""Unit tests for BackgroundSweeper (the daemon-thread runner extracted from Store)."""
import threading
import time

from vortis.sweeper import BackgroundSweeper


def test_runs_task_periodically():
    calls = []
    s = BackgroundSweeper(0.01, lambda: calls.append(1))
    s.start()
    try:
        deadline = time.monotonic() + 1.0
        while len(calls) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert len(calls) >= 3  # fired multiple times
    finally:
        s.stop()


def test_is_running_reflects_state():
    s = BackgroundSweeper(0.05, lambda: None)
    assert s.is_running() is False
    s.start()
    assert s.is_running() is True
    s.stop()
    assert s.is_running() is False


def test_start_is_idempotent():
    s = BackgroundSweeper(0.05, lambda: None)
    s.start()
    thread = s._thread
    s.start()  # second start must not replace the running thread
    try:
        assert s._thread is thread
    finally:
        s.stop()


def test_stop_is_idempotent_and_safe_without_start():
    s = BackgroundSweeper(0.05, lambda: None)
    s.stop()  # never started — must not raise
    s.start()
    s.stop()
    s.stop()  # double stop is fine
    assert s.is_running() is False


def test_stop_is_prompt():
    # Event.wait()-based loop should exit well within the interval+join budget,
    # not wait out a full long interval.
    s = BackgroundSweeper(5.0, lambda: None)  # long interval
    s.start()
    start = time.monotonic()
    s.stop()
    assert time.monotonic() - start < 2.0  # didn't block for the full 5s


def test_context_manager_starts_and_stops():
    with BackgroundSweeper(0.05, lambda: None) as s:
        assert s.is_running() is True
    assert s.is_running() is False


def test_task_exception_does_not_kill_the_sweeper():
    # A throwing task must not permanently kill the loop — the sweeper should
    # survive and keep firing on later ticks (resilience), then stop cleanly.
    calls = []

    def flaky():
        calls.append(1)
        raise RuntimeError("task failed")  # every run raises

    s = BackgroundSweeper(0.01, flaky)
    s.start()
    try:
        deadline = time.monotonic() + 1.0
        while len(calls) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)
        # Fired repeatedly despite every call raising -> loop survived.
        assert len(calls) >= 3
        assert s.is_running() is True
    finally:
        s.stop()
    assert s.is_running() is False

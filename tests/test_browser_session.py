"""Tests for browser-session auto shutdown behavior."""
import time
import importlib

import utils.browser_session as browser_session


def _configure_watchdog(monkeypatch):
    browser_session._reset_state_for_tests()
    monkeypatch.setattr(browser_session, "_AUTO_STOP_ENABLED", True)
    monkeypatch.setattr(browser_session, "_HEARTBEAT_TTL_SECONDS", 0.01)
    monkeypatch.setattr(browser_session, "_EMPTY_GRACE_SECONDS", 0.01)
    monkeypatch.setattr(browser_session, "_WATCH_INTERVAL_SECONDS", 0.01)


def test_should_stop_after_last_explicit_disconnect(monkeypatch):
    _configure_watchdog(monkeypatch)

    browser_session.touch_browser_client("tab-1")
    browser_session.disconnect_browser_client("tab-1")
    time.sleep(0.02)

    with browser_session._lock:
        assert browser_session._should_stop_locked(time.monotonic()) is True


def test_should_not_stop_when_client_only_times_out(monkeypatch):
    _configure_watchdog(monkeypatch)

    browser_session.touch_browser_client("tab-1")
    time.sleep(0.03)

    with browser_session._lock:
        assert browser_session._should_stop_locked(time.monotonic()) is False


def test_watchdog_invokes_shutdown_callback_once(monkeypatch):
    _configure_watchdog(monkeypatch)

    shutdown_calls: list[str] = []
    browser_session.touch_browser_client("tab-1")
    browser_session.disconnect_browser_client("tab-1")

    browser_session.start_browser_session_watchdog(
        shutdown_callback=lambda: shutdown_calls.append("stop"),
    )
    deadline = time.time() + 1.0
    while time.time() < deadline and not shutdown_calls:
        time.sleep(0.02)

    assert shutdown_calls == ["stop"]


def test_auto_stop_defaults_to_enabled(monkeypatch):
    monkeypatch.delenv("GTS_AUTO_STOP_ON_LAST_PAGE", raising=False)
    reloaded = importlib.reload(browser_session)
    try:
        assert reloaded.is_browser_auto_shutdown_enabled() is True
    finally:
        importlib.reload(reloaded)


def test_auto_stop_can_be_disabled_by_env(monkeypatch):
    monkeypatch.setenv("GTS_AUTO_STOP_ON_LAST_PAGE", "0")
    reloaded = importlib.reload(browser_session)
    try:
        assert reloaded.is_browser_auto_shutdown_enabled() is False
    finally:
        importlib.reload(reloaded)

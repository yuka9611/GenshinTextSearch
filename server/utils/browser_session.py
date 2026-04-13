import os
import threading
import time
from collections.abc import Callable


def _read_seconds(name: str, default: float, minimum: float) -> float:
    raw_value = os.environ.get(name, "").strip()
    if not raw_value:
        return default
    try:
        return max(float(raw_value), minimum)
    except ValueError:
        return default


_AUTO_STOP_ENABLED = os.environ.get("GTS_AUTO_STOP_ON_LAST_PAGE", "").strip() == "1"
_HEARTBEAT_TTL_SECONDS = _read_seconds("GTS_BROWSER_HEARTBEAT_TTL", 75.0, 15.0)
_EMPTY_GRACE_SECONDS = _read_seconds("GTS_BROWSER_EMPTY_GRACE", 30.0, 5.0)
_WATCH_INTERVAL_SECONDS = _read_seconds("GTS_BROWSER_WATCH_INTERVAL", 1.0, 0.5)

_lock = threading.Lock()
_active_clients: dict[str, float] = {}
_seen_any_client = False
_empty_since: float | None = None
_empty_can_shutdown = False
_watchdog_started = False
_shutdown_requested = False


def is_browser_auto_shutdown_enabled() -> bool:
    return _AUTO_STOP_ENABLED


def _prune_stale_clients_locked(now: float) -> None:
    expired_client_ids = [
        client_id
        for client_id, last_seen in _active_clients.items()
        if now - last_seen > _HEARTBEAT_TTL_SECONDS
    ]
    for client_id in expired_client_ids:
        _active_clients.pop(client_id, None)


def _refresh_empty_state_locked(now: float, *, allow_shutdown_if_empty: bool = False) -> None:
    global _empty_since, _empty_can_shutdown
    if _active_clients:
        _empty_since = None
        _empty_can_shutdown = False
        return
    if not _seen_any_client:
        _empty_since = None
        _empty_can_shutdown = False
        return
    if _empty_since is None:
        _empty_since = now
    if allow_shutdown_if_empty:
        _empty_can_shutdown = True


def touch_browser_client(client_id: str) -> int:
    now = time.monotonic()
    global _seen_any_client
    with _lock:
        _prune_stale_clients_locked(now)
        _active_clients[client_id] = now
        _seen_any_client = True
        _refresh_empty_state_locked(now)
        return len(_active_clients)


def disconnect_browser_client(client_id: str) -> int:
    now = time.monotonic()
    with _lock:
        _prune_stale_clients_locked(now)
        _active_clients.pop(client_id, None)
        _refresh_empty_state_locked(now, allow_shutdown_if_empty=True)
        return len(_active_clients)


def get_active_browser_client_count() -> int:
    now = time.monotonic()
    with _lock:
        _prune_stale_clients_locked(now)
        _refresh_empty_state_locked(now)
        return len(_active_clients)


def _should_stop_locked(now: float) -> bool:
    _prune_stale_clients_locked(now)
    _refresh_empty_state_locked(now)
    if not _seen_any_client or _active_clients or _empty_since is None:
        return False
    if not _empty_can_shutdown:
        return False
    return now - _empty_since >= _EMPTY_GRACE_SECONDS


def _request_shutdown(logger=None, shutdown_callback: Callable[[], None] | None = None) -> bool:
    global _shutdown_requested
    with _lock:
        if _shutdown_requested:
            return False
        _shutdown_requested = True

    if logger is not None:
        logger.info("No active browser pages remain, shutting down the server.")

    if shutdown_callback is None:
        if logger is not None:
            logger.warning("Browser-session auto shutdown skipped because no shutdown callback was provided.")
        return False

    try:
        shutdown_callback()
    except Exception:
        if logger is not None:
            logger.exception("Browser-session auto shutdown callback failed.")
        return False

    return True


def start_browser_session_watchdog(logger=None, shutdown_callback: Callable[[], None] | None = None) -> None:
    global _watchdog_started
    if not _AUTO_STOP_ENABLED:
        return

    with _lock:
        if _watchdog_started:
            return
        _watchdog_started = True

    def _watch() -> None:
        while True:
            time.sleep(_WATCH_INTERVAL_SECONDS)
            now = time.monotonic()
            with _lock:
                should_stop = _should_stop_locked(now)
            if not should_stop:
                continue
            _request_shutdown(logger=logger, shutdown_callback=shutdown_callback)
            return

    threading.Thread(
        target=_watch,
        name="browser-session-watchdog",
        daemon=True,
    ).start()


def _reset_state_for_tests() -> None:
    global _seen_any_client, _empty_since, _empty_can_shutdown, _watchdog_started, _shutdown_requested
    with _lock:
        _active_clients.clear()
        _seen_any_client = False
        _empty_since = None
        _empty_can_shutdown = False
        _watchdog_started = False
        _shutdown_requested = False

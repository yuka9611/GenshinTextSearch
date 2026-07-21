"""Runtime policy helpers for public/cloud deployments."""

from __future__ import annotations

import os
import threading
import time
from collections import deque


_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}
_RATE_LIMIT_LOCK = threading.Lock()
_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = {}


def _read_bool(name: str, default: bool) -> bool:
    raw_value = os.environ.get(name, "").strip().lower()
    if not raw_value:
        return default
    if raw_value in _TRUE_VALUES:
        return True
    if raw_value in _FALSE_VALUES:
        return False
    return default


def _read_positive_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, "").strip() or default))
    except ValueError:
        return default


def is_cloud_mode() -> bool:
    return _read_bool("GTS_CLOUD_MODE", False)


def local_features_enabled() -> bool:
    return _read_bool("GTS_ENABLE_LOCAL_FEATURES", not is_cloud_mode())


def settings_writable() -> bool:
    return _read_bool("GTS_ALLOW_SETTINGS_WRITE", not is_cloud_mode())


def voice_playback_enabled() -> bool:
    return _read_bool("GTS_ENABLE_VOICE_PLAYBACK", not is_cloud_mode())


def trusted_proxy_enabled() -> bool:
    return _read_bool("GTS_TRUST_PROXY", False)


def cors_origins() -> list[str]:
    raw_value = os.environ.get("GTS_CORS_ORIGINS", "")
    return [origin.strip().rstrip("/") for origin in raw_value.split(",") if origin.strip()]


def public_runtime_payload() -> dict[str, bool]:
    return {
        "cloudMode": is_cloud_mode(),
        "localFeaturesEnabled": local_features_enabled(),
        "settingsWritable": settings_writable(),
        "voicePlaybackEnabled": voice_playback_enabled(),
    }


def _client_key(request) -> str:
    return request.remote_addr or "unknown"


def enforce_rate_limit():
    """Return a Flask response when a cloud client exceeds its fixed-window quota."""
    if not is_cloud_mode():
        return None

    from flask import jsonify, request

    if request.method == "OPTIONS" or not request.path.startswith("/api/"):
        return None

    request_limit = _read_positive_int("GTS_RATE_LIMIT_REQUESTS", 120)
    window_seconds = _read_positive_int("GTS_RATE_LIMIT_WINDOW_SECONDS", 60)
    now = time.monotonic()
    cutoff = now - window_seconds
    client_key = _client_key(request)

    with _RATE_LIMIT_LOCK:
        bucket = _RATE_LIMIT_BUCKETS.setdefault(client_key, deque())
        while bucket and bucket[0] <= cutoff:
            bucket.popleft()

        if len(bucket) >= request_limit:
            retry_after = max(1, int(window_seconds - (now - bucket[0])))
            response = jsonify({
                "data": None,
                "code": 429,
                "msg": "Too many requests",
            })
            response.status_code = 429
            response.headers["Retry-After"] = str(retry_after)
            return response

        bucket.append(now)

        # Bound stale client bookkeeping without adding a cleanup thread.
        if len(_RATE_LIMIT_BUCKETS) > 4096:
            stale_keys = [
                key for key, values in _RATE_LIMIT_BUCKETS.items()
                if not values or values[-1] <= cutoff
            ]
            for key in stale_keys[:1024]:
                _RATE_LIMIT_BUCKETS.pop(key, None)

    return None


def cloud_feature_forbidden(feature: str):
    from flask import jsonify

    response = jsonify({
        "data": None,
        "code": 403,
        "msg": f"{feature} is disabled in cloud mode",
    })
    response.status_code = 403
    return response


def _reset_rate_limit_for_tests() -> None:
    with _RATE_LIMIT_LOCK:
        _RATE_LIMIT_BUCKETS.clear()

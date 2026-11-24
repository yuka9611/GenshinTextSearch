"""Optional read-only compact text sidecar support for fast single-char search."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from pathlib import Path

_ALIAS = "genshin_compact"
_DEFAULT_ALLOWED_LANGS = frozenset({1, 4, 9})
FLAG_HAS_VOICE = 1
FLAG_HAS_KNOWN_SOURCE = 2
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_REQUIRED_COLUMNS = frozenset(
    {
        "lang",
        "id",
        "hash",
        "content",
        "created_version_id",
        "updated_version_id",
        "flags",
    }
)

_META_CACHE_LOCK = threading.Lock()
_META_CACHE: dict[
    tuple[str, tuple[int, int, int], str, tuple[int, int, int]],
    frozenset[int] | None,
] = {}


def resolve_compact_text_path(source_path: Path) -> Path:
    """Resolve the compact sidecar path via GTS_COMPACT_TEXT_PATH or alongside source DB."""
    override = (os.environ.get("GTS_COMPACT_TEXT_PATH") or "").strip()
    if override:
        return Path(override)
    return Path(source_path).resolve().with_name("compact_text_cache.db")


def detach_compact_text(connection: sqlite3.Connection | None) -> None:
    """Detach genshin_compact if currently attached on connection."""
    if connection is None:
        return
    try:
        attached = {
            row[1]: row[2]
            for row in connection.execute("PRAGMA database_list").fetchall()
        }
        if _ALIAS in attached:
            connection.execute(f"DETACH DATABASE {_ALIAS}")
    except sqlite3.Error as exc:
        if "interrupted" in str(exc).lower():
            raise


def _active_wal(source_path: Path) -> bool:
    try:
        return Path(str(source_path) + "-wal").stat().st_size > 0
    except OSError:
        return False


def _parse_allowed_langs(metadata: dict[str, str]) -> frozenset[int] | None:
    if "languages" not in metadata:
        return _DEFAULT_ALLOWED_LANGS
    try:
        raw_langs = json.loads(metadata["languages"])
    except (TypeError, ValueError):
        return None
    if not isinstance(raw_langs, list):
        return None
    parsed: set[int] = set()
    for item in raw_langs:
        try:
            parsed.add(int(str(item)))
        except (TypeError, ValueError):
            return None
    return frozenset(parsed)


def _clear_meta_cache() -> None:
    with _META_CACHE_LOCK:
        _META_CACHE.clear()


def _ensure_compact_text_attached(
    connection: sqlite3.Connection | None,
) -> frozenset[int] | None:
    if connection is None:
        return None
    try:
        if getattr(connection, "total_changes", 0) > 0:
            detach_compact_text(connection)
            return None

        databases = {
            row[1]: row[2]
            for row in connection.execute("PRAGMA database_list").fetchall()
        }
        main_file = (databases.get("main") or "").strip()
        if not main_file:
            return None

        source_path = Path(main_file).resolve()
        if _active_wal(source_path):
            detach_compact_text(connection)
            return None

        source_stat = source_path.stat()
        source_stat_tuple = (
            source_stat.st_size,
            source_stat.st_mtime_ns,
            source_stat.st_ino,
        )

        candidate_cache = resolve_compact_text_path(source_path)
        if not candidate_cache.is_file():
            detach_compact_text(connection)
            return None

        cache_path = candidate_cache.resolve()
        cache_stat = cache_path.stat()
        cache_stat_tuple = (
            cache_stat.st_size,
            cache_stat.st_mtime_ns,
            cache_stat.st_ino,
        )
        cache_key = (
            str(source_path),
            source_stat_tuple,
            str(cache_path),
            cache_stat_tuple,
        )

        attached_same = False
        if _ALIAS in databases and databases[_ALIAS]:
            try:
                attached_same = Path(databases[_ALIAS]).resolve() == cache_path
            except OSError:
                attached_same = False

        if attached_same:
            with _META_CACHE_LOCK:
                if cache_key in _META_CACHE:
                    cached_langs = _META_CACHE[cache_key]
                    if cached_langs is None:
                        detach_compact_text(connection)
                        return None
                    live_stat = source_path.stat()
                    if _active_wal(source_path) or (
                        live_stat.st_size,
                        live_stat.st_mtime_ns,
                        live_stat.st_ino,
                    ) != source_stat_tuple:
                        detach_compact_text(connection)
                        return None
                    return cached_langs

        if _ALIAS in databases and not attached_same:
            connection.execute(f"DETACH DATABASE {_ALIAS}")
            attached_same = False

        if not attached_same:
            connection.execute(
                f"ATTACH DATABASE ? AS {_ALIAS}",
                (cache_path.as_uri() + "?mode=ro&immutable=1",),
            )

        with _META_CACHE_LOCK:
            if cache_key in _META_CACHE:
                cached_langs = _META_CACHE[cache_key]
                if cached_langs is None:
                    detach_compact_text(connection)
                return cached_langs

        metadata = dict(
            connection.execute(f"SELECT k, v FROM {_ALIAS}.cache_meta").fetchall()
        )
        binding_ok = (
            metadata.get("format") == "1"
            and metadata.get("game") == "genshin"
            and metadata.get("has_fetter_voice") == "1"
            and metadata.get("source_size") == str(source_stat.st_size)
            and metadata.get("source_mtime_ns") == str(source_stat.st_mtime_ns)
            and metadata.get("source_inode") == str(source_stat.st_ino)
            and bool(_SHA256_RE.fullmatch(metadata.get("source_sha256") or ""))
        )
        allowed_langs = _parse_allowed_langs(metadata) if binding_ok else None
        columns = {
            row[1]
            for row in connection.execute(
                f"PRAGMA {_ALIAS}.table_info(compact_text)"
            ).fetchall()
            if len(row) > 1
        }
        schema_ok = _REQUIRED_COLUMNS.issubset(columns)

        live_stat = source_path.stat()
        live_ok = (
            not _active_wal(source_path)
            and (live_stat.st_size, live_stat.st_mtime_ns, live_stat.st_ino)
            == source_stat_tuple
        )

        if not (binding_ok and allowed_langs is not None and schema_ok and live_ok):
            if live_ok:
                with _META_CACHE_LOCK:
                    _META_CACHE[cache_key] = None
            detach_compact_text(connection)
            return None

        # Verify the attached compact table is readable before exposing it.
        connection.execute(
            f"SELECT lang FROM {_ALIAS}.compact_text LIMIT 1"
        ).fetchone()
        with _META_CACHE_LOCK:
            _META_CACHE[cache_key] = allowed_langs
        return allowed_langs
    except (OSError, TypeError, ValueError, sqlite3.Error) as exc:
        if isinstance(exc, sqlite3.Error) and "interrupted" in str(exc).lower():
            raise
        try:
            detach_compact_text(connection)
        except Exception:
            pass
        return None


def warmup_compact_text(connection: sqlite3.Connection | None) -> bool:
    """Attach and validate genshin_compact eagerly during startup warmup."""
    return _ensure_compact_text_attached(connection) is not None


def resolve_compact_text_table(
    connection: sqlite3.Connection | None,
    lang_code: int,
) -> str | None:
    """Return 'genshin_compact.compact_text' when sidecar cache covers lang_code."""
    allowed_langs = _ensure_compact_text_attached(connection)
    if allowed_langs is None or lang_code not in allowed_langs:
        return None
    return f"{_ALIAS}.compact_text"

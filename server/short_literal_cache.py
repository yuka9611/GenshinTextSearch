"""Optional read-only bigram FTS sidecar support for fast short-literal search."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from pathlib import Path

import fts_tokenizer

_ALIAS = "genshin_short"
_DEFAULT_ALLOWED_LANGS = frozenset({1, 4, 9})
_CANDIDATE_SUBQUERY = (
    f"select rowid from {_ALIAS}.short_fts where short_fts match ?"
)
_SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_FTS5_SCHEMA_RE = re.compile(r"\bUSING\s+fts5\s*\(", re.IGNORECASE)

_META_CACHE_LOCK = threading.Lock()
_META_CACHE: dict[
    tuple[str, tuple[int, int, int], str, tuple[int, int, int]],
    frozenset[int] | None,
] = {}


def short_term(pair: str) -> str:
    """Encode a 2-char slice into the lowercase-ASCII UTF-8 hex token format."""
    return "".join(
        chr(ord(char) + 32) if "A" <= char <= "Z" else char for char in pair
    ).encode("utf-8").hex()


def build_short_fts_match_expr(keyword: str | None) -> str | None:
    """Build a conjunctive bigram FTS5 MATCH expression across search terms."""
    raw = str(keyword or "").strip()
    if not raw or "\0" in raw:
        return None

    terms = fts_tokenizer.split_search_terms(raw)
    if not terms:
        return None

    pairs: dict[str, None] = {}
    for term in terms:
        if len(term) < 2 or "\0" in term:
            continue
        for pos in range(len(term) - 1):
            pairs[short_term(term[pos:pos + 2])] = None
            if len(pairs) == 64:
                break
        if len(pairs) == 64:
            break

    if not pairs:
        return None
    return " AND ".join(f'"{token}"' for token in pairs)


def resolve_short_fts_path(source_path: Path) -> Path:
    """Resolve the sidecar path via GTS/GENSHIN env overrides or alongside source DB."""
    override = (
        os.environ.get("GTS_SHORT_FTS_PATH")
        or os.environ.get("GENSHIN_SHORT_FTS_PATH")
        or ""
    ).strip()
    if override:
        return Path(override)
    return Path(source_path).resolve().with_name("short_literal_fts.db")


def detach_short_fts(connection: sqlite3.Connection | None) -> None:
    """Detach genshin_short if currently attached on connection."""
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


def _ensure_short_fts_attached(connection: sqlite3.Connection | None) -> frozenset[int] | None:
    if connection is None:
        return None
    try:
        if getattr(connection, "total_changes", 0) > 0:
            detach_short_fts(connection)
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
            detach_short_fts(connection)
            return None

        source_stat = source_path.stat()
        source_stat_tuple = (
            source_stat.st_size,
            source_stat.st_mtime_ns,
            source_stat.st_ino,
        )

        candidate_cache = resolve_short_fts_path(source_path)
        if not candidate_cache.is_file():
            detach_short_fts(connection)
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
                        detach_short_fts(connection)
                        return None
                    live_stat = source_path.stat()
                    if _active_wal(source_path) or (
                        live_stat.st_size,
                        live_stat.st_mtime_ns,
                        live_stat.st_ino,
                    ) != source_stat_tuple:
                        detach_short_fts(connection)
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
                    detach_short_fts(connection)
                return cached_langs

        metadata = dict(
            connection.execute(f"SELECT k, v FROM {_ALIAS}.cache_meta").fetchall()
        )
        binding_ok = (
            metadata.get("format") == "1"
            and metadata.get("source_size") == str(source_stat.st_size)
            and metadata.get("source_mtime_ns") == str(source_stat.st_mtime_ns)
            and metadata.get("source_inode") == str(source_stat.st_ino)
            and bool(_SHA256_RE.fullmatch(metadata.get("source_sha256") or ""))
            and (metadata.get("game") is None or metadata.get("game") == "genshin")
        )
        allowed_langs = _parse_allowed_langs(metadata) if binding_ok else None
        schema_row = connection.execute(
            f"SELECT sql FROM {_ALIAS}.sqlite_master "
            "WHERE type='table' AND name='short_fts' LIMIT 1"
        ).fetchone()
        schema_ok = bool(schema_row and _FTS5_SCHEMA_RE.search(schema_row[0] or ""))

        live_stat = source_path.stat()
        live_ok = (
            not _active_wal(source_path)
            and (live_stat.st_size, live_stat.st_mtime_ns, live_stat.st_ino)
            == source_stat_tuple
        )

        if not (binding_ok and allowed_langs is not None and schema_ok and live_ok):
            if binding_ok and (not schema_ok or allowed_langs is None):
                with _META_CACHE_LOCK:
                    _META_CACHE[cache_key] = None
            elif not live_ok:
                pass
            else:
                with _META_CACHE_LOCK:
                    _META_CACHE[cache_key] = None
            detach_short_fts(connection)
            return None

        # Verify the attached FTS5 table is readable before exposing its query SQL.
        connection.execute(
            f"SELECT rowid FROM {_ALIAS}.short_fts WHERE short_fts MATCH '\"00\"' LIMIT 1"
        ).fetchone()
        with _META_CACHE_LOCK:
            _META_CACHE[cache_key] = allowed_langs
        return allowed_langs
    except (OSError, TypeError, ValueError, sqlite3.Error) as exc:
        if isinstance(exc, sqlite3.Error) and "interrupted" in str(exc).lower():
            raise
        try:
            detach_short_fts(connection)
        except Exception:
            pass
        return None


def warmup_short_fts(connection: sqlite3.Connection | None) -> bool:
    """Attach and validate genshin_short eagerly during startup warmup."""
    return _ensure_short_fts_attached(connection) is not None


def resolve_short_fts_query(
    connection: sqlite3.Connection | None,
    keyword: str | None,
    lang_code: int,
) -> tuple[str, str] | None:
    """Return (rowid_subquery_sql, match_expr) when sidecar cache can accelerate keyword."""
    match_expr = build_short_fts_match_expr(keyword)
    if match_expr is None:
        return None
    allowed_langs = _ensure_short_fts_attached(connection)
    if allowed_langs is None or lang_code not in allowed_langs:
        return None
    return _CANDIDATE_SUBQUERY, match_expr

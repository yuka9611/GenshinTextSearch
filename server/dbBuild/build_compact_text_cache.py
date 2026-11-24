#!/usr/bin/env python3
"""Build a disposable read-only compact text sidecar without modifying data.db."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time
from contextlib import closing
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
_SERVER_DIR = _SCRIPT_DIR.parent if _SCRIPT_DIR.name == "dbBuild" else _SCRIPT_DIR
for _sys_path in (str(_SCRIPT_DIR), str(_SERVER_DIR)):
    if _sys_path not in sys.path:
        sys.path.insert(0, _sys_path)

from compact_text_cache import FLAG_HAS_KNOWN_SOURCE, FLAG_HAS_VOICE  # noqa: E402


def _parse_langs(raw_langs: str | list[int] | tuple[int, ...]) -> tuple[int, ...]:
    if isinstance(raw_langs, (list, tuple)):
        items = raw_langs
    else:
        items = [part.strip() for part in str(raw_langs).split(",") if part.strip()]
    parsed: list[int] = []
    seen: set[int] = set()
    for item in items:
        lang_id = int(item)
        if lang_id not in seen:
            seen.add(lang_id)
            parsed.append(lang_id)
    if not parsed:
        raise ValueError("At least one language ID is required")
    return tuple(sorted(parsed))


def _table_columns(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {
        row[1]
        for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        if len(row) > 1
    }


def _collect_voiced_hashes(
    src: sqlite3.Connection,
    existing_tables: set[str],
) -> tuple[set[int], bool]:
    voiced: set[int] = set()
    if "dialogue" in existing_tables and "voice" in existing_tables:
        d_cols = _table_columns(src, "dialogue")
        v_cols = _table_columns(src, "voice")
        if {"dialogueId", "textHash"}.issubset(d_cols) and "dialogueId" in v_cols:
            for (text_hash,) in src.execute(
                "SELECT d.textHash FROM dialogue d "
                "JOIN voice v ON v.dialogueId = d.dialogueId "
                "WHERE d.textHash IS NOT NULL"
            ):
                voiced.add(int(text_hash))

    has_fetter_voice = "fetterVoice" in existing_tables or "fetters" not in existing_tables
    if "fetters" in existing_tables and "fetterVoice" in existing_tables:
        f_cols = _table_columns(src, "fetters")
        fv_cols = _table_columns(src, "fetterVoice")
        if (
            {"avatarId", "voiceFile", "voiceFileTextTextMapHash"}.issubset(f_cols)
            and {"avatarId", "voiceFile"}.issubset(fv_cols)
        ):
            for (text_hash,) in src.execute(
                "SELECT f.voiceFileTextTextMapHash FROM fetters f "
                "JOIN fetterVoice fv ON fv.avatarId = f.avatarId AND fv.voiceFile = f.voiceFile "
                "WHERE f.voiceFileTextTextMapHash IS NOT NULL"
            ):
                voiced.add(int(text_hash))

    return voiced, has_fetter_voice


def _collect_known_source_hashes(
    src: sqlite3.Connection,
    existing_tables: set[str],
) -> set[int]:
    known: set[int] = set()
    table_columns: dict[str, set[str]] = {}

    def has_col(table_name: str, col_name: str) -> bool:
        if table_name not in existing_tables:
            return False
        cols = table_columns.get(table_name)
        if cols is None:
            cols = _table_columns(src, table_name)
            table_columns[table_name] = cols
        return col_name in cols

    source_queries: list[tuple[str, str]] = [
        ("dialogue", "textHash"),
        ("fetters", "voiceFileTextTextMapHash"),
        ("fetterStory", "storyContextTextMapHash"),
        ("fetterStory", "storyContext2TextMapHash"),
        ("quest_hash_map", "hash"),
        ("text_source_entity", "text_hash"),
        ("text_source_entity", "title_hash"),
        ("readable", "titleTextMapHash"),
    ]
    for table_name, col_name in source_queries:
        if not has_col(table_name, col_name):
            continue
        for (text_hash,) in src.execute(
            f"SELECT {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL"
        ):
            known.add(int(text_hash))
    return known


def build(
    source: Path,
    output: Path,
    *,
    langs: str | list[int] | tuple[int, ...] = "1,4,9",
    force: bool = False,
) -> dict:
    """Build a source-bound compact_text_cache.db sidecar for Genshin textMap."""
    source = source.resolve()
    output = output.absolute()
    if output.exists() and not force:
        raise FileExistsError(output)

    parsed_langs = _parse_langs(langs)
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, candidate_name = tempfile.mkstemp(
        prefix=f".{output.name}-", suffix=".db", dir=output.parent
    )
    os.close(descriptor)
    candidate = Path(candidate_name)
    try:
        stats = _build(source, candidate, parsed_langs)
        if force:
            os.replace(candidate, output)
        else:
            os.link(candidate, output)
        return {"output": str(output), **stats}
    finally:
        candidate.unlink(missing_ok=True)


def _build(source: Path, output: Path, langs: tuple[int, ...]) -> dict:
    before = source.stat()
    wal = Path(str(source) + "-wal")
    if wal.exists() and wal.stat().st_size:
        raise RuntimeError(
            "Checkpoint the owning database WAL before building compact_text_cache.db"
        )

    digest = hashlib.sha256()
    with source.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    started = time.monotonic()
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as src, closing(
        sqlite3.connect(output)
    ) as dst:
        existing_tables = {
            row[0]
            for row in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        voiced_hashes, has_fetter_voice = _collect_voiced_hashes(src, existing_tables)
        known_hashes = _collect_known_source_hashes(src, existing_tables)
        tm_cols = _table_columns(src, "textMap")
        has_version_cols = (
            "created_version_id" in tm_cols and "updated_version_id" in tm_cols
        )

        dst.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=MEMORY;
            PRAGMA page_size=8192;
            PRAGMA cache_size=-131072;
            CREATE TABLE cache_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
            CREATE TABLE compact_text (
                lang INTEGER NOT NULL,
                id INTEGER NOT NULL,
                hash INTEGER NOT NULL,
                content TEXT NOT NULL,
                created_version_id INTEGER,
                updated_version_id INTEGER,
                flags INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (lang, id)
            ) WITHOUT ROWID;
            """
        )
        metadata = {
            "format": "1",
            "game": "genshin",
            "languages": json.dumps([str(lang_id) for lang_id in langs]),
            "has_fetter_voice": "1" if has_fetter_voice else "0",
            "source_sha256": digest.hexdigest(),
            "source_size": str(before.st_size),
            "source_mtime_ns": str(before.st_mtime_ns),
            "source_inode": str(before.st_ino),
        }
        dst.executemany("INSERT INTO cache_meta VALUES (?,?)", metadata.items())

        placeholders = ",".join("?" for _ in langs)
        version_select = (
            "created_version_id, updated_version_id"
            if has_version_cols
            else "NULL AS created_version_id, NULL AS updated_version_id"
        )
        select_sql = (
            f"SELECT lang, id, hash, content, {version_select} "
            f"FROM textMap WHERE lang IN ({placeholders}) "
            "ORDER BY lang, id"
        )

        pending: list[tuple[int, int, int, str, int | None, int | None, int]] = []
        count = 0
        for lang_id, row_id, text_hash, content, created_vid, updated_vid in src.execute(
            select_sql, langs
        ):
            if text_hash is None:
                continue
            hash_int = int(text_hash)
            flags = 0
            if hash_int in voiced_hashes:
                flags |= FLAG_HAS_VOICE
            if hash_int in known_hashes:
                flags |= FLAG_HAS_KNOWN_SOURCE
            pending.append(
                (
                    int(lang_id),
                    int(row_id),
                    hash_int,
                    str(content or ""),
                    created_vid,
                    updated_vid,
                    flags,
                )
            )
            count += 1
            if len(pending) >= 5000:
                dst.executemany(
                    "INSERT INTO compact_text VALUES (?,?,?,?,?,?,?)", pending
                )
                pending.clear()
            if count % 250000 == 0:
                print(
                    json.dumps(
                        {"rows": count, "seconds": round(time.monotonic() - started, 1)}
                    ),
                    flush=True,
                )

        if pending:
            dst.executemany("INSERT INTO compact_text VALUES (?,?,?,?,?,?,?)", pending)
        dst.commit()

        after = source.stat()
        if (after.st_ino, after.st_mtime_ns, after.st_size) != (
            before.st_ino,
            before.st_mtime_ns,
            before.st_size,
        ) or (wal.exists() and wal.stat().st_size):
            raise RuntimeError(
                "Source changed during cache build; discard candidate"
            )
        if dst.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("Candidate compact_text_cache quick_check failed")

    return {
        "bytes": output.stat().st_size,
        "rows": count,
        "langs": list(langs),
        "source_sha256": digest.hexdigest(),
        "seconds": round(time.monotonic() - started, 1),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=_SERVER_DIR / "data.db",
        help="Path to the source data.db (default: server/data.db)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_SERVER_DIR / "compact_text_cache.db",
        help="Output sidecar path (default: server/compact_text_cache.db)",
    )
    parser.add_argument(
        "--langs",
        default="1,4,9",
        help="Comma-separated language codes to include (default: 1,4,9)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Atomically overwrite output if it already exists",
    )
    args = parser.parse_args(argv)
    stats = build(args.source, args.output, langs=args.langs, force=args.force)
    print(json.dumps(stats), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

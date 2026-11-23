#!/usr/bin/env python3
"""Build a disposable read-only bigram FTS sidecar without modifying data.db."""

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

_SERVER_DIR = Path(__file__).resolve().parents[1]
if str(_SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(_SERVER_DIR))

from short_literal_cache import short_term  # noqa: E402


def build(source: Path, output: Path, *, force: bool = False) -> dict:
    """Build a source-bound short_literal_fts.db sidecar for Genshin textMap."""
    source = source.resolve()
    output = output.absolute()
    if output.exists() and not force:
        raise FileExistsError(output)

    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, candidate_name = tempfile.mkstemp(
        prefix=f".{output.name}-", suffix=".db", dir=output.parent
    )
    os.close(descriptor)
    candidate = Path(candidate_name)
    try:
        stats = _build(source, candidate)
        if force:
            os.replace(candidate, output)
        else:
            os.link(candidate, output)
        return {"output": str(output), **stats}
    finally:
        candidate.unlink(missing_ok=True)


def _build(source: Path, output: Path) -> dict:
    before = source.stat()
    wal = Path(str(source) + "-wal")
    if wal.exists() and wal.stat().st_size:
        raise RuntimeError(
            "Checkpoint the owning database WAL before building short_literal_fts.db"
        )

    digest = hashlib.sha256()
    with source.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)

    started = time.monotonic()
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as src, closing(
        sqlite3.connect(output)
    ) as dst:
        dst.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            PRAGMA temp_store=MEMORY;
            PRAGMA cache_size=-131072;
            CREATE TABLE cache_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
            CREATE VIRTUAL TABLE short_fts USING fts5(
                grams, content='', detail='none', columnsize=0,
                tokenize='unicode61 remove_diacritics 0'
            );
            """
        )
        metadata = {
            "format": "1",
            "game": "genshin",
            "languages": json.dumps(["1", "4", "9"]),
            "source_sha256": digest.hexdigest(),
            "source_size": str(before.st_size),
            "source_mtime_ns": str(before.st_mtime_ns),
            "source_inode": str(before.st_ino),
        }
        dst.executemany("INSERT INTO cache_meta VALUES (?,?)", metadata.items())

        pending: list[tuple[int, str]] = []
        count = 0
        for rowid, content in src.execute(
            "SELECT id, content FROM textMap WHERE lang IN (1, 4, 9)"
        ):
            text = str(content or "")
            terms = {
                short_term(text[idx:idx + 2])
                for idx in range(len(text) - 1)
                if "\x00" not in text[idx:idx + 2]
            }
            if terms:
                pending.append((rowid, " ".join(sorted(terms))))
            count += 1
            if len(pending) >= 2000:
                dst.executemany(
                    "INSERT INTO short_fts(rowid, grams) VALUES (?,?)", pending
                )
                pending.clear()
            if count % 100000 == 0:
                print(
                    json.dumps(
                        {"rows": count, "seconds": round(time.monotonic() - started, 1)}
                    ),
                    flush=True,
                )

        if pending:
            dst.executemany("INSERT INTO short_fts(rowid, grams) VALUES (?,?)", pending)
        dst.execute("INSERT INTO short_fts(short_fts) VALUES ('optimize')")
        dst.commit()

        after = source.stat()
        if (after.st_ino, after.st_mtime_ns, after.st_size) != (
            before.st_ino,
            before.st_mtime_ns,
            before.st_size,
        ) or (wal.exists() and wal.stat().st_size):
            raise RuntimeError("Source changed during cache build; discard candidate")
        if dst.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("Candidate short_literal_fts quick_check failed")

    return {
        "bytes": output.stat().st_size,
        "rows": count,
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
        default=_SERVER_DIR / "short_literal_fts.db",
        help="Output sidecar path (default: server/short_literal_fts.db)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Atomically overwrite output if it already exists",
    )
    args = parser.parse_args(argv)
    stats = build(args.source, args.output, force=args.force)
    print(json.dumps(stats), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

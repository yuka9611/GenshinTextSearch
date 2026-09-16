"""Read-only coverage gate for source-scoped Talk dialogue payloads."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from genshin_data_core.talk import (
    extract_talk_dialogue_payload_with_schema,
    is_non_dialog_talk_obj,
    normalize_talk_dialogue_rows,
)


def _source_rows(data_path: str, valid_text_hashes: set[int] | None = None):
    root = Path(data_path) / "BinOutput" / "Talk"
    expected: set[tuple[int, int, int, int]] = set()
    expected_links: set[tuple[int, int, int]] = set()
    recognized_talk_ids: set[int] = set()
    content_talk_ids: set[int] = set()
    source_files = Counter()
    empty_or_non_text_ids: set[int] = set()
    samples: dict[tuple[int, int], list[tuple[int, int, str]]] = defaultdict(list)
    scoped_candidates: dict[tuple[int, int], list[tuple[int, list[tuple]]]] = defaultdict(list)
    scoped_files: dict[tuple[int, int], list[str]] = defaultdict(list)
    if not root.is_dir():
        return {
            "expected": expected,
            "expected_links": expected_links,
            "recognized_talk_ids": recognized_talk_ids,
            "content_talk_ids": content_talk_ids,
            "source_files": source_files,
            "empty_or_non_text_ids": empty_or_non_text_ids,
            "samples": samples,
        }

    for path in sorted(root.rglob("*.json")):
        relative = path.relative_to(root).as_posix()
        try:
            with path.open(encoding="utf-8") as handle:
                obj = json.load(handle)
        except (OSError, json.JSONDecodeError):
            source_files["invalid_json"] += 1
            continue
        if not isinstance(obj, dict):
            source_files["unrecognized"] += 1
            continue
        if is_non_dialog_talk_obj(obj):
            source_files["non_dialog"] += 1
            continue
        parsed = extract_talk_dialogue_payload_with_schema(obj)
        if parsed is None:
            source_files["unrecognized"] += 1
            continue

        source_files["dialog"] += 1
        schema_rank, talk_id, rows = parsed
        recognized_talk_ids.add(int(talk_id))
        coop_match = re.fullmatch(r"Coop/([0-9]+)_[0-9]+\.json", relative)
        coop_quest_id = int(coop_match.group(1)) if coop_match else 0
        scope = (int(talk_id), coop_quest_id)
        scoped_candidates[scope].append((schema_rank, rows))
        scoped_files[scope].append(relative)

    for scope, candidates in scoped_candidates.items():
        talk_id, coop_quest_id = scope
        rows = normalize_talk_dialogue_rows(
            candidates,
            valid_text_hashes=valid_text_hashes,
        )
        if not rows:
            empty_or_non_text_ids.add(int(talk_id))
        for dialogue_id, text_hash, _talker_id, _talker_type in rows:
            expected.add(
                (int(talk_id), coop_quest_id, int(dialogue_id), int(text_hash))
            )
            expected_links.add(
                (int(talk_id), coop_quest_id, int(dialogue_id))
            )
            content_talk_ids.add(int(talk_id))
            if len(samples[scope]) < 3:
                samples[scope].append(
                    (int(dialogue_id), int(text_hash), scoped_files[scope][0])
                )

    return {
        "expected": expected,
        "expected_links": expected_links,
        "recognized_talk_ids": recognized_talk_ids,
        "content_talk_ids": content_talk_ids,
        "source_files": source_files,
        "empty_or_non_text_ids": empty_or_non_text_ids,
        "samples": samples,
    }


def audit_talk_dialogue_coverage(cursor, data_path: str) -> dict:
    """Compare source-scoped dialogue content and links without writing."""
    has_textmap = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='textMap' LIMIT 1"
    ).fetchone()
    valid_text_hashes = (
        {int(row[0]) for row in cursor.execute("SELECT DISTINCT hash FROM textMap").fetchall()}
        if has_textmap
        else None
    )
    source = _source_rows(data_path, valid_text_hashes)
    actual_content = set(
        cursor.execute(
            "SELECT talkId, coopQuestId, dialogueId, textHash "
            "FROM talk_dialogue_content"
        ).fetchall()
    )
    actual_links = set(
        cursor.execute(
            "SELECT talkId, coopQuestId, dialogueId FROM talk_dialogue_link"
        ).fetchall()
    )
    expected = source["expected"]
    expected_links = source["expected_links"]
    missing_content = expected - actual_content
    extra_content = actual_content - expected
    missing_links = expected_links - actual_links
    extra_links = actual_links - expected_links
    invalid_content = {
        row for row in actual_content
        if row[3] is None or row[3] == 0
        or (valid_text_hashes is not None and int(row[3]) not in valid_text_hashes)
    }
    actual_content_keys = {
        (row[0], row[1], row[2]) for row in actual_content
    }
    orphan_links = actual_links - actual_content_keys
    return {
        "source_files": dict(source["source_files"]),
        "source_recognized_talk_ids": len(source["recognized_talk_ids"]),
        "source_talk_ids_with_dialogue_rows": len(source["content_talk_ids"]),
        "source_empty_or_non_text_talk_ids": sorted(source["empty_or_non_text_ids"]),
        "source_unique_content_rows": len(expected),
        "temp_content_rows": len(actual_content),
        "missing_content_rows": len(missing_content),
        "extra_content_rows": len(extra_content),
        "source_unique_scope_dialogue_links": len(expected_links),
        "temp_link_rows": len(actual_links),
        "missing_exact_links": len(missing_links),
        "extra_exact_links": len(extra_links),
        "invalid_content_rows": len(invalid_content),
        "orphan_link_rows": len(orphan_links),
        # Keep the gate artifact reviewable; the set comparison above remains
        # exhaustive, while the sample section is only illustrative.
        "samples": {
            f"{talk_id}:{coop_quest_id}": rows
            for (talk_id, coop_quest_id), rows in sorted(source["samples"].items())[:20]
        },
    }


def assert_talk_dialogue_coverage(cursor, data_path: str) -> dict:
    report = audit_talk_dialogue_coverage(cursor, data_path)
    failures = {
        key: report[key]
        for key in (
            "missing_content_rows",
            "extra_content_rows",
            "missing_exact_links",
            "extra_exact_links",
            "invalid_content_rows",
            "orphan_link_rows",
        )
        if report[key]
    }
    if failures:
        raise RuntimeError(f"Talk dialogue coverage gate failed: {failures}")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()
    db_path = os.path.abspath(args.db)
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        report = audit_talk_dialogue_coverage(connection.cursor(), args.data_path)
    finally:
        connection.close()
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if not any(
        report[key] for key in (
            "missing_content_rows", "extra_content_rows", "missing_exact_links",
            "extra_exact_links", "invalid_content_rows", "orphan_link_rows",
        )
    ) else 2


if __name__ == "__main__":
    raise SystemExit(main())

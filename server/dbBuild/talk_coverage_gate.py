"""Read-only coverage gate for source-scoped Talk dialogue payloads."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

from genshin_data_core.talk import extract_talk_dialogue_payload, is_non_dialog_talk_obj


def _source_rows(data_path: str):
    root = Path(data_path) / "BinOutput" / "Talk"
    expected: set[tuple[int, int, int, int]] = set()
    expected_links: set[tuple[int, int, int]] = set()
    recognized_talk_ids: set[int] = set()
    content_talk_ids: set[int] = set()
    source_files = Counter()
    empty_or_non_text_ids: set[int] = set()
    samples: dict[tuple[int, int], list[tuple[int, int, str]]] = defaultdict(list)
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
        payload = extract_talk_dialogue_payload(obj)
        if payload is None:
            source_files["unrecognized"] += 1
            continue

        source_files["dialog"] += 1
        talk_id, rows = payload
        recognized_talk_ids.add(int(talk_id))
        coop_match = re.fullmatch(r"Coop/([0-9]+)_[0-9]+\.json", relative)
        coop_quest_id = int(coop_match.group(1)) if coop_match else 0
        scope = (int(talk_id), coop_quest_id)
        if not rows:
            empty_or_non_text_ids.add(int(talk_id))
        for dialogue_id, text_hash, _talker_id, _talker_type in rows:
            try:
                normalized_dialogue_id = int(dialogue_id)
                normalized_hash = int(text_hash)
            except (TypeError, ValueError):
                continue
            expected.add(
                (int(talk_id), coop_quest_id, normalized_dialogue_id, normalized_hash)
            )
            expected_links.add(
                (int(talk_id), coop_quest_id, normalized_dialogue_id)
            )
            content_talk_ids.add(int(talk_id))
            if len(samples[scope]) < 3:
                samples[scope].append(
                    (normalized_dialogue_id, normalized_hash, relative)
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
    source = _source_rows(data_path)
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
        report[key] for key in ("missing_content_rows", "extra_content_rows", "missing_exact_links")
    ) else 2


if __name__ == "__main__":
    raise SystemExit(main())

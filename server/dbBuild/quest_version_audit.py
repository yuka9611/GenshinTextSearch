"""Generate the pre-import quest version and task-text audit.

This command is intentionally read-only with respect to the selected SQLite
database.  It opens the database in SQLite ``mode=ro`` and only uses TEMP
tables for task/hash aggregation.  The resulting JSON is the input consumed by
the import-time provenance gate.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import sqlite3
import sys
from typing import Any


DBBUILD_DIR = os.path.dirname(os.path.abspath(__file__))
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from quest_version_provenance import calculate_quest_text_version_audit
from version_control import calculate_quest_created_version_candidates


def _app_meta_snapshot(cursor) -> dict[str, str]:
    try:
        rows = cursor.execute(
            "SELECT key, value FROM app_meta "
            "WHERE key IN ('db_current_commit','db_version','agd_source_url','agd_remote_ref') "
            "ORDER BY key"
        ).fetchall()
    except sqlite3.Error:
        try:
            rows = cursor.execute(
                "SELECT k, v FROM app_meta "
                "WHERE k IN ('db_current_commit','db_version','agd_source_url','agd_remote_ref') "
                "ORDER BY k"
            ).fetchall()
        except sqlite3.Error:
            return {}
    return {str(key): str(value) for key, value in rows if value is not None}


def generate_audit(db_path: str, output_path: str) -> dict[str, Any]:
    resolved_db_path = os.path.abspath(db_path)
    stat = os.stat(resolved_db_path)
    connection = sqlite3.connect(
        f"file:{resolved_db_path}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    try:
        cursor = connection.cursor()
        candidates = calculate_quest_created_version_candidates(cursor)
        final_versions = {
            quest_id: item.get("final_created_version_id")
            for quest_id, item in candidates.items()
        }
        text_audit = calculate_quest_text_version_audit(cursor, final_versions)
        text_by_quest = {
            int(row["questId"]): row
            for row in text_audit.get("records", [])
            if isinstance(row, dict)
        }
        records: list[dict[str, Any]] = []
        for quest_id in sorted(candidates):
            candidate = dict(candidates[quest_id])
            candidate.update(text_by_quest.get(quest_id, {
                "associated_hash_count": 0,
                "text_version_adjustment_count": 0,
                "shared_conflict_count": 0,
                "unresolved_count": 0,
            }))
            records.append(candidate)

        status_counts: dict[str, int] = {}
        source_counts: dict[str, int] = {}
        for row in records:
            status = str(row.get("status") or "unresolved")
            status_counts[status] = status_counts.get(status, 0) + 1
            source = str(row.get("candidate_source") or "unresolved")
            source_counts[source] = source_counts.get(source, 0) + 1
        text_summary = text_audit.get("summary", {})
        payload: dict[str, Any] = {
            "audit_kind": "genshin_quest_created_version_and_task_text",
            "rule": {
                "manual_difference": "candidate_created_version_id != current_created_version_id",
                "unresolved": "candidate_created_version_id is null; do not lock",
                "task_text": "all enumerated task-associated TextMap hashes use final_created_version_id in task-scoped provenance",
                "global_textmap": "never overwrite globally when task versions conflict",
            },
            "source_database": {
                "path": resolved_db_path,
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
                "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                "app_meta": _app_meta_snapshot(cursor),
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "candidate_count": len(records),
                "difference_count": status_counts.get("manual_difference", 0),
                "same_count": status_counts.get("same", 0),
                "unresolved_count": status_counts.get("unresolved", 0),
                "candidate_source_counts": source_counts,
                "associated_hash_count": int(text_summary.get("associated_hash_count", 0) or 0),
                "text_version_adjustment_count": int(
                    text_summary.get("text_version_adjustment_count", 0) or 0
                ),
                "shared_conflict_hash_count": int(
                    text_summary.get("shared_conflict_hash_count", 0) or 0
                ),
                "task_text_unresolved_count": int(text_summary.get("unresolved_count", 0) or 0),
            },
            "records": records,
        }
    finally:
        connection.close()

    output_path = os.path.abspath(output_path)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=False)
        handle.write("\n")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_db = os.path.abspath(os.path.join(DBBUILD_DIR, "..", "data.db"))
    parser.add_argument("--db", default=default_db, help="SQLite database to inspect")
    parser.add_argument("--output", required=True, help="JSON audit output path")
    args = parser.parse_args()
    payload = generate_audit(args.db, args.output)
    summary = payload["summary"]
    print(
        "Quest version/text audit: "
        f"candidates={summary['candidate_count']} "
        f"differences={summary['difference_count']} "
        f"same={summary['same_count']} "
        f"unresolved={summary['unresolved_count']} "
        f"task_text_conflicts={summary['shared_conflict_hash_count']} "
        f"task_text_unresolved={summary['task_text_unresolved_count']}"
    )
    print(f"Audit written: {os.path.abspath(args.output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

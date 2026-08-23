"""Read-only pre-atomic gate for Quest version anomalies and task text.

The gate is deliberately separate from history replay.  It classifies the
anomalies reported by the same import run, compares them with the untouched
production database, and refuses replacement when a NULL task version or an
invalid version reference would make task-text alignment unverifiable.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
from typing import Any


REPORTED_ANOMALIES: tuple[tuple[str, int], ...] = (
    ("missing_created_version", 7001),
    ("missing_created_version", 77147),
    ("missing_git_version", 7001),
    ("missing_git_version", 77147),
    ("missing_git_version", 77142),
    ("missing_git_version", 77501),
    ("missing_git_version", 77660),
    ("missing_git_version", 7012),
    ("quest_version_older_than_min_update", 1032),
)


def _open_read_only(path: str | Path) -> sqlite3.Connection:
    resolved = Path(path).resolve()
    return sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)


def _meta(connection: sqlite3.Connection) -> dict[str, str]:
    try:
        rows = connection.execute(
            "SELECT key, value FROM app_meta ORDER BY key"
        ).fetchall()
    except sqlite3.Error:
        rows = connection.execute("SELECT k, v FROM app_meta ORDER BY k").fetchall()
    return {str(key): str(value) for key, value in rows if value is not None}


def _version_sort_keys(connection: sqlite3.Connection) -> dict[int, int]:
    return {
        int(row[0]): int(row[1])
        for row in connection.execute(
            "SELECT id, version_sort_key FROM version_dim "
            "WHERE version_sort_key IS NOT NULL"
        ).fetchall()
    }


def _validation(connection: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    sort_keys = _version_sort_keys(connection)
    no_created = [
        {"questId": int(row[0]), "created": row[1], "git": row[2]}
        for row in connection.execute(
            "SELECT questId, created_version_id, git_created_version_id "
            "FROM quest WHERE created_version_id IS NULL ORDER BY questId"
        ).fetchall()
    ]
    no_git = [
        {"questId": int(row[0]), "created": row[1], "git": row[2]}
        for row in connection.execute(
            "SELECT questId, created_version_id, git_created_version_id "
            "FROM quest WHERE git_created_version_id IS NULL ORDER BY questId"
        ).fetchall()
    ]
    invalid = [
        {"questId": int(row[0]), "created": row[1], "git": row[2]}
        for row in connection.execute(
            "SELECT questId, created_version_id, git_created_version_id "
            "FROM quest WHERE created_version_id <= 0 OR git_created_version_id <= 0 "
            "ORDER BY questId"
        ).fetchall()
    ]
    no_quest = [
        {"questId": int(row[0]), "lang": row[1], "updated": row[2]}
        for row in connection.execute(
            "SELECT qv.questId, qv.lang, qv.updated_version_id "
            "FROM quest_version qv LEFT JOIN quest q ON q.questId=qv.questId "
            "WHERE q.questId IS NULL ORDER BY qv.questId, qv.lang"
        ).fetchall()
    ]
    no_updated = [
        {"questId": int(row[0]), "lang": row[1], "updated": row[2]}
        for row in connection.execute(
            "SELECT questId, lang, updated_version_id FROM quest_version "
            "WHERE updated_version_id IS NULL ORDER BY questId, lang"
        ).fetchall()
    ]
    invalid_updated = [
        {"questId": int(row[0]), "lang": row[1], "updated": row[2]}
        for row in connection.execute(
            "SELECT questId, lang, updated_version_id FROM quest_version "
            "WHERE updated_version_id <= 0 ORDER BY questId, lang"
        ).fetchall()
    ]
    created = {
        int(row[0]): row[1]
        for row in connection.execute(
            "SELECT questId, created_version_id FROM quest "
            "WHERE created_version_id IS NOT NULL"
        ).fetchall()
    }
    min_updates = {
        int(row[0]): row[1]
        for row in connection.execute(
            "SELECT questId, MIN(updated_version_id) FROM quest_version "
            "GROUP BY questId"
        ).fetchall()
    }
    older = []
    for quest_id, min_update in min_updates.items():
        current = created.get(quest_id)
        if (
            current is not None
            and min_update is not None
            and sort_keys.get(int(current)) is not None
            and sort_keys.get(int(min_update)) is not None
            and sort_keys[int(current)] > sort_keys[int(min_update)]
        ):
            older.append(
                {
                    "questId": quest_id,
                    "min_updated_version_id": min_update,
                    "created_version_id": current,
                }
            )
    return {
        "missing_created_version": no_created,
        "missing_git_version": no_git,
        "invalid_version": invalid,
        "quest_version_without_quest": no_quest,
        "quest_version_without_updated": no_updated,
        "quest_version_invalid_updated": invalid_updated,
        "quest_version_older_than_min_update": older,
    }


def _source_evidence(
    connection: sqlite3.Connection,
    data_path: str | Path,
    quest_ids: set[int],
) -> dict[int, dict[str, Any]]:
    """Map the small gate set through actual source files and source metadata."""
    import sys

    dbbuild_dir = Path(__file__).resolve().parent
    if str(dbbuild_dir) not in sys.path:
        sys.path.insert(0, str(dbbuild_dir))
    from genshin_data_core.quest import QuestParser

    parser = QuestParser()
    root = Path(data_path).resolve() / "BinOutput" / "Quest"
    paths: dict[int, str] = {}
    for path in sorted(root.glob("*.json")):
        try:
            obj = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        row = parser.extract_quest_row(obj)
        if row is not None and row.quest_id in quest_ids:
            paths[row.quest_id] = f"BinOutput/Quest/{path.name}"

    result: dict[int, dict[str, Any]] = {}
    for quest_id, path in paths.items():
        source = connection.execute(
            "SELECT created_version, last_updated_version, last_change_type "
            "FROM source_file_version WHERE path=?",
            (path,),
        ).fetchone()
        result[quest_id] = {
            "path": path,
            "created_version": source[0] if source else None,
            "last_updated_version": source[1] if source else None,
            "last_change_type": source[2] if source else None,
        }
    return result


def _task_text_rows(
    connection: sqlite3.Connection,
    quest_id: int,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT hash, created_version_id, relation_types, alignment_status, "
        "textmap_row_count, textmap_version_count, textmap_unresolved_row_count "
        "FROM quest_text_version WHERE questId=? ORDER BY hash",
        (quest_id,),
    ).fetchall()
    result = []
    for row in rows:
        global_rows = connection.execute(
            "SELECT COUNT(*) FROM textMap WHERE hash=?",
            (row[0],),
        ).fetchone()[0]
        result.append(
            {
                "hash": int(row[0]),
                "created_version_id": row[1],
                "relation_types": row[2],
                "alignment_status": row[3],
                "textmap_row_count": int(row[4] or 0),
                "textmap_version_count": int(row[5] or 0),
                "textmap_unresolved_row_count": int(row[6] or 0),
                "global_textmap_row_count": int(global_rows),
            }
        )
    return result


def _version_id_for_raw(
    connection: sqlite3.Connection,
    raw_version: str | None,
) -> int | None:
    if not raw_version:
        return None
    row = connection.execute(
        "SELECT id FROM version_dim WHERE raw_version=?",
        (str(raw_version),),
    ).fetchone()
    return int(row[0]) if row else None


def _quest_row(connection: sqlite3.Connection, quest_id: int) -> dict[str, Any] | None:
    row = connection.execute(
        "SELECT created_version_id, git_created_version_id FROM quest WHERE questId=?",
        (quest_id,),
    ).fetchone()
    if row is None:
        return None
    return {"created_version_id": row[0], "git_created_version_id": row[1]}


def generate_gate(
    *,
    original_database: str,
    target_database: str,
    audit_path: str,
    output_path: str,
    data_path: str,
    target_version_raw: str,
) -> dict[str, Any]:
    original = _open_read_only(original_database)
    target = _open_read_only(target_database)
    try:
        target_version = target.execute(
            "SELECT id, version_sort_key FROM version_dim WHERE raw_version=?",
            (target_version_raw,),
        ).fetchone()
        if target_version is None:
            raise RuntimeError(f"target version is absent from version_dim: {target_version_raw}")
        target_version_id = int(target_version[0])

        with open(audit_path, "r", encoding="utf-8") as handle:
            pre_import_audit = json.load(handle)
        pre_records = {
            int(row["questId"]): row
            for row in pre_import_audit.get("records", [])
            if isinstance(row, dict) and row.get("questId") is not None
        }

        quest_ids = {quest_id for _, quest_id in REPORTED_ANOMALIES}
        source = _source_evidence(target, data_path, quest_ids)
        original_validation = _validation(original)
        target_validation = _validation(target)
        original_anomaly_ids = {
            anomaly_type: {
                int(row["questId"])
                for row in rows
                if isinstance(row, dict) and row.get("questId") is not None
            }
            for anomaly_type, rows in original_validation.items()
        }
        records: list[dict[str, Any]] = []
        reasons: list[str] = []
        for anomaly_type, quest_id in REPORTED_ANOMALIES:
            before = _quest_row(original, quest_id)
            after = _quest_row(target, quest_id)
            pre_candidate = pre_records.get(quest_id, {}).get(
                "candidate_created_version_id"
            )
            source_candidate = _version_id_for_raw(
                target,
                (source.get(quest_id) or {}).get("created_version"),
            )
            if anomaly_type in {
                "missing_created_version",
                "missing_git_version",
            } and source_candidate is not None:
                candidate = source_candidate
                candidate_source = "source_file_version"
            elif pre_candidate is not None:
                candidate = pre_candidate
                candidate_source = "pre_import_read_only_candidate"
            else:
                candidate = (after or {}).get("created_version_id")
                candidate_source = "target_final_value_fallback"

            text_rows = _task_text_rows(target, quest_id)
            manual_lock = target.execute(
                "SELECT locked_created_version_id, candidate_created_version_id "
                "FROM quest_created_version_override WHERE questId=?",
                (quest_id,),
            ).fetchone()
            records.append(
                {
                    "anomaly_type": anomaly_type,
                    "questId": quest_id,
                    "current_before_import": before,
                    "candidate_created_version_id": candidate,
                    "candidate_source": candidate_source,
                    "final_after_targeted_repairs": after,
                    "source_evidence": source.get(quest_id),
                    "pre_import_existing_anomaly": quest_id in original_anomaly_ids.get(
                        anomaly_type, set()
                    ),
                    "pre_import_existing_anomaly_types": sorted(
                        anomaly_name
                        for anomaly_name, quest_ids_for_type in original_anomaly_ids.items()
                        if quest_id in quest_ids_for_type
                    ),
                    "manual_lock": (
                        {
                            "locked_created_version_id": manual_lock[0],
                            "candidate_created_version_id": manual_lock[1],
                        }
                        if manual_lock
                        else None
                    ),
                    "task_text": {
                        "associated_hash_count": len(text_rows),
                        "rows": text_rows,
                    },
                }
            )

        # The original validation report was emitted before the two targeted
        # NULL repairs and before source-file Git provenance repair.  Compare
        # the current target state separately so every transition is visible.
        current_counts = {key: len(value) for key, value in target_validation.items()}
        if current_counts["missing_created_version"]:
            reasons.append("target still contains missing created_version_id")
        if current_counts["missing_git_version"]:
            reasons.append("target still contains missing git_created_version_id")
        for key in (
            "invalid_version",
            "quest_version_without_quest",
            "quest_version_without_updated",
            "quest_version_invalid_updated",
        ):
            if current_counts[key]:
                reasons.append(f"target contains {key}: {current_counts[key]}")

        actual_locks = {
            int(row[0]): (row[1], row[2])
            for row in target.execute(
                "SELECT questId, locked_created_version_id, candidate_created_version_id "
                "FROM quest_created_version_override"
            ).fetchall()
        }
        expected_locks = {
            int(row["questId"]): (
                row.get("final_created_version_id"),
                row.get("candidate_created_version_id"),
            )
            for row in pre_import_audit.get("records", [])
            if isinstance(row, dict) and row.get("status") == "manual_difference"
        }
        if actual_locks != expected_locks:
            reasons.append("manual-lock set/value differs from the pre-import audit")

        # The one remaining validation exception belongs to a manually locked
        # task.  Its updated-version rows are all valid and its task-scoped
        # text versions stay on the locked created version; the exception is
        # therefore intentional rather than a broken reference.
        for row in target_validation["quest_version_older_than_min_update"]:
            quest_id = int(row["questId"])
            lock = actual_locks.get(quest_id)
            text_rows = _task_text_rows(target, quest_id)
            if not lock or any(
                text_row["created_version_id"] != lock[0]
                for text_row in text_rows
            ):
                reasons.append(
                    f"older-version exception is not protected by a consistent manual lock: {quest_id}"
                )

        # The lock is field-scoped, but every task-text row belonging to a
        # locked task must still carry the locked task version.  This checks
        # all 135 locks, not only the nine validation exceptions.
        locked_text_mismatches = target.execute(
            "SELECT qtv.questId, qtv.hash, q.created_version_id, "
            "qtv.created_version_id "
            "FROM quest_text_version qtv "
            "JOIN quest q ON q.questId=qtv.questId "
            "JOIN quest_created_version_override o ON o.questId=qtv.questId "
            "WHERE qtv.created_version_id IS NULL "
            "OR qtv.created_version_id != q.created_version_id "
            "ORDER BY qtv.questId, qtv.hash"
        ).fetchall()
        if locked_text_mismatches:
            reasons.append(
                "manual-locked task text version mismatch: "
                f"{len(locked_text_mismatches)} rows"
            )

        unresolved_text_with_global_rows = target.execute(
            "SELECT COUNT(*) FROM quest_text_version qtv "
            "JOIN textMap tm ON tm.hash=qtv.hash "
            "WHERE qtv.alignment_status='unresolved_textmap_hash'"
        ).fetchone()[0]
        if unresolved_text_with_global_rows:
            reasons.append(
                "unresolved task hashes have global textMap rows: "
                f"{unresolved_text_with_global_rows} rows"
            )

        # A missing TextMap hash is safe to downgrade only when there is no
        # global textMap row at all.  It must never be presented as aligned.
        for record in records:
            for text_row in record["task_text"]["rows"]:
                if text_row["alignment_status"] == "unresolved_quest_version":
                    reasons.append(
                        f"quest {record['questId']} still has unresolved task version for hash {text_row['hash']}"
                    )
                if (
                    text_row["alignment_status"] == "unresolved_textmap_hash"
                    and text_row["global_textmap_row_count"] != 0
                ):
                    reasons.append(
                        f"unresolved task hash has global rows: quest={record['questId']} hash={text_row['hash']}"
                    )

        # Every task now has a non-NULL created version, and all version rows
        # have valid references.  Shared/global text conflicts remain in the
        # task-scoped table and are not silently rewritten.
        all_task_text_nonnull = target.execute(
            "SELECT COUNT(*) FROM quest_text_version WHERE created_version_id IS NULL"
        ).fetchone()[0]
        if all_task_text_nonnull:
            reasons.append(f"task text rows without task version: {all_task_text_nonnull}")

        report = {
            "audit_kind": "quest_version_validation_pre_atomic",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "target_version": {
                "id": target_version_id,
                "raw_version": target_version_raw,
                "version_sort_key": target_version[1],
            },
            "original_database": {
                "path": str(Path(original_database).resolve()),
                "size_bytes": Path(original_database).stat().st_size,
                "mtime_ns": Path(original_database).stat().st_mtime_ns,
                "app_meta": _meta(original),
                "validation": original_validation,
            },
            "target_database": {
                "path": str(Path(target_database).resolve()),
                "size_bytes": Path(target_database).stat().st_size,
                "mtime_ns": Path(target_database).stat().st_mtime_ns,
                "app_meta": _meta(target),
                "validation": target_validation,
            },
            "reported_anomalies_before_targeted_repairs": [
                {"anomaly_type": anomaly_type, "questId": quest_id}
                for anomaly_type, quest_id in REPORTED_ANOMALIES
            ],
            "summary": {
                "reported_anomaly_count": len(REPORTED_ANOMALIES),
                "reported_missing_created_count": 2,
                "reported_missing_git_count": 6,
                "reported_older_count": 1,
                "current_missing_created_count": current_counts["missing_created_version"],
                "current_missing_git_count": current_counts["missing_git_version"],
                "current_older_count": current_counts["quest_version_older_than_min_update"],
                "manual_locked_quest_count": len(actual_locks),
                "task_text_rows_without_version": int(all_task_text_nonnull),
                "manual_locked_task_text_mismatch_count": len(
                    locked_text_mismatches
                ),
                "unresolved_textmap_hash_rows_with_global_textmap": int(
                    unresolved_text_with_global_rows
                ),
                "pre_import_existing_reported_anomaly_count": sum(
                    quest_id in original_anomaly_ids.get(anomaly_type, set())
                    for anomaly_type, quest_id in REPORTED_ANOMALIES
                ),
                "task_text_status_counts": dict(
                    target.execute(
                        "SELECT alignment_status, COUNT(*) FROM quest_text_version "
                        "GROUP BY alignment_status ORDER BY alignment_status"
                    ).fetchall()
                ),
            },
            "records": records,
            "disposition": {
                "safe_to_replace": not reasons,
                "reasons": reasons,
                "allowed_downgrades": [
                    {
                        "kind": "unresolved_textmap_hash",
                        "rule": "only hashes with zero global textMap rows; no text row exists to rewrite",
                    },
                    {
                        "kind": "manual_locked_older_update",
                        "rule": "created value remains locked; updated history rows remain valid and task text is task-scoped",
                    },
                ],
            },
        }
    finally:
        original.close()
        target.close()

    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--original-database", required=True)
    parser.add_argument("--target-database", required=True)
    parser.add_argument("--audit", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--data-path", required=True)
    parser.add_argument("--target-version", required=True)
    args = parser.parse_args()
    report = generate_gate(
        original_database=args.original_database,
        target_database=args.target_database,
        audit_path=args.audit,
        output_path=args.output,
        data_path=args.data_path,
        target_version_raw=args.target_version,
    )
    disposition = report["disposition"]
    print(
        "Quest version gate: "
        f"safe_to_replace={disposition['safe_to_replace']} "
        f"reported={report['summary']['reported_anomaly_count']} "
        f"current_missing_created={report['summary']['current_missing_created_count']} "
        f"current_missing_git={report['summary']['current_missing_git_count']} "
        f"current_older={report['summary']['current_older_count']}"
    )
    print(f"Gate report written: {Path(args.output).resolve()}")
    return 0 if disposition["safe_to_replace"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

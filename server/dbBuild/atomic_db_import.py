"""Run an authorized Genshin diff import on a cloned SQLite database.

The production database is never opened for writing by this module.  A
consistent SQLite backup is made first, the import subprocess points
``GTS_DB_PATH`` at ``server/data.db.tmp``, and the target is replaced only
after integrity/provenance checks pass.  Failed imports retain both the backup
and the temporary database for diagnosis.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import sqlite3
import subprocess
import sys
import time
from typing import Any


def _path(path: str | os.PathLike[str]) -> Path:
    return Path(path).expanduser().resolve()


def _unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    raise FileExistsError(f"refusing to overwrite existing path: {path}")


def _sqlite_backup(source_path: Path, destination_path: Path, label: str) -> None:
    """Create a consistent SQLite backup without writing the source database."""
    _unique_path(destination_path)
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(f"file:{source_path}?mode=ro", uri=True)
    destination = sqlite3.connect(str(destination_path))
    started = time.monotonic()
    last_report = started

    def report(status: int, remaining: int, total: int) -> None:
        nonlocal last_report
        now = time.monotonic()
        if now - last_report >= 15 or remaining == 0:
            copied = max(0, total - remaining)
            print(
                f"[{label}] SQLite backup progress: pages={copied}/{total} "
                f"remaining={remaining} elapsed={now - started:.1f}s",
                flush=True,
            )
            last_report = now

    try:
        source.backup(destination, pages=2048, progress=report, sleep=0.05)
        destination.commit()
    except Exception:
        destination.close()
        source.close()
        try:
            destination_path.unlink()
        except FileNotFoundError:
            pass
        raise
    else:
        destination.close()
        source.close()


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _archive_sidecars(db_path: Path, backup_path: Path) -> list[tuple[Path, Path]]:
    archived: list[tuple[Path, Path]] = []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{db_path}{suffix}")
        if not sidecar.exists():
            continue
        destination = Path(f"{backup_path}{suffix}")
        if destination.exists():
            # SQLite may create an empty WAL/SHM sidecar during a read-only
            # verification.  Never overwrite the sidecar belonging to the
            # saved backup; retain the current-main sidecar beside it.
            destination = Path(f"{backup_path}.current-main-{stamp}{suffix}")
            _unique_path(destination)
        os.replace(sidecar, destination)
        archived.append((sidecar, destination))
    return archived


def _restore_sidecars(archived: list[tuple[Path, Path]]) -> None:
    for original, archived_path in reversed(archived):
        if archived_path.exists() and not original.exists():
            os.replace(archived_path, original)


def _checkpoint_temp_database(temp_path: Path) -> None:
    connection = sqlite3.connect(str(temp_path))
    try:
        try:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.DatabaseError:
            pass
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
        except sqlite3.DatabaseError:
            pass
        connection.commit()
    finally:
        connection.close()
    for suffix in ("-wal", "-shm"):
        sidecar = Path(f"{temp_path}{suffix}")
        if sidecar.exists():
            sidecar.unlink()


def _integrity_check(path: Path) -> str:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = connection.execute("PRAGMA integrity_check").fetchone()
        result = str(row[0]) if row else ""
    finally:
        connection.close()
    if result.lower() != "ok":
        raise RuntimeError(f"SQLite integrity_check failed for {path}: {result!r}")
    return result


def _database_summary(path: Path) -> dict[str, Any]:
    stat = path.stat()
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        summary: dict[str, Any] = {
            "path": str(path),
            "size_bytes": int(stat.st_size),
            "mtime_ns": int(stat.st_mtime_ns),
            "tables": sorted(tables),
        }
        for table, key in (
            ("quest", "quest_count"),
            ("dialogue", "dialogue_count"),
            ("textMap", "textmap_count"),
            ("quest_created_version_override", "manual_locked_quest_count"),
            ("quest_text_version", "task_text_version_count"),
        ):
            if table in tables:
                summary[key] = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        if "app_meta" in tables:
            try:
                rows = connection.execute(
                    "SELECT key, value FROM app_meta "
                    "WHERE key IN ('db_current_commit','db_version','agd_source_url')"
                ).fetchall()
            except sqlite3.DatabaseError:
                rows = connection.execute(
                    "SELECT k, v FROM app_meta "
                    "WHERE k IN ('db_current_commit','db_version','agd_source_url')"
                ).fetchall()
            summary["app_meta"] = {str(k): str(v) for k, v in rows if v is not None}
        return summary
    finally:
        connection.close()


def _table_names(path: Path) -> set[str]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        connection.close()


def _schema_sql(path: Path, table_name: str) -> str | None:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else None
    finally:
        connection.close()


def _row_stat(path: Path, table_name: str) -> tuple[int, int, int | None, int | None]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        try:
            row = connection.execute(
                f"SELECT COUNT(*), COALESCE(SUM(rowid), 0), MIN(rowid), MAX(rowid) "
                f"FROM {table_name}"
            ).fetchone()
        except sqlite3.DatabaseError:
            row = connection.execute(
                f"SELECT COUNT(*), 0, NULL, NULL FROM {table_name}"
            ).fetchone()
        return (int(row[0] or 0), int(row[1] or 0), row[2], row[3])
    finally:
        connection.close()


def _compare_attached_tables(
    original_path: Path,
    target_path: Path,
    table_names: tuple[str, ...],
) -> list[str]:
    """Return exact row differences for small, immutable tables.

    The target connection is used only for read-only comparison.  SQLite's
    EXCEPT queries avoid loading the full tables into Python and make this
    check independent of row order.
    """
    connection = sqlite3.connect(f"file:{target_path}?mode=ro", uri=True)
    differences: list[str] = []
    try:
        connection.execute("ATTACH DATABASE ? AS original_db", (str(original_path),))
        for table_name in table_names:
            if table_name not in _table_names(original_path) or table_name not in _table_names(target_path):
                differences.append(f"missing immutable table: {table_name}")
                continue
            columns = [
                str(row[1])
                for row in connection.execute(f"PRAGMA original_db.table_info({table_name})")
            ]
            if not columns:
                continue
            quoted = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
            old_only = connection.execute(
                f"SELECT COUNT(*) FROM (SELECT {quoted} FROM original_db.\"{table_name}\" "
                f"EXCEPT SELECT {quoted} FROM main.\"{table_name}\")"
            ).fetchone()[0]
            new_only = connection.execute(
                f"SELECT COUNT(*) FROM (SELECT {quoted} FROM main.\"{table_name}\" "
                f"EXCEPT SELECT {quoted} FROM original_db.\"{table_name}\")"
            ).fetchone()[0]
            if old_only or new_only:
                differences.append(
                    f"immutable table changed: {table_name} old_only={old_only} new_only={new_only}"
                )
    finally:
        try:
            connection.execute("DETACH DATABASE original_db")
        except sqlite3.DatabaseError:
            pass
        connection.close()
    return differences


def _compare_database_snapshot(original_path: Path, target_path: Path) -> None:
    """Refuse repair if the current main DB drifted from its saved snapshot."""
    original_summary = _database_summary(original_path)
    target_summary = _database_summary(target_path)
    for key in ("tables", "quest_count", "dialogue_count", "textmap_count", "manual_locked_quest_count", "task_text_version_count", "app_meta"):
        if original_summary.get(key) != target_summary.get(key):
            raise RuntimeError(
                f"saved database snapshot differs from current main for {key}: "
                f"snapshot={original_summary.get(key)!r} current={target_summary.get(key)!r}"
            )
    _integrity_check(original_path)
    _integrity_check(target_path)
    differences = _compare_attached_tables(
        original_path,
        target_path,
        (
            "quest",
            "quest_version",
            "questTalk",
            "version_dim",
            "version_catalog",
            "app_meta",
            "source_file_version",
            "quest_created_version_override",
            "quest_version_override_audit",
        ),
    )
    if differences:
        raise RuntimeError("saved database snapshot is not the current main database: " + "; ".join(differences))


def _verify_talk_repair_scope(original_path: Path, target_path: Path) -> dict[str, Any]:
    """Prove a Talk repair did not delete or rewrite unrelated database data."""
    allowed_new_tables = {"talk_dialogue_content"}
    original_tables = _table_names(original_path)
    target_tables = _table_names(target_path)
    missing_tables = sorted(original_tables - target_tables)
    unexpected_tables = sorted(target_tables - original_tables - allowed_new_tables)
    if missing_tables:
        raise RuntimeError(f"Talk repair removed tables: {missing_tables}")
    if unexpected_tables:
        raise RuntimeError(f"Talk repair created unexpected tables: {unexpected_tables}")

    allowed_changed = {
        "dialogue",
        "talk_dialogue_link",
        "talk_dialogue_content",
        "quest_hash_map",
        "quest_text_version",
    }
    schema_differences = []
    for table_name in sorted(original_tables - allowed_changed):
        if _schema_sql(original_path, table_name) != _schema_sql(target_path, table_name):
            schema_differences.append(table_name)
    if schema_differences:
        raise RuntimeError(f"Talk repair changed unrelated table schemas: {schema_differences}")

    core_differences = _compare_attached_tables(
        original_path,
        target_path,
        (
            "quest",
            "quest_version",
            "questTalk",
            "version_dim",
            "version_catalog",
            "app_meta",
            "source_file_version",
            "quest_created_version_override",
            "quest_version_override_audit",
        ),
    )
    if core_differences:
        raise RuntimeError("Talk repair changed immutable rows: " + "; ".join(core_differences))

    connection = sqlite3.connect(f"file:{target_path}?mode=ro", uri=True)
    try:
        connection.execute("ATTACH DATABASE ? AS original_db", (str(original_path),))
        for table_name, columns, key_columns in (
            (
                "dialogue",
                ("talkerType", "talkerId", "talkId", "textHash", "coopQuestId"),
                ("dialogueId",),
            ),
            (
                "talk_dialogue_link",
                ("coopQuestId", "dialogueId"),
                ("talkId", "coopQuestId", "dialogueId"),
            ),
            (
                "quest_hash_map",
                ("hash", "source_type"),
                ("questId", "hash", "source_type"),
            ),
            (
                "quest_text_version",
                tuple(),
                ("questId", "hash"),
            ),
        ):
            if table_name not in original_tables:
                continue
            join = " AND ".join(
                f"new.\"{column}\" = old.\"{column}\""
                for column in key_columns
            )
            missing = connection.execute(
                f"SELECT COUNT(*) FROM original_db.\"{table_name}\" old "
                f"LEFT JOIN main.\"{table_name}\" new ON {join} "
                "WHERE " + " AND ".join(
                    f"new.\"{column}\" IS NULL" for column in key_columns
                )
            ).fetchone()[0]
            if missing:
                raise RuntimeError(
                    f"Talk repair deleted {missing} existing {table_name} rows"
                )
            if table_name in {"dialogue", "talk_dialogue_link"}:
                predicates = " OR ".join(
                    f"NOT (new.\"{column}\" IS old.\"{column}\")"
                    for column in columns
                )
                changed = connection.execute(
                    f"SELECT COUNT(*) FROM original_db.\"{table_name}\" old "
                    f"JOIN main.\"{table_name}\" new ON {join} "
                    f"WHERE {predicates}"
                ).fetchone()[0]
                if changed:
                    raise RuntimeError(
                        f"Talk repair rewrote {changed} existing {table_name} rows"
                    )

        quest_version_mismatches = connection.execute(
            "SELECT COUNT(*) FROM main.quest_text_version qtv "
            "JOIN main.quest q ON q.questId=qtv.questId "
            "WHERE qtv.created_version_id IS NULL "
            "OR qtv.created_version_id IS NOT q.created_version_id"
        ).fetchone()[0]
        if quest_version_mismatches:
            raise RuntimeError(
                f"Talk repair left {quest_version_mismatches} task-text version mismatches"
            )
        lock_mismatches = connection.execute(
            "SELECT COUNT(*) FROM main.quest_created_version_override o "
            "JOIN main.quest q ON q.questId=o.questId "
            "WHERE q.created_version_id IS NOT o.locked_created_version_id"
        ).fetchone()[0]
        if lock_mismatches:
            raise RuntimeError(f"Talk repair changed {lock_mismatches} manual quest locks")
        unresolved_global_rows = connection.execute(
            "SELECT COUNT(*) FROM main.quest_text_version qtv "
            "JOIN main.textMap tm ON tm.hash=qtv.hash "
            "WHERE qtv.alignment_status='unresolved_textmap_hash'"
        ).fetchone()[0]
        if unresolved_global_rows:
            raise RuntimeError(
                f"Talk repair left {unresolved_global_rows} unresolved task hashes with global rows"
            )
        return {
            "original_tables": len(original_tables),
            "target_tables": len(target_tables),
            "dialogue_rows_before": _row_stat(original_path, "dialogue")[0],
            "dialogue_rows_after": _row_stat(target_path, "dialogue")[0],
            "link_rows_before": _row_stat(original_path, "talk_dialogue_link")[0],
            "link_rows_after": _row_stat(target_path, "talk_dialogue_link")[0],
            "quest_hash_rows_before": _row_stat(original_path, "quest_hash_map")[0],
            "quest_hash_rows_after": _row_stat(target_path, "quest_hash_map")[0],
            "task_text_rows_before": _row_stat(original_path, "quest_text_version")[0],
            "task_text_rows_after": _row_stat(target_path, "quest_text_version")[0],
        }
    finally:
        try:
            connection.execute("DETACH DATABASE original_db")
        except sqlite3.DatabaseError:
            pass
        connection.close()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rebase_talk_repair_audit(
    audit_path: Path,
    source_database: Path,
    output_path: Path,
) -> Path:
    """Create an explicit audit lineage after the original 6.x->7.0 import."""
    _unique_path(output_path)
    with audit_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
        raise ValueError("quest audit must be an object containing records")
    rebased = dict(payload)
    rebased["audit_kind"] = "quest_created_version_text_audit_rebased_for_talk_repair"
    rebased["parent_audit_path"] = str(audit_path)
    rebased["parent_audit_sha256"] = _sha256_file(audit_path)
    rebased["repair_scope"] = {
        "tables_allowed_to_grow": [
            "dialogue",
            "talk_dialogue_link",
            "talk_dialogue_content",
            "quest_hash_map",
            "quest_text_version",
        ],
        "reason": "merge current Talk source without deleting or rewriting historical rows",
    }
    rebased["source_database"] = _database_summary(source_database)
    rebased["generated_at"] = datetime.now(timezone.utc).isoformat()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(rebased, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _archive_existing_temp(temp_path: Path, stamp: str) -> list[str]:
    archived: list[str] = []
    for suffix in ("", "-wal", "-shm"):
        source = Path(f"{temp_path}{suffix}")
        if not source.exists():
            continue
        destination = Path(f"{temp_path}.superseded-{stamp}{suffix}")
        _unique_path(destination)
        os.replace(source, destination)
        archived.append(str(destination))
    return archived


def prepare_talk_repair_temp(
    *,
    db_path: str,
    data_path: str,
    backup_path: str,
    python_executable: str | None = None,
) -> dict[str, Any]:
    """Clone main and merge current Talk data without destructive rebuilds."""
    target_path = _path(db_path)
    source_data_path = _path(data_path)
    backup_file = _path(backup_path)
    temp_path = Path(f"{target_path}.tmp")
    for required in (target_path, backup_file):
        if not required.is_file():
            raise FileNotFoundError(required)
    _compare_database_snapshot(backup_file, target_path)
    superseded: list[str] = []
    if temp_path.exists() or Path(f"{temp_path}-wal").exists() or Path(f"{temp_path}-shm").exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        superseded = _archive_existing_temp(temp_path, stamp)
        print(f"[TALK-REPAIR] retained superseded temporary files: {superseded}", flush=True)

    shutil.copy2(backup_file, temp_path)
    _fsync_file(temp_path)
    print(f"[TALK-REPAIR] cloned current-main backup to {temp_path}", flush=True)
    child_env = os.environ.copy()
    child_env.update(
        {
            "GTS_DB_PATH": str(temp_path),
            "GTS_DATA_PATH": str(source_data_path),
            "PYTHONUNBUFFERED": "1",
        }
    )
    child_script = (
        "from DBConfig import conn; "
        "from questImport import mergeAllTalkItems; "
        "from quest_version_provenance import refresh_quest_text_versions; "
        "rows=mergeAllTalkItems(commit=False); "
        "stats=refresh_quest_text_versions(conn.cursor()); "
        "conn.commit(); "
        "print({'imported_rows': rows, 'provenance': stats}, flush=True)"
    )
    exit_code = _run_child(
        [python_executable or sys.executable, "-c", child_script],
        child_env,
        Path(__file__).resolve().parent,
    )
    if exit_code != 0:
        raise RuntimeError(
            f"non-destructive Talk merge failed with exit code {exit_code}; "
            f"temporary database retained at {temp_path}"
        )
    _checkpoint_temp_database(temp_path)
    integrity = _integrity_check(temp_path)
    return {
        "temporary_path": str(temp_path),
        "superseded_temporary_paths": superseded,
        "integrity_check": integrity,
        "summary": _database_summary(temp_path),
    }


def _write_talk_coverage_report(
    database_path: Path,
    data_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    from talk_coverage_gate import assert_talk_dialogue_coverage

    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True)
    try:
        report = assert_talk_dialogue_coverage(connection.cursor(), str(data_path))
    finally:
        connection.close()
    output_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def finalize_talk_repair(
    *,
    db_path: str,
    data_path: str,
    audit_path: str,
    backup_path: str,
    target_version_raw: str,
    gate_output_path: str,
    talk_gate_output_path: str,
    rebased_audit_output_path: str,
) -> dict[str, Any]:
    """Validate and atomically install a non-destructive Talk repair temp DB."""
    target_path = _path(db_path)
    source_data_path = _path(data_path)
    audit_file = _path(audit_path)
    backup_file = _path(backup_path)
    temp_path = Path(f"{target_path}.tmp")
    for required in (target_path, temp_path, audit_file, backup_file):
        if not required.exists():
            raise FileNotFoundError(required)

    _compare_database_snapshot(backup_file, target_path)
    parent_provenance = _verify_prepared_provenance(target_path, audit_file)
    rebased_audit = _rebase_talk_repair_audit(
        audit_file,
        target_path,
        _path(rebased_audit_output_path),
    )
    _checkpoint_temp_database(temp_path)
    temporary_integrity = _integrity_check(temp_path)
    scope = _verify_talk_repair_scope(target_path, temp_path)
    provenance = _verify_prepared_provenance(temp_path, rebased_audit)
    talk_gate = _write_talk_coverage_report(
        temp_path,
        source_data_path,
        _path(talk_gate_output_path),
    )
    target_summary = _database_summary(target_path)
    _validate_audit_source(rebased_audit, target_summary)
    quest_gate = _run_quest_version_gate(
        original_path=target_path,
        target_path=temp_path,
        audit_path=rebased_audit,
        data_path=source_data_path,
        target_version_raw=target_version_raw,
        output_path=_path(gate_output_path),
    )
    temporary_summary = _database_summary(temp_path)
    print(f"[TALK-REPAIR] temporary integrity_check={temporary_integrity}", flush=True)
    print(f"[TALK-REPAIR] scope verification: {scope}", flush=True)
    print(f"[TALK-REPAIR] Talk coverage gate: {talk_gate}", flush=True)
    print(f"[TALK-REPAIR] provenance verification: {provenance}", flush=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived_sidecars: list[tuple[Path, Path]] = []
    replaced = False
    try:
        archived_sidecars = _archive_sidecars(target_path, backup_file)
        os.replace(temp_path, target_path)
        replaced = True
        _fsync_directory(target_path.parent)
        final_integrity = _integrity_check(target_path)
        final_scope = _verify_talk_repair_scope(backup_file, target_path)
        final_talk_gate = _write_talk_coverage_report(
            target_path,
            source_data_path,
            _path(talk_gate_output_path),
        )
    except Exception:
        if replaced and target_path.exists():
            failed_path = Path(f"{target_path}.failed-{stamp}")
            _unique_path(failed_path)
            os.replace(target_path, failed_path)
            shutil.copy2(backup_file, target_path)
            _fsync_file(target_path)
        _restore_sidecars(archived_sidecars)
        _fsync_directory(target_path.parent)
        raise

    final_summary = _database_summary(target_path)
    print(f"[TALK-REPAIR] atomic replacement complete: {target_path}", flush=True)
    print(f"[TALK-REPAIR] final integrity_check={final_integrity}", flush=True)
    return {
        "source": target_summary,
        "backup_path": str(backup_file),
        "temporary_path": str(temp_path),
        "rebased_audit_path": str(rebased_audit),
        "temporary": temporary_summary,
        "final": final_summary,
        "integrity_check": final_integrity,
        "scope": scope,
        "final_scope": final_scope,
        "talk_coverage_gate": final_talk_gate,
        "provenance": {"parent": parent_provenance, "temporary": provenance},
        "quest_version_gate": quest_gate,
        "archived_sidecars": [str(path) for _, path in archived_sidecars],
    }


def _verify_prepared_provenance(path: Path, audit_path: Path) -> dict[str, int]:
    with audit_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    records = payload.get("records", []) if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise ValueError("manual quest audit must contain a records list")
    expected_locks = {
        int(row["questId"]): (
            row.get("final_created_version_id"),
            row.get("candidate_created_version_id"),
        )
        for row in records
        if isinstance(row, dict) and row.get("status") == "manual_difference"
    }
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        actual_rows = connection.execute(
            "SELECT questId, locked_created_version_id, candidate_created_version_id "
            "FROM quest_created_version_override ORDER BY questId"
        ).fetchall()
        actual_locks = {
            int(row[0]): (row[1], row[2])
            for row in actual_rows
        }
        actual_quest_values = {
            int(row[0]): row[1]
            for row in connection.execute(
                "SELECT questId, created_version_id FROM quest "
                "WHERE questId IN (SELECT questId FROM quest_created_version_override)"
            ).fetchall()
        }
        prepared = connection.execute(
            "SELECT COUNT(*) FROM quest_version_override_audit WHERE status='prepared'"
        ).fetchone()[0]
    finally:
        connection.close()
    if set(actual_locks) != set(expected_locks):
        raise RuntimeError(
            "temporary database manual-lock set differs from read-only audit: "
            f"expected={len(expected_locks)} actual={len(actual_locks)}"
        )
    for quest_id, (expected_final, expected_candidate) in expected_locks.items():
        actual_locked, actual_candidate = actual_locks[quest_id]
        normalized_final = int(expected_final) if expected_final is not None else None
        normalized_candidate = int(expected_candidate) if expected_candidate is not None else None
        if (actual_locked, actual_candidate) != (normalized_final, normalized_candidate):
            raise RuntimeError(
                "temporary database manual-lock value differs from read-only audit: "
                f"questId={quest_id} expected={(normalized_final, normalized_candidate)!r} "
                f"actual={(actual_locked, actual_candidate)!r}"
            )
        if actual_quest_values.get(quest_id) != normalized_final:
            raise RuntimeError(
                "temporary database quest created_version_id differs from locked audit value: "
                f"questId={quest_id} expected={normalized_final!r} "
                f"actual={actual_quest_values.get(quest_id)!r}"
            )
    if int(prepared or 0) <= 0:
        raise RuntimeError("temporary database has no prepared quest version audit marker")
    return {"expected_locked": len(expected_locks), "prepared_audit_rows": int(prepared)}


def _validate_audit_source(audit_path: Path, source_summary: dict[str, Any]) -> None:
    with audit_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    source = payload.get("source_database", {}) if isinstance(payload, dict) else {}
    expected_size = source.get("size_bytes")
    expected_mtime_ns = source.get("mtime_ns")
    if expected_size is not None and int(expected_size) != int(source_summary["size_bytes"]):
        raise RuntimeError(
            "pre-import audit was generated from a different database size: "
            f"audit={expected_size} current={source_summary['size_bytes']}"
        )
    if expected_mtime_ns is not None and int(expected_mtime_ns) != int(source_summary["mtime_ns"]):
        raise RuntimeError(
            "pre-import audit was generated from a different database mtime; "
            "refusing to guess which quest values are manual"
        )


def _rebase_audit_with_existing_manual_locks(
    source_database: Path,
    audit_path: Path,
    output_path: Path,
) -> Path:
    """Merge immutable existing manual locks into a freshly generated audit."""
    connection = sqlite3.connect(f"file:{source_database}?mode=ro", uri=True)
    try:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if "quest_created_version_override" not in tables:
            return audit_path
        lock_rows = connection.execute(
            "SELECT o.questId, o.locked_created_version_id, "
            "o.current_created_version_id, o.candidate_created_version_id, "
            "q.created_version_id "
            "FROM quest_created_version_override o "
            "LEFT JOIN quest q ON q.questId=o.questId ORDER BY o.questId"
        ).fetchall()
    finally:
        connection.close()
    if not lock_rows:
        return audit_path

    with audit_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
        raise ValueError("quest audit must be an object containing records")
    records = [dict(row) for row in payload["records"] if isinstance(row, dict)]
    records_by_id: dict[int, dict[str, Any]] = {}
    for record in records:
        try:
            records_by_id[int(record.get("questId"))] = record
        except (TypeError, ValueError):
            continue

    for quest_id, locked_id, current_id, candidate_id, actual_id in lock_rows:
        quest_id = int(quest_id)
        if actual_id != locked_id or current_id != locked_id:
            raise RuntimeError(
                "existing manual quest lock differs from current database value: "
                f"questId={quest_id} locked={locked_id!r} current={current_id!r} "
                f"actual={actual_id!r}"
            )
        record = records_by_id.get(quest_id)
        if record is None:
            raise RuntimeError(
                f"fresh quest audit omitted existing manually locked questId={quest_id}"
            )
        previous_status = record.get("status")
        previous_candidate = record.get("candidate_created_version_id")
        record.update(
            {
                "status": "manual_difference",
                "current_created_version_id": current_id,
                "candidate_created_version_id": candidate_id,
                "final_created_version_id": locked_id,
                "manual_lock_rebased": True,
                "pre_rebase_status": previous_status,
                "pre_rebase_candidate_created_version_id": previous_candidate,
            }
        )

    rebased = dict(payload)
    rebased["audit_kind"] = "quest_created_version_text_audit_with_existing_manual_locks"
    rebased["parent_audit_path"] = str(audit_path)
    rebased["parent_audit_sha256"] = _sha256_file(audit_path)
    rebased["existing_manual_lock_count"] = len(lock_rows)
    rebased["records"] = records
    summary = dict(payload.get("summary") or {})
    summary["candidate_count"] = len(records)
    summary["difference_count"] = sum(
        1 for record in records if record.get("status") == "manual_difference"
    )
    summary["unresolved_count"] = sum(
        1 for record in records if record.get("status") == "unresolved"
    )
    summary["same_count"] = sum(
        1 for record in records if record.get("status") == "same"
    )
    rebased["summary"] = summary
    rebased["generated_at"] = datetime.now(timezone.utc).isoformat()

    _unique_path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(rebased, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path


def _run_child(command: list[str], env: dict[str, str], cwd: Path) -> int:
    process = subprocess.Popen(
        command,
        cwd=str(cwd),
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    print(f"[IMPORT] child pid={process.pid}: {' '.join(command)}", flush=True)
    selector = selectors.DefaultSelector()
    assert process.stdout is not None
    selector.register(process.stdout, selectors.EVENT_READ)
    started = time.monotonic()
    last_output = started
    try:
        while True:
            events = selector.select(timeout=15)
            if events:
                for key, _ in events:
                    line = key.fileobj.readline()
                    if line:
                        print(f"[IMPORT] {line.rstrip()}", flush=True)
                        last_output = time.monotonic()
                    else:
                        selector.unregister(key.fileobj)
            return_code = process.poll()
            if return_code is not None:
                for line in process.stdout:
                    print(f"[IMPORT] {line.rstrip()}", flush=True)
                print(
                    f"[IMPORT] child pid={process.pid} exited code={return_code} "
                    f"elapsed={time.monotonic() - started:.1f}s",
                    flush=True,
                )
                return int(return_code)
            if time.monotonic() - last_output >= 15:
                print(
                    f"[IMPORT] child pid={process.pid} still running; "
                    f"elapsed={time.monotonic() - started:.1f}s",
                    flush=True,
                )
                last_output = time.monotonic()
    finally:
        selector.close()


def _resolve_target_version_raw(data_path: Path, to_commit: str) -> str:
    """Read the exact release label from the checked-out AnimeGameData commit."""
    output = subprocess.check_output(
        ["git", "-C", str(data_path), "show", "-s", "--format=%s", to_commit],
        text=True,
    ).strip()
    if not output:
        raise RuntimeError(f"target commit has no release label: {to_commit}")
    return output


def _run_quest_version_gate(
    *,
    original_path: Path,
    target_path: Path,
    audit_path: Path,
    data_path: Path,
    target_version_raw: str,
    output_path: Path,
) -> dict[str, Any]:
    # Import locally so read-only gate tests do not initialize the importer.
    from quest_version_gate import generate_gate

    report = generate_gate(
        original_database=str(original_path),
        target_database=str(target_path),
        audit_path=str(audit_path),
        output_path=str(output_path),
        data_path=str(data_path),
        target_version_raw=target_version_raw,
    )
    disposition = report.get("disposition", {})
    print(
        "[IMPORT] Quest version gate: "
        f"safe_to_replace={disposition.get('safe_to_replace')} "
        f"report={output_path}",
        flush=True,
    )
    if not disposition.get("safe_to_replace"):
        reasons = disposition.get("reasons") or ["unspecified gate failure"]
        raise RuntimeError(
            "Quest version gate failed; original database was not replaced: "
            + "; ".join(str(reason) for reason in reasons)
        )
    return report


def atomic_diff_update(
    *,
    db_path: str,
    data_path: str,
    audit_path: str,
    from_commit: str,
    to_commit: str,
    backup_path: str | None = None,
    prune_missing: bool = True,
    python_executable: str | None = None,
    target_version_raw: str | None = None,
    gate_output_path: str | None = None,
    rebased_audit_output_path: str | None = None,
) -> dict[str, Any]:
    target_path = _path(db_path)
    source_data_path = _path(data_path)
    audit_file = _path(audit_path)
    if not target_path.is_file():
        raise FileNotFoundError(target_path)
    if not audit_file.is_file():
        raise FileNotFoundError(
            f"pre-import quest version/text audit is required: {audit_file}"
        )
    temp_path = Path(f"{target_path}.tmp")
    if temp_path.exists():
        raise FileExistsError(
            f"refusing to overwrite existing temporary database; inspect/remove only after diagnosis: {temp_path}"
        )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_file = _path(backup_path) if backup_path else Path(f"{target_path}.backup-{stamp}")
    source_summary = _database_summary(target_path)
    _validate_audit_source(audit_file, source_summary)
    effective_audit_file = _rebase_audit_with_existing_manual_locks(
        target_path,
        audit_file,
        _path(rebased_audit_output_path)
        if rebased_audit_output_path
        else Path(f"/tmp/{audit_file.stem}-manual-locks-{stamp}.json"),
    )
    print(f"[IMPORT] source summary: {json.dumps(source_summary, ensure_ascii=False, sort_keys=True)}", flush=True)
    print(f"[IMPORT] backup path: {backup_file}", flush=True)
    print(f"[IMPORT] temporary path: {temp_path}", flush=True)
    print(f"[IMPORT] audit path: {audit_file}", flush=True)
    if effective_audit_file != audit_file:
        print(f"[IMPORT] effective audit path: {effective_audit_file}", flush=True)

    _sqlite_backup(target_path, backup_file, "backup")
    _fsync_file(backup_file)
    shutil.copy2(backup_file, temp_path)
    _fsync_file(temp_path)
    print(f"[IMPORT] cloned source to temporary database: {temp_path}", flush=True)

    child_env = os.environ.copy()
    child_env.update(
        {
            "GTS_DB_PATH": str(temp_path),
            "GTS_DATA_PATH": str(source_data_path),
            "GTS_MANUAL_QUEST_VERSION_AUDIT": str(effective_audit_file),
            "PYTHONUNBUFFERED": "1",
        }
    )
    dbbuild_dir = Path(__file__).resolve().parent
    command = [
        python_executable or sys.executable,
        "DBBuild.py",
        "--diff-update",
        "--from-commit",
        from_commit,
        "--to-commit",
        to_commit,
        "--no-fetch",
    ]
    if not prune_missing:
        command.append("--no-prune-missing")
    exit_code = _run_child(command, child_env, dbbuild_dir)
    if exit_code != 0:
        raise RuntimeError(
            f"diff import failed with exit code {exit_code}; original database was not replaced; "
            f"temporary database retained at {temp_path}"
        )

    _checkpoint_temp_database(temp_path)
    integrity = _integrity_check(temp_path)
    provenance = _verify_prepared_provenance(temp_path, effective_audit_file)
    target_version_raw = target_version_raw or _resolve_target_version_raw(
        source_data_path, to_commit
    )
    gate_label = "".join(
        char if char.isalnum() else "-" for char in target_version_raw
    ).strip("-") or "target"
    gate_file = _path(gate_output_path) if gate_output_path else Path(
        f"/tmp/genshin-quest-version-gate-{gate_label}.json"
    )
    gate = _run_quest_version_gate(
        original_path=target_path,
        target_path=temp_path,
        audit_path=effective_audit_file,
        data_path=source_data_path,
        target_version_raw=target_version_raw,
        output_path=gate_file,
    )
    temporary_summary = _database_summary(temp_path)
    print(f"[IMPORT] temporary integrity_check={integrity}", flush=True)
    print(f"[IMPORT] temporary summary: {json.dumps(temporary_summary, ensure_ascii=False, sort_keys=True)}", flush=True)
    print(f"[IMPORT] provenance verification: {provenance}", flush=True)

    archived_sidecars: list[tuple[Path, Path]] = []
    replaced = False
    try:
        archived_sidecars = _archive_sidecars(target_path, backup_file)
        os.replace(temp_path, target_path)
        replaced = True
        _fsync_directory(target_path.parent)
    except Exception:
        _restore_sidecars(archived_sidecars)
        raise
    try:
        final_summary = _database_summary(target_path)
        final_integrity = _integrity_check(target_path)
    except Exception:
        if replaced:
            # Keep the verified backup and restore the original target if a
            # post-replacement validation fails.  The temporary/new target is
            # moved aside rather than deleted for diagnosis.
            failed_path = Path(f"{target_path}.failed-{stamp}")
            _unique_path(failed_path)
            os.replace(target_path, failed_path)
            shutil.copy2(backup_file, target_path)
            _fsync_file(target_path)
            _restore_sidecars(archived_sidecars)
            _fsync_directory(target_path.parent)
        raise
    print(f"[IMPORT] atomic replacement complete: {target_path}", flush=True)
    print(f"[IMPORT] final integrity_check={final_integrity}", flush=True)
    return {
        "source": source_summary,
        "backup_path": str(backup_file),
        "temporary_path": str(temp_path),
        "temporary": temporary_summary,
        "final": final_summary,
        "integrity_check": final_integrity,
        "audit_path": str(audit_file),
        "effective_audit_path": str(effective_audit_file),
        "provenance": provenance,
        "quest_version_gate": gate,
        "archived_sidecars": [str(path) for _, path in archived_sidecars],
    }


def finalize_existing_temp(
    *,
    db_path: str,
    data_path: str,
    audit_path: str,
    backup_path: str,
    target_version_raw: str,
    gate_output_path: str,
) -> dict[str, Any]:
    """Finish a child import that completed before a safety pause.

    This path never starts another import and never overwrites an existing
    backup.  All checks, including the Quest anomaly gate, run before the
    target is moved; an unsafe result therefore leaves both databases intact.
    """
    target_path = _path(db_path)
    source_data_path = _path(data_path)
    audit_file = _path(audit_path)
    backup_file = _path(backup_path)
    temp_path = Path(f"{target_path}.tmp")
    for required in (target_path, temp_path, audit_file, backup_file):
        if not required.exists():
            raise FileNotFoundError(required)

    source_summary = _database_summary(target_path)
    _validate_audit_source(audit_file, source_summary)
    _checkpoint_temp_database(temp_path)
    integrity = _integrity_check(temp_path)
    provenance = _verify_prepared_provenance(temp_path, audit_file)
    gate = _run_quest_version_gate(
        original_path=target_path,
        target_path=temp_path,
        audit_path=audit_file,
        data_path=source_data_path,
        target_version_raw=target_version_raw,
        output_path=_path(gate_output_path),
    )
    temporary_summary = _database_summary(temp_path)
    print(f"[FINALIZE] temporary integrity_check={integrity}", flush=True)
    print(
        "[FINALIZE] temporary summary: "
        f"{json.dumps(temporary_summary, ensure_ascii=False, sort_keys=True)}",
        flush=True,
    )
    print(f"[FINALIZE] provenance verification: {provenance}", flush=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archived_sidecars: list[tuple[Path, Path]] = []
    replaced = False
    try:
        archived_sidecars = _archive_sidecars(target_path, backup_file)
        os.replace(temp_path, target_path)
        replaced = True
        _fsync_directory(target_path.parent)
        final_summary = _database_summary(target_path)
        final_integrity = _integrity_check(target_path)
    except Exception:
        if replaced and target_path.exists():
            failed_path = Path(f"{target_path}.failed-{stamp}")
            _unique_path(failed_path)
            os.replace(target_path, failed_path)
            shutil.copy2(backup_file, target_path)
            _fsync_file(target_path)
        _restore_sidecars(archived_sidecars)
        _fsync_directory(target_path.parent)
        raise

    print(f"[FINALIZE] atomic replacement complete: {target_path}", flush=True)
    print(f"[FINALIZE] final integrity_check={final_integrity}", flush=True)
    return {
        "source": source_summary,
        "backup_path": str(backup_file),
        "temporary_path": str(temp_path),
        "temporary": temporary_summary,
        "final": final_summary,
        "integrity_check": final_integrity,
        "provenance": provenance,
        "quest_version_gate": gate,
        "archived_sidecars": [str(path) for _, path in archived_sidecars],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    default_root = Path(__file__).resolve().parents[2]
    parser.add_argument("--db", default=str(default_root / "server" / "data.db"))
    parser.add_argument("--data-path", default=str(default_root.parent / "AnimeGameData"))
    parser.add_argument("--audit", required=True)
    parser.add_argument("--from-commit", default="")
    parser.add_argument("--to-commit", default="")
    parser.add_argument("--backup-path", default="")
    parser.add_argument("--target-version", default="")
    parser.add_argument("--gate-output", default="")
    parser.add_argument(
        "--finalize-existing-temp",
        action="store_true",
        help="validate and atomically replace an already completed server/data.db.tmp",
    )
    parser.add_argument(
        "--prepare-talk-repair",
        action="store_true",
        help="clone main and run the non-destructive recursive Talk merge",
    )
    parser.add_argument(
        "--finalize-talk-repair",
        action="store_true",
        help="validate and atomically replace a non-destructive Talk repair temp DB",
    )
    parser.add_argument("--talk-gate-output", default="")
    parser.add_argument("--rebased-audit-output", default="")
    parser.add_argument("--no-prune-missing", action="store_true")
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    if args.prepare_talk_repair:
        if not args.backup_path:
            parser.error("--prepare-talk-repair requires --backup-path")
        result = prepare_talk_repair_temp(
            db_path=args.db,
            data_path=args.data_path,
            backup_path=args.backup_path,
            python_executable=args.python,
        )
    elif args.finalize_talk_repair:
        if not args.backup_path or not args.target_version or not args.gate_output:
            parser.error(
                "--finalize-talk-repair requires --backup-path, --target-version, and --gate-output"
            )
        talk_gate_output = args.talk_gate_output or "/tmp/genshin-talk-coverage-gate-talkfix.json"
        rebased_audit_output = args.rebased_audit_output or "/tmp/genshin-quest-created-version-text-audit-talkfix.json"
        result = finalize_talk_repair(
            db_path=args.db,
            data_path=args.data_path,
            audit_path=args.audit,
            backup_path=args.backup_path,
            target_version_raw=args.target_version,
            gate_output_path=args.gate_output,
            talk_gate_output_path=talk_gate_output,
            rebased_audit_output_path=rebased_audit_output,
        )
    elif args.finalize_existing_temp:
        if not args.backup_path or not args.target_version or not args.gate_output:
            parser.error(
                "--finalize-existing-temp requires --backup-path, --target-version, and --gate-output"
            )
        result = finalize_existing_temp(
            db_path=args.db,
            data_path=args.data_path,
            audit_path=args.audit,
            backup_path=args.backup_path,
            target_version_raw=args.target_version,
            gate_output_path=args.gate_output,
        )
    else:
        if not args.from_commit or not args.to_commit:
            parser.error("normal atomic import requires --from-commit and --to-commit")
        result = atomic_diff_update(
            db_path=args.db,
            data_path=args.data_path,
            audit_path=args.audit,
            from_commit=args.from_commit,
            to_commit=args.to_commit,
            backup_path=args.backup_path or None,
            prune_missing=not args.no_prune_missing,
            python_executable=args.python,
            target_version_raw=args.target_version or None,
            gate_output_path=args.gate_output or None,
            rebased_audit_output_path=args.rebased_audit_output or None,
        )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

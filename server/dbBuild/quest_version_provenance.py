"""Persistent provenance for quest creation-version overrides and task text links.

The upstream database stores ``textMap.created_version_id`` once per language/hash.
That is not sufficient when one text hash is used by quests whose creation-version
decisions differ.  This module keeps the task-scoped effective version separately;
the global TextMap row is never silently overwritten for those shared hashes.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from typing import Iterable


QUEST_CREATED_VERSION_OVERRIDE_TABLE = "quest_created_version_override"
QUEST_VERSION_OVERRIDE_AUDIT_TABLE = "quest_version_override_audit"
QUEST_TEXT_VERSION_TABLE = "quest_text_version"


def ensure_quest_version_provenance_schema(cursor) -> None:
    """Create the durable override and task-text provenance tables idempotently."""
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUEST_CREATED_VERSION_OVERRIDE_TABLE} (
            questId INTEGER PRIMARY KEY,
            locked_created_version_id INTEGER,
            current_created_version_id INTEGER,
            candidate_created_version_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL,
            locked_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {QUEST_CREATED_VERSION_OVERRIDE_TABLE}_candidate_index
        ON {QUEST_CREATED_VERSION_OVERRIDE_TABLE}(candidate_created_version_id)
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUEST_VERSION_OVERRIDE_AUDIT_TABLE} (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_path TEXT NOT NULL,
            source_sha256 TEXT,
            candidate_count INTEGER NOT NULL,
            difference_count INTEGER NOT NULL,
            unresolved_count INTEGER NOT NULL,
            missing_quest_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            prepared_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {QUEST_TEXT_VERSION_TABLE} (
            questId INTEGER NOT NULL,
            hash INTEGER NOT NULL,
            created_version_id INTEGER,
            relation_types TEXT NOT NULL DEFAULT '',
            alignment_status TEXT NOT NULL,
            textmap_row_count INTEGER NOT NULL DEFAULT 0,
            textmap_created_version_row_count INTEGER NOT NULL DEFAULT 0,
            textmap_version_count INTEGER NOT NULL DEFAULT 0,
            textmap_unresolved_row_count INTEGER NOT NULL DEFAULT 0,
            aligned_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (questId, hash)
        )
        """
    )
    existing_columns = _table_columns(cursor, QUEST_TEXT_VERSION_TABLE)
    for column, definition in (
        ("textmap_row_count", "INTEGER NOT NULL DEFAULT 0"),
        ("textmap_created_version_row_count", "INTEGER NOT NULL DEFAULT 0"),
        ("textmap_version_count", "INTEGER NOT NULL DEFAULT 0"),
        ("textmap_unresolved_row_count", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if column not in existing_columns:
            cursor.execute(
                f"ALTER TABLE {QUEST_TEXT_VERSION_TABLE} ADD COLUMN {column} {definition}"
            )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {QUEST_TEXT_VERSION_TABLE}_hash_index
        ON {QUEST_TEXT_VERSION_TABLE}(hash)
        """
    )
    cursor.execute(
        f"""
        CREATE INDEX IF NOT EXISTS {QUEST_TEXT_VERSION_TABLE}_version_index
        ON {QUEST_TEXT_VERSION_TABLE}(created_version_id)
        """
    )


def _table_exists(cursor, table_name: str) -> bool:
    row = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(cursor, table_name: str) -> set[str]:
    return {str(row[1]) for row in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _association_sql(cursor, target_table: str = "_quest_text_target") -> list[str]:
    """Return the real-schema task-to-hash relation queries available in this DB."""
    fragments: list[str] = []
    q_columns = _table_columns(cursor, "quest")
    target_join = f"JOIN {target_table} target ON target.questId=q.questId"
    if "titleTextMapHash" in q_columns:
        fragments.append(
            f"SELECT q.questId, q.titleTextMapHash AS hash, 'quest_title' AS relation "
            f"FROM quest q {target_join} "
            "WHERE q.titleTextMapHash IS NOT NULL AND q.titleTextMapHash<>0"
        )
    if "descTextMapHash" in q_columns:
        fragments.append(
            f"SELECT q.questId, q.descTextMapHash, 'quest_desc' "
            f"FROM quest q {target_join} "
            "WHERE q.descTextMapHash IS NOT NULL AND q.descTextMapHash<>0"
        )
    if "longDescTextMapHash" in q_columns:
        fragments.append(
            f"SELECT q.questId, q.longDescTextMapHash, 'quest_long_desc' "
            f"FROM quest q {target_join} "
            "WHERE q.longDescTextMapHash IS NOT NULL AND q.longDescTextMapHash<>0"
        )
    if _table_exists(cursor, "chapter") and "chapterId" in q_columns:
        chapter_columns = _table_columns(cursor, "chapter")
        for column, relation in (
            ("chapterTitleTextMapHash", "chapter_title"),
            ("chapterNumTextMapHash", "chapter_number"),
        ):
            if column in chapter_columns:
                fragments.append(
                    f"SELECT q.questId, c.{column}, '{relation}' "
                    f"FROM quest q {target_join} JOIN chapter c ON c.chapterId=q.chapterId "
                    f"WHERE c.{column} IS NOT NULL AND c.{column}<>0"
                )
    if _table_exists(cursor, "questTalk"):
        quest_talk_columns = _table_columns(cursor, "questTalk")
        if "stepTitleTextMapHash" in quest_talk_columns:
            fragments.append(
                f"SELECT qt.questId, qt.stepTitleTextMapHash, 'step_title' "
                f"FROM questTalk qt JOIN {target_table} target ON target.questId=qt.questId "
                "WHERE qt.stepTitleTextMapHash IS NOT NULL AND qt.stepTitleTextMapHash<>0"
            )
    if _table_exists(cursor, "quest_hash_map"):
        fragments.append(
            f"SELECT qhm.questId, qhm.hash, 'qhm_'||qhm.source_type "
            f"FROM quest_hash_map qhm JOIN {target_table} target ON target.questId=qhm.questId "
            "WHERE qhm.hash IS NOT NULL AND qhm.hash<>0"
        )
    if _table_exists(cursor, "quest_text_signature"):
        signature_columns = _table_columns(cursor, "quest_text_signature")
        if "titleTextMapHash" in signature_columns:
            fragments.append(
                f"SELECT qts.questId, qts.titleTextMapHash, 'signature_title' "
                f"FROM quest_text_signature qts JOIN {target_table} target ON target.questId=qts.questId "
                "WHERE qts.titleTextMapHash IS NOT NULL AND qts.titleTextMapHash<>0"
            )
    if _table_exists(cursor, "dialogue") and _table_exists(cursor, "questTalk"):
        dialogue_columns = _table_columns(cursor, "dialogue")
        qt_columns = _table_columns(cursor, "questTalk")
        if "textHash" in dialogue_columns and {"talkId", "questId"}.issubset(qt_columns):
            has_scoped_content = (
                _table_exists(cursor, "talk_dialogue_content")
                and cursor.execute("SELECT 1 FROM talk_dialogue_content LIMIT 1").fetchone()
                is not None
            )
            if has_scoped_content:
                fragments.append(
                    f"SELECT qt.questId, d.textHash, 'scoped_dialogue' "
                    f"FROM questTalk qt JOIN {target_table} target ON target.questId=qt.questId "
                    "JOIN talk_dialogue_content d "
                    "ON d.talkId=qt.talkId AND d.coopQuestId=coalesce(qt.coopQuestId,0) "
                    "WHERE d.textHash IS NOT NULL AND d.textHash<>0"
                )
                return fragments
            coop_join = ""
            if "coopQuestId" in qt_columns and "coopQuestId" in dialogue_columns:
                coop_join = (
                    " AND ((coalesce(qt.coopQuestId,0)=0 AND d.coopQuestId IS NULL) "
                    "OR (coalesce(qt.coopQuestId,0)>0 AND d.coopQuestId=qt.coopQuestId))"
                )
            fragments.append(
                f"SELECT qt.questId, d.textHash, 'direct_dialogue' "
                f"FROM questTalk qt JOIN {target_table} target ON target.questId=qt.questId "
                f"JOIN dialogue d ON d.talkId=qt.talkId{coop_join} "
                "WHERE d.textHash IS NOT NULL AND d.textHash<>0"
            )
            if _table_exists(cursor, "talk_dialogue_link") and "dialogueId" in dialogue_columns:
                link_columns = _table_columns(cursor, "talk_dialogue_link")
                link_join = (
                    "ON tdl.talkId=qt.talkId AND tdl.coopQuestId=coalesce(qt.coopQuestId,0)"
                    if "coopQuestId" in link_columns
                    else "ON tdl.talkId=qt.talkId"
                )
                fragments.append(
                    f"SELECT qt.questId, d.textHash, 'linked_dialogue' "
                    f"FROM questTalk qt JOIN {target_table} target ON target.questId=qt.questId "
                    f"JOIN talk_dialogue_link tdl {link_join} "
                    "JOIN dialogue d ON d.dialogueId=tdl.dialogueId "
                    "WHERE d.textHash IS NOT NULL AND d.textHash<>0"
                )
    return fragments


def _prepare_target_table(cursor, quest_ids: Iterable[int] | None) -> str:
    cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _quest_text_target(questId INTEGER PRIMARY KEY)")
    cursor.execute("DELETE FROM _quest_text_target")
    if quest_ids is None:
        cursor.execute("INSERT INTO _quest_text_target(questId) SELECT questId FROM quest")
    else:
        normalized: list[tuple[int]] = []
        seen: set[int] = set()
        for raw in quest_ids:
            try:
                value = int(raw)
            except (TypeError, ValueError):
                continue
            if value in seen:
                continue
            seen.add(value)
            normalized.append((value,))
        if normalized:
            cursor.executemany(
                "INSERT OR IGNORE INTO _quest_text_target(questId) VALUES (?)",
                normalized,
            )
    return "_quest_text_target"


def refresh_quest_text_versions(
    cursor,
    *,
    quest_ids: Iterable[int] | None = None,
) -> dict[str, int]:
    """Refresh task-to-hash provenance and align effective versions to each quest.

    This deliberately updates only the task-scoped table.  Global TextMap rows are
    left untouched here, so shared hashes cannot contaminate other sources.
    """
    ensure_quest_version_provenance_schema(cursor)
    target_table = _prepare_target_table(cursor, quest_ids)
    fragments = _association_sql(cursor, target_table)
    target_filter = "a.questId IN (SELECT questId FROM _quest_text_target)"
    if not fragments:
        cursor.execute(
            f"DELETE FROM {QUEST_TEXT_VERSION_TABLE} "
            "WHERE questId IN (SELECT questId FROM _quest_text_target)"
        )
        return {"association_rows": 0, "aligned_rows": 0, "conflict_hashes": 0, "unresolved_rows": 0}

    cursor.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS _quest_text_association (
            questId INTEGER NOT NULL,
            hash INTEGER NOT NULL,
            relation TEXT NOT NULL,
            PRIMARY KEY (questId, hash, relation)
        )
        """
    )
    cursor.execute("DELETE FROM _quest_text_association")
    cursor.execute(
        "INSERT OR IGNORE INTO _quest_text_association(questId,hash,relation) "
        + " UNION ALL ".join(fragments)
    )

    cursor.execute(
        f"DELETE FROM {QUEST_TEXT_VERSION_TABLE} WHERE questId IN (SELECT questId FROM _quest_text_target) "
        f"AND NOT EXISTS (SELECT 1 FROM _quest_text_association a WHERE a.questId={QUEST_TEXT_VERSION_TABLE}.questId AND a.hash={QUEST_TEXT_VERSION_TABLE}.hash)"
    )

    conflict_rows = cursor.execute(
        f"""
        SELECT a.hash
        FROM _quest_text_association a
        JOIN quest q ON q.questId=a.questId
        WHERE {target_filter} AND q.created_version_id IS NOT NULL
        GROUP BY a.hash
        HAVING COUNT(DISTINCT q.created_version_id)>1
        """
    ).fetchall()
    conflict_hashes = {int(row[0]) for row in conflict_rows}

    has_textmap = _table_exists(cursor, "textMap")
    textmap_join = "LEFT JOIN textMap tm ON tm.hash=a.hash" if has_textmap else ""
    textmap_row_count_expr = "COUNT(tm.hash)" if has_textmap else "0"
    textmap_created_count_expr = "COUNT(tm.created_version_id)" if has_textmap else "0"
    textmap_version_count_expr = "COUNT(DISTINCT tm.created_version_id)" if has_textmap else "0"
    textmap_unresolved_count_expr = (
        "COUNT(tm.hash)-COUNT(tm.created_version_id)" if has_textmap else "0"
    )
    textmap_adjustment_expr = (
        "SUM(CASE WHEN q.created_version_id IS NOT NULL "
        "AND tm.created_version_id IS NOT q.created_version_id THEN 1 ELSE 0 END)"
        if has_textmap
        else "0"
    )
    rows = cursor.execute(
        f"""
        SELECT a.questId, a.hash,
               q.created_version_id,
               group_concat(DISTINCT a.relation) AS relation_types,
               {textmap_row_count_expr} AS textmap_row_count,
               {textmap_created_count_expr} AS textmap_created_version_row_count,
               {textmap_version_count_expr} AS textmap_version_count,
               {textmap_unresolved_count_expr} AS textmap_unresolved_row_count,
               {textmap_adjustment_expr} AS textmap_adjustment_count
        FROM _quest_text_association a
        JOIN quest q ON q.questId=a.questId
        {textmap_join}
        WHERE {target_filter}
        GROUP BY a.questId, a.hash
        """
    ).fetchall()
    upsert_sql = f"""
        INSERT INTO {QUEST_TEXT_VERSION_TABLE}
            (questId, hash, created_version_id, relation_types, alignment_status,
             textmap_row_count, textmap_created_version_row_count, textmap_version_count,
             textmap_unresolved_row_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(questId, hash) DO UPDATE SET
            created_version_id=excluded.created_version_id,
            relation_types=excluded.relation_types,
            alignment_status=excluded.alignment_status,
            textmap_row_count=excluded.textmap_row_count,
            textmap_created_version_row_count=excluded.textmap_created_version_row_count,
            textmap_version_count=excluded.textmap_version_count,
            textmap_unresolved_row_count=excluded.textmap_unresolved_row_count,
            aligned_at=datetime('now')
    """
    payload = []
    unresolved = 0
    aligned = 0
    adjustment_rows = 0
    for (
        quest_id,
        text_hash,
        created_version_id,
        relation_types,
        textmap_row_count,
        textmap_created_version_row_count,
        textmap_version_count,
        textmap_unresolved_row_count,
        textmap_adjustment_count,
    ) in rows:
        if created_version_id is None:
            status = "unresolved_quest_version"
            unresolved += 1
        elif int(textmap_row_count or 0) == 0:
            status = "unresolved_textmap_hash"
            unresolved += 1
        elif int(textmap_unresolved_row_count or 0) > 0:
            status = "unresolved_textmap_version"
            unresolved += 1
        elif int(text_hash) in conflict_hashes:
            status = "shared_conflict_task_scoped"
            aligned += 1
        else:
            status = "aligned_task_scoped"
            aligned += 1
        adjustment_rows += int(textmap_adjustment_count or 0)
        payload.append(
            (
                quest_id,
                text_hash,
                created_version_id,
                relation_types or "",
                status,
                int(textmap_row_count or 0),
                int(textmap_created_version_row_count or 0),
                int(textmap_version_count or 0),
                int(textmap_unresolved_row_count or 0),
            )
        )
    if payload:
        cursor.executemany(upsert_sql, payload)
    return {
        "association_rows": len(rows),
        "aligned_rows": aligned,
        "conflict_hashes": len(conflict_hashes),
        "unresolved_rows": unresolved,
        "textmap_adjustment_rows": adjustment_rows,
    }


def calculate_quest_text_version_audit(
    cursor,
    final_versions: Mapping[int, int | None],
) -> dict[str, object]:
    """Calculate task-text alignment facts without changing persistent tables.

    The query uses the same task/hash relations as ``refresh_quest_text_versions``
    but stores only TEMP tables.  It is therefore safe to run against the
    production database opened read-only before an import.  ``final_versions``
    must already contain the result of the no-write quest candidate calculation;
    no version is inferred or changed here.
    """
    target_table = "_quest_text_audit_target"
    final_table = "_quest_text_audit_final"
    association_table = "_quest_text_audit_association"
    cursor.execute(f"CREATE TEMP TABLE IF NOT EXISTS {target_table}(questId INTEGER PRIMARY KEY)")
    cursor.execute(f"DELETE FROM {target_table}")
    cursor.execute(
        f"CREATE TEMP TABLE IF NOT EXISTS {final_table}(questId INTEGER PRIMARY KEY, final_created_version_id INTEGER)"
    )
    cursor.execute(f"DELETE FROM {final_table}")
    quest_rows = cursor.execute(
        "SELECT questId, created_version_id FROM quest ORDER BY questId"
    ).fetchall()
    target_rows: list[tuple[int]] = []
    final_rows: list[tuple[int, int | None]] = []
    for quest_id, current_id in quest_rows:
        quest_id = int(quest_id)
        target_rows.append((quest_id,))
        final_rows.append(
            (
                quest_id,
                final_versions.get(
                    quest_id,
                    int(current_id) if current_id is not None else None,
                ),
            )
        )
    if target_rows:
        cursor.executemany(f"INSERT INTO {target_table}(questId) VALUES (?)", target_rows)
        cursor.executemany(
            f"INSERT INTO {final_table}(questId, final_created_version_id) VALUES (?,?)",
            final_rows,
        )

    cursor.execute(
        f"CREATE TEMP TABLE IF NOT EXISTS {association_table} ("
        "questId INTEGER NOT NULL, hash INTEGER NOT NULL, relation TEXT NOT NULL, "
        "PRIMARY KEY(questId, hash, relation))"
    )
    cursor.execute(f"DELETE FROM {association_table}")
    fragments = _association_sql(cursor, target_table)
    if fragments:
        cursor.execute(
            f"INSERT OR IGNORE INTO {association_table}(questId,hash,relation) "
            + " UNION ALL ".join(fragments)
        )

    has_textmap = _table_exists(cursor, "textMap")
    textmap_join = "LEFT JOIN textMap tm ON tm.hash=a.hash" if has_textmap else ""
    textmap_count = "COUNT(tm.hash)" if has_textmap else "0"
    textmap_adjustment = (
        "SUM(CASE WHEN f.final_created_version_id IS NOT NULL "
        "AND tm.hash IS NOT NULL "
        "AND tm.created_version_id IS NOT f.final_created_version_id THEN 1 ELSE 0 END)"
        if has_textmap
        else "0"
    )
    unresolved = (
        "COUNT(DISTINCT CASE WHEN f.final_created_version_id IS NULL "
        "OR tm.hash IS NULL OR tm.created_version_id IS NULL THEN a.hash END)"
        if has_textmap
        else "COUNT(DISTINCT a.hash)"
    )
    cursor.execute(
        f"CREATE TEMP TABLE IF NOT EXISTS _quest_text_audit_conflict(hash INTEGER PRIMARY KEY)"
    )
    cursor.execute("DELETE FROM _quest_text_audit_conflict")
    cursor.execute(
        f"""
        INSERT INTO _quest_text_audit_conflict(hash)
        SELECT a.hash
        FROM {association_table} a
        JOIN {final_table} f ON f.questId=a.questId
        WHERE f.final_created_version_id IS NOT NULL
        GROUP BY a.hash
        HAVING COUNT(DISTINCT f.final_created_version_id)>1
        """
    )
    rows = cursor.execute(
        f"""
        SELECT f.questId,
               f.final_created_version_id,
               COUNT(DISTINCT a.hash) AS associated_hash_count,
               COALESCE({textmap_adjustment}, 0) AS text_version_adjustment_count,
               COUNT(DISTINCT CASE WHEN c.hash IS NOT NULL THEN a.hash END) AS shared_conflict_count,
               {unresolved} AS unresolved_count
        FROM {final_table} f
        LEFT JOIN {association_table} a ON a.questId=f.questId
        {textmap_join}
        LEFT JOIN _quest_text_audit_conflict c ON c.hash=a.hash
        GROUP BY f.questId, f.final_created_version_id
        ORDER BY f.questId
        """
    ).fetchall()
    records = [
        {
            "questId": int(quest_id),
            "final_created_version_id": (
                int(final_id) if final_id is not None else None
            ),
            "associated_hash_count": int(associated_count or 0),
            "text_version_adjustment_count": int(adjustment_count or 0),
            "shared_conflict_count": int(conflict_count or 0),
            "unresolved_count": int(unresolved_count or 0),
        }
        for (
            quest_id,
            final_id,
            associated_count,
            adjustment_count,
            conflict_count,
            unresolved_count,
        ) in rows
    ]
    return {
        "records": records,
        "summary": {
            "quest_count": len(records),
            "associated_hash_count": sum(row["associated_hash_count"] for row in records),
            "text_version_adjustment_count": sum(
                row["text_version_adjustment_count"] for row in records
            ),
            "shared_conflict_hash_count": int(
                cursor.execute("SELECT COUNT(*) FROM _quest_text_audit_conflict").fetchone()[0]
                or 0
            ),
            "unresolved_count": sum(row["unresolved_count"] for row in records),
        },
    }


def count_manual_locked_quests(cursor) -> int:
    if not _table_exists(cursor, QUEST_CREATED_VERSION_OVERRIDE_TABLE):
        return 0
    row = cursor.execute(f"SELECT COUNT(*) FROM {QUEST_CREATED_VERSION_OVERRIDE_TABLE}").fetchone()
    return int(row[0] or 0) if row else 0


def persist_manual_created_version_overrides(
    cursor,
    candidates: dict[int, dict[str, int | str | None]],
    *,
    source: str = "automatic_history_candidate",
    reason: str = "current_created_version_differs_from_automatic_candidate",
) -> dict[str, int]:
    """Persist only user-defined candidate/current differences, idempotently."""
    ensure_quest_version_provenance_schema(cursor)
    rows = []
    unresolved = 0
    differences = 0
    for quest_id, item in candidates.items():
        status = item.get("status")
        if status == "unresolved":
            unresolved += 1
            continue
        if status != "manual_difference":
            continue
        candidate = item.get("candidate_created_version_id")
        if candidate is None:
            unresolved += 1
            continue
        differences += 1
        rows.append(
            (
                int(quest_id),
                item.get("current_created_version_id"),
                item.get("current_created_version_id"),
                int(candidate),
                source,
                reason,
            )
        )
    for row in rows:
        quest_id, locked_id, current_id, candidate_id, row_source, row_reason = row
        existing = cursor.execute(
            f"SELECT locked_created_version_id, current_created_version_id, "
            f"candidate_created_version_id, source, reason "
            f"FROM {QUEST_CREATED_VERSION_OVERRIDE_TABLE} WHERE questId=?",
            (quest_id,),
        ).fetchone()
        if existing is not None:
            existing_lock = tuple(existing[:3])
            requested_lock = (locked_id, current_id, candidate_id)
            if existing_lock != requested_lock:
                raise ValueError(
                    f"manual quest override conflict for questId={quest_id}: "
                    f"existing={tuple(existing)!r}, requested={row!r}"
                )
            # The original source path/reason are immutable provenance for the
            # user decision. A later equivalent audit may live at a different
            # path without changing the locked values.
            continue
        cursor.execute(
            f"""
            INSERT INTO {QUEST_CREATED_VERSION_OVERRIDE_TABLE}
                (questId, locked_created_version_id, current_created_version_id,
                 candidate_created_version_id, source, reason)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            row,
        )
    return {
        "candidate_count": len(candidates),
        "difference_count": differences,
        "unresolved_count": unresolved,
        "inserted_or_existing_locked_count": len(rows),
        "locked_count": count_manual_locked_quests(cursor),
    }


def _audit_records(payload: object) -> list[dict]:
    if isinstance(payload, dict):
        for key in ("records", "quests", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                payload = value
                break
    if not isinstance(payload, list):
        raise ValueError("quest version audit must be a JSON list or an object containing records")
    return [row for row in payload if isinstance(row, dict)]


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manual_created_version_audit(
    cursor,
    path: str,
    *,
    strict: bool = True,
) -> dict[str, int | str]:
    """Load a pre-import read-only candidate audit and lock its difference rows.

    The audit is deliberately checked against the database values before any
    importer/history code can update them.  A mismatch is an unsafe stale-audit
    condition and aborts the import rather than guessing which value to keep.
    """
    ensure_quest_version_provenance_schema(cursor)
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    records = _audit_records(payload)
    candidates: dict[int, dict[str, int | str | None]] = {}
    missing_quest_count = 0
    for row in records:
        quest_id_raw = row.get("questId")
        try:
            quest_id = int(quest_id_raw)
        except (TypeError, ValueError):
            if strict:
                raise ValueError(f"invalid questId in audit: {quest_id_raw!r}")
            continue
        current_id = row.get("current_created_version_id")
        candidate_id = row.get("candidate_created_version_id")
        status = str(row.get("status") or "")
        current_id = int(current_id) if current_id is not None else None
        candidate_id = int(candidate_id) if candidate_id is not None else None
        candidates[quest_id] = {
            "questId": quest_id,
            "current_created_version_id": current_id,
            "candidate_created_version_id": candidate_id,
            "status": status,
            "final_created_version_id": row.get("final_created_version_id"),
        }
        actual = cursor.execute(
            "SELECT created_version_id FROM quest WHERE questId=?",
            (quest_id,),
        ).fetchone()
        if actual is None:
            missing_quest_count += 1
            continue
        actual_id = int(actual[0]) if actual[0] is not None else None
        if status == "manual_difference" and actual_id != current_id:
            raise ValueError(
                f"stale quest version audit for questId={quest_id}: "
                f"audit_current={current_id!r}, database_current={actual_id!r}"
            )

    stats = persist_manual_created_version_overrides(
        cursor,
        candidates,
        source=os.path.abspath(path),
        reason="user_rule_candidate_differs_from_current_created_version",
    )
    for quest_id, item in candidates.items():
        if item.get("status") != "manual_difference":
            continue
        cursor.execute(
            f"UPDATE quest SET created_version_id=? WHERE questId=? "
            f"AND EXISTS (SELECT 1 FROM {QUEST_CREATED_VERSION_OVERRIDE_TABLE} o WHERE o.questId=quest.questId)",
            (item.get("current_created_version_id"), quest_id),
        )

    source_path = os.path.abspath(path)
    source_sha256 = _sha256_file(source_path)
    candidate_count = len(records)
    difference_count = sum(1 for row in records if row.get("status") == "manual_difference")
    unresolved_count = sum(1 for row in records if row.get("status") == "unresolved")
    cursor.execute(
        f"""
        INSERT INTO {QUEST_VERSION_OVERRIDE_AUDIT_TABLE}
            (source_path, source_sha256, candidate_count, difference_count,
             unresolved_count, missing_quest_count, status)
        VALUES (?, ?, ?, ?, ?, ?, 'prepared')
        """,
        (
            source_path,
            source_sha256,
            candidate_count,
            difference_count,
            unresolved_count,
            missing_quest_count,
        ),
    )
    return {
        **stats,
        "missing_quest_count": missing_quest_count,
        "audit_path": source_path,
        "audit_sha256": source_sha256,
    }


def mark_manual_created_version_audit_prepared(
    cursor,
    *,
    source_path: str = "test-or-prepared-existing-state",
    candidate_count: int = 0,
    difference_count: int | None = None,
    unresolved_count: int = 0,
) -> None:
    """Record an explicit preparation marker for callers that already imported an audit."""
    ensure_quest_version_provenance_schema(cursor)
    locked_count = count_manual_locked_quests(cursor)
    cursor.execute(
        f"""
        INSERT INTO {QUEST_VERSION_OVERRIDE_AUDIT_TABLE}
            (source_path, source_sha256, candidate_count, difference_count,
             unresolved_count, missing_quest_count, status)
        VALUES (?, NULL, ?, ?, ?, 0, 'prepared')
        """,
        (
            source_path,
            int(candidate_count),
            int(locked_count if difference_count is None else difference_count),
            int(unresolved_count),
        ),
    )


def manual_created_version_audit_is_prepared(cursor) -> bool:
    if not _table_exists(cursor, QUEST_VERSION_OVERRIDE_AUDIT_TABLE):
        return False
    row = cursor.execute(
        f"SELECT 1 FROM {QUEST_VERSION_OVERRIDE_AUDIT_TABLE} "
        "WHERE status='prepared' ORDER BY audit_id DESC LIMIT 1"
    ).fetchone()
    return row is not None


def prepare_manual_created_version_overrides(
    cursor,
    *,
    audit_path: str | None = None,
    required: bool = True,
) -> dict[str, int | str]:
    """Require/load the pre-import audit before any automatic quest backfill.

    A non-empty quest table without either an already prepared audit or the
    explicit ``GTS_MANUAL_QUEST_VERSION_AUDIT`` file is a hard stop.  This keeps
    a direct importer invocation from silently overwriting production-created
    values before the atomic import wrapper has carried the locks over.
    """
    ensure_quest_version_provenance_schema(cursor)
    resolved_path = audit_path or os.environ.get("GTS_MANUAL_QUEST_VERSION_AUDIT", "").strip()
    if resolved_path:
        if not os.path.isfile(resolved_path):
            raise FileNotFoundError(
                f"manual quest version audit does not exist: {resolved_path}"
            )
        return load_manual_created_version_audit(cursor, resolved_path)
    if manual_created_version_audit_is_prepared(cursor):
        return {
            "prepared": 1,
            "locked_count": count_manual_locked_quests(cursor),
            "audit_path": "database:quest_version_override_audit",
        }
    row = cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
    quest_count = int(row[0] or 0) if row else 0
    if quest_count == 0:
        mark_manual_created_version_audit_prepared(
            cursor,
            source_path="empty-quest-table:no-production-values-to-lock",
            candidate_count=0,
            difference_count=0,
            unresolved_count=0,
        )
        return {
            "prepared": 1,
            "locked_count": 0,
            "candidate_count": 0,
            "difference_count": 0,
            "unresolved_count": 0,
            "audit_path": "empty-quest-table:no-production-values-to-lock",
        }
    if required:
        raise RuntimeError(
            "quest history backfill is gated: load the read-only manual quest "
            "version audit via GTS_MANUAL_QUEST_VERSION_AUDIT before importing"
        )
    return {"prepared": 0, "locked_count": count_manual_locked_quests(cursor)}


def is_quest_created_version_locked(cursor, quest_id: int) -> bool:
    if not _table_exists(cursor, QUEST_CREATED_VERSION_OVERRIDE_TABLE):
        return False
    row = cursor.execute(
        f"SELECT 1 FROM {QUEST_CREATED_VERSION_OVERRIDE_TABLE} WHERE questId=? LIMIT 1",
        (int(quest_id),),
    ).fetchone()
    return row is not None

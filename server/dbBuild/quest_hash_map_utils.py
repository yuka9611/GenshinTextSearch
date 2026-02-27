from __future__ import annotations

from import_utils import DEFAULT_BATCH_SIZE, executemany_batched


def ensure_quest_hash_map_schema(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS quest_hash_map (
            questId INTEGER NOT NULL,
            hash INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            PRIMARY KEY (questId, hash, source_type)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS quest_hash_map_hash_index ON quest_hash_map(hash)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS quest_hash_map_questId_index ON quest_hash_map(questId)"
    )


def _normalize_int_ids(raw_ids) -> list[int]:
    normalized: list[int] = []
    seen = set()
    for raw in raw_ids or []:
        try:
            val = int(raw)
        except Exception:
            continue
        if val in seen:
            continue
        seen.add(val)
        normalized.append(val)
    return normalized


def refresh_quest_hash_map_for_quest_ids(
    cursor,
    quest_ids,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    ensure_quest_hash_map_schema(cursor)
    normalized_ids = _normalize_int_ids(quest_ids)
    if not normalized_ids:
        return 0

    cursor.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _qhm_target_quest_id(questId INTEGER PRIMARY KEY)"
    )
    cursor.execute("DELETE FROM _qhm_target_quest_id")
    executemany_batched(
        cursor,
        "INSERT OR IGNORE INTO _qhm_target_quest_id(questId) VALUES (?)",
        ((qid,) for qid in normalized_ids),
        batch_size=batch_size,
    )

    _refresh_quest_hash_map_by_target_table(cursor)
    return len(normalized_ids)


def refresh_quest_hash_map_for_talk_ids(
    cursor,
    talk_ids,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    ensure_quest_hash_map_schema(cursor)
    normalized_ids = _normalize_int_ids(talk_ids)
    if not normalized_ids:
        return 0

    cursor.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _qhm_target_talk_id(talkId INTEGER PRIMARY KEY)"
    )
    cursor.execute("DELETE FROM _qhm_target_talk_id")
    executemany_batched(
        cursor,
        "INSERT OR IGNORE INTO _qhm_target_talk_id(talkId) VALUES (?)",
        ((tid,) for tid in normalized_ids),
        batch_size=batch_size,
    )

    cursor.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _qhm_target_quest_id(questId INTEGER PRIMARY KEY)"
    )
    cursor.execute("DELETE FROM _qhm_target_quest_id")
    cursor.execute(
        """
        INSERT OR IGNORE INTO _qhm_target_quest_id(questId)
        SELECT DISTINCT qt.questId
        FROM questTalk qt
        JOIN _qhm_target_talk_id t ON t.talkId = qt.talkId
        """
    )
    count_row = cursor.execute(
        "SELECT COUNT(*) FROM _qhm_target_quest_id"
    ).fetchone()
    touched = int(count_row[0] or 0) if count_row else 0
    if touched <= 0:
        return 0

    _refresh_quest_hash_map_by_target_table(cursor)
    return touched


def _refresh_quest_hash_map_by_target_table(cursor):
    cursor.execute(
        """
        DELETE FROM quest_hash_map
        WHERE questId IN (SELECT questId FROM _qhm_target_quest_id)
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO quest_hash_map(questId, hash, source_type)
        SELECT q.questId, q.titleTextMapHash, 'title'
        FROM quest q
        JOIN _qhm_target_quest_id t ON t.questId = q.questId
        WHERE q.titleTextMapHash IS NOT NULL
          AND q.titleTextMapHash <> 0
        """
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO quest_hash_map(questId, hash, source_type)
        SELECT DISTINCT qt.questId, d.textHash, 'dialogue'
        FROM questTalk qt
        JOIN _qhm_target_quest_id t ON t.questId = qt.questId
        JOIN dialogue d ON d.talkId = qt.talkId
        WHERE d.textHash IS NOT NULL
          AND d.textHash <> 0
        """
    )


def count_unresolved_quest_versions(cursor) -> tuple[int, int]:
    total_row = cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
    total = int(total_row[0] or 0) if total_row else 0
    unresolved_row = cursor.execute(
        """
        SELECT COUNT(*)
        FROM quest
        WHERE created_version_id IS NULL
           OR updated_version_id IS NULL
        """
    ).fetchone()
    unresolved = int(unresolved_row[0] or 0) if unresolved_row else 0
    return total, unresolved


def unresolved_created_quest_ids(cursor) -> set[int]:
    rows = cursor.execute(
        "SELECT questId FROM quest WHERE created_version_id IS NULL"
    ).fetchall()
    return {int(row[0]) for row in rows}


def refresh_all_quest_hash_map(cursor, *, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    rows = cursor.execute("SELECT questId FROM quest").fetchall()
    if not rows:
        ensure_quest_hash_map_schema(cursor)
        cursor.execute("DELETE FROM quest_hash_map")
        return 0
    quest_ids = [int(row[0]) for row in rows]
    return refresh_quest_hash_map_for_quest_ids(
        cursor,
        quest_ids,
        batch_size=batch_size,
    )

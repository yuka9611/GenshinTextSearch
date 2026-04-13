from __future__ import annotations

from import_utils import DEFAULT_BATCH_SIZE, executemany_batched, normalize_unique_ints

QUEST_HASH_SOURCE_TYPE_TITLE = "title"
QUEST_HASH_SOURCE_TYPE_DESC = "desc"
QUEST_HASH_SOURCE_TYPE_LONG_DESC = "long_desc"
QUEST_HASH_SOURCE_TYPE_DIALOGUE = "dialogue"
TALK_DIALOGUE_LINK_TABLE = "talk_dialogue_link"
QUEST_VERSION_TRACKED_HASH_SOURCE_TYPES = (
    QUEST_HASH_SOURCE_TYPE_TITLE,
    QUEST_HASH_SOURCE_TYPE_DIALOGUE,
)


def _quest_talk_dialogue_join_condition(qt_alias: str = "qt", d_alias: str = "d") -> str:
    return (
        f"(({qt_alias}.coopQuestId IS NULL OR {qt_alias}.coopQuestId = 0) AND {d_alias}.coopQuestId IS NULL) "
        f"OR ({qt_alias}.coopQuestId > 0 AND {d_alias}.coopQuestId = {qt_alias}.coopQuestId)"
    )


def ensure_talk_dialogue_link_schema(cursor):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TALK_DIALOGUE_LINK_TABLE} (
            talkId INTEGER NOT NULL,
            coopQuestId INTEGER NOT NULL DEFAULT 0,
            dialogueId INTEGER NOT NULL,
            PRIMARY KEY (talkId, coopQuestId, dialogueId)
        )
        """
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {TALK_DIALOGUE_LINK_TABLE}_dialogueId_index "
        f"ON {TALK_DIALOGUE_LINK_TABLE}(dialogueId)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {TALK_DIALOGUE_LINK_TABLE}_talkId_coopQuestId_index "
        f"ON {TALK_DIALOGUE_LINK_TABLE}(talkId, coopQuestId)"
    )


def _quest_talk_dialogue_link_join_sql(
    qt_alias: str = "qt",
    tdl_alias: str = "tdl",
    d_alias: str = "d",
) -> str:
    return (
        f"JOIN {TALK_DIALOGUE_LINK_TABLE} {tdl_alias} "
        f"ON {tdl_alias}.talkId = {qt_alias}.talkId "
        f"AND {tdl_alias}.coopQuestId = coalesce({qt_alias}.coopQuestId, 0) "
        f"JOIN dialogue {d_alias} ON {d_alias}.dialogueId = {tdl_alias}.dialogueId "
    )


def ensure_quest_hash_map_schema(cursor):
    ensure_talk_dialogue_link_schema(cursor)
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
def refresh_quest_hash_map_for_quest_ids(
    cursor,
    quest_ids,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    ensure_quest_hash_map_schema(cursor)
    normalized_ids = normalize_unique_ints(quest_ids)
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
    normalized_ids = normalize_unique_ints(talk_ids)
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
        SELECT q.questId, q.titleTextMapHash, ?
        FROM quest q
        JOIN _qhm_target_quest_id t ON t.questId = q.questId
        WHERE q.titleTextMapHash IS NOT NULL
          AND q.titleTextMapHash <> 0
        """,
        (QUEST_HASH_SOURCE_TYPE_TITLE,),
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO quest_hash_map(questId, hash, source_type)
        SELECT q.questId, q.descTextMapHash, ?
        FROM quest q
        JOIN _qhm_target_quest_id t ON t.questId = q.questId
        WHERE q.descTextMapHash IS NOT NULL
          AND q.descTextMapHash <> 0
        """,
        (QUEST_HASH_SOURCE_TYPE_DESC,),
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO quest_hash_map(questId, hash, source_type)
        SELECT q.questId, q.longDescTextMapHash, ?
        FROM quest q
        JOIN _qhm_target_quest_id t ON t.questId = q.questId
        WHERE q.longDescTextMapHash IS NOT NULL
          AND q.longDescTextMapHash <> 0
        """,
        (QUEST_HASH_SOURCE_TYPE_LONG_DESC,),
    )
    # Keep quest updated-version tracking scoped to title/dialogue text only.
    # Quest description and step-title metadata are UI-only and must not affect quest_version.
    cursor.execute(
        """
        INSERT OR IGNORE INTO quest_hash_map(questId, hash, source_type)
        SELECT DISTINCT qt.questId, d.textHash, ?
        FROM questTalk qt
        JOIN _qhm_target_quest_id t ON t.questId = qt.questId
        """
        + _quest_talk_dialogue_link_join_sql("qt", "tdl", "d")
        + """
        WHERE d.textHash IS NOT NULL
          AND d.textHash <> 0
        """,
        (QUEST_HASH_SOURCE_TYPE_DIALOGUE,),
    )


def count_unresolved_quest_versions(cursor) -> tuple[int, int]:
    total_row = cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
    total = int(total_row[0] or 0) if total_row else 0
    unresolved_row = cursor.execute(
        """
        SELECT COUNT(*)
        FROM quest
        WHERE created_version_id IS NULL
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

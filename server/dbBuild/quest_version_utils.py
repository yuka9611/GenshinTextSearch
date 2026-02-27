from versioning import get_or_create_version_id


def _quest_hash_map_available(cursor) -> bool:
    row = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='quest_hash_map' LIMIT 1"
    ).fetchone()
    if row is None:
        return False
    row = cursor.execute("SELECT 1 FROM quest_hash_map LIMIT 1").fetchone()
    return row is not None


def _build_qh_source_sql(
    cursor,
    *,
    target_filter_q: str,
    target_filter_qt: str,
    target_updated_version_id: int | None,
) -> tuple[str, tuple]:
    if _quest_hash_map_available(cursor):
        if target_updated_version_id is not None:
            qh_sql = f"""
                SELECT DISTINCT q.questId AS questId, qhm.hash AS hash
                FROM quest q
                JOIN quest_hash_map qhm ON qhm.questId = q.questId
                WHERE q.updated_version_id=?
                  AND qhm.hash IS NOT NULL
                  AND qhm.hash <> 0
                  {target_filter_q}
            """
            return qh_sql, (target_updated_version_id,)
        qh_sql = f"""
            SELECT DISTINCT q.questId AS questId, qhm.hash AS hash
            FROM quest q
            JOIN quest_hash_map qhm ON qhm.questId = q.questId
            WHERE qhm.hash IS NOT NULL
              AND qhm.hash <> 0
              {target_filter_q}
        """
        return qh_sql, tuple()

    if target_updated_version_id is not None:
        qh_sql = f"""
            SELECT q.questId AS questId, q.titleTextMapHash AS hash
            FROM quest q
            WHERE q.updated_version_id=?
              AND q.titleTextMapHash IS NOT NULL
              AND q.titleTextMapHash <> 0
              {target_filter_q}
            UNION ALL
            SELECT q.questId AS questId, d.textHash AS hash
            FROM quest q
            JOIN questTalk qt ON qt.questId = q.questId
            JOIN dialogue d ON d.talkId = qt.talkId
            WHERE q.updated_version_id=?
              AND d.textHash IS NOT NULL
              {target_filter_q}
        """
        return qh_sql, (target_updated_version_id, target_updated_version_id)

    qh_sql = f"""
        SELECT q.questId AS questId, q.titleTextMapHash AS hash
        FROM quest q
        WHERE q.titleTextMapHash IS NOT NULL
          AND q.titleTextMapHash <> 0
          {target_filter_q}
        UNION ALL
        SELECT qt.questId AS questId, d.textHash AS hash
        FROM questTalk qt
        JOIN dialogue d ON d.talkId = qt.talkId
        WHERE d.textHash IS NOT NULL
          {target_filter_qt}
    """
    return qh_sql, tuple()


def backfill_quest_created_version_from_textmap(
    cursor,
    *,
    quest_updated_version: str | None = None,
    quest_ids: list[int] | set[int] | tuple[int, ...] | None = None,
    overwrite_existing: bool = False,
    overwrite_updated_existing: bool = False,
    with_stats: bool = False,
) -> int | tuple[int, int]:
    cursor.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _quest_inferred_created_version("
        "questId INTEGER PRIMARY KEY, inferred_created_version_id INTEGER)"
    )
    cursor.execute("DELETE FROM _quest_inferred_created_version")
    cursor.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _quest_inferred_updated_version("
        "questId INTEGER PRIMARY KEY, inferred_updated_version_id INTEGER)"
    )
    cursor.execute("DELETE FROM _quest_inferred_updated_version")

    target_filter_q = ""
    target_filter_qt = ""
    if quest_ids is not None:
        normalized_ids = []
        seen = set()
        for raw in quest_ids:
            try:
                qid = int(raw)
            except Exception:
                continue
            if qid in seen:
                continue
            seen.add(qid)
            normalized_ids.append((qid,))
        if not normalized_ids:
            return (0, 0) if with_stats else 0
        cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _target_quest_id(questId INTEGER PRIMARY KEY)")
        cursor.execute("DELETE FROM _target_quest_id")
        cursor.executemany("INSERT OR IGNORE INTO _target_quest_id(questId) VALUES (?)", normalized_ids)
        target_filter_q = " AND q.questId IN (SELECT questId FROM _target_quest_id)"
        target_filter_qt = " AND qt.questId IN (SELECT questId FROM _target_quest_id)"

    target_updated_version_id: int | None = None
    if quest_updated_version:
        target_updated_version_id = get_or_create_version_id(quest_updated_version)
        if target_updated_version_id is None:
            return (0, 0) if with_stats else 0

    qh_sql, qh_params = _build_qh_source_sql(
        cursor,
        target_filter_q=target_filter_q,
        target_filter_qt=target_filter_qt,
        target_updated_version_id=target_updated_version_id,
    )

    cursor.execute(
        f"""
        WITH qh AS (
            {qh_sql}
        ),
        candidates AS (
            SELECT
                qh.questId AS questId,
                tm.created_version_id AS created_version_id,
                CASE
                    WHEN vd.version_tag IS NOT NULL AND instr(vd.version_tag, '.') > 0
                    THEN CAST(substr(vd.version_tag, 1, instr(vd.version_tag, '.') - 1) AS INTEGER)
                    ELSE NULL
                END AS major_v,
                CASE
                    WHEN vd.version_tag IS NOT NULL AND instr(vd.version_tag, '.') > 0
                    THEN CAST(substr(vd.version_tag, instr(vd.version_tag, '.') + 1) AS INTEGER)
                    ELSE NULL
                END AS minor_v
            FROM qh
            JOIN textMap tm ON tm.hash = qh.hash
            LEFT JOIN version_dim vd ON vd.id = tm.created_version_id
            WHERE tm.created_version_id IS NOT NULL
              AND qh.hash <> 0
        ),
        ranked AS (
            SELECT
                questId,
                created_version_id,
                ROW_NUMBER() OVER (
                    PARTITION BY questId
                    ORDER BY
                        CASE WHEN major_v IS NULL OR minor_v IS NULL THEN 1 ELSE 0 END ASC,
                        major_v ASC,
                        minor_v ASC,
                        created_version_id ASC
                ) AS rn
            FROM candidates
        )
        INSERT OR REPLACE INTO _quest_inferred_created_version(questId, inferred_created_version_id)
        SELECT questId, created_version_id
        FROM ranked
        WHERE rn = 1
        """,
        qh_params,
    )

    overwrite_filter = "" if overwrite_existing else " AND created_version_id IS NULL"
    cursor.execute(
        f"""
        UPDATE quest
        SET created_version_id = (
            SELECT t.inferred_created_version_id
            FROM _quest_inferred_created_version t
            WHERE t.questId = quest.questId
        )
        WHERE questId IN (SELECT questId FROM _quest_inferred_created_version)
          {overwrite_filter}
          AND COALESCE(created_version_id, -1) <> COALESCE(
            (
                SELECT t.inferred_created_version_id
                FROM _quest_inferred_created_version t
                WHERE t.questId = quest.questId
            ),
            -1
          )
        """
    )
    created_backfilled = cursor.rowcount

    cursor.execute(
        f"""
        WITH qh AS (
            {qh_sql}
        ),
        candidates AS (
            SELECT
                qh.questId AS questId,
                tm.updated_version_id AS updated_version_id,
                CASE
                    WHEN vd.version_tag IS NOT NULL AND instr(vd.version_tag, '.') > 0
                    THEN CAST(substr(vd.version_tag, 1, instr(vd.version_tag, '.') - 1) AS INTEGER)
                    ELSE NULL
                END AS major_v,
                CASE
                    WHEN vd.version_tag IS NOT NULL AND instr(vd.version_tag, '.') > 0
                    THEN CAST(substr(vd.version_tag, instr(vd.version_tag, '.') + 1) AS INTEGER)
                    ELSE NULL
                END AS minor_v
            FROM qh
            JOIN textMap tm ON tm.hash = qh.hash
            LEFT JOIN version_dim vd ON vd.id = tm.updated_version_id
            WHERE tm.updated_version_id IS NOT NULL
              AND qh.hash <> 0
        ),
        ranked AS (
            SELECT
                questId,
                updated_version_id,
                ROW_NUMBER() OVER (
                    PARTITION BY questId
                    ORDER BY
                        CASE WHEN major_v IS NULL OR minor_v IS NULL THEN 1 ELSE 0 END ASC,
                        major_v DESC,
                        minor_v DESC,
                        updated_version_id DESC
                ) AS rn
            FROM candidates
        )
        INSERT OR REPLACE INTO _quest_inferred_updated_version(questId, inferred_updated_version_id)
        SELECT questId, updated_version_id
        FROM ranked
        WHERE rn = 1
        """,
        qh_params,
    )

    if overwrite_updated_existing:
        cursor.execute(
            """
            UPDATE quest
            SET updated_version_id = (
                SELECT t.inferred_updated_version_id
                FROM _quest_inferred_updated_version t
                WHERE t.questId = quest.questId
            )
            WHERE questId IN (SELECT questId FROM _quest_inferred_updated_version)
              AND (
                updated_version_id IS NULL
                OR updated_version_id < (
                    SELECT t.inferred_updated_version_id
                    FROM _quest_inferred_updated_version t
                    WHERE t.questId = quest.questId
                )
              )
              AND COALESCE(updated_version_id, -1) <> COALESCE(
                (
                    SELECT t.inferred_updated_version_id
                    FROM _quest_inferred_updated_version t
                    WHERE t.questId = quest.questId
                ),
                -1
              )
            """
        )
    else:
        cursor.execute(
            """
            UPDATE quest
            SET updated_version_id = (
                SELECT t.inferred_updated_version_id
                FROM _quest_inferred_updated_version t
                WHERE t.questId = quest.questId
            )
            WHERE questId IN (SELECT questId FROM _quest_inferred_updated_version)
              AND updated_version_id IS NULL
              AND COALESCE(updated_version_id, -1) <> COALESCE(
                (
                    SELECT t.inferred_updated_version_id
                    FROM _quest_inferred_updated_version t
                    WHERE t.questId = quest.questId
                ),
                -1
              )
            """
        )
    updated_backfilled = cursor.rowcount
    return (created_backfilled, updated_backfilled) if with_stats else created_backfilled

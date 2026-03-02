from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from DBConfig import conn


# 版本控制核心逻辑

# 版本比较相关函数
def _compare_version_ids(existing_id: int | None, fallback_id: int | None, is_created: bool) -> int | None:
    """
    Compare version IDs and return the appropriate version ID based on whether it's a created or updated version.
    For created_version_id: use the older version
    For updated_version_id: use the newer version
    """
    if existing_id is None:
        return fallback_id
    if fallback_id is None:
        return existing_id
    if is_created:
        return fallback_id if fallback_id < existing_id else existing_id
    else:
        return fallback_id if fallback_id > existing_id else existing_id


def should_update_version(existing_id: int | None, new_id: int | None, is_created: bool) -> bool:
    """
    检查版本是否需要更新

    Args:
        existing_id: 现有版本ID
        new_id: 新版本ID
        is_created: 是否为created_version_id

    Returns:
        bool: 是否需要更新版本
    """
    if existing_id is None:
        return new_id is not None
    if new_id is None:
        return False
    # 使用_compare_version_ids函数来确定是否需要更新
    # 如果比较结果与现有版本不同，则需要更新
    return _compare_version_ids(existing_id, new_id, is_created) != existing_id


# SQL生成函数
def build_versioned_upsert_sql(
    *,
    table: str,
    insert_columns: list[str],
    conflict_columns: list[str],
    update_columns: list[str],
    compare_columns: list[str] | None = None,
    content_column: str = "content",
) -> str:
    """
    Build a common UPSERT SQL with created_version_id/updated_version_id guard semantics.
    `updated_version_id` is bumped only when content changes; otherwise existing value is kept.
    """
    compare_cols = compare_columns or update_columns
    placeholders = ",".join(["?"] * len(insert_columns))
    set_parts = [f"{col}=excluded.{col}" for col in update_columns]
    set_parts.append(
        "created_version_id=CASE "
        f"WHEN excluded.created_version_id IS NULL THEN {table}.created_version_id "
        f"WHEN {table}.created_version_id IS NULL THEN excluded.created_version_id "
        f"WHEN excluded.created_version_id < {table}.created_version_id THEN excluded.created_version_id "
        f"ELSE {table}.created_version_id "
        "END"
    )
    
    where_parts = [f"NOT ({table}.{col} IS excluded.{col})" for col in compare_cols]
    where_parts.append(f"{table}.created_version_id IS NULL")
    
    if table != 'quest':
        # For non-quest tables, include updated_version_id handling
        set_parts.append(
            "updated_version_id=CASE "
            f"WHEN COALESCE({table}.{content_column}, '') <> COALESCE(excluded.{content_column}, '') "
            "THEN CASE "
            f"WHEN excluded.updated_version_id IS NULL THEN COALESCE({table}.updated_version_id, excluded.updated_version_id) "
            f"WHEN {table}.updated_version_id IS NULL THEN excluded.updated_version_id "
            f"WHEN excluded.updated_version_id > {table}.updated_version_id THEN excluded.updated_version_id "
            f"ELSE {table}.updated_version_id "
            "END "
            f"ELSE COALESCE({table}.updated_version_id, excluded.updated_version_id) "
            "END"
        )
        where_parts.append(f"{table}.updated_version_id IS NULL")

    set_sql = ", ".join(set_parts)
    where_sql = " OR ".join(where_parts)
    return (
        f"INSERT INTO {table}({','.join(insert_columns)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT({','.join(conflict_columns)}) DO UPDATE SET "
        f"{set_sql} "
        f"WHERE {where_sql}"
    )


def build_guarded_created_updated_sql(table_name: str, key_predicate_sql: str) -> str:
    """
    Build SQL for updating created_version_id and updated_version_id with version tag comparison.
    """
    if table_name == 'quest':
        # For quest table, only update created_version_id since updated_version_id is in quest_version
        return (
            f"UPDATE {table_name} SET "
            "created_version_id=CASE "
            "WHEN created_version_id IS NULL OR "
            "(SELECT CASE WHEN version_tag IS NULL THEN 0 ELSE "
            "CAST(SUBSTR(version_tag, 1, INSTR(version_tag, '.') - 1) AS INTEGER) * 1000 + "
            "CAST(SUBSTR(version_tag, INSTR(version_tag, '.') + 1) AS INTEGER) "
            "END FROM version_dim WHERE id = created_version_id) > "
            "(SELECT CASE WHEN version_tag IS NULL THEN 0 ELSE "
            "CAST(SUBSTR(version_tag, 1, INSTR(version_tag, '.') - 1) AS INTEGER) * 1000 + "
            "CAST(SUBSTR(version_tag, INSTR(version_tag, '.') + 1) AS INTEGER) "
            "END FROM version_dim WHERE id = ?) THEN ? "
            "ELSE created_version_id END "
            f"WHERE {key_predicate_sql} "
        )
    return (
        f"UPDATE {table_name} SET "
        "created_version_id=CASE "
        "WHEN created_version_id IS NULL OR "
        "(SELECT CASE WHEN version_tag IS NULL THEN 0 ELSE "
        "CAST(SUBSTR(version_tag, 1, INSTR(version_tag, '.') - 1) AS INTEGER) * 1000 + "
        "CAST(SUBSTR(version_tag, INSTR(version_tag, '.') + 1) AS INTEGER) "
        "END FROM version_dim WHERE id = created_version_id) > "
        "(SELECT CASE WHEN version_tag IS NULL THEN 0 ELSE "
        "CAST(SUBSTR(version_tag, 1, INSTR(version_tag, '.') - 1) AS INTEGER) * 1000 + "
        "CAST(SUBSTR(version_tag, INSTR(version_tag, '.') + 1) AS INTEGER) "
        "END FROM version_dim WHERE id = ?) THEN ? "
        "ELSE created_version_id END, "
        "updated_version_id=? "
        f"WHERE {key_predicate_sql} "
        "AND (updated_version_id IS NULL OR "
        "(SELECT CASE WHEN version_tag IS NULL THEN 0 ELSE "
        "CAST(SUBSTR(version_tag, 1, INSTR(version_tag, '.') - 1) AS INTEGER) * 1000 + "
        "CAST(SUBSTR(version_tag, INSTR(version_tag, '.') + 1) AS INTEGER) "
        "END FROM version_dim WHERE id = updated_version_id) <= "
        "(SELECT CASE WHEN version_tag IS NULL THEN 0 ELSE "
        "CAST(SUBSTR(version_tag, 1, INSTR(version_tag, '.') - 1) AS INTEGER) * 1000 + "
        "CAST(SUBSTR(version_tag, INSTR(version_tag, '.') + 1) AS INTEGER) "
        "END FROM version_dim WHERE id = ?))"
    )


# 文本比较函数
def readable_text_changed(old_text: str | None, new_text: str | None) -> bool:
    """
    Check if readable text has changed.
    """
    return (old_text or "") != (new_text or "")


_MISSING = object()

def _analyze_same_key_changes(
    old_content_by_key: Mapping[str, str | None],
    new_key_content_pairs: Sequence[tuple[str, str | None]],
) -> tuple[set[str], list[str | None]]:
    """
    Analyze changes for the same key.
    """
    exact_unchanged_keys: set[str] = set()
    changed_same_key_old_contents: list[str | None] = []
    for key, new_content in new_key_content_pairs:
        old_content = old_content_by_key.get(key, _MISSING)
        if old_content is _MISSING:
            continue
        if old_content == new_content:
            exact_unchanged_keys.add(key)
        else:
            changed_same_key_old_contents.append(old_content)  # type: ignore
    return exact_unchanged_keys, changed_same_key_old_contents


def _consume_counter(counter: Counter, value):
    """
    Consume a value from the counter.
    """
    count = counter.get(value, 0)
    if count <= 0:
        return False
    if count == 1:
        del counter[value]
    else:
        counter[value] = count - 1
    return True


def subtitle_text_changed_keys(old_rows: Mapping[str, str | None], new_rows: Mapping[str, str | None]) -> list[str]:
    """
    Return subtitle keys whose TEXT changed.
    Key-only movement caused by start/end timestamp changes is treated as unchanged.
    """
    old_content_by_key = dict(old_rows)
    new_pairs = list(new_rows.items())
    exact_unchanged_keys, changed_same_key_old_contents = _analyze_same_key_changes(
        old_content_by_key,
        new_pairs,
    )

    reusable_old_content = Counter()
    for key, old_content in old_content_by_key.items():
        if key in exact_unchanged_keys:
            continue
        reusable_old_content[old_content] += 1
    for old_content in changed_same_key_old_contents:
        _consume_counter(reusable_old_content, old_content)

    changed_keys: list[str] = []
    for key, new_content in new_pairs:
        old_content = old_content_by_key.get(key, _MISSING)
        if old_content is not _MISSING:
            if old_content != new_content:
                changed_keys.append(key)
            continue

        if _consume_counter(reusable_old_content, new_content):
            # Same content moved to another subtitleKey (timestamp-only movement).
            continue
        changed_keys.append(key)

    return changed_keys


# 版本分配函数
def assign_readable_versions_by_text(
    existing_row: tuple[str | None, int | None, int | None] | None,
    new_content: str,
    fallback_version_id: int | None,
) -> tuple[int | None, int | None]:
    """
    Assign (created_version_id, updated_version_id) by text comparison.
    If content is unchanged, keep existing versions; otherwise use fallback version for updated.
    """
    if existing_row is None:
        return fallback_version_id, fallback_version_id

    old_content, old_created, old_updated = existing_row
    if not readable_text_changed(old_content, new_content):
        return old_created, old_updated

    # For created_version_id: use the older version
    new_created_id = _compare_version_ids(old_created, fallback_version_id, is_created=True)
    # For updated_version_id: always use the fallback version when content changes
    new_updated_id = fallback_version_id

    return new_created_id, new_updated_id


def assign_subtitle_versions_by_text(
    existing_rows: list[tuple[str, str | None, int | None, int | None]],
    new_rows: list[tuple[str, float, float, str]],
    fallback_version_id: int | None,
) -> list[tuple[str, float, float, str, int | None, int | None]]:
    """
    Assign (created_version_id, updated_version_id) for new subtitle rows.
    Rows judged as timestamp-only movement keep old version ids.
    """
    old_by_key: dict[str, tuple[str | None, int | None, int | None]] = {}
    old_content_by_key: dict[str, str | None] = {}
    for key, old_content, created_id, updated_id in existing_rows:
        old_by_key[key] = (old_content, created_id, updated_id)
        old_content_by_key[key] = old_content

    new_pairs = [(key, text_content) for key, _s, _e, text_content in new_rows]
    new_rows_dict = {key: text for key, text in new_pairs}
    text_changed_keys = set(subtitle_text_changed_keys(old_content_by_key, new_rows_dict))
    exact_unchanged_keys, changed_same_key_old_contents = _analyze_same_key_changes(
        old_content_by_key,
        new_pairs,
    )

    reusable_versions: dict[str | None, list[tuple[int | None, int | None]]] = defaultdict(list)
    for key, old_content, created_id, updated_id in existing_rows:
        if key in exact_unchanged_keys:
            continue
        reusable_versions[old_content].append((created_id, updated_id))
    for old_content in changed_same_key_old_contents:
        candidates = reusable_versions.get(old_content)
        if candidates:
            candidates.pop()
            if not candidates:
                del reusable_versions[old_content]

    assigned_rows: list[tuple[str, float, float, str, int | None, int | None]] = []
    for key, start_time, end_time, text_content in new_rows:
        old_entry = old_by_key.get(key)
        if old_entry is not None:
            old_content, created_id, updated_id = old_entry
            if key not in text_changed_keys and old_content == text_content:
                assigned_rows.append((key, start_time, end_time, text_content, created_id, updated_id))
                continue
            # Use the common version comparison function
            new_created_id = _compare_version_ids(created_id, fallback_version_id, is_created=True)
            new_updated_id = _compare_version_ids(updated_id, fallback_version_id, is_created=False)
            assigned_rows.append(
                (
                    key,
                    start_time,
                    end_time,
                    text_content,
                    new_created_id,
                    new_updated_id,
                )
            )
            continue

        if key not in text_changed_keys:
            candidates = reusable_versions.get(text_content)
            if candidates:
                created_id, updated_id = candidates.pop()
                if not candidates:
                    del reusable_versions[text_content]
                assigned_rows.append((key, start_time, end_time, text_content, created_id, updated_id))
                continue

        assigned_rows.append(
            (
                key,
                start_time,
                end_time,
                text_content,
                fallback_version_id,
                fallback_version_id,
            )
        )

    return assigned_rows


# Quest相关函数
def _quest_hash_map_available(cursor) -> bool:
    """
    Check if quest_hash_map table is available.
    """
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
    """
    Build quest hash source SQL based on availability of quest_hash_map table.
    """
    if _quest_hash_map_available(cursor):
        if target_updated_version_id is not None:
            qh_sql = f"""
                SELECT DISTINCT q.questId AS questId, qhm.hash AS hash
                FROM quest q
                JOIN quest_hash_map qhm ON qhm.questId = q.questId
                JOIN quest_version qv ON qv.questId = q.questId
                WHERE qv.updated_version_id=?
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
            JOIN quest_version qv ON qv.questId = q.questId
            WHERE qv.updated_version_id=?
              AND q.titleTextMapHash IS NOT NULL
              AND q.titleTextMapHash <> 0
              {target_filter_q}
            UNION ALL
            SELECT q.questId AS questId, d.textHash AS hash
            FROM quest q
            JOIN questTalk qt ON qt.questId = q.questId
            JOIN dialogue d ON d.talkId = qt.talkId
            JOIN quest_version qv ON qv.questId = q.questId
            WHERE qv.updated_version_id=?
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


def _build_version_ranking_sql(base_sql: str, version_column: str, is_created: bool) -> str:
    """
    Build SQL for ranking versions based on version tag and version ID.
    """
    order_direction = "ASC" if is_created else "DESC"
    return f"""
    WITH qh AS (
        {base_sql}
    ),
    candidates AS (
        SELECT
            qh.questId AS questId,
            tm.{version_column} AS {version_column},
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
        LEFT JOIN version_dim vd ON vd.id = tm.{version_column}
        WHERE tm.{version_column} IS NOT NULL
          AND qh.hash <> 0
    ),
    ranked AS (
        SELECT
            questId,
            {version_column},
            ROW_NUMBER() OVER (
                PARTITION BY questId
                ORDER BY
                    CASE WHEN major_v IS NULL OR minor_v IS NULL THEN 1 ELSE 0 END ASC,
                    major_v {order_direction},
                    minor_v {order_direction},
                    {version_column} {order_direction}
            ) AS rn
        FROM candidates
    )
    """


def _build_update_sql(table: str, column: str, temp_table: str, overwrite_existing: bool, is_created: bool) -> str:
    """
    Build SQL for updating version IDs in the quest table.
    """
    if is_created:
        overwrite_filter = "" if overwrite_existing else " AND created_version_id IS NULL"
    else:
        # 对于updated_version_id，我们不再更新quest表，而是在backfill_quest_created_version_from_textmap函数中处理quest_version表
        # 这里只处理created_version_id的更新
        overwrite_filter = ""

    return f"""
    UPDATE quest
    SET {column} = (
        SELECT t.inferred_{column}
        FROM {temp_table} t
        WHERE t.questId = quest.questId
    )
    WHERE questId IN (SELECT questId FROM {temp_table})
      {overwrite_filter}
      AND COALESCE({column}, -1) <> COALESCE(
        (
            SELECT t.inferred_{column}
            FROM {temp_table} t
            WHERE t.questId = quest.questId
        ),
        -1
      )
    """


def backfill_quest_created_version_from_textmap(
    cursor,
    *,
    quest_updated_version: str | None = None,
    quest_ids: list[int] | set[int] | tuple[int, ...] | None = None,
    overwrite_existing: bool = False,
    overwrite_updated_existing: bool = False,
    with_stats: bool = False,
) -> int | tuple[int, int]:
    """
    Backfill quest created_version_id from textMap.
    通过CHS的TextMap和git对比确定创建版本，所有语言共用一个创建版本
    """
    cursor.execute(
        "CREATE TEMP TABLE IF NOT EXISTS _quest_inferred_created_version("
        "questId INTEGER PRIMARY KEY, inferred_created_version_id INTEGER)"
    )
    cursor.execute("DELETE FROM _quest_inferred_created_version")

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

    # 构建CHS语言的quest hash source SQL（用于确定创建版本）
    qh_sql, qh_params = _build_qh_source_sql(
        cursor,
        target_filter_q=target_filter_q,
        target_filter_qt=target_filter_qt,
        target_updated_version_id=target_updated_version_id,
    )

    # 构建CHS语言的创建版本查询SQL
    created_sql = f"""
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
          AND tm.lang = (SELECT id FROM langCode WHERE codeName = 'TextMapCHS.json' LIMIT 1) -- 只使用CHS语言确定创建版本
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
    """

    # 插入推断的创建版本
    cursor.execute(
        f"""
        {created_sql}
        INSERT OR REPLACE INTO _quest_inferred_created_version(questId, inferred_created_version_id)
        SELECT questId, created_version_id
        FROM ranked
        WHERE rn = 1
        """,
        qh_params,
    )

    # 更新quest表的创建版本（共通版本）
    update_created_sql = """
    UPDATE quest
    SET created_version_id = (
        SELECT t.inferred_created_version_id
        FROM _quest_inferred_created_version t
        WHERE t.questId = quest.questId
    )
    WHERE questId IN (SELECT questId FROM _quest_inferred_created_version)
      AND (created_version_id IS NULL OR created_version_id > (
          SELECT t.inferred_created_version_id
          FROM _quest_inferred_created_version t
          WHERE t.questId = quest.questId
      ))
    """
    cursor.execute(update_created_sql)
    created_backfilled = cursor.rowcount

    # 获取所有可用的语言
    lang_rows = cursor.execute("SELECT id FROM langCode WHERE imported=1").fetchall()
    languages = [row[0] for row in lang_rows]

    # 为每种语言单独计算更新版本并存储在quest_version表中
    updated_backfilled = 0
    for lang in languages:
        # 构建该语言的更新版本查询SQL
        updated_sql = f"""
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
              AND tm.lang = ? -- 只使用当前语言确定更新版本
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
        INSERT INTO quest_version(questId, lang, updated_version_id)
        SELECT questId, ?, updated_version_id
        FROM ranked
        WHERE rn = 1
        ON CONFLICT(questId, lang) DO UPDATE SET
        updated_version_id=CASE
        WHEN excluded.updated_version_id IS NULL THEN quest_version.updated_version_id
        WHEN quest_version.updated_version_id IS NULL THEN excluded.updated_version_id
        WHEN excluded.updated_version_id > quest_version.updated_version_id THEN excluded.updated_version_id
        ELSE quest_version.updated_version_id
        END
        """
        cursor.execute(updated_sql, (lang, lang))
        updated_backfilled += cursor.rowcount

    return (created_backfilled, updated_backfilled) if with_stats else created_backfilled


# 从 versioning.py 导入核心功能
from versioning import (
    normalize_version_label,
    _extract_version_tag,
    _normalize_version_catalog_tables,
    _table_columns,
    _table_exists,
    _ensure_column,
    _ensure_index,
    get_or_create_version_id,
    _backfill_version_dim_and_ids,
    _ensure_updated_version_autofill_rules,
    _ensure_version_catalog_table,
    _ensure_quest_hash_map_table,
    rebuild_version_catalog,
    ensure_version_schema,
    resolve_version_label,
    get_current_version,
    set_current_version,
    get_meta,
    set_meta,
)

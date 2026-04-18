from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
import os
import sys
from DBConfig import conn
from DBInit import ensure_base_schema
from import_utils import reset_temp_table
from quest_hash_map_utils import _quest_talk_dialogue_join_condition
from versioning import (
    VERSION_DIM_TABLE,
    ensure_version_schema as _ensure_version_schema_impl,
    get_current_version as _get_current_version_impl,
    get_or_create_version_id as _get_or_create_version_id_impl,
    rebuild_version_catalog as _rebuild_version_catalog_impl,
    set_current_version as _set_current_version_impl,
)

try:
    from quest_text_filters import (
        build_quest_version_dialogue_not_excluded_sql,
        get_quest_text_filter_lang_id,
    )
except ImportError:
    SERVER_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))
    if SERVER_DIR not in sys.path:
        sys.path.insert(0, SERVER_DIR)
    from quest_text_filters import (  # type: ignore
        build_quest_version_dialogue_not_excluded_sql,
        get_quest_text_filter_lang_id,
    )


_VERSION_SORT_KEY_MAX = 2147483647
_VERSION_SORT_KEY_MIN = -1
_version_sort_key_cache: dict[int, int | None] = {}
_version_sort_key_cache_primed = False


def _clear_version_sort_key_cache():
    global _version_sort_key_cache_primed
    _version_sort_key_cache.clear()
    _version_sort_key_cache_primed = False


def _prime_version_sort_key_cache():
    global _version_sort_key_cache_primed
    if _version_sort_key_cache_primed:
        return
    cur = conn.cursor()
    try:
        try:
            rows = cur.execute(
                f"SELECT id, version_sort_key FROM {VERSION_DIM_TABLE}"
            ).fetchall()
        except Exception:
            rows = []
    finally:
        cur.close()
    _version_sort_key_cache.update(
        {int(row[0]): (int(row[1]) if row[1] is not None else None) for row in rows}
    )
    _version_sort_key_cache_primed = True


def _remember_version_sort_key(version_id: int | None):
    if version_id is None:
        return
    vid = int(version_id)
    cur = conn.cursor()
    try:
        try:
            row = cur.execute(
                f"SELECT version_sort_key FROM {VERSION_DIM_TABLE} WHERE id=? LIMIT 1",
                (vid,),
            ).fetchone()
        except Exception:
            row = None
    finally:
        cur.close()
    _version_sort_key_cache[vid] = int(row[0]) if row and row[0] is not None else None


def _get_version_sort_key(version_id: int | None) -> int | None:
    if version_id is None:
        return None
    vid = int(version_id)
    if vid in _version_sort_key_cache:
        return _version_sort_key_cache[vid]
    if not _version_sort_key_cache_primed:
        _prime_version_sort_key_cache()
        if vid in _version_sort_key_cache:
            return _version_sort_key_cache[vid]
    _remember_version_sort_key(vid)
    return _version_sort_key_cache.get(vid)


def _version_sort_key_sql(id_expr: str, null_sentinel: int) -> str:
    return (
        f"COALESCE((SELECT version_sort_key FROM {VERSION_DIM_TABLE} "
        f"WHERE id = {id_expr}), {null_sentinel})"
    )


def _version_precedes_sql(candidate_expr: str, current_expr: str) -> str:
    candidate_key = _version_sort_key_sql(candidate_expr, _VERSION_SORT_KEY_MAX)
    current_key = _version_sort_key_sql(current_expr, _VERSION_SORT_KEY_MAX)
    return f"({candidate_key} < {current_key})"


def _version_succeeds_sql(candidate_expr: str, current_expr: str) -> str:
    candidate_key = _version_sort_key_sql(candidate_expr, _VERSION_SORT_KEY_MIN)
    current_key = _version_sort_key_sql(current_expr, _VERSION_SORT_KEY_MIN)
    return f"({candidate_key} > {current_key})"


def _build_version_preference_case_sql(
    *,
    existing_expr: str,
    candidate_expr: str,
    is_created: bool,
) -> str:
    better_sql = (
        _version_precedes_sql(candidate_expr, existing_expr)
        if is_created
        else _version_succeeds_sql(candidate_expr, existing_expr)
    )
    return (
        "CASE "
        f"WHEN {candidate_expr} IS NULL THEN {existing_expr} "
        f"WHEN {existing_expr} IS NULL THEN {candidate_expr} "
        f"WHEN {better_sql} THEN {candidate_expr} "
        f"ELSE {existing_expr} "
        "END"
    )


def _build_version_order_by_sql(id_expr: str, is_created: bool) -> str:
    direction = "ASC" if is_created else "DESC"
    sentinel = _VERSION_SORT_KEY_MAX if is_created else _VERSION_SORT_KEY_MIN
    return f"{_version_sort_key_sql(id_expr, sentinel)} {direction}"


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
    existing_sort_key = _get_version_sort_key(existing_id)
    fallback_sort_key = _get_version_sort_key(fallback_id)

    if existing_sort_key is None and fallback_sort_key is not None:
        return fallback_id
    if existing_sort_key is not None and fallback_sort_key is None:
        return existing_id

    if existing_sort_key is None and fallback_sort_key is None:
        return existing_id

    if fallback_sort_key == existing_sort_key:
        return existing_id

    if is_created:
        return fallback_id if fallback_sort_key < existing_sort_key else existing_id  # type: ignore
    return fallback_id if fallback_sort_key > existing_sort_key else existing_id  # type: ignore


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
    return _compare_version_ids(existing_id, new_id, is_created) != existing_id


def merge_history_versions(
    existing_created_id: int | None,
    existing_updated_id: int | None,
    candidate_version_id: int | None,
    *,
    content_changed: bool,
) -> tuple[int | None, int | None]:
    """
    Merge one history commit into created/updated version ids.
    `created` keeps the earliest version; `updated` keeps the latest
    content-changing version, while also repairing missing updated values.
    """
    created_id = existing_created_id
    updated_id = existing_updated_id

    if should_update_version(existing_created_id, candidate_version_id, is_created=True):
        created_id = candidate_version_id

    if content_changed or existing_updated_id is None:
        if should_update_version(existing_updated_id, candidate_version_id, is_created=False):
            updated_id = candidate_version_id

    return created_id, updated_id


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
        "created_version_id="
        + _build_version_preference_case_sql(
            existing_expr=f"{table}.created_version_id",
            candidate_expr="excluded.created_version_id",
            is_created=True,
        )
    )

    where_parts = [f"NOT ({table}.{col} IS excluded.{col})" for col in compare_cols]
    where_parts.append(f"{table}.created_version_id IS NULL")

    if table != 'quest':
        # For non-quest tables, include updated_version_id handling
        set_parts.append(
            "updated_version_id=CASE "
            f"WHEN COALESCE({table}.{content_column}, '') <> COALESCE(excluded.{content_column}, '') "
            "THEN "
            + _build_version_preference_case_sql(
                existing_expr=f"{table}.updated_version_id",
                candidate_expr="excluded.updated_version_id",
                is_created=False,
            )
            + " "
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


def build_guarded_created_updated_sql(table_name: str, key_columns: Sequence[str]) -> str:
    """
    Build SQL for updating created_version_id and updated_version_id with version tag comparison.
    """
    if not key_columns:
        raise ValueError("key_columns must not be empty")
    key_predicate_sql = " AND ".join(
        f"{column}=?{index}"
        for index, column in enumerate(key_columns, start=4 if table_name != "quest" else 3)
    )
    if table_name == 'quest':
        # For quest table, only update created_version_id since updated_version_id is in quest_version
        return (
            f"UPDATE {table_name} SET "
            "created_version_id=CASE "
            "WHEN created_version_id IS NULL OR "
            f"{_version_precedes_sql('?1', 'created_version_id')} THEN ?2 "
            "ELSE created_version_id END "
            f"WHERE {key_predicate_sql} "
        )
    return (
        f"UPDATE {table_name} SET "
        "created_version_id=CASE "
        "WHEN created_version_id IS NULL OR "
        f"{_version_precedes_sql('?1', 'created_version_id')} THEN ?2 "
        "ELSE created_version_id END, "
        "updated_version_id=?3 "
        f"WHERE {key_predicate_sql} "
        "AND (updated_version_id IS NULL OR "
        f"{_version_succeeds_sql('?1', 'updated_version_id')})"
    )


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
                SELECT DISTINCT q.questId AS questId, qhm.hash AS hash, qhm.source_type AS source_type
                FROM quest q
                JOIN quest_hash_map qhm ON qhm.questId = q.questId
                JOIN quest_version qv ON qv.questId = q.questId
                WHERE qv.updated_version_id=?
                  AND qhm.source_type IN ('title', 'dialogue')
                  AND qhm.hash IS NOT NULL
                  AND qhm.hash <> 0
                  {target_filter_q}
            """
            return qh_sql, (target_updated_version_id,)
        qh_sql = f"""
            SELECT DISTINCT q.questId AS questId, qhm.hash AS hash, qhm.source_type AS source_type
            FROM quest q
            JOIN quest_hash_map qhm ON qhm.questId = q.questId
            WHERE qhm.source_type IN ('title', 'dialogue')
              AND qhm.hash IS NOT NULL
              AND qhm.hash <> 0
              {target_filter_q}
        """
        return qh_sql, tuple()

    if target_updated_version_id is not None:
        qh_sql = f"""
            SELECT q.questId AS questId, q.titleTextMapHash AS hash, 'title' AS source_type
            FROM quest q
            JOIN quest_version qv ON qv.questId = q.questId
            WHERE qv.updated_version_id=?
              AND q.titleTextMapHash IS NOT NULL
              AND q.titleTextMapHash <> 0
              {target_filter_q}
            UNION ALL
            SELECT q.questId AS questId, d.textHash AS hash, 'dialogue' AS source_type
            FROM quest q
            JOIN questTalk qt ON qt.questId = q.questId
            JOIN dialogue d ON d.talkId = qt.talkId
               AND ({_quest_talk_dialogue_join_condition('qt', 'd')})
            JOIN quest_version qv ON qv.questId = q.questId
            WHERE qv.updated_version_id=?
              AND d.textHash IS NOT NULL
              {target_filter_q}
        """
        return qh_sql, (target_updated_version_id, target_updated_version_id)

    qh_sql = f"""
        SELECT q.questId AS questId, q.titleTextMapHash AS hash, 'title' AS source_type
        FROM quest q
        WHERE q.titleTextMapHash IS NOT NULL
          AND q.titleTextMapHash <> 0
          {target_filter_q}
        UNION ALL
        SELECT qt.questId AS questId, d.textHash AS hash, 'dialogue' AS source_type
        FROM questTalk qt
        JOIN dialogue d ON d.talkId = qt.talkId
           AND ({_quest_talk_dialogue_join_condition('qt', 'd')})
        WHERE d.textHash IS NOT NULL
          {target_filter_qt}
    """
    return qh_sql, tuple()


def _build_version_ranking_sql(base_sql: str, version_column: str, is_created: bool) -> str:
    """
    Build SQL for ranking versions based on version tag and version ID.
    """
    return f"""
    WITH qh AS (
        {base_sql}
    ),
    candidates AS (
        SELECT
            qh.questId AS questId,
            tm.{version_column} AS {version_column},
            vd.version_sort_key AS version_sort_key
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
                    {_build_version_order_by_sql(version_column, is_created)}
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
    authoritative: bool = False,
    with_stats: bool = False,
) -> int | tuple[int, int]:
    """
    Backfill quest created_version_id from textMap.
    """
    reset_temp_table(
        cursor,
        "CREATE TEMP TABLE IF NOT EXISTS _quest_inferred_created_version("
        "questId INTEGER PRIMARY KEY, inferred_created_version_id INTEGER)",
        "_quest_inferred_created_version",
    )
    reset_temp_table(
        cursor,
        "CREATE TEMP TABLE IF NOT EXISTS _quest_inferred_updated_version("
        "questId INTEGER NOT NULL, lang INTEGER NOT NULL, inferred_updated_version_id INTEGER, "
        "PRIMARY KEY(questId, lang))",
        "_quest_inferred_updated_version",
    )
    use_target_quest_id = authoritative or quest_ids is not None
    if use_target_quest_id:
        reset_temp_table(
            cursor,
            "CREATE TEMP TABLE IF NOT EXISTS _target_quest_id(questId INTEGER PRIMARY KEY)",
            "_target_quest_id",
        )
    if authoritative:
        reset_temp_table(
            cursor,
            "CREATE TEMP TABLE IF NOT EXISTS _quest_authoritative_created_version("
            "questId INTEGER PRIMARY KEY, final_created_version_id INTEGER)",
            "_quest_authoritative_created_version",
        )
    reset_temp_table(
        cursor,
        "CREATE TEMP TABLE IF NOT EXISTS _quest_hash_source("
        "questId INTEGER NOT NULL, hash INTEGER NOT NULL, source_type TEXT NOT NULL)",
        "_quest_hash_source",
    )

    try:
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
            cursor.executemany("INSERT OR IGNORE INTO _target_quest_id(questId) VALUES (?)", normalized_ids)
            target_filter_q = " AND q.questId IN (SELECT questId FROM _target_quest_id)"
            target_filter_qt = " AND qt.questId IN (SELECT questId FROM _target_quest_id)"
        elif authoritative:
            cursor.execute("INSERT OR IGNORE INTO _target_quest_id(questId) SELECT questId FROM quest")
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
            f"INSERT INTO _quest_hash_source(questId, hash, source_type) {qh_sql}",
            qh_params,
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS _quest_hash_source_hash_idx ON _quest_hash_source(hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS _quest_hash_source_quest_idx ON _quest_hash_source(questId)")

        chs_lang_id = get_quest_text_filter_lang_id(cursor)
        created_dialogue_filter_sql = ""
        created_filter_params: tuple[object, ...] = tuple()
        created_lang_sql = "tm.lang = (SELECT id FROM langCode WHERE codeName = 'TextMapCHS.json' LIMIT 1)"
        if chs_lang_id is not None:
            created_lang_sql = "tm.lang = ?"
            created_dialogue_filter_sql, created_not_excluded_params = (
                build_quest_version_dialogue_not_excluded_sql("tm.content")
            )
            created_dialogue_filter_sql = (
                " AND (qh.source_type <> 'dialogue' OR "
                + created_dialogue_filter_sql
                + ")"
            )
            created_filter_params = (chs_lang_id, *created_not_excluded_params)

        created_sql = f"""
        WITH candidates AS (
            SELECT
                qh.questId AS questId,
                tm.created_version_id AS created_version_id,
                vd.version_sort_key AS version_sort_key
            FROM _quest_hash_source qh
            JOIN textMap tm ON tm.hash = qh.hash
            LEFT JOIN version_dim vd ON vd.id = tm.created_version_id
            WHERE tm.created_version_id IS NOT NULL
              AND qh.hash <> 0
              AND {created_lang_sql}
              {created_dialogue_filter_sql}
        ),
        ranked AS (
            SELECT
                questId,
                created_version_id,
                ROW_NUMBER() OVER (
                    PARTITION BY questId
                    ORDER BY
                        {_build_version_order_by_sql("created_version_id", True)}
                ) AS rn
            FROM candidates
        )
        """

        cursor.execute(
            f"""
            {created_sql}
            INSERT OR REPLACE INTO _quest_inferred_created_version(questId, inferred_created_version_id)
            SELECT questId, created_version_id
            FROM ranked
            WHERE rn = 1
            """,
            created_filter_params,
        )

        if authoritative:
            cursor.execute(
                f"""
                INSERT OR REPLACE INTO _quest_authoritative_created_version(questId, final_created_version_id)
                SELECT
                    target.questId,
                    CASE
                        WHEN inferred.inferred_created_version_id IS NULL THEN quest.git_created_version_id
                        WHEN quest.git_created_version_id IS NULL THEN inferred.inferred_created_version_id
                        WHEN {_version_precedes_sql('inferred.inferred_created_version_id', 'quest.git_created_version_id')}
                        THEN inferred.inferred_created_version_id
                        ELSE quest.git_created_version_id
                    END AS final_created_version_id
                FROM _target_quest_id target
                JOIN quest ON quest.questId = target.questId
                LEFT JOIN _quest_inferred_created_version inferred
                    ON inferred.questId = target.questId
                """
            )
            cursor.execute(
                """
                UPDATE quest
                SET created_version_id = (
                    SELECT final_created_version_id
                    FROM _quest_authoritative_created_version t
                    WHERE t.questId = quest.questId
                )
                WHERE questId IN (SELECT questId FROM _target_quest_id)
                  AND COALESCE(created_version_id, -1) <> COALESCE(
                      (
                          SELECT final_created_version_id
                          FROM _quest_authoritative_created_version t
                          WHERE t.questId = quest.questId
                      ),
                      -1
                  )
                """
            )
            created_backfilled = int(cursor.rowcount or 0)
        else:
            update_created_sql = f"""
            UPDATE quest
            SET created_version_id = (
                SELECT t.inferred_created_version_id
                FROM _quest_inferred_created_version t
                WHERE t.questId = quest.questId
            )
            WHERE questId IN (SELECT questId FROM _quest_inferred_created_version)
              AND (created_version_id IS NULL OR {_version_precedes_sql(
                  "(SELECT t.inferred_created_version_id FROM _quest_inferred_created_version t WHERE t.questId = quest.questId)",
                  "created_version_id",
              )})
            """
            cursor.execute(update_created_sql)
            created_backfilled = int(cursor.rowcount or 0)

        lang_rows = cursor.execute("SELECT id FROM langCode WHERE imported=1").fetchall()
        languages = [row[0] for row in lang_rows]

        updated_backfilled = 0
        for lang in languages:
            updated_not_excluded_sql, updated_not_excluded_params = (
                build_quest_version_dialogue_not_excluded_sql("tm.content")
            )
            updated_sql = f"""
            WITH candidates AS (
                SELECT
                    qh.questId AS questId,
                    tm.updated_version_id AS updated_version_id,
                    vd.version_sort_key AS version_sort_key
                FROM _quest_hash_source qh
                JOIN textMap tm ON tm.hash = qh.hash
                LEFT JOIN version_dim vd ON vd.id = tm.updated_version_id
                WHERE tm.updated_version_id IS NOT NULL
                  AND qh.hash <> 0
                  AND tm.lang = ?
                  AND (
                      qh.source_type <> 'dialogue'
                      OR {updated_not_excluded_sql}
                  )
            ),
            ranked AS (
                SELECT
                    questId,
                    updated_version_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY questId
                        ORDER BY
                            {_build_version_order_by_sql("updated_version_id", False)}
                    ) AS rn
                FROM candidates
            )
            INSERT OR REPLACE INTO _quest_inferred_updated_version(questId, lang, inferred_updated_version_id)
            SELECT questId, ?, updated_version_id
            FROM ranked
            WHERE rn = 1
            """
            cursor.execute(updated_sql, (lang, *updated_not_excluded_params, lang))
            if not authoritative:
                cursor.execute(
                    f"""
                    INSERT INTO quest_version(questId, lang, updated_version_id)
                    SELECT questId, lang, inferred_updated_version_id
                    FROM _quest_inferred_updated_version
                    WHERE lang = ?
                    ON CONFLICT(questId, lang) DO UPDATE SET
                    updated_version_id={_build_version_preference_case_sql(
                        existing_expr="quest_version.updated_version_id",
                        candidate_expr="excluded.updated_version_id",
                        is_created=False,
                    )}
                    """,
                    (lang,),
                )
                updated_backfilled += int(cursor.rowcount or 0)

        if authoritative:
            cursor.execute("DELETE FROM quest_version WHERE questId IN (SELECT questId FROM _target_quest_id)")
            cursor.execute(
                """
                INSERT INTO quest_version(questId, lang, updated_version_id)
                SELECT questId, lang, inferred_updated_version_id
                FROM _quest_inferred_updated_version
                WHERE inferred_updated_version_id IS NOT NULL
                """
            )
            updated_backfilled = int(cursor.rowcount or 0)

        return (created_backfilled, updated_backfilled) if with_stats else created_backfilled
    finally:
        cursor.execute("DELETE FROM _quest_hash_source")
        cursor.execute("DELETE FROM _quest_inferred_created_version")
        cursor.execute("DELETE FROM _quest_inferred_updated_version")
        if authoritative:
            cursor.execute("DELETE FROM _quest_authoritative_created_version")
        if use_target_quest_id:
            cursor.execute("DELETE FROM _target_quest_id")


def ensure_version_schema():
    ensure_base_schema()
    _ensure_version_schema_impl()
    _clear_version_sort_key_cache()


def get_current_version(default: str = "unknown") -> str:
    return _get_current_version_impl(default)


def rebuild_version_catalog(
    source_tables: tuple[str, ...] | list[str] | None = None,
) -> dict[str, int]:
    return _rebuild_version_catalog_impl(source_tables)


def set_current_version(
    commit: str,
    remote_ref: str = "origin/master",
    version_label: str | None = None,
) -> None:
    _set_current_version_impl(commit, remote_ref=remote_ref, version_label=version_label)


def get_or_create_version_id(raw_version: str | None) -> int | None:
    version_id = _get_or_create_version_id_impl(raw_version)
    if version_id is not None:
        _remember_version_sort_key(version_id)
    return version_id

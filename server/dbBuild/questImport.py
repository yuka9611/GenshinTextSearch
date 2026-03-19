import os
import json
import hashlib
import glob
import re
from lightweight_progress import LightweightProgress

from DBConfig import conn, DATA_PATH
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched
from quest_hash_map_utils import (
    ensure_quest_hash_map_schema as _ensure_quest_hash_map_schema,
    refresh_quest_hash_map_for_quest_ids as _refresh_quest_hash_map_for_quest_ids,
    refresh_quest_hash_map_for_talk_ids as _refresh_quest_hash_map_for_talk_ids,
)
from quest_utils import extract_quest_id, extract_quest_row, extract_quest_talk_ids
from version_control import backfill_quest_created_version_from_textmap as _backfill_quest_created_version_from_textmap
from version_control import (
    _build_version_preference_case_sql,
    get_current_version,
    get_or_create_version_id,
    should_update_version,
)


def _print_skip_summary(title: str, skipped_files: list[str], sample_size: int = 10):
    if not skipped_files:
        return
    samples = skipped_files[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(skipped_files) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SKIP] {title}: {len(skipped_files)} files skipped. samples: {sample_text}")


def _print_issue_summary(title: str, issues: list[str], sample_size: int = 10):
    if not issues:
        return
    samples = issues[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(issues) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SUMMARY] {title}: {len(issues)}. samples: {sample_text}")


_MAIN_QUEST_DESC_HASH_BY_ID: dict[int, int | None] | None = None
_QUEST_SOURCE_RAW_BY_ID: dict[int, str] | None = None
_HANGOUT_QUEST_IDS: set[int] | None = None
_MAIN_COOP_IDS_BY_QUEST_ID: dict[int, list[int]] | None = None

SOURCE_TYPE_AQ = "AQ"
SOURCE_TYPE_LQ = "LQ"
SOURCE_TYPE_WQ = "WQ"
SOURCE_TYPE_EQ = "EQ"
SOURCE_TYPE_IQ = "IQ"
SOURCE_TYPE_HANGOUT = "HANGOUT"
SOURCE_TYPE_ANECDOTE = "ANECDOTE"
SOURCE_TYPE_UNKNOWN = "UNKNOWN"
_BASE_QUEST_SOURCE_TYPES = {
    SOURCE_TYPE_AQ,
    SOURCE_TYPE_LQ,
    SOURCE_TYPE_WQ,
    SOURCE_TYPE_EQ,
    SOURCE_TYPE_IQ,
}
QUEST_TALK_NORMAL_COOP_ID = 0
QUEST_TALK_SCOPE_ALL = "all"
QUEST_TALK_SCOPE_NORMAL = "normal"
QUEST_TALK_SCOPE_COOP = "coop"


def _load_json_file(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _normalize_nonzero_int(value):
    if isinstance(value, int) and value != 0:
        return value
    return None


def _normalize_source_code_raw(value) -> str:
    if not isinstance(value, str):
        return SOURCE_TYPE_UNKNOWN
    normalized = value.strip().upper()
    if normalized in _BASE_QUEST_SOURCE_TYPES:
        return normalized
    return SOURCE_TYPE_UNKNOWN


def _reset_quest_source_caches():
    global _QUEST_SOURCE_RAW_BY_ID, _HANGOUT_QUEST_IDS, _MAIN_COOP_IDS_BY_QUEST_ID
    _QUEST_SOURCE_RAW_BY_ID = None
    _HANGOUT_QUEST_IDS = None
    _MAIN_COOP_IDS_BY_QUEST_ID = None


def _load_quest_source_raw_by_id() -> dict[int, str]:
    global _QUEST_SOURCE_RAW_BY_ID
    if _QUEST_SOURCE_RAW_BY_ID is not None:
        return _QUEST_SOURCE_RAW_BY_ID

    mapping: dict[int, str] = {}
    folder = os.path.join(DATA_PATH, "BinOutput", "QuestBrief")
    if os.path.isdir(folder):
        for file_name in sorted(os.listdir(folder)):
            if not file_name.endswith(".json"):
                continue
            path = os.path.join(folder, file_name)
            try:
                obj = _load_json_file(path)
            except Exception:
                continue
            quest_id = extract_quest_id(obj)
            if not isinstance(quest_id, int) or quest_id <= 0:
                continue
            mapping[quest_id] = _normalize_source_code_raw(obj.get("DLPKMDPABFM"))

    _QUEST_SOURCE_RAW_BY_ID = mapping
    return mapping


def _load_main_coop_ids_by_quest_id() -> dict[int, list[int]]:
    global _MAIN_COOP_IDS_BY_QUEST_ID
    if _MAIN_COOP_IDS_BY_QUEST_ID is not None:
        return _MAIN_COOP_IDS_BY_QUEST_ID

    mapping: dict[int, list[int]] = {}
    main_coop_path = os.path.join(DATA_PATH, "ExcelBinOutput", "MainCoopExcelConfigData.json")
    if os.path.isfile(main_coop_path):
        try:
            rows = _load_json_file(main_coop_path)
        except Exception:
            rows = []
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                raw_id = row.get("id")
                if not isinstance(raw_id, int) or raw_id <= 0:
                    raw_id = row.get("JLJFKNHFLJP")
                if not isinstance(raw_id, int) or raw_id <= 0:
                    continue
                quest_id = raw_id // 100
                bucket = mapping.setdefault(quest_id, [])
                if raw_id not in bucket:
                    bucket.append(raw_id)

    coop_folder = os.path.join(DATA_PATH, "BinOutput", "Coop")
    if os.path.isdir(coop_folder):
        for file_name in os.listdir(coop_folder):
            match = re.match(r"^Coop(\d+)\.json$", file_name)
            if not match:
                continue
            raw_id = int(match.group(1))
            quest_id = raw_id // 100
            bucket = mapping.setdefault(quest_id, [])
            if raw_id not in bucket:
                bucket.append(raw_id)

    _MAIN_COOP_IDS_BY_QUEST_ID = mapping
    return mapping


def _load_hangout_quest_ids() -> set[int]:
    global _HANGOUT_QUEST_IDS
    if _HANGOUT_QUEST_IDS is not None:
        return _HANGOUT_QUEST_IDS

    quest_ids: set[int] = set(_load_main_coop_ids_by_quest_id().keys())

    coop_folder = os.path.join(DATA_PATH, "BinOutput", "Coop")
    if os.path.isdir(coop_folder):
        for file_name in os.listdir(coop_folder):
            match = re.match(r"^Coop(\d+)\.json$", file_name)
            if not match:
                continue
            try:
                quest_ids.add(int(match.group(1)) // 100)
            except Exception:
                continue

    _HANGOUT_QUEST_IDS = quest_ids
    return quest_ids


def _resolve_quest_source_fields(quest_id: int | None, *, is_anecdote: bool = False) -> tuple[str, str]:
    if is_anecdote:
        return SOURCE_TYPE_ANECDOTE, SOURCE_TYPE_ANECDOTE
    if not isinstance(quest_id, int) or quest_id <= 0:
        return SOURCE_TYPE_UNKNOWN, SOURCE_TYPE_UNKNOWN

    source_code_raw = _load_quest_source_raw_by_id().get(quest_id, SOURCE_TYPE_UNKNOWN)
    if quest_id in _load_hangout_quest_ids():
        return SOURCE_TYPE_HANGOUT, source_code_raw
    if source_code_raw in _BASE_QUEST_SOURCE_TYPES:
        return source_code_raw, source_code_raw
    return SOURCE_TYPE_UNKNOWN, source_code_raw


def _build_quest_upsert_sql(*, with_created_version: bool = False) -> str:
    if with_created_version:
        return (
            "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw, created_version_id) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(questId) DO UPDATE SET "
            "titleTextMapHash=excluded.titleTextMapHash, "
            "descTextMapHash=excluded.descTextMapHash, "
            "chapterId=excluded.chapterId, "
            "source_type=excluded.source_type, "
            "source_code_raw=excluded.source_code_raw, "
            "created_version_id="
            + _build_version_preference_case_sql(
                existing_expr="quest.created_version_id",
                candidate_expr="excluded.created_version_id",
                is_created=True,
            )
            + " "
            "WHERE "
            "NOT (quest.titleTextMapHash IS excluded.titleTextMapHash) "
            "OR NOT (quest.descTextMapHash IS excluded.descTextMapHash) "
            "OR NOT (quest.chapterId IS excluded.chapterId) "
            "OR NOT (quest.source_type IS excluded.source_type) "
            "OR NOT (quest.source_code_raw IS excluded.source_code_raw) "
            "OR quest.created_version_id IS NULL"
        )
    return (
        "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw) "
        "VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(questId) DO UPDATE SET "
        "titleTextMapHash=excluded.titleTextMapHash, "
        "descTextMapHash=excluded.descTextMapHash, "
        "chapterId=excluded.chapterId, "
        "source_type=excluded.source_type, "
        "source_code_raw=excluded.source_code_raw "
        "WHERE "
        "NOT (quest.titleTextMapHash IS excluded.titleTextMapHash) "
        "OR NOT (quest.descTextMapHash IS excluded.descTextMapHash) "
        "OR NOT (quest.chapterId IS excluded.chapterId) "
        "OR NOT (quest.source_type IS excluded.source_type) "
        "OR NOT (quest.source_code_raw IS excluded.source_code_raw)"
    )


def _is_hidden_quest_obj(obj: dict) -> bool:
    quest_type = None
    if isinstance(obj, dict):
        if "questType" in obj:
            quest_type = obj.get("questType")
        elif "NCDLPENPKKC" in obj:
            quest_type = obj.get("NCDLPENPKKC")
    return quest_type == "QUEST_HIDDEN"


def _ensure_quest_version_tables(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS quest_text_signature (
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            dialogue_signature TEXT NOT NULL
        )
        """
    )
    quest_columns = {row[1] for row in cursor.execute("PRAGMA table_info(quest)").fetchall()}
    if "descTextMapHash" not in quest_columns:
        cursor.execute("ALTER TABLE quest ADD COLUMN descTextMapHash INTEGER")
    if "git_created_version_id" not in quest_columns:
        cursor.execute("ALTER TABLE quest ADD COLUMN git_created_version_id INTEGER")
    if "source_type" not in quest_columns:
        cursor.execute("ALTER TABLE quest ADD COLUMN source_type TEXT")
    if "source_code_raw" not in quest_columns:
        cursor.execute("ALTER TABLE quest ADD COLUMN source_code_raw TEXT")
    quest_talk_columns = {row[1] for row in cursor.execute("PRAGMA table_info(questTalk)").fetchall()}
    if "stepTitleTextMapHash" not in quest_talk_columns:
        cursor.execute("ALTER TABLE questTalk ADD COLUMN stepTitleTextMapHash INTEGER")
    if "coopQuestId" not in quest_talk_columns:
        cursor.execute("ALTER TABLE questTalk ADD COLUMN coopQuestId INTEGER NOT NULL DEFAULT 0")
        cursor.execute("UPDATE questTalk SET coopQuestId = 0 WHERE coopQuestId IS NULL")
    cursor.execute("DROP INDEX IF EXISTS questTalk_questId_talkId_uindex")
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS questTalk_questId_talkId_coopQuestId_uindex "
        "ON questTalk(questId, talkId, coopQuestId)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS questTalk_talkId_coopQuestId_index ON questTalk(talkId, coopQuestId)"
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS quest_source_type_index ON quest(source_type)")
    _ensure_quest_hash_map_schema(cursor)


def _normalize_talk_ids(talk_ids):
    normalized = []
    seen = set()
    for talk_id in talk_ids:
        try:
            tid = int(talk_id)
        except Exception:
            continue
        if tid in seen:
            continue
        seen.add(tid)
        normalized.append(tid)
    return normalized


def _normalize_coop_quest_id(value) -> int:
    try:
        coop_quest_id = int(value)
    except Exception:
        return QUEST_TALK_NORMAL_COOP_ID
    return coop_quest_id if coop_quest_id > 0 else QUEST_TALK_NORMAL_COOP_ID


def _normalize_quest_talk_rows(rows) -> list[tuple[int, int | None, int]]:
    normalized: list[tuple[int, int | None, int]] = []
    seen: set[tuple[int, int]] = set()
    for row in rows or []:
        if not isinstance(row, (tuple, list)):
            continue
        if len(row) == 2:
            talk_id, step_hash = row
            coop_quest_id = QUEST_TALK_NORMAL_COOP_ID
        elif len(row) >= 3:
            talk_id, step_hash, coop_quest_id = row[:3]
        else:
            continue
        try:
            talk_id = int(talk_id)
        except Exception:
            continue
        if talk_id <= 0:
            continue
        normalized_coop_id = _normalize_coop_quest_id(coop_quest_id)
        key = (talk_id, normalized_coop_id)
        if key in seen:
            continue
        seen.add(key)
        normalized.append((talk_id, step_hash, normalized_coop_id))
    normalized.sort(key=lambda item: (item[2], item[0]))
    return normalized


def _build_quest_talk_insert_sql(*, upsert_step_title: bool = False) -> str:
    sql = "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)"
    if upsert_step_title:
        sql += (
            " ON CONFLICT(questId, talkId, coopQuestId) DO UPDATE SET "
            "stepTitleTextMapHash=excluded.stepTitleTextMapHash "
            "WHERE NOT (questTalk.stepTitleTextMapHash IS excluded.stepTitleTextMapHash)"
        )
    return sql


def _quest_talk_scope_where(scope: str) -> tuple[str, tuple]:
    if scope == QUEST_TALK_SCOPE_NORMAL:
        return " AND coalesce(coopQuestId, 0) = 0", tuple()
    if scope == QUEST_TALK_SCOPE_COOP:
        return " AND coalesce(coopQuestId, 0) > 0", tuple()
    return "", tuple()


def _fetch_existing_quest_talk_rows(cursor, quest_id: int, *, scope: str = QUEST_TALK_SCOPE_ALL):
    where_sql, params = _quest_talk_scope_where(scope)
    return cursor.execute(
        "SELECT talkId, stepTitleTextMapHash, coalesce(coopQuestId, 0) "
        "FROM questTalk WHERE questId=?"
        + where_sql
        + " ORDER BY coalesce(coopQuestId, 0), talkId",
        (quest_id, *params),
    ).fetchall()


def _delete_quest_talk_rows(cursor, quest_id: int, *, scope: str = QUEST_TALK_SCOPE_ALL):
    where_sql, params = _quest_talk_scope_where(scope)
    cursor.execute(
        "DELETE FROM questTalk WHERE questId=?"
        + where_sql,
        (quest_id, *params),
    )


def _insert_quest_talk_rows(cursor, quest_id: int, talk_rows):
    normalized_rows = _normalize_quest_talk_rows(talk_rows)
    if not normalized_rows:
        return
    cursor.executemany(
        _build_quest_talk_insert_sql(),
        (
            (quest_id, talk_id, step_hash, coop_quest_id)
            for talk_id, step_hash, coop_quest_id in normalized_rows
        ),
    )


def _replace_quest_talk_rows(cursor, quest_id: int, talk_rows, *, scope: str = QUEST_TALK_SCOPE_ALL):
    _delete_quest_talk_rows(cursor, quest_id, scope=scope)
    _insert_quest_talk_rows(cursor, quest_id, talk_rows)


def _load_main_quest_desc_hash_by_id() -> dict[int, int | None]:
    global _MAIN_QUEST_DESC_HASH_BY_ID
    if _MAIN_QUEST_DESC_HASH_BY_ID is not None:
        return _MAIN_QUEST_DESC_HASH_BY_ID

    mapping: dict[int, int | None] = {}
    path = os.path.join(DATA_PATH, "ExcelBinOutput", "MainQuestExcelConfigData.json")
    if os.path.isfile(path):
        try:
            rows = _load_json_file(path)
        except Exception:
            rows = []
        if isinstance(rows, list):
            for row in rows:
                if not isinstance(row, dict):
                    continue
                quest_id = row.get("id")
                if not isinstance(quest_id, int):
                    continue
                desc_hash = row.get("descTextMapHash")
                mapping[quest_id] = desc_hash if isinstance(desc_hash, int) and desc_hash != 0 else None

    _MAIN_QUEST_DESC_HASH_BY_ID = mapping
    return mapping


def _get_quest_desc_text_map_hash(quest_id: int | None) -> int | None:
    if not isinstance(quest_id, int):
        return None
    return _load_main_quest_desc_hash_by_id().get(quest_id)


def _get_quest_subquests(obj: dict) -> list[dict]:
    subquests = obj.get("NLCNGJKMAEN")
    if not isinstance(subquests, list):
        subquests = obj.get("subQuests")
    if not isinstance(subquests, list):
        subquests = obj.get("GFLHMKOOHHA")
    if not isinstance(subquests, list):
        return []
    return [item for item in subquests if isinstance(item, dict)]


def _get_step_desc_text_map_hash(step_obj: dict) -> int | None:
    for key in ("stepDescTextMapHash", "OCMKKHHNKJO", "BMBANCMPPOM", "NAEMBIJFJCA", "HMLBMECMBGA"):
        value = step_obj.get(key)
        if isinstance(value, int) and value != 0:
            return value
    return None


def _get_step_talk_ids(step_obj: dict) -> list[int]:
    talk_ids: list[int] = []
    seen: set[int] = set()

    conditions = step_obj.get("AACKELGGJGC")
    if not isinstance(conditions, list):
        conditions = step_obj.get("finishCond")
    if not isinstance(conditions, list):
        conditions = step_obj.get("KBFJAAFDHKJ")
    if not isinstance(conditions, list):
        return talk_ids

    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        cond_type = (
            condition.get("DLPKMDPABFM")
            or condition.get("type")
            or condition.get("PAINLIBBLDK")
        )
        if cond_type != "QUEST_CONTENT_COMPLETE_TALK":
            continue
        params = (
            condition.get("IEKGEJMAOCN")
            or condition.get("param")
            or condition.get("paramList")
            or condition.get("LNHLPKELCAL")
        )
        if not isinstance(params, list) or not params:
            continue
        talk_id = params[0]
        if isinstance(talk_id, int) and talk_id > 0 and talk_id not in seen:
            seen.add(talk_id)
            talk_ids.append(talk_id)
    return talk_ids


def _build_step_title_hash_by_talk_id(obj: dict) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for subquest in _get_quest_subquests(obj):
        step_hash = _get_step_desc_text_map_hash(subquest)
        if not isinstance(step_hash, int) or step_hash == 0:
            continue
        for talk_id in _get_step_talk_ids(subquest):
            mapping.setdefault(talk_id, step_hash)
    return mapping


def _build_quest_talk_rows(obj: dict, talk_ids: list[int]) -> list[tuple[int, int | None, int]]:
    step_title_hash_by_talk_id = _build_step_title_hash_by_talk_id(obj)
    rows: list[tuple[int, int | None, int]] = []
    for talk_id in sorted(_normalize_talk_ids(talk_ids)):
        rows.append((talk_id, step_title_hash_by_talk_id.get(talk_id), QUEST_TALK_NORMAL_COOP_ID))
    return _normalize_quest_talk_rows(rows)


def _resolve_main_quest_id_for_subquest(subquest: dict, fallback_quest_id: int | None = None) -> int | None:
    if isinstance(fallback_quest_id, int) and fallback_quest_id > 0:
        return fallback_quest_id
    for key in ("mainQuestId", "GNGFBMPFBOK", "JKHGFFKOFFN"):
        value = subquest.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def _iter_subquest_talk_rows(obj: dict, fallback_quest_id: int | None = None):
    for subquest in _get_quest_subquests(obj):
        main_quest_id = _resolve_main_quest_id_for_subquest(subquest, fallback_quest_id)
        if not isinstance(main_quest_id, int) or main_quest_id <= 0:
            continue
        step_hash = _get_step_desc_text_map_hash(subquest)
        for talk_id in _get_step_talk_ids(subquest):
            yield (main_quest_id, talk_id, step_hash, QUEST_TALK_NORMAL_COOP_ID)


def _build_quest_dialogue_signature(cursor, talk_rows):
    normalized_rows = _normalize_quest_talk_rows(talk_rows)
    if not normalized_rows:
        return ""
    conditions: list[str] = []
    params: list[int] = []
    for talk_id, _step_hash, coop_quest_id in normalized_rows:
        if coop_quest_id > 0:
            conditions.append("(talkId = ? AND coopQuestId = ?)")
            params.extend((talk_id, coop_quest_id))
        else:
            conditions.append("(talkId = ? AND coopQuestId IS NULL)")
            params.append(talk_id)
    rows = cursor.execute(
        """
        SELECT textHash, COUNT(*)
        FROM dialogue
        WHERE (
        """
        + " OR ".join(conditions)
        + """
        )
          AND textHash IS NOT NULL
          AND textHash <> 0
        GROUP BY textHash
        ORDER BY textHash
        """,
        tuple(params),
    ).fetchall()
    payload = "|".join(f"{text_hash}:{count}" for text_hash, count in rows)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _upsert_quest_text_signature(cursor, quest_id, title_text_map_hash, dialogue_signature):
    cursor.execute(
        "INSERT INTO quest_text_signature(questId, titleTextMapHash, dialogue_signature) VALUES (?,?,?) "
        "ON CONFLICT(questId) DO UPDATE SET "
        "titleTextMapHash=excluded.titleTextMapHash, "
        "dialogue_signature=excluded.dialogue_signature "
        "WHERE "
        "NOT (quest_text_signature.dialogue_signature IS excluded.dialogue_signature)",
        (quest_id, title_text_map_hash, dialogue_signature),
    )


def importQuest(
    fileName: str,
    *,
    cursor=None,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
        _ensure_quest_version_tables(cursor)
    obj = _load_json_file(os.path.join(DATA_PATH, "BinOutput", "Quest", fileName))

    sql1 = _build_quest_upsert_sql(with_created_version=False)
    sql2 = _build_quest_talk_insert_sql()

    quest_row = extract_quest_row(obj)
    if quest_row is None:
        if skip_collector is not None:
            skip_collector.append(fileName)
        elif log_skip:
            print("Skipping " + fileName)
        if own_cursor:
            cursor.close()
        return None, False

    questId, titleTextMapHash, chapterId = quest_row
    descTextMapHash = _get_quest_desc_text_map_hash(questId)
    source_type, source_code_raw = _resolve_quest_source_fields(questId)
    if titleTextMapHash in (None, 0):
        titleTextMapHash = None
        if not _is_hidden_quest_obj(obj):
            if missing_title_collector is not None:
                missing_title_collector.append(f"{questId} ({fileName})")
            else:
                print("questId {} don't have TitleTextMapHash!".format(questId))
    if chapterId == 0:
        chapterId = None

    talk_ids = extract_quest_talk_ids(obj)
    if not talk_ids:
        if no_talk_collector is not None:
            no_talk_collector.append(f"{questId} ({fileName})")
        else:
            print("questId {} don't have talk!".format(questId))
    new_talk_rows = _build_quest_talk_rows(obj, talk_ids)

    new_signature = _build_quest_dialogue_signature(cursor, new_talk_rows)
    existing_quest_row = cursor.execute(
        "SELECT titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw FROM quest WHERE questId=?",
        (questId,),
    ).fetchone()
    is_new_quest = existing_quest_row is None
    quest_changed = (
        is_new_quest
        or existing_quest_row[0] != titleTextMapHash
        or existing_quest_row[1] != descTextMapHash
        or existing_quest_row[2] != chapterId
        or existing_quest_row[3] != source_type
        or existing_quest_row[4] != source_code_raw
    )

    old_talk_rows = _fetch_existing_quest_talk_rows(cursor, questId, scope=QUEST_TALK_SCOPE_NORMAL)
    talk_links_changed = old_talk_rows != new_talk_rows

    if quest_changed:
        cursor.execute(sql1, (questId, titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw))
    if talk_links_changed:
        _delete_quest_talk_rows(cursor, questId, scope=QUEST_TALK_SCOPE_NORMAL)
        cursor.executemany(sql2, ((questId, talkId, step_hash, coop_quest_id) for talkId, step_hash, coop_quest_id in new_talk_rows))

    _upsert_quest_text_signature(cursor, questId, titleTextMapHash, new_signature)

    if own_cursor:
        cursor.close()
    return questId, is_new_quest


def importQuestForDiff(
    fileName: str,
    current_version: str,
    *,
    cursor=None,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
        _ensure_quest_version_tables(cursor)
    obj = _load_json_file(os.path.join(DATA_PATH, "BinOutput", "Quest", fileName))

    version = current_version or get_current_version()
    get_or_create_version_id(version)
    sql1 = _build_quest_upsert_sql(with_created_version=True)
    sql2 = _build_quest_talk_insert_sql()

    quest_row = extract_quest_row(obj)
    if quest_row is None:
        if skip_collector is not None:
            skip_collector.append(fileName)
        elif log_skip:
            print("Skipping " + fileName)
        if own_cursor:
            cursor.close()
        return None, False

    questId, titleTextMapHash, chapterId = quest_row
    descTextMapHash = _get_quest_desc_text_map_hash(questId)
    source_type, source_code_raw = _resolve_quest_source_fields(questId)
    if titleTextMapHash in (None, 0):
        titleTextMapHash = None
        if not _is_hidden_quest_obj(obj):
            if missing_title_collector is not None:
                missing_title_collector.append(f"{questId} ({fileName})")
            else:
                print("questId {} don't have TitleTextMapHash!".format(questId))
    if chapterId == 0:
        chapterId = None

    talk_ids = extract_quest_talk_ids(obj)
    if not talk_ids:
        if no_talk_collector is not None:
            no_talk_collector.append(f"{questId} ({fileName})")
        else:
            print("questId {} don't have talk!".format(questId))
    new_talk_rows = _build_quest_talk_rows(obj, talk_ids)

    new_signature = _build_quest_dialogue_signature(cursor, new_talk_rows)
    old_signature_row = cursor.execute(
        "SELECT dialogue_signature FROM quest_text_signature WHERE questId=?",
        (questId,),
    ).fetchone()
    dialogue_changed = old_signature_row is None or old_signature_row[0] != new_signature

    title_changed = False
    if titleTextMapHash:
        current_title_content = cursor.execute(
            "SELECT content FROM textMap WHERE hash=? LIMIT 1",
            (titleTextMapHash,),
        ).fetchone()
        old_title_hash_row = cursor.execute(
            "SELECT titleTextMapHash FROM quest WHERE questId=?",
            (questId,),
        ).fetchone()

        if old_title_hash_row and old_title_hash_row[0]:
            old_title_content = cursor.execute(
                "SELECT content FROM textMap WHERE hash=? LIMIT 1",
                (old_title_hash_row[0],),
            ).fetchone()
            current_content = current_title_content[0] if current_title_content else None
            old_content = old_title_content[0] if old_title_content else None
            title_changed = current_content != old_content
        else:
            title_changed = True

    text_changed = dialogue_changed or title_changed

    old_quest_meta_row = cursor.execute(
        "SELECT titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw FROM quest WHERE questId=?",
        (questId,),
    ).fetchone()
    quest_row_changed = (
        old_quest_meta_row is None
        or old_quest_meta_row[0] != titleTextMapHash
        or old_quest_meta_row[1] != descTextMapHash
        or old_quest_meta_row[2] != chapterId
        or old_quest_meta_row[3] != source_type
        or old_quest_meta_row[4] != source_code_raw
    )

    old_talk_rows = _fetch_existing_quest_talk_rows(cursor, questId, scope=QUEST_TALK_SCOPE_NORMAL)
    talk_links_changed = old_talk_rows != new_talk_rows

    old_version_row = cursor.execute(
        "SELECT created_version_id FROM quest WHERE questId=?",
        (questId,),
    ).fetchone()
    is_new_quest = old_version_row is None

    old_created_version = old_version_row[0] if old_version_row else None
    created_version = old_created_version
    created_version_changed = should_update_version(old_created_version, created_version, is_created=True)

    if is_new_quest or quest_row_changed or text_changed or talk_links_changed or created_version_changed:
        cursor.execute(
            sql1,
            (questId, titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw, created_version),
        )
    if talk_links_changed:
        _delete_quest_talk_rows(cursor, questId, scope=QUEST_TALK_SCOPE_NORMAL)
        cursor.executemany(sql2, ((questId, talkId, step_hash, coop_quest_id) for talkId, step_hash, coop_quest_id in new_talk_rows))

    _upsert_quest_text_signature(cursor, questId, titleTextMapHash, new_signature)

    if own_cursor:
        cursor.close()
    return questId, is_new_quest


def importAllQuests(
    sync_delete: bool = False,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    _reset_quest_source_caches()
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)

    quest_folder = os.path.join(DATA_PATH, "BinOutput", "Quest")
    files = os.listdir(quest_folder)
    imported_quest_ids = set()
    new_quest_ids = set()
    skipped_quest_files: list[str] = []
    missing_title_quests: list[str] = []
    no_talk_quests: list[str] = []

    try:
        with LightweightProgress(len(files), desc="Quest files", unit="files") as pbar:
            for fileName in files:
                quest_id, is_new_quest = importQuest(
                    fileName,
                    cursor=cursor,
                    skip_collector=skipped_quest_files,
                    log_skip=False,
                    missing_title_collector=missing_title_quests,
                    no_talk_collector=no_talk_quests,
                )
                if quest_id is not None:
                    imported_quest_ids.add(quest_id)
                if is_new_quest and quest_id is not None:
                    new_quest_ids.add(quest_id)
                pbar.update()

        if sync_delete:
            if imported_quest_ids:
                placeholders = ",".join(["?"] * len(imported_quest_ids))
                params = (SOURCE_TYPE_ANECDOTE, *tuple(imported_quest_ids))
                delete_target_sql = (
                    f"SELECT questId FROM quest "
                    f"WHERE coalesce(source_type, '') <> ? AND questId NOT IN ({placeholders})"
                )
                cursor.execute(f"DELETE FROM questTalk WHERE questId IN ({delete_target_sql})", params)
                cursor.execute(f"DELETE FROM quest_text_signature WHERE questId IN ({delete_target_sql})", params)
                cursor.execute(f"DELETE FROM quest_hash_map WHERE questId IN ({delete_target_sql})", params)
                cursor.execute(f"DELETE FROM quest WHERE questId IN ({delete_target_sql})", params)
            else:
                cursor.execute("DELETE FROM questTalk WHERE questId IN (SELECT questId FROM quest WHERE coalesce(source_type, '') <> ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_text_signature WHERE questId IN (SELECT questId FROM quest WHERE coalesce(source_type, '') <> ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_hash_map WHERE questId IN (SELECT questId FROM quest WHERE coalesce(source_type, '') <> ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest WHERE coalesce(source_type, '') <> ?", (SOURCE_TYPE_ANECDOTE,))

        if imported_quest_ids:
            refreshed_hash_map_quests = _refresh_quest_hash_map_for_quest_ids(
                cursor,
                imported_quest_ids,
                batch_size=batch_size,
            )
        else:
            refreshed_hash_map_quests = 0

        conn.commit()
    except Exception as e:
        print(f"Error in importAllQuests: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("quest", skipped_quest_files)
    _print_issue_summary("quest missing titleTextMapHash", missing_title_quests)
    _print_issue_summary("quest without talk ids", no_talk_quests)
    return {
        "files_total": len(files),
        "imported_quest_count": len(imported_quest_ids),
        "new_quest_count": len(new_quest_ids),
        "skipped_file_count": len(skipped_quest_files),
        "skipped_file_samples": skipped_quest_files[:10],
        "missing_title_count": len(missing_title_quests),
        "no_talk_count": len(no_talk_quests),
        "hash_map_refreshed_quest_count": int(refreshed_hash_map_quests or 0),
    }


def importAllQuestsForDiff(
    current_version: str,
    sync_delete: bool = False,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    _reset_quest_source_caches()
    version = current_version or get_current_version()
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)

    quest_folder = os.path.join(DATA_PATH, "BinOutput", "Quest")
    files = os.listdir(quest_folder)
    imported_quest_ids = set()
    new_quest_ids = set()
    skipped_quest_files: list[str] = []
    missing_title_quests: list[str] = []
    no_talk_quests: list[str] = []

    try:
        with LightweightProgress(len(files), desc="Quest files", unit="files") as pbar:
            for fileName in files:
                quest_id, is_new_quest = importQuestForDiff(
                    fileName,
                    version,
                    cursor=cursor,
                    skip_collector=skipped_quest_files,
                    log_skip=False,
                    missing_title_collector=missing_title_quests,
                    no_talk_collector=no_talk_quests,
                )
                if quest_id is not None:
                    imported_quest_ids.add(quest_id)
                if is_new_quest and quest_id is not None:
                    new_quest_ids.add(quest_id)
                pbar.update()

        if sync_delete:
            if imported_quest_ids:
                placeholders = ",".join(["?"] * len(imported_quest_ids))
                params = (SOURCE_TYPE_ANECDOTE, *tuple(imported_quest_ids))
                delete_target_sql = (
                    f"SELECT questId FROM quest "
                    f"WHERE coalesce(source_type, '') <> ? AND questId NOT IN ({placeholders})"
                )
                cursor.execute(f"DELETE FROM questTalk WHERE questId IN ({delete_target_sql})", params)
                cursor.execute(f"DELETE FROM quest_text_signature WHERE questId IN ({delete_target_sql})", params)
                cursor.execute(f"DELETE FROM quest_hash_map WHERE questId IN ({delete_target_sql})", params)
                cursor.execute(f"DELETE FROM quest WHERE questId IN ({delete_target_sql})", params)
            else:
                cursor.execute("DELETE FROM questTalk WHERE questId IN (SELECT questId FROM quest WHERE coalesce(source_type, '') <> ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_text_signature WHERE questId IN (SELECT questId FROM quest WHERE coalesce(source_type, '') <> ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_hash_map WHERE questId IN (SELECT questId FROM quest WHERE coalesce(source_type, '') <> ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest WHERE coalesce(source_type, '') <> ?", (SOURCE_TYPE_ANECDOTE,))

        if imported_quest_ids:
            refreshed_hash_map_quests = _refresh_quest_hash_map_for_quest_ids(
                cursor,
                imported_quest_ids,
                batch_size=batch_size,
            )
        else:
            refreshed_hash_map_quests = 0

        if imported_quest_ids:
            _backfill_quest_created_version_from_textmap(
                cursor,
                quest_ids=imported_quest_ids,
                overwrite_existing=False,
            )

        conn.commit()
    except Exception as e:
        print(f"Error in importAllQuestsForDiff: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("quest", skipped_quest_files)
    _print_issue_summary("quest missing titleTextMapHash", missing_title_quests)
    _print_issue_summary("quest without talk ids", no_talk_quests)
    return {
        "files_total": len(files),
        "imported_quest_count": len(imported_quest_ids),
        "new_quest_count": len(new_quest_ids),
        "skipped_file_count": len(skipped_quest_files),
        "skipped_file_samples": skipped_quest_files[:10],
        "missing_title_count": len(missing_title_quests),
        "no_talk_count": len(no_talk_quests),
        "hash_map_refreshed_quest_count": int(refreshed_hash_map_quests or 0),
    }


def backfillQuestMetadata(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)

    quest_folder = os.path.join(DATA_PATH, "BinOutput", "Quest")
    quest_files = sorted(os.listdir(quest_folder)) if os.path.isdir(quest_folder) else []
    quest_desc_rows: list[tuple[int | None, int]] = []
    quest_talk_rows: list[tuple[int | None, int, int, int]] = []
    skipped_quest_files: list[str] = []

    brief_folder = os.path.join(DATA_PATH, "BinOutput", "QuestBrief")
    brief_files = sorted(os.listdir(brief_folder)) if os.path.isdir(brief_folder) else []
    skipped_brief_files: list[str] = []

    try:
        cursor.execute("UPDATE quest SET descTextMapHash = NULL WHERE descTextMapHash IS NOT NULL")
        cursor.execute("UPDATE questTalk SET stepTitleTextMapHash = NULL WHERE stepTitleTextMapHash IS NOT NULL")

        with LightweightProgress(len(quest_files), desc="Quest metadata", unit="files") as pbar:
            for fileName in quest_files:
                try:
                    obj = _load_json_file(os.path.join(quest_folder, fileName))
                except Exception:
                    skipped_quest_files.append(fileName)
                    pbar.update()
                    continue

                quest_row = extract_quest_row(obj)
                if quest_row is None:
                    skipped_quest_files.append(fileName)
                    pbar.update()
                    continue

                quest_id = int(quest_row[0])
                quest_desc_rows.append((_get_quest_desc_text_map_hash(quest_id), quest_id))
                for talk_id, step_hash, coop_quest_id in _build_quest_talk_rows(obj, extract_quest_talk_ids(obj)):
                    quest_talk_rows.append((step_hash, quest_id, talk_id, coop_quest_id))
                pbar.update()

        brief_talk_rows: list[tuple[int | None, int, int, int]] = []
        if brief_files:
            with LightweightProgress(len(brief_files), desc="QuestBrief metadata", unit="files") as pbar:
                for fileName in brief_files:
                    if not fileName.endswith(".json"):
                        pbar.update()
                        continue
                    try:
                        obj = _load_json_file(os.path.join(brief_folder, fileName))
                    except Exception:
                        skipped_brief_files.append(fileName)
                        pbar.update()
                        continue
                    quest_id = extract_quest_id(obj)
                    brief_talk_rows.extend(_iter_subquest_talk_rows(obj, quest_id))
                    pbar.update()

        executemany_batched(
            cursor,
            "UPDATE quest SET descTextMapHash=? WHERE questId=?",
            quest_desc_rows,
            batch_size=batch_size,
        )
        executemany_batched(
            cursor,
            "UPDATE questTalk SET stepTitleTextMapHash=? WHERE questId=? AND talkId=? AND coalesce(coopQuestId, 0)=?",
            quest_talk_rows,
            batch_size=batch_size,
        )
        if brief_talk_rows:
            executemany_batched(
                cursor,
                "UPDATE questTalk SET stepTitleTextMapHash=? WHERE questId=? AND talkId=? AND coalesce(coopQuestId, 0)=?",
                brief_talk_rows,
                batch_size=batch_size,
            )

        if commit:
            conn.commit()
    except Exception:
        if commit:
            conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("quest metadata", skipped_quest_files)
    _print_skip_summary("quest brief metadata", skipped_brief_files)
    return {
        "quest_file_count": len(quest_files),
        "quest_desc_row_count": len(quest_desc_rows),
        "quest_talk_row_count": len(quest_talk_rows),
        "quest_brief_row_count": len(brief_talk_rows),
        "skipped_quest_file_count": len(skipped_quest_files),
        "skipped_brief_file_count": len(skipped_brief_files),
    }


def _iter_talk_excel_config_paths() -> list[str]:
    pattern = os.path.join(DATA_PATH, "ExcelBinOutput", "TalkExcelConfigData*.json")
    return sorted(glob.glob(pattern))


def _load_talk_excel_perform_cfg_map() -> dict[int, list[str]]:
    mapping: dict[int, list[str]] = {}
    for path in _iter_talk_excel_config_paths():
        try:
            rows = _load_json_file(path)
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            quest_id = row.get("questId")
            perform_cfg = row.get("performCfg")
            if not isinstance(quest_id, int) or quest_id <= 0:
                continue
            if not isinstance(perform_cfg, str) or not perform_cfg:
                continue
            mapping.setdefault(quest_id, []).append(perform_cfg)
    return mapping


def _extract_storyboard_group_talk_ids(obj: dict) -> list[int]:
    if not isinstance(obj, dict):
        return []
    items = obj.get("DGJMIPFDEOF")
    if not isinstance(items, list):
        items = obj.get("talks")
    if not isinstance(items, list):
        return []
    talk_ids: list[int] = []
    seen: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        talk_id = item.get("BLKKAMEMBBJ")
        if not isinstance(talk_id, int) or talk_id <= 0 or talk_id in seen:
            continue
        seen.add(talk_id)
        talk_ids.append(talk_id)
    return talk_ids


def _extract_anecdote_payload(
    row: dict,
    *,
    talk_excel_map: dict[int, list[str]] | None = None,
    missing_group_collector: list[str] | None = None,
    missing_talk_excel_collector: list[str] | None = None,
) -> dict | None:
    if not isinstance(row, dict):
        return None
    anecdote_id = row.get("DBGCFNMLHAJ")
    if not isinstance(anecdote_id, int) or anecdote_id <= 0:
        return None

    title_text_map_hash = _normalize_nonzero_int(row.get("EJMLGHMLPLD"))
    desc_text_map_hash = _normalize_nonzero_int(row.get("JKNBFACAMCF"))
    group_ids = _normalize_talk_ids(row.get("LIIPHELCPKJ"))
    talk_ids: list[int] = []
    seen_talk_ids: set[int] = set()

    storyboard_group_root = os.path.join(DATA_PATH, "BinOutput", "Talk", "StoryboardGroup")
    for group_id in group_ids:
        if talk_excel_map is not None and group_id not in talk_excel_map:
            if missing_talk_excel_collector is not None:
                missing_talk_excel_collector.append(f"{anecdote_id}:{group_id}")
        group_path = os.path.join(storyboard_group_root, f"{group_id}.json")
        if not os.path.isfile(group_path):
            if missing_group_collector is not None:
                missing_group_collector.append(f"{anecdote_id}:{group_id}")
            continue
        try:
            group_obj = _load_json_file(group_path)
        except Exception:
            if missing_group_collector is not None:
                missing_group_collector.append(f"{anecdote_id}:{group_id}")
            continue
        for talk_id in _extract_storyboard_group_talk_ids(group_obj):
            if talk_id in seen_talk_ids:
                continue
            seen_talk_ids.add(talk_id)
            talk_ids.append(talk_id)

    return {
        "quest_id": anecdote_id,
        "title_text_map_hash": title_text_map_hash,
        "desc_text_map_hash": desc_text_map_hash,
        "talk_rows": [(talk_id, None, QUEST_TALK_NORMAL_COOP_ID) for talk_id in sorted(talk_ids)],
        "source_type": SOURCE_TYPE_ANECDOTE,
        "source_code_raw": SOURCE_TYPE_ANECDOTE,
    }


def importAnecdote(
    row: dict,
    *,
    cursor=None,
    talk_excel_map: dict[int, list[str]] | None = None,
    skip_collector: list[str] | None = None,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
    missing_group_collector: list[str] | None = None,
    missing_talk_excel_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
        _ensure_quest_version_tables(cursor)

    payload = _extract_anecdote_payload(
        row,
        talk_excel_map=talk_excel_map,
        missing_group_collector=missing_group_collector,
        missing_talk_excel_collector=missing_talk_excel_collector,
    )
    if payload is None:
        if skip_collector is not None:
            skip_collector.append(str(row.get("DBGCFNMLHAJ", "unknown")))
        if own_cursor:
            cursor.close()
        return None, False

    quest_id = payload["quest_id"]
    title_text_map_hash = payload["title_text_map_hash"]
    desc_text_map_hash = payload["desc_text_map_hash"]
    new_talk_rows = payload["talk_rows"]
    source_type = payload["source_type"]
    source_code_raw = payload["source_code_raw"]

    if title_text_map_hash is None and missing_title_collector is not None:
        missing_title_collector.append(str(quest_id))
    if not new_talk_rows and no_talk_collector is not None:
        no_talk_collector.append(str(quest_id))

    sql1 = _build_quest_upsert_sql(with_created_version=False)
    sql2 = _build_quest_talk_insert_sql()
    new_signature = _build_quest_dialogue_signature(cursor, new_talk_rows)

    existing_quest_row = cursor.execute(
        "SELECT titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw FROM quest WHERE questId=?",
        (quest_id,),
    ).fetchone()
    is_new_quest = existing_quest_row is None
    quest_changed = (
        is_new_quest
        or existing_quest_row[0] != title_text_map_hash
        or existing_quest_row[1] != desc_text_map_hash
        or existing_quest_row[2] is not None
        or existing_quest_row[3] != source_type
        or existing_quest_row[4] != source_code_raw
    )
    old_talk_rows = _fetch_existing_quest_talk_rows(cursor, quest_id)
    talk_links_changed = old_talk_rows != new_talk_rows

    if quest_changed:
        cursor.execute(
            sql1,
            (quest_id, title_text_map_hash, desc_text_map_hash, None, source_type, source_code_raw),
        )
    if talk_links_changed:
        _delete_quest_talk_rows(cursor, quest_id)
        cursor.executemany(
            sql2,
            ((quest_id, talk_id, step_hash, coop_quest_id) for talk_id, step_hash, coop_quest_id in new_talk_rows),
        )

    _upsert_quest_text_signature(cursor, quest_id, title_text_map_hash, new_signature)

    if own_cursor:
        cursor.close()
    return quest_id, is_new_quest


def importAnecdoteForDiff(
    row: dict,
    current_version: str,
    *,
    cursor=None,
    talk_excel_map: dict[int, list[str]] | None = None,
    skip_collector: list[str] | None = None,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
    missing_group_collector: list[str] | None = None,
    missing_talk_excel_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
        _ensure_quest_version_tables(cursor)

    version = current_version or get_current_version()
    get_or_create_version_id(version)

    payload = _extract_anecdote_payload(
        row,
        talk_excel_map=talk_excel_map,
        missing_group_collector=missing_group_collector,
        missing_talk_excel_collector=missing_talk_excel_collector,
    )
    if payload is None:
        if skip_collector is not None:
            skip_collector.append(str(row.get("DBGCFNMLHAJ", "unknown")))
        if own_cursor:
            cursor.close()
        return None, False

    quest_id = payload["quest_id"]
    title_text_map_hash = payload["title_text_map_hash"]
    desc_text_map_hash = payload["desc_text_map_hash"]
    new_talk_rows = payload["talk_rows"]
    source_type = payload["source_type"]
    source_code_raw = payload["source_code_raw"]

    if title_text_map_hash is None and missing_title_collector is not None:
        missing_title_collector.append(str(quest_id))
    if not new_talk_rows and no_talk_collector is not None:
        no_talk_collector.append(str(quest_id))

    sql1 = _build_quest_upsert_sql(with_created_version=True)
    sql2 = _build_quest_talk_insert_sql()
    new_signature = _build_quest_dialogue_signature(cursor, new_talk_rows)
    old_signature_row = cursor.execute(
        "SELECT dialogue_signature FROM quest_text_signature WHERE questId=?",
        (quest_id,),
    ).fetchone()
    dialogue_changed = old_signature_row is None or old_signature_row[0] != new_signature

    title_changed = False
    if title_text_map_hash:
        current_title_content = cursor.execute(
            "SELECT content FROM textMap WHERE hash=? LIMIT 1",
            (title_text_map_hash,),
        ).fetchone()
        old_title_hash_row = cursor.execute(
            "SELECT titleTextMapHash FROM quest WHERE questId=?",
            (quest_id,),
        ).fetchone()
        if old_title_hash_row and old_title_hash_row[0]:
            old_title_content = cursor.execute(
                "SELECT content FROM textMap WHERE hash=? LIMIT 1",
                (old_title_hash_row[0],),
            ).fetchone()
            current_content = current_title_content[0] if current_title_content else None
            old_content = old_title_content[0] if old_title_content else None
            title_changed = current_content != old_content
        else:
            title_changed = True

    text_changed = dialogue_changed or title_changed
    old_quest_meta_row = cursor.execute(
        "SELECT titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw FROM quest WHERE questId=?",
        (quest_id,),
    ).fetchone()
    quest_row_changed = (
        old_quest_meta_row is None
        or old_quest_meta_row[0] != title_text_map_hash
        or old_quest_meta_row[1] != desc_text_map_hash
        or old_quest_meta_row[2] is not None
        or old_quest_meta_row[3] != source_type
        or old_quest_meta_row[4] != source_code_raw
    )
    old_talk_rows = _fetch_existing_quest_talk_rows(cursor, quest_id)
    talk_links_changed = old_talk_rows != new_talk_rows

    old_version_row = cursor.execute(
        "SELECT created_version_id FROM quest WHERE questId=?",
        (quest_id,),
    ).fetchone()
    is_new_quest = old_version_row is None
    old_created_version = old_version_row[0] if old_version_row else None
    created_version = old_created_version
    created_version_changed = should_update_version(old_created_version, created_version, is_created=True)

    if is_new_quest or quest_row_changed or text_changed or talk_links_changed or created_version_changed:
        cursor.execute(
            sql1,
            (
                quest_id,
                title_text_map_hash,
                desc_text_map_hash,
                None,
                source_type,
                source_code_raw,
                created_version,
            ),
        )
    if talk_links_changed:
        _delete_quest_talk_rows(cursor, quest_id)
        cursor.executemany(
            sql2,
            ((quest_id, talk_id, step_hash, coop_quest_id) for talk_id, step_hash, coop_quest_id in new_talk_rows),
        )

    _upsert_quest_text_signature(cursor, quest_id, title_text_map_hash, new_signature)

    if own_cursor:
        cursor.close()
    return quest_id, is_new_quest


def importAllAnecdotes(
    sync_delete: bool = False,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    anecdote_path = os.path.join(DATA_PATH, "ExcelBinOutput", "AnecdoteExcelConfigData.json")
    if not os.path.isfile(anecdote_path):
        cursor.close()
        return {
            "anecdote_row_count": 0,
            "imported_quest_count": 0,
            "new_quest_count": 0,
            "missing_title_count": 0,
            "no_talk_count": 0,
            "missing_group_count": 0,
            "missing_talk_excel_count": 0,
            "hash_map_refreshed_quest_count": 0,
        }

    rows = _load_json_file(anecdote_path)
    if not isinstance(rows, list):
        rows = []
    talk_excel_map = _load_talk_excel_perform_cfg_map()
    imported_quest_ids: set[int] = set()
    new_quest_ids: set[int] = set()
    skipped_rows: list[str] = []
    missing_title_rows: list[str] = []
    no_talk_rows: list[str] = []
    missing_group_rows: list[str] = []
    missing_talk_excel_rows: list[str] = []

    try:
        with LightweightProgress(len(rows), desc="Anecdote rows", unit="rows") as pbar:
            for row in rows:
                quest_id, is_new_quest = importAnecdote(
                    row,
                    cursor=cursor,
                    talk_excel_map=talk_excel_map,
                    skip_collector=skipped_rows,
                    missing_title_collector=missing_title_rows,
                    no_talk_collector=no_talk_rows,
                    missing_group_collector=missing_group_rows,
                    missing_talk_excel_collector=missing_talk_excel_rows,
                )
                if quest_id is not None:
                    imported_quest_ids.add(quest_id)
                if is_new_quest and quest_id is not None:
                    new_quest_ids.add(quest_id)
                pbar.update()

        if sync_delete:
            if imported_quest_ids:
                placeholders = ",".join(["?"] * len(imported_quest_ids))
                params = tuple(imported_quest_ids)
                delete_target_sql = (
                    f"SELECT questId FROM quest WHERE source_type = ? AND questId NOT IN ({placeholders})"
                )
                delete_params = (SOURCE_TYPE_ANECDOTE, *params)
                cursor.execute(f"DELETE FROM questTalk WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_text_signature WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_hash_map WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest WHERE questId IN ({delete_target_sql})", delete_params)
            else:
                cursor.execute("DELETE FROM questTalk WHERE questId IN (SELECT questId FROM quest WHERE source_type = ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_text_signature WHERE questId IN (SELECT questId FROM quest WHERE source_type = ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_hash_map WHERE questId IN (SELECT questId FROM quest WHERE source_type = ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest WHERE source_type = ?", (SOURCE_TYPE_ANECDOTE,))

        if imported_quest_ids:
            refreshed_hash_map_quests = _refresh_quest_hash_map_for_quest_ids(
                cursor,
                imported_quest_ids,
                batch_size=batch_size,
            )
        else:
            refreshed_hash_map_quests = 0

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("anecdote", skipped_rows)
    _print_issue_summary("anecdote missing titleTextMapHash", missing_title_rows)
    _print_issue_summary("anecdote without talk ids", no_talk_rows)
    _print_issue_summary("anecdote missing storyboard group", missing_group_rows)
    _print_issue_summary("anecdote missing talk excel mapping", missing_talk_excel_rows)
    return {
        "anecdote_row_count": len(rows),
        "imported_quest_count": len(imported_quest_ids),
        "new_quest_count": len(new_quest_ids),
        "skipped_row_count": len(skipped_rows),
        "missing_title_count": len(missing_title_rows),
        "no_talk_count": len(no_talk_rows),
        "missing_group_count": len(missing_group_rows),
        "missing_talk_excel_count": len(missing_talk_excel_rows),
        "hash_map_refreshed_quest_count": int(refreshed_hash_map_quests or 0),
    }


def importAllAnecdotesForDiff(
    current_version: str,
    sync_delete: bool = False,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    version = current_version or get_current_version()
    get_or_create_version_id(version)
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    anecdote_path = os.path.join(DATA_PATH, "ExcelBinOutput", "AnecdoteExcelConfigData.json")
    if not os.path.isfile(anecdote_path):
        cursor.close()
        return {
            "anecdote_row_count": 0,
            "imported_quest_count": 0,
            "new_quest_count": 0,
            "missing_title_count": 0,
            "no_talk_count": 0,
            "missing_group_count": 0,
            "missing_talk_excel_count": 0,
            "hash_map_refreshed_quest_count": 0,
        }

    rows = _load_json_file(anecdote_path)
    if not isinstance(rows, list):
        rows = []
    talk_excel_map = _load_talk_excel_perform_cfg_map()
    imported_quest_ids: set[int] = set()
    new_quest_ids: set[int] = set()
    skipped_rows: list[str] = []
    missing_title_rows: list[str] = []
    no_talk_rows: list[str] = []
    missing_group_rows: list[str] = []
    missing_talk_excel_rows: list[str] = []

    try:
        with LightweightProgress(len(rows), desc="Anecdote rows", unit="rows") as pbar:
            for row in rows:
                quest_id, is_new_quest = importAnecdoteForDiff(
                    row,
                    version,
                    cursor=cursor,
                    talk_excel_map=talk_excel_map,
                    skip_collector=skipped_rows,
                    missing_title_collector=missing_title_rows,
                    no_talk_collector=no_talk_rows,
                    missing_group_collector=missing_group_rows,
                    missing_talk_excel_collector=missing_talk_excel_rows,
                )
                if quest_id is not None:
                    imported_quest_ids.add(quest_id)
                if is_new_quest and quest_id is not None:
                    new_quest_ids.add(quest_id)
                pbar.update()

        if sync_delete:
            if imported_quest_ids:
                placeholders = ",".join(["?"] * len(imported_quest_ids))
                params = tuple(imported_quest_ids)
                delete_target_sql = (
                    f"SELECT questId FROM quest WHERE source_type = ? AND questId NOT IN ({placeholders})"
                )
                delete_params = (SOURCE_TYPE_ANECDOTE, *params)
                cursor.execute(f"DELETE FROM questTalk WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_text_signature WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_hash_map WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest WHERE questId IN ({delete_target_sql})", delete_params)
            else:
                cursor.execute("DELETE FROM questTalk WHERE questId IN (SELECT questId FROM quest WHERE source_type = ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_text_signature WHERE questId IN (SELECT questId FROM quest WHERE source_type = ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest_hash_map WHERE questId IN (SELECT questId FROM quest WHERE source_type = ?)", (SOURCE_TYPE_ANECDOTE,))
                cursor.execute("DELETE FROM quest WHERE source_type = ?", (SOURCE_TYPE_ANECDOTE,))

        if imported_quest_ids:
            refreshed_hash_map_quests = _refresh_quest_hash_map_for_quest_ids(
                cursor,
                imported_quest_ids,
                batch_size=batch_size,
            )
            _backfill_quest_created_version_from_textmap(
                cursor,
                quest_ids=imported_quest_ids,
                overwrite_existing=False,
            )
        else:
            refreshed_hash_map_quests = 0

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("anecdote", skipped_rows)
    _print_issue_summary("anecdote missing titleTextMapHash", missing_title_rows)
    _print_issue_summary("anecdote without talk ids", no_talk_rows)
    _print_issue_summary("anecdote missing storyboard group", missing_group_rows)
    _print_issue_summary("anecdote missing talk excel mapping", missing_talk_excel_rows)
    return {
        "anecdote_row_count": len(rows),
        "imported_quest_count": len(imported_quest_ids),
        "new_quest_count": len(new_quest_ids),
        "skipped_row_count": len(skipped_rows),
        "missing_title_count": len(missing_title_rows),
        "no_talk_count": len(no_talk_rows),
        "missing_group_count": len(missing_group_rows),
        "missing_talk_excel_count": len(missing_talk_excel_rows),
        "hash_map_refreshed_quest_count": int(refreshed_hash_map_quests or 0),
    }


def _extract_nested_text_map_hash(node) -> int | None:
    if isinstance(node, int) and node != 0:
        return node
    if not isinstance(node, dict):
        return None
    for key in ("AEMBEELBLML", "textMapHash", "hash"):
        value = node.get(key)
        if isinstance(value, int) and value != 0:
            return value
    return None


def _load_hangout_codex_hashes(quest_id: int) -> tuple[int | None, int | None]:
    for candidate_id in (quest_id, quest_id + 10000):
        path = os.path.join(DATA_PATH, "BinOutput", "CodexQuest", f"{candidate_id}.json")
        if not os.path.isfile(path):
            continue
        try:
            obj = _load_json_file(path)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        title_hash = _extract_nested_text_map_hash(obj.get("HCGANIMKKLM"))
        desc_hash = _extract_nested_text_map_hash(obj.get("NCBJBOHPGNA"))
        if title_hash is not None or desc_hash is not None:
            return title_hash, desc_hash
    return None, None


def _extract_hangout_coop_quest_ids(coop_obj: dict) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    root = coop_obj.get("NPOJHKBJIDO")
    if isinstance(root, dict):
        for raw_key, value in root.items():
            candidates = [raw_key]
            if isinstance(value, dict):
                candidates.append(value.get("BLKKAMEMBBJ"))
            for candidate in candidates:
                try:
                    coop_quest_id = int(candidate)
                except Exception:
                    continue
                if coop_quest_id <= 0 or coop_quest_id in seen:
                    continue
                seen.add(coop_quest_id)
                result.append(coop_quest_id)
    return result


def _collect_hangout_talk_rows(
    main_coop_id: int,
    *,
    missing_coop_collector: list[str] | None = None,
) -> list[tuple[int, int | None, int]]:
    coop_path = os.path.join(DATA_PATH, "BinOutput", "Coop", f"Coop{main_coop_id}.json")
    if not os.path.isfile(coop_path):
        if missing_coop_collector is not None:
            missing_coop_collector.append(str(main_coop_id))
        return []
    try:
        coop_obj = _load_json_file(coop_path)
    except Exception:
        if missing_coop_collector is not None:
            missing_coop_collector.append(str(main_coop_id))
        return []
    if not isinstance(coop_obj, dict):
        if missing_coop_collector is not None:
            missing_coop_collector.append(str(main_coop_id))
        return []

    talk_rows: list[tuple[int, int | None, int]] = []
    seen_pairs: set[tuple[int, int]] = set()
    talk_root = os.path.join(DATA_PATH, "BinOutput", "Talk", "Coop")
    for coop_quest_id in _extract_hangout_coop_quest_ids(coop_obj):
        pattern = os.path.join(talk_root, f"{coop_quest_id}_*.json")
        for talk_path in sorted(glob.glob(pattern)):
            try:
                talk_obj = _load_json_file(talk_path)
            except Exception:
                continue
            if not isinstance(talk_obj, dict):
                continue
            talk_id = talk_obj.get("LBPGKDMGFBN")
            if not isinstance(talk_id, int) or talk_id <= 0:
                continue
            key = (talk_id, coop_quest_id)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            talk_rows.append((talk_id, None, coop_quest_id))
    return _normalize_quest_talk_rows(talk_rows)


def _is_existing_real_hangout_quest(existing_row) -> bool:
    if not existing_row:
        return False
    chapter_id = existing_row[2]
    source_code_raw = existing_row[4]
    return chapter_id is not None or source_code_raw in _BASE_QUEST_SOURCE_TYPES


def _extract_hangout_payload(
    quest_id: int,
    *,
    cursor,
    missing_coop_collector: list[str] | None = None,
) -> dict | None:
    main_coop_ids = _load_main_coop_ids_by_quest_id().get(quest_id)
    if not main_coop_ids:
        return None

    existing_quest_row = cursor.execute(
        "SELECT titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw FROM quest WHERE questId=?",
        (quest_id,),
    ).fetchone()
    is_real_existing_quest = _is_existing_real_hangout_quest(existing_quest_row)
    codex_title_hash, codex_desc_hash = _load_hangout_codex_hashes(quest_id)

    talk_rows: list[tuple[int, int | None, int]] = []
    for main_coop_id in main_coop_ids:
        talk_rows.extend(
            _collect_hangout_talk_rows(
                main_coop_id,
                missing_coop_collector=missing_coop_collector,
            )
        )
    normalized_talk_rows = _normalize_quest_talk_rows(talk_rows)

    if is_real_existing_quest:
        title_text_map_hash = existing_quest_row[0] if existing_quest_row[0] not in (0,) else codex_title_hash
        desc_text_map_hash = existing_quest_row[1] if existing_quest_row[1] not in (0,) else codex_desc_hash
        chapter_id = existing_quest_row[2]
        raw_source = existing_quest_row[4]
        source_code_raw = raw_source if raw_source in _BASE_QUEST_SOURCE_TYPES else SOURCE_TYPE_HANGOUT
    else:
        title_text_map_hash = codex_title_hash
        desc_text_map_hash = codex_desc_hash
        chapter_id = None
        source_code_raw = SOURCE_TYPE_HANGOUT

    return {
        "quest_id": quest_id,
        "title_text_map_hash": title_text_map_hash,
        "desc_text_map_hash": desc_text_map_hash,
        "chapter_id": chapter_id,
        "source_type": SOURCE_TYPE_HANGOUT,
        "source_code_raw": source_code_raw,
        "talk_rows": normalized_talk_rows,
        "is_real_existing_quest": is_real_existing_quest,
        "existing_quest_row": existing_quest_row,
    }


def importHangout(
    quest_id: int,
    *,
    cursor=None,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
    missing_coop_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
        _ensure_quest_version_tables(cursor)

    payload = _extract_hangout_payload(
        quest_id,
        cursor=cursor,
        missing_coop_collector=missing_coop_collector,
    )
    if payload is None:
        if own_cursor:
            cursor.close()
        return None, False

    title_text_map_hash = payload["title_text_map_hash"]
    desc_text_map_hash = payload["desc_text_map_hash"]
    chapter_id = payload["chapter_id"]
    source_type = payload["source_type"]
    source_code_raw = payload["source_code_raw"]
    new_talk_rows = payload["talk_rows"]
    is_real_existing_quest = payload["is_real_existing_quest"]
    existing_quest_row = payload["existing_quest_row"]

    if title_text_map_hash is None and missing_title_collector is not None:
        missing_title_collector.append(str(quest_id))
    if not new_talk_rows and no_talk_collector is not None:
        no_talk_collector.append(str(quest_id))

    sql1 = _build_quest_upsert_sql(with_created_version=False)
    sql2 = _build_quest_talk_insert_sql()
    new_signature = _build_quest_dialogue_signature(cursor, new_talk_rows)

    is_new_quest = existing_quest_row is None
    quest_changed = (
        is_new_quest
        or existing_quest_row[0] != title_text_map_hash
        or existing_quest_row[1] != desc_text_map_hash
        or existing_quest_row[2] != chapter_id
        or existing_quest_row[3] != source_type
        or existing_quest_row[4] != source_code_raw
    )
    compare_scope = QUEST_TALK_SCOPE_COOP if is_real_existing_quest else QUEST_TALK_SCOPE_ALL
    old_talk_rows = _fetch_existing_quest_talk_rows(cursor, quest_id, scope=compare_scope)
    talk_links_changed = old_talk_rows != new_talk_rows

    if quest_changed:
        cursor.execute(
            sql1,
            (quest_id, title_text_map_hash, desc_text_map_hash, chapter_id, source_type, source_code_raw),
        )
    if talk_links_changed:
        _delete_quest_talk_rows(cursor, quest_id, scope=compare_scope)
        cursor.executemany(
            sql2,
            ((quest_id, talk_id, step_hash, coop_quest_id) for talk_id, step_hash, coop_quest_id in new_talk_rows),
        )

    _upsert_quest_text_signature(cursor, quest_id, title_text_map_hash, new_signature)

    if own_cursor:
        cursor.close()
    return quest_id, is_new_quest


def importHangoutForDiff(
    quest_id: int,
    current_version: str,
    *,
    cursor=None,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
    missing_coop_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
        _ensure_quest_version_tables(cursor)

    version = current_version or get_current_version()
    get_or_create_version_id(version)
    payload = _extract_hangout_payload(
        quest_id,
        cursor=cursor,
        missing_coop_collector=missing_coop_collector,
    )
    if payload is None:
        if own_cursor:
            cursor.close()
        return None, False

    title_text_map_hash = payload["title_text_map_hash"]
    desc_text_map_hash = payload["desc_text_map_hash"]
    chapter_id = payload["chapter_id"]
    source_type = payload["source_type"]
    source_code_raw = payload["source_code_raw"]
    new_talk_rows = payload["talk_rows"]
    is_real_existing_quest = payload["is_real_existing_quest"]

    if title_text_map_hash is None and missing_title_collector is not None:
        missing_title_collector.append(str(quest_id))
    if not new_talk_rows and no_talk_collector is not None:
        no_talk_collector.append(str(quest_id))

    sql1 = _build_quest_upsert_sql(with_created_version=True)
    sql2 = _build_quest_talk_insert_sql()
    new_signature = _build_quest_dialogue_signature(cursor, new_talk_rows)
    old_signature_row = cursor.execute(
        "SELECT dialogue_signature FROM quest_text_signature WHERE questId=?",
        (quest_id,),
    ).fetchone()
    dialogue_changed = old_signature_row is None or old_signature_row[0] != new_signature

    title_changed = False
    if title_text_map_hash:
        current_title_content = cursor.execute(
            "SELECT content FROM textMap WHERE hash=? LIMIT 1",
            (title_text_map_hash,),
        ).fetchone()
        old_title_hash_row = cursor.execute(
            "SELECT titleTextMapHash FROM quest WHERE questId=?",
            (quest_id,),
        ).fetchone()
        if old_title_hash_row and old_title_hash_row[0]:
            old_title_content = cursor.execute(
                "SELECT content FROM textMap WHERE hash=? LIMIT 1",
                (old_title_hash_row[0],),
            ).fetchone()
            current_content = current_title_content[0] if current_title_content else None
            old_content = old_title_content[0] if old_title_content else None
            title_changed = current_content != old_content
        else:
            title_changed = True

    text_changed = dialogue_changed or title_changed
    old_quest_meta_row = cursor.execute(
        "SELECT titleTextMapHash, descTextMapHash, chapterId, source_type, source_code_raw FROM quest WHERE questId=?",
        (quest_id,),
    ).fetchone()
    quest_row_changed = (
        old_quest_meta_row is None
        or old_quest_meta_row[0] != title_text_map_hash
        or old_quest_meta_row[1] != desc_text_map_hash
        or old_quest_meta_row[2] != chapter_id
        or old_quest_meta_row[3] != source_type
        or old_quest_meta_row[4] != source_code_raw
    )
    compare_scope = QUEST_TALK_SCOPE_COOP if is_real_existing_quest else QUEST_TALK_SCOPE_ALL
    old_talk_rows = _fetch_existing_quest_talk_rows(cursor, quest_id, scope=compare_scope)
    talk_links_changed = old_talk_rows != new_talk_rows

    old_version_row = cursor.execute(
        "SELECT created_version_id FROM quest WHERE questId=?",
        (quest_id,),
    ).fetchone()
    is_new_quest = old_version_row is None
    old_created_version = old_version_row[0] if old_version_row else None
    created_version = old_created_version
    created_version_changed = should_update_version(old_created_version, created_version, is_created=True)

    if is_new_quest or quest_row_changed or text_changed or talk_links_changed or created_version_changed:
        cursor.execute(
            sql1,
            (quest_id, title_text_map_hash, desc_text_map_hash, chapter_id, source_type, source_code_raw, created_version),
        )
    if talk_links_changed:
        _delete_quest_talk_rows(cursor, quest_id, scope=compare_scope)
        cursor.executemany(
            sql2,
            ((quest_id, talk_id, step_hash, coop_quest_id) for talk_id, step_hash, coop_quest_id in new_talk_rows),
        )

    _upsert_quest_text_signature(cursor, quest_id, title_text_map_hash, new_signature)

    if own_cursor:
        cursor.close()
    return quest_id, is_new_quest


def importAllHangouts(
    sync_delete: bool = False,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    _reset_quest_source_caches()
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    quest_ids = sorted(_load_main_coop_ids_by_quest_id().keys())
    imported_quest_ids: set[int] = set()
    new_quest_ids: set[int] = set()
    missing_title_rows: list[str] = []
    no_talk_rows: list[str] = []
    missing_coop_rows: list[str] = []

    try:
        with LightweightProgress(len(quest_ids), desc="Hangout rows", unit="rows") as pbar:
            for quest_id in quest_ids:
                imported_quest_id, is_new_quest = importHangout(
                    quest_id,
                    cursor=cursor,
                    missing_title_collector=missing_title_rows,
                    no_talk_collector=no_talk_rows,
                    missing_coop_collector=missing_coop_rows,
                )
                if imported_quest_id is not None:
                    imported_quest_ids.add(imported_quest_id)
                if is_new_quest and imported_quest_id is not None:
                    new_quest_ids.add(imported_quest_id)
                pbar.update()

        if sync_delete:
            if imported_quest_ids:
                placeholders = ",".join(["?"] * len(imported_quest_ids))
                delete_target_sql = (
                    "SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ? "
                    f"AND questId NOT IN ({placeholders})"
                )
                delete_params = (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT, *tuple(imported_quest_ids))
                cursor.execute(f"DELETE FROM questTalk WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_text_signature WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_hash_map WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest WHERE questId IN ({delete_target_sql})", delete_params)
            else:
                cursor.execute(
                    "DELETE FROM questTalk WHERE questId IN "
                    "(SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ?)",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )
                cursor.execute(
                    "DELETE FROM quest_text_signature WHERE questId IN "
                    "(SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ?)",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )
                cursor.execute(
                    "DELETE FROM quest_hash_map WHERE questId IN "
                    "(SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ?)",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )
                cursor.execute(
                    "DELETE FROM quest WHERE source_type = ? AND source_code_raw = ?",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )

        if imported_quest_ids:
            refreshed_hash_map_quests = _refresh_quest_hash_map_for_quest_ids(
                cursor,
                imported_quest_ids,
                batch_size=batch_size,
            )
        else:
            refreshed_hash_map_quests = 0

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

    _print_issue_summary("hangout missing titleTextMapHash", missing_title_rows)
    _print_issue_summary("hangout without talk ids", no_talk_rows)
    _print_issue_summary("hangout missing coop cfg", missing_coop_rows)
    return {
        "hangout_row_count": len(quest_ids),
        "imported_quest_count": len(imported_quest_ids),
        "new_quest_count": len(new_quest_ids),
        "missing_title_count": len(missing_title_rows),
        "no_talk_count": len(no_talk_rows),
        "missing_coop_count": len(missing_coop_rows),
        "hash_map_refreshed_quest_count": int(refreshed_hash_map_quests or 0),
    }


def importAllHangoutsForDiff(
    current_version: str,
    sync_delete: bool = False,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    _reset_quest_source_caches()
    version = current_version or get_current_version()
    get_or_create_version_id(version)
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    quest_ids = sorted(_load_main_coop_ids_by_quest_id().keys())
    imported_quest_ids: set[int] = set()
    new_quest_ids: set[int] = set()
    missing_title_rows: list[str] = []
    no_talk_rows: list[str] = []
    missing_coop_rows: list[str] = []

    try:
        with LightweightProgress(len(quest_ids), desc="Hangout rows", unit="rows") as pbar:
            for quest_id in quest_ids:
                imported_quest_id, is_new_quest = importHangoutForDiff(
                    quest_id,
                    version,
                    cursor=cursor,
                    missing_title_collector=missing_title_rows,
                    no_talk_collector=no_talk_rows,
                    missing_coop_collector=missing_coop_rows,
                )
                if imported_quest_id is not None:
                    imported_quest_ids.add(imported_quest_id)
                if is_new_quest and imported_quest_id is not None:
                    new_quest_ids.add(imported_quest_id)
                pbar.update()

        if sync_delete:
            if imported_quest_ids:
                placeholders = ",".join(["?"] * len(imported_quest_ids))
                delete_target_sql = (
                    "SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ? "
                    f"AND questId NOT IN ({placeholders})"
                )
                delete_params = (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT, *tuple(imported_quest_ids))
                cursor.execute(f"DELETE FROM questTalk WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_text_signature WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest_hash_map WHERE questId IN ({delete_target_sql})", delete_params)
                cursor.execute(f"DELETE FROM quest WHERE questId IN ({delete_target_sql})", delete_params)
            else:
                cursor.execute(
                    "DELETE FROM questTalk WHERE questId IN "
                    "(SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ?)",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )
                cursor.execute(
                    "DELETE FROM quest_text_signature WHERE questId IN "
                    "(SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ?)",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )
                cursor.execute(
                    "DELETE FROM quest_hash_map WHERE questId IN "
                    "(SELECT questId FROM quest WHERE source_type = ? AND source_code_raw = ?)",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )
                cursor.execute(
                    "DELETE FROM quest WHERE source_type = ? AND source_code_raw = ?",
                    (SOURCE_TYPE_HANGOUT, SOURCE_TYPE_HANGOUT),
                )

        if imported_quest_ids:
            refreshed_hash_map_quests = _refresh_quest_hash_map_for_quest_ids(
                cursor,
                imported_quest_ids,
                batch_size=batch_size,
            )
            _backfill_quest_created_version_from_textmap(
                cursor,
                quest_ids=imported_quest_ids,
                overwrite_existing=False,
            )
        else:
            refreshed_hash_map_quests = 0

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()

    _print_issue_summary("hangout missing titleTextMapHash", missing_title_rows)
    _print_issue_summary("hangout without talk ids", no_talk_rows)
    _print_issue_summary("hangout missing coop cfg", missing_coop_rows)
    return {
        "hangout_row_count": len(quest_ids),
        "imported_quest_count": len(imported_quest_ids),
        "new_quest_count": len(new_quest_ids),
        "missing_title_count": len(missing_title_rows),
        "no_talk_count": len(no_talk_rows),
        "missing_coop_count": len(missing_coop_rows),
        "hash_map_refreshed_quest_count": int(refreshed_hash_map_quests or 0),
    }


def importTalk(
    fileName: str,
    *,
    cursor=None,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    refresh_hash_map: bool = True,
    touched_talk_collector: set[int] | None = None,
) -> int:
    own_cursor = cursor is None
    if own_cursor:
        cursor = conn.cursor()
    obj = _load_json_file(os.path.join(DATA_PATH, "BinOutput", "Talk", fileName))
    if _is_non_dialog_talk_obj(obj):
        if own_cursor:
            cursor.close()
        return 0

    if "talkId" in obj:
        talkIdKey = "talkId"
        dialogueListKey = "dialogList"
        dialogueIdKey = "id"
        talkRoleKey = "talkRole"
        talkRoleTypeKey = "type"
        talkRoleIdKey = "_id"
        talkContentTextMapHashKey = "talkContentTextMapHash"
    elif "ADHLLDAPKCM" in obj:
        talkIdKey = "ADHLLDAPKCM"
        dialogueListKey = "MOEOFGCKILF"
        dialogueIdKey = "ILHDNJDDEOP"
        talkRoleKey = "LCECPDILLEE"
        talkRoleTypeKey = "_type"
        talkRoleIdKey = "_id"
        talkContentTextMapHashKey = "GABLFFECBDO"
    elif "FEOACBMDCKJ" in obj and "AAOAAFLLOJI" in obj:
        talkIdKey = "FEOACBMDCKJ"
        dialogueListKey = "AAOAAFLLOJI"
        dialogueIdKey = "CCFPGAKINNB"
        talkRoleKey = "HJLEMJIGNFE"
        talkRoleTypeKey = "type"
        talkRoleIdKey = "id"
        talkContentTextMapHashKey = "BDOKCLNNDGN"
    elif "LBPGKDMGFBN" in obj and "LOJEOMAPIIM" in obj:
        talkIdKey = "LBPGKDMGFBN"
        dialogueListKey = "LOJEOMAPIIM"
        dialogueIdKey = "BLKKAMEMBBJ"
        talkRoleKey = "HJIPOJOECIF"
        talkRoleTypeKey = "_type"
        talkRoleIdKey = "_id"
        talkContentTextMapHashKey = "CMKPOJOEHHA"
    else:
        if skip_collector is not None:
            skip_collector.append(fileName)
        elif log_skip:
            print("Skipping " + fileName)
        if own_cursor:
            cursor.close()
        return 0

    talkId = obj[talkIdKey]
    if dialogueListKey not in obj or len(obj[dialogueListKey]) == 0:
        if own_cursor:
            cursor.close()
        return 0

    sql = (
        "INSERT INTO dialogue(dialogueId, talkerId, talkerType, talkId, textHash, coopQuestId) "
        "VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(dialogueId) DO UPDATE SET "
        "talkerId=excluded.talkerId, "
        "talkerType=excluded.talkerType, "
        "talkId=excluded.talkId, "
        "textHash=excluded.textHash, "
        "coopQuestId=excluded.coopQuestId "
        "WHERE "
        "NOT (dialogue.talkerId IS excluded.talkerId) "
        "OR NOT (dialogue.talkerType IS excluded.talkerType) "
        "OR NOT (dialogue.talkId IS excluded.talkId) "
        "OR NOT (dialogue.textHash IS excluded.textHash) "
        "OR NOT (dialogue.coopQuestId IS excluded.coopQuestId)"
    )

    coopMatch = re.match(r"^Coop[\\,/]([0-9]+)_[0-9]+.json$", fileName)
    if coopMatch:
        coopQuestId = coopMatch.group(1)
    else:
        coopQuestId = None

    rows = []
    for dialogue in obj[dialogueListKey]:
        dialogueId = dialogue.get(dialogueIdKey)
        if dialogueId is None:
            continue
        if talkRoleKey in dialogue and talkRoleIdKey in dialogue[talkRoleKey] and talkRoleTypeKey in dialogue[talkRoleKey]:
            talkRoleId = dialogue[talkRoleKey][talkRoleIdKey]
            talkRoleType = dialogue[talkRoleKey][talkRoleTypeKey]
        else:
            talkRoleId = -1
            talkRoleType = None

        if talkContentTextMapHashKey not in dialogue:
            continue
        textHash = dialogue[talkContentTextMapHashKey]
        rows.append((dialogueId, talkRoleId, talkRoleType, talkId, textHash, coopQuestId))

    if rows:
        executemany_batched(cursor, sql, rows, batch_size=batch_size)
    if touched_talk_collector is not None:
        try:
            touched_talk_collector.add(int(talkId))
        except Exception:
            pass
    if refresh_hash_map:
        _refresh_quest_hash_map_for_talk_ids(cursor, [talkId], batch_size=batch_size)

    if own_cursor:
        cursor.close()
        if commit:
            conn.commit()
    return len(rows)


def _is_non_dialog_talk_obj(obj: dict) -> bool:
    if not isinstance(obj, dict):
        return False
    keys = set(obj.keys())

    if keys == {"activityId", "talks"}:
        return True
    if "talks" in obj and isinstance(obj.get("talks"), list):
        return True

    if "DGJMIPFDEOF" in obj and isinstance(obj.get("DGJMIPFDEOF"), list):
        if (
            "CAKFHGJGEEK" in obj
            or "BLPHCANGKPL" in obj
            or "EOFLGOBJBCG" in obj
            or "configId" in obj
            or "groupId" in obj
            or "npcId" in obj
        ):
            return True

    if "DLPKMDPABFM" in obj and "LBPGKDMGFBN" in obj:
        if not isinstance(obj.get("LOJEOMAPIIM"), list):
            return True

    if "AFKIEPNELHE" in obj and "IKCBIFLCCOH" in obj and "PDFCHAAMEHA" in obj:
        return True
    if "AFNAKLCPGNF" in obj and "speed" in obj and "maxSpeed" in obj:
        return True
    if "FDAAMLIPKAK" in obj and "reApplyModifierOnStateChange" in obj:
        return True

    return False


def importAllTalkItems(
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    talk_root = os.path.join(DATA_PATH, "BinOutput", "Talk")
    if not os.path.isdir(talk_root):
        print("Talk folder not found, skipping.")
        return 0

    imported_rows = 0
    talk_files: list[str] = []
    skipped_files: list[str] = []
    touched_talk_ids: set[int] = set()

    # Collect subfolders first so traversal order stays stable across runs.
    folders = sorted(os.listdir(talk_root))
    for folder in folders:
        folder_path = os.path.join(talk_root, folder)
        if not os.path.isdir(folder_path):
            continue
        for file_name in sorted(os.listdir(folder_path)):
            file_path = os.path.join(folder_path, file_name)
            if os.path.isfile(file_path):
                talk_files.append(folder + "\\" + file_name)

    print(f"importing talk files ({len(talk_files)})")
    cursor = conn.cursor()

    # 謇ｹ驥丞､・炊莨伜喧・壼㍼蟆台ｺ句苅謠蝉ｺ､谺｡謨ｰ
    try:
        with LightweightProgress(len(talk_files), desc="Talk files", unit="files") as pbar:
            for file_name in talk_files:
                imported_rows += importTalk(
                    file_name,
                    cursor=cursor,
                    commit=False,
                    batch_size=batch_size,
                    skip_collector=skipped_files,
                    log_skip=False,
                    refresh_hash_map=False,
                    touched_talk_collector=touched_talk_ids,
                )
                pbar.update()

        # 謇ｹ驥乗峩譁ｰ蜩亥ｸ梧丐蟆・ｼ悟㍼蟆第焚謐ｮ蠎捺桃菴・        if touched_talk_ids:
            _refresh_quest_hash_map_for_talk_ids(
                cursor,
                touched_talk_ids,
                batch_size=batch_size,
            )

        if commit:
            conn.commit()
    except Exception as e:
        print(f"Error in importAllTalkItems: {e}")
        if commit:
            conn.rollback()
        raise
    finally:
        cursor.close()

    _print_skip_summary("talk", skipped_files)
    return imported_rows


def importQuestBriefs(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    folder = os.path.join(DATA_PATH, "BinOutput", "QuestBrief")
    if not os.path.isdir(folder):
        print("QuestBrief folder not found, skipping.")
        cursor.close()
        return

    sql = _build_quest_talk_insert_sql(upsert_step_title=True)
    files = os.listdir(folder)
    touched_quest_ids: set[int] = set()

    def _iter_rows():
        with LightweightProgress(len(files), desc="QuestBrief files", unit="files") as pbar:
            for i, fileName in enumerate(files):
                if not fileName.endswith(".json"):
                    pbar.update()
                    continue
                try:
                    obj = _load_json_file(os.path.join(folder, fileName))
                except Exception:
                    pbar.update()
                    continue

                questId = extract_quest_id(obj)
                for mainQuestId, talkId, step_hash, coop_quest_id in _iter_subquest_talk_rows(obj, questId):
                    try:
                        touched_quest_ids.add(int(mainQuestId))
                    except Exception:
                        pass
                    yield (mainQuestId, talkId, step_hash, coop_quest_id)
                pbar.update()

    executemany_batched(cursor, sql, _iter_rows(), batch_size=batch_size)
    _refresh_quest_hash_map_for_quest_ids(
        cursor,
        touched_quest_ids,
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()


def refreshQuestHashMapByTalkIds(
    talk_ids,
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    refreshed = _refresh_quest_hash_map_for_talk_ids(
        cursor,
        talk_ids,
        batch_size=batch_size,
    )
    cursor.close()
    if commit:
        conn.commit()
    return int(refreshed or 0)


def refreshQuestHashMapByQuestIds(
    quest_ids,
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    cursor = conn.cursor()
    _ensure_quest_version_tables(cursor)
    refreshed = _refresh_quest_hash_map_for_quest_ids(
        cursor,
        quest_ids,
        batch_size=batch_size,
    )
    cursor.close()
    if commit:
        conn.commit()
    return int(refreshed or 0)


def runQuestOnly(
    *,
    prune_missing: bool = True,
    include_quests: bool = True,
    include_talks: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    talk_rows = 0
    if include_talks:
        talk_rows = importAllTalkItems(commit=True, batch_size=batch_size)

    if include_quests:
        quest_stats = importAllQuests(
            sync_delete=prune_missing,
            batch_size=batch_size,
        )
        importQuestBriefs(commit=True, batch_size=batch_size)
        hangout_stats = importAllHangouts(
            sync_delete=prune_missing,
            batch_size=batch_size,
        )
        anecdote_stats = importAllAnecdotes(
            sync_delete=prune_missing,
            batch_size=batch_size,
        )
    else:
        quest_stats = {
            "files_total": 0,
            "imported_quest_count": 0,
            "new_quest_count": 0,
            "skipped_file_count": 0,
            "skipped_file_samples": [],
            "missing_title_count": 0,
            "no_talk_count": 0,
        }
        hangout_stats = {
            "hangout_row_count": 0,
            "imported_quest_count": 0,
            "new_quest_count": 0,
            "missing_title_count": 0,
            "no_talk_count": 0,
            "missing_coop_count": 0,
        }
        anecdote_stats = {
            "anecdote_row_count": 0,
            "imported_quest_count": 0,
            "new_quest_count": 0,
            "missing_title_count": 0,
            "no_talk_count": 0,
        }

    result = dict(quest_stats or {})
    result["hangout_row_count"] = int(hangout_stats.get("hangout_row_count", 0) or 0)
    result["hangout_imported_count"] = int(hangout_stats.get("imported_quest_count", 0) or 0)
    result["hangout_new_count"] = int(hangout_stats.get("new_quest_count", 0) or 0)
    result["anecdote_row_count"] = int(anecdote_stats.get("anecdote_row_count", 0) or 0)
    result["anecdote_imported_count"] = int(anecdote_stats.get("imported_quest_count", 0) or 0)
    result["anecdote_new_count"] = int(anecdote_stats.get("new_quest_count", 0) or 0)
    result["talk_rows_imported"] = int(talk_rows or 0)
    result["quests_processed"] = bool(include_quests)
    result["talks_processed"] = bool(include_talks)
    return result

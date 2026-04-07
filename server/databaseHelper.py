import json
import sqlite3
import threading
from contextlib import closing
import re
import os
from pathlib import Path

import config
import fts_tokenizer


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_fts_tokenizer(tokenizer: str | None) -> str:
    text = str(tokenizer or "").strip()
    return text or "trigram"


def _resolve_fts_settings() -> tuple[str, str, str]:
    tokenizer = _normalize_fts_tokenizer(
        os.environ.get("GTS_FTS_TOKENIZER") or config.getFtsTokenizer()
    )
    ext_path = (
        os.environ.get("GTS_FTS_EXTENSION_PATH")
        or config.getFtsExtensionPath()
        or ""
    ).strip()
    ext_entry = (
        os.environ.get("GTS_FTS_EXTENSION_ENTRY")
        or config.getFtsExtensionEntry()
        or ""
    ).strip()
    return tokenizer, ext_path, ext_entry


def _resolve_fts_chinese_segmenter_settings() -> tuple[str, str]:
    segmenter = (
        os.environ.get("GTS_FTS_CHINESE_SEGMENTER")
        or config.getFtsChineseSegmenter()
        or "auto"
    )
    user_dict = (
        os.environ.get("GTS_FTS_JIEBA_USER_DICT")
        or config.getFtsJiebaUserDict()
        or ""
    )
    return str(segmenter).strip().lower(), str(user_dict).strip()


def _register_fts_content_function(connection: sqlite3.Connection, tokenizer: str) -> None:
    tokenizer_name = tokenizer.split()[0] if tokenizer else "trigram"
    segmenter, user_dict = _resolve_fts_chinese_segmenter_settings()

    def _fts_content(lang_code, content):
        try:
            lang_value = int(lang_code)
        except Exception:
            lang_value = 0
        return fts_tokenizer.build_fts_index_text(
            str(content or ""),
            lang_value,
            tokenizer_name,
            segmenter_mode=segmenter,
            user_dict_path=user_dict,
        )

    try:
        connection.create_function("gts_fts_content", 2, _fts_content)
    except Exception:
        pass


def _try_load_fts_extension(
    connection: sqlite3.Connection,
    extension_path: str,
    extension_entry: str,
) -> bool:
    if not extension_path:
        return False
    try:
        connection.enable_load_extension(True)
    except Exception:
        return False

    try:
        # SQLite的load_extension方法只接受一个参数
        connection.load_extension(extension_path)
        return True
    except Exception:
        return False
    finally:
        try:
            connection.enable_load_extension(False)
        except Exception:
            pass


def _apply_connection_pragmas(connection: sqlite3.Connection) -> None:
    # Favor read-heavy query latency for local desktop usage.
    pragmas = (
        "PRAGMA temp_store=MEMORY",  # Store temporary tables in memory.
        "PRAGMA cache_size=-262144",  # Reserve about 256 MiB of page cache.
        "PRAGMA mmap_size=2147483648",  # Allow up to 2 GiB of memory-mapped I/O.
        "PRAGMA synchronous=NORMAL",  # Balance durability with write latency.
        "PRAGMA journal_mode=WAL",  # Keep WAL enabled for safer concurrent reads.
        "PRAGMA foreign_keys=ON",  # Enforce foreign keys when schemas use them.
        "PRAGMA optimize",  # Let SQLite refresh planner statistics opportunistically.
    )
    with closing(connection.cursor()) as cursor:
        for pragma in pragmas:
            try:
                cursor.execute(pragma)
            except sqlite3.DatabaseError:
                continue


def _ensure_runtime_query_indexes(connection: sqlite3.Connection) -> None:
    index_sqls = (
        "CREATE INDEX IF NOT EXISTS dialogue_talkerType_talkerId_talkId_coopQuestId_dialogueId_index "
        "ON dialogue(talkerType, talkerId, talkId, coopQuestId, dialogueId)",
        "CREATE INDEX IF NOT EXISTS dialogue_talkId_coopQuestId_dialogueId_index "
        "ON dialogue(talkId, coopQuestId, dialogueId)",
    )
    with closing(connection.cursor()) as cursor:
        for sql in index_sqls:
            try:
                cursor.execute(sql)
            except sqlite3.DatabaseError:
                continue
    try:
        connection.commit()
    except sqlite3.DatabaseError:
        pass


def _configure_connection(connection: sqlite3.Connection) -> None:
    """Register FTS helpers and apply default runtime PRAGMAs."""
    tokenizer, ext_path, ext_entry = _resolve_fts_settings()
    _register_fts_content_function(connection, tokenizer)
    _try_load_fts_extension(connection, ext_path, ext_entry)
    _apply_connection_pragmas(connection)
    _ensure_runtime_query_indexes(connection)


def get_connection() -> sqlite3.Connection:
    """
    获取数据库连接
    配置连接参数并返回一个sqlite3.Connection对象
    """
    db_path: Path = config.get_db_path()
    if not db_path.exists():
        # 数据库文件不存在
        raise FileNotFoundError(
            f"Database file not found: {db_path}. "
            "Please place data.db in the server folder."
        )

    connection = sqlite3.connect(str(db_path), check_same_thread=False)
    _configure_connection(connection)
    return connection


# 全局数据库连接
conn = get_connection()

# 缓存字典
_CACHE: dict[str, dict] = {
    "column": {},  # 表列信息缓存
    "fts": {  # FTS相关缓存
        "available": None,
        "tokenizer": None,
        "langs": None
    },
    "version": {},  # 版本相关缓存
    "names": {  # 名称缓存
        "characters": {},  # 角色名称缓存
        "wander": {},  # 旅行者名称缓存
        "traveller": {}  # 空旅行者名称缓存
    }
}


_RAW_CHARACTER_NAME_CACHE: dict[str, str | None] = {}
_FETTER_VOICE_TABLE = "fetterVoice"
_FETTER_VOICE_SYNC_LOCK = threading.Lock()
_FETTER_VOICE_SYNC_ATTEMPTED = False
_FETTER_AVATAR_MAPPINGS: dict[str, int] | None = None
_ANIME_GAME_DATA_JSON_CACHE: dict[str, object | None] = {}
_MAIN_QUEST_ROWS_BY_ID: dict[int, dict] | None = None
_QUEST_STEP_ROWS_BY_MAIN_ID: dict[int, list[dict]] | None = None
_QUEST_STEP_TALK_MAP_CACHE: dict[tuple[int, int], dict[int, str]] = {}


def _get_gender_cache_key() -> str:
    gender = config.getIsMale()
    return str(gender)


def _normalize_output_text(text: str | None, langCode: int):
    if text is None:
        return None
    import placeholderHandler

    return placeholderHandler.replace(text, config.getIsMale(), langCode)


def getCharacterNameRaw(avatarId: int, langCode: int = 1):
    cache_key = f"{avatarId}:{langCode}"
    if cache_key in _RAW_CHARACTER_NAME_CACHE:
        return _RAW_CHARACTER_NAME_CACHE[cache_key]
    with closing(conn.cursor()) as cursor:
        sql = "select content from avatar, textMap where avatarId=? and avatar.nameTextMapHash=textMap.hash and lang=?"
        cursor.execute(sql, (avatarId, langCode))
        rows = cursor.fetchall()
        result = rows[0][0] if rows else None
        _RAW_CHARACTER_NAME_CACHE[cache_key] = result
        return result


def _table_has_column(table_name: str, column_name: str) -> bool:
    """
    检查表是否包含指定列
    """
    key = f"{table_name}:{column_name}"
    if key in _CACHE["column"]:
        return _CACHE["column"][key]
    with closing(conn.cursor()) as cursor:
        try:
            rows = cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
        except Exception:
            _CACHE["column"][key] = False
            return False
    exists = any(row[1] == column_name for row in rows)
    _CACHE["column"][key] = exists
    return exists


# 版本处理相关常量
_VERSION_CATALOG_TABLE = "version_catalog"
_VERSION_DIM_TABLE = "version_dim"
_VERSION_SOURCE_TABLES = ("textMap", "quest", "subtitle", "readable", "npc")
_VERSION_TAG_RE = re.compile(r"(\d+)\.(\d+)(?:\.\d+)?")


# 版本处理相关函数
def _extract_version_tag(raw_version: str | None) -> str | None:
    """
    从版本字符串中提取版本标签
    """
    if raw_version is None:
        return None
    text = str(raw_version).strip()
    if not text:
        return None
    matches = _VERSION_TAG_RE.findall(text)
    if not matches:
        return None
    major, minor = matches[-1]
    return f"{major}.{minor}"


def _normalize_version_filter(value: str | None) -> str | None:
    """
    标准化版本过滤器值，提取主要版本号和次要版本号
    """
    return _extract_version_tag(value)


def _has_version_id_columns(table_name: str) -> bool:
    """
    检查表是否有版本ID列
    """
    if table_name == 'quest':
        # For quest table, only check created_version_id since updated_version_id is in quest_version
        return _table_has_column(table_name, "created_version_id")
    if table_name == 'npc':
        return _table_has_column(table_name, "created_version_id")
    return (
        _table_has_column(table_name, "created_version_id")
        and _table_has_column(table_name, "updated_version_id")
    )


def _has_version_dim() -> bool:
    """
    检查版本维度表是否存在
    """
    return _table_has_column(_VERSION_DIM_TABLE, "id") and _table_has_column(
        _VERSION_DIM_TABLE, "raw_version"
    )


def _version_value_expr(table_alias: str, prefix: str, table_name: str | None = None) -> str:
    """
    构建版本值表达式
    """
    version_id_col = f"{prefix}_version_id"
    has_id_mode = bool(table_name and _has_version_id_columns(table_name) and _has_version_dim())
    if has_id_mode:
        return (
            f"(SELECT raw_version FROM {_VERSION_DIM_TABLE} vd "
            f"WHERE vd.id = {table_alias}.{version_id_col})"
        )
    return "NULL"


def _version_select_expr(table_alias: str, table_name: str | None = None, lang_code: int | None = None) -> str:
    """
    构建版本选择表达式
    """
    if not table_name:
        return "NULL as created_version, NULL as updated_version"
    if table_name == 'quest':
        # For quest table, only check created_version_id since updated_version_id is in quest_version
        if not _table_has_column(table_name, "created_version_id"):
            return "NULL as created_version, NULL as updated_version"
        created_expr = _version_value_expr(table_alias, "created", table_name)
        if lang_code is not None:
            # For quest table, updated version is in quest_version table with language filter
            updated_expr = f"(SELECT vd.raw_version FROM version_dim vd JOIN quest_version qv ON vd.id = qv.updated_version_id WHERE qv.questId = {table_alias}.questId AND qv.lang = {lang_code} LIMIT 1)"
        else:
            updated_expr = "NULL"
        return f"{created_expr} as created_version, {updated_expr} as updated_version"
    if table_name == 'npc':
        if not _table_has_column(table_name, "created_version_id"):
            return "NULL as created_version, NULL as updated_version"
        created_expr = _version_value_expr(table_alias, "created", table_name)
        return f"{created_expr} as created_version, NULL as updated_version"
    if not _has_version_id_columns(table_name):
        return "NULL as created_version, NULL as updated_version"
    created_expr = _version_value_expr(table_alias, "created", table_name)
    updated_expr = _version_value_expr(table_alias, "updated", table_name)
    return f"{created_expr} as created_version, {updated_expr} as updated_version"


def _ensure_version_catalog_schema(cursor: sqlite3.Cursor):
    """
    确保版本目录表结构存在
    """
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_VERSION_CATALOG_TABLE} (
            source_table TEXT NOT NULL,
            raw_version TEXT NOT NULL,
            version_tag TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (source_table, raw_version)
        )
        """
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {_VERSION_CATALOG_TABLE}_source_version_tag_index "
        f"ON {_VERSION_CATALOG_TABLE}(source_table, version_tag)"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS {_VERSION_CATALOG_TABLE}_version_tag_index "
        f"ON {_VERSION_CATALOG_TABLE}(version_tag)"
    )


def _rebuild_version_catalog(cursor: sqlite3.Cursor, source_tables: tuple[str, ...] = _VERSION_SOURCE_TABLES):
    """
    重建版本目录
    """
    for table_name in source_tables:
        cursor.execute(
            f"DELETE FROM {_VERSION_CATALOG_TABLE} WHERE source_table=?",
            (table_name,),
        )
        if not _has_version_id_columns(table_name):
            continue
        query_parts: list[str] = []
        if _has_version_id_columns(table_name) and _has_version_dim():
            query_parts.extend(
                [
                    f"SELECT vd.raw_version AS v FROM {table_name} t "
                    f"JOIN {_VERSION_DIM_TABLE} vd ON vd.id = t.created_version_id "
                    f"WHERE t.created_version_id IS NOT NULL",
                ]
            )
        if table_name == 'quest' and _has_version_id_columns(table_name) and _has_version_dim():
            if _table_has_column("quest_version", "updated_version_id"):
                query_parts.extend(
                    [
                        f"SELECT vd.raw_version AS v FROM quest_version qv "
                        f"JOIN {_VERSION_DIM_TABLE} vd ON vd.id = qv.updated_version_id "
                        f"WHERE qv.updated_version_id IS NOT NULL",
                    ]
                )
        elif table_name != 'npc' and _has_version_id_columns(table_name) and _has_version_dim():
            # For non-quest, non-npc tables, also include updated_version_id.
            query_parts.extend(
                [
                    f"SELECT vd.raw_version AS v FROM {table_name} t "
                    f"JOIN {_VERSION_DIM_TABLE} vd ON vd.id = t.updated_version_id "
                    f"WHERE t.updated_version_id IS NOT NULL",
                ]
            )
        if not query_parts:
            continue
        union_sql = " UNION ".join(query_parts)
        rows = cursor.execute(
            f"SELECT DISTINCT v FROM ({union_sql})"
        ).fetchall()

        payload: list[tuple[str, str, str | None]] = []
        for (raw_value,) in rows:
            if raw_value is None:
                continue
            text = str(raw_value).strip()
            if not text:
                continue
            payload.append((table_name, text, _extract_version_tag(text)))
        if payload:
            cursor.executemany(
                f"""
                INSERT OR REPLACE INTO {_VERSION_CATALOG_TABLE}
                (source_table, raw_version, version_tag, updated_at)
                VALUES (?, ?, ?, datetime('now'))
                """,
                payload,
            )


def _escape_like(value: str) -> str:
    return (value
            .replace("\\", "\\\\")
            .replace("%", r"\%")
            .replace("_", r"\_"))


CHINESE_LANG_CODES = {1, 2}
_TEXTMAP_FTS_TABLE = "textMap_fts"
_TEXTMAP_FTS_AVAILABLE: bool | None = None
_TEXTMAP_FTS_TOKENIZER: str | None = None
_TEXTMAP_FTS_LANGS: set[int] | None = None
_READABLE_LANG_PATTERN = re.compile(r"^Text(?:Map)?([A-Za-z0-9_]+)\.json$", re.IGNORECASE)
_SUBTITLE_LANG_SUFFIX_BY_ID = {
    1: "CHS",
    2: "CHT",
    3: "DE",
    4: "EN",
    5: "ES",
    6: "FR",
    7: "ID",
    8: "IT",
    9: "JP",
    10: "KR",
    11: "PT",
    12: "RU",
    13: "TH",
    14: "TR",
    15: "VI",
}
_SUBTITLE_LANG_SUFFIX_SET = set(_SUBTITLE_LANG_SUFFIX_BY_ID.values())


def _has_textmap_fts() -> bool:
    """
    检查是否启用了文本映射的全文搜索
    """
    if not config.getEnableTextMapFts():
        _CACHE["fts"]["available"] = False
        return False
    if _CACHE["fts"]["available"] is not None:
        return _CACHE["fts"]["available"]
    with closing(conn.cursor()) as cursor:
        row = cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (_TEXTMAP_FTS_TABLE,),
        ).fetchone()
    _CACHE["fts"]["available"] = row is not None
    if row and row[0]:
        m = re.search(r"tokenize='([^']+)'", str(row[0]))
        if m:
            token_spec = _normalize_fts_tokenizer(m.group(1))
            _CACHE["fts"]["tokenizer"] = token_spec.split()[0] if token_spec else "trigram"
    return _CACHE["fts"]["available"]


def _get_textmap_fts_tokenizer() -> str:
    """
    获取文本映射的全文搜索分词器
    """
    if _CACHE["fts"]["tokenizer"]:
        return _CACHE["fts"]["tokenizer"]
    token_spec = _resolve_fts_settings()[0]
    _CACHE["fts"]["tokenizer"] = token_spec.split()[0] if token_spec else "trigram"
    return _CACHE["fts"]["tokenizer"]


def _get_textmap_fts_langs() -> set[int]:
    """
    获取启用了全文搜索的语言代码
    """
    if _CACHE["fts"]["langs"] is not None:
        return _CACHE["fts"]["langs"]

    langs: set[int] = set()
    with closing(conn.cursor()) as cursor:
        try:
            row = cursor.execute(
                "SELECT v FROM app_meta WHERE k='textmap_fts_langs' LIMIT 1"
            ).fetchone()
        except Exception:
            row = None
    if row and row[0]:
        for part in str(row[0]).split(","):
            part = part.strip()
            if not part:
                continue
            try:
                langs.add(int(part))
            except Exception:
                continue
    if not langs:
        langs = set(config.getFtsLangAllowList())
    _CACHE["fts"]["langs"] = langs
    return _CACHE["fts"]["langs"]


def _is_textmap_fts_lang_enabled(lang_code: int) -> bool:
    if not _has_textmap_fts():
        return False
    return lang_code in _get_textmap_fts_langs()


def _resolve_fts_query_filters() -> tuple[set[str], int, int]:
    stopwords_text = os.environ.get("GTS_FTS_STOPWORDS")
    if stopwords_text is None:
        raw_words = config.getFtsStopwords()
    else:
        raw_words = [part.strip() for part in stopwords_text.split(",")]
    stopwords = {w.lower() for w in raw_words if w and str(w).strip()}

    def _read_int(name: str, default_value: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return default_value
        try:
            return int(raw)
        except Exception:
            return default_value

    min_len = max(1, _read_int("GTS_FTS_MIN_TOKEN_LENGTH", config.getFtsMinTokenLength()))
    max_len = max(min_len, _read_int("GTS_FTS_MAX_TOKEN_LENGTH", config.getFtsMaxTokenLength()))
    return stopwords, min_len, max_len


def _build_textmap_fts_match(keyword: str, lang_code: int) -> str | None:
    text = (keyword or "").strip()
    if not text:
        return None

    tokenizer = _get_textmap_fts_tokenizer()
    if lang_code in CHINESE_LANG_CODES:
        text = "".join(text.split())
    else:
        text = re.sub(r"\s+", " ", text)

    stopwords, min_len, max_len = _resolve_fts_query_filters()
    segmenter, user_dict = _resolve_fts_chinese_segmenter_settings()

    if tokenizer == "trigram" and len(text) < 3:
        # trigram tokenizer has poor recall on very short terms
        return None

    # When using word tokenizers (e.g. unicode61), keep non-Chinese
    # languages on LIKE path to preserve substring-match behavior.
    if tokenizer != "trigram" and lang_code not in CHINESE_LANG_CODES:
        return None

    if tokenizer == "trigram":
        text_len = len(text)
        if text_len < min_len or text_len > max_len:
            return None
        if text.lower() in stopwords:
            return None
        escaped = text.replace('"', '""')
        return f"\"{escaped}\""

    parts = fts_tokenizer.build_fts_query_terms(
        text,
        lang_code,
        tokenizer,
        segmenter_mode=segmenter,
        user_dict_path=user_dict,
    )
    if not parts:
        parts = [text]

    dedup_parts = []
    seen = set()
    for part in parts:
        if part in seen:
            continue
        seen.add(part)
        dedup_parts.append(part)
    parts = dedup_parts

    filtered_parts = []
    for part in parts:
        plen = len(part)
        if plen < min_len or plen > max_len:
            continue
        if part.lower() in stopwords:
            continue
        filtered_parts.append(part)
    parts = filtered_parts
    if not parts:
        return None
    return " AND ".join([f"\"{seg.replace('\"', '\"\"')}\"" for seg in parts])


def _execute_with_fallback(
    cursor: sqlite3.Cursor,
    sql_main: str,
    params_main: list,
    sql_fallback: str | None = None,
    params_fallback: list | None = None,
):
    """
    执行SQL查询，如果失败则使用备用查询
    """
    def _should_use_fallback(exc: sqlite3.Error) -> bool:
        if sql_fallback is None:
            return False
        if isinstance(exc, sqlite3.OperationalError):
            return True
        if not isinstance(exc, sqlite3.DatabaseError):
            return False

        sql_lower = sql_main.lower()
        if _TEXTMAP_FTS_TABLE.lower() not in sql_lower and " match " not in sql_lower:
            return False

        message = str(exc).lower()
        fallback_markers = (
            "fts",
            "match",
            "unable to use function",
            "no such module",
            "no such table",
            "no such column: fts",
        )
        return any(marker in message for marker in fallback_markers)

    try:
        cursor.execute(sql_main, params_main)
    except sqlite3.Error as exc:
        if not _should_use_fallback(exc):
            raise
        if sql_fallback is None:
            raise
        fallback_params = params_fallback if params_fallback is not None else []
        cursor.execute(sql_fallback, fallback_params)


def _safe_execute(cursor: sqlite3.Cursor, sql: str, params: list = []) -> list:
    """
    安全执行SQL查询，处理异常并返回结果
    """
    try:
        cursor.execute(sql, params)
        return cursor.fetchall()
    except Exception:
        return []


def _build_like_patterns(keyword: str, lang_code: int) -> tuple[str, str]:
    """
    构建LIKE查询模式
    """
    escaped = _escape_like(keyword)
    exact = f"%{escaped}%"

    if lang_code in CHINESE_LANG_CODES:
        fuzzy_source = "".join(keyword.split())
        fuzzy_escaped_chars = [_escape_like(ch) for ch in fuzzy_source]
        fuzzy = "%" + "%".join(fuzzy_escaped_chars) + "%" if fuzzy_escaped_chars else exact
    else:
        fuzzy = exact
    return exact, fuzzy


def _normalize_match_keyword(keyword: str | None, lang_code: int) -> str:
    text = str(keyword or "").strip()
    if lang_code in CHINESE_LANG_CODES:
        text = "".join(text.split())
    return text.lower()


def _build_normalized_match_expr(field_expr: str, lang_code: int) -> str:
    trimmed = f"trim(coalesce({field_expr}, ''))"
    if lang_code in CHINESE_LANG_CODES:
        return (
            "lower(replace(replace(replace(replace("
            f"{trimmed}, ' ', ''), char(9), ''), char(10), ''), char(13), ''))"
        )
    return f"lower({trimmed})"


def _build_match_sort_case(
    field_expr: str,
    keyword: str,
    lang_code: int,
    base_rank: int = 0,
) -> tuple[str, list]:
    normalized_keyword = _normalize_match_keyword(keyword, lang_code)
    if not normalized_keyword:
        return f"case when 1=1 then {base_rank} end", []

    normalized_expr = _build_normalized_match_expr(field_expr, lang_code)
    escaped = _escape_like(normalized_keyword)
    prefix = f"{escaped}%"
    contains = f"%{escaped}%"
    sql = (
        "case "
        f"when {normalized_expr} = ? then {base_rank} "
        f"when {normalized_expr} like ? escape '\\' then {base_rank + 1} "
        f"when {normalized_expr} like ? escape '\\' then {base_rank + 2} "
        f"else {base_rank + 3} end"
    )
    return sql, [normalized_keyword, prefix, contains]


def _build_textmap_query(
    use_fts: bool,
    keyword: str,
    langCode: int,
    exact: str,
    fuzzy: str,
    fts_match: str | None,
    voice_expr: str | None,
    voice_filter: str | None,
    created_version: str | None,
    updated_version: str | None,
    hash_value: int | None = None,
    limit: int | None = None,
    offset: int | None = None
) -> tuple[str, list]:
    """
    构建文本映射查询
    """
    params: list = []
    version_select = _version_select_expr("tm", "textMap")

    if use_fts and fts_match:
        # 使用FTS查询
        sql = (
            f"select tm.hash, tm.content, {version_select} from textMap tm "
            f"where tm.id in (select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ? and lang=?) "
            "and tm.lang=? "
            "and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
        )
        params = [fts_match, langCode, langCode, exact, fuzzy]
    else:
        # 使用LIKE查询
        sql = (
            f"select tm.hash, tm.content, {version_select} from textMap tm "
            "where tm.lang=? and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
        )
        params = [langCode, exact, fuzzy]

    # 添加版本过滤
    sql = _append_version_filter_clause(
        sql,
        params,
        "tm",
        created_version,
        updated_version,
        "textMap",
    )

    # 添加语音过滤
    if voice_filter and voice_expr:
        if voice_filter == "with":
            sql += f"and ({voice_expr}) "
        elif voice_filter == "without":
            sql += f"and not ({voice_expr}) "

    match_sort_sql, match_sort_params = _build_match_sort_case("tm.content", keyword, langCode)

    # 简化排序逻辑：只在SQL中做基本排序
    # 复杂的子查询（source_presence, dialogue exist check）移到应用层避免性能问题
    if hash_value is not None:
        sql += (
            "order by "
            "case when tm.hash = ? then 0 else 1 end, "
            f"{match_sort_sql}, "
            f"{_voice_order_expr('tm.hash')} "
        )
        params.append(hash_value)
        params.extend(match_sort_params)
    else:
        sql += (
            "order by "
            f"{match_sort_sql}, "
            f"{_voice_order_expr('tm.hash')} "
        )
        params.extend(match_sort_params)

    if limit is not None:
        sql += "limit ?"
        params.append(limit)
        if offset is not None:
            sql += " offset ?"
            params.append(offset)

    return sql, params


def _build_textmap_count_query(
    use_fts: bool,
    langCode: int,
    exact: str,
    fuzzy: str,
    fts_match: str | None,
    voice_expr: str | None,
    voice_filter: str | None,
    created_version: str | None,
    updated_version: str | None
) -> tuple[str, list]:
    """
    构建文本映射计数查询
    """
    params: list = []

    if use_fts and fts_match:
        # 使用FTS查询
        sql = (
            f"select count(*) from textMap tm "
            f"where tm.id in (select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ? and lang=?) "
            "and tm.lang=? "
            "and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
        )
        params = [fts_match, langCode, langCode, exact, fuzzy]
    else:
        # 使用LIKE查询
        sql = (
            "select count(*) from textMap tm "
            "where tm.lang=? and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
        )
        params = [langCode, exact, fuzzy]

    # 添加版本过滤
    sql = _append_version_filter_clause(
        sql,
        params,
        "tm",
        created_version,
        updated_version,
        "textMap",
    )

    # 添加语音过滤
    if voice_filter and voice_expr:
        if voice_filter == "with":
            sql += f"and ({voice_expr})"
        elif voice_filter == "without":
            sql += f"and not ({voice_expr})"

    return sql, params


def _readable_lang_aliases(lang_value: str | None) -> list[str]:
    if lang_value is None:
        return []
    text = str(lang_value).strip()
    if not text:
        return []

    aliases: list[str] = []
    seen: set[str] = set()

    def add_alias(value: str):
        if not value:
            return
        if value in seen:
            return
        seen.add(value)
        aliases.append(value)

    add_alias(text)
    add_alias(text.upper())

    m = _READABLE_LANG_PATTERN.match(text)
    if m:
        short = m.group(1)
        add_alias(short)
        add_alias(short.upper())
        add_alias(f"TextMap{short.upper()}.json")
        add_alias(f"Text{short.upper()}.json")
    elif re.fullmatch(r"[A-Za-z]{2,4}", text):
        upper = text.upper()
        add_alias(f"TextMap{upper}.json")
        add_alias(f"Text{upper}.json")

    return aliases


def _expand_readable_langs(langs: list[str]) -> list[str]:
    expanded: list[str] = []
    seen: set[str] = set()
    for lang in langs:
        for alias in _readable_lang_aliases(lang):
            if alias in seen:
                continue
            seen.add(alias)
            expanded.append(alias)
    return expanded


def _subtitle_base_name(file_name: str | None) -> str | None:
    if file_name is None:
        return None
    text = str(file_name).strip()
    if not text:
        return None
    m = re.match(r"^(.*)_([A-Z]{2,3})$", text)
    if m and m.group(2) in _SUBTITLE_LANG_SUFFIX_SET:
        return m.group(1)
    return text


def _subtitle_file_candidates(file_name: str | None, langs: list[int] | None = None) -> list[str]:
    base = _subtitle_base_name(file_name)
    if not base:
        return []
    if not langs:
        # Fallback: all language variants.
        return [f"{base}_{suffix}" for suffix in _SUBTITLE_LANG_SUFFIX_BY_ID.values()]

    candidates: list[str] = []
    seen: set[str] = set()
    for lang in langs:
        suffix = _SUBTITLE_LANG_SUFFIX_BY_ID.get(lang)
        if not suffix:
            continue
        file_variant = f"{base}_{suffix}"
        if file_variant in seen:
            continue
        seen.add(file_variant)
        candidates.append(file_variant)
    if not candidates:
        # Keep compatibility when lang id is unknown.
        candidates.append(base)
    return candidates


def _ensure_fetter_voice_schema(cursor: sqlite3.Cursor | None = None) -> None:
    def ensure(local_cursor: sqlite3.Cursor) -> None:
        row = local_cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (_FETTER_VOICE_TABLE,),
        ).fetchone()
        if row:
            columns = {
                column_row[1]
                for column_row in local_cursor.execute(f"PRAGMA table_info({_FETTER_VOICE_TABLE})").fetchall()
            }
            if "avatarId" not in columns:
                local_cursor.execute(f"DROP TABLE IF EXISTS {_FETTER_VOICE_TABLE}")

        local_cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {_FETTER_VOICE_TABLE}
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                avatarId INTEGER NOT NULL,
                voiceFile INTEGER NOT NULL,
                voicePath TEXT NOT NULL
            )
            """
        )
        local_cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {_FETTER_VOICE_TABLE}_avatarId_voiceFile_index "
            f"ON {_FETTER_VOICE_TABLE}(avatarId, voiceFile)"
        )
        local_cursor.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_FETTER_VOICE_TABLE}_avatarId_voiceFile_voicePath_uindex "
            f"ON {_FETTER_VOICE_TABLE}(avatarId, voiceFile, voicePath)"
        )

    if cursor is not None:
        ensure(cursor)
        return
    with closing(conn.cursor()) as local_cursor:
        ensure(local_cursor)
    conn.commit()


def _resolve_anime_game_data_root() -> Path | None:
    candidates: list[Path] = []
    env_path = os.environ.get("GTS_ANIME_GAME_DATA", "").strip()
    if env_path:
        candidates.append(Path(env_path))
    try:
        repo_root = config.project_root()
        candidates.extend([
            repo_root / "AnimeGameData",
            repo_root.parent / "AnimeGameData",
        ])
    except Exception:
        pass
    current_root = Path(__file__).resolve().parents[1]
    candidates.extend([
        current_root / "AnimeGameData",
        current_root.parent / "AnimeGameData",
    ])

    seen: set[str] = set()
    for candidate in candidates:
        normalized = candidate.expanduser()
        key = str(normalized)
        if key in seen:
            continue
        seen.add(key)
        if (normalized / "BinOutput" / "Voice" / "Items").is_dir():
            return normalized
    return None


def _read_anime_game_data_json(relative_path: str):
    if relative_path in _ANIME_GAME_DATA_JSON_CACHE:
        return _ANIME_GAME_DATA_JSON_CACHE[relative_path]

    data_root = _resolve_anime_game_data_root()
    if data_root is None:
        _ANIME_GAME_DATA_JSON_CACHE[relative_path] = None
        return None

    file_path = data_root / relative_path
    if not file_path.is_file():
        _ANIME_GAME_DATA_JSON_CACHE[relative_path] = None
        return None

    try:
        with open(file_path, encoding="utf-8-sig") as fp:
            payload = json.load(fp)
    except Exception:
        payload = None

    _ANIME_GAME_DATA_JSON_CACHE[relative_path] = payload
    return payload


def _load_main_quest_rows_by_id() -> dict[int, dict]:
    global _MAIN_QUEST_ROWS_BY_ID
    if _MAIN_QUEST_ROWS_BY_ID is not None:
        return _MAIN_QUEST_ROWS_BY_ID

    rows = _read_anime_game_data_json("ExcelBinOutput/MainQuestExcelConfigData.json")
    mapping: dict[int, dict] = {}
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            quest_id = row.get("id")
            if isinstance(quest_id, int):
                mapping[quest_id] = row

    _MAIN_QUEST_ROWS_BY_ID = mapping
    return mapping


def _load_quest_step_rows_by_main_id() -> dict[int, list[dict]]:
    global _QUEST_STEP_ROWS_BY_MAIN_ID
    if _QUEST_STEP_ROWS_BY_MAIN_ID is not None:
        return _QUEST_STEP_ROWS_BY_MAIN_ID

    rows = _read_anime_game_data_json("ExcelBinOutput/QuestExcelConfigData.json")
    grouped: dict[int, list[dict]] = {}
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            main_id = row.get("mainId")
            if not isinstance(main_id, int):
                continue
            grouped.setdefault(main_id, []).append(row)

    for main_id, items in grouped.items():
        items.sort(key=lambda item: (item.get("order", 10**9), item.get("subId", 10**9)))

    _QUEST_STEP_ROWS_BY_MAIN_ID = grouped
    return grouped


def _get_quest_bin_output(questId: int) -> dict | None:
    payload = _read_anime_game_data_json(f"BinOutput/Quest/{questId}.json")
    return payload if isinstance(payload, dict) else None


def _resolve_quest_step_text_hash(step_obj: dict) -> int | None:
    keys = (
        "stepDescTextMapHash",
        "OCMKKHHNKJO",
        "BMBANCMPPOM",
        "NAEMBIJFJCA",
        "HMLBMECMBGA",
    )
    for key in keys:
        value = step_obj.get(key)
        if isinstance(value, int) and value not in (0,):
            return value
    return None


def _get_quest_bin_step_list(quest_bin: dict) -> list[dict]:
    steps = quest_bin.get("NLCNGJKMAEN")
    if not isinstance(steps, list):
        steps = quest_bin.get("subQuests")
    if not isinstance(steps, list):
        steps = quest_bin.get("GFLHMKOOHHA")
    if not isinstance(steps, list):
        return []
    return [item for item in steps if isinstance(item, dict)]


def _extract_step_talk_ids_from_quest_bin(quest_bin: dict) -> dict[int, list[int]]:
    result: dict[int, list[int]] = {}
    for step in _get_quest_bin_step_list(quest_bin):
        sub_id = step.get("MPKBGPAKIOA")
        if not isinstance(sub_id, int):
            sub_id = step.get("subId")
        if not isinstance(sub_id, int):
            continue

        result.setdefault(sub_id, [])
        conditions = step.get("AACKELGGJGC")
        if not isinstance(conditions, list):
            conditions = step.get("finishCond")
        if not isinstance(conditions, list):
            conditions = step.get("KBFJAAFDHKJ")
        if not isinstance(conditions, list):
            continue

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
            if isinstance(talk_id, int) and talk_id > 0 and talk_id not in result[sub_id]:
                result[sub_id].append(talk_id)
    return result


def getQuestDescription(questId: int, langCode: int = 1) -> str:
    if _table_has_column("quest", "descTextMapHash"):
        with closing(conn.cursor()) as cursor:
            row = cursor.execute(
                "select content from quest join textMap on quest.descTextMapHash=textMap.hash "
                "where quest.questId=? and textMap.lang=?",
                (questId, langCode),
            ).fetchone()
        if row and row[0]:
            return _normalize_output_text(row[0], langCode) or ""

    row = _load_main_quest_rows_by_id().get(questId)
    if not isinstance(row, dict):
        return ""
    desc_hash = row.get("descTextMapHash")
    if not isinstance(desc_hash, int) or desc_hash == 0:
        return ""
    return getTextMapContent(desc_hash, langCode) or ""


def getQuestStepTitleMap(questId: int, langCode: int = 1) -> dict[int, str]:
    cache_key = (int(questId), int(langCode))
    if cache_key in _QUEST_STEP_TALK_MAP_CACHE:
        return _QUEST_STEP_TALK_MAP_CACHE[cache_key]

    if _table_has_column("questTalk", "stepTitleTextMapHash"):
        with closing(conn.cursor()) as cursor:
            rows = cursor.execute(
                "select questTalk.talkId, textMap.content "
                "from questTalk join textMap on questTalk.stepTitleTextMapHash=textMap.hash "
                "where questTalk.questId=? and textMap.lang=? "
                "and coalesce(questTalk.coopQuestId, 0)=0 "
                "and questTalk.stepTitleTextMapHash is not null and questTalk.stepTitleTextMapHash<>0 "
                "order by questTalk.talkId",
                (questId, langCode),
            ).fetchall()
        if rows:
            talk_title_map = {
                int(talk_id): _normalize_output_text(content, langCode) or ""
                for talk_id, content in rows
                if isinstance(talk_id, int) and content
            }
            _QUEST_STEP_TALK_MAP_CACHE[cache_key] = talk_title_map
            return talk_title_map

    title_by_sub_id: dict[int, str] = {}
    order_by_sub_id: dict[int, int] = {}

    for row in _load_quest_step_rows_by_main_id().get(questId, []):
        if not isinstance(row, dict):
            continue
        sub_id = row.get("subId")
        if not isinstance(sub_id, int):
            continue
        text_hash = row.get("stepDescTextMapHash")
        if isinstance(text_hash, int) and text_hash != 0:
            title = getTextMapContent(text_hash, langCode) or ""
            if title:
                title_by_sub_id[sub_id] = title
        order = row.get("order")
        if isinstance(order, int):
            order_by_sub_id[sub_id] = order

    quest_bin = _get_quest_bin_output(questId)
    if isinstance(quest_bin, dict):
        for step in _get_quest_bin_step_list(quest_bin):
            sub_id = step.get("MPKBGPAKIOA")
            if not isinstance(sub_id, int):
                sub_id = step.get("subId")
            if not isinstance(sub_id, int):
                continue
            if sub_id not in title_by_sub_id:
                text_hash = _resolve_quest_step_text_hash(step)
                if isinstance(text_hash, int):
                    title = getTextMapContent(text_hash, langCode) or ""
                    if title:
                        title_by_sub_id[sub_id] = title
            if sub_id not in order_by_sub_id:
                order = step.get("EDICBFEMNNF")
                if not isinstance(order, int):
                    order = step.get("order")
                if isinstance(order, int):
                    order_by_sub_id[sub_id] = order

    talk_ids_by_sub_id = _extract_step_talk_ids_from_quest_bin(quest_bin) if isinstance(quest_bin, dict) else {}
    talk_title_map: dict[int, str] = {}

    def sort_key(item: tuple[int, list[int]]) -> tuple[int, int]:
        sub_id = item[0]
        return (order_by_sub_id.get(sub_id, 10**9), sub_id)

    for sub_id, talk_ids in sorted(talk_ids_by_sub_id.items(), key=sort_key):
        title = title_by_sub_id.get(sub_id) or ""
        if not title:
            continue
        for talk_id in talk_ids:
            if isinstance(talk_id, int) and talk_id > 0:
                talk_title_map.setdefault(talk_id, title)

    _QUEST_STEP_TALK_MAP_CACHE[cache_key] = talk_title_map
    return talk_title_map


def _collect_fetter_switch_names(config_node, output: set[str]) -> None:
    if isinstance(config_node, dict):
        for value in config_node.values():
            _collect_fetter_switch_names(value, output)
        return
    if isinstance(config_node, list):
        for item in config_node:
            _collect_fetter_switch_names(item, output)
        return
    if isinstance(config_node, str):
        lowered = config_node.lower()
        if lowered.startswith("switch_"):
            output.add(lowered)


def _extract_fetter_internal_names(avatar_config: dict) -> set[str]:
    switch_names: set[str] = set()
    _collect_fetter_switch_names(avatar_config, switch_names)
    generic_names = {
        "switch_boy",
        "switch_girl",
        "switch_male",
        "switch_female",
        "switch_loli",
        "switch_lady",
        "switch_other",
    }
    filtered = {name for name in switch_names if name not in generic_names}
    if filtered:
        return filtered

    legacy_name = (
        avatar_config.get("PLJBIDOIHEA", {})
        .get("KDKDKOMMCOB", {})
        .get("GLMJHDNIGID")
    )
    if isinstance(legacy_name, str) and legacy_name:
        return {legacy_name.lower()}
    return set()


def _load_fetter_avatar_mappings() -> dict[str, int]:
    global _FETTER_AVATAR_MAPPINGS
    if _FETTER_AVATAR_MAPPINGS is not None:
        return _FETTER_AVATAR_MAPPINGS

    data_root = _resolve_anime_game_data_root()
    if data_root is None:
        _FETTER_AVATAR_MAPPINGS = {}
        return _FETTER_AVATAR_MAPPINGS

    avatar_excel = data_root / "ExcelBinOutput" / "AvatarExcelConfigData.json"
    avatar_dir = data_root / "BinOutput" / "Avatar"
    if not avatar_excel.is_file() or not avatar_dir.is_dir():
        _FETTER_AVATAR_MAPPINGS = {}
        return _FETTER_AVATAR_MAPPINGS

    try:
        with open(avatar_excel, encoding="utf-8") as fp:
            avatars_json = json.load(fp)
    except Exception:
        _FETTER_AVATAR_MAPPINGS = {}
        return _FETTER_AVATAR_MAPPINGS

    mappings: dict[str, int] = {}
    for avatar in avatars_json:
        if not isinstance(avatar, dict):
            continue
        avatar_id = avatar.get("id")
        if not isinstance(avatar_id, int) or avatar_id >= 11000000:
            continue
        icon_name = str(avatar.get("iconName") or "")
        if len(icon_name) <= 14:
            continue
        config_path = avatar_dir / f"ConfigAvatar_{icon_name[14:]}.json"
        if not config_path.is_file():
            continue
        try:
            with open(config_path, encoding="utf-8") as fp:
                avatar_config = json.load(fp)
        except Exception:
            continue
        for internal_name in _extract_fetter_internal_names(avatar_config):
            mappings.setdefault(internal_name, avatar_id)

    _FETTER_AVATAR_MAPPINGS = mappings
    return mappings


def _map_fetter_switch_name_to_avatar_id(raw_name: str | None) -> int:
    text = str(raw_name or "").strip().lower()
    if not text:
        return 0
    if text.startswith("switch_gcg") or text.startswith("gcg"):
        return 0
    if text == "switch_hero":
        return 10000005
    if text == "switch_heroine":
        return 10000007
    if text == "switch_tartaglia_melee":
        text = "switch_tartaglia"
    return _load_fetter_avatar_mappings().get(text, 0)


def _extract_fetter_voice_rows(content: dict) -> list[tuple[int, int, str]]:
    raw_type = content.get("IACPGADBANJ")
    if raw_type is None:
        raw_type = content.get("Type", content.get("type"))
    if str(raw_type or "").strip().lower() != "fetter":
        return []

    raw_voice_file = content.get("MGOMDNKKLCP")
    if raw_voice_file is None:
        raw_voice_file = content.get("voiceFile", content.get("Id", content.get("id")))
    voice_file = _coerce_optional_int(raw_voice_file)
    if voice_file is None:
        return []

    source_nodes = content.get("OPGDOEDEJOJ")
    if source_nodes is None:
        source_nodes = content.get("SourceNames", content.get("sourceNames", []))
    if not isinstance(source_nodes, list):
        return []

    rows: list[tuple[int, int, str]] = []
    seen_rows: set[tuple[int, str]] = set()
    for source in source_nodes:
        if not isinstance(source, dict):
            continue
        switch_name = (
            source.get("IEPAMKPOOII")
            or source.get("avatarName")
            or source.get("GDIJGLOHHFM")
            or source.get("switchName")
        )
        avatar_id = _map_fetter_switch_name_to_avatar_id(switch_name)
        if avatar_id <= 0:
            continue
        voice_path = source.get("MDOCAGOFPAP")
        if voice_path is None:
            voice_path = source.get("sourceFileName", source.get("DCIHFJLBLAP", source.get("BJDAJEKPCFP")))
        voice_path_text = str(voice_path or "").strip()
        row_key = (avatar_id, voice_path_text)
        if not voice_path_text or row_key in seen_rows:
            continue
        seen_rows.add(row_key)
        rows.append((avatar_id, voice_file, voice_path_text))
    return rows


def _load_fetter_voice_rows_from_data() -> list[tuple[int, int, str]]:
    data_root = _resolve_anime_game_data_root()
    if data_root is None:
        return []

    items_dir = data_root / "BinOutput" / "Voice" / "Items"
    rows: list[tuple[int, int, str]] = []
    for json_file in sorted(items_dir.glob("*.json")):
        try:
            with open(json_file, encoding="utf-8") as fp:
                payload = json.load(fp)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        for content in payload.values():
            if not isinstance(content, dict):
                continue
            rows.extend(_extract_fetter_voice_rows(content))
    return rows


def _ensure_fetter_voice_data() -> None:
    global _FETTER_VOICE_SYNC_ATTEMPTED
    _ensure_fetter_voice_schema()
    with closing(conn.cursor()) as cursor:
        row = cursor.execute(f"SELECT 1 FROM {_FETTER_VOICE_TABLE} LIMIT 1").fetchone()
        if row:
            return
    if _FETTER_VOICE_SYNC_ATTEMPTED:
        return

    with _FETTER_VOICE_SYNC_LOCK:
        with closing(conn.cursor()) as cursor:
            _ensure_fetter_voice_schema(cursor)
            row = cursor.execute(f"SELECT 1 FROM {_FETTER_VOICE_TABLE} LIMIT 1").fetchone()
            if row:
                _FETTER_VOICE_SYNC_ATTEMPTED = True
                conn.commit()
                return

        rows = _load_fetter_voice_rows_from_data()
        with closing(conn.cursor()) as cursor:
            _ensure_fetter_voice_schema(cursor)
            if rows:
                cursor.executemany(
                    f"""
                    INSERT INTO {_FETTER_VOICE_TABLE}(avatarId, voiceFile, voicePath)
                    VALUES (?, ?, ?)
                    ON CONFLICT(avatarId, voiceFile, voicePath) DO NOTHING
                    """,
                    rows,
                )
        conn.commit()
        _FETTER_VOICE_SYNC_ATTEMPTED = True


def _text_hash_has_voice_expr(text_hash_field: str) -> str:
    return (
        "exists ("
        "select 1 from dialogue d "
        "join voice v on v.dialogueId = d.dialogueId "
        f"where d.textHash = {text_hash_field} limit 1"
        ") or exists ("
        "select 1 from fetters f "
        f"join {_FETTER_VOICE_TABLE} fv on fv.avatarId = f.avatarId and fv.voiceFile = f.voiceFile "
        f"where f.voiceFileTextTextMapHash = {text_hash_field} limit 1"
        ")"
    )


def _voice_exists_expr(text_hash_field: str) -> str:
    return _text_hash_has_voice_expr(text_hash_field)


def _voice_order_expr(text_hash_field: str) -> str:
    return (
        "case when "
        f"{_text_hash_has_voice_expr(text_hash_field)} "
        "then 0 else 1 end"
    )


def _has_avatar_scoped_fetter_voice(textHash: int) -> bool:
    _ensure_fetter_voice_data()
    with closing(conn.cursor()) as cursor:
        sql = (
            "SELECT EXISTS("
            "SELECT 1 FROM dialogue d "
            "JOIN voice v ON v.dialogueId = d.dialogueId "
            "WHERE d.textHash = ?"
            ") OR EXISTS("
            "SELECT 1 FROM fetters f "
            f"JOIN {_FETTER_VOICE_TABLE} fv ON fv.avatarId = f.avatarId AND fv.voiceFile = f.voiceFile "
            "WHERE f.voiceFileTextTextMapHash = ?"
            ")"
        )
        cursor.execute(sql, (textHash, textHash))
        row = cursor.fetchone()
        return bool(row and row[0])


def _select_avatar_scoped_voice_path_from_text_hash(textHash: int):
    _ensure_fetter_voice_data()
    with closing(conn.cursor()) as cursor:
        sql_dialogue = (
            "select voicePath from dialogue "
            "join voice on voice.dialogueId = dialogue.dialogueId "
            "where textHash=? limit 1"
        )
        cursor.execute(sql_dialogue, (textHash,))
        match = cursor.fetchone()
        if match:
            return match[0]

        sql_fetter = (
            "select fv.voicePath from fetters "
            f"join {_FETTER_VOICE_TABLE} fv on fv.avatarId = fetters.avatarId and fv.voiceFile = fetters.voiceFile "
            "where voiceFileTextTextMapHash=? limit 1"
        )
        cursor.execute(sql_fetter, (textHash,))
        match = cursor.fetchone()
        if match:
            return match[0]
        return None


def _get_avatar_scoped_voice_path(voice_hash: str, lang: int):
    try:
        text_hash = int(voice_hash)
    except (TypeError, ValueError):
        return None
    return _select_avatar_scoped_voice_path_from_text_hash(text_hash)


def _select_avatar_scoped_voice_items(avatarId: int, limit: int = 400):
    _ensure_fetter_voice_data()
    with closing(conn.cursor()) as cursor:
        sql = (
            f"select fetters.voiceTitleTextMapHash, fetters.voiceFileTextTextMapHash, fv.voicePath "
            "from fetters "
            f"left join {_FETTER_VOICE_TABLE} fv on fv.avatarId = fetters.avatarId and fv.voiceFile = fetters.voiceFile "
            "where fetters.avatarId=? "
            "order by fetters.fetterId, fv.voicePath "
            "limit ?"
        )
        cursor.execute(sql, (avatarId, limit))
        return cursor.fetchall()


def _select_avatar_scoped_voice_items_by_filters(
    keyword: str | None,
    langCode: int,
    limit: int | None = 800,
    created_version: str | None = None,
    updated_version: str | None = None,
    version_lang_code: int | None = None,
):
    _ensure_fetter_voice_data()
    with closing(conn.cursor()) as cursor:
        keyword_text = (keyword or "").strip()
        exact = fuzzy = None
        if keyword_text:
            escaped = _escape_like(keyword_text)
            exact = f"%{escaped}%"
            fuzzy = exact

        sql = (
            "select fetters.avatarId, fetters.voiceTitleTextMapHash, "
            f"fetters.voiceFileTextTextMapHash, fv.voicePath "
            "from fetters "
            f"left join {_FETTER_VOICE_TABLE} fv on fv.avatarId = fetters.avatarId and fv.voiceFile = fetters.voiceFile "
            "left join textMap as titleText on titleText.hash = fetters.voiceTitleTextMapHash "
            "and titleText.lang = ? "
            "left join textMap as contentText on contentText.hash = fetters.voiceFileTextTextMapHash "
            "and contentText.lang = ? "
            "where 1=1 "
        )
        params: list[object] = [langCode, langCode]

        if keyword_text:
            sql += (
                "and ("
                "(titleText.content like ? escape '\\' or titleText.content like ? escape '\\') "
                "or (contentText.content like ? escape '\\' or contentText.content like ? escape '\\')"
                ") "
            )
            params.extend([exact, fuzzy, exact, fuzzy])

        sql = _append_textmap_exists_version_filter(
            sql,
            params,
            "fetters.voiceFileTextTextMapHash",
            created_version,
            updated_version,
            version_lang_code,
        )

        if keyword_text:
            sql += (
                "order by case "
                "when titleText.content like ? escape '\\' then 0 "
                "when contentText.content like ? escape '\\' then 1 "
                "else 2 end, "
                "length(coalesce(titleText.content, contentText.content, '')), fetters.fetterId, fv.voicePath "
            )
            params.extend([exact, exact])
        else:
            sql += "order by fetters.fetterId, fv.voicePath "

        if limit is not None:
            sql += "limit ?"
            params.append(limit)

        cursor.execute(sql, params)
        return cursor.fetchall()


def _append_version_filter_clause(
    sql: str,
    params: list,
    table_alias: str,
    created_version: str | None,
    updated_version: str | None,
    table_name: str | None = None,
    lang_code: int | None = None,
) -> str:
    created = _normalize_version_filter(created_version)
    updated = _normalize_version_filter(updated_version)
    if table_name == 'quest':
        # For quest table, only check created_version_id since updated_version_id is in quest_version
        if not _table_has_column(table_name, "created_version_id"):
            if created or updated:
                sql += "and 1=0 "
            return sql
        has_id_mode = bool(_table_has_column(table_name, "created_version_id") and _has_version_dim())
    elif table_name == 'npc':
        if not _table_has_column(table_name, "created_version_id"):
            if created or updated:
                sql += "and 1=0 "
            return sql
        has_id_mode = bool(_table_has_column(table_name, "created_version_id") and _has_version_dim())
    else:
        if table_name and not _has_version_id_columns(table_name):
            if created or updated:
                sql += "and 1=0 "
            return sql
        has_id_mode = bool(table_name and _has_version_id_columns(table_name) and _has_version_dim())
    if created:
        if has_id_mode:
            sql += (
                f"and exists ("
                f"select 1 from {_VERSION_DIM_TABLE} vdc "
                f"where vdc.id = {table_alias}.created_version_id "
                f"and coalesce(vdc.version_tag, '') = ? "
                f"limit 1) "
            )
            params.append(created)
        else:
            sql += "and 1=0 "
    if updated:
        if has_id_mode:
            if table_name == 'quest' and lang_code is not None:
                # For quest table, updated version is in quest_version table with language filter
                sql += (
                    f"and exists ("
                    f"select 1 from quest_version qv "
                    f"join {_VERSION_DIM_TABLE} vdu on vdu.id = qv.updated_version_id "
                    f"where qv.questId = {table_alias}.questId "
                    f"and qv.lang = ? "
                    f"and coalesce(vdu.version_tag, '') = ? "
                    f"limit 1) "
                )
                params.append(lang_code)
                params.append(updated)

                # "updated version" filter should only include rows that were actually updated
                # after creation, excluding rows where created_version == updated_version.
                sql += (
                    f"and not exists ("
                    f"select 1 from {_VERSION_DIM_TABLE} vdc "
                    f"join quest_version qv on qv.questId = {table_alias}.questId "
                    f"join {_VERSION_DIM_TABLE} vdu on vdu.id = qv.updated_version_id "
                    f"where vdc.id = {table_alias}.created_version_id "
                    f"and qv.lang = ? "
                    f"and lower(trim(coalesce(vdc.version_tag, vdc.raw_version, ''))) "
                    f"= lower(trim(coalesce(vdu.version_tag, vdu.raw_version, ''))) "
                    f"limit 1) "
                )
                params.append(lang_code)
            elif table_name == 'npc':
                sql += "and 1=0 "
            else:
                # For other tables, updated version is in the same table
                sql += (
                    f"and exists ("
                    f"select 1 from {_VERSION_DIM_TABLE} vdu "
                    f"where vdu.id = {table_alias}.updated_version_id "
                    f"and coalesce(vdu.version_tag, '') = ? "
                    f"limit 1) "
                )
                params.append(updated)

                # "updated version" filter should only include rows that were actually updated
                # after creation, excluding rows where created_version == updated_version.
                sql += (
                    f"and not exists ("
                    f"select 1 from {_VERSION_DIM_TABLE} vdc "
                    f"join {_VERSION_DIM_TABLE} vdu on vdu.id = {table_alias}.updated_version_id "
                    f"where vdc.id = {table_alias}.created_version_id "
                    f"and lower(trim(coalesce(vdc.version_tag, vdc.raw_version, ''))) "
                    f"= lower(trim(coalesce(vdu.version_tag, vdu.raw_version, ''))) "
                    f"limit 1) "
                )
        else:
            sql += "and 1=0 "
    return sql


_QUEST_SOURCE_TYPE_FILTERS = {
    "AQ",
    "LQ",
    "WQ",
    "EQ",
    "IQ",
    "HANGOUT",
    "ANECDOTE",
    "UNKNOWN",
}

READABLE_CATEGORY_LABELS = {
    "BOOK": "书籍",
    "COSTUME": "衣装",
    "RELIC": "圣遗物",
    "WEAPON": "武器",
    "WINGS": "风之翼",
    "OTHER": "其他",
}

_READABLE_CATEGORY_PREFIXES = (
    ("BOOK", "Book"),
    ("COSTUME", "Costume"),
    ("RELIC", "Relic"),
    ("WEAPON", "Weapon"),
    ("WINGS", "Wings"),
)

_READABLE_CATEGORY_FILTERS = set(READABLE_CATEGORY_LABELS.keys())


def _normalize_quest_source_type_filter(source_type: str | None) -> str | None:
    if source_type is None:
        return None
    normalized = str(source_type).strip().upper()
    if not normalized:
        return None
    if normalized in _QUEST_SOURCE_TYPE_FILTERS:
        return normalized
    return "__INVALID__"


def _append_quest_source_type_filter_clause(
    sql: str,
    params: list,
    table_alias: str,
    source_type: str | None,
) -> str:
    normalized = _normalize_quest_source_type_filter(source_type)
    if normalized is None:
        return sql
    if normalized == "__INVALID__":
        return sql + "and 1=0 "
    if not _table_has_column("quest", "source_type"):
        return sql + "and 1=0 "
    sql += f"and {table_alias}.source_type = ? "
    params.append(normalized)
    return sql


def getReadableCategoryCode(file_name: str | None) -> str:
    normalized_name = str(file_name or "").strip()
    for code, prefix in _READABLE_CATEGORY_PREFIXES:
        if normalized_name.startswith(prefix):
            return code
    return "OTHER"


def _normalize_readable_category_filter(category: str | None) -> str | None:
    if category is None:
        return None
    normalized = str(category).strip().upper()
    if not normalized:
        return None
    if normalized in _READABLE_CATEGORY_FILTERS:
        return normalized
    return "__INVALID__"


def _readable_category_case_expr(table_alias: str) -> str:
    file_name_expr = f"{table_alias}.fileName"
    return (
        f"case "
        f"when {file_name_expr} glob 'Book*' then 'BOOK' "
        f"when {file_name_expr} glob 'Costume*' then 'COSTUME' "
        f"when {file_name_expr} glob 'Relic*' then 'RELIC' "
        f"when {file_name_expr} glob 'Weapon*' then 'WEAPON' "
        f"when {file_name_expr} glob 'Wings*' then 'WINGS' "
        f"else 'OTHER' end"
    )


def _append_readable_category_filter_clause(
    sql: str,
    params: list,
    table_alias: str,
    category: str | None,
) -> str:
    normalized = _normalize_readable_category_filter(category)
    if normalized is None:
        return sql
    if normalized == "__INVALID__":
        return sql + "and 1=0 "
    sql += f"and {_readable_category_case_expr(table_alias)} = ? "
    params.append(normalized)
    return sql


def _quest_talk_dialogue_join_condition(qt_alias: str = "qt", d_alias: str = "d") -> str:
    return (
        f"(({qt_alias}.coopQuestId IS NULL OR {qt_alias}.coopQuestId = 0) AND {d_alias}.coopQuestId IS NULL) "
        f"OR ({qt_alias}.coopQuestId > 0 AND {d_alias}.coopQuestId = {qt_alias}.coopQuestId)"
    )


def _append_textmap_exists_version_filter(
    sql: str,
    params: list,
    text_hash_expr: str,
    created_version: str | None,
    updated_version: str | None,
    lang_code: int | None = None,
) -> str:
    created = _normalize_version_filter(created_version)
    updated = _normalize_version_filter(updated_version)
    if not _has_version_id_columns("textMap"):
        if created or updated:
            sql += "and 1=0 "
        return sql
    if not created and not updated:
        return sql
    has_id_mode = _has_version_id_columns("textMap") and _has_version_dim()
    sql += f"and exists (select 1 from textMap tmv where tmv.hash = {text_hash_expr} "
    if lang_code is not None:
        sql += "and tmv.lang = ? "
        params.append(lang_code)
    if created:
        if has_id_mode:
            sql += (
                f"and exists (select 1 from {_VERSION_DIM_TABLE} vdc "
                f"where vdc.id = tmv.created_version_id "
                f"and coalesce(vdc.version_tag, '') = ? "
                f"limit 1) "
            )
            params.append(created)
        else:
            sql += "and 1=0 "
    if updated:
        if has_id_mode:
            sql += (
                f"and exists (select 1 from {_VERSION_DIM_TABLE} vdu "
                f"where vdu.id = tmv.updated_version_id "
                f"and coalesce(vdu.version_tag, '') = ? "
                f"limit 1) "
            )
            params.append(updated)
        else:
            sql += "and 1=0 "
        # Keep semantics consistent with direct table filtering.
        if has_id_mode:
            sql += (
                f"and not exists ("
                f"select 1 from {_VERSION_DIM_TABLE} vdc "
                f"join {_VERSION_DIM_TABLE} vdu on vdu.id = tmv.updated_version_id "
                f"where vdc.id = tmv.created_version_id "
                f"and lower(trim(coalesce(vdc.version_tag, vdc.raw_version, ''))) "
                f"= lower(trim(coalesce(vdu.version_tag, vdu.raw_version, ''))) "
                f"limit 1) "
            )
    sql += "limit 1) "
    return sql


def selectTextMapFromKeyword(keyWord: str, langCode: int, limit: int | None = None):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        fts_match = _build_textmap_fts_match(keyWord, langCode)
        match_sort_sql, match_sort_params = _build_match_sort_case("tm.content", keyWord, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('tm.content', langCode)})"

        sql_like = (
            "select tm.hash, tm.content from textMap tm "
            "where tm.lang=? and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
            f"order by {match_sort_sql}, {normalized_length_sql}"
        )
        params_like = [langCode, exact, fuzzy] + match_sort_params
        if limit is not None:
            sql_like += " limit ?"
            params_like.append(limit)

        if _is_textmap_fts_lang_enabled(langCode) and fts_match is not None:
            sql_fts = (
                f"select tm.hash, tm.content from textMap tm "
                f"where tm.id in (select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ? and lang=?) "
                "and tm.lang=? "
                "and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
                f"order by {match_sort_sql}, {normalized_length_sql}"
            )
            params_fts = [fts_match, langCode, langCode, exact, fuzzy] + match_sort_params
            if limit is not None:
                sql_fts += " limit ?"
                params_fts.append(limit)
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        return cursor.fetchall()


def selectTextMapFromKeywordPaged(
    keyWord: str,
    langCode: int,
    limit: int,
    offset: int,
    hash_value: int | None = None,
    voice_filter: str | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    """
    分页搜索文本，优化搜索速度和结果质量

    优化措施：
    1. 使用FTS（全文搜索）与LIKE查询结合，提高搜索速度和准确性
    2. 优化FTS查询：使用JOIN代替子查询，减少查询开销
    3. 优先使用精确匹配，然后使用模糊匹配，提高搜索结果质量
    4. 按匹配程度和文本长度排序，确保最相关的结果排在前面
    5. 支持版本过滤，允许用户按游戏版本筛选结果
    6. 支持语音过滤，允许用户筛选带语音的文本

    这些优化措施在保证搜索速度的同时，不会增加数据库体积，实现了速度与体积的平衡
    """
    _ensure_fetter_voice_data()
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        fts_match = _build_textmap_fts_match(keyWord, langCode)
        hash_value = hash_value if hash_value is not None else -1
        voice_expr = _voice_exists_expr("tm.hash")

        # 构建LIKE查询
        sql_like, params_like = _build_textmap_query(
            use_fts=False,
            keyword=keyWord,
            langCode=langCode,
            exact=exact,
            fuzzy=fuzzy,
            fts_match=fts_match,
            voice_expr=voice_expr,
            voice_filter=voice_filter,
            created_version=created_version,
            updated_version=updated_version,
            hash_value=hash_value,
            limit=limit,
            offset=offset
        )

        # 如果启用了FTS且有匹配表达式，构建FTS查询
        if _is_textmap_fts_lang_enabled(langCode) and fts_match is not None:
            sql_fts, params_fts = _build_textmap_query(
                use_fts=True,
                keyword=keyWord,
                langCode=langCode,
                exact=exact,
                fuzzy=fuzzy,
                fts_match=fts_match,
                voice_expr=voice_expr,
                voice_filter=voice_filter,
                created_version=created_version,
                updated_version=updated_version,
                hash_value=hash_value,
                limit=limit,
                offset=offset
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        return cursor.fetchall()


def countTextMapFromKeyword(
    keyWord: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    """
    计算搜索结果数量，优化搜索速度

    优化措施：
    1. 使用FTS（全文搜索）与LIKE查询结合，提高搜索速度
    2. 优化FTS查询：使用JOIN代替子查询，减少查询开销
    3. 支持版本过滤，允许用户按游戏版本筛选结果
    4. 使用execute_with_fallback确保查询在不同环境下都能正常执行

    这些优化措施在保证计数速度的同时，不会增加数据库体积，实现了速度与体积的平衡
    """
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        fts_match = _build_textmap_fts_match(keyWord, langCode)

        # 构建LIKE查询
        sql_like, params_like = _build_textmap_count_query(
            use_fts=False,
            langCode=langCode,
            exact=exact,
            fuzzy=fuzzy,
            fts_match=fts_match,
            voice_expr=None,
            voice_filter=None,
            created_version=created_version,
            updated_version=updated_version
        )

        if _is_textmap_fts_lang_enabled(langCode) and fts_match is not None:
            # 构建FTS查询
            sql_fts, params_fts = _build_textmap_count_query(
                use_fts=True,
                langCode=langCode,
                exact=exact,
                fuzzy=fuzzy,
                fts_match=fts_match,
                voice_expr=None,
                voice_filter=None,
                created_version=created_version,
                updated_version=updated_version
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


# ── 来源类型筛选查询 ──────────────────────────────────────────────

_SOURCE_TYPE_TO_ENTITY_CODE = {
    "item": 1, "food": 2, "furnishing": 3, "gadget": 4,
    "costume": 5, "suit": 6,
    "weapon": 9, "reliquary": 10, "monster": 11, "creature": 12,
    "material": 13,
    "blueprint": 14, "gcg": 15, "namecard": 16, "performance": 17,
    "avatar_intro": 18, "dressing": 19, "music_theme": 20, "other_mat": 21,
    "avatar_mat": 22, "achievement": 23, "viewpoint": 24, "dungeon": 25,
    "loading_tip": 26,
}


def _build_source_type_join(source_type: str) -> tuple[str, list]:
    """根据来源类型构建 JOIN 子句，在数据库层过滤结果。"""
    if source_type == "dialogue":
        return (
            "JOIN dialogue _sj ON _sj.textHash = tm.hash "
            "AND NOT EXISTS (SELECT 1 FROM questTalk _qt WHERE _qt.talkId = _sj.talkId) "
            "AND (_sj.coopQuestId IS NULL OR _sj.coopQuestId = 0) ",
            [],
        )
    elif source_type == "voice":
        return "JOIN fetters _sj ON _sj.voiceFileTextTextMapHash = tm.hash ", []
    elif source_type == "story":
        return (
            "JOIN ("
            "SELECT storyContextTextMapHash AS text_hash FROM fetterStory WHERE storyContextTextMapHash IS NOT NULL "
            "UNION "
            "SELECT storyContext2TextMapHash AS text_hash FROM fetterStory WHERE storyContext2TextMapHash IS NOT NULL"
            ") _sj ON _sj.text_hash = tm.hash ",
            [],
        )
    elif source_type == "quest":
        return "JOIN quest_hash_map _sj ON _sj.hash = tm.hash ", []
    elif source_type in _SOURCE_TYPE_TO_ENTITY_CODE:
        code = _SOURCE_TYPE_TO_ENTITY_CODE[source_type]
        return "JOIN text_source_entity _sj ON _sj.text_hash = tm.hash AND _sj.source_type_code = ? ", [code]
    elif source_type.startswith("custom_"):
        try:
            code = int(source_type[len("custom_"):])
            return "JOIN text_source_entity _sj ON _sj.text_hash = tm.hash AND _sj.source_type_code = ? ", [code]
        except ValueError:
            pass
    return "", []


def selectTextMapFromKeywordBySourceType(
    keyWord: str,
    langCode: int,
    source_type: str,
    limit: int,
    offset: int = 0,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    """在数据库层通过 JOIN 来源表筛选 textMap 关键词搜索结果。"""
    join_clause, join_params = _build_source_type_join(source_type)
    if not join_clause:
        return []

    _ensure_fetter_voice_data()
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        fts_match = _build_textmap_fts_match(keyWord, langCode)
        version_select = _version_select_expr("tm", "textMap")

        # LIKE 查询
        sql_like = (
            f"SELECT DISTINCT tm.hash, tm.content, {version_select} FROM textMap tm "
            f"{join_clause}"
            "WHERE tm.lang=? AND (tm.content LIKE ? ESCAPE '\\' OR tm.content LIKE ? ESCAPE '\\') "
        )
        params_like = [*join_params, langCode, exact, fuzzy]
        sql_like = _append_version_filter_clause(sql_like, params_like, "tm", created_version, updated_version, "textMap")
        match_sort_sql, match_sort_params = _build_match_sort_case("tm.content", keyWord, langCode)
        sql_like += f"ORDER BY {match_sort_sql}, length(tm.content) "
        params_like.extend(match_sort_params)
        sql_like += "LIMIT ? OFFSET ?"
        params_like.extend([limit, offset])

        # FTS 查询
        if _is_textmap_fts_lang_enabled(langCode) and fts_match is not None:
            sql_fts = (
                f"SELECT DISTINCT tm.hash, tm.content, {version_select} FROM textMap tm "
                f"{join_clause}"
                f"WHERE tm.id IN (SELECT rowid FROM {_TEXTMAP_FTS_TABLE} WHERE {_TEXTMAP_FTS_TABLE} MATCH ? AND lang=?) "
                "AND tm.lang=? "
                "AND (tm.content LIKE ? ESCAPE '\\' OR tm.content LIKE ? ESCAPE '\\') "
            )
            params_fts = [*join_params, fts_match, langCode, langCode, exact, fuzzy]
            sql_fts = _append_version_filter_clause(sql_fts, params_fts, "tm", created_version, updated_version, "textMap")
            sql_fts += f"ORDER BY {match_sort_sql}, length(tm.content) "
            params_fts.extend(match_sort_params)
            sql_fts += "LIMIT ? OFFSET ?"
            params_fts.extend([limit, offset])
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)

        return cursor.fetchall()


def countTextMapFromKeywordBySourceType(
    keyWord: str,
    langCode: int,
    source_type: str,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    """统计带来源类型筛选的 textMap 搜索结果数量。"""
    join_clause, join_params = _build_source_type_join(source_type)
    if not join_clause:
        return 0

    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        fts_match = _build_textmap_fts_match(keyWord, langCode)

        sql_like = (
            f"SELECT COUNT(DISTINCT tm.hash) FROM textMap tm "
            f"{join_clause}"
            "WHERE tm.lang=? AND (tm.content LIKE ? ESCAPE '\\' OR tm.content LIKE ? ESCAPE '\\') "
        )
        params_like = [*join_params, langCode, exact, fuzzy]
        sql_like = _append_version_filter_clause(sql_like, params_like, "tm", created_version, updated_version, "textMap")

        if _is_textmap_fts_lang_enabled(langCode) and fts_match is not None:
            sql_fts = (
                f"SELECT COUNT(DISTINCT tm.hash) FROM textMap tm "
                f"{join_clause}"
                f"WHERE tm.id IN (SELECT rowid FROM {_TEXTMAP_FTS_TABLE} WHERE {_TEXTMAP_FTS_TABLE} MATCH ? AND lang=?) "
                "AND tm.lang=? "
                "AND (tm.content LIKE ? ESCAPE '\\' OR tm.content LIKE ? ESCAPE '\\') "
            )
            params_fts = [*join_params, fts_match, langCode, langCode, exact, fuzzy]
            sql_fts = _append_version_filter_clause(sql_fts, params_fts, "tm", created_version, updated_version, "textMap")
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)

        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countTextMapFromKeywordVoice(
    keyWord: str,
    langCode: int,
    voice_filter: str | None,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    """
    计算带语音过滤的搜索结果数量，优化搜索速度

    优化措施：
    1. 使用FTS（全文搜索）与LIKE查询结合，提高搜索速度
    2. 优化FTS查询：使用JOIN代替子查询，减少查询开销
    3. 支持语音过滤，允许用户筛选带语音的文本
    4. 支持版本过滤，允许用户按游戏版本筛选结果
    5. 使用execute_with_fallback确保查询在不同环境下都能正常执行

    这些优化措施在保证计数速度的同时，不会增加数据库体积，实现了速度与体积的平衡
    """
    _ensure_fetter_voice_data()
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        fts_match = _build_textmap_fts_match(keyWord, langCode)
        voice_expr = _voice_exists_expr("tm.hash")

        # 构建LIKE查询
        sql_like, params_like = _build_textmap_count_query(
            use_fts=False,
            langCode=langCode,
            exact=exact,
            fuzzy=fuzzy,
            fts_match=fts_match,
            voice_expr=voice_expr,
            voice_filter=voice_filter,
            created_version=created_version,
            updated_version=updated_version
        )

        if _is_textmap_fts_lang_enabled(langCode) and fts_match is not None:
            # 构建FTS查询
            sql_fts, params_fts = _build_textmap_count_query(
                use_fts=True,
                langCode=langCode,
                exact=exact,
                fuzzy=fuzzy,
                fts_match=fts_match,
                voice_expr=voice_expr,
                voice_filter=voice_filter,
                created_version=created_version,
                updated_version=updated_version
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def _legacy_hasVoiceForTextHashDb(textHash: int) -> bool:
    with closing(conn.cursor()) as cursor:
        # 优化语音存在性检查：直接查询dialogue和voice表，减少子查询嵌套
        sql = (
            "SELECT 1 FROM dialogue d JOIN voice v ON v.dialogueId = d.dialogueId WHERE d.textHash = ? LIMIT 1 "
            "UNION "
            "SELECT 1 FROM fetters f JOIN voice v ON v.dialogueId = f.voiceFile AND (v.avatarId = f.avatarId OR v.avatarId = 0) WHERE f.voiceFileTextTextMapHash = ? LIMIT 1"
        )
        cursor.execute(sql, (textHash, textHash))
        return cursor.fetchone() is not None


def isTextMapHashInKeyword(textHash: int, keyword: str, langCode: int) -> bool:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select 1 from textMap "
            "where hash=? and lang=? and (content like ? escape '\\' or content like ? escape '\\') "
            "limit 1"
        )
        cursor.execute(sql, (textHash, langCode, exact, fuzzy))
        return cursor.fetchone() is not None


def selectTextMapFromTextHash(textHash: int, langs: list[int] | None = None):
    safe_text_hash = _coerce_optional_int(textHash)
    if safe_text_hash is None:
        return []

    with closing(conn.cursor()) as cursor:
        if langs is not None and len(langs) > 0:
            langStr = ','.join([str(i) for i in langs])
            sql1 = f"select content, lang from textMap where hash=? and lang in ({langStr})"
        else:
            sql1 = "select content, lang from textMap where hash=?"
        cursor.execute(sql1, (safe_text_hash,))
        return cursor.fetchall()


def getTextMapVersionInfo(textHash: int, preferred_lang: int | None = None):
    if not _has_version_id_columns("textMap"):
        return None, None
    try:
        textHash = int(textHash)
    except (TypeError, ValueError):
        return None, None
    if preferred_lang is not None:
        try:
            preferred_lang = int(preferred_lang)
        except (TypeError, ValueError):
            preferred_lang = None
    created_expr = _version_value_expr("tm", "created", "textMap")
    updated_expr = _version_value_expr("tm", "updated", "textMap")
    with closing(conn.cursor()) as cursor:
        if preferred_lang is not None:
            sql = (
                f"select {created_expr}, {updated_expr} from textMap tm "
                "where tm.hash=? and tm.lang=? limit 1"
            )
            cursor.execute(sql, (textHash, preferred_lang))
            row = cursor.fetchone()
            if row:
                # 即使版本信息为空，也返回首选语言的版本信息
                return row
        # 如果没有首选语言或首选语言没有数据，返回None
        return None, None


def _legacy_selectVoicePathFromTextHash(textHash: int):
    with closing(conn.cursor()) as cursor:
        # 先查询dialogue表
        sql_dialogue = "select voicePath from dialogue join voice on voice.dialogueId = dialogue.dialogueId where textHash=? limit 1"
        cursor.execute(sql_dialogue, (textHash,))
        match = cursor.fetchone()
        if match:
            return match[0]

        # 再查询fetters表
        sql_fetter = (
            "select voicePath from fetters join voice on voice.dialogueId = fetters.voiceFile "
            "where voiceFileTextTextMapHash=? and (fetters.avatarId=voice.avatarId or voice.avatarId=0) limit 1"
        )
        cursor.execute(sql_fetter, (textHash,))
        match = cursor.fetchone()
        if match:
            return match[0]

        return None


def getImportedTextMapLangs():
    with closing(conn.cursor()) as cursor:
        sql1 = "select id,displayName from langCode where imported=1"
        cursor.execute(sql1)
        return cursor.fetchall()


def getAllVersionValues() -> list[str]:
    values: set[str] = set()
    with closing(conn.cursor()) as cursor:
        _ensure_version_catalog_schema(cursor)
        placeholders = ",".join(["?"] * len(_VERSION_SOURCE_TABLES))

        existing_count_row = cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM {_VERSION_CATALOG_TABLE}
            WHERE source_table IN ({placeholders})
            """,
            _VERSION_SOURCE_TABLES,
        ).fetchone()
        existing_count = int(existing_count_row[0] or 0) if existing_count_row else 0
        if existing_count == 0:
            _rebuild_version_catalog(cursor, _VERSION_SOURCE_TABLES)
            conn.commit()

        rows = cursor.execute(
            f"""
            SELECT DISTINCT raw_version
            FROM {_VERSION_CATALOG_TABLE}
            WHERE source_table IN ({placeholders})
              AND COALESCE(raw_version, '') <> ''
            """,
            _VERSION_SOURCE_TABLES,
        ).fetchall()
        for (raw_value,) in rows:
            text = str(raw_value).strip()
            if text:
                values.add(text)

        if values:
            return list(values)

        # Fallback for old DBs where version_catalog creation failed.
        for table_name in ("npc", "quest", "readable", "subtitle", "textMap"):
            if not _has_version_id_columns(table_name):
                continue
            query_parts: list[str] = []
            if _has_version_id_columns(table_name) and _has_version_dim():
                query_parts.extend(
                    [
                        f"SELECT vd.raw_version AS v FROM {table_name} t "
                        f"JOIN {_VERSION_DIM_TABLE} vd ON vd.id = t.created_version_id "
                        f"WHERE t.created_version_id IS NOT NULL",
                    ]
                )
            if table_name == 'quest' and _has_version_id_columns(table_name) and _has_version_dim():
                if _table_has_column("quest_version", "updated_version_id"):
                    query_parts.extend(
                        [
                            f"SELECT vd.raw_version AS v FROM quest_version qv "
                            f"JOIN {_VERSION_DIM_TABLE} vd ON vd.id = qv.updated_version_id "
                            f"WHERE qv.updated_version_id IS NOT NULL",
                        ]
                    )
            elif table_name != 'npc' and _has_version_id_columns(table_name) and _has_version_dim():
                # For non-quest, non-npc tables, also include updated_version_id.
                query_parts.extend(
                    [
                        f"SELECT vd.raw_version AS v FROM {table_name} t "
                        f"JOIN {_VERSION_DIM_TABLE} vd ON vd.id = t.updated_version_id "
                        f"WHERE t.updated_version_id IS NOT NULL",
                    ]
                )
            if not query_parts:
                continue
            rows = cursor.execute(
                f"SELECT DISTINCT v FROM ({' UNION '.join(query_parts)})"
            ).fetchall()
            for (raw_value,) in rows:
                if raw_value is None:
                    continue
                text = str(raw_value).strip()
                if text:
                    values.add(text)
    return list(values)


def getSourceFromFetter(textHash: int, langCode: int = 1):
    with closing(conn.cursor()) as cursor:
        sql1 = ('select avatarId, content from fetters, textMap '
                'where voiceFileTextTextMapHash=? and voiceTitleTextMapHash = hash and lang=?')
        cursor.execute(sql1, (textHash, langCode))
        ans = cursor.fetchall()
        if len(ans) == 0:
            return None
        avatarId, voiceTitle = ans[0]

        avatarName = getCharterName(avatarId, langCode)
        if avatarName is None:
            return None

        return "{} · {}".format(avatarName, _normalize_output_text(voiceTitle, langCode))


def selectQuestHashSources(text_hash: int, *, source_type: str | None = None) -> list[tuple[int, str]]:
    with closing(conn.cursor()) as cursor:
        if source_type:
            rows = cursor.execute(
                "SELECT questId, source_type FROM quest_hash_map WHERE hash=? AND source_type=?",
                (text_hash, source_type),
            ).fetchall()
        else:
            rows = cursor.execute(
                "SELECT questId, source_type FROM quest_hash_map WHERE hash=?",
                (text_hash,),
            ).fetchall()
    return [(int(row[0]), str(row[1] or "")) for row in rows]


def selectEntitySourcesByTextHash(text_hash: int) -> list[tuple[int, int, int, int]]:
    with closing(conn.cursor()) as cursor:
        try:
            rows = cursor.execute(
                """
                SELECT source_type_code, entity_id, title_hash, extra
                FROM text_source_entity
                WHERE text_hash=?
                ORDER BY source_type_code, entity_id
                """,
                (text_hash,),
            ).fetchall()
        except Exception:
            return []
    return [
        (int(row[0]), int(row[1]), int(row[2]), int(row[3] or 0))
        for row in rows
        if row and row[0] is not None and row[1] is not None and row[2] is not None
    ]


def selectEntitySourcesByTitleHash(title_hash: int) -> list[tuple[int, int, int, int]]:
    with closing(conn.cursor()) as cursor:
        try:
            rows = cursor.execute(
                """
                SELECT source_type_code, entity_id, title_hash, extra
                FROM text_source_entity
                WHERE title_hash=?
                ORDER BY source_type_code, entity_id
                """,
                (title_hash,),
            ).fetchall()
        except Exception:
            return []
    return [
        (int(row[0]), int(row[1]), int(row[2]), int(row[3] or 0))
        for row in rows
        if row and row[0] is not None and row[1] is not None and row[2] is not None
    ]


def selectEntityTextHashesByEntity(source_type_code: int, entity_id: int) -> list[tuple[int, int, int, int]]:
    with closing(conn.cursor()) as cursor:
        try:
            rows = cursor.execute(
                """
                SELECT text_hash, title_hash, extra, sub_category
                FROM text_source_entity
                WHERE source_type_code=? AND entity_id=?
                ORDER BY extra, text_hash
                """,
                (int(source_type_code), int(entity_id)),
            ).fetchall()
        except Exception:
            return []
    return [
        (int(row[0]), int(row[1]), int(row[2] or 0), int(row[3] or 0))
        for row in rows
        if row and row[0] is not None and row[1] is not None
    ]


def _build_catalog_entity_base_query(
    keyword: str,
    lang_code: int,
    source_type_code: int | None = None,
    sub_category: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> tuple[str, list]:
    has_version = _has_version_dim() and _has_version_id_columns("textMap")
    version_lang_code = config.getSourceLanguage()

    sql = (
        "WITH catalog_base AS ("
        "SELECT "
        "e.entity_id, "
        "e.source_type_code, "
        "MIN(e.title_hash) AS title_hash, "
        "MIN(e.sub_category) AS sub_category, "
        "MIN(tm_title.content) AS title_text, "
    )
    params: list = []

    if has_version:
        title_actual_update_expr = (
            "lower(trim(coalesce(vdtc.version_tag, vdtc.raw_version, ''))) "
            "!= lower(trim(coalesce(vdtu.version_tag, vdtu.raw_version, '')))"
        )
        body_actual_update_expr = (
            "lower(trim(coalesce(vdbc.version_tag, vdbc.raw_version, ''))) "
            "!= lower(trim(coalesce(vdbu.version_tag, vdbu.raw_version, '')))"
        )
        sql += (
            "MIN(CASE WHEN tm_title_version.created_version_id IS NOT NULL THEN tm_title_version.created_version_id END) AS title_created_version_id, "
            "MIN(CASE WHEN vdtc.version_sort_key IS NOT NULL THEN vdtc.version_sort_key END) AS title_created_sort_key, "
            "MIN(CASE WHEN tm_body_version.created_version_id IS NOT NULL THEN tm_body_version.created_version_id END) AS body_created_version_id, "
            "MIN(CASE WHEN vdbc.version_sort_key IS NOT NULL THEN vdbc.version_sort_key END) AS body_created_sort_key, "
            f"MAX(CASE WHEN tm_title_version.updated_version_id IS NOT NULL AND {title_actual_update_expr} "
            "THEN tm_title_version.updated_version_id END) AS title_updated_version_id, "
            f"MAX(CASE WHEN tm_title_version.updated_version_id IS NOT NULL AND {title_actual_update_expr} "
            "THEN vdtu.version_sort_key END) AS title_updated_sort_key, "
            f"MAX(CASE WHEN tm_body_version.updated_version_id IS NOT NULL AND {body_actual_update_expr} "
            "THEN tm_body_version.updated_version_id END) AS body_updated_version_id, "
            f"MAX(CASE WHEN tm_body_version.updated_version_id IS NOT NULL AND {body_actual_update_expr} "
            "THEN vdbu.version_sort_key END) AS body_updated_sort_key "
        )
    else:
        sql += (
            "NULL AS title_created_version_id, NULL AS title_created_sort_key, "
            "NULL AS body_created_version_id, NULL AS body_created_sort_key, "
            "NULL AS title_updated_version_id, NULL AS title_updated_sort_key, "
            "NULL AS body_updated_version_id, NULL AS body_updated_sort_key "
        )

    sql += (
        "FROM text_source_entity e "
        "JOIN textMap tm_title ON tm_title.hash = e.title_hash AND tm_title.lang = ? "
    )
    params.append(lang_code)

    if has_version:
        sql += (
            "LEFT JOIN textMap tm_title_version ON tm_title_version.hash = e.title_hash AND tm_title_version.lang = ? "
            "LEFT JOIN textMap tm_body_version ON tm_body_version.hash = e.text_hash AND tm_body_version.lang = ? "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdtc ON vdtc.id = tm_title_version.created_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdtu ON vdtu.id = tm_title_version.updated_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdbc ON vdbc.id = tm_body_version.created_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdbu ON vdbu.id = tm_body_version.updated_version_id "
        )
        params.extend([version_lang_code, version_lang_code])

    sql += "WHERE 1=1 "

    if keyword:
        exact, fuzzy = _build_like_patterns(keyword, lang_code)
        sql += "AND (tm_title.content LIKE ? ESCAPE '\\' OR tm_title.content LIKE ? ESCAPE '\\') "
        params.extend([exact, fuzzy])

    if source_type_code is not None:
        sql += "AND e.source_type_code = ? "
        params.append(int(source_type_code))
    if sub_category is not None:
        sql += "AND e.sub_category = ? "
        params.append(int(sub_category))

    sql += (
        "GROUP BY e.entity_id, e.source_type_code), "
        "catalog_final AS ("
        "SELECT "
        "base.entity_id, "
        "base.source_type_code, "
        "base.title_hash, "
        "base.sub_category, "
        "base.title_text, "
        "CASE "
        "WHEN base.title_created_sort_key IS NULL THEN base.body_created_version_id "
        "WHEN base.body_created_sort_key IS NULL THEN base.title_created_version_id "
        "WHEN base.title_created_sort_key <= base.body_created_sort_key THEN base.title_created_version_id "
        "ELSE base.body_created_version_id "
        "END AS created_version_id, "
        "CASE "
        "WHEN base.title_updated_sort_key IS NULL THEN base.body_updated_version_id "
        "WHEN base.body_updated_sort_key IS NULL THEN base.title_updated_version_id "
        "WHEN base.title_updated_sort_key >= base.body_updated_sort_key THEN base.title_updated_version_id "
        "ELSE base.body_updated_version_id "
        "END AS updated_version_id "
        "FROM catalog_base base) "
    )
    return sql, params


def selectCatalogEntities(
    keyword: str,
    lang_code: int,
    source_type_code: int | None = None,
    sub_category: int | None = None,
    limit: int = 50,
    offset: int = 0,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> list[tuple]:
    """Search entities by title name. Returns (entity_id, source_type_code, title_hash, sub_category, title_text, created_version, updated_version)."""
    created = _normalize_version_filter(created_version)
    updated = _normalize_version_filter(updated_version)
    has_version = _has_version_dim() and _has_version_id_columns("textMap")
    if not has_version and (created or updated):
        return []

    base_sql, params = _build_catalog_entity_base_query(
        keyword,
        lang_code,
        source_type_code=source_type_code,
        sub_category=sub_category,
        created_version=created_version,
        updated_version=updated_version,
    )

    with closing(conn.cursor()) as cursor:
        if has_version:
            sql = (
                base_sql +
                "SELECT "
                "base.entity_id, "
                "base.source_type_code, "
                "base.title_hash, "
                "base.sub_category, "
                "base.title_text, "
                "vdc.raw_version AS created_version, "
                "vdu.raw_version AS updated_version "
                "FROM catalog_final base "
                f"LEFT JOIN {_VERSION_DIM_TABLE} vdc ON vdc.id = base.created_version_id "
                f"LEFT JOIN {_VERSION_DIM_TABLE} vdu ON vdu.id = base.updated_version_id "
                "WHERE 1=1 "
            )
        else:
            sql = (
                base_sql +
                "SELECT "
                "base.entity_id, "
                "base.source_type_code, "
                "base.title_hash, "
                "base.sub_category, "
                "base.title_text, "
                "NULL AS created_version, "
                "NULL AS updated_version "
                "FROM catalog_final base "
                "WHERE 1=1 "
            )

        if created:
            sql += "AND coalesce(vdc.version_tag, '') = ? "
            params.append(created)
        if updated:
            sql += "AND coalesce(vdu.version_tag, '') = ? "
            params.append(updated)

        sql += "ORDER BY length(coalesce(base.title_text, '')), coalesce(base.title_text, '') LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        try:
            cursor.execute(sql, params)
            return cursor.fetchall()
        except Exception:
            return []


def countCatalogEntities(
    keyword: str,
    lang_code: int,
    source_type_code: int | None = None,
    sub_category: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    """Count entities matching title name search."""
    created = _normalize_version_filter(created_version)
    updated = _normalize_version_filter(updated_version)
    has_version = _has_version_dim() and _has_version_id_columns("textMap")
    if not has_version and (created or updated):
        return 0

    sql, params = _build_catalog_entity_base_query(
        keyword,
        lang_code,
        source_type_code=source_type_code,
        sub_category=sub_category,
        created_version=created_version,
        updated_version=updated_version,
    )
    if has_version:
        sql += (
            "SELECT COUNT(*) FROM catalog_final base "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdc ON vdc.id = base.created_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdu ON vdu.id = base.updated_version_id "
            "WHERE 1=1 "
        )
    else:
        sql += "SELECT COUNT(*) FROM catalog_final base WHERE 1=1 "
    if created:
        sql += "AND coalesce(vdc.version_tag, '') = ? "
        params.append(created)
    if updated:
        sql += "AND coalesce(vdu.version_tag, '') = ? "
        params.append(updated)

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(sql, params)
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0


def getCatalogEntityVersionInfo(source_type_code: int, entity_id: int, version_lang_code: int | None = None):
    has_version = _has_version_dim() and _has_version_id_columns("textMap")
    if not has_version:
        return None, None

    try:
        source_type_code = int(source_type_code)
        entity_id = int(entity_id)
    except (TypeError, ValueError):
        return None, None

    version_lang = config.getSourceLanguage() if version_lang_code is None else version_lang_code
    try:
        version_lang = int(version_lang)
    except (TypeError, ValueError):
        version_lang = config.getSourceLanguage()

    title_actual_update_expr = (
        "lower(trim(coalesce(vdtc.version_tag, vdtc.raw_version, ''))) "
        "!= lower(trim(coalesce(vdtu.version_tag, vdtu.raw_version, '')))"
    )
    body_actual_update_expr = (
        "lower(trim(coalesce(vdbc.version_tag, vdbc.raw_version, ''))) "
        "!= lower(trim(coalesce(vdbu.version_tag, vdbu.raw_version, '')))"
    )

    sql = (
        "WITH entity_versions AS ("
        "SELECT "
        "MIN(CASE WHEN tm_title.created_version_id IS NOT NULL THEN tm_title.created_version_id END) AS title_created_version_id, "
        "MIN(CASE WHEN vdtc.version_sort_key IS NOT NULL THEN vdtc.version_sort_key END) AS title_created_sort_key, "
        "MIN(CASE WHEN tm_body.created_version_id IS NOT NULL THEN tm_body.created_version_id END) AS body_created_version_id, "
        "MIN(CASE WHEN vdbc.version_sort_key IS NOT NULL THEN vdbc.version_sort_key END) AS body_created_sort_key, "
        f"MAX(CASE WHEN tm_title.updated_version_id IS NOT NULL AND {title_actual_update_expr} THEN tm_title.updated_version_id END) AS title_updated_version_id, "
        f"MAX(CASE WHEN tm_title.updated_version_id IS NOT NULL AND {title_actual_update_expr} THEN vdtu.version_sort_key END) AS title_updated_sort_key, "
        f"MAX(CASE WHEN tm_body.updated_version_id IS NOT NULL AND {body_actual_update_expr} THEN tm_body.updated_version_id END) AS body_updated_version_id, "
        f"MAX(CASE WHEN tm_body.updated_version_id IS NOT NULL AND {body_actual_update_expr} THEN vdbu.version_sort_key END) AS body_updated_sort_key "
        "FROM text_source_entity e "
        "LEFT JOIN textMap tm_title ON tm_title.hash = e.title_hash AND tm_title.lang = ? "
        "LEFT JOIN textMap tm_body ON tm_body.hash = e.text_hash AND tm_body.lang = ? "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdtc ON vdtc.id = tm_title.created_version_id "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdtu ON vdtu.id = tm_title.updated_version_id "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdbc ON vdbc.id = tm_body.created_version_id "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdbu ON vdbu.id = tm_body.updated_version_id "
        "WHERE e.source_type_code = ? AND e.entity_id = ?), "
        "catalog_final AS ("
        "SELECT "
        "CASE "
        "WHEN base.title_created_sort_key IS NULL THEN base.body_created_version_id "
        "WHEN base.body_created_sort_key IS NULL THEN base.title_created_version_id "
        "WHEN base.title_created_sort_key <= base.body_created_sort_key THEN base.title_created_version_id "
        "ELSE base.body_created_version_id "
        "END AS created_version_id, "
        "CASE "
        "WHEN base.title_updated_sort_key IS NULL THEN base.body_updated_version_id "
        "WHEN base.body_updated_sort_key IS NULL THEN base.title_updated_version_id "
        "WHEN base.title_updated_sort_key >= base.body_updated_sort_key THEN base.title_updated_version_id "
        "ELSE base.body_updated_version_id "
        "END AS updated_version_id "
        "FROM entity_versions base) "
        "SELECT vdc.raw_version AS created_version, vdu.raw_version AS updated_version "
        "FROM catalog_final base "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdc ON vdc.id = base.created_version_id "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdu ON vdu.id = base.updated_version_id"
    )

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(sql, (version_lang, version_lang, source_type_code, entity_id))
            row = cursor.fetchone()
            if not row:
                return None, None
            return row[0], row[1]
        except Exception:
            return None, None


def getCharterName(avatarId: int, langCode: int = 1):
    """
    获取角色名称
    """
    cache_key = f"{avatarId}:{langCode}:{_get_gender_cache_key()}"
    if cache_key in _CACHE["names"]["characters"]:
        return _CACHE["names"]["characters"][cache_key]
    result = _normalize_output_text(getCharacterNameRaw(avatarId, langCode), langCode)
    _CACHE["names"]["characters"][cache_key] = result
    return result


def getWanderName(langCode: int = 1):
    """
    获取旅行者名称
    """
    cache_key = f"{langCode}:{_get_gender_cache_key()}"
    if cache_key in _CACHE["names"]["wander"]:
        return _CACHE["names"]["wander"][cache_key]
    _CACHE["names"]["wander"][cache_key] = getCharterName(10000075, langCode)
    return _CACHE["names"]["wander"][cache_key]


def getTravellerName(langCode: int = 1):
    """
    获取空旅行者名称
    """
    cache_key = f"{langCode}:{_get_gender_cache_key()}"
    if cache_key in _CACHE["names"]["traveller"]:
        return _CACHE["names"]["traveller"][cache_key]
    _CACHE["names"]["traveller"][cache_key] = getCharterName(10000005, langCode)
    return _CACHE["names"]["traveller"][cache_key]


def getTalkInfo(textHash):
    with closing(conn.cursor()) as cursor:
        try:
            # 确保 textHash 是整数类型
            textHash_int = int(textHash)
            sql1 = 'select talkerType, talkerId, talkId, coopQuestId from dialogue where textHash=?'
            cursor.execute(sql1, (textHash_int,))
            ans = cursor.fetchall()
            if len(ans) == 0:
                return None
            talkerType, talkerId, talkId, coopQuestId = ans[0]
            return talkId, talkerType, talkerId, coopQuestId
        except (ValueError, TypeError):
            return None


def getTalkerName(talkerType: str, talkerId: int, langCode: int = 1):
    with closing(conn.cursor()) as cursor:
        talkerName = None
        if talkerType == "TALK_ROLE_NPC":
            sqlGetNpcName = 'select content from npc, textMap where npcId = ? and textHash = hash and lang = ?'
            cursor.execute(sqlGetNpcName, (talkerId, langCode))
            ansNpcName = cursor.fetchall()
            if len(ansNpcName) > 0:
                talkerName = ansNpcName[0][0]
        elif talkerType == "TALK_ROLE_PLAYER":
            talkerName = "主角"
        elif talkerType == "TALK_ROLE_MATE_AVATAR":
            talkerName = "同伴"

        return _normalize_output_text(talkerName, langCode)


def getTalkerNameFromTextHash(textHash: int, langCode: int = 1):
    talkInfo = getTalkInfo(textHash)
    if talkInfo is not None:
        _, talkerType, talkerId, _ = talkInfo
        talkerName = getTalkerName(talkerType, talkerId, langCode)
        if talkerName:
            return talkerName

    with closing(conn.cursor()) as cursor:
        sql = "select avatarId from fetters where voiceFileTextTextMapHash=? limit 1"
        cursor.execute(sql, (textHash,))
        ans = cursor.fetchall()
        if len(ans) > 0:
            avatarId = ans[0][0]
            return getCharterName(avatarId, langCode)

    return None


def getTalkQuestId(talkId: int):
    with closing(conn.cursor()) as cursor:
        sql2 = ('select quest.questId from questTalk, quest '
                'where talkId=? and coalesce(questTalk.coopQuestId, 0)=0 and quest.questId=questTalk.questId')
        cursor.execute(sql2, (talkId,))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        return ans2[0][0]


def getQuestName(questId, langCode) -> str:
    with closing(conn.cursor()) as cursor:
        sql2 = ('select content from quest, textMap '
                'where quest.questId=? and titleTextMapHash=hash and lang=?')
        cursor.execute(sql2, (questId, langCode))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return "对话文本"

        questTitle = _normalize_output_text(ans2[0][0], langCode) or "对话文本"

        sql3 = 'select chapterTitleTextMapHash,chapterNumTextMapHash from chapter, quest where questId=? and quest.chapterId=chapter.chapterId'
        cursor.execute(sql3, (questId,))
        ans3 = cursor.fetchall()
        if len(ans3) == 0:
            return questTitle
        chapterTitleTextMapHash, chapterNumTextMapHash = ans3[0]

        sql4 = 'select content from textMap where hash=? and lang=?'
        cursor.execute(sql4, (chapterTitleTextMapHash, langCode))
        ans4 = cursor.fetchall()
        if len(ans4) == 0:
            return questTitle

        chapterTitleText = _normalize_output_text(ans4[0][0], langCode)

        cursor.execute(sql4, (chapterNumTextMapHash, langCode))
        ans5 = cursor.fetchall()

        if len(ans5) > 0:
            chapterNumText = _normalize_output_text(ans5[0][0], langCode)
            questCompleteName = '{} · {} · {}'.format(chapterNumText, chapterTitleText, questTitle)
        else:
            questCompleteName = '{} · {}'.format(chapterTitleText, questTitle)

        return questCompleteName


def getTalkQuestName(talkId: int, langCode: int = 1) -> str:
    questId = getTalkQuestId(talkId)
    if questId is None:
        return "对话文本"
    return getQuestName(questId, langCode)


def getCoopTalkQuestName(coopQuestId, langCode) -> str:
    return getQuestName(coopQuestId // 100, langCode)


def getSourceFromDialogue(textHash: int, langCode: int = 1) -> str | None:
    talkInfo = getTalkInfo(textHash)
    if talkInfo is None:
        return None

    talkId, talkerType, talkerId, coopQuestId = talkInfo
    talkerName = getTalkerName(talkerType, talkerId, langCode)

    if coopQuestId is None:
        questCompleteName = getTalkQuestName(talkId, langCode)
    else:
        questCompleteName = getCoopTalkQuestName(coopQuestId, langCode)

    if talkerName is None:
        return questCompleteName
    else:
        return f"{talkerName}, {questCompleteName}"


def getManualTextMap(placeHolderName, lang):
    with closing(conn.cursor()) as cursor:
        sql1 = 'select content from manualTextMap, textMap where textMapId=? and textHash = hash and lang=?'
        cursor.execute(sql1, (placeHolderName, lang))
        ans = cursor.fetchall()
        if len(ans) == 0:
            return None
        return ans[0][0]


def selectVoiceFromKeywordPaged(keyWord: str, page: int, size: int, langCode: int):
    """
    分页搜索语音
    """
    with closing(conn.cursor()) as cursor:
        # 构建查询
        sql = """
        SELECT DISTINCT v.voicePath, d.textHash, tm.content
        FROM voice v
        JOIN dialogue d ON v.dialogueId = d.dialogueId
        JOIN textMap tm ON tm.hash = d.textHash
        WHERE tm.lang = ? AND (tm.content LIKE ? ESCAPE '\\' OR tm.content LIKE ? ESCAPE '\\')
        ORDER BY tm.content
        LIMIT ? OFFSET ?
        """

        # 构建LIKE模式
        exact, fuzzy = _build_like_patterns(keyWord, langCode)

        # 计算偏移量
        offset = (page - 1) * size

        # 执行查询
        cursor.execute(sql, (langCode, exact, fuzzy, size, offset))

        # 处理结果
        results = []
        for voicePath, textHash, content in cursor.fetchall():
            results.append({
                'voicePath': voicePath,
                'textHash': textHash,
                'content': content
            })

        return results


def _legacy_getVoicePath(voice_hash: str, lang: int):
    """
    获取语音路径
    """
    with closing(conn.cursor()) as cursor:
        # 先查询dialogue表
        sql_dialogue = """
        SELECT v.voicePath
        FROM voice v
        JOIN dialogue d ON v.dialogueId = d.dialogueId
        WHERE d.textHash = ?
        LIMIT 1
        """
        cursor.execute(sql_dialogue, (voice_hash,))
        result = cursor.fetchone()
        if result:
            return result[0]

        # 再查询fetters表
        sql_fetter = """
        SELECT v.voicePath
        FROM voice v
        JOIN fetters f ON v.dialogueId = f.voiceFile
        WHERE f.voiceFileTextTextMapHash = ? AND (v.avatarId = f.avatarId OR v.avatarId = 0)
        LIMIT 1
        """
        cursor.execute(sql_fetter, (voice_hash,))
        result = cursor.fetchone()
        if result:
            return result[0]

        return None


def getTextMapByHash(hash_val: str, lang: int):
    """
    根据哈希值获取文本映射
    """
    with closing(conn.cursor()) as cursor:
        sql = """
        SELECT content
        FROM textMap
        WHERE hash = ? AND lang = ?
        LIMIT 1
        """
        cursor.execute(sql, (hash_val, lang))
        result = cursor.fetchone()
        return result[0] if result else None


# 从 dbBuild/versioning.py 导入 get_current_version 函数
try:
    import sys
    import os
    import importlib.util

    # 使用 importlib 加载模块
    versioning_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dbBuild', 'versioning.py')
    spec = importlib.util.spec_from_file_location("versioning", versioning_path)
    if spec and spec.loader:
        versioning = importlib.util.module_from_spec(spec)
        sys.modules["versioning"] = versioning
        spec.loader.exec_module(versioning)
        get_current_version = versioning.get_current_version
    else:
        raise ImportError("Failed to create module spec")
except Exception:
    # 如果导入失败，定义一个默认函数
    def get_current_version():
        return "unknown"


def getVersionData(lang_code: int | None = None, include_current: bool = False):
    """
    获取版本数据

    Args:
        lang_code: 语言代码，用于过滤版本（None表示获取所有版本）
        include_current: 是否包含当前版本信息

    Returns:
        如果include_current为True：返回包含versions和currentVersion的字典
        如果include_current为False：返回版本值列表
    """
    if lang_code is None:
        versions = getAllVersionValues()
    else:
        # 根据语言代码从 quest_version 表中获取版本
        values: set[str] = set()
        with closing(conn.cursor()) as cursor:
            # 从 quest_version 表中获取指定语言的更新版本
            cursor.execute(
                """
                SELECT DISTINCT vd.raw_version
                FROM quest_version qv
                JOIN version_dim vd ON vd.id = qv.updated_version_id
                WHERE qv.lang = ?
                  AND vd.raw_version IS NOT NULL
                """,
                (lang_code,)
            )
            rows = cursor.fetchall()
            for (raw_value,) in rows:
                text = str(raw_value).strip()
                if text:
                    values.add(text)

            # 从 quest 表中获取创建版本（共通版本）
            cursor.execute(
                """
                SELECT DISTINCT vd.raw_version
                FROM quest q
                JOIN version_dim vd ON vd.id = q.created_version_id
                WHERE vd.raw_version IS NOT NULL
                """
            )
            rows = cursor.fetchall()
            for (raw_value,) in rows:
                text = str(raw_value).strip()
                if text:
                    values.add(text)

        versions = list(values)

    if include_current:
        # 构建版本数据
        version_data = {
            'versions': versions,
            'currentVersion': get_current_version()
        }
        return version_data
    else:
        return versions


def getTalkContent(talkId: int, coopQuestId: int | None):
    with closing(conn.cursor()) as cursor:
        if coopQuestId is None:
            sql1 = ('select textHash, talkerType, talkerId, dialogueId '
                    'from dialogue where talkId = ? and coopQuestId is null order by dialogueId')
            cursor.execute(sql1, (talkId,))
        else:
            sql1 = ('select textHash, talkerType, talkerId, dialogueId '
                    'from dialogue where talkId = ? and coopQuestId = ? order by dialogueId')
            cursor.execute(sql1, (talkId, coopQuestId))
        ans = cursor.fetchall()
        return ans if len(ans) > 0 else None


def countTalkContent(talkId: int, coopQuestId: int | None) -> int:
    with closing(conn.cursor()) as cursor:
        if coopQuestId is None:
            cursor.execute(
                "select count(*) from dialogue where talkId = ? and coopQuestId is null",
                (talkId,),
            )
        else:
            cursor.execute(
                "select count(*) from dialogue where talkId = ? and coopQuestId = ?",
                (talkId, coopQuestId),
            )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def getDialogueInfoById(dialogueId: int):
    with closing(conn.cursor()) as cursor:
        row = cursor.execute(
            "select textHash, talkerType, talkerId, dialogueId, talkId, coopQuestId "
            "from dialogue where dialogueId = ? limit 1",
            (dialogueId,),
        ).fetchone()
        return row


def countDialogueGroupContent(
    talkId: int,
    coopQuestId: int | None,
    dialogueIdFallback: int | None = None,
) -> int:
    if dialogueIdFallback is not None:
        with closing(conn.cursor()) as cursor:
            row = cursor.execute(
                "select count(*) from dialogue where dialogueId = ?",
                (dialogueIdFallback,),
            ).fetchone()
            return int(row[0]) if row else 0
    return countTalkContent(talkId, coopQuestId)


def selectTalkContentPaged(
    talkId: int,
    coopQuestId: int | None,
    limit: int,
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        params: list[int] = [talkId]
        if coopQuestId is None:
            sql = (
                "select textHash, talkerType, talkerId, dialogueId "
                "from dialogue where talkId = ? and coopQuestId is null "
                "order by dialogueId"
            )
        else:
            sql = (
                "select textHash, talkerType, talkerId, dialogueId "
                "from dialogue where talkId = ? and coopQuestId = ? "
                "order by dialogueId"
            )
            params.append(coopQuestId)

        if limit > 0:
            sql += " limit ?"
            params.append(int(limit))
        if offset is not None and offset > 0:
            sql += " offset ?"
            params.append(int(offset))

        cursor.execute(sql, params)
        return cursor.fetchall()


def selectDialogueGroupContentPaged(
    talkId: int,
    coopQuestId: int | None,
    dialogueIdFallback: int | None,
    limit: int,
    offset: int | None = None,
):
    if dialogueIdFallback is not None:
        with closing(conn.cursor()) as cursor:
            params: list[int] = [int(dialogueIdFallback)]
            sql = (
                "select textHash, talkerType, talkerId, dialogueId "
                "from dialogue where dialogueId = ? "
                "order by dialogueId"
            )
            if limit > 0:
                sql += " limit ?"
                params.append(int(limit))
            if offset is not None and offset > 0:
                sql += " offset ?"
                params.append(int(offset))
            cursor.execute(sql, params)
            return cursor.fetchall()
    return selectTalkContentPaged(talkId, coopQuestId, limit, offset)


def getDialogueGroupVersionInfo(
    talkId: int,
    coopQuestId: int | None,
    dialogueIdFallback: int | None = None,
    langCode: int | None = None,
) -> tuple[str | None, str | None]:
    source_lang_code = config.getSourceLanguage() if langCode is None else langCode
    with closing(conn.cursor()) as cursor:
        if dialogueIdFallback is not None:
            where_sql = "dg.dialogueId = ?"
            params: list[object] = [source_lang_code, dialogueIdFallback]
            updated_params: list[object] = [source_lang_code, dialogueIdFallback]
        elif coopQuestId is None:
            where_sql = "dg.talkId = ? and dg.coopQuestId is null"
            params = [source_lang_code, talkId]
            updated_params = [source_lang_code, talkId]
        else:
            where_sql = "dg.talkId = ? and dg.coopQuestId = ?"
            params = [source_lang_code, talkId, coopQuestId]
            updated_params = [source_lang_code, talkId, coopQuestId]

        created_row = cursor.execute(
            "select vd.raw_version "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {where_sql} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1",
            params,
        ).fetchone()
        updated_row = cursor.execute(
            "select vd.raw_version "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.updated_version_id "
            f"where {where_sql} and tm.updated_version_id is not null "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc "
            "limit 1",
            updated_params,
        ).fetchone()
        created_raw = str(created_row[0]) if created_row and created_row[0] is not None else None
        updated_raw = str(updated_row[0]) if updated_row and updated_row[0] is not None else None
        return created_raw, updated_raw


def getTalkContentPageForTextHash(
    textHash: int,
    talkId: int,
    coopQuestId: int | None,
    page_size: int,
) -> int:
    safe_page_size = max(1, int(page_size))
    with closing(conn.cursor()) as cursor:
        if coopQuestId is None:
            cursor.execute(
                """
                select count(*)
                from dialogue
                where talkId = ?
                  and coopQuestId is null
                  and dialogueId < (
                    select min(dialogueId)
                    from dialogue
                    where textHash = ?
                      and talkId = ?
                      and coopQuestId is null
                  )
                """,
                (talkId, textHash, talkId),
            )
        else:
            cursor.execute(
                """
                select count(*)
                from dialogue
                where talkId = ?
                  and coopQuestId = ?
                  and dialogueId < (
                    select min(dialogueId)
                    from dialogue
                    where textHash = ?
                      and talkId = ?
                      and coopQuestId = ?
                  )
                """,
                (talkId, coopQuestId, textHash, talkId, coopQuestId),
            )
        row = cursor.fetchone()
        before_count = int(row[0]) if row and row[0] is not None else 0
        return before_count // safe_page_size + 1


def isTextHashFromQuest(textHash: int) -> bool:
    """
    检查文本哈希是否来自任务相关的对话
    """
    talkInfo = getTalkInfo(textHash)
    if talkInfo is None:
        return False
    talkId, _, _, coopQuestId = talkInfo
    questId = getTalkQuestId(talkId)
    return questId is not None


def getLangCodeMap():
    with closing(conn.cursor()) as cursor:
        sql = "select id, codeName from langCode"
        cursor.execute(sql)
        rows = cursor.fetchall()
        mapping = {}
        for row in rows:
            if not row[1]:
                continue
            match = re.match(r'^Text(?:Map)?(.+)\.json$', str(row[1]).strip(), re.IGNORECASE)
            if match:
                mapping[row[0]] = match.group(1).upper()
        return mapping


def selectReadableFromKeyword(
    keyword: str,
    langCode: int,
    langStr: str,
    limit: int | None = None,
    offset: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
    category: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        match_sort_sql, match_sort_params = _build_match_sort_case("content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('content', langCode)})"
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return []
        lang_placeholders = ",".join(["?"] * len(readable_langs))
        version_select = _version_select_expr("readable", "readable")
        sql = (
            f"select fileName, content, titleTextMapHash, readableId, {version_select} from readable "
            f"where lang in ({lang_placeholders}) and (content like ? escape '\\' or content like ? escape '\\') "
        )
        params = []
        for lang in readable_langs:
            params.append(lang)
        params.append(exact)
        params.append(fuzzy)
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "readable",
            created_version,
            updated_version,
            "readable",
        )
        sql = _append_readable_category_filter_clause(sql, params, "readable", category)
        sql += f"order by {match_sort_sql}, {normalized_length_sql} "
        params.extend(match_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(int(limit))
        if offset is not None and offset > 0:
            sql += " offset ?"
            params.append(int(offset))
        cursor.execute(sql, params)
        return cursor.fetchall()


def countReadableFromKeyword(
    keyword: str,
    langCode: int,
    langStr: str,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return 0
        lang_placeholders = ",".join(["?"] * len(readable_langs))
        sql = (
            "select count(*) from readable "
            f"where lang in ({lang_placeholders}) and (content like ? escape '\\' or content like ? escape '\\')"
        )
        params = readable_langs + [exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "readable",
            created_version,
            updated_version,
            "readable",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def selectReadableFromFileName(fileName: str, langs: list[str]):
    with closing(conn.cursor()) as cursor:
        readable_langs = _expand_readable_langs(langs)
        if not readable_langs:
            return []
        placeholders = ','.join(['?'] * len(readable_langs))
        sql = f"select content, lang from readable where fileName=? and lang in ({placeholders}) order by lang"
        params = [fileName] + readable_langs
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableFromReadableId(readableId: int, langs: list[str]):
    with closing(conn.cursor()) as cursor:
        readable_langs = _expand_readable_langs(langs)
        if not readable_langs:
            return []
        placeholders = ','.join(['?'] * len(readable_langs))
        sql = f"select content, lang from readable where readableId=? and lang in ({placeholders}) order by lang"
        params = [readableId] + readable_langs
        cursor.execute(sql, params)
        return cursor.fetchall()


def getReadableInfo(readableId: int | None = None, fileName: str | None = None):
    with closing(conn.cursor()) as cursor:
        if readableId is not None:
            sql = (
                "select fileName, titleTextMapHash, readableId "
                "from readable "
                "where readableId=? "
                "order by case when titleTextMapHash is null then 1 else 0 end, fileName "
                "limit 1"
            )
            cursor.execute(sql, (readableId,))
        elif fileName is not None:
            sql = (
                "select fileName, titleTextMapHash, readableId "
                "from readable where fileName=? "
                "order by case when titleTextMapHash is null then 1 else 0 end "
                "limit 1"
            )
            cursor.execute(sql, (fileName,))
        else:
            return None
        ans = cursor.fetchall()
        if len(ans) > 0:
            return ans[0]
        return None


def resolveReadableTitleHash(readableId: int | None = None, fileName: str | None = None):
    with closing(conn.cursor()) as cursor:
        if readableId is not None:
            row = cursor.execute(
                "select titleTextMapHash from readable "
                "where readableId=? and titleTextMapHash is not null "
                "limit 1",
                (readableId,),
            ).fetchone()
            if row and row[0] is not None:
                return row[0]
        if fileName is not None:
            row = cursor.execute(
                "select titleTextMapHash from readable "
                "where fileName=? and titleTextMapHash is not null "
                "limit 1",
                (fileName,),
            ).fetchone()
            if row and row[0] is not None:
                return row[0]
        return None


def getReadableInfoByTitleHash(titleTextMapHash: int, category: str | None = None):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select fileName, titleTextMapHash, readableId "
            "from readable where titleTextMapHash=? "
        )
        params: list[object] = [int(titleTextMapHash)]
        sql = _append_readable_category_filter_clause(sql, params, "readable", category)
        sql += (
            "order by case when readableId is null then 1 else 0 end, "
            "readableId, fileName limit 1"
        )
        row = cursor.execute(sql, params).fetchone()
        return row if row else None


def selectReadableRefsByTitleHash(titleTextMapHash: int, category: str | None = None):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select min(fileName) as fileName, titleTextMapHash, readableId "
            "from readable where titleTextMapHash=? "
        )
        params: list[object] = [int(titleTextMapHash)]
        sql = _append_readable_category_filter_clause(sql, params, "readable", category)
        sql += (
            "group by titleTextMapHash, readableId "
            "order by case when readableId is null then 1 else 0 end, "
            "readableId, fileName"
        )
        rows = cursor.execute(sql, params).fetchall()
        return rows


def selectReadableRefsByFileNamePrefix(prefix: str | list[str] | tuple[str, ...]):
    if isinstance(prefix, (list, tuple)):
        raw_prefixes = prefix
    else:
        raw_prefixes = [prefix]

    prefixes: list[str] = []
    seen: set[str] = set()
    for value in raw_prefixes:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        prefixes.append(text)

    if not prefixes:
        return []

    with closing(conn.cursor()) as cursor:
        clauses = " or ".join(["fileName like ?"] * len(prefixes))
        params = [f"{item}%" for item in prefixes]
        sql = (
            "select min(fileName) as fileName, titleTextMapHash, readableId "
            f"from readable where {clauses} "
            "group by titleTextMapHash, readableId "
            "order by case when readableId is null then 1 else 0 end, readableId, fileName"
        )
        return cursor.execute(sql, params).fetchall()


def getReadableVersionInfo(readableId: int | None = None, fileName: str | None = None):
    if not _has_version_id_columns("readable"):
        return None, None
    created_expr = _version_value_expr("r", "created", "readable")
    updated_expr = _version_value_expr("r", "updated", "readable")
    with closing(conn.cursor()) as cursor:
        if readableId is not None:
            sql = (
                f"select {created_expr}, {updated_expr} from readable r "
                "where r.readableId=? limit 1"
            )
            cursor.execute(sql, (readableId,))
        elif fileName is not None:
            sql = (
                f"select {created_expr}, {updated_expr} from readable r "
                "where r.fileName=? limit 1"
            )
            cursor.execute(sql, (fileName,))
        else:
            return None, None
        row = cursor.fetchone()
        if row:
            return row
        return None, None


def getTextMapContent(textHash: int, langCode: int):
    with closing(conn.cursor()) as cursor:
        sql = "select content from textMap where hash=? and lang=?"
        cursor.execute(sql, (textHash, langCode))
        ans = cursor.fetchall()
        if len(ans) > 0:
            return ans[0][0]
        return None


def selectQuestByTitleKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        match_sort_sql, match_sort_params = _build_match_sort_case("textMap.content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('textMap.content', langCode)})"
        sql = (
            f"select quest.questId, textMap.content, {source_type_select} as source_type, {version_select} from quest "
            "join textMap on quest.titleTextMapHash=textMap.hash "
            "where textMap.lang=? and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') "
        )
        params = [langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql,
            params,
            "quest",
            created_version,
            updated_version,
            "quest",
        )
        sql = _append_quest_source_type_filter_clause(sql, params, "quest", source_type)
        sql += (
            f"order by {match_sort_sql}, {normalized_length_sql} "
            "limit 200"
        )
        params.extend(match_sort_params)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByIdContains(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        escaped = _escape_like(keyword)
        pattern = f"%{escaped}%"
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        match_sort_sql, match_sort_params = _build_match_sort_case("cast(quest.questId as text)", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('cast(quest.questId as text)', langCode)})"
        sql = (
            f"select quest.questId, textMap.content, {source_type_select} as source_type, {version_select} "
            "from quest "
            "left join textMap on quest.titleTextMapHash=textMap.hash and textMap.lang=? "
            "where cast(quest.questId as text) like ? escape '\\' "
        )
        params = [langCode, pattern]
        sql = _append_version_filter_clause(
            sql,
            params,
            "quest",
            created_version,
            updated_version,
            "quest",
            langCode,
        )
        sql = _append_quest_source_type_filter_clause(sql, params, "quest", source_type)
        sql += f"order by {match_sort_sql}, {normalized_length_sql} limit 200"
        params.extend(match_sort_params)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByContentKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        match_sql_parts: list[str] = []
        if _table_has_column("quest", "descTextMapHash"):
            match_sql_parts.append(
                "select q.questId as questId, 0 as match_rank, descText.content as matched_text "
                "from quest q "
                "join textMap descText on q.descTextMapHash=descText.hash "
                "where descText.lang=? "
                "and (descText.content like ? escape '\\' or descText.content like ? escape '\\') "
            )
        match_sql_parts.append(
            "select distinct qt.questId as questId, 1 as match_rank, dialogueText.content as matched_text "
            "from questTalk qt "
            "join dialogue d on d.talkId = qt.talkId "
            "and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "join textMap dialogueText on dialogueText.hash=d.textHash "
            "where dialogueText.lang=? "
            "and (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
        )
        match_sql = " union all ".join(match_sql_parts)
        sql = (
            "with matched as ("
            + match_sql
            + "), ranked as ("
            "select questId, min(match_rank) as best_rank "
            "from matched "
            "group by questId"
            ") "
            f"select quest.questId, quest.titleTextMapHash, titleText.content, "
            "(select min(m2.matched_text) from matched m2 "
            "where m2.questId=ranked.questId and m2.match_rank=ranked.best_rank) as matched_text, "
            f"{source_type_select} as source_type, {version_select}, ranked.best_rank "
            "from ranked "
            "join quest on ranked.questId=quest.questId "
            "left join textMap titleText on quest.titleTextMapHash=titleText.hash and titleText.lang=? "
        )
        params: list = []
        if _table_has_column("quest", "descTextMapHash"):
            params.extend([langCode, exact, fuzzy])
        params.extend([langCode, exact, fuzzy, langCode])
        sql = _append_version_filter_clause(
            sql,
            params,
            "quest",
            created_version,
            updated_version,
            "quest",
            langCode,
        )
        sql = _append_quest_source_type_filter_clause(sql, params, "quest", source_type)
        sql += "order by ranked.best_rank, quest.questId limit 200"
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByNpcSpeakerKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        sql = (
            "select distinct quest.questId, titleText.content, "
            f"{source_type_select} as source_type, "
            f"{version_select} "
            "from quest "
            "join questTalk qt on qt.questId = quest.questId "
            "join dialogue d on d.talkId = qt.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "join npc on d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = npc.npcId "
            "join textMap npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            "left join textMap titleText on quest.titleTextMapHash = titleText.hash and titleText.lang = ? "
            "where (npcName.content like ? escape '\\' or npcName.content like ? escape '\\') "
        )
        params: list[object] = [langCode, langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql,
            params,
            "quest",
            created_version,
            updated_version,
            "quest",
            langCode,
        )
        sql = _append_quest_source_type_filter_clause(sql, params, "quest", source_type)
        sql += "order by quest.questId limit 200"
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByTalkerType(
    talkerType: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        sql = (
            "select distinct quest.questId, titleText.content, "
            f"{source_type_select} as source_type, "
            f"{version_select} "
            "from quest "
            "join questTalk qt on qt.questId = quest.questId "
            "join dialogue d on d.talkId = qt.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "left join textMap titleText on quest.titleTextMapHash = titleText.hash and titleText.lang = ? "
            "where d.talkerType = ? "
        )
        params: list[object] = [langCode, talkerType]
        sql = _append_version_filter_clause(
            sql,
            params,
            "quest",
            created_version,
            updated_version,
            "quest",
            langCode,
        )
        sql = _append_quest_source_type_filter_clause(sql, params, "quest", source_type)
        sql += "order by quest.questId limit 200"
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectNpcDialogueSearchEntries(
    keyword: str | None,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    limit: int | None = 200,
):
    with closing(conn.cursor()) as cursor:
        keyword_text = (keyword or "").strip()
        source_lang_code = config.getSourceLanguage()
        sql = (
            "with base_npc as ("
            "select npc.npcId as npcId, npcName.content as npcName, "
            "vd_created.raw_version as created_version, "
            "coalesce(vd_created.version_tag, '') as created_version_tag, "
            "coalesce(vd_created.version_sort_key, 2147483647) as created_version_sort_key "
            "from npc "
            "join textMap npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            "left join version_dim vd_created on vd_created.id = npc.created_version_id "
            "where 1=1 "
        )
        params: list[object] = [langCode]
        if keyword_text:
            exact, fuzzy = _build_like_patterns(keyword_text, langCode)
            sql += "and (npcName.content like ? escape '\\' or npcName.content like ? escape '\\') "
            params.extend([exact, fuzzy])
        sql += (
            "), visible_npc as ("
            "select distinct base.npcId, base.npcName, base.created_version, "
            "base.created_version_tag, base.created_version_sort_key "
            "from base_npc base "
            "join dialogue d on d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = base.npcId "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "where qt.questId is null"
            "), npc_updated_ranked as ("
            "select visible.npcId as npcId, visible.npcName as npcName, vd.raw_version as dialogue_updated_version, "
            "coalesce(vd.version_tag, '') as dialogue_updated_tag, "
            "coalesce(vd.version_sort_key, -1) as dialogue_updated_sort_key, "
            "row_number() over ("
            "partition by visible.npcId "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc"
            ") as row_num "
            "from dialogue d "
            "join visible_npc visible on visible.npcId = d.talkerId "
            "join textMap tm on tm.hash = d.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.updated_version_id "
            "where d.talkerType = 'TALK_ROLE_NPC' "
            "and tm.updated_version_id is not null"
            "), npc_updated as ("
            "select npcId, npcName, dialogue_updated_version, dialogue_updated_tag, dialogue_updated_sort_key "
            "from npc_updated_ranked "
            "where row_num = 1"
            "), group_created_ranked as ("
            "select visible.npcName as npcName, visible.created_version as created_version, "
            "visible.created_version_tag as created_version_tag, "
            "row_number() over ("
            "partition by visible.npcName "
            "order by visible.created_version_sort_key asc, visible.npcId asc"
            ") as row_num "
            "from visible_npc visible "
            "where visible.created_version is not null"
            "), group_created as ("
            "select npcName, created_version, created_version_tag "
            "from group_created_ranked "
            "where row_num = 1"
            "), group_updated_ranked as ("
            "select visible.npcName as npcName, npc_updated.dialogue_updated_version as dialogue_updated_version, "
            "npc_updated.dialogue_updated_tag as dialogue_updated_tag, "
            "row_number() over ("
            "partition by visible.npcName "
            "order by coalesce(npc_updated.dialogue_updated_sort_key, -1) desc, visible.npcId asc"
            ") as row_num "
            "from visible_npc visible "
            "join npc_updated on npc_updated.npcId = visible.npcId "
            "), group_updated as ("
            "select npcName, dialogue_updated_version, dialogue_updated_tag "
            "from group_updated_ranked "
            "where row_num = 1"
            "), grouped_npc as ("
            "select visible.npcName as npcName, group_concat(visible.npcId) as npc_ids, min(visible.npcId) as sort_npc_id "
            "from visible_npc visible "
            "group by visible.npcName"
            ") "
            "select grouped.npcName, grouped.npc_ids, group_created.created_version, group_updated.dialogue_updated_version "
            "from grouped_npc grouped "
            "left join group_created on group_created.npcName = grouped.npcName "
            "left join group_updated on group_updated.npcName = grouped.npcName "
            "where 1=1 "
        )
        params.append(source_lang_code)
        created_version_tag = _normalize_version_filter(created_version)
        if created_version_tag:
            sql += "and coalesce(group_created.created_version_tag, '') = ? "
            params.append(created_version_tag)
        updated_version_tag = _normalize_version_filter(updated_version)
        if updated_version_tag:
            sql += "and coalesce(group_updated.dialogue_updated_tag, '') = ? "
            params.append(updated_version_tag)

        if keyword_text:
            match_sort_sql, match_sort_params = _build_match_sort_case("grouped.npcName", keyword_text, langCode)
            normalized_length_sql = f"length({_build_normalized_match_expr('grouped.npcName', langCode)})"
            sql += f"order by {match_sort_sql}, {normalized_length_sql}, grouped.sort_npc_id "
            params.extend(match_sort_params)
        else:
            sql += "order by grouped.sort_npc_id "
        if limit is not None:
            sql += "limit ?"
            params.append(int(limit))
        cursor.execute(sql, params)
        return cursor.fetchall()


def getNpcSpeakerUpdatedVersionRaw(npcId: int, langCode: int | None = None) -> str | None:
    source_lang_code = config.getSourceLanguage() if langCode is None else langCode
    with closing(conn.cursor()) as cursor:
        row = cursor.execute(
            "select vd.raw_version "
            "from dialogue d "
            "join textMap tm on tm.hash = d.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.updated_version_id "
            "where d.talkerType = 'TALK_ROLE_NPC' "
            "and d.talkerId = ? "
            "and tm.updated_version_id is not null "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc "
            "limit 1",
            (source_lang_code, npcId),
        ).fetchone()
        return str(row[0]) if row and row[0] is not None else None


def getNpcNonTaskDialogueStats(npcId: int) -> tuple[int, int]:
    with closing(conn.cursor()) as cursor:
        group_join = (
            "((mg.dialogueIdFallback is not null and dg.dialogueId = mg.dialogueIdFallback) "
            "or (mg.dialogueIdFallback is null and dg.talkId = mg.talkId "
            "and ((mg.coopQuestId is null and dg.coopQuestId is null) "
            "or dg.coopQuestId = mg.coopQuestId)))"
        )
        row = cursor.execute(
            "with matching_groups as ("
            "select d.talkId as talkId, "
            "d.coopQuestId as coopQuestId, "
            "case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end as dialogueIdFallback, "
            "min(d.dialogueId) as sortDialogueId "
            "from dialogue d "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = ? and qt.questId is null "
            "group by d.talkId, d.coopQuestId, case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end"
            "), group_line_counts as ("
            "select mg.talkId, mg.coopQuestId, mg.dialogueIdFallback, count(dg.dialogueId) as lineCount "
            "from matching_groups mg "
            f"join dialogue dg on {group_join} "
            "group by mg.talkId, mg.coopQuestId, mg.dialogueIdFallback"
            ") "
            "select count(*), coalesce(sum(lineCount), 0) from group_line_counts",
            (npcId,),
        ).fetchone()
        if not row:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)


def countNpcNonTaskDialogueGroups(npcId: int) -> int:
    with closing(conn.cursor()) as cursor:
        row = cursor.execute(
            "with matching_groups as ("
            "select d.talkId as talkId, "
            "d.coopQuestId as coopQuestId, "
            "case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end as dialogueIdFallback "
            "from dialogue d "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = ? and qt.questId is null "
            "group by d.talkId, d.coopQuestId, case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end"
            ") "
            "select count(*) from matching_groups",
            (npcId,),
        ).fetchone()
        return int(row[0] or 0) if row else 0


def countNpcNonTaskDialogueGroupsForNpcIds(npcIds: list[int]) -> int:
    normalized_ids = []
    for value in npcIds:
        try:
            npc_id = int(value)
        except Exception:
            continue
        if npc_id <= 0 or npc_id in normalized_ids:
            continue
        normalized_ids.append(npc_id)
    if not normalized_ids:
        return 0

    with closing(conn.cursor()) as cursor:
        placeholders = ",".join("?" for _ in normalized_ids)
        row = cursor.execute(
            "with matching_groups as ("
            "select d.talkId as talkId, "
            "d.coopQuestId as coopQuestId, "
            "case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end as dialogueIdFallback "
            "from dialogue d "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            f"where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId in ({placeholders}) and qt.questId is null "
            "group by d.talkId, d.coopQuestId, case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end"
            ") "
            "select count(*) from matching_groups",
            normalized_ids,
        ).fetchone()
        return int(row[0] or 0) if row else 0


def selectNpcNonTaskDialogueGroupPage(
    npcId: int,
    limit: int,
    offset: int = 0,
):
    source_lang_code = config.getSourceLanguage()
    with closing(conn.cursor()) as cursor:
        group_join = (
            "((mg.dialogueIdFallback is not null and dg.dialogueId = mg.dialogueIdFallback) "
            "or (mg.dialogueIdFallback is null and dg.talkId = mg.talkId "
            "and ((mg.coopQuestId is null and dg.coopQuestId is null) "
            "or dg.coopQuestId = mg.coopQuestId)))"
        )
        created_sort_key_expr = (
            "select coalesce(vd.version_sort_key, 2147483647) "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        params: list[object] = [npcId, source_lang_code, max(1, int(limit))]
        sql = (
            "with matching_groups as ("
            "select d.talkId as talkId, "
            "d.coopQuestId as coopQuestId, "
            "case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end as dialogueIdFallback, "
            "min(d.dialogueId) as sortDialogueId "
            "from dialogue d "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = ? and qt.questId is null "
            "group by d.talkId, d.coopQuestId, case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end"
            ") "
            "select mg.talkId, mg.coopQuestId, mg.dialogueIdFallback, mg.sortDialogueId, count(dg.dialogueId) as lineCount "
            "from matching_groups mg "
            f"join dialogue dg on {group_join} "
            "group by mg.talkId, mg.coopQuestId, mg.dialogueIdFallback, mg.sortDialogueId "
            f"order by coalesce(({created_sort_key_expr}), 2147483647), mg.sortDialogueId "
            "limit ?"
        )
        if offset and int(offset) > 0:
            sql += " offset ?"
            params.append(int(offset))
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectNpcNonTaskDialogueGroupPageForNpcIds(
    npcIds: list[int],
    limit: int,
    offset: int = 0,
):
    normalized_ids = []
    for value in npcIds:
        try:
            npc_id = int(value)
        except Exception:
            continue
        if npc_id <= 0 or npc_id in normalized_ids:
            continue
        normalized_ids.append(npc_id)
    if not normalized_ids:
        return []

    source_lang_code = config.getSourceLanguage()
    with closing(conn.cursor()) as cursor:
        group_join = (
            "((mg.dialogueIdFallback is not null and dg.dialogueId = mg.dialogueIdFallback) "
            "or (mg.dialogueIdFallback is null and dg.talkId = mg.talkId "
            "and ((mg.coopQuestId is null and dg.coopQuestId is null) "
            "or dg.coopQuestId = mg.coopQuestId)))"
        )
        created_sort_key_expr = (
            "select coalesce(vd.version_sort_key, 2147483647) "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        placeholders = ",".join("?" for _ in normalized_ids)
        params: list[object] = [*normalized_ids, source_lang_code, max(1, int(limit))]
        sql = (
            "with matching_groups as ("
            "select d.talkId as talkId, "
            "d.coopQuestId as coopQuestId, "
            "case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end as dialogueIdFallback, "
            "min(d.dialogueId) as sortDialogueId "
            "from dialogue d "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            f"where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId in ({placeholders}) and qt.questId is null "
            "group by d.talkId, d.coopQuestId, case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end"
            ") "
            "select mg.talkId, mg.coopQuestId, mg.dialogueIdFallback, mg.sortDialogueId, count(dg.dialogueId) as lineCount "
            "from matching_groups mg "
            f"join dialogue dg on {group_join} "
            "group by mg.talkId, mg.coopQuestId, mg.dialogueIdFallback, mg.sortDialogueId "
            f"order by coalesce(({created_sort_key_expr}), 2147483647), mg.sortDialogueId "
            "limit ?"
        )
        if offset and int(offset) > 0:
            sql += " offset ?"
            params.append(int(offset))
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectNpcNonTaskDialogueGroupSummaries(
    npcId: int,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    source_lang_code = config.getSourceLanguage()
    with closing(conn.cursor()) as cursor:
        group_join = (
            "((mg.dialogueIdFallback is not null and dg.dialogueId = mg.dialogueIdFallback) "
            "or (mg.dialogueIdFallback is null and dg.talkId = mg.talkId "
            "and ((mg.coopQuestId is null and dg.coopQuestId is null) "
            "or dg.coopQuestId = mg.coopQuestId)))"
        )
        created_raw_expr = (
            "select vd.raw_version "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        created_tag_expr = (
            "select coalesce(vd.version_tag, '') "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        created_sort_key_expr = (
            "select coalesce(vd.version_sort_key, 2147483647) "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        updated_raw_expr = (
            "select vd.raw_version "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.updated_version_id "
            f"where {group_join} and tm.updated_version_id is not null "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc "
            "limit 1"
        )
        updated_tag_expr = (
            "select coalesce(vd.version_tag, '') "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.updated_version_id "
            f"where {group_join} and tm.updated_version_id is not null "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc "
            "limit 1"
        )
        sql = (
            "with matching_groups as ("
            "select d.talkId as talkId, "
            "d.coopQuestId as coopQuestId, "
            "case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end as dialogueIdFallback, "
            "min(d.dialogueId) as sortDialogueId "
            "from dialogue d "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = ? and qt.questId is null "
            "group by d.talkId, d.coopQuestId, case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end"
            ") "
            "select mg.talkId, mg.coopQuestId, mg.dialogueIdFallback, mg.sortDialogueId, "
            f"(select count(*) from dialogue dg where {group_join}) as lineCount, "
            f"({created_raw_expr}) as created_version, "
            f"({updated_raw_expr}) as updated_version "
            "from matching_groups mg "
            "where 1=1 "
        )
        params: list[object] = [npcId, source_lang_code, source_lang_code]
        created_tag = _normalize_version_filter(created_version)
        updated_tag = _normalize_version_filter(updated_version)
        if created_tag:
            sql += f"and coalesce(({created_tag_expr}), '') = ? "
            params.extend([source_lang_code, created_tag])
        if updated_tag:
            sql += f"and coalesce(({updated_tag_expr}), '') = ? "
            params.extend([source_lang_code, updated_tag])
        sql += f"order by coalesce(({created_sort_key_expr}), 2147483647), mg.sortDialogueId"
        params.append(source_lang_code)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectNpcNonTaskDialogueGroupSummariesForNpcIds(
    npcIds: list[int],
    created_version: str | None = None,
    updated_version: str | None = None,
):
    normalized_ids = []
    for value in npcIds:
        try:
            npc_id = int(value)
        except Exception:
            continue
        if npc_id <= 0 or npc_id in normalized_ids:
            continue
        normalized_ids.append(npc_id)
    if not normalized_ids:
        return []

    source_lang_code = config.getSourceLanguage()
    with closing(conn.cursor()) as cursor:
        group_join = (
            "((mg.dialogueIdFallback is not null and dg.dialogueId = mg.dialogueIdFallback) "
            "or (mg.dialogueIdFallback is null and dg.talkId = mg.talkId "
            "and ((mg.coopQuestId is null and dg.coopQuestId is null) "
            "or dg.coopQuestId = mg.coopQuestId)))"
        )
        created_raw_expr = (
            "select vd.raw_version "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        created_tag_expr = (
            "select coalesce(vd.version_tag, '') "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        created_sort_key_expr = (
            "select coalesce(vd.version_sort_key, 2147483647) "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.created_version_id "
            f"where {group_join} and tm.created_version_id is not null "
            "order by coalesce(vd.version_sort_key, 2147483647) asc, vd.id asc "
            "limit 1"
        )
        updated_raw_expr = (
            "select vd.raw_version "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.updated_version_id "
            f"where {group_join} and tm.updated_version_id is not null "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc "
            "limit 1"
        )
        updated_tag_expr = (
            "select coalesce(vd.version_tag, '') "
            "from dialogue dg "
            "join textMap tm on tm.hash = dg.textHash and tm.lang = ? "
            "join version_dim vd on vd.id = tm.updated_version_id "
            f"where {group_join} and tm.updated_version_id is not null "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc "
            "limit 1"
        )
        placeholders = ",".join("?" for _ in normalized_ids)
        sql = (
            "with matching_groups as ("
            "select d.talkId as talkId, "
            "d.coopQuestId as coopQuestId, "
            "case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end as dialogueIdFallback, "
            "min(d.dialogueId) as sortDialogueId "
            "from dialogue d "
            "left join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            f"where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId in ({placeholders}) and qt.questId is null "
            "group by d.talkId, d.coopQuestId, case when d.talkId = 0 and d.coopQuestId is null then d.dialogueId else null end"
            ") "
            "select mg.talkId, mg.coopQuestId, mg.dialogueIdFallback, mg.sortDialogueId, "
            f"(select count(*) from dialogue dg where {group_join}) as lineCount, "
            f"({created_raw_expr}) as created_version, "
            f"({updated_raw_expr}) as updated_version "
            "from matching_groups mg "
            "where 1=1 "
        )
        params: list[object] = [*normalized_ids, source_lang_code, source_lang_code]
        created_tag = _normalize_version_filter(created_version)
        updated_tag = _normalize_version_filter(updated_version)
        if created_tag:
            sql += f"and coalesce(({created_tag_expr}), '') = ? "
            params.extend([source_lang_code, created_tag])
        if updated_tag:
            sql += f"and coalesce(({updated_tag_expr}), '') = ? "
            params.extend([source_lang_code, updated_tag])
        sql += f"order by coalesce(({created_sort_key_expr}), 2147483647), mg.sortDialogueId"
        params.append(source_lang_code)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectAvatarByNameKeyword(keyword: str, langCode: int):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        match_sort_sql, match_sort_params = _build_match_sort_case("textMap.content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('textMap.content', langCode)})"
        sql = (
            "select avatar.avatarId, textMap.content "
            "from avatar join textMap on avatar.nameTextMapHash=textMap.hash "
            "where textMap.lang=? and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') "
            f"order by {match_sort_sql}, {normalized_length_sql} "
            "limit 200"
        )
        cursor.execute(sql, (langCode, exact, fuzzy, *match_sort_params))
        return cursor.fetchall()


def _legacy_selectAvatarVoiceItems(avatarId: int, limit: int = 400):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select fetters.voiceTitleTextMapHash, fetters.voiceFileTextTextMapHash, voice.voicePath "
            "from fetters "
            "left join voice on voice.dialogueId = fetters.voiceFile "
            "and (voice.avatarId = fetters.avatarId or voice.avatarId = 0) "
            "where fetters.avatarId=? "
            "order by fetters.fetterId "
            "limit ?"
        )
        cursor.execute(sql, (avatarId, limit))
        return cursor.fetchall()


def selectAvatarStories(avatarId: int, limit: int = 800):
    with closing(conn.cursor()) as cursor:
        try:
            sql = (
                "select fetterId, storyTitleTextMapHash, storyTitle2TextMapHash, "
                "storyTitleLockedTextMapHash, storyContextTextMapHash, storyContext2TextMapHash "
                "from fetterStory "
                "where avatarId=? "
                "order by fetterId "
                "limit ?"
            )
            cursor.execute(sql, (avatarId, limit))
            return cursor.fetchall()
        except sqlite3.OperationalError:
            return []


def _avatar_story_entries_subquery() -> str:
    return (
        "select avatarId, fetterId, "
        "storyTitleTextMapHash as titleHash, "
        "storyTitleLockedTextMapHash as lockedTitleHash, "
        "storyContextTextMapHash as contextHash "
        "from fetterStory where storyContextTextMapHash is not null "
        "union all "
        "select avatarId, fetterId, "
        "storyTitle2TextMapHash as titleHash, "
        "storyTitleLockedTextMapHash as lockedTitleHash, "
        "storyContext2TextMapHash as contextHash "
        "from fetterStory where storyContext2TextMapHash is not null"
    )


def selectStorySourcesByTextHash(textHash: int):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select entries.avatarId, entries.fetterId, entries.titleHash, entries.lockedTitleHash "
            f"from ({_avatar_story_entries_subquery()}) as entries "
            "where entries.contextHash = ? "
            "order by entries.fetterId"
        )
        cursor.execute(sql, (textHash,))
        return cursor.fetchall()


def _legacy_selectAvatarVoiceItemsByFilters(
    keyword: str | None,
    langCode: int,
    limit: int | None = 800,
    created_version: str | None = None,
    updated_version: str | None = None,
    version_lang_code: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        keyword_text = (keyword or "").strip()
        exact = fuzzy = None
        if keyword_text:
            escaped = _escape_like(keyword_text)
            exact = f"%{escaped}%"
            fuzzy = exact

        sql = (
            "select fetters.avatarId, fetters.voiceTitleTextMapHash, "
            "fetters.voiceFileTextTextMapHash, voice.voicePath "
            "from fetters "
            "left join voice on voice.dialogueId = fetters.voiceFile "
            "and (voice.avatarId = fetters.avatarId or voice.avatarId = 0) "
            "left join textMap as titleText on titleText.hash = fetters.voiceTitleTextMapHash "
            "and titleText.lang = ? "
            "left join textMap as contentText on contentText.hash = fetters.voiceFileTextTextMapHash "
            "and contentText.lang = ? "
            "where 1=1 "
        )
        params: list[object] = []
        params.append(langCode)
        params.append(langCode)

        if keyword_text:
            sql += (
                "and ("
                "(titleText.content like ? escape '\\' or titleText.content like ? escape '\\') "
                "or (contentText.content like ? escape '\\' or contentText.content like ? escape '\\')"
                ") "
            )
            params.append(exact)
            params.append(fuzzy)
            params.append(exact)
            params.append(fuzzy)

        sql = _append_textmap_exists_version_filter(
            sql,
            params,
            "fetters.voiceFileTextTextMapHash",
            created_version,
            updated_version,
            version_lang_code,
        )

        if keyword_text:
            sql += (
                "order by case "
                "when titleText.content like ? escape '\\' then 0 "
                "when contentText.content like ? escape '\\' then 1 "
                "else 2 end, "
                "length(coalesce(titleText.content, contentText.content, '')), fetters.fetterId "
            )
            params.extend([exact, exact])
        else:
            sql += "order by fetters.fetterId "

        if limit is not None:
            sql += "limit ?"
            params.append(limit)

        cursor.execute(sql, params)
        return cursor.fetchall()


_hasVoiceForTextHashDb_impl = _has_avatar_scoped_fetter_voice
_selectVoicePathFromTextHash_impl = _select_avatar_scoped_voice_path_from_text_hash
_getVoicePath_impl = _get_avatar_scoped_voice_path
_selectAvatarVoiceItems_impl = _select_avatar_scoped_voice_items
_selectAvatarVoiceItemsByFilters_impl = _select_avatar_scoped_voice_items_by_filters


def selectAvatarStoryItemsByFilters(
    keyword: str | None,
    langCode: int,
    limit: int | None = 800,
    created_version: str | None = None,
    updated_version: str | None = None,
    version_lang_code: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        keyword_text = (keyword or "").strip()
        exact = fuzzy = None
        if keyword_text:
            escaped = _escape_like(keyword_text)
            exact = f"%{escaped}%"
            fuzzy = exact

        sql = (
            "select entries.avatarId, entries.fetterId, entries.titleHash, "
            "entries.lockedTitleHash, entries.contextHash "
            f"from ({_avatar_story_entries_subquery()}) as entries "
            "left join textMap as titleText on titleText.hash = entries.titleHash and titleText.lang = ? "
            "left join textMap as lockedText on lockedText.hash = entries.lockedTitleHash and lockedText.lang = ? "
            "left join textMap as contextText on contextText.hash = entries.contextHash and contextText.lang = ? "
            "where 1=1 "
        )
        params = []
        params.append(langCode)
        params.append(langCode)
        params.append(langCode)

        if keyword_text:
            sql += (
                "and ("
                "(titleText.content like ? escape '\\' or titleText.content like ? escape '\\') "
                "or (lockedText.content like ? escape '\\' or lockedText.content like ? escape '\\') "
                "or (contextText.content like ? escape '\\' or contextText.content like ? escape '\\')"
                ") "
            )
            params.append(exact)
            params.append(fuzzy)
            params.append(exact)
            params.append(fuzzy)
            params.append(exact)
            params.append(fuzzy)

        sql = _append_textmap_exists_version_filter(
            sql,
            params,
            "entries.contextHash",
            created_version,
            updated_version,
            version_lang_code,
        )

        if keyword_text:
            sql += (
                "order by case "
                "when titleText.content like ? escape '\\' then 0 "
                "when lockedText.content like ? escape '\\' then 1 "
                "when contextText.content like ? escape '\\' then 2 "
                "else 3 end, "
                "length(coalesce(titleText.content, lockedText.content, contextText.content, '')), "
                "entries.fetterId "
            )
            params.append(exact)
            params.append(exact)
            params.append(exact)
        else:
            sql += "order by entries.fetterId "

        if limit is not None:
            sql += "limit ?"
            params.append(limit)

        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByChapterKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        sql = (
            "select quest.questId, questTitle.content, chapterTitle.content, chapterNum.content, "
            f"{source_type_select} as source_type, "
            f"{version_select} "
            "from quest "
            "join textMap as questTitle on quest.titleTextMapHash=questTitle.hash "
            "join chapter on quest.chapterId=chapter.chapterId "
            "left join textMap as chapterTitle on chapter.chapterTitleTextMapHash=chapterTitle.hash "
            "and chapterTitle.lang=? "
            "left join textMap as chapterNum on chapter.chapterNumTextMapHash=chapterNum.hash "
            "and chapterNum.lang=? "
            "where questTitle.lang=? and ("
            "chapterTitle.content like ? escape '\\' or chapterNum.content like ? escape '\\' "
            "or chapterTitle.content like ? escape '\\' or chapterNum.content like ? escape '\\'"
            ") "
        )
        params = [
            langCode,
            langCode,
            langCode,
            exact,
            exact,
            fuzzy,
            fuzzy,
        ]
        sql = _append_version_filter_clause(
            sql,
            params,
            "quest",
            created_version,
            updated_version,
            "quest",
            langCode,
        )
        sql = _append_quest_source_type_filter_clause(sql, params, "quest", source_type)
        sql += (
            "order by case when (chapterTitle.content like ? escape '\\' or chapterNum.content like ? escape '\\') then 0 else 1 end, "
            "length(coalesce(chapterTitle.content, chapterNum.content)) "
            "limit 200"
        )
        params.extend([exact, exact])
        cursor.execute(sql, params)
        return cursor.fetchall()


def getQuestChapterName(questId: int, langCode: int):
    with closing(conn.cursor()) as cursor:
        sql = "select chapterId from quest where questId=?"
        cursor.execute(sql, (questId,))
        ans = cursor.fetchall()
        if len(ans) == 0 or ans[0][0] is None:
            return None
        chapterId = ans[0][0]

        sql2 = ("select chapterTitleTextMapHash, chapterNumTextMapHash "
                "from chapter where chapterId=?")
        cursor.execute(sql2, (chapterId,))
        chapter_row = cursor.fetchall()
        if len(chapter_row) == 0:
            return None
        chapterTitleTextMapHash, chapterNumTextMapHash = chapter_row[0]

        sql3 = 'select content from textMap where hash=? and lang=?'
        cursor.execute(sql3, (chapterTitleTextMapHash, langCode))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        chapterTitleText = ans2[0][0]

        cursor.execute(sql3, (chapterNumTextMapHash, langCode))
        ans3 = cursor.fetchall()
        if len(ans3) > 0:
            chapterNumText = ans3[0][0]
            return '{} · {}'.format(chapterNumText, chapterTitleText)
        return chapterTitleText


def getQuestVersionInfo(questId: int, langCode: int | None = None):
    if not _has_version_id_columns("quest"):
        return None, None
    with closing(conn.cursor()) as cursor:
        version_select = _version_select_expr("q", "quest", langCode)
        sql = f"select {version_select} from quest q where q.questId=? limit 1"
        cursor.execute(sql, (questId,))
        row = cursor.fetchone()
        if row:
            return row
        return None, None


def selectQuestTalkIds(questId: int):
    with closing(conn.cursor()) as cursor:
        sql = "select talkId from questTalk where questId=? order by talkId"
        cursor.execute(sql, (questId,))
        return [row[0] for row in cursor.fetchall()]


def countQuestDialogues(questId: int) -> int:
    with closing(conn.cursor()) as cursor:
        sql = (
            "select count(*) from dialogue d "
            "join (select distinct talkId, coalesce(coopQuestId, 0) as coopQuestId from questTalk where questId=?) qt "
            "on qt.talkId = d.talkId "
            "and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ")"
        )
        cursor.execute(sql, (questId,))
        row = cursor.fetchone()
        return row[0] if row else 0


def selectQuestDialoguesPaged(
    questId: int,
    limit: int | None = None,
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select d.textHash, d.talkerType, d.talkerId, d.dialogueId, d.talkId "
            "from dialogue d "
            "join (select distinct talkId, coalesce(coopQuestId, 0) as coopQuestId from questTalk where questId=?) qt "
            "on qt.talkId = d.talkId "
            "and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
            "order by qt.coopQuestId, d.talkId, d.dialogueId"
        )
        params = [questId]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        if offset is not None and offset > 0:
            if limit is None:
                sql += " limit -1"
            sql += " offset ?"
            params.append(offset)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableByTitleKeyword(
    keyword: str,
    langCode: int,
    langStr: str,
    created_version: str | None = None,
    updated_version: str | None = None,
    category: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        match_sort_sql, match_sort_params = _build_match_sort_case("textMap.content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('textMap.content', langCode)})"
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return []
        readable_lang_placeholders = ",".join(["?"] * len(readable_langs))
        version_select = _version_select_expr("readable", "readable")
        version_group = "readable.created_version_id, readable.updated_version_id"
        sql = (f"select readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
               f"{version_select} "
               "from readable join textMap on readable.titleTextMapHash=textMap.hash "
               f"where readable.lang in ({readable_lang_placeholders}) and textMap.lang=? "
               "and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') ")
        params = readable_langs + [langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql,
            params,
            "readable",
            created_version,
            updated_version,
            "readable",
        )
        sql = _append_readable_category_filter_clause(sql, params, "readable", category)
        sql += ("group by readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
                f"{version_group} "
                f"order by {match_sort_sql}, {normalized_length_sql} "
                "limit 200")
        params.extend(match_sort_params)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableByFileNameContains(
    keyword: str,
    langCode: int,
    langStr: str,
    created_version: str | None = None,
    updated_version: str | None = None,
    category: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        escaped = _escape_like(keyword)
        pattern = f"%{escaped}%"
        match_sort_sql, match_sort_params = _build_match_sort_case("readable.fileName", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('readable.fileName', langCode)})"
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return []
        readable_lang_placeholders = ",".join(["?"] * len(readable_langs))
        version_select = _version_select_expr("readable", "readable")
        version_group = "readable.created_version_id, readable.updated_version_id"
        sql = (
            f"select readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
            f"{version_select} "
            "from readable "
            "left join textMap on readable.titleTextMapHash=textMap.hash and textMap.lang=? "
            f"where readable.lang in ({readable_lang_placeholders}) and readable.fileName like ? escape '\\' "
        )
        params = [langCode] + readable_langs + [pattern]
        sql = _append_version_filter_clause(
            sql,
            params,
            "readable",
            created_version,
            updated_version,
            "readable",
        )
        sql = _append_readable_category_filter_clause(sql, params, "readable", category)
        sql += ("group by readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
                f"{version_group} "
                f"order by {match_sort_sql}, {normalized_length_sql} "
                "limit 200")
        params.extend(match_sort_params)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByVersion(
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
    limit: int | None = 2000,
):
    with closing(conn.cursor()) as cursor:
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        sql = (
            f"select quest.questId, textMap.content, {source_type_select} as source_type, {version_select} "
            "from quest "
            "left join textMap on quest.titleTextMapHash=textMap.hash and textMap.lang=? "
            "where 1=1 "
        )
        params = [langCode]
        sql = _append_version_filter_clause(
            sql,
            params,
            "quest",
            created_version,
            updated_version,
            "quest",
            langCode,
        )
        sql = _append_quest_source_type_filter_clause(sql, params, "quest", source_type)
        sql += "order by quest.questId "
        if limit is not None:
            sql += "limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableByVersion(
    langCode: int,
    langStr: str,
    created_version: str | None = None,
    updated_version: str | None = None,
    limit: int | None = 2000,
    category: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return []
        readable_lang_placeholders = ",".join(["?"] * len(readable_langs))
        version_select = _version_select_expr("readable", "readable")
        sql = (
            f"select readable.fileName, readable.readableId, readable.titleTextMapHash, "
            f"textMap.content as title, {version_select} "
            "from readable "
            "left join textMap on readable.titleTextMapHash=textMap.hash and textMap.lang=? "
            f"where readable.lang in ({readable_lang_placeholders}) "
        )
        params = [langCode] + readable_langs
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "readable",
            created_version,
            updated_version,
            "readable",
        )
        sql = _append_readable_category_filter_clause(sql, params, "readable", category)
        sql += "order by readable.readableId, readable.fileName "
        if limit is not None:
            sql += "limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectSubtitleFromKeyword(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    offset: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        version_select = _version_select_expr("subtitle", "subtitle")
        match_sort_sql, match_sort_params = _build_match_sort_case("content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('content', langCode)})"
        sql = (
            f"select fileName, content, startTime, endTime, subtitleId, {version_select} from subtitle "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\') "
        )
        params = [langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "subtitle",
            created_version,
            updated_version,
            "subtitle",
        )
        sql += f"order by {match_sort_sql}, {normalized_length_sql} "
        params.extend(match_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(int(limit))
        if offset is not None and offset > 0:
            sql += " offset ?"
            params.append(int(offset))
        cursor.execute(sql, params)
        return cursor.fetchall()


def countSubtitleFromKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) from subtitle "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\')"
        )
        params = [langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "subtitle",
            created_version,
            updated_version,
            "subtitle",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def getSubtitleVersionInfo(
    fileName: str,
    startTime: float | None = None,
    subtitleId: int | None = None,
    lang: int | None = None,
):
    if not _has_version_id_columns("subtitle"):
        return None, None
    # Subtitle versioning is file-level per language file.
    return getSubtitleFileVersionInfo(fileName)


def getSubtitleFileVersionInfo(fileName: str):
    if not _has_version_id_columns("subtitle"):
        return None, None
    exact_name = (fileName or "").strip()
    if not exact_name:
        return None, None
    created_expr = _version_value_expr("s", "created", "subtitle")
    updated_expr = _version_value_expr("s", "updated", "subtitle")
    with closing(conn.cursor()) as cursor:
        sql_created = (
            f"select {created_expr} from subtitle s "
            f"where s.fileName=? and coalesce({created_expr}, '') <> '' "
            "order by rowid asc limit 1"
        )
        cursor.execute(sql_created, (exact_name,))
        row_created = cursor.fetchone()
        created_raw = row_created[0] if row_created else None

        sql_updated = (
            f"select {updated_expr} from subtitle s "
            f"where s.fileName=? and coalesce({updated_expr}, '') <> '' "
            "order by rowid desc limit 1"
        )
        cursor.execute(sql_updated, (exact_name,))
        row_updated = cursor.fetchone()
        updated_raw = row_updated[0] if row_updated else None
        return created_raw, updated_raw


def selectDialogueByTalkerKeyword(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("npcName.content", keyword, langCode)
        speaker_length_sql = f"length({_build_normalized_match_expr('npcName.content', langCode)})"
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap as npcName on npc.textHash = npcName.hash and npcName.lang=? "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash and dialogueText.lang=? "
            "where (npcName.content like ? escape '\\' or npcName.content like ? escape '\\') "
        )
        params = [langCode, langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += f"order by {speaker_sort_sql}, {speaker_length_sql}, dialogue.dialogueId "
        params.extend(speaker_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def countDialogueByTalkerKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from dialogue "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap as npcName on npc.textHash = npcName.hash and npcName.lang=? "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash and dialogueText.lang=? "
            "where (npcName.content like ? escape '\\' or npcName.content like ? escape '\\') "
        )
        params = [langCode, langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql,
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def selectDialogueByTalkerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        dialogue_sort_sql, dialogue_sort_params = _build_match_sort_case("dialogueText.content", keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("npcName.content", speaker_keyword, langCode)
        dialogue_length_sql = f"length({_build_normalized_match_expr('dialogueText.content', langCode)})"
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap as npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            "where (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
            "and (npcName.content like ? escape '\\' or npcName.content like ? escape '\\') "
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
        ]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += (
            f"order by {dialogue_sort_sql}, {speaker_sort_sql}, "
            f"{dialogue_length_sql}, dialogue.dialogueId "
        )
        params.extend(dialogue_sort_params)
        params.extend(speaker_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectDialogueByTalkerTypeAndKeyword(
    talkerType: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        dialogue_sort_sql, dialogue_sort_params = _build_match_sort_case("dialogueText.content", keyword, langCode)
        dialogue_length_sql = f"length({_build_normalized_match_expr('dialogueText.content', langCode)})"
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "where dialogue.talkerType = ? "
            "and (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
        )
        params = [langCode, talkerType, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += (
            f"order by {dialogue_sort_sql}, "
            f"{dialogue_length_sql}, dialogue.dialogueId "
        )
        params.extend(dialogue_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectFetterBySpeakerKeyword(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("avatarName.content", keyword, langCode)
        speaker_length_sql = f"length({_build_normalized_match_expr('avatarName.content', langCode)})"
        sql = (
            "select fetters.voiceFileTextTextMapHash, fetters.avatarId "
            "from fetters "
            "join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "join avatar on fetters.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\') "
        )
        params = [langCode, langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "voiceText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += f"order by {speaker_sort_sql}, {speaker_length_sql}, fetters.fetterId "
        params.extend(speaker_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectFetterBySpeakerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        voice_sort_sql, voice_sort_params = _build_match_sort_case("voiceText.content", keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("avatarName.content", speaker_keyword, langCode)
        voice_length_sql = f"length({_build_normalized_match_expr('voiceText.content', langCode)})"
        sql = (
            "select fetters.voiceFileTextTextMapHash, fetters.avatarId "
            "from fetters "
            "join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "join avatar on fetters.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (voiceText.content like ? escape '\\' or voiceText.content like ? escape '\\') "
            "and (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\') "
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
        ]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "voiceText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += (
            f"order by {voice_sort_sql}, {speaker_sort_sql}, "
            f"{voice_length_sql}, fetters.fetterId "
        )
        params.extend(voice_sort_params)
        params.extend(speaker_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectAvatarStoryBySpeakerKeyword(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("avatarName.content", keyword, langCode)
        speaker_length_sql = f"length({_build_normalized_match_expr('avatarName.content', langCode)})"
        sql = (
            "select entries.contextHash, entries.avatarId "
            f"from ({_avatar_story_entries_subquery()}) as entries "
            "join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            "join avatar on entries.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\') "
        )
        params = [langCode, langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "storyText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += f"order by {speaker_sort_sql}, {speaker_length_sql}, entries.fetterId "
        params.extend(speaker_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectAvatarStoryBySpeakerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        story_sort_sql, story_sort_params = _build_match_sort_case("storyText.content", keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("avatarName.content", speaker_keyword, langCode)
        story_length_sql = f"length({_build_normalized_match_expr('storyText.content', langCode)})"
        sql = (
            "select entries.contextHash, entries.avatarId "
            f"from ({_avatar_story_entries_subquery()}) as entries "
            "join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            "join avatar on entries.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (storyText.content like ? escape '\\' or storyText.content like ? escape '\\') "
            "and (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\') "
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
        ]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "storyText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += (
            f"order by {story_sort_sql}, {speaker_sort_sql}, "
            f"{story_length_sql}, entries.fetterId "
        )
        params.extend(story_sort_params)
        params.extend(speaker_sort_params)
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectDialogueByTalkerType(
    talkerType: str,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select textHash, talkerType, talkerId, dialogueId "
            "from dialogue where talkerType=? "
            " "
        )
        params = []
        params.append(talkerType)
        sql = _append_textmap_exists_version_filter(
            sql,
            params,
            "dialogue.textHash",
            created_version,
            updated_version,
        )
        sql += "order by dialogueId"
        if limit is not None:
            sql += " limit ?"
            params.append(int(limit))
        cursor.execute(sql, params)
        return cursor.fetchall()


def countDialogueByTalkerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap as npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            "where (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
            "and (npcName.content like ? escape '\\' or npcName.content like ? escape '\\')"
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
        ]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countDialogueByTalkerTypeAndKeyword(
    talkerType: str,
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "where dialogue.talkerType = ? "
            "and (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\')"
        )
        params = [langCode, talkerType, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countFetterBySpeakerKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from fetters "
            "join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "join avatar on fetters.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\')"
        )
        params = [langCode, langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "voiceText",
            created_version,
            updated_version,
            "textMap",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countFetterBySpeakerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from fetters "
            "join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "join avatar on fetters.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (voiceText.content like ? escape '\\' or voiceText.content like ? escape '\\') "
            "and (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\')"
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
        ]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "voiceText",
            created_version,
            updated_version,
            "textMap",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countAvatarStoryBySpeakerKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            f"from ({_avatar_story_entries_subquery()}) as entries "
            "join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            "join avatar on entries.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\')"
        )
        params = [langCode, langCode, exact, fuzzy]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "storyText",
            created_version,
            updated_version,
            "textMap",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countAvatarStoryBySpeakerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            f"from ({_avatar_story_entries_subquery()}) as entries "
            "join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            "join avatar on entries.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (storyText.content like ? escape '\\' or storyText.content like ? escape '\\') "
            "and (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\')"
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
        ]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "storyText",
            created_version,
            updated_version,
            "textMap",
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countDialogueByTalkerType(
    talkerType: str,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        sql = "select count(*) from dialogue where talkerType=? "
        params = [talkerType]
        sql = _append_textmap_exists_version_filter(
            sql,
            params,
            "dialogue.textHash",
            created_version,
            updated_version,
        )
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def selectSubtitleTranslations(fileName: str, startTime: float, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        file_candidates = _subtitle_file_candidates(fileName, langs)
        if not file_candidates:
            return []
        lang_placeholders = ','.join(['?'] * len(langs))
        file_placeholders = ','.join(['?'] * len(file_candidates))
        sql = f"""
            select content, lang
            from subtitle
            where fileName in ({file_placeholders})
            and lang in ({lang_placeholders})
            and startTime between ? and ?
        """
        params = file_candidates + langs + [startTime - 0.5, startTime + 0.5]
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectSubtitleTranslationsBySubtitleId(subtitleId: int, startTime: float, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        anchor_sql = "select fileName, startTime from subtitle where subtitleId=? and startTime between ? and ? limit 1"
        cursor.execute(anchor_sql, (subtitleId, startTime - 0.5, startTime + 0.5))
        anchor = cursor.fetchone()
        if not anchor:
            anchor_sql = "select fileName, startTime from subtitle where subtitleId=? limit 1"
            cursor.execute(anchor_sql, (subtitleId,))
            anchor = cursor.fetchone()
        if not anchor:
            return []
        anchor_file, anchor_start = anchor
        file_candidates = _subtitle_file_candidates(anchor_file, langs)
        if not file_candidates:
            return []
        lang_placeholders = ','.join(['?'] * len(langs))
        file_placeholders = ','.join(['?'] * len(file_candidates))
        sql = f"""
            select content, lang
            from subtitle
            where fileName in ({file_placeholders})
            and lang in ({lang_placeholders})
            and startTime between ? and ?
        """
        params = file_candidates + langs + [anchor_start - 0.5, anchor_start + 0.5]
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectSubtitleContext(fileName: str, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        file_candidates = _subtitle_file_candidates(fileName, langs)
        if not file_candidates:
            return []
        lang_placeholders = ','.join(['?'] * len(langs))
        file_placeholders = ','.join(['?'] * len(file_candidates))
        sql = (
            f"select content, lang, startTime, endTime from subtitle "
            f"where fileName in ({file_placeholders}) and lang in ({lang_placeholders}) "
            f"order by startTime"
        )
        params = file_candidates + langs
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectSubtitleContextBySubtitleId(subtitleId: int, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        anchor_sql = "select fileName, startTime from subtitle where subtitleId=? limit 1"
        cursor.execute(anchor_sql, (subtitleId,))
        anchor = cursor.fetchone()
        if not anchor:
            return []
        anchor_file, anchor_start = anchor
        file_candidates = _subtitle_file_candidates(anchor_file, langs)
        if not file_candidates:
            return []
        lang_placeholders = ','.join(['?'] * len(langs))
        file_placeholders = ','.join(['?'] * len(file_candidates))
        sql = (
            f"select content, lang, startTime, endTime from subtitle "
            f"where fileName in ({file_placeholders}) and lang in ({lang_placeholders}) "
            f"and startTime between ? and ? order by startTime"
        )
        params = file_candidates + langs + [anchor_start - 0.5, anchor_start + 0.5]
        cursor.execute(sql, params)
        return cursor.fetchall()


def hasVoiceForTextHashDb(textHash: int) -> bool:
    return _hasVoiceForTextHashDb_impl(textHash)


def selectVoicePathFromTextHash(textHash: int) -> str | None:
    return _selectVoicePathFromTextHash_impl(textHash)


def getVoicePath(voice_hash: str, lang: int) -> str | None:
    return _getVoicePath_impl(voice_hash, lang)


def selectAvatarVoiceItems(avatarId: int, limit: int = 400):
    return _selectAvatarVoiceItems_impl(avatarId, limit)


def selectAvatarVoiceItemsByFilters(
    keyword: str | None,
    langCode: int,
    limit: int | None = 800,
    created_version: str | None = None,
    updated_version: str | None = None,
    version_lang_code: int | None = None,
):
    return _selectAvatarVoiceItemsByFilters_impl(
        keyword,
        langCode,
        limit=limit,
        created_version=created_version,
        updated_version=updated_version,
        version_lang_code=version_lang_code,
    )

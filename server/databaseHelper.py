import json
import sqlite3
import threading
from contextlib import closing
import re
import os
from pathlib import Path

import config
import compact_text_cache
import fts_tokenizer
import short_literal_cache
from quest_text_filters import build_quest_text_not_excluded_sql, is_excluded_quest_text
from utils.database_connection import ThreadLocalConnection


_READABLE_LANG_SUFFIX_RE = re.compile(r"_(CHS|CHT|DE|EN|ES|FR|ID|IT|JP|KR|PT|RU|TH|TR|VI)$", re.IGNORECASE)


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
    # Avoid runtime `PRAGMA optimize`, which triggers analysis_limit-capped
    # ANALYZE writes and skews sqlite_stat1 cardinalities on large tables.
    pragmas = (
        "PRAGMA temp_store=MEMORY",  # Store temporary tables in memory.
        "PRAGMA cache_size=-65536",  # Four request threads share a roughly 256 MiB page-cache budget.
        "PRAGMA mmap_size=2147483648",  # Allow up to 2 GiB of memory-mapped I/O.
        "PRAGMA synchronous=NORMAL",  # Balance durability with write latency.
        "PRAGMA journal_mode=WAL",  # Keep WAL enabled for safer concurrent reads.
        "PRAGMA foreign_keys=ON",  # Enforce foreign keys when schemas use them.
    )
    with closing(connection.cursor()) as cursor:
        for pragma in pragmas:
            try:
                cursor.execute(pragma)
            except sqlite3.DatabaseError:
                continue


def _ensure_runtime_query_indexes(connection: sqlite3.Connection) -> None:
    index_sqls = (
        "CREATE INDEX IF NOT EXISTS readable_titleTextMapHash_index "
        "ON readable(titleTextMapHash)",
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


def _normalize_readable_file_name(file_name: str | None) -> str:
    normalized = str(file_name or "").replace("\\", "/").strip()
    base_name = os.path.basename(normalized)
    if not base_name:
        return ""
    root, ext = os.path.splitext(base_name)
    return _READABLE_LANG_SUFFIX_RE.sub("", root) + ext


def _ensure_runtime_sql_functions(connection: sqlite3.Connection) -> None:
    # Register per connection. Object IDs can be reused after a connection
    # closes, so an ID-based process cache can skip a new connection by mistake.
    try:
        connection.create_function("gts_normalize_readable_file_name", 1, _normalize_readable_file_name)
        connection.create_function(
            "gts_has_voice",
            1,
            lambda text_hash, _conn=connection: _eval_gts_has_voice(_conn, text_hash),
            deterministic=True,
        )
    except Exception:
        return


def _configure_connection(connection: sqlite3.Connection) -> None:
    """Register FTS helpers and apply default runtime PRAGMAs."""
    tokenizer, ext_path, ext_entry = _resolve_fts_settings()
    _ensure_runtime_sql_functions(connection)
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


# Keep the existing access API while giving each request thread its own connection.
conn = ThreadLocalConnection(get_connection)

# 缓存字典
_CACHE: dict[str, dict] = {
    "column": {},  # 表列信息缓存
    "table": {},  # 表存在性缓存
    "fts": {  # FTS相关缓存
        "available": None,
        "tokenizer": None,
        "detail": None,
        "langs": None
    },
    "version": {},  # 版本相关缓存
    "names": {  # 名称缓存
        "characters": {},  # 角色名称缓存
        "wander": {},  # 旅行者名称缓存
        "traveller": {},  # 空旅行者名称缓存
        "mate_avatar": {}  # 血亲同伴名称缓存
    }
}


_RAW_CHARACTER_NAME_CACHE: dict[str, str | None] = {}
_VOICED_HASH_CACHE_LOCK = threading.Lock()
_VOICED_HASH_CACHE: dict[str, object] = {"key": None, "hashes": None}
_KNOWN_SOURCE_HASH_CACHE_LOCK = threading.Lock()
_KNOWN_SOURCE_HASH_CACHE: dict[str, object] = {"key": None, "hashes": None}
_FETTER_VOICE_TABLE = "fetterVoice"
_FETTER_VOICE_SYNC_LOCK = threading.Lock()
_FETTER_VOICE_SYNC_ATTEMPTED = False
_FETTER_AVATAR_MAPPINGS: dict[str, int] | None = None
_ANIME_GAME_DATA_JSON_CACHE: dict[str, object | None] = {}
_MAIN_QUEST_ROWS_BY_ID: dict[int, dict] | None = None
_QUEST_STEP_ROWS_BY_MAIN_ID: dict[int, list[dict]] | None = None
_QUEST_STEP_TALK_MAP_CACHE: dict[tuple[int, int], dict[int, str]] = {}
_QUEST_STEP_TALK_CONDITION_TYPES = {
    "QUEST_CONTENT_COMPLETE_TALK",
    "QUEST_CONTENT_FINISH_PLOT",
}


def _get_gender_cache_key() -> str:
    gender = config.getIsMale()
    return str(gender)


def _get_name_cache_bucket(bucket_name: str) -> dict:
    names_cache = _CACHE.setdefault("names", {})
    return names_cache.setdefault(bucket_name, {})


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


def _table_exists(table_name: str) -> bool:
    if table_name in _CACHE["table"]:
        return _CACHE["table"][table_name]
    with closing(conn.cursor()) as cursor:
        try:
            row = cursor.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                (table_name,),
            ).fetchone()
        except Exception:
            _CACHE["table"][table_name] = False
            return False
    exists = row is not None
    _CACHE["table"][table_name] = exists
    return exists


# 版本处理相关常量
_VERSION_CATALOG_TABLE = "version_catalog"
_VERSION_DIM_TABLE = "version_dim"
_VERSION_SOURCE_TABLES = ("textMap", "quest", "subtitle", "readable", "npc", "text_source_entity")
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
    if table_name == 'text_source_entity':
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
        elif (
            table_name != 'npc'
            and _has_version_id_columns(table_name)
            and _table_has_column(table_name, "updated_version_id")
            and _has_version_dim()
        ):
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
    fts_cache = _CACHE.setdefault("fts", {})
    if not config.getEnableTextMapFts():
        fts_cache["available"] = False
        return False
    if fts_cache.get("available") is not None:
        return bool(fts_cache["available"])
    with closing(conn.cursor()) as cursor:
        row = cursor.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (_TEXTMAP_FTS_TABLE,),
        ).fetchone()
    fts_cache["available"] = row is not None
    if row and row[0]:
        m = re.search(r"tokenize='([^']+)'", str(row[0]))
        if m:
            token_spec = _normalize_fts_tokenizer(m.group(1))
            fts_cache["tokenizer"] = token_spec.split()[0] if token_spec else "trigram"
        detail = re.search(r"\bdetail\s*=\s*['\"]?(none|column|full)\b", str(row[0]), re.IGNORECASE)
        fts_cache["detail"] = detail.group(1).lower() if detail else "full"
    return bool(fts_cache["available"])


def _get_textmap_fts_tokenizer() -> str:
    """
    获取文本映射的全文搜索分词器
    """
    fts_cache = _CACHE.setdefault("fts", {})
    if fts_cache.get("tokenizer"):
        return fts_cache["tokenizer"]
    token_spec = _resolve_fts_settings()[0]
    fts_cache["tokenizer"] = token_spec.split()[0] if token_spec else "trigram"
    return fts_cache["tokenizer"]


def _get_textmap_fts_detail() -> str:
    fts_cache = _CACHE.setdefault("fts", {})
    if fts_cache.get("detail") is None:
        _has_textmap_fts()
    return fts_cache.get("detail") or "full"


def _get_textmap_fts_langs() -> set[int]:
    """
    获取启用了全文搜索的语言代码
    """
    fts_cache = _CACHE.setdefault("fts", {})
    if fts_cache.get("langs") is not None:
        return fts_cache["langs"]

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
    fts_cache["langs"] = langs
    return fts_cache["langs"]


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
    raw_text = (keyword or "").strip()
    text = fts_tokenizer.normalize_search_keyword(raw_text) or raw_text
    if not text:
        return None
    literal_text = text

    tokenizer = _get_textmap_fts_tokenizer()
    stopwords, min_len, max_len = _resolve_fts_query_filters()
    segmenter, user_dict = _resolve_fts_chinese_segmenter_settings()

    # When using word tokenizers (e.g. unicode61), keep non-Chinese
    # languages on LIKE path to preserve substring-match behavior.
    if tokenizer != "trigram" and lang_code not in CHINESE_LANG_CODES:
        return None

    terms = fts_tokenizer.split_search_terms(raw_text)
    if not terms:
        return None

    if len(terms) > 1:
        if tokenizer == "trigram":
            detail = _get_textmap_fts_detail()
            fts_tokens: list[str] = []
            seen_tokens: set[str] = set()

            def _add_trigram_token(token: str) -> None:
                if token in seen_tokens:
                    return
                seen_tokens.add(token)
                escaped_token = token.replace('"', '""')
                fts_tokens.append(f'"{escaped_token}"')

            for term in terms:
                term_len = len(term)
                if term_len < 3 or term_len < min_len or term_len > max_len:
                    continue
                if term.lower() in stopwords:
                    continue
                if term_len == 3 or detail == "full":
                    _add_trigram_token(term)
                else:
                    for index in range(term_len - 2):
                        _add_trigram_token(term[index:index + 3])

            if not fts_tokens:
                return None
            return " AND ".join(fts_tokens)

        def _build_non_trigram_term_clause(term_text: str) -> str | None:
            def _filter_parts(parts: list[str]) -> list[str]:
                dedup_parts = []
                seen = set()
                for part in parts:
                    if part in seen:
                        continue
                    seen.add(part)
                    dedup_parts.append(part)

                filtered_parts = []
                for part in dedup_parts:
                    plen = len(part)
                    if plen < min_len or plen > max_len:
                        continue
                    if part.lower() in stopwords:
                        continue
                    filtered_parts.append(part)
                return filtered_parts

            def _build_and_clause(parts: list[str]) -> str | None:
                filtered_parts = _filter_parts(parts)
                if not filtered_parts:
                    return None
                return " AND ".join([f"\"{seg.replace('\"', '\"\"')}\"" for seg in filtered_parts])

            term_clauses: list[str] = []
            primary_parts = fts_tokenizer.build_fts_query_terms(
                term_text,
                lang_code,
                tokenizer,
                segmenter_mode=segmenter,
                user_dict_path=user_dict,
            )
            primary_clause = _build_and_clause(primary_parts or [term_text])
            if primary_clause:
                term_clauses.append(primary_clause)

            if lang_code in CHINESE_LANG_CODES and segmenter != "char_bigram":
                bigram_parts = fts_tokenizer.build_fts_query_terms(
                    term_text,
                    lang_code,
                    tokenizer,
                    segmenter_mode="char_bigram",
                    user_dict_path=user_dict,
                )
                bigram_clause = _build_and_clause(bigram_parts)
                if bigram_clause:
                    term_clauses.append(bigram_clause)

            term_len = len(term_text)
            if min_len <= term_len <= max_len and term_text.lower() not in stopwords:
                term_clauses.append(f"\"{term_text.replace('\"', '\"\"')}\"*")

            dedup_term_clauses: list[str] = []
            seen_term_clauses: set[str] = set()
            for clause in term_clauses:
                if clause in seen_term_clauses:
                    continue
                seen_term_clauses.add(clause)
                dedup_term_clauses.append(clause)

            if not dedup_term_clauses:
                return None
            if len(dedup_term_clauses) == 1:
                return dedup_term_clauses[0]
            return " OR ".join([f"({clause})" for clause in dedup_term_clauses])

        multi_clauses: list[str] = []
        seen_multi: set[str] = set()
        for term in terms:
            # 1-char terms may not exist as standalone tokens in bigram/word indexes.
            if len(term) < 2:
                continue
            clause = _build_non_trigram_term_clause(term)
            if not clause or clause in seen_multi:
                continue
            seen_multi.add(clause)
            multi_clauses.append(clause)

        if not multi_clauses:
            return None
        if len(multi_clauses) == 1:
            return multi_clauses[0]
        return " AND ".join([f"({clause})" for clause in multi_clauses])

    if lang_code in CHINESE_LANG_CODES:
        text = "".join(text.split())
    else:
        text = re.sub(r"\s+", " ", text)

    if tokenizer == "trigram" and len(text) < 3:
        # trigram tokenizer has poor recall on very short terms
        return None

    if tokenizer == "trigram":
        text_len = len(text)
        if text_len < min_len or text_len > max_len:
            return None
        if text.lower() in stopwords:
            return None
        if len(text) > 3 and _get_textmap_fts_detail() != "full":
            # Reduced-detail trigram indexes cannot execute multi-token phrases.
            # Every overlapping 3-gram of a contiguous substring is a necessary
            # candidate condition; LIKE verifies order and adjacency afterward.
            source_text = text if lang_code in CHINESE_LANG_CODES else literal_text
            trigrams = dict.fromkeys(source_text[index:index + 3] for index in range(len(source_text) - 2))
            return " AND ".join('"' + part.replace('"', '""') + '"' for part in trigrams)
        escaped = text.replace('"', '""')
        return f"\"{escaped}\""

    def _filter_parts(parts: list[str]) -> list[str]:
        dedup_parts = []
        seen = set()
        for part in parts:
            if part in seen:
                continue
            seen.add(part)
            dedup_parts.append(part)

        filtered_parts = []
        for part in dedup_parts:
            plen = len(part)
            if plen < min_len or plen > max_len:
                continue
            if part.lower() in stopwords:
                continue
            filtered_parts.append(part)
        return filtered_parts

    def _build_and_clause(parts: list[str]) -> str | None:
        filtered_parts = _filter_parts(parts)
        if not filtered_parts:
            return None
        return " AND ".join([f"\"{seg.replace('\"', '\"\"')}\"" for seg in filtered_parts])

    clauses: list[str] = []

    primary_parts = fts_tokenizer.build_fts_query_terms(
        text,
        lang_code,
        tokenizer,
        segmenter_mode=segmenter,
        user_dict_path=user_dict,
    )
    primary_clause = _build_and_clause(primary_parts or [text])
    if primary_clause:
        clauses.append(primary_clause)

    # Query-time segmentation can differ from the historical index build
    # environment (for example when jieba is available now but wasn't when
    # the FTS index was created). Add a char-bigram fallback so old indexes
    # remain searchable without rebuilding the DB.
    if lang_code in CHINESE_LANG_CODES and segmenter != "char_bigram":
        bigram_parts = fts_tokenizer.build_fts_query_terms(
            text,
            lang_code,
            tokenizer,
            segmenter_mode="char_bigram",
            user_dict_path=user_dict,
        )
        bigram_clause = _build_and_clause(bigram_parts)
        if bigram_clause:
            clauses.append(bigram_clause)

    text_len = len(text)
    if min_len <= text_len <= max_len and text.lower() not in stopwords:
        clauses.append(f"\"{text.replace('\"', '\"\"')}\"*")

    dedup_clauses: list[str] = []
    seen_clauses: set[str] = set()
    for clause in clauses:
        if clause in seen_clauses:
            continue
        seen_clauses.add(clause)
        dedup_clauses.append(clause)

    if not dedup_clauses:
        return None
    if len(dedup_clauses) == 1:
        return dedup_clauses[0]
    return " OR ".join([f"({clause})" for clause in dedup_clauses])


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
        if (
            _TEXTMAP_FTS_TABLE.lower() not in sql_lower
            and "genshin_short" not in sql_lower
            and "genshin_compact" not in sql_lower
            and " match " not in sql_lower
        ):
            return False

        if "genshin_compact" in sql_lower:
            return True

        message = str(exc).lower()
        fallback_markers = (
            "fts",
            "match",
            "unable to use function",
            "no such module",
            "no such table",
            "no such column: fts",
            "compact_text",
        )
        return any(marker in message for marker in fallback_markers)

    try:
        cursor.execute(sql_main, params_main)
    except sqlite3.Error as exc:
        if not _should_use_fallback(exc):
            raise
        if sql_fallback is None:
            raise
        sql_lower = sql_main.lower()
        if "genshin_short" in sql_lower:
            try:
                short_literal_cache.detach_short_fts(cursor.connection)
            except Exception:
                pass
        if "genshin_compact" in sql_lower:
            try:
                compact_text_cache.detach_compact_text(cursor.connection)
            except Exception:
                pass
        fallback_params = params_fallback if params_fallback is not None else []
        cursor.execute(sql_fallback, fallback_params)


def _resolve_textmap_fts_source(
    keyword: str,
    lang_code: int,
    connection: sqlite3.Connection | None = None,
) -> tuple[str, str] | None:
    """Resolve primary textMap_fts or sidecar short_literal_fts candidate subquery and match expr."""
    fts_match = _build_textmap_fts_match(keyword, lang_code)
    if fts_match is not None and _is_textmap_fts_lang_enabled(lang_code):
        return (
            f"select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ?",
            fts_match,
        )
    if config.getEnableTextMapFts():
        target_conn = _unwrap_sqlite_connection(
            connection if connection is not None else conn
        )
        return short_literal_cache.resolve_short_fts_query(
            target_conn,
            keyword,
            lang_code,
        )
    return None


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
    构建LIKE查询模式：单关键词统一采用连续子串匹配，非连续模糊检索仅在空格分隔多词时启用。
    """
    raw_keyword = str(keyword or "").strip()
    keyword = fts_tokenizer.normalize_search_keyword(raw_keyword) or raw_keyword
    escaped = _escape_like(keyword)
    exact = f"%{escaped}%"
    return exact, exact


def _build_literal_substring_clause(field_expr: str, term: str) -> tuple[str, list[str]]:
    if not any("A" <= ch <= "Z" or "a" <= ch <= "z" for ch in term):
        return f"instr({field_expr}, ?) > 0", [term]
    if len(term) == 1 and term.isascii() and term.isalpha():
        return (
            f"(instr({field_expr}, ?) > 0 OR instr({field_expr}, ?) > 0)",
            [term.lower(), term.upper()],
        )
    return f"{field_expr} LIKE ? ESCAPE '\\'", [f"%{_escape_like(term)}%"]


def _build_keyword_like_clause(
    field_expr: str,
    keyword: str,
    lang_code: int,
) -> tuple[str, list[str]]:
    """
    构建关键词子串查询子句：
    - 多词（空白分隔）要求同一文本包含全部词项（不限顺序）
    - 单词保持连续子串匹配，并对非 ASCII 与单 ASCII 字母使用等价更快的 instr()
    """
    terms = fts_tokenizer.split_search_terms(keyword)
    if len(terms) > 1:
        clauses: list[str] = []
        params: list[str] = []
        for term in terms:
            sub_sql, sub_params = _build_literal_substring_clause(field_expr, term)
            clauses.append(sub_sql)
            params.extend(sub_params)
        return "(" + " AND ".join(clauses) + ")", params
    exact, fuzzy = _build_like_patterns(keyword, lang_code)
    if fuzzy == exact:
        raw_keyword = str(keyword or "").strip()
        normalized = fts_tokenizer.normalize_search_keyword(raw_keyword) or raw_keyword
        if len(terms) == 1 and normalized == terms[0]:
            sub_sql, sub_params = _build_literal_substring_clause(field_expr, terms[0])
            return f"({sub_sql})", sub_params
        return f"({field_expr} LIKE ? ESCAPE '\\')", [exact]
    return (
        f"({field_expr} LIKE ? ESCAPE '\\' OR {field_expr} LIKE ? ESCAPE '\\')",
        [exact, fuzzy],
    )


def _normalize_match_keyword(keyword: str | None, lang_code: int) -> str:
    raw_text = str(keyword or "").strip()
    text = fts_tokenizer.normalize_search_keyword(raw_text) or raw_text
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
    offset: int | None = None,
    like_sql: str | None = None,
    like_params: list[str] | None = None,
    include_total: bool = False,
    extra_where_sql: str = "",
    extra_where_params: list | None = None,
    use_cached_voice: bool | None = None,
    fts_subquery: str | None = None,
) -> tuple[str, list]:
    """
    构建文本映射查询
    """
    params: list = []
    version_select = _version_select_expr("tm", "textMap")
    select_fields = f"tm.hash, tm.content, {version_select}"
    if include_total:
        select_fields += ", count(*) over() as _total_count"
    if like_sql is None:
        if fuzzy == exact:
            like_sql = "(tm.content like ? escape '\\')"
            resolved_like_params: list[str] = [exact]
        else:
            like_sql = "(tm.content like ? escape '\\' or tm.content like ? escape '\\')"
            resolved_like_params = [exact, fuzzy]
    else:
        resolved_like_params = list(like_params or [])

    if use_fts and fts_match:
        # Check language on textMap below. Accessing the unindexed FTS lang
        # column would fetch each external content row a second time.
        subquery_sql = (
            fts_subquery
            or f"select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ?"
        )
        sql = (
            f"select {select_fields} from textMap tm "
            f"where tm.id in ({subquery_sql}) "
            "and tm.lang=? "
            f"and {like_sql} "
        )
        params = [fts_match, langCode, *resolved_like_params]
    else:
        # 使用LIKE查询
        sql = (
            f"select {select_fields} from textMap tm "
            f"where tm.lang=? and {like_sql} "
        )
        params = [langCode, *resolved_like_params]

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

    if extra_where_sql:
        sql += extra_where_sql + " "
        if extra_where_params:
            params.extend(extra_where_params)

    match_sort_sql, match_sort_params = _build_match_sort_case("tm.content", keyword, langCode)
    voice_order_sql = _voice_order_expr("tm.hash", use_cached_voice=use_cached_voice)

    # 简化排序逻辑：只在SQL中做基本排序
    # 复杂的子查询（source_presence, dialogue exist check）移到应用层避免性能问题
    if hash_value is not None:
        sql += (
            "order by "
            "case when tm.hash = ? then 0 else 1 end, "
            f"{match_sort_sql}, "
            f"{voice_order_sql} "
        )
        params.append(hash_value)
        params.extend(match_sort_params)
    else:
        sql += (
            "order by "
            f"{match_sort_sql}, "
            f"{voice_order_sql} "
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
    updated_version: str | None,
    like_sql: str | None = None,
    like_params: list[str] | None = None,
    fts_subquery: str | None = None,
) -> tuple[str, list]:
    """
    构建文本映射计数查询
    """
    params: list = []
    if like_sql is None:
        if fuzzy == exact:
            like_sql = "(tm.content like ? escape '\\')"
            resolved_like_params: list[str] = [exact]
        else:
            like_sql = "(tm.content like ? escape '\\' or tm.content like ? escape '\\')"
            resolved_like_params = [exact, fuzzy]
    else:
        resolved_like_params = list(like_params or [])

    if use_fts and fts_match:
        # 使用FTS查询
        subquery_sql = (
            fts_subquery
            or f"select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ?"
        )
        sql = (
            f"select count(*) from textMap tm "
            f"where tm.id in ({subquery_sql}) "
            "and tm.lang=? "
            f"and {like_sql} "
        )
        params = [fts_match, langCode, *resolved_like_params]
    else:
        # 使用LIKE查询
        sql = (
            "select count(*) from textMap tm "
            f"where tm.lang=? and {like_sql} "
        )
        params = [langCode, *resolved_like_params]

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


def _is_hidden_quest_source_obj(obj: object) -> bool:
    if not isinstance(obj, dict):
        return False
    for key in ("showType", "FAPKBAGMFND", "questType", "NCDLPENPKKC", "OIJGOOIJBCH"):
        if obj.get(key) == "QUEST_HIDDEN":
            return True
    return False


def isHiddenQuestWithoutBody(questId: int) -> bool:
    quest_id = _coerce_optional_int(questId)
    if quest_id is None:
        return False
    quest_bin = _get_quest_bin_output(quest_id)
    main_quest = _load_main_quest_rows_by_id().get(quest_id)
    if not (_is_hidden_quest_source_obj(quest_bin) or _is_hidden_quest_source_obj(main_quest)):
        return False
    return countQuestDialogues(quest_id) <= 0


def _resolve_quest_step_text_hash(step_obj: dict) -> int | None:
    keys = (
        "AJGGCMPLKHK",
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
    steps = quest_bin.get("MEGJPCLADOG")
    if not isinstance(steps, list):
        steps = quest_bin.get("NLCNGJKMAEN")
    if not isinstance(steps, list):
        steps = quest_bin.get("subQuests")
    if not isinstance(steps, list):
        steps = quest_bin.get("GFLHMKOOHHA")
    if not isinstance(steps, list):
        return []
    return [item for item in steps if isinstance(item, dict)]


def _get_quest_bin_step_sub_id(step_obj: dict) -> int | None:
    for key in ("MPKBGPAKIOA", "subId", "KKMJBEPGLGD"):
        value = step_obj.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def _extract_step_talk_ids_from_quest_bin(quest_bin: dict) -> dict[int, list[int]]:
    result: dict[int, list[int]] = {}
    for step in _get_quest_bin_step_list(quest_bin):
        sub_id = _get_quest_bin_step_sub_id(step)
        if not isinstance(sub_id, int):
            continue

        result.setdefault(sub_id, [])
        conditions = step.get("POPHAFEBKIH")
        if not isinstance(conditions, list):
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
                condition.get("HAHEIAHBPEJ")
                or condition.get("DLPKMDPABFM")
                or condition.get("type")
                or condition.get("PAINLIBBLDK")
            )
            if cond_type not in _QUEST_STEP_TALK_CONDITION_TYPES:
                continue
            params = (
                condition.get("AAHAKNIPEDM")
                or condition.get("IEKGEJMAOCN")
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


def _get_quest_bin_talk_rows(quest_bin: dict) -> list[dict]:
    for key in ("NFFIGDHFAJG", "talks", "IBEGAHMEABP", "DGJMIPFDEOF", "DCHHEHNNEOO"):
        value = quest_bin.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _get_quest_bin_talk_id(talk_obj: dict) -> int | None:
    for key in ("NFIEHACCECI", "id", "ILHDNJDDEOP", "BLKKAMEMBBJ", "BPMABFNPCMI"):
        value = talk_obj.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def _get_talk_start_condition_sub_ids(talk_obj: dict) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    conditions = talk_obj.get("MPFAEHLBPJE")
    if not isinstance(conditions, list):
        conditions = talk_obj.get("beginCond")
    if not isinstance(conditions, list):
        return result

    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        cond_type = (
            condition.get("_type")
            or condition.get("type")
            or condition.get("HAHEIAHBPEJ")
            or condition.get("DLPKMDPABFM")
        )
        if cond_type != "QUEST_COND_STATE_EQUAL":
            continue
        params = (
            condition.get("_param")
            or condition.get("param")
            or condition.get("paramList")
            or condition.get("AAHAKNIPEDM")
        )
        if not isinstance(params, list) or not params:
            continue
        try:
            sub_id = int(str(params[0]))
        except (TypeError, ValueError):
            continue
        if sub_id > 0 and sub_id not in seen:
            seen.add(sub_id)
            result.append(sub_id)
    return result


def _extract_top_level_talk_step_ids_from_quest_bin(quest_bin: dict) -> dict[int, list[int]]:
    result: dict[int, list[int]] = {}
    for talk_obj in _get_quest_bin_talk_rows(quest_bin):
        talk_id = _get_quest_bin_talk_id(talk_obj)
        if not isinstance(talk_id, int):
            continue
        result.setdefault(talk_id, [])
        result[talk_id].append(talk_id)
        for sub_id in _get_talk_start_condition_sub_ids(talk_obj):
            if sub_id not in result[talk_id]:
                result[talk_id].append(sub_id)
    return result


def getQuestDescription(questId: int, langCode: int = 1) -> str:
    if _table_has_column("quest", "descTextMapHash"):
        with closing(conn.cursor()) as cursor:
            row = cursor.execute(
                "select content from quest join textMap on quest.descTextMapHash=textMap.hash "
                "where quest.questId=? and textMap.lang=?",
                (questId, langCode),
            ).fetchone()
        if row and row[0] and not is_excluded_quest_text(row[0]):
            return _normalize_output_text(row[0], langCode) or ""

    row = _load_main_quest_rows_by_id().get(questId)
    if not isinstance(row, dict):
        return ""
    desc_hash = row.get("descTextMapHash")
    if not isinstance(desc_hash, int) or desc_hash == 0:
        return ""
    desc_text = getTextMapContent(desc_hash, langCode)
    if is_excluded_quest_text(desc_text):
        return ""
    return desc_text or ""


def getQuestLongDescription(questId: int, langCode: int = 1) -> str:
    if not _table_has_column("quest", "longDescTextMapHash"):
        return ""
    with closing(conn.cursor()) as cursor:
        row = cursor.execute(
            "select content from quest join textMap on quest.longDescTextMapHash=textMap.hash "
            "where quest.questId=? and textMap.lang=?",
            (questId, langCode),
        ).fetchone()
    if row and row[0] and not is_excluded_quest_text(row[0]):
        return _normalize_output_text(row[0], langCode) or ""
    return ""


def getQuestStepTitleMap(questId: int, langCode: int = 1) -> dict[int, str]:
    cache_key = (int(questId), int(langCode))
    if cache_key in _QUEST_STEP_TALK_MAP_CACHE:
        return _QUEST_STEP_TALK_MAP_CACHE[cache_key]

    talk_title_map: dict[int, str] = {}
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
            talk_title_map.update(
                {
                    int(talk_id): _normalize_output_text(content, langCode) or ""
                    for talk_id, content in rows
                    if isinstance(talk_id, int) and content and not is_excluded_quest_text(content)
                }
            )

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
            sub_id = _get_quest_bin_step_sub_id(step)
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
                    order = step.get("DGINIFCGMGL")
                if not isinstance(order, int):
                    order = step.get("order")
                if isinstance(order, int):
                    order_by_sub_id[sub_id] = order

    talk_ids_by_sub_id = _extract_step_talk_ids_from_quest_bin(quest_bin) if isinstance(quest_bin, dict) else {}

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

    if isinstance(quest_bin, dict):
        for talk_id, sub_ids in _extract_top_level_talk_step_ids_from_quest_bin(quest_bin).items():
            if talk_id in talk_title_map:
                continue
            for sub_id in sub_ids:
                title = title_by_sub_id.get(sub_id) or ""
                if title:
                    talk_title_map.setdefault(talk_id, title)
                    break

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


def _pick_fetter_source_value(source: dict, keys: tuple[str, ...]):
    for key in keys:
        value = source.get(key)
        if value is not None:
            return value
    return None


def _extract_fetter_voice_rows(content: dict) -> list[tuple[int, int, str]]:
    raw_type = _pick_fetter_source_value(
        content,
        ("JOMDCMBGNMB", "IACPGADBANJ", "Type", "type"),
    )
    if str(raw_type or "").strip().lower() != "fetter":
        return []

    raw_voice_file = _pick_fetter_source_value(
        content,
        ("BKGHEBIGJNC", "MGOMDNKKLCP", "voiceFile", "Id", "id"),
    )
    voice_file = _coerce_optional_int(raw_voice_file)
    if voice_file is None:
        return []

    source_nodes = _pick_fetter_source_value(
        content,
        ("NEJBKIJIBHJ", "OPGDOEDEJOJ", "SourceNames", "sourceNames"),
    )
    if source_nodes is None:
        source_nodes = []
    if not isinstance(source_nodes, list):
        return []

    rows: list[tuple[int, int, str]] = []
    seen_rows: set[tuple[int, str]] = set()
    for source in source_nodes:
        if not isinstance(source, dict):
            continue
        switch_name = _pick_fetter_source_value(
            source,
            ("FBCCEBGJEDB", "IEPAMKPOOII", "avatarName", "GDIJGLOHHFM", "switchName"),
        )
        avatar_id = _map_fetter_switch_name_to_avatar_id(switch_name)
        if avatar_id <= 0:
            continue
        voice_path = _pick_fetter_source_value(
            source,
            ("sourceFileName", "NKPOOPONEHG", "MDOCAGOFPAP", "DCIHFJLBLAP", "BJDAJEKPCFP"),
        )
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


def _unwrap_sqlite_connection(target):
    if target is None:
        return None
    getter = getattr(target, "_get_connection", None)
    if callable(getter):
        try:
            return getter()
        except Exception:
            return None
    return target


def _get_connection_db_cache_key(connection: sqlite3.Connection | None) -> tuple | None:
    if connection is None:
        return None
    try:
        if getattr(connection, "total_changes", 0) != 0:
            return None
        db_rows = connection.execute("PRAGMA database_list").fetchall()
        main_path = ""
        for _seq, name, file_path in db_rows:
            if name == "main":
                main_path = str(file_path or "").strip()
                break
        if not main_path or main_path == ":memory:":
            return None
        st = os.stat(main_path)
        data_ver_row = connection.execute("PRAGMA data_version").fetchone()
        data_version = int(data_ver_row[0]) if data_ver_row else 0
        return (main_path, st.st_dev, st.st_ino, st.st_mtime_ns, st.st_size, data_version)
    except Exception:
        return None


def _get_voiced_text_hash_set(connection: sqlite3.Connection | None = None) -> frozenset[int] | None:
    raw_conn = _unwrap_sqlite_connection(connection if connection is not None else conn)
    key = _get_connection_db_cache_key(raw_conn)
    if key is None:
        return None

    cached = _VOICED_HASH_CACHE
    if cached.get("key") == key and cached.get("hashes") is not None:
        return cached["hashes"]  # type: ignore[return-value]

    try:
        with closing(raw_conn.cursor()) as cursor:
            required_tables = ("dialogue", "voice", "fetters", _FETTER_VOICE_TABLE)
            placeholders = ",".join("?" for _ in required_tables)
            existing = {
                row[0]
                for row in cursor.execute(
                    f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})",
                    required_tables,
                ).fetchall()
            }
            if len(existing) != len(required_tables):
                return None
    except Exception:
        return None

    with _VOICED_HASH_CACHE_LOCK:
        if _VOICED_HASH_CACHE.get("key") == key and _VOICED_HASH_CACHE.get("hashes") is not None:
            return _VOICED_HASH_CACHE["hashes"]  # type: ignore[return-value]
        latest_key = _get_connection_db_cache_key(raw_conn)
        if latest_key != key:
            return None
        try:
            hashes: set[int] = set()
            with closing(raw_conn.cursor()) as cursor:
                for (text_hash,) in cursor.execute(
                    "SELECT d.textHash FROM dialogue d "
                    "JOIN voice v ON v.dialogueId = d.dialogueId "
                    "WHERE d.textHash IS NOT NULL"
                ):
                    hashes.add(int(text_hash))
                for (text_hash,) in cursor.execute(
                    f"SELECT f.voiceFileTextTextMapHash FROM fetters f "
                    f"JOIN {_FETTER_VOICE_TABLE} fv ON fv.avatarId = f.avatarId AND fv.voiceFile = f.voiceFile "
                    "WHERE f.voiceFileTextTextMapHash IS NOT NULL"
                ):
                    hashes.add(int(text_hash))
            frozen = frozenset(hashes)
            _VOICED_HASH_CACHE["key"] = key
            _VOICED_HASH_CACHE["hashes"] = frozen
            return frozen
        except Exception:
            return None


def _eval_gts_has_voice(connection: sqlite3.Connection | None, text_hash) -> int:
    if text_hash is None:
        return 0
    try:
        normalized = int(text_hash)
    except (TypeError, ValueError):
        return 0
    hashes = _get_voiced_text_hash_set(connection)
    if hashes is None:
        return 0
    return 1 if normalized in hashes else 0


def _can_use_cached_voice_fn() -> bool:
    raw_conn = _unwrap_sqlite_connection(conn)
    hashes = _get_voiced_text_hash_set(raw_conn)
    if hashes is None or raw_conn is None:
        return False
    try:
        def _gts_has_voice(text_hash, _hashes=hashes):
            if text_hash is None:
                return 0
            try:
                return 1 if int(text_hash) in _hashes else 0
            except (TypeError, ValueError):
                return 0

        raw_conn.create_function("gts_has_voice", 1, _gts_has_voice, deterministic=True)
        return True
    except Exception:
        return False


def _get_known_primary_source_hash_set(
    connection: sqlite3.Connection | None = None,
) -> frozenset[int] | None:
    raw_conn = _unwrap_sqlite_connection(connection if connection is not None else conn)
    key = _get_connection_db_cache_key(raw_conn)
    if key is None:
        return None

    cached = _KNOWN_SOURCE_HASH_CACHE
    if cached.get("key") == key and cached.get("hashes") is not None:
        return cached["hashes"]  # type: ignore[return-value]

    source_specs = (
        ("dialogue", "textHash"),
        ("fetters", "voiceFileTextTextMapHash"),
        ("fetterStory", "storyContextTextMapHash"),
        ("fetterStory", "storyContext2TextMapHash"),
        ("quest_hash_map", "hash"),
        ("text_source_entity", "text_hash"),
        ("text_source_entity", "title_hash"),
        ("readable", "titleTextMapHash"),
    )
    active_specs: list[tuple[str, str]] = []
    for table_name, col_name in source_specs:
        if _table_exists(table_name) and _table_has_column(table_name, col_name):
            active_specs.append((table_name, col_name))

    with _KNOWN_SOURCE_HASH_CACHE_LOCK:
        if (
            _KNOWN_SOURCE_HASH_CACHE.get("key") == key
            and _KNOWN_SOURCE_HASH_CACHE.get("hashes") is not None
        ):
            return _KNOWN_SOURCE_HASH_CACHE["hashes"]  # type: ignore[return-value]
        latest_key = _get_connection_db_cache_key(raw_conn)
        if latest_key != key:
            return None
        try:
            hashes: set[int] = set()
            with closing(raw_conn.cursor()) as cursor:
                for table_name, col_name in active_specs:
                    for (text_hash,) in cursor.execute(
                        f"SELECT {col_name} FROM {table_name} WHERE {col_name} IS NOT NULL"
                    ):
                        hashes.add(int(text_hash))
            frozen = frozenset(hashes)
            _KNOWN_SOURCE_HASH_CACHE["key"] = key
            _KNOWN_SOURCE_HASH_CACHE["hashes"] = frozen
            return frozen
        except Exception:
            return None


def warmup_runtime_caches() -> None:
    """Warm process-wide read-only caches in the background without failing startup."""
    try:
        _ensure_fetter_voice_data()
        _get_voiced_text_hash_set()
    except Exception:
        pass
    try:
        short_literal_cache.warmup_short_fts(_unwrap_sqlite_connection(conn))
    except Exception:
        pass
    try:
        compact_text_cache.warmup_compact_text(_unwrap_sqlite_connection(conn))
    except Exception:
        return


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


def _voice_exists_expr(text_hash_field: str, use_cached_voice: bool | None = None) -> str:
    if use_cached_voice is None:
        use_cached_voice = _can_use_cached_voice_fn()
    if use_cached_voice:
        return f"gts_has_voice({text_hash_field}) = 1"
    return _text_hash_has_voice_expr(text_hash_field)


def _voice_order_expr(text_hash_field: str, use_cached_voice: bool | None = None) -> str:
    if use_cached_voice is None:
        use_cached_voice = _can_use_cached_voice_fn()
    if use_cached_voice:
        return f"case when gts_has_voice({text_hash_field}) = 1 then 0 else 1 end"
    return (
        "case when "
        f"{_text_hash_has_voice_expr(text_hash_field)} "
        "then 0 else 1 end"
    )


def _has_avatar_scoped_fetter_voice(textHash: int) -> bool:
    _ensure_fetter_voice_data()
    hashes = _get_voiced_text_hash_set()
    if hashes is not None:
        try:
            return int(textHash) in hashes
        except (TypeError, ValueError):
            return False
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
        exact = None
        title_like_sql = content_like_sql = ""
        title_like_params: list[str] = []
        content_like_params: list[str] = []
        if keyword_text:
            exact = _build_like_patterns(keyword_text, langCode)[0]
            title_like_sql, title_like_params = _build_keyword_like_clause(
                "titleText.content",
                keyword_text,
                langCode,
            )
            content_like_sql, content_like_params = _build_keyword_like_clause(
                "contentText.content",
                keyword_text,
                langCode,
            )

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
            sql += f"and ({title_like_sql} or {content_like_sql}) "
            params.extend(title_like_params)
            params.extend(content_like_params)

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
                f"when {title_like_sql} then 1 "
                "when contentText.content like ? escape '\\' then 2 "
                f"when {content_like_sql} then 3 "
                "else 4 end, "
                "length(coalesce(titleText.content, contentText.content, '')), fetters.fetterId, fv.voicePath "
            )
            params.append(exact)
            params.extend(title_like_params)
            params.append(exact)
            params.extend(content_like_params)
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
    "ITEM": "道具",
    "READABLE": "阅读物",
    "COSTUME": "角色装扮",
    "RELIC": "圣遗物",
    "WEAPON": "武器",
    "WINGS": "风之翼",
}

_READABLE_CATEGORY_PREFIXES = (
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


def _quest_text_not_excluded_clause(content_expr: str) -> tuple[str, tuple[str, ...]]:
    return build_quest_text_not_excluded_sql(content_expr)


def getReadableCategoryCode(file_name: str | None) -> str:
    normalized_file_name = _normalize_readable_file_name(file_name)
    if _table_exists("readable_meta"):
        with closing(conn.cursor()) as cursor:
            row = cursor.execute(
                "SELECT readable_category FROM readable_meta WHERE normalized_file_name=? LIMIT 1",
                (normalized_file_name,),
            ).fetchone()
        if row and row[0]:
            normalized_category = str(row[0]).strip().upper()
            if normalized_category in _READABLE_CATEGORY_FILTERS:
                return normalized_category
    normalized_name = os.path.splitext(normalized_file_name)[0]
    for code, prefix in _READABLE_CATEGORY_PREFIXES:
        if normalized_name.startswith(prefix):
            return code
    return "READABLE"


def _normalize_readable_category_filter(category: str | None) -> str | None:
    if category is None:
        return None
    normalized = str(category).strip().upper()
    if not normalized:
        return None
    if normalized in _READABLE_CATEGORY_FILTERS:
        return normalized
    return "__INVALID__"


def _fallback_readable_category_case_expr(table_alias: str) -> str:
    file_name_expr = f"{table_alias}.fileName"
    return (
        f"case "
        f"when {file_name_expr} glob 'Costume*' then 'COSTUME' "
        f"when {file_name_expr} glob 'Relic*' then 'RELIC' "
        f"when {file_name_expr} glob 'Weapon*' then 'WEAPON' "
        f"when {file_name_expr} glob 'Wings*' then 'WINGS' "
        f"else 'READABLE' end"
    )


def _build_readable_meta_join_sql(
    table_alias: str,
    meta_alias: str = "readable_meta",
) -> str:
    if not _table_exists("readable_meta"):
        return ""
    _ensure_runtime_sql_functions(conn)
    return (
        f"left join readable_meta {meta_alias} "
        f"on {meta_alias}.normalized_file_name = gts_normalize_readable_file_name({table_alias}.fileName) "
    )


def _readable_category_case_expr(table_alias: str, meta_alias: str = "readable_meta") -> str:
    fallback_expr = _fallback_readable_category_case_expr(table_alias)
    if not _table_exists("readable_meta"):
        return fallback_expr
    return f"coalesce({meta_alias}.readable_category, {fallback_expr})"


def _append_readable_category_filter_clause(
    sql: str,
    params: list,
    table_alias: str,
    category: str | None,
    meta_alias: str = "readable_meta",
) -> str:
    normalized = _normalize_readable_category_filter(category)
    if normalized is None:
        return sql
    if normalized == "__INVALID__":
        return sql + "and 1=0 "
    if normalized in {"BOOK", "ITEM"} and not _table_exists("readable_meta"):
        return sql + "and 1=0 "
    sql += f"and {_readable_category_case_expr(table_alias, meta_alias)} = ? "
    params.append(normalized)
    return sql


def _quest_talk_dialogue_join_condition(qt_alias: str = "qt", d_alias: str = "d") -> str:
    return (
        f"(({qt_alias}.coopQuestId IS NULL OR {qt_alias}.coopQuestId = 0) AND {d_alias}.coopQuestId IS NULL) "
        f"OR ({qt_alias}.coopQuestId > 0 AND {d_alias}.coopQuestId = {qt_alias}.coopQuestId)"
    )


def _has_talk_dialogue_link_table() -> bool:
    return _table_exists("talk_dialogue_link")


def _has_talk_dialogue_content_rows() -> bool:
    if not _table_exists("talk_dialogue_content"):
        return False
    with closing(conn.cursor()) as cursor:
        return cursor.execute("SELECT 1 FROM talk_dialogue_content LIMIT 1").fetchone() is not None


def _quest_talk_dialogue_join_sql(
    qt_alias: str = "qt",
    d_alias: str = "d",
    tdl_alias: str = "tdl",
) -> str:
    if _has_talk_dialogue_content_rows():
        return (
            f"join talk_dialogue_content {d_alias} "
            f"on {d_alias}.talkId = {qt_alias}.talkId "
            f"and {d_alias}.coopQuestId = coalesce({qt_alias}.coopQuestId, 0) "
        )
    if _has_talk_dialogue_link_table():
        return (
            f"join talk_dialogue_link {tdl_alias} "
            f"on {tdl_alias}.talkId = {qt_alias}.talkId "
            f"and {tdl_alias}.coopQuestId = coalesce({qt_alias}.coopQuestId, 0) "
            f"join dialogue {d_alias} on {d_alias}.dialogueId = {tdl_alias}.dialogueId "
        )
    return (
        f"join dialogue {d_alias} on {d_alias}.talkId = {qt_alias}.talkId "
        f"and ({_quest_talk_dialogue_join_condition(qt_alias, d_alias)}) "
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
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        match_sort_sql, match_sort_params = _build_match_sort_case("tm.content", keyWord, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('tm.content', langCode)})"

        sql_like = (
            "select tm.hash, tm.content from textMap tm "
            f"where tm.lang=? and {like_sql} "
            f"order by {match_sort_sql}, {normalized_length_sql}"
        )
        params_like = [langCode, *like_params, *match_sort_params]
        if limit is not None:
            sql_like += " limit ?"
            params_like.append(limit)

        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts = (
                f"select tm.hash, tm.content from textMap tm "
                f"where tm.id in ({fts_subquery}) "
                "and tm.lang=? "
                f"and {like_sql} "
                f"order by {match_sort_sql}, {normalized_length_sql}"
            )
            params_fts = [fts_match, langCode, *like_params, *match_sort_params]
            if limit is not None:
                sql_fts += " limit ?"
                params_fts.append(limit)
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            compact_table = (
                compact_text_cache.resolve_compact_text_table(cursor.connection, langCode)
                if config.getEnableTextMapFts()
                else None
            )
            if compact_table is not None:
                sql_compact = sql_like.replace("from textMap tm ", f"from {compact_table} tm ", 1)
                _execute_with_fallback(cursor, sql_compact, params_like, sql_like, params_like)
            else:
                cursor.execute(sql_like, params_like)
        return cursor.fetchall()


def _build_unknown_source_not_exists_clause(hash_field: str) -> str:
    clauses: list[str] = []
    if _table_exists("dialogue") and _table_has_column("dialogue", "textHash"):
        clauses.append(f"not exists (select 1 from dialogue _ud where _ud.textHash = {hash_field})")
    if _table_exists("fetters") and _table_has_column("fetters", "voiceFileTextTextMapHash"):
        clauses.append(
            f"not exists (select 1 from fetters _uf where _uf.voiceFileTextTextMapHash = {hash_field})"
        )
    if _table_exists("fetterStory"):
        if _table_has_column("fetterStory", "storyContextTextMapHash"):
            clauses.append(
                f"not exists (select 1 from fetterStory _us1 where _us1.storyContextTextMapHash = {hash_field})"
            )
        if _table_has_column("fetterStory", "storyContext2TextMapHash"):
            clauses.append(
                f"not exists (select 1 from fetterStory _us2 where _us2.storyContext2TextMapHash = {hash_field})"
            )
    if _table_exists("quest_hash_map") and _table_has_column("quest_hash_map", "hash"):
        clauses.append(f"not exists (select 1 from quest_hash_map _uq where _uq.hash = {hash_field})")
    if _table_exists("text_source_entity"):
        if _table_has_column("text_source_entity", "text_hash"):
            clauses.append(
                f"not exists (select 1 from text_source_entity _uet where _uet.text_hash = {hash_field})"
            )
        if _table_has_column("text_source_entity", "title_hash"):
            clauses.append(
                f"not exists (select 1 from text_source_entity _ueh where _ueh.title_hash = {hash_field})"
            )
    if _table_exists("readable") and _table_has_column("readable", "titleTextMapHash"):
        clauses.append(f"not exists (select 1 from readable _ur where _ur.titleTextMapHash = {hash_field})")
    if not clauses:
        return ""
    return "".join(f"and {clause} " for clause in clauses)


def _split_window_count_rows(rows: list[tuple]) -> tuple[list[tuple], int | None]:
    if not rows:
        return [], None
    total = int(rows[0][-1])
    return [row[:-1] for row in rows], total


def _resolve_textmap_version_filter(
    cursor: sqlite3.Cursor,
    created_version: str | None,
    updated_version: str | None,
) -> tuple[bool, dict[int, str | None], dict[int, str], tuple[int, ...] | None, tuple[int, ...] | None, bool]:
    created = _normalize_version_filter(created_version)
    updated = _normalize_version_filter(updated_version)
    has_version_cols = _has_version_id_columns("textMap") and _has_version_dim()
    if not has_version_cols:
        return False, {}, {}, None, None, bool(created or updated)

    rows = cursor.execute(
        f"SELECT id, raw_version, version_tag FROM {_VERSION_DIM_TABLE}"
    ).fetchall()
    id_to_raw: dict[int, str | None] = {}
    id_to_norm_tag: dict[int, str] = {}
    created_list: list[int] = []
    updated_list: list[int] = []
    for vid, raw_v, vtag in rows:
        vid_int = int(vid)
        id_to_raw[vid_int] = raw_v
        base_tag = vtag if vtag is not None else (raw_v if raw_v is not None else "")
        id_to_norm_tag[vid_int] = str(base_tag).strip().lower()
        if created and (vtag or "") == created:
            created_list.append(vid_int)
        if updated and (vtag or "") == updated:
            updated_list.append(vid_int)

    created_ids = tuple(created_list) if created else None
    updated_ids = tuple(updated_list) if updated else None
    impossible = bool((created and not created_list) or (updated and not updated_list))
    return True, id_to_raw, id_to_norm_tag, created_ids, updated_ids, impossible


def _compute_textmap_match_rank(content: str | None, norm_kw: str, is_zh: bool) -> int:
    if not norm_kw:
        return 0
    s = (content or "").strip()
    if is_zh and (" " in s or "\t" in s or "\n" in s or "\r" in s):
        s = s.replace(" ", "").replace("\t", "").replace("\n", "").replace("\r", "")
    s_low = s.lower()
    if s_low == norm_kw:
        return 0
    if s_low.startswith(norm_kw):
        return 1
    if norm_kw in s_low:
        return 2
    return 3


def _stream_textmap_bucketed_paged(
    cursor: sqlite3.Cursor,
    keyWord: str,
    langCode: int,
    limit: int | None,
    offset: int | None,
    hash_value: int | None,
    voice_filter: str | None,
    created_version: str | None,
    updated_version: str | None,
    *,
    unknown_only: bool = False,
) -> tuple[list[tuple], int] | None:
    raw_conn = _unwrap_sqlite_connection(cursor.connection)
    compact_table = (
        compact_text_cache.resolve_compact_text_table(raw_conn, langCode)
        if config.getEnableTextMapFts()
        else None
    )
    voiced_set: frozenset[int] | None = None
    known_set: frozenset[int] | None = None
    if compact_table is None:
        voiced_set = _get_voiced_text_hash_set(raw_conn)
        if voiced_set is None:
            return None
        if unknown_only:
            known_set = _get_known_primary_source_hash_set(raw_conn)
            if known_set is None:
                return None

    (
        has_version_cols,
        id_to_raw,
        id_to_norm_tag,
        created_ids,
        updated_ids,
        impossible,
    ) = _resolve_textmap_version_filter(cursor, created_version, updated_version)
    if impossible:
        return [], 0

    like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
    where_parts = ["tm.lang = ?", like_sql]
    params: list = [langCode, *like_params]
    if has_version_cols:
        if created_ids is not None:
            placeholders = ",".join("?" for _ in created_ids)
            where_parts.append(f"tm.created_version_id IN ({placeholders})")
            params.extend(created_ids)
        if updated_ids is not None:
            placeholders = ",".join("?" for _ in updated_ids)
            where_parts.append(f"tm.updated_version_id IN ({placeholders})")
            params.extend(updated_ids)

    if compact_table is not None:
        if voice_filter == "with":
            where_parts.append("(tm.flags & 1) != 0")
        elif voice_filter == "without":
            where_parts.append("(tm.flags & 1) = 0")
        if unknown_only:
            where_parts.append("(tm.flags & 2) = 0")
        sql_stream = (
            "SELECT tm.hash, tm.content, tm.created_version_id, tm.updated_version_id, tm.flags "
            f"FROM {compact_table} tm WHERE {' AND '.join(where_parts)}"
        )
    else:
        ver_cols = (
            "tm.created_version_id, tm.updated_version_id"
            if has_version_cols
            else "NULL, NULL"
        )
        sql_stream = (
            f"SELECT tm.hash, tm.content, {ver_cols}, 0 "
            f"FROM textMap tm WHERE {' AND '.join(where_parts)}"
        )

    try:
        cursor.execute(sql_stream, params)
    except sqlite3.Error as exc:
        if "interrupted" in str(exc).lower():
            raise
        if compact_table is not None:
            compact_text_cache.detach_compact_text(raw_conn)
            return None
        raise

    safe_offset = max(0, int(offset or 0))
    safe_limit = None if limit is None else max(0, int(limit))
    cap = None if safe_limit is None else (safe_offset + safe_limit)
    norm_kw = _normalize_match_keyword(keyWord, langCode)
    is_zh = langCode in CHINESE_LANG_CODES
    check_hash = hash_value is not None
    check_actual_update = updated_ids is not None
    need_py_voice_filter = (compact_table is None) and (voice_filter in ("with", "without"))
    need_py_unknown_filter = (compact_table is None) and unknown_only

    buckets: list[list[tuple[object, object, object, object]]] = [[] for _ in range(16)]
    bucket_counts = [0] * 16
    total = 0

    for row_hash, content, cv_id, uv_id, flags in cursor:
        if (
            check_actual_update
            and cv_id is not None
            and uv_id is not None
            and cv_id in id_to_norm_tag
            and uv_id in id_to_norm_tag
            and id_to_norm_tag[cv_id] == id_to_norm_tag[uv_id]
        ):
            continue
        if need_py_unknown_filter and row_hash in known_set:  # type: ignore[operator]
            continue
        if compact_table is not None:
            has_voice = bool(flags & 1)
        else:
            has_voice = row_hash in voiced_set  # type: ignore[operator]
            if need_py_voice_filter:
                if (voice_filter == "with" and not has_voice) or (
                    voice_filter == "without" and has_voice
                ):
                    continue
        total += 1
        if cap == 0:
            continue
        hash_rank = 0 if (check_hash and row_hash == hash_value) else 1
        match_rank = _compute_textmap_match_rank(content, norm_kw, is_zh)
        voice_rank = 0 if has_voice else 1
        bidx = (hash_rank * 8) + (match_rank * 2) + voice_rank
        if cap is None or bucket_counts[bidx] < cap:
            buckets[bidx].append((row_hash, content, cv_id, uv_id))
            bucket_counts[bidx] += 1

    if safe_limit == 0 or safe_offset >= total:
        return [], total

    ordered: list[tuple[object, object, object, object]] = []
    for bucket in buckets:
        if not bucket:
            continue
        if cap is None:
            ordered.extend(bucket)
        else:
            remaining = cap - len(ordered)
            if remaining <= 0:
                break
            ordered.extend(bucket[:remaining])

    page_slice = (
        ordered[safe_offset:]
        if safe_limit is None
        else ordered[safe_offset:safe_offset + safe_limit]
    )
    hydrated = [
        (
            row_hash,
            content,
            id_to_raw.get(int(cv_id)) if has_version_cols and cv_id is not None else None,
            id_to_raw.get(int(uv_id)) if has_version_cols and uv_id is not None else None,
        )
        for row_hash, content, cv_id, uv_id in page_slice
    ]
    return hydrated, total


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
    """
    _ensure_fetter_voice_data()
    use_cached_voice = _can_use_cached_voice_fn()
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        fts_subquery, fts_match = fts_source if fts_source is not None else (None, None)
        hash_value = hash_value if hash_value is not None else -1

        if fts_subquery is None or fts_match is None:
            streamed = _stream_textmap_bucketed_paged(
                cursor,
                keyWord,
                langCode,
                limit,
                offset,
                hash_value,
                voice_filter,
                created_version,
                updated_version,
            )
            if streamed is not None:
                return streamed[0]

        voice_expr = _voice_exists_expr("tm.hash", use_cached_voice=use_cached_voice)

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
            offset=offset,
            like_sql=like_sql,
            like_params=like_params,
            use_cached_voice=use_cached_voice,
        )

        # 如果启用了FTS且有匹配表达式，构建FTS查询
        if fts_subquery is not None and fts_match is not None:
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
                offset=offset,
                like_sql=like_sql,
                like_params=like_params,
                use_cached_voice=use_cached_voice,
                fts_subquery=fts_subquery,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        return cursor.fetchall()


def selectTextMapFromKeywordPagedWithCount(
    keyWord: str,
    langCode: int,
    limit: int,
    offset: int,
    hash_value: int | None = None,
    voice_filter: str | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> tuple[list[tuple], int]:
    """Single-pass paged textMap search returning (rows, total_count)."""
    _ensure_fetter_voice_data()
    use_cached_voice = _can_use_cached_voice_fn()
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        fts_subquery, fts_match = fts_source if fts_source is not None else (None, None)
        hash_value = hash_value if hash_value is not None else -1

        if fts_subquery is None or fts_match is None:
            streamed = _stream_textmap_bucketed_paged(
                cursor,
                keyWord,
                langCode,
                limit,
                offset,
                hash_value,
                voice_filter,
                created_version,
                updated_version,
            )
            if streamed is not None:
                return streamed

        voice_expr = _voice_exists_expr("tm.hash", use_cached_voice=use_cached_voice)

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
            offset=offset,
            like_sql=like_sql,
            like_params=like_params,
            include_total=True,
            use_cached_voice=use_cached_voice,
        )

        if fts_subquery is not None and fts_match is not None:
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
                offset=offset,
                like_sql=like_sql,
                like_params=like_params,
                include_total=True,
                use_cached_voice=use_cached_voice,
                fts_subquery=fts_subquery,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        raw_rows = cursor.fetchall()

    rows, total = _split_window_count_rows(raw_rows)
    if total is not None:
        return rows, total
    if not offset or offset <= 0:
        return [], 0
    if voice_filter in ("with", "without"):
        return [], countTextMapFromKeywordVoice(
            keyWord, langCode, voice_filter, created_version, updated_version
        )
    return [], countTextMapFromKeyword(keyWord, langCode, created_version, updated_version)


def selectUnknownTextMapFromKeywordPagedWithCount(
    keyWord: str,
    langCode: int,
    limit: int,
    offset: int = 0,
    hash_value: int | None = None,
    voice_filter: str | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> tuple[list[tuple], int]:
    """Filter textMap rows without any known primary source directly in SQL."""
    _ensure_fetter_voice_data()
    use_cached_voice = _can_use_cached_voice_fn()
    extra_where_sql = _build_unknown_source_not_exists_clause("tm.hash")
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        fts_subquery, fts_match = fts_source if fts_source is not None else (None, None)
        resolved_hash = hash_value if hash_value is not None else -1

        if fts_subquery is None or fts_match is None:
            streamed = _stream_textmap_bucketed_paged(
                cursor,
                keyWord,
                langCode,
                limit,
                offset,
                resolved_hash,
                voice_filter,
                created_version,
                updated_version,
                unknown_only=True,
            )
            if streamed is not None:
                return streamed

        voice_expr = _voice_exists_expr("tm.hash", use_cached_voice=use_cached_voice)

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
            hash_value=resolved_hash,
            limit=limit,
            offset=offset,
            like_sql=like_sql,
            like_params=like_params,
            include_total=True,
            extra_where_sql=extra_where_sql,
            use_cached_voice=use_cached_voice,
        )

        if fts_subquery is not None and fts_match is not None:
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
                hash_value=resolved_hash,
                limit=limit,
                offset=offset,
                like_sql=like_sql,
                like_params=like_params,
                include_total=True,
                extra_where_sql=extra_where_sql,
                use_cached_voice=use_cached_voice,
                fts_subquery=fts_subquery,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        raw_rows = cursor.fetchall()

    rows, total = _split_window_count_rows(raw_rows)
    if total is not None:
        return rows, total
    if not offset or offset <= 0:
        return [], 0

    with closing(conn.cursor()) as cursor:
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        fts_subquery, fts_match = fts_source if fts_source is not None else (None, None)
        count_voice_filter = voice_filter if voice_filter in ("with", "without") else None
        sql_count_like, count_params_like = _build_textmap_count_query(
            use_fts=False,
            langCode=langCode,
            exact=exact,
            fuzzy=fuzzy,
            fts_match=fts_match,
            voice_expr=voice_expr if count_voice_filter else None,
            voice_filter=count_voice_filter,
            created_version=created_version,
            updated_version=updated_version,
            like_sql=like_sql,
            like_params=like_params,
        )
        if extra_where_sql:
            sql_count_like += f" {extra_where_sql}"

        if fts_subquery is not None and fts_match is not None:
            sql_count_fts, count_params_fts = _build_textmap_count_query(
                use_fts=True,
                langCode=langCode,
                exact=exact,
                fuzzy=fuzzy,
                fts_match=fts_match,
                voice_expr=voice_expr if count_voice_filter else None,
                voice_filter=count_voice_filter,
                created_version=created_version,
                updated_version=updated_version,
                like_sql=like_sql,
                like_params=like_params,
                fts_subquery=fts_subquery,
            )
            if extra_where_sql:
                sql_count_fts += f" {extra_where_sql}"
            _execute_with_fallback(
                cursor, sql_count_fts, count_params_fts, sql_count_like, count_params_like
            )
        else:
            cursor.execute(sql_count_like, count_params_like)
        row = cursor.fetchone()
        return [], int(row[0]) if row else 0


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
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        fts_subquery, fts_match = fts_source if fts_source is not None else (None, None)

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
            updated_version=updated_version,
            like_sql=like_sql,
            like_params=like_params,
        )

        if fts_subquery is not None and fts_match is not None:
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
                updated_version=updated_version,
                like_sql=like_sql,
                like_params=like_params,
                fts_subquery=fts_subquery,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            compact_table = (
                compact_text_cache.resolve_compact_text_table(cursor.connection, langCode)
                if config.getEnableTextMapFts()
                else None
            )
            if compact_table is not None:
                sql_compact = sql_like.replace("from textMap tm ", f"from {compact_table} tm ", 1)
                _execute_with_fallback(cursor, sql_compact, params_like, sql_like, params_like)
            else:
                cursor.execute(sql_like, params_like)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


# ── 来源类型筛选查询 ──────────────────────────────────────────────

_SOURCE_TYPE_TO_ENTITY_CODE = {
    "item": 1, "food": 2, "furnishing": 3, "gadget": 4,
    "costume": 5, "suit": 6,
    "qianxing_emoji": 27, "qianxing_pose": 28, "qianxing_effect": 29, "qianxing_hall": 30,
    "weapon": 9, "reliquary": 10, "monster": 11, "creature": 12,
    "material": 13,
    "blueprint": 14, "gcg": 15, "namecard": 16, "performance": 17,
    "avatar_intro": 18, "dressing": 19, "music_theme": 20, "other_mat": 21,
    "avatar_mat": 22, "achievement": 23, "viewpoint": 24, "dungeon": 25,
    "loading_tip": 26,
    "map": 31,
}

_QIANXING_SOURCE_TYPE_CODES: tuple[int, ...] = (5, 6, 27, 28, 29, 30)
_QIANXING_DISPLAY_SOURCE_TYPE_CODE = 5
_MAP_SOURCE_TYPE_CODES: tuple[int, ...] = (31, 32, 33, 34, 35)
_MAP_DISPLAY_SOURCE_TYPE_CODE = 31


def _expand_qianxing_source_type_codes(source_type_code: int | None) -> tuple[int, ...] | None:
    if source_type_code is None:
        return None
    try:
        normalized = int(source_type_code)
    except (TypeError, ValueError):
        return None
    if normalized == _QIANXING_DISPLAY_SOURCE_TYPE_CODE:
        return _QIANXING_SOURCE_TYPE_CODES
    if normalized == _MAP_DISPLAY_SOURCE_TYPE_CODE:
        return _MAP_SOURCE_TYPE_CODES
    return (normalized,)


def _display_catalog_source_type_code(source_type_code: int) -> int:
    normalized = int(source_type_code)
    if normalized in _QIANXING_SOURCE_TYPE_CODES:
        return _QIANXING_DISPLAY_SOURCE_TYPE_CODE
    if normalized in _MAP_SOURCE_TYPE_CODES:
        return _MAP_DISPLAY_SOURCE_TYPE_CODE
    return normalized


def _build_entity_source_join_clause(source_codes: tuple[int, ...]) -> tuple[str, list]:
    placeholders = ",".join("?" for _ in source_codes)
    return (
        f"JOIN text_source_entity _sj ON _sj.text_hash = tm.hash AND _sj.source_type_code IN ({placeholders}) ",
        list(source_codes),
    )


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
    elif source_type == "costume":
        return _build_entity_source_join_clause(_QIANXING_SOURCE_TYPE_CODES)
    elif source_type == "map":
        return _build_entity_source_join_clause(_MAP_SOURCE_TYPE_CODES)
    elif source_type in _SOURCE_TYPE_TO_ENTITY_CODE:
        code = _SOURCE_TYPE_TO_ENTITY_CODE[source_type]
        return _build_entity_source_join_clause((code,))
    elif source_type.startswith("custom_"):
        try:
            code = int(source_type[len("custom_"):])
            return _build_entity_source_join_clause((code,))
        except ValueError:
            pass
    return "", []


def _build_small_source_driven_join(source_type: str) -> tuple[str, str, list] | None:
    """For small source tables when FTS is off, return (from_clause, hash_expr, params)."""
    if source_type == "voice":
        return "FROM fetters _sj ", "_sj.voiceFileTextTextMapHash", []
    if source_type == "story":
        return (
            "FROM ("
            "SELECT storyContextTextMapHash AS text_hash FROM fetterStory WHERE storyContextTextMapHash IS NOT NULL "
            "UNION "
            "SELECT storyContext2TextMapHash AS text_hash FROM fetterStory WHERE storyContext2TextMapHash IS NOT NULL"
            ") _sj ",
            "_sj.text_hash",
            [],
        )
    if source_type == "costume":
        placeholders = ",".join("?" for _ in _QIANXING_SOURCE_TYPE_CODES)
        return (
            f"FROM (SELECT text_hash FROM text_source_entity WHERE source_type_code IN ({placeholders})) _sj ",
            "_sj.text_hash",
            list(_QIANXING_SOURCE_TYPE_CODES),
        )
    if source_type == "map":
        placeholders = ",".join("?" for _ in _MAP_SOURCE_TYPE_CODES)
        return (
            f"FROM (SELECT text_hash FROM text_source_entity WHERE source_type_code IN ({placeholders})) _sj ",
            "_sj.text_hash",
            list(_MAP_SOURCE_TYPE_CODES),
        )
    if source_type in _SOURCE_TYPE_TO_ENTITY_CODE:
        code = _SOURCE_TYPE_TO_ENTITY_CODE[source_type]
        return (
            "FROM (SELECT text_hash FROM text_source_entity WHERE source_type_code = ?) _sj ",
            "_sj.text_hash",
            [code],
        )
    if source_type.startswith("custom_"):
        try:
            code = int(source_type[len("custom_"):])
            return (
                "FROM (SELECT text_hash FROM text_source_entity WHERE source_type_code = ?) _sj ",
                "_sj.text_hash",
                [code],
            )
        except ValueError:
            return None
    return None


def _build_source_type_like_query(
    keyWord: str,
    langCode: int,
    source_type: str,
    like_sql: str,
    like_params: list,
    created_version: str | None,
    updated_version: str | None,
    version_select: str,
) -> tuple[str, list] | None:
    small_driven = _build_small_source_driven_join(source_type)
    if small_driven is not None:
        from_clause, hash_expr, join_params = small_driven
        sql = (
            f"SELECT DISTINCT tm.hash AS hash, tm.content AS content, {version_select} "
            f"{from_clause}"
            f"CROSS JOIN textMap tm ON tm.hash = {hash_expr} AND tm.lang = ? "
            f"WHERE {like_sql} "
        )
        params = [*join_params, langCode, *like_params]
        sql = _append_version_filter_clause(sql, params, "tm", created_version, updated_version, "textMap")
        return sql, params

    join_clause, join_params = _build_source_type_join(source_type)
    if not join_clause:
        return None
    sql = (
        f"SELECT DISTINCT tm.hash AS hash, tm.content AS content, {version_select} FROM textMap tm "
        f"{join_clause}"
        f"WHERE tm.lang=? AND {like_sql} "
    )
    params = [*join_params, langCode, *like_params]
    sql = _append_version_filter_clause(sql, params, "tm", created_version, updated_version, "textMap")
    return sql, params


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
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        version_select = _version_select_expr("tm", "textMap")

        built_like = _build_source_type_like_query(
            keyWord, langCode, source_type, like_sql, like_params,
            created_version, updated_version, version_select,
        )
        if not built_like:
            return []
        sql_like, params_like = built_like
        match_sort_sql, match_sort_params = _build_match_sort_case("tm.content", keyWord, langCode)
        sql_like += f"ORDER BY {match_sort_sql}, length(tm.content), tm.hash LIMIT ? OFFSET ?"
        params_like.extend([*match_sort_params, limit, offset])

        # FTS 查询
        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts = (
                f"SELECT DISTINCT tm.hash, tm.content, {version_select} FROM textMap tm "
                f"{join_clause}"
                f"WHERE tm.id IN ({fts_subquery}) "
                "AND tm.lang=? "
                f"AND {like_sql} "
            )
            params_fts = [*join_params, fts_match, langCode, *like_params]
            sql_fts = _append_version_filter_clause(sql_fts, params_fts, "tm", created_version, updated_version, "textMap")
            sql_fts += f"ORDER BY {match_sort_sql}, length(tm.content), tm.hash LIMIT ? OFFSET ?"
            params_fts.extend([*match_sort_params, limit, offset])
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            compact_table = (
                compact_text_cache.resolve_compact_text_table(cursor.connection, langCode)
                if config.getEnableTextMapFts() and _build_small_source_driven_join(source_type) is None
                else None
            )
            if compact_table is not None:
                sql_compact = sql_like.replace("FROM textMap tm ", f"FROM {compact_table} tm ", 1)
                _execute_with_fallback(cursor, sql_compact, params_like, sql_like, params_like)
            else:
                cursor.execute(sql_like, params_like)

        return cursor.fetchall()


def selectTextMapFromKeywordBySourceTypeWithCount(
    keyWord: str,
    langCode: int,
    source_type: str,
    limit: int,
    offset: int = 0,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> tuple[list[tuple], int]:
    """Single-pass source-type filtered textMap search returning (rows, total_count)."""
    join_clause, join_params = _build_source_type_join(source_type)
    if not join_clause:
        return [], 0

    _ensure_fetter_voice_data()
    has_text_version = _has_version_id_columns("textMap") and _has_version_dim()
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        inner_version_select = (
            "tm.created_version_id AS created_version_id, tm.updated_version_id AS updated_version_id"
            if has_text_version
            else "NULL AS created_version, NULL AS updated_version"
        )
        outer_sort_sql, outer_sort_params = _build_match_sort_case("content", keyWord, langCode)

        built_like = _build_source_type_like_query(
            keyWord, langCode, source_type, like_sql, like_params,
            created_version, updated_version, inner_version_select,
        )
        if not built_like:
            return [], 0
        inner_like_sql, inner_like_params = built_like

        def _wrap_paged_count(inner_sql: str) -> str:
            if has_text_version:
                return (
                    f"WITH deduped AS ({inner_sql}) "
                    "SELECT p.hash, p.content, "
                    f"(SELECT raw_version FROM {_VERSION_DIM_TABLE} vd WHERE vd.id = p.created_version_id) AS created_version, "
                    f"(SELECT raw_version FROM {_VERSION_DIM_TABLE} vd WHERE vd.id = p.updated_version_id) AS updated_version, "
                    "p._total_count "
                    "FROM ("
                    "SELECT hash, content, created_version_id, updated_version_id, COUNT(*) OVER() AS _total_count "
                    f"FROM deduped ORDER BY {outer_sort_sql}, length(content), hash LIMIT ? OFFSET ?"
                    ") p"
                )
            return (
                f"WITH deduped AS ({inner_sql}) "
                "SELECT hash, content, created_version, updated_version, COUNT(*) OVER() AS _total_count "
                f"FROM deduped ORDER BY {outer_sort_sql}, length(content), hash LIMIT ? OFFSET ?"
            )

        sql_like = _wrap_paged_count(inner_like_sql)
        params_like = [*inner_like_params, *outer_sort_params, limit, offset]

        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            inner_fts_sql = (
                f"SELECT DISTINCT tm.hash AS hash, tm.content AS content, {inner_version_select} FROM textMap tm "
                f"{join_clause}"
                f"WHERE tm.id IN ({fts_subquery}) "
                "AND tm.lang=? "
                f"AND {like_sql} "
            )
            inner_fts_params = [*join_params, fts_match, langCode, *like_params]
            inner_fts_sql = _append_version_filter_clause(
                inner_fts_sql, inner_fts_params, "tm", created_version, updated_version, "textMap"
            )
            sql_fts = _wrap_paged_count(inner_fts_sql)
            params_fts = [*inner_fts_params, *outer_sort_params, limit, offset]
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            compact_table = (
                compact_text_cache.resolve_compact_text_table(cursor.connection, langCode)
                if config.getEnableTextMapFts() and _build_small_source_driven_join(source_type) is None
                else None
            )
            if compact_table is not None:
                inner_compact_sql = inner_like_sql.replace("FROM textMap tm ", f"FROM {compact_table} tm ", 1)
                sql_compact = _wrap_paged_count(inner_compact_sql)
                _execute_with_fallback(cursor, sql_compact, params_like, sql_like, params_like)
            else:
                cursor.execute(sql_like, params_like)

        raw_rows = cursor.fetchall()

    rows, total = _split_window_count_rows(raw_rows)
    if total is not None:
        return rows, total
    if not offset or offset <= 0:
        return [], 0
    return [], countTextMapFromKeywordBySourceType(
        keyWord, langCode, source_type, created_version, updated_version
    )


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
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)

        sql_like = (
            f"SELECT COUNT(DISTINCT tm.hash) FROM textMap tm "
            f"{join_clause}"
            f"WHERE tm.lang=? AND {like_sql} "
        )
        params_like = [*join_params, langCode, *like_params]
        sql_like = _append_version_filter_clause(sql_like, params_like, "tm", created_version, updated_version, "textMap")

        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts = (
                f"SELECT COUNT(DISTINCT tm.hash) FROM textMap tm "
                f"{join_clause}"
                f"WHERE tm.id IN ({fts_subquery}) "
                "AND tm.lang=? "
                f"AND {like_sql} "
            )
            params_fts = [*join_params, fts_match, langCode, *like_params]
            sql_fts = _append_version_filter_clause(sql_fts, params_fts, "tm", created_version, updated_version, "textMap")
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            compact_table = (
                compact_text_cache.resolve_compact_text_table(cursor.connection, langCode)
                if config.getEnableTextMapFts() and _build_small_source_driven_join(source_type) is None
                else None
            )
            if compact_table is not None:
                sql_compact = sql_like.replace("FROM textMap tm ", f"FROM {compact_table} tm ", 1)
                _execute_with_fallback(cursor, sql_compact, params_like, sql_like, params_like)
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
    """
    _ensure_fetter_voice_data()
    use_cached_voice = _can_use_cached_voice_fn()
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        fts_source = _resolve_textmap_fts_source(keyWord, langCode, connection=cursor.connection)
        fts_subquery, fts_match = fts_source if fts_source is not None else (None, None)
        voice_expr = _voice_exists_expr("tm.hash", use_cached_voice=use_cached_voice)

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
            updated_version=updated_version,
            like_sql=like_sql,
            like_params=like_params,
        )

        if fts_subquery is not None and fts_match is not None:
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
                updated_version=updated_version,
                like_sql=like_sql,
                like_params=like_params,
                fts_subquery=fts_subquery,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            compact_table = (
                compact_text_cache.resolve_compact_text_table(cursor.connection, langCode)
                if config.getEnableTextMapFts()
                else None
            )
            if compact_table is not None:
                sql_compact, params_compact = _build_textmap_count_query(
                    use_fts=False,
                    langCode=langCode,
                    exact=exact,
                    fuzzy=fuzzy,
                    fts_match=None,
                    voice_expr="(tm.flags & 1) != 0",
                    voice_filter=voice_filter,
                    created_version=created_version,
                    updated_version=updated_version,
                    like_sql=like_sql,
                    like_params=like_params,
                )
                sql_compact = sql_compact.replace("from textMap tm ", f"from {compact_table} tm ", 1)
                _execute_with_fallback(cursor, sql_compact, params_compact, sql_like, params_like)
            else:
                cursor.execute(sql_like, params_like)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def isTextMapHashInKeyword(textHash: int, keyword: str, langCode: int) -> bool:
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("content", keyword, langCode)
        sql = (
            "select 1 from textMap "
            f"where hash=? and lang=? and {like_sql} "
            "limit 1"
        )
        cursor.execute(sql, (textHash, langCode, *like_params))
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


def _normalize_batch_int_ids(values: list[int] | tuple[int, ...] | set[int]) -> list[int]:
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for value in values or []:
        normalized = _coerce_optional_int(value)
        if normalized is None or normalized in seen:
            continue
        seen.add(normalized)
        normalized_ids.append(normalized)
    return normalized_ids


def selectTextMapFromTextHashesBatch(
    text_hashes: list[int],
    langs: list[int] | None = None,
) -> dict[int, list[tuple[str, int]]]:
    normalized_ids = _normalize_batch_int_ids(text_hashes)
    if not normalized_ids:
        return {}

    normalized_langs = _normalize_batch_int_ids(langs or []) if langs else []
    if langs is not None and len(langs) > 0 and not normalized_langs:
        return {}

    result: dict[int, list[tuple[str, int]]] = {}
    chunk_size = 500
    with closing(conn.cursor()) as cursor:
        lang_clause = ""
        lang_params: list[int] = []
        if normalized_langs:
            lang_placeholders = ",".join("?" for _ in normalized_langs)
            lang_clause = f" and lang in ({lang_placeholders})"
            lang_params = normalized_langs

        for start in range(0, len(normalized_ids), chunk_size):
            chunk = normalized_ids[start:start + chunk_size]
            hash_placeholders = ",".join("?" for _ in chunk)
            sql = (
                f"select hash, content, lang from textMap "
                f"where hash in ({hash_placeholders}){lang_clause}"
            )
            for text_hash, content, lang_id in cursor.execute(sql, [*chunk, *lang_params]).fetchall():
                if text_hash is None or lang_id is None:
                    continue
                result.setdefault(int(text_hash), []).append((content, int(lang_id)))
    return result


def selectVoicePathsFromTextHashesBatch(text_hashes: list[int]) -> dict[int, list[str]]:
    _ensure_fetter_voice_data()
    normalized_ids = _normalize_batch_int_ids(text_hashes)
    if not normalized_ids:
        return {}

    voiced_set = _get_voiced_text_hash_set()
    if voiced_set is not None:
        normalized_ids = [h for h in normalized_ids if h in voiced_set]
        if not normalized_ids:
            return {}

    resolved_paths: dict[int, str] = {}
    chunk_size = 500
    with closing(conn.cursor()) as cursor:
        if _table_exists("dialogue") and _table_exists("voice"):
            for start in range(0, len(normalized_ids), chunk_size):
                chunk = normalized_ids[start:start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = cursor.execute(
                    f"select dialogue.textHash, voice.voicePath from dialogue "
                    f"join voice on voice.dialogueId = dialogue.dialogueId "
                    f"where dialogue.textHash in ({placeholders})",
                    chunk,
                ).fetchall()
                for text_hash, voice_path in rows:
                    if text_hash is not None and voice_path:
                        key = int(text_hash)
                        if key not in resolved_paths:
                            resolved_paths[key] = voice_path

        remaining = [h for h in normalized_ids if h not in resolved_paths]
        if remaining and _table_exists("fetters") and _table_exists(_FETTER_VOICE_TABLE):
            for start in range(0, len(remaining), chunk_size):
                chunk = remaining[start:start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                rows = cursor.execute(
                    f"select fetters.voiceFileTextTextMapHash, fv.voicePath from fetters "
                    f"join {_FETTER_VOICE_TABLE} fv on fv.avatarId = fetters.avatarId and fv.voiceFile = fetters.voiceFile "
                    f"where fetters.voiceFileTextTextMapHash in ({placeholders})",
                    chunk,
                ).fetchall()
                for text_hash, voice_path in rows:
                    if text_hash is not None and voice_path:
                        key = int(text_hash)
                        if key not in resolved_paths:
                            resolved_paths[key] = voice_path

    return {text_hash: [voice_path] for text_hash, voice_path in resolved_paths.items()}


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


def getTextMapVersionInfoBatch(
    text_hashes: list[int],
    source_lang_code: int | None = None,
) -> dict[int, tuple[str | None, str | None]]:
    if not _has_version_id_columns("textMap") or not _has_version_dim():
        return {}
    preferred_lang = _coerce_optional_int(source_lang_code)
    if preferred_lang is None:
        return {}
    normalized_ids = _normalize_batch_int_ids(text_hashes)
    if not normalized_ids:
        return {}

    result: dict[int, tuple[str | None, str | None]] = {}
    chunk_size = 500
    with closing(conn.cursor()) as cursor:
        for start in range(0, len(normalized_ids), chunk_size):
            chunk = normalized_ids[start:start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            sql = (
                f"select tm.hash, vdc.raw_version, vdu.raw_version "
                f"from textMap tm "
                f"left join {_VERSION_DIM_TABLE} vdc on vdc.id = tm.created_version_id "
                f"left join {_VERSION_DIM_TABLE} vdu on vdu.id = tm.updated_version_id "
                f"where tm.lang=? and tm.hash in ({placeholders})"
            )
            for text_hash, created_raw, updated_raw in cursor.execute(
                sql, [preferred_lang, *chunk]
            ).fetchall():
                if text_hash is not None:
                    result[int(text_hash)] = (created_raw, updated_raw)
    return result


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
        for table_name in _VERSION_SOURCE_TABLES:
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
            elif (
                table_name != 'npc'
                and _has_version_id_columns(table_name)
                and _table_has_column(table_name, "updated_version_id")
                and _has_version_dim()
            ):
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


def getVersionFilterValues() -> dict[str, list[str]]:
    values: dict[str, set[str]] = {
        "created": set(),
        "updated": set(),
    }

    def add_rows(target: str, rows):
        for (raw_value,) in rows:
            if raw_value is None:
                continue
            text = str(raw_value).strip()
            if text:
                values[target].add(text)

    def select_versions(cursor, table_name: str, column_name: str):
        # Probe each of the small version dimension's IDs through an existing
        # index instead of scanning millions of repeated IDs with DISTINCT.
        # Older databases without that index retain their original scan plan.
        has_index = False
        for index in cursor.execute(f'PRAGMA index_list("{table_name}")').fetchall():
            if index[4]:  # A partial index may not cover this predicate.
                continue
            index_name = str(index[1]).replace('"', '""')
            columns = cursor.execute(f'PRAGMA index_info("{index_name}")').fetchall()
            if columns and columns[0][2] == column_name:
                has_index = True
                break
        if has_index:
            predicate = (
                f"EXISTS (SELECT 1 FROM {table_name} t "
                f"WHERE t.{column_name} = vd.id)"
            )
        else:
            predicate = (
                f"vd.id IN (SELECT DISTINCT t.{column_name} FROM {table_name} t "
                f"WHERE t.{column_name} IS NOT NULL)"
            )
        return cursor.execute(
            f"SELECT vd.raw_version FROM {_VERSION_DIM_TABLE} vd "
            f"WHERE {predicate} AND COALESCE(vd.raw_version, '') <> ''"
        ).fetchall()

    with closing(conn.cursor()) as cursor:
        if not _has_version_dim():
            return {"created": [], "updated": []}

        for table_name in _VERSION_SOURCE_TABLES:
            if not _table_exists(table_name) or not _table_has_column(table_name, "created_version_id"):
                continue

            rows = select_versions(cursor, table_name, "created_version_id")
            add_rows("created", rows)

            if table_name == "quest":
                if _table_exists("quest_version") and _table_has_column("quest_version", "updated_version_id"):
                    rows = select_versions(cursor, "quest_version", "updated_version_id")
                    add_rows("updated", rows)
            elif table_name != "npc" and _table_has_column(table_name, "updated_version_id"):
                rows = select_versions(cursor, table_name, "updated_version_id")
                add_rows("updated", rows)

    return {
        "created": list(values["created"]),
        "updated": list(values["updated"]),
    }


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


def selectTextHashesWithKnownPrimarySource(text_hashes: list[int]) -> set[int]:
    normalized_ids: list[int] = []
    seen_ids: set[int] = set()
    for value in text_hashes or []:
        normalized = _coerce_optional_int(value)
        if normalized is None or normalized in seen_ids:
            continue
        seen_ids.add(normalized)
        normalized_ids.append(normalized)
    if not normalized_ids:
        return set()

    placeholders = ",".join("?" for _ in normalized_ids)
    query_specs: list[str] = []

    if _table_exists("dialogue") and _table_has_column("dialogue", "textHash"):
        query_specs.append(
            f"SELECT DISTINCT textHash FROM dialogue WHERE textHash IN ({placeholders})"
        )
    if _table_exists("fetters") and _table_has_column("fetters", "voiceFileTextTextMapHash"):
        query_specs.append(
            f"SELECT DISTINCT voiceFileTextTextMapHash FROM fetters "
            f"WHERE voiceFileTextTextMapHash IN ({placeholders})"
        )
    if _table_exists("fetterStory"):
        query_specs.append(
            "SELECT DISTINCT entries.contextHash "
            f"FROM ({_avatar_story_entries_subquery()}) AS entries "
            f"WHERE entries.contextHash IN ({placeholders})"
        )
    if _table_exists("quest_hash_map") and _table_has_column("quest_hash_map", "hash"):
        query_specs.append(
            f"SELECT DISTINCT hash FROM quest_hash_map WHERE hash IN ({placeholders})"
        )
    if _table_exists("text_source_entity"):
        if _table_has_column("text_source_entity", "text_hash"):
            query_specs.append(
                f"SELECT DISTINCT text_hash FROM text_source_entity "
                f"WHERE text_hash IN ({placeholders})"
            )
        if _table_has_column("text_source_entity", "title_hash"):
            query_specs.append(
                f"SELECT DISTINCT title_hash FROM text_source_entity "
                f"WHERE title_hash IN ({placeholders})"
            )
    if _table_exists("readable") and _table_has_column("readable", "titleTextMapHash"):
        query_specs.append(
            f"SELECT DISTINCT titleTextMapHash FROM readable "
            f"WHERE titleTextMapHash IN ({placeholders})"
        )

    matches: set[int] = set()
    with closing(conn.cursor()) as cursor:
        for sql in query_specs:
            try:
                rows = cursor.execute(sql, normalized_ids).fetchall()
            except sqlite3.OperationalError:
                continue
            for row in rows:
                normalized = _coerce_optional_int(row[0] if row else None)
                if normalized is not None:
                    matches.add(normalized)
    return matches


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
    use_fts: bool = False,
    fts_match: str | None = None,
    fts_subquery: str | None = None,
    include_versions: bool = True,
    compact_table: str | None = None,
) -> tuple[str, list]:
    has_text_version = _has_version_dim() and _has_version_id_columns("textMap")
    has_entity_version = _has_version_dim() and _table_has_column("text_source_entity", "created_version_id")
    has_version = include_versions and (has_text_version or has_entity_version)
    version_lang_code = config.getSourceLanguage()

    source_type_codes = _expand_qianxing_source_type_codes(source_type_code)
    params: list = []

    if keyword and use_fts and fts_match:
        like_sql, like_params = _build_keyword_like_clause("tm_title.content", keyword, lang_code)
        subquery_sql = (
            fts_subquery
            or f"SELECT rowid FROM {_TEXTMAP_FTS_TABLE} WHERE {_TEXTMAP_FTS_TABLE} MATCH ?"
        )
        sql = (
            "WITH filtered_titles AS MATERIALIZED ("
            "SELECT tm_title.hash AS title_hash, tm_title.content AS title_text "
            "FROM textMap tm_title "
            f"WHERE tm_title.id IN ({subquery_sql}) "
            "AND tm_title.lang = ? "
            f"AND {like_sql}"
            "), "
        )
        params.extend([fts_match, lang_code, *like_params])
    else:
        entity_filter_sql = "WHERE 1=1 "
        if source_type_codes:
            placeholders = ",".join("?" for _ in source_type_codes)
            entity_filter_sql += f"AND e0.source_type_code IN ({placeholders}) "
            params.extend(source_type_codes)
        if sub_category is not None:
            entity_filter_sql += "AND e0.sub_category = ? "
            params.append(int(sub_category))

        if keyword and compact_table:
            like_sql, like_params = _build_keyword_like_clause("tm_title.content", keyword, lang_code)
            sql = (
                "WITH distinct_titles AS MATERIALIZED ("
                "SELECT DISTINCT e0.title_hash "
                "FROM text_source_entity e0 "
                f"{entity_filter_sql}"
                "), filtered_titles AS MATERIALIZED ("
                "SELECT tm_title.hash AS title_hash, tm_title.content AS title_text "
                f"FROM {compact_table} tm_title "
                "WHERE tm_title.lang = ? "
                f"AND (tm_title.flags & {compact_text_cache.FLAG_HAS_KNOWN_SOURCE}) != 0 "
                "AND tm_title.hash IN (SELECT title_hash FROM distinct_titles) "
                f"AND {like_sql} "
                "), "
            )
            params.append(lang_code)
            params.extend(like_params)
        else:
            sql = (
                "WITH distinct_titles AS MATERIALIZED ("
                "SELECT DISTINCT e0.title_hash "
                "FROM text_source_entity e0 "
                f"{entity_filter_sql}"
                "), filtered_titles AS MATERIALIZED ("
                "SELECT dt.title_hash AS title_hash, tm_title.content AS title_text "
                "FROM distinct_titles dt "
                "CROSS JOIN textMap tm_title ON tm_title.hash = dt.title_hash AND tm_title.lang = ? "
            )
            params.append(lang_code)
            if keyword:
                like_sql, like_params = _build_keyword_like_clause("tm_title.content", keyword, lang_code)
                sql += f"WHERE {like_sql} "
                params.extend(like_params)
            sql += "), "

    if not include_versions:
        sql += (
            "catalog_final AS ("
            "SELECT "
            "e.entity_id, "
            "e.source_type_code, "
            "MIN(e.title_hash) AS title_hash, "
            "MIN(e.sub_category) AS sub_category, "
            "MIN(ft.title_text) AS title_text "
            "FROM filtered_titles ft "
            "CROSS JOIN text_source_entity e ON e.title_hash = ft.title_hash "
            "WHERE 1=1 "
        )
        if source_type_codes:
            placeholders = ",".join("?" for _ in source_type_codes)
            sql += f"AND e.source_type_code IN ({placeholders}) "
            params.extend(source_type_codes)
        if sub_category is not None:
            sql += "AND e.sub_category = ? "
            params.append(int(sub_category))
        sql += "GROUP BY e.entity_id, e.source_type_code) "
        return sql, params

    sql += (
        "catalog_base AS ("
        "SELECT "
        "e.entity_id, "
        "e.source_type_code, "
        "MIN(e.title_hash) AS title_hash, "
        "MIN(e.sub_category) AS sub_category, "
        "MIN(ft.title_text) AS title_text, "
    )

    if has_version:
        if has_entity_version:
            sql += (
                "MIN(CASE WHEN e.created_version_id IS NOT NULL THEN e.created_version_id END) AS entity_created_version_id, "
                "MIN(CASE WHEN vdec.version_sort_key IS NOT NULL THEN vdec.version_sort_key END) AS entity_created_sort_key, "
            )
        else:
            sql += "NULL AS entity_created_version_id, NULL AS entity_created_sort_key, "

    if has_text_version:
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
        if not has_entity_version:
            sql += "NULL AS entity_created_version_id, NULL AS entity_created_sort_key, "
        sql += (
            "NULL AS title_created_version_id, NULL AS title_created_sort_key, "
            "NULL AS body_created_version_id, NULL AS body_created_sort_key, "
            "NULL AS title_updated_version_id, NULL AS title_updated_sort_key, "
            "NULL AS body_updated_version_id, NULL AS body_updated_sort_key "
        )

    sql += (
        "FROM filtered_titles ft "
        "CROSS JOIN text_source_entity e ON e.title_hash = ft.title_hash "
    )

    if has_entity_version:
        sql += (
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdec ON vdec.id = e.created_version_id "
        )

    if has_text_version:
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
    if source_type_codes:
        placeholders = ",".join("?" for _ in source_type_codes)
        sql += f"AND e.source_type_code IN ({placeholders}) "
        params.extend(source_type_codes)
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
        "WHEN base.entity_created_version_id IS NOT NULL THEN base.entity_created_version_id "
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


def _hydrate_catalog_entity_versions(
    cursor: sqlite3.Cursor,
    pairs: list[tuple[int, int]],
    keyword: str,
    lang_code: int,
    sub_category: int | None,
) -> dict[tuple[int, int], tuple[str | None, str | None]]:
    if not pairs:
        return {}
    has_text_version = _has_version_dim() and _has_version_id_columns("textMap")
    has_entity_version = _has_version_dim() and _table_has_column("text_source_entity", "created_version_id")
    if not (has_text_version or has_entity_version):
        return {}

    unique_pairs: list[tuple[int, int]] = []
    seen_pairs: set[tuple[int, int]] = set()
    for eid, stc in pairs:
        pair = (int(eid), int(stc))
        if pair not in seen_pairs:
            seen_pairs.add(pair)
            unique_pairs.append(pair)

    version_lang_code = config.getSourceLanguage()
    params: list = []
    pair_clauses = " OR ".join(
        "(e.source_type_code = ? AND e.entity_id = ?)" for _ in unique_pairs
    )

    sql = "WITH catalog_base AS (SELECT e.entity_id, e.source_type_code, "
    if has_entity_version:
        sql += (
            "MIN(CASE WHEN e.created_version_id IS NOT NULL THEN e.created_version_id END) AS entity_created_version_id, "
            "MIN(CASE WHEN vdec.version_sort_key IS NOT NULL THEN vdec.version_sort_key END) AS entity_created_sort_key, "
        )
    else:
        sql += "NULL AS entity_created_version_id, NULL AS entity_created_sort_key, "

    if has_text_version:
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

    if has_entity_version:
        sql += f"LEFT JOIN {_VERSION_DIM_TABLE} vdec ON vdec.id = e.created_version_id "
    if has_text_version:
        sql += (
            "LEFT JOIN textMap tm_title_version ON tm_title_version.hash = e.title_hash AND tm_title_version.lang = ? "
            "LEFT JOIN textMap tm_body_version ON tm_body_version.hash = e.text_hash AND tm_body_version.lang = ? "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdtc ON vdtc.id = tm_title_version.created_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdtu ON vdtu.id = tm_title_version.updated_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdbc ON vdbc.id = tm_body_version.created_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdbu ON vdbu.id = tm_body_version.updated_version_id "
        )
        params.extend([version_lang_code, version_lang_code])

    sql += f"WHERE ({pair_clauses}) "
    for eid, stc in unique_pairs:
        params.extend([stc, eid])
    if sub_category is not None:
        sql += "AND e.sub_category = ? "
        params.append(int(sub_category))
    if keyword:
        like_sql, like_params = _build_keyword_like_clause("tm_title.content", keyword, lang_code)
        sql += f"AND {like_sql} "
        params.extend(like_params)

    sql += (
        "GROUP BY e.entity_id, e.source_type_code), "
        "catalog_final AS ("
        "SELECT "
        "base.entity_id, "
        "base.source_type_code, "
        "CASE "
        "WHEN base.entity_created_version_id IS NOT NULL THEN base.entity_created_version_id "
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
        "SELECT cf.entity_id, cf.source_type_code, vdc.raw_version, vdu.raw_version "
        "FROM catalog_final cf "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdc ON vdc.id = cf.created_version_id "
        f"LEFT JOIN {_VERSION_DIM_TABLE} vdu ON vdu.id = cf.updated_version_id"
    )
    rows = cursor.execute(sql, params).fetchall()
    return {
        (int(eid), int(stc)): (created_v, updated_v)
        for eid, stc, created_v, updated_v in rows
    }


def _assemble_catalog_select_query(
    keyword: str,
    lang_code: int,
    source_type_code: int | None,
    sub_category: int | None,
    created_version: str | None,
    updated_version: str | None,
    *,
    has_version: bool,
    created: str | None,
    updated: str | None,
    limit: int,
    offset: int,
    include_total: bool,
    use_fts: bool,
    fts_match: str | None,
    fts_subquery: str | None = None,
    compact_table: str | None = None,
) -> tuple[str, list]:
    inline_versions = bool(has_version and (created or updated))
    base_sql, params = _build_catalog_entity_base_query(
        keyword,
        lang_code,
        source_type_code=source_type_code,
        sub_category=sub_category,
        created_version=created_version,
        updated_version=updated_version,
        use_fts=use_fts,
        fts_match=fts_match,
        fts_subquery=fts_subquery,
        include_versions=inline_versions,
        compact_table=compact_table,
    )
    total_col = ", COUNT(*) OVER() AS _total_count" if include_total else ""
    if inline_versions:
        sql = (
            base_sql +
            "SELECT "
            "base.entity_id, "
            "base.source_type_code, "
            "base.title_hash, "
            "base.sub_category, "
            "base.title_text, "
            "vdc.raw_version AS created_version, "
            f"vdu.raw_version AS updated_version{total_col} "
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
            f"NULL AS updated_version{total_col} "
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
    return sql, params


def _execute_catalog_query(
    cursor: sqlite3.Cursor,
    keyword: str,
    lang_code: int,
    build_query_fn,
) -> None:
    """Execute a catalog entity query via FTS, compact_text sidecar, or main textMap."""
    sql_like, params_like = build_query_fn(use_fts=False, fts_match=None)
    fts_source = (
        _resolve_textmap_fts_source(keyword, lang_code, connection=cursor.connection)
        if keyword
        else None
    )
    if fts_source is not None:
        fts_subquery, fts_match = fts_source
        sql_fts, params_fts = build_query_fn(
            use_fts=True,
            fts_match=fts_match,
            fts_subquery=fts_subquery,
        )
        _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        return
    if keyword:
        compact_table = (
            compact_text_cache.resolve_compact_text_table(cursor.connection, lang_code)
            if config.getEnableTextMapFts()
            else None
        )
        if compact_table is not None:
            sql_compact, params_compact = build_query_fn(
                use_fts=False,
                fts_match=None,
                compact_table=compact_table,
            )
            _execute_with_fallback(cursor, sql_compact, params_compact, sql_like, params_like)
            return
    cursor.execute(sql_like, params_like)


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
    has_version = _has_version_dim() and (
        _has_version_id_columns("textMap")
        or _table_has_column("text_source_entity", "created_version_id")
    )
    if not has_version and (created or updated):
        return []

    def _build_query(
        use_fts: bool,
        fts_match: str | None,
        fts_subquery: str | None = None,
        compact_table: str | None = None,
    ) -> tuple[str, list]:
        return _assemble_catalog_select_query(
            keyword,
            lang_code,
            source_type_code,
            sub_category,
            created_version,
            updated_version,
            has_version=has_version,
            created=created,
            updated=updated,
            limit=limit,
            offset=offset,
            include_total=False,
            use_fts=use_fts,
            fts_match=fts_match,
            fts_subquery=fts_subquery,
            compact_table=compact_table,
        )

    with closing(conn.cursor()) as cursor:
        try:
            _execute_catalog_query(cursor, keyword, lang_code, _build_query)
            rows = cursor.fetchall()
            if rows and has_version and not created and not updated:
                version_map = _hydrate_catalog_entity_versions(
                    cursor,
                    [(int(r[0]), int(r[1])) for r in rows],
                    keyword,
                    lang_code,
                    sub_category,
                )
                rows = [
                    (
                        r[0],
                        r[1],
                        r[2],
                        r[3],
                        r[4],
                        *version_map.get((int(r[0]), int(r[1])), (None, None)),
                    )
                    for r in rows
                ]
            return rows
        except Exception:
            return []


def selectCatalogEntitiesWithCount(
    keyword: str,
    lang_code: int,
    source_type_code: int | None = None,
    sub_category: int | None = None,
    limit: int = 50,
    offset: int = 0,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> tuple[list[tuple], int]:
    """Single-pass catalog search returning (rows, total_count)."""
    created = _normalize_version_filter(created_version)
    updated = _normalize_version_filter(updated_version)
    has_version = _has_version_dim() and (
        _has_version_id_columns("textMap")
        or _table_has_column("text_source_entity", "created_version_id")
    )
    if not has_version and (created or updated):
        return [], 0

    def _build_query(
        use_fts: bool,
        fts_match: str | None,
        fts_subquery: str | None = None,
        compact_table: str | None = None,
    ) -> tuple[str, list]:
        return _assemble_catalog_select_query(
            keyword,
            lang_code,
            source_type_code,
            sub_category,
            created_version,
            updated_version,
            has_version=has_version,
            created=created,
            updated=updated,
            limit=limit,
            offset=offset,
            include_total=True,
            use_fts=use_fts,
            fts_match=fts_match,
            fts_subquery=fts_subquery,
            compact_table=compact_table,
        )

    with closing(conn.cursor()) as cursor:
        try:
            _execute_catalog_query(cursor, keyword, lang_code, _build_query)
            raw_rows = cursor.fetchall()
            rows, total = _split_window_count_rows(raw_rows)
            if rows and has_version and not created and not updated:
                version_map = _hydrate_catalog_entity_versions(
                    cursor,
                    [(int(r[0]), int(r[1])) for r in rows],
                    keyword,
                    lang_code,
                    sub_category,
                )
                rows = [
                    (
                        r[0],
                        r[1],
                        r[2],
                        r[3],
                        r[4],
                        *version_map.get((int(r[0]), int(r[1])), (None, None)),
                    )
                    for r in rows
                ]
        except Exception:
            return [], 0

    if total is not None:
        return rows, total
    if not offset or offset <= 0:
        return [], 0
    return [], countCatalogEntities(
        keyword,
        lang_code,
        source_type_code=source_type_code,
        sub_category=sub_category,
        created_version=created_version,
        updated_version=updated_version,
    )


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
    has_version = _has_version_dim() and (
        _has_version_id_columns("textMap")
        or _table_has_column("text_source_entity", "created_version_id")
    )
    if not has_version and (created or updated):
        return 0

    inline_versions = bool(has_version and (created or updated))

    def _build_count_sql(
        use_fts: bool,
        fts_match: str | None,
        fts_subquery: str | None = None,
        compact_table: str | None = None,
    ) -> tuple[str, list]:
        sql, params = _build_catalog_entity_base_query(
            keyword,
            lang_code,
            source_type_code=source_type_code,
            sub_category=sub_category,
            created_version=created_version,
            updated_version=updated_version,
            use_fts=use_fts,
            fts_match=fts_match,
            fts_subquery=fts_subquery,
            include_versions=inline_versions,
            compact_table=compact_table,
        )
        if inline_versions:
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
        return sql, params

    with closing(conn.cursor()) as cursor:
        try:
            _execute_catalog_query(cursor, keyword, lang_code, _build_count_sql)
            row = cursor.fetchone()
            return int(row[0]) if row else 0
        except Exception:
            return 0


def selectCatalogCategoryPairs() -> list[tuple[int, int]]:
    """Return distinct (source_type_code, sub_category) pairs present in catalog data."""
    with closing(conn.cursor()) as cursor:
        try:
            rows = cursor.execute(
                """
                SELECT DISTINCT source_type_code, sub_category
                FROM text_source_entity
                ORDER BY source_type_code, sub_category
                """
            ).fetchall()
        except Exception:
            return []
    result: list[tuple[int, int]] = []
    seen_pairs: set[tuple[int, int]] = set()
    for row in rows:
        if not row or row[0] is None:
            continue
        pair = (_display_catalog_source_type_code(int(row[0])), int(row[1] or 0))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        result.append(pair)
    return result


def getCatalogEntityVersionInfo(source_type_code: int, entity_id: int, version_lang_code: int | None = None):
    has_text_version = _has_version_dim() and _has_version_id_columns("textMap")
    has_entity_version = _has_version_dim() and _table_has_column("text_source_entity", "created_version_id")
    if not (has_text_version or has_entity_version):
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

    sql = (
        "WITH entity_versions AS ("
        "SELECT "
    )
    params: list[object] = []
    if has_entity_version:
        sql += (
            "MIN(CASE WHEN e.created_version_id IS NOT NULL THEN e.created_version_id END) AS entity_created_version_id, "
            "MIN(CASE WHEN vdec.version_sort_key IS NOT NULL THEN vdec.version_sort_key END) AS entity_created_sort_key, "
        )
    else:
        sql += "NULL AS entity_created_version_id, NULL AS entity_created_sort_key, "

    if has_text_version:
        title_actual_update_expr = (
            "lower(trim(coalesce(vdtc.version_tag, vdtc.raw_version, ''))) "
            "!= lower(trim(coalesce(vdtu.version_tag, vdtu.raw_version, '')))"
        )
        body_actual_update_expr = (
            "lower(trim(coalesce(vdbc.version_tag, vdbc.raw_version, ''))) "
            "!= lower(trim(coalesce(vdbu.version_tag, vdbu.raw_version, '')))"
        )
        sql += (
            "MIN(CASE WHEN tm_title.created_version_id IS NOT NULL THEN tm_title.created_version_id END) AS title_created_version_id, "
            "MIN(CASE WHEN vdtc.version_sort_key IS NOT NULL THEN vdtc.version_sort_key END) AS title_created_sort_key, "
            "MIN(CASE WHEN tm_body.created_version_id IS NOT NULL THEN tm_body.created_version_id END) AS body_created_version_id, "
            "MIN(CASE WHEN vdbc.version_sort_key IS NOT NULL THEN vdbc.version_sort_key END) AS body_created_sort_key, "
            f"MAX(CASE WHEN tm_title.updated_version_id IS NOT NULL AND {title_actual_update_expr} THEN tm_title.updated_version_id END) AS title_updated_version_id, "
            f"MAX(CASE WHEN tm_title.updated_version_id IS NOT NULL AND {title_actual_update_expr} THEN vdtu.version_sort_key END) AS title_updated_sort_key, "
            f"MAX(CASE WHEN tm_body.updated_version_id IS NOT NULL AND {body_actual_update_expr} THEN tm_body.updated_version_id END) AS body_updated_version_id, "
            f"MAX(CASE WHEN tm_body.updated_version_id IS NOT NULL AND {body_actual_update_expr} THEN vdbu.version_sort_key END) AS body_updated_sort_key "
        )
    else:
        sql += (
            "NULL AS title_created_version_id, NULL AS title_created_sort_key, "
            "NULL AS body_created_version_id, NULL AS body_created_sort_key, "
            "NULL AS title_updated_version_id, NULL AS title_updated_sort_key, "
            "NULL AS body_updated_version_id, NULL AS body_updated_sort_key "
        )

    sql += "FROM text_source_entity e "
    if has_entity_version:
        sql += f"LEFT JOIN {_VERSION_DIM_TABLE} vdec ON vdec.id = e.created_version_id "
    if has_text_version:
        sql += (
            "LEFT JOIN textMap tm_title ON tm_title.hash = e.title_hash AND tm_title.lang = ? "
            "LEFT JOIN textMap tm_body ON tm_body.hash = e.text_hash AND tm_body.lang = ? "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdtc ON vdtc.id = tm_title.created_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdtu ON vdtu.id = tm_title.updated_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdbc ON vdbc.id = tm_body.created_version_id "
            f"LEFT JOIN {_VERSION_DIM_TABLE} vdbu ON vdbu.id = tm_body.updated_version_id "
        )
        params.extend([version_lang, version_lang])
    sql += (
        "WHERE e.source_type_code = ? AND e.entity_id = ?), "
        "catalog_final AS ("
        "SELECT "
        "CASE "
        "WHEN base.entity_created_version_id IS NOT NULL THEN base.entity_created_version_id "
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
    params.extend([source_type_code, entity_id])

    with closing(conn.cursor()) as cursor:
        try:
            cursor.execute(sql, params)
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
    name_cache = _get_name_cache_bucket("characters")
    if cache_key in name_cache:
        return name_cache[cache_key]
    result = _normalize_output_text(getCharacterNameRaw(avatarId, langCode), langCode)
    name_cache[cache_key] = result
    return result


def getWanderName(langCode: int = 1):
    """
    获取旅行者名称
    """
    cache_key = f"{langCode}:{_get_gender_cache_key()}"
    name_cache = _get_name_cache_bucket("wander")
    if cache_key in name_cache:
        return name_cache[cache_key]
    name_cache[cache_key] = getCharterName(10000075, langCode)
    return name_cache[cache_key]


def getTravellerName(langCode: int = 1):
    """
    获取空旅行者名称
    """
    cache_key = f"{langCode}:{_get_gender_cache_key()}"
    name_cache = _get_name_cache_bucket("traveller")
    if cache_key in name_cache:
        return name_cache[cache_key]
    name_cache[cache_key] = getCharterName(10000005, langCode)
    return name_cache[cache_key]


def _get_manual_name_or_default(placeHolderName: str, langCode: int, default: str) -> str:
    try:
        value = getManualTextMap(placeHolderName, langCode)
    except Exception:
        value = None
    return value or default


def getMateAvatarName(langCode: int = 1):
    """
    获取血亲同伴名称。男主线中同伴为荧，女主线中同伴为空。
    """
    cache_key = f"{langCode}:{_get_gender_cache_key()}"
    name_cache = _get_name_cache_bucket("mate_avatar")
    if cache_key in name_cache:
        return name_cache[cache_key]

    female_twin_name = _get_manual_name_or_default("INFO_FEMALE_PRONOUN_YING", langCode, "荧")
    male_twin_name = _get_manual_name_or_default("INFO_MALE_PRONOUN_KONG", langCode, "空")
    player_is_male = config.getIsMale()
    if player_is_male == "both":
        result = f"{{{female_twin_name}/{male_twin_name}}}"
    elif player_is_male:
        result = female_twin_name
    else:
        result = male_twin_name

    name_cache[cache_key] = result
    return result


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
    if talkerType == "TALK_ROLE_PLAYER":
        return "旅行者"
    if talkerType == "TALK_ROLE_MATE_AVATAR":
        return getMateAvatarName(langCode)

    with closing(conn.cursor()) as cursor:
        talkerName = None
        if talkerType == "TALK_ROLE_NPC":
            sqlGetNpcName = 'select content from npc, textMap where npcId = ? and textHash = hash and lang = ?'
            cursor.execute(sqlGetNpcName, (talkerId, langCode))
            ansNpcName = cursor.fetchall()
            if len(ansNpcName) > 0:
                talkerName = ansNpcName[0][0]

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
        like_sql, like_params = _build_keyword_like_clause("tm.content", keyWord, langCode)
        # 构建查询
        sql = f"""
        SELECT DISTINCT v.voicePath, d.textHash, tm.content
        FROM voice v
        JOIN dialogue d ON v.dialogueId = d.dialogueId
        JOIN textMap tm ON tm.hash = d.textHash
        WHERE tm.lang = ? AND {like_sql}
        ORDER BY tm.content
        LIMIT ? OFFSET ?
        """

        # 计算偏移量
        offset = (page - 1) * size

        # 执行查询
        cursor.execute(sql, (langCode, *like_params, size, offset))

        # 处理结果
        results = []
        for voicePath, textHash, content in cursor.fetchall():
            results.append({
                'voicePath': voicePath,
                'textHash': textHash,
                'content': content
            })

        return results


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


# 从 dbBuild/versioning.py 导入 get_current_version 函数。精简部署可能不会包含
# dbBuild；这种情况下仍应直接从运行时数据库元数据读取当前版本。
def _get_current_version_from_app_meta(default: str = "unknown") -> str:
    try:
        with closing(conn.cursor()) as cursor:
            for key in ("db_current_commit_title", "db_current_commit"):
                row = cursor.execute(
                    "SELECT v FROM app_meta WHERE k=? LIMIT 1", (key,)
                ).fetchone()
                if row and row[0]:
                    value = str(row[0]).strip()
                    if value:
                        return value
    except (AttributeError, sqlite3.DatabaseError):
        pass
    return default


try:
    import sys
    import os
    import importlib.util

    # 使用 importlib 加载模块
    dbbuild_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dbBuild')
    if dbbuild_dir not in sys.path:
        sys.path.insert(0, dbbuild_dir)
    versioning_path = os.path.join(dbbuild_dir, 'versioning.py')
    spec = importlib.util.spec_from_file_location("database_helper_versioning", versioning_path)
    if spec and spec.loader:
        versioning = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(versioning)
        _versioning_get_current_version = versioning.get_current_version
    else:
        raise ImportError("Failed to create module spec")
except Exception:
    _versioning_get_current_version = None


def get_current_version(default: str = "unknown") -> str:
    if _versioning_get_current_version is not None:
        try:
            return _versioning_get_current_version(default)
        except Exception:
            pass
    return _get_current_version_from_app_meta(default)


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
    rows, _total = selectReadableFromKeywordWithCount(
        keyword,
        langCode,
        langStr,
        limit=limit,
        offset=offset,
        created_version=created_version,
        updated_version=updated_version,
        category=category,
        include_total=False,
    )
    return rows


def selectReadableFromKeywordWithCount(
    keyword: str,
    langCode: int,
    langStr: str,
    limit: int | None = None,
    offset: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
    category: str | None = None,
    include_total: bool = True,
) -> tuple[list[tuple], int]:
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("content", keyword, langCode)
        match_sort_sql, match_sort_params = _build_match_sort_case("content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('content', langCode)})"
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return [], 0
        lang_placeholders = ",".join(["?"] * len(readable_langs))
        version_select = _version_select_expr("readable", "readable")
        readable_meta_join = _build_readable_meta_join_sql("readable")
        total_col = ", count(*) over() as _total_count" if include_total else ""
        sql = (
            f"select fileName, content, titleTextMapHash, readableId, {version_select}{total_col} from readable "
            f"{readable_meta_join}"
            f"where lang in ({lang_placeholders}) and {like_sql} "
        )
        params = list(readable_langs) + list(like_params)
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
        raw_rows = cursor.fetchall()

    if not include_total:
        return raw_rows, len(raw_rows)
    rows, total = _split_window_count_rows(raw_rows)
    if total is not None:
        return rows, total
    if not offset or offset <= 0:
        return [], 0
    return [], countReadableFromKeyword(keyword, langCode, langStr, created_version, updated_version)


def countReadableFromKeyword(
    keyword: str,
    langCode: int,
    langStr: str,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("content", keyword, langCode)
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return 0
        lang_placeholders = ",".join(["?"] * len(readable_langs))
        sql = (
            "select count(*) from readable "
            f"where lang in ({lang_placeholders}) and {like_sql}"
        )
        params = readable_langs + like_params
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
        readable_meta_join = _build_readable_meta_join_sql("readable")
        sql = (
            "select fileName, titleTextMapHash, readableId "
            f"from readable {readable_meta_join} where titleTextMapHash=? "
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
        readable_meta_join = _build_readable_meta_join_sql("readable")
        sql = (
            "select min(fileName) as fileName, titleTextMapHash, readableId "
            f"from readable {readable_meta_join} where titleTextMapHash=? "
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
        like_sql, like_params = _build_keyword_like_clause("textMap.content", keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        match_sort_sql, match_sort_params = _build_match_sort_case("textMap.content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('textMap.content', langCode)})"
        sql = (
            f"select quest.questId, textMap.content, {source_type_select} as source_type, {version_select} from quest "
            "cross join textMap on quest.titleTextMapHash=textMap.hash and textMap.lang=? "
            f"where {like_sql} "
        )
        params = [langCode, *like_params]
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
    has_desc = _table_has_column("quest", "descTextMapHash")
    version_select = _version_select_expr("quest", "quest", langCode)
    source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
    has_tdc = _has_talk_dialogue_content_rows()
    has_tdl = _has_talk_dialogue_link_table()

    def _dialogue_join_from_tm(tm_alias: str) -> str:
        if has_tdc:
            return (
                f"cross join talk_dialogue_content d on d.textHash = {tm_alias}.hash "
                "cross join questTalk qt on qt.talkId = d.talkId and qt.coopQuestId = d.coopQuestId "
            )
        if has_tdl:
            return (
                f"cross join dialogue d on d.textHash = {tm_alias}.hash "
                "cross join talk_dialogue_link tdl on tdl.dialogueId = d.dialogueId "
                "cross join questTalk qt on qt.talkId = tdl.talkId and qt.coopQuestId = tdl.coopQuestId "
            )
        return (
            f"cross join dialogue d on d.textHash = {tm_alias}.hash "
            "join questTalk qt on qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ") "
        )

    desc_rows: list[tuple] = []
    with closing(conn.cursor()) as cursor:
        if has_desc:
            desc_like_sql, desc_like_params = _build_keyword_like_clause("descText.content", keyword, langCode)
            desc_filter_sql, desc_filter_params = _quest_text_not_excluded_clause("descText.content")
            sql_desc = (
                "select quest.questId, quest.titleTextMapHash, titleText.content, "
                "descText.content as matched_text, "
                f"{source_type_select} as source_type, {version_select}, 0 as best_rank "
                "from quest "
                "cross join textMap descText on quest.descTextMapHash = descText.hash and descText.lang = ? "
                "left join textMap titleText on quest.titleTextMapHash = titleText.hash and titleText.lang = ? "
                f"where {desc_like_sql} and {desc_filter_sql} "
            )
            params_desc: list = [langCode, langCode, *desc_like_params, *desc_filter_params]
            sql_desc = _append_version_filter_clause(
                sql_desc,
                params_desc,
                "quest",
                created_version,
                updated_version,
                "quest",
                langCode,
            )
            sql_desc = _append_quest_source_type_filter_clause(sql_desc, params_desc, "quest", source_type)
            sql_desc += "order by quest.questId limit 200"
            desc_rows = cursor.execute(sql_desc, params_desc).fetchall()
            if len(desc_rows) >= 200:
                return desc_rows[:200]

        remaining = 200 - len(desc_rows)
        desc_quest_ids = [int(row[0]) for row in desc_rows]
        exclude_desc_sql = ""
        exclude_params: list = []
        if desc_quest_ids:
            placeholders = ",".join("?" for _ in desc_quest_ids)
            exclude_desc_sql = f"and quest.questId not in ({placeholders}) "
            exclude_params.extend(desc_quest_ids)

        def _build_dialogue_query(
            tm_table: str,
            use_fts: bool,
            fts_match: str | None = None,
            fts_subquery: str | None = None,
        ) -> tuple[str, list]:
            tm_like_sql, tm_like_params = _build_keyword_like_clause("tm.content", keyword, langCode)
            dialogue_filter_sql, dialogue_filter_params = _quest_text_not_excluded_clause("matched_tm.content")
            params: list = []

            if use_fts and fts_match:
                subquery_sql = (
                    fts_subquery
                    or f"select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ?"
                )
                matched_tm_cte = (
                    "with matched_tm as materialized ("
                    "select tm.hash, tm.content from textMap tm "
                    f"where tm.id in ({subquery_sql}) "
                    f"and tm.lang = ? and {tm_like_sql}"
                    "), "
                )
                params.extend([fts_match, langCode, *tm_like_params])
            else:
                known_clause = (
                    f"and (tm.flags & {compact_text_cache.FLAG_HAS_KNOWN_SOURCE}) != 0 "
                    if tm_table != "textMap"
                    else ""
                )
                matched_tm_cte = (
                    "with matched_tm as materialized ("
                    f"select tm.hash, tm.content from {tm_table} tm "
                    f"where tm.lang = ? {known_clause}and {tm_like_sql}"
                    "), "
                )
                params.extend([langCode, *tm_like_params])

            sql = (
                matched_tm_cte
                + "matched_dlg as ("
                + "select qt.questId as questId, min(matched_tm.content) as matched_text "
                + "from matched_tm "
                + _dialogue_join_from_tm("matched_tm")
                + f"where {dialogue_filter_sql} "
                + "group by qt.questId"
                + "), top_quests as ("
                + "select quest.questId, matched_dlg.matched_text "
                + "from matched_dlg "
                + "cross join quest on matched_dlg.questId = quest.questId "
                + "where 1=1 "
                + exclude_desc_sql
            )
            params.extend(dialogue_filter_params)
            params.extend(exclude_params)
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
                "order by quest.questId limit ?) "
                f"select quest.questId, quest.titleTextMapHash, titleText.content, "
                "top_quests.matched_text as matched_text, "
                f"{source_type_select} as source_type, {version_select}, 1 as best_rank "
                "from top_quests "
                "cross join quest on top_quests.questId = quest.questId "
                "left join textMap titleText on quest.titleTextMapHash = titleText.hash and titleText.lang = ? "
                "order by quest.questId"
            )
            params.extend([remaining, langCode])
            return sql, params

        sql_like, params_like = _build_dialogue_query("textMap", use_fts=False)
        fts_source = _resolve_textmap_fts_source(keyword, langCode, connection=cursor.connection)
        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts, params_fts = _build_dialogue_query(
                "textMap",
                use_fts=True,
                fts_match=fts_match,
                fts_subquery=fts_subquery,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            compact_table = (
                compact_text_cache.resolve_compact_text_table(cursor.connection, langCode)
                if config.getEnableTextMapFts()
                else None
            )
            if compact_table is not None:
                sql_compact, params_compact = _build_dialogue_query(compact_table, use_fts=False)
                _execute_with_fallback(cursor, sql_compact, params_compact, sql_like, params_like)
            else:
                cursor.execute(sql_like, params_like)
        dlg_rows = cursor.fetchall()
        return desc_rows + dlg_rows


def selectQuestByNpcSpeakerKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    source_type: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("npcName.content", keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        sql = (
            "select distinct quest.questId, titleText.content, "
            f"{source_type_select} as source_type, "
            f"{version_select} "
            "from quest "
            "join questTalk qt on qt.questId = quest.questId "
            + _quest_talk_dialogue_join_sql("qt", "d", "tdl")
            + "join npc on d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = npc.npcId "
            + "join textMap npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            + "left join textMap titleText on quest.titleTextMapHash = titleText.hash and titleText.lang = ? "
            + f"where {like_sql} "
        )
        params: list[object] = [langCode, langCode, *like_params]
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
            + _quest_talk_dialogue_join_sql("qt", "d", "tdl")
            + "left join textMap titleText on quest.titleTextMapHash = titleText.hash and titleText.lang = ? "
            + "where d.talkerType = ? "
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
            "with base_npc as materialized ("
            "select npc.npcId as npcId, npcName.content as npcName, "
            "vd_created.raw_version as created_version, "
            "coalesce(vd_created.version_tag, '') as created_version_tag, "
            "coalesce(vd_created.version_sort_key, 2147483647) as created_version_sort_key "
            "from npc "
            "cross join textMap npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            "left join version_dim vd_created on vd_created.id = npc.created_version_id "
            "where 1=1 "
        )
        params: list[object] = [langCode]
        if keyword_text:
            like_sql, like_params = _build_keyword_like_clause("npcName.content", keyword_text, langCode)
            sql += f"and {like_sql} "
            params.extend(like_params)
        sql += (
            "), visible_npc as materialized ("
            "select base.npcId, base.npcName, base.created_version, "
            "base.created_version_tag, base.created_version_sort_key "
            "from base_npc base "
            "where exists ("
            "select 1 from dialogue d "
            "where d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = base.npcId "
            "and not exists ("
            "select 1 from questTalk qt "
            "where qt.talkId = d.talkId and ("
            + _quest_talk_dialogue_join_condition("qt", "d")
            + ")"
            ")"
            ")"
            "), npc_updated_ranked as ("
            "select visible.npcId as npcId, visible.npcName as npcName, vd.raw_version as dialogue_updated_version, "
            "coalesce(vd.version_tag, '') as dialogue_updated_tag, "
            "coalesce(vd.version_sort_key, -1) as dialogue_updated_sort_key, "
            "row_number() over ("
            "partition by visible.npcId "
            "order by coalesce(vd.version_sort_key, -1) desc, vd.id desc"
            ") as row_num "
            "from visible_npc visible "
            "cross join dialogue d on d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = visible.npcId "
            "cross join textMap tm on tm.hash = d.textHash and tm.lang = ? "
            "cross join version_dim vd on vd.id = tm.updated_version_id "
            "where tm.updated_version_id is not null"
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
        like_sql, like_params = _build_keyword_like_clause("textMap.content", keyword, langCode)
        match_sort_sql, match_sort_params = _build_match_sort_case("textMap.content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('textMap.content', langCode)})"
        sql = (
            "select avatar.avatarId, textMap.content "
            "from avatar join textMap on avatar.nameTextMapHash=textMap.hash "
            f"where textMap.lang=? and {like_sql} "
            f"order by {match_sort_sql}, {normalized_length_sql} "
            "limit 200"
        )
        cursor.execute(sql, (langCode, *like_params, *match_sort_params))
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
        exact = None
        title_like_sql = locked_like_sql = context_like_sql = ""
        title_like_params: list[str] = []
        locked_like_params: list[str] = []
        context_like_params: list[str] = []
        if keyword_text:
            exact = _build_like_patterns(keyword_text, langCode)[0]
            title_like_sql, title_like_params = _build_keyword_like_clause(
                "titleText.content",
                keyword_text,
                langCode,
            )
            locked_like_sql, locked_like_params = _build_keyword_like_clause(
                "lockedText.content",
                keyword_text,
                langCode,
            )
            context_like_sql, context_like_params = _build_keyword_like_clause(
                "contextText.content",
                keyword_text,
                langCode,
            )

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
            sql += f"and ({title_like_sql} or {locked_like_sql} or {context_like_sql}) "
            params.extend(title_like_params)
            params.extend(locked_like_params)
            params.extend(context_like_params)

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
                f"when {title_like_sql} then 1 "
                "when lockedText.content like ? escape '\\' then 2 "
                f"when {locked_like_sql} then 3 "
                "when contextText.content like ? escape '\\' then 4 "
                f"when {context_like_sql} then 5 "
                "else 6 end, "
                "length(coalesce(titleText.content, lockedText.content, contextText.content, '')), "
                "entries.fetterId "
            )
            params.append(exact)
            params.extend(title_like_params)
            params.append(exact)
            params.extend(locked_like_params)
            params.append(exact)
            params.extend(context_like_params)
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
        exact = _build_like_patterns(keyword, langCode)[0]
        title_like_sql, title_like_params = _build_keyword_like_clause("chapterTitle.content", keyword, langCode)
        num_like_sql, num_like_params = _build_keyword_like_clause("chapterNum.content", keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        source_type_select = "quest.source_type" if _table_has_column("quest", "source_type") else "NULL"
        sql = (
            "with matched_chapters as materialized ("
            "select chapter.chapterId as chapterId, "
            "chapterTitle.content as chapter_title, "
            "chapterNum.content as chapter_num "
            "from chapter "
            "left join textMap as chapterTitle on chapter.chapterTitleTextMapHash=chapterTitle.hash and chapterTitle.lang=? "
            "left join textMap as chapterNum on chapter.chapterNumTextMapHash=chapterNum.hash and chapterNum.lang=? "
            f"where ({title_like_sql} or {num_like_sql})"
            ") "
            "select quest.questId, questTitle.content, mc.chapter_title, mc.chapter_num, "
            f"{source_type_select} as source_type, "
            f"{version_select} "
            "from matched_chapters mc "
            "cross join quest on quest.chapterId=mc.chapterId "
            "left join textMap as questTitle on quest.titleTextMapHash=questTitle.hash and questTitle.lang=? "
            "where 1=1 "
        )
        params = [
            langCode,
            langCode,
            *title_like_params,
            *num_like_params,
            langCode,
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
            "order by case when (mc.chapter_title like ? escape '\\' or mc.chapter_num like ? escape '\\') then 0 else 1 end, "
            "length(coalesce(mc.chapter_title, mc.chapter_num)) "
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


def getQuestChapterNamesBatch(quest_ids: list[int], lang_code: int) -> dict[int, str | None]:
    normalized_ids = _normalize_batch_int_ids(quest_ids)
    if not normalized_ids:
        return {}

    result: dict[int, str | None] = {qid: None for qid in normalized_ids}
    chunk_size = 500
    with closing(conn.cursor()) as cursor:
        for start in range(0, len(normalized_ids), chunk_size):
            chunk = normalized_ids[start:start + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            sql = (
                "select q.questId, title_tm.content, num_tm.content "
                "from quest q "
                "join chapter c on c.chapterId = q.chapterId "
                "join textMap title_tm on title_tm.hash = c.chapterTitleTextMapHash and title_tm.lang = ? "
                "left join textMap num_tm on num_tm.hash = c.chapterNumTextMapHash and num_tm.lang = ? "
                f"where q.questId in ({placeholders})"
            )
            for quest_id, chapter_title, chapter_num in cursor.execute(
                sql, [lang_code, lang_code, *chunk]
            ).fetchall():
                if quest_id is None:
                    continue
                qid = int(quest_id)
                if chapter_num is not None:
                    result[qid] = "{} · {}".format(chapter_num, chapter_title)
                else:
                    result[qid] = chapter_title
    return result


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
        use_text_filter = _table_exists("textMap")
        source_lang = config.getSourceLanguage() if use_text_filter else None
        dialogue_filter_sql, dialogue_filter_params = (
            _quest_text_not_excluded_clause("dialogueText.content")
            if use_text_filter
            else ("1=1", tuple())
        )
        if _has_talk_dialogue_link_table():
            sql = (
                "select count(*) "
                "from (select distinct talkId, coalesce(coopQuestId, 0) as coopQuestId from questTalk where questId=?) qt "
                + _quest_talk_dialogue_join_sql("qt", "d", "tdl")
                + (
                    "left join textMap dialogueText on d.textHash = dialogueText.hash and dialogueText.lang = ? "
                    if use_text_filter
                    else ""
                )
                + f"where {dialogue_filter_sql} "
            )
        else:
            sql = (
                "select count(*) from dialogue d "
                "join (select distinct talkId, coalesce(coopQuestId, 0) as coopQuestId from questTalk where questId=?) qt "
                "on qt.talkId = d.talkId "
                "and ("
                + _quest_talk_dialogue_join_condition("qt", "d")
                + ") "
                + (
                    "left join textMap dialogueText on d.textHash = dialogueText.hash and dialogueText.lang = ? "
                    if use_text_filter
                    else ""
                )
                + f"where {dialogue_filter_sql} "
            )
        params = [questId]
        if use_text_filter:
            params.append(source_lang)
        params.extend(dialogue_filter_params)
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return row[0] if row else 0


def selectQuestDialoguesPaged(
    questId: int,
    limit: int | None = None,
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        use_text_filter = _table_exists("textMap")
        source_lang = config.getSourceLanguage() if use_text_filter else None
        dialogue_filter_sql, dialogue_filter_params = (
            _quest_text_not_excluded_clause("dialogueText.content")
            if use_text_filter
            else ("1=1", tuple())
        )
        if _has_talk_dialogue_link_table():
            sql = (
                "select d.textHash, d.talkerType, d.talkerId, d.dialogueId, qt.talkId "
                "from (select distinct talkId, coalesce(coopQuestId, 0) as coopQuestId from questTalk where questId=?) qt "
                + _quest_talk_dialogue_join_sql("qt", "d", "tdl")
                + (
                    "left join textMap dialogueText on d.textHash = dialogueText.hash and dialogueText.lang = ? "
                    if use_text_filter
                    else ""
                )
                + f"where {dialogue_filter_sql} "
                + "order by qt.coopQuestId, qt.talkId, d.dialogueId"
            )
        else:
            sql = (
                "select d.textHash, d.talkerType, d.talkerId, d.dialogueId, d.talkId "
                "from dialogue d "
                "join (select distinct talkId, coalesce(coopQuestId, 0) as coopQuestId from questTalk where questId=?) qt "
                "on qt.talkId = d.talkId "
                "and ("
                + _quest_talk_dialogue_join_condition("qt", "d")
                + ") "
                + (
                    "left join textMap dialogueText on d.textHash = dialogueText.hash and dialogueText.lang = ? "
                    if use_text_filter
                    else ""
                )
                + f"where {dialogue_filter_sql} "
                "order by qt.coopQuestId, d.talkId, d.dialogueId"
            )
        params = [questId]
        if use_text_filter:
            params.append(source_lang)
        params.extend(dialogue_filter_params)
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
    limit: int | None = 200,
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("textMap.content", keyword, langCode)
        match_sort_sql, match_sort_params = _build_match_sort_case("textMap.content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('textMap.content', langCode)})"
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return []
        readable_lang_placeholders = ",".join(["?"] * len(readable_langs))
        version_select = _version_select_expr("readable", "readable")
        version_group = "readable.created_version_id, readable.updated_version_id"
        readable_meta_join = _build_readable_meta_join_sql("readable")
        sql = (f"select readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
               f"{version_select} "
               f"from readable {readable_meta_join}"
               "cross join textMap on readable.titleTextMapHash=textMap.hash and textMap.lang=? "
               f"where readable.lang in ({readable_lang_placeholders}) "
               f"and {like_sql} ")
        params = [langCode] + readable_langs + list(like_params)
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
                f"order by {match_sort_sql}, {normalized_length_sql} ")
        params.extend(match_sort_params)
        if limit is not None:
            sql += "limit ? "
            params.append(limit)
        if offset is not None and offset > 0:
            if limit is None:
                sql += "limit -1 "
            sql += "offset ? "
            params.append(offset)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableByFileNameContains(
    keyword: str,
    langCode: int,
    langStr: str,
    created_version: str | None = None,
    updated_version: str | None = None,
    category: str | None = None,
    limit: int | None = 200,
    offset: int | None = None,
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
        readable_meta_join = _build_readable_meta_join_sql("readable")
        sql = (
            f"select readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
            f"{version_select} "
            f"from readable {readable_meta_join}"
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
                f"order by {match_sort_sql}, {normalized_length_sql} ")
        params.extend(match_sort_params)
        if limit is not None:
            sql += "limit ? "
            params.append(limit)
        if offset is not None and offset > 0:
            if limit is None:
                sql += "limit -1 "
            sql += "offset ? "
            params.append(offset)
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
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        readable_langs = _expand_readable_langs([langStr])
        if not readable_langs:
            return []
        readable_lang_placeholders = ",".join(["?"] * len(readable_langs))
        version_select = _version_select_expr("readable", "readable")
        readable_meta_join = _build_readable_meta_join_sql("readable")
        sql = (
            f"select readable.fileName, readable.readableId, readable.titleTextMapHash, "
            f"textMap.content as title, {version_select} "
            f"from readable {readable_meta_join}"
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
        if offset is not None and offset > 0:
            if limit is None:
                sql += " limit -1"
            sql += " offset ?"
            params.append(offset)
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
    rows, _total = selectSubtitleFromKeywordWithCount(
        keyword,
        langCode,
        limit=limit,
        offset=offset,
        created_version=created_version,
        updated_version=updated_version,
        include_total=False,
    )
    return rows


def selectSubtitleFromKeywordWithCount(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    offset: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
    include_total: bool = True,
) -> tuple[list[tuple], int]:
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("content", keyword, langCode)
        version_select = _version_select_expr("subtitle", "subtitle")
        match_sort_sql, match_sort_params = _build_match_sort_case("content", keyword, langCode)
        normalized_length_sql = f"length({_build_normalized_match_expr('content', langCode)})"
        total_col = ", count(*) over() as _total_count" if include_total else ""
        sql = (
            f"select fileName, content, startTime, endTime, subtitleId, {version_select}{total_col} from subtitle "
            f"where lang=? and {like_sql} "
        )
        params = [langCode, *like_params]
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
        raw_rows = cursor.fetchall()

    if not include_total:
        return raw_rows, len(raw_rows)
    rows, total = _split_window_count_rows(raw_rows)
    if total is not None:
        return rows, total
    if not offset or offset <= 0:
        return [], 0
    return [], countSubtitleFromKeyword(keyword, langCode, created_version, updated_version)


def countSubtitleFromKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("content", keyword, langCode)
        sql = (
            "select count(*) from subtitle "
            f"where lang=? and {like_sql}"
        )
        params = [langCode, *like_params]
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


def _build_matched_npc_cte(speaker_keyword: str, lang_code: int) -> tuple[str, list]:
    speaker_like_sql, speaker_like_params = _build_keyword_like_clause("npcName.content", speaker_keyword, lang_code)
    cte_sql = (
        "with matched_npc as materialized ("
        "select npc.npcId as npcId, npcName.content as npc_name "
        "from npc "
        "cross join textMap as npcName on npc.textHash = npcName.hash and npcName.lang = ? "
        f"where {speaker_like_sql}"
        ") "
    )
    return cte_sql, [lang_code, *speaker_like_params]


def _build_matched_avatar_cte(speaker_keyword: str, lang_code: int) -> tuple[str, list]:
    speaker_like_sql, speaker_like_params = _build_keyword_like_clause("avatarName.content", speaker_keyword, lang_code)
    cte_sql = (
        "with matched_avatar as materialized ("
        "select avatar.avatarId as avatarId, avatarName.content as avatar_name "
        "from avatar "
        "cross join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash and avatarName.lang = ? "
        f"where {speaker_like_sql}"
        ") "
    )
    return cte_sql, [lang_code, *speaker_like_params]


def _build_dialogue_by_talker_and_keyword_sql(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    created_version: str | None,
    updated_version: str | None,
    *,
    use_fts: bool,
    fts_match: str | None,
    fts_subquery: str | None = None,
    limit: int | None = None,
    include_total: bool = False,
    is_count: bool = False,
) -> tuple[str, list]:
    speaker_like_sql, speaker_like_params = _build_keyword_like_clause("npcName.content", speaker_keyword, langCode)
    keyword_like_sql, keyword_like_params = _build_keyword_like_clause("tm.content", keyword, langCode)
    params: list = [langCode, *speaker_like_params]
    cte_sql = (
        "with matched_npc as materialized ("
        "select npc.npcId as npcId, npcName.content as npc_name "
        "from npc "
        "cross join textMap as npcName on npc.textHash = npcName.hash and npcName.lang = ? "
        f"where {speaker_like_sql}"
        ")"
    )
    if use_fts and fts_match:
        subquery_sql = (
            fts_subquery
            or f"select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ?"
        )
        tm_sql = (
            ", matched_text as materialized ("
            "select tm.hash as hash, tm.content as content "
            "from textMap tm "
            f"where tm.id in ({subquery_sql}) "
            "and tm.lang = ? "
            f"and {keyword_like_sql} "
        )
        params.extend([fts_match, langCode, *keyword_like_params])
        tm_sql = _append_version_filter_clause(
            tm_sql, params, "tm", created_version, updated_version, "textMap"
        )
        tm_sql += ") "
        from_sql = (
            "from matched_text tm "
            "cross join dialogue d on d.textHash = tm.hash "
            "join matched_npc mn on mn.npcId = d.talkerId "
            "where d.talkerType = 'TALK_ROLE_NPC' "
            "and d.talkerId in (select npcId from matched_npc) "
        )
    else:
        tm_sql = " "
        from_sql = (
            "from matched_npc mn "
            "cross join dialogue d on d.talkerType = 'TALK_ROLE_NPC' and d.talkerId = mn.npcId "
            "cross join textMap as tm on d.textHash = tm.hash and tm.lang = ? "
            f"where {keyword_like_sql} "
        )
        params.extend([langCode, *keyword_like_params])
        from_sql = _append_version_filter_clause(
            from_sql, params, "tm", created_version, updated_version, "textMap"
        )

    if is_count:
        return f"{cte_sql}{tm_sql}select count(*) {from_sql}", params

    dialogue_sort_sql, dialogue_sort_params = _build_match_sort_case("tm.content", keyword, langCode)
    speaker_sort_sql, speaker_sort_params = _build_match_sort_case("mn.npc_name", speaker_keyword, langCode)
    dialogue_length_sql = f"length({_build_normalized_match_expr('tm.content', langCode)})"
    total_col = ", count(*) over() as _total_count" if include_total else ""
    sql = (
        f"{cte_sql}{tm_sql}"
        f"select d.textHash, d.talkerType, d.talkerId, d.dialogueId{total_col} "
        f"{from_sql}"
        f"order by {dialogue_sort_sql}, {speaker_sort_sql}, "
        f"{dialogue_length_sql}, d.dialogueId "
    )
    params.extend(dialogue_sort_params)
    params.extend(speaker_sort_params)
    if limit is not None:
        sql += " limit ?"
        params.append(limit)
    return sql, params


def selectDialogueByTalkerKeyword(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        cte_sql, params = _build_matched_npc_cte(keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("mn.npc_name", keyword, langCode)
        speaker_length_sql = f"length({_build_normalized_match_expr('mn.npc_name', langCode)})"
        sql = (
            f"{cte_sql}"
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from matched_npc mn "
            "cross join dialogue on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = mn.npcId "
            "cross join textMap as dialogueText on dialogue.textHash = dialogueText.hash and dialogueText.lang=? "
            "where 1=1 "
        )
        params.append(langCode)
        sql = _append_version_filter_clause(
            sql,
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
        cte_sql, params = _build_matched_npc_cte(keyword, langCode)
        sql = (
            f"{cte_sql}"
            "select count(*) "
            "from matched_npc mn "
            "cross join dialogue on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = mn.npcId "
            "cross join textMap as dialogueText on dialogue.textHash = dialogueText.hash and dialogueText.lang=? "
            "where 1=1 "
        )
        params.append(langCode)
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
    rows, _total = selectDialogueByTalkerAndKeywordWithCount(
        speaker_keyword,
        keyword,
        langCode,
        limit=limit,
        created_version=created_version,
        updated_version=updated_version,
        include_total=False,
    )
    return rows


def selectDialogueByTalkerAndKeywordWithCount(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
    include_total: bool = True,
) -> tuple[list[tuple], int]:
    sql_like, params_like = _build_dialogue_by_talker_and_keyword_sql(
        speaker_keyword,
        keyword,
        langCode,
        created_version,
        updated_version,
        use_fts=False,
        fts_match=None,
        limit=limit,
        include_total=include_total,
    )
    with closing(conn.cursor()) as cursor:
        fts_source = _resolve_textmap_fts_source(keyword, langCode, connection=cursor.connection)
        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts, params_fts = _build_dialogue_by_talker_and_keyword_sql(
                speaker_keyword,
                keyword,
                langCode,
                created_version,
                updated_version,
                use_fts=True,
                fts_match=fts_match,
                fts_subquery=fts_subquery,
                limit=limit,
                include_total=include_total,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        raw_rows = cursor.fetchall()

    if not include_total:
        return raw_rows, len(raw_rows)
    rows, total = _split_window_count_rows(raw_rows)
    return rows, (total if total is not None else 0)


def selectDialogueByTalkerTypeAndKeyword(
    talkerType: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        like_sql, like_params = _build_keyword_like_clause("dialogueText.content", keyword, langCode)
        dialogue_sort_sql, dialogue_sort_params = _build_match_sort_case("dialogueText.content", keyword, langCode)
        dialogue_length_sql = f"length({_build_normalized_match_expr('dialogueText.content', langCode)})"
        sql_like = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "where dialogue.talkerType = ? "
            f"and {like_sql} "
        )
        params_like = [langCode, talkerType, *like_params]
        sql_like = _append_version_filter_clause(
            sql_like + " ",
            params_like,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        sql_like += (
            f"order by {dialogue_sort_sql}, "
            f"{dialogue_length_sql}, dialogue.dialogueId "
        )
        params_like.extend(dialogue_sort_params)
        if limit is not None:
            sql_like += " limit ?"
            params_like.append(limit)

        fts_source = _resolve_textmap_fts_source(keyword, langCode, connection=cursor.connection)
        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts = (
                "with matched_text as materialized ("
                "select dialogueText.hash as hash, dialogueText.content as content "
                "from textMap as dialogueText "
                f"where dialogueText.id in ({fts_subquery}) "
                "and dialogueText.lang = ? "
                f"and {like_sql} "
            )
            params_fts = [fts_match, langCode, *like_params]
            sql_fts = _append_version_filter_clause(
                sql_fts,
                params_fts,
                "dialogueText",
                created_version,
                updated_version,
                "textMap",
            )
            sql_fts += (
                ") "
                "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
                "from matched_text as dialogueText "
                "cross join dialogue on dialogue.textHash = dialogueText.hash "
                "where dialogue.talkerType = ? "
                f"order by {dialogue_sort_sql}, {dialogue_length_sql}, dialogue.dialogueId "
            )
            params_fts.extend([talkerType, *dialogue_sort_params])
            if limit is not None:
                sql_fts += " limit ?"
                params_fts.append(limit)
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        return cursor.fetchall()


def selectFetterBySpeakerKeyword(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        cte_sql, params = _build_matched_avatar_cte(keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("ma.avatar_name", keyword, langCode)
        speaker_length_sql = f"length({_build_normalized_match_expr('ma.avatar_name', langCode)})"
        sql = (
            f"{cte_sql}"
            "select fetters.voiceFileTextTextMapHash, fetters.avatarId "
            "from matched_avatar ma "
            "cross join fetters on fetters.avatarId = ma.avatarId "
            "cross join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "where 1=1 "
        )
        params.append(langCode)
        sql = _append_version_filter_clause(
            sql,
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
        cte_sql, params = _build_matched_avatar_cte(speaker_keyword, langCode)
        keyword_like_sql, keyword_like_params = _build_keyword_like_clause("voiceText.content", keyword, langCode)
        voice_sort_sql, voice_sort_params = _build_match_sort_case("voiceText.content", keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("ma.avatar_name", speaker_keyword, langCode)
        voice_length_sql = f"length({_build_normalized_match_expr('voiceText.content', langCode)})"
        sql = (
            f"{cte_sql}"
            "select fetters.voiceFileTextTextMapHash, fetters.avatarId "
            "from matched_avatar ma "
            "cross join fetters on fetters.avatarId = ma.avatarId "
            "cross join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            f"where {keyword_like_sql} "
        )
        params.extend([langCode, *keyword_like_params])
        sql = _append_version_filter_clause(
            sql,
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
        cte_sql, params = _build_matched_avatar_cte(keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("ma.avatar_name", keyword, langCode)
        speaker_length_sql = f"length({_build_normalized_match_expr('ma.avatar_name', langCode)})"
        sql = (
            f"{cte_sql}"
            "select entries.contextHash, entries.avatarId "
            "from matched_avatar ma "
            f"cross join ({_avatar_story_entries_subquery()}) as entries on entries.avatarId = ma.avatarId "
            "cross join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            "where 1=1 "
        )
        params.append(langCode)
        sql = _append_version_filter_clause(
            sql,
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
        cte_sql, params = _build_matched_avatar_cte(speaker_keyword, langCode)
        keyword_like_sql, keyword_like_params = _build_keyword_like_clause("storyText.content", keyword, langCode)
        story_sort_sql, story_sort_params = _build_match_sort_case("storyText.content", keyword, langCode)
        speaker_sort_sql, speaker_sort_params = _build_match_sort_case("ma.avatar_name", speaker_keyword, langCode)
        story_length_sql = f"length({_build_normalized_match_expr('storyText.content', langCode)})"
        sql = (
            f"{cte_sql}"
            "select entries.contextHash, entries.avatarId "
            "from matched_avatar ma "
            f"cross join ({_avatar_story_entries_subquery()}) as entries on entries.avatarId = ma.avatarId "
            "cross join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            f"where {keyword_like_sql} "
        )
        params.extend([langCode, *keyword_like_params])
        sql = _append_version_filter_clause(
            sql,
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
    sql_like, params_like = _build_dialogue_by_talker_and_keyword_sql(
        speaker_keyword,
        keyword,
        langCode,
        created_version,
        updated_version,
        use_fts=False,
        fts_match=None,
        is_count=True,
    )
    with closing(conn.cursor()) as cursor:
        fts_source = _resolve_textmap_fts_source(keyword, langCode, connection=cursor.connection)
        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts, params_fts = _build_dialogue_by_talker_and_keyword_sql(
                speaker_keyword,
                keyword,
                langCode,
                created_version,
                updated_version,
                use_fts=True,
                fts_match=fts_match,
                fts_subquery=fts_subquery,
                is_count=True,
            )
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
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
        like_sql, like_params = _build_keyword_like_clause("dialogueText.content", keyword, langCode)
        sql_like = (
            "select count(*) "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "where dialogue.talkerType = ? "
            f"and {like_sql}"
        )
        params_like = [langCode, talkerType, *like_params]
        sql_like = _append_version_filter_clause(
            sql_like + " ",
            params_like,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        fts_source = _resolve_textmap_fts_source(keyword, langCode, connection=cursor.connection)
        if fts_source is not None:
            fts_subquery, fts_match = fts_source
            sql_fts = (
                "with matched_text as materialized ("
                "select dialogueText.hash as hash "
                "from textMap as dialogueText "
                f"where dialogueText.id in ({fts_subquery}) "
                "and dialogueText.lang = ? "
                f"and {like_sql} "
            )
            params_fts = [fts_match, langCode, *like_params]
            sql_fts = _append_version_filter_clause(
                sql_fts,
                params_fts,
                "dialogueText",
                created_version,
                updated_version,
                "textMap",
            )
            sql_fts += (
                ") "
                "select count(*) from matched_text mt "
                "cross join dialogue on dialogue.textHash = mt.hash "
                "where dialogue.talkerType = ?"
            )
            params_fts.append(talkerType)
            _execute_with_fallback(cursor, sql_fts, params_fts, sql_like, params_like)
        else:
            cursor.execute(sql_like, params_like)
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countFetterBySpeakerKeyword(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
) -> int:
    with closing(conn.cursor()) as cursor:
        cte_sql, params = _build_matched_avatar_cte(keyword, langCode)
        sql = (
            f"{cte_sql}"
            "select count(*) "
            "from matched_avatar ma "
            "cross join fetters on fetters.avatarId = ma.avatarId "
            "cross join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "where 1=1 "
        )
        params.append(langCode)
        sql = _append_version_filter_clause(
            sql,
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
        cte_sql, params = _build_matched_avatar_cte(speaker_keyword, langCode)
        keyword_like_sql, keyword_like_params = _build_keyword_like_clause("voiceText.content", keyword, langCode)
        sql = (
            f"{cte_sql}"
            "select count(*) "
            "from matched_avatar ma "
            "cross join fetters on fetters.avatarId = ma.avatarId "
            "cross join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            f"where {keyword_like_sql} "
        )
        params.extend([langCode, *keyword_like_params])
        sql = _append_version_filter_clause(
            sql,
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
        cte_sql, params = _build_matched_avatar_cte(keyword, langCode)
        sql = (
            f"{cte_sql}"
            "select count(*) "
            "from matched_avatar ma "
            f"cross join ({_avatar_story_entries_subquery()}) as entries on entries.avatarId = ma.avatarId "
            "cross join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            "where 1=1 "
        )
        params.append(langCode)
        sql = _append_version_filter_clause(
            sql,
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
        cte_sql, params = _build_matched_avatar_cte(speaker_keyword, langCode)
        keyword_like_sql, keyword_like_params = _build_keyword_like_clause("storyText.content", keyword, langCode)
        sql = (
            f"{cte_sql}"
            "select count(*) "
            "from matched_avatar ma "
            f"cross join ({_avatar_story_entries_subquery()}) as entries on entries.avatarId = ma.avatarId "
            "cross join textMap as storyText on entries.contextHash = storyText.hash "
            "and storyText.lang = ? "
            f"where {keyword_like_sql} "
        )
        params.extend([langCode, *keyword_like_params])
        sql = _append_version_filter_clause(
            sql,
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

import sqlite3
from contextlib import closing
import re
import os
from pathlib import Path

import config
import fts_tokenizer


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


def _configure_connection(connection: sqlite3.Connection) -> None:
    """Register FTS helpers and apply default runtime PRAGMAs."""
    tokenizer, ext_path, ext_entry = _resolve_fts_settings()
    _register_fts_content_function(connection, tokenizer)
    _try_load_fts_extension(connection, ext_path, ext_entry)
    _apply_connection_pragmas(connection)


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
_VERSION_SOURCE_TABLES = ("textMap", "quest", "subtitle", "readable")
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
        if table_name != 'quest' and _has_version_id_columns(table_name) and _has_version_dim():
            # For non-quest tables, also include updated_version_id
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
    try:
        cursor.execute(sql_main, params_main)
    except sqlite3.OperationalError:
        if sql_fallback is None:
            raise
        cursor.execute(sql_fallback, params_fallback if params_fallback is not None else [])


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


def _build_textmap_query(
    use_fts: bool,
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
            f"join {_TEXTMAP_FTS_TABLE} fts on fts.rowid = tm.id "
            f"where fts match ? and fts.lang=? "
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

    # 添加排序和分页
    if hash_value is not None:
        sql += (
            "order by "
            "case when tm.hash = ? then 0 else 1 end, "
            "case when tm.content like ? escape '\\' then 0 else 1 end, "
            "case when exists (select 1 from dialogue d join voice v on v.dialogueId = d.dialogueId where d.textHash = tm.hash limit 1) or exists (select 1 from fetters f join voice v on v.dialogueId = f.voiceFile and (v.avatarId = f.avatarId or v.avatarId = 0) where f.voiceFileTextTextMapHash = tm.hash limit 1) then 0 else 1 end, "
            "case when exists (select 1 from dialogue d join questTalk qt on d.talkId = qt.talkId join quest q on qt.questId = q.questId where d.textHash = tm.hash limit 1) then 0 else 1 end "
        )
        params.extend([hash_value, exact])
    else:
        sql += (
            "order by "
            "case when tm.content like ? escape '\\' then 0 else 1 end, "
            "case when exists (select 1 from dialogue d join voice v on v.dialogueId = d.dialogueId where d.textHash = tm.hash limit 1) or exists (select 1 from fetters f join voice v on v.dialogueId = f.voiceFile and (v.avatarId = f.avatarId or v.avatarId = 0) where f.voiceFileTextTextMapHash = tm.hash limit 1) then 0 else 1 end, "
            "case when exists (select 1 from dialogue d join questTalk qt on d.talkId = qt.talkId join quest q on qt.questId = q.questId where d.textHash = tm.hash limit 1) then 0 else 1 end "
        )
        params.append(exact)

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
            f"join {_TEXTMAP_FTS_TABLE} fts on fts.rowid = tm.id "
            f"where fts match ? and fts.lang=? "
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


def _voice_exists_expr(text_hash_field: str) -> str:
    return (
        "exists ("
        "select 1 from dialogue d "
        "join voice v on v.dialogueId = d.dialogueId "
        f"where d.textHash = {text_hash_field} limit 1"
        ") or exists ("
        "select 1 from fetters f "
        "join voice v on v.dialogueId = f.voiceFile "
        "and (v.avatarId = f.avatarId or v.avatarId = 0) "
        f"where f.voiceFileTextTextMapHash = {text_hash_field} limit 1"
        ")"
    )


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

        sql_like = (
            "select tm.hash, tm.content from textMap tm "
            "where tm.lang=? and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
            "order by case when tm.content like ? escape '\\' then 0 else 1 end, length(tm.content)"
        )
        params_like = [langCode, exact, fuzzy, exact]
        if limit is not None:
            sql_like += " limit ?"
            params_like.append(limit)

        if _is_textmap_fts_lang_enabled(langCode) and fts_match is not None:
            sql_fts = (
                f"select tm.hash, tm.content from textMap tm "
                f"where tm.id in (select rowid from {_TEXTMAP_FTS_TABLE} where {_TEXTMAP_FTS_TABLE} match ? and lang=?) "
                "and tm.lang=? "
                "and (tm.content like ? escape '\\' or tm.content like ? escape '\\') "
                "order by case when tm.content like ? escape '\\' then 0 else 1 end, length(tm.content)"
            )
            params_fts = [fts_match, langCode, langCode, exact, fuzzy, exact]
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
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        fts_match = _build_textmap_fts_match(keyWord, langCode)
        hash_value = hash_value if hash_value is not None else -1
        voice_expr = _voice_exists_expr("tm.hash")

        # 构建LIKE查询
        sql_like, params_like = _build_textmap_query(
            use_fts=False,
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


def hasVoiceForTextHashDb(textHash: int) -> bool:
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
    with closing(conn.cursor()) as cursor:
        if langs is not None and len(langs) > 0:
            langStr = ','.join([str(i) for i in langs])
            sql1 = f"select content, lang from textMap where hash=? and lang in ({langStr})"
        else:
            sql1 = "select content, lang from textMap where hash=?"
        cursor.execute(sql1, (textHash,))
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


def selectVoicePathFromTextHash(textHash: int):
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
        for table_name in ("quest", "readable", "subtitle", "textMap"):
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
            if table_name != 'quest' and _has_version_id_columns(table_name) and _has_version_dim():
                # For non-quest tables, also include updated_version_id
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

        sql2 = 'select content from avatar, textMap where avatarId=? and avatar.nameTextMapHash=textMap.hash and lang=?'
        cursor.execute(sql2, (avatarId, langCode))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        avatarName = ans2[0][0]

        return "{} · {}".format(avatarName, voiceTitle)


def getCharterName(avatarId: int, langCode: int = 1):
    """
    获取角色名称
    """
    cache_key = f"{avatarId}:{langCode}"
    if cache_key in _CACHE["names"]["characters"]:
        return _CACHE["names"]["characters"][cache_key]
    with closing(conn.cursor()) as cursor:
        sql2 = 'select content from avatar, textMap where avatarId=? and avatar.nameTextMapHash=textMap.hash and lang=?'
        cursor.execute(sql2, (avatarId, langCode,))
        ans2 = cursor.fetchall()
        result = ans2[0][0] if ans2 else None
        _CACHE["names"]["characters"][cache_key] = result
        return result


def getWanderName(langCode: int = 1):
    """
    获取旅行者名称
    """
    if langCode in _CACHE["names"]["wander"]:
        return _CACHE["names"]["wander"][langCode]
    _CACHE["names"]["wander"][langCode] = getCharterName(10000075, langCode)
    return _CACHE["names"]["wander"][langCode]


def getTravellerName(langCode: int = 1):
    """
    获取空旅行者名称
    """
    if langCode in _CACHE["names"]["traveller"]:
        return _CACHE["names"]["traveller"][langCode]
    _CACHE["names"]["traveller"][langCode] = getCharterName(10000005, langCode)
    return _CACHE["names"]["traveller"][langCode]


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
            sqlGetNpcName = 'select content from npc, textMap indexed by textMap_hash_index where npcId = ? and textHash = hash and lang = ?'
            cursor.execute(sqlGetNpcName, (talkerId, langCode))
            ansNpcName = cursor.fetchall()
            if len(ansNpcName) > 0:
                talkerName = ansNpcName[0][0]
        elif talkerType == "TALK_ROLE_PLAYER":
            talkerName = "主角"
        elif talkerType == "TALK_ROLE_MATE_AVATAR":
            talkerName = "同伴"

        if talkerName == '#{REALNAME[ID(1)|HOSTONLY(true)]}':
            talkerName = getWanderName(langCode)
        return talkerName


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
                'where talkId=? and quest.questId=questTalk.questId')
        cursor.execute(sql2, (talkId,))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        return ans2[0][0]


def getQuestName(questId, langCode):
    with closing(conn.cursor()) as cursor:
        sql2 = ('select content from quest, textMap '
                'where quest.questId=? and titleTextMapHash=hash and lang=?')
        cursor.execute(sql2, (questId, langCode))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return "对话文本"

        questTitle = ans2[0][0]

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

        chapterTitleText = ans4[0][0]

        cursor.execute(sql4, (chapterNumTextMapHash, langCode))
        ans5 = cursor.fetchall()

        if len(ans5) > 0:
            chapterNumText = ans5[0][0]
            questCompleteName = '{} · {} · {}'.format(chapterNumText, chapterTitleText, questTitle)
        else:
            questCompleteName = '{} · {}'.format(chapterTitleText, questTitle)

        return questCompleteName


def getTalkQuestName(talkId: int, langCode: int = 1) -> str:
    questId = getTalkQuestId(talkId)
    if questId is None:
        return "对话文本"
    return getQuestName(questId, langCode)


def getCoopTalkQuestName(coopQuestId, langCode):
    return getQuestName(coopQuestId // 100, langCode)


def getSourceFromDialogue(textHash: int, langCode: int = 1):
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


def getVoicePath(voice_hash: str, lang: int):
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
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
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
        params.append(exact)
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "readable",
            created_version,
            updated_version,
            "readable",
        )
        sql += "order by case when content like ? escape '\\' then 0 else 1 end, length(content) "
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
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        sql = (
            f"select quest.questId, textMap.content, {version_select} from quest "
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
        sql += (
            "order by case when textMap.content like ? escape '\\' then 0 else 1 end, length(textMap.content) "
            "limit 200"
        )
        params.append(exact)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByIdContains(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        escaped = _escape_like(keyword)
        pattern = f"%{escaped}%"
        version_select = _version_select_expr("quest", "quest", langCode)
        sql = (
            f"select quest.questId, textMap.content, {version_select} "
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
        sql += "order by length(cast(quest.questId as text)) limit 200"
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectAvatarByNameKeyword(keyword: str, langCode: int):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select avatar.avatarId, textMap.content "
            "from avatar join textMap on avatar.nameTextMapHash=textMap.hash "
            "where textMap.lang=? and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') "
            "order by case when textMap.content like ? escape '\\' then 0 else 1 end, length(textMap.content) "
            "limit 200"
        )
        cursor.execute(sql, (langCode, exact, fuzzy, exact))
        return cursor.fetchall()


def selectAvatarVoiceItems(avatarId: int, limit: int = 400):
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


def selectAvatarVoiceItemsByFilters(
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
        params = []
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
            "from ("
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
            ") as entries "
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
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        version_select = _version_select_expr("quest", "quest", langCode)
        sql = (
            "select quest.questId, questTitle.content, chapterTitle.content, chapterNum.content, "
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


def getQuestVersionInfo(questId: int):
    if not _has_version_id_columns("quest"):
        return None, None
    with closing(conn.cursor()) as cursor:
        version_select = _version_select_expr("q", "quest")
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
            "join (select distinct talkId from questTalk where questId=?) qt "
            "on qt.talkId = d.talkId "
            "where d.coopQuestId is null"
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
            "join (select distinct talkId from questTalk where questId=?) qt "
            "on qt.talkId = d.talkId "
            "where d.coopQuestId is null "
            "order by d.talkId, d.dialogueId"
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
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
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
        sql += ("group by readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
                f"{version_group} "
                "order by case when textMap.content like ? escape '\\' then 0 else 1 end, length(textMap.content) "
                "limit 200")
        params.append(exact)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableByFileNameContains(
    keyword: str,
    langCode: int,
    langStr: str,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        escaped = _escape_like(keyword)
        pattern = f"%{escaped}%"
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
        sql += ("group by readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content, "
                f"{version_group} "
                "order by length(readable.fileName) "
                "limit 200")
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectQuestByVersion(
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    limit: int | None = 2000,
):
    with closing(conn.cursor()) as cursor:
        version_select = _version_select_expr("quest", "quest", langCode)
        sql = (
            f"select quest.questId, textMap.content, {version_select} "
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
        sql = (
            f"select fileName, content, startTime, endTime, subtitleId, {version_select} from subtitle "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\') "
        )
        params = [langCode, exact, fuzzy, exact]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "subtitle",
            created_version,
            updated_version,
            "subtitle",
        )
        sql += "order by case when content like ? escape '\\' then 0 else 1 end, length(content) "
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
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap as npcName on npc.textHash = npcName.hash and npcName.lang=? "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash and dialogueText.lang=? "
            "where (npcName.content like ? escape '\\' or npcName.content like ? escape '\\') "
        )
        params = [langCode, langCode, exact, fuzzy, exact]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += "order by case when npcName.content like ? escape '\\' then 0 else 1 end, length(npcName.content), dialogue.dialogueId "
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
            keyword_exact,
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
            "order by case when dialogueText.content like ? escape '\\' then 0 else 1 end, "
            "length(dialogueText.content), dialogue.dialogueId "
        )
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
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "where dialogue.talkerType = ? "
            "and (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
        )
        params = [langCode, talkerType, exact, fuzzy, exact]
        sql = _append_version_filter_clause(
            sql + " ",
            params,
            "dialogueText",
            created_version,
            updated_version,
            "textMap",
        )
        sql += (
            "order by case when dialogueText.content like ? escape '\\' then 0 else 1 end, "
            "length(dialogueText.content), dialogue.dialogueId "
        )
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
            keyword_exact,
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
            "order by case when voiceText.content like ? escape '\\' then 0 else 1 end, "
            "length(voiceText.content), fetters.fetterId "
        )
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

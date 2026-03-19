import subprocess
import os
import re
import sys
from pathlib import Path

from DBConfig import conn, DATA_PATH

SPECIAL_VERSION_LABEL_MAP = {
    "BinOutput": "1.5",
}
_FTS_DETAIL_MODE = "none"
_FTS_COLUMNSIZE = 0
VERSION_CATALOG_TABLE = "version_catalog"
VERSION_DIM_TABLE = "version_dim"
VERSION_SOURCE_TABLES: tuple[str, ...] = ("textMap", "quest", "subtitle", "readable")
_VERSION_TAG_RE = re.compile(r"(\d+)\.(\d+)(?:\.\d+)?")


def normalize_version_label(label: str | None) -> str | None:
    if label is None:
        return None
    text = str(label).strip()
    if not text:
        return text
    return SPECIAL_VERSION_LABEL_MAP.get(text, text)


def _extract_version_tag(raw_version: str | None) -> str | None:
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


def _version_tag_to_sort_key(version_tag: str | None) -> int | None:
    if version_tag is None:
        return None
    text = str(version_tag).strip()
    if not text:
        return None
    parts = text.split(".", 1)
    if len(parts) != 2:
        return None
    try:
        major = int(parts[0])
        minor = int(parts[1])
    except Exception:
        return None
    return major * 1000 + minor


def _normalize_version_catalog_tables(
    source_tables: tuple[str, ...] | list[str] | None = None,
) -> list[str]:
    allowed = set(VERSION_SOURCE_TABLES)
    if source_tables is None:
        return list(VERSION_SOURCE_TABLES)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in source_tables:
        table_name = str(raw).strip()
        if table_name not in allowed or table_name in seen:
            continue
        seen.add(table_name)
        normalized.append(table_name)
    return normalized


def _table_columns(table_name: str) -> set[str]:
    cur = conn.cursor()
    rows = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
    cur.close()
    return {row[1] for row in rows}


def _table_exists(table_name: str) -> bool:
    cur = conn.cursor()
    row = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    cur.close()
    return row is not None


def _ensure_column(table_name: str, column_name: str, ddl_type: str):
    if not _table_exists(table_name):
        return
    cols = _table_columns(table_name)
    if column_name in cols:
        return
    cur = conn.cursor()
    cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}")
    conn.commit()
    cur.close()


def _ensure_index(index_sql: str):
    cur = conn.cursor()
    cur.execute(index_sql)
    conn.commit()
    cur.close()


def _normalize_fts_tokenizer(tokenizer: str | None) -> str:
    text = str(tokenizer or "").strip()
    return text or "trigram"


def _normalize_fts_langs(raw_langs) -> list[int]:
    langs: list[int] = []
    seen: set[int] = set()
    for raw in raw_langs or []:
        try:
            value = int(raw)
        except Exception:
            continue
        if value in seen:
            continue
        seen.add(value)
        langs.append(value)
    return langs


def _read_runtime_fts_settings() -> tuple[bool, list[int], str, str, str, str, str, str, int, int, list[str]]:
    try:
        server_dir = Path(__file__).resolve().parents[1]
        if str(server_dir) not in sys.path:
            sys.path.insert(0, str(server_dir))
        import config as runtime_config

        enabled = runtime_config.getEnableTextMapFts()
        langs = _normalize_fts_langs(runtime_config.getFtsLangAllowList())
        tokenizer = runtime_config.getFtsTokenizer()
        tokenizer_args = runtime_config.getFtsTokenizerArgs()
        chinese_segmenter = runtime_config.getFtsChineseSegmenter()
        jieba_user_dict = runtime_config.getFtsJiebaUserDict()
        ext_path = runtime_config.getFtsExtensionPath()
        ext_entry = runtime_config.getFtsExtensionEntry()
        min_len = runtime_config.getFtsMinTokenLength()
        max_len = runtime_config.getFtsMaxTokenLength()
        stopwords = runtime_config.getFtsStopwords()
        return (
            bool(enabled),
            langs,
            str(tokenizer or ""),
            str(tokenizer_args or ""),
            str(chinese_segmenter or "auto"),
            str(jieba_user_dict or ""),
            str(ext_path or ""),
            str(ext_entry or ""),
            int(min_len),
            int(max_len),
            [str(w) for w in stopwords],
        )
    except Exception:
        return True, [1, 4, 9], "", "", "auto", "", "", "", 1, 32, []


def _resolve_fts_settings() -> tuple[bool, list[int], str, str, str, str, str, str, int, int, list[str]]:
    (
        cfg_enabled,
        cfg_langs,
        cfg_tokenizer,
        cfg_tokenizer_args,
        cfg_chinese_segmenter,
        cfg_jieba_user_dict,
        cfg_ext_path,
        cfg_ext_entry,
        cfg_min_len,
        cfg_max_len,
        cfg_stopwords,
    ) = _read_runtime_fts_settings()
    env_enabled_raw = os.environ.get("GTS_ENABLE_TEXTMAP_FTS")
    if env_enabled_raw is None:
        enabled = cfg_enabled
    else:
        enabled = env_enabled_raw.strip().lower() in ("1", "true", "yes", "on")

    env_langs = os.environ.get("GTS_FTS_LANG_ALLOW_LIST")
    if env_langs is None:
        langs = cfg_langs
    else:
        langs = _normalize_fts_langs([part.strip() for part in env_langs.split(",")])

    tokenizer = _normalize_fts_tokenizer(
        os.environ.get("GTS_FTS_TOKENIZER") or cfg_tokenizer
    )
    tokenizer_args = str(
        os.environ.get("GTS_FTS_TOKENIZER_ARGS") or cfg_tokenizer_args or ""
    ).strip()
    chinese_segmenter = str(
        os.environ.get("GTS_FTS_CHINESE_SEGMENTER") or cfg_chinese_segmenter or "auto"
    ).strip().lower()
    if chinese_segmenter not in ("auto", "jieba", "char_bigram", "none"):
        chinese_segmenter = "auto"
    jieba_user_dict = str(
        os.environ.get("GTS_FTS_JIEBA_USER_DICT") or cfg_jieba_user_dict or ""
    ).strip()
    ext_path = str(
        os.environ.get("GTS_FTS_EXTENSION_PATH") or cfg_ext_path or ""
    ).strip()
    ext_entry = str(
        os.environ.get("GTS_FTS_EXTENSION_ENTRY") or cfg_ext_entry or ""
    ).strip()

    min_len_raw = os.environ.get("GTS_FTS_MIN_TOKEN_LENGTH")
    max_len_raw = os.environ.get("GTS_FTS_MAX_TOKEN_LENGTH")
    try:
        min_len = int(min_len_raw) if min_len_raw is not None else int(cfg_min_len)
    except Exception:
        min_len = int(cfg_min_len)
    try:
        max_len = int(max_len_raw) if max_len_raw is not None else int(cfg_max_len)
    except Exception:
        max_len = int(cfg_max_len)
    min_len = max(1, min_len)
    max_len = max(min_len, max_len)

    env_stopwords = os.environ.get("GTS_FTS_STOPWORDS")
    if env_stopwords is None:
        stopwords = [str(w).strip() for w in cfg_stopwords if str(w).strip()]
    else:
        stopwords = [part.strip() for part in env_stopwords.split(",") if part.strip()]

    if not langs:
        langs = [1, 4, 9]
    return (
        enabled,
        langs,
        tokenizer,
        tokenizer_args,
        chinese_segmenter,
        jieba_user_dict,
        ext_path,
        ext_entry,
        min_len,
        max_len,
        stopwords,
    )


def _try_load_fts_extension(extension_path: str, extension_entry: str) -> bool:
    if not extension_path:
        return False
    try:
        conn.enable_load_extension(True)
    except Exception:
        return False

    try:
        if extension_entry:
            try:
                conn.load_extension(extension_path)
            except TypeError:
                conn.load_extension(extension_path)
        else:
            conn.load_extension(extension_path)
        return True
    except Exception:
        return False
    finally:
        try:
            conn.enable_load_extension(False)
        except Exception:
            pass


def _tokenizer_from_fts_sql(create_sql: str | None) -> str | None:
    if not create_sql:
        return None
    m = re.search(r"tokenize='([^']+)'", str(create_sql))
    if not m:
        return None
    return _normalize_fts_tokenizer(m.group(1))


def _supports_fts_tokenizer(cursor, tokenizer: str) -> bool:
    token_escaped = tokenizer.replace("'", "''")
    probe_name = "_gts_fts_probe_tokenizer"
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {probe_name}")
        cursor.execute(
            f"CREATE VIRTUAL TABLE {probe_name} USING fts5(content, tokenize='{token_escaped}')"
        )
        cursor.execute(f"DROP TABLE IF EXISTS {probe_name}")
        return True
    except Exception:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {probe_name}")
        except Exception:
            pass
        return False


def _fts_langs_signature(langs: list[int]) -> str:
    normalized = sorted(set(int(v) for v in langs))
    return ",".join(str(v) for v in normalized)


def _fts_runtime_signature(
    token_spec: str,
    langs: list[int],
    detail_mode: str,
    columnsize: int,
    chinese_segmenter: str,
    jieba_user_dict: str,
) -> str:
    user_dict_text = str(jieba_user_dict or "").strip()
    return (
        f"{token_spec}|langs={_fts_langs_signature(langs)}"
        f"|detail={detail_mode}|columnsize={columnsize}"
        f"|zhseg={chinese_segmenter}|zhdict={user_dict_text}"
    )


def _fts_table_has_required_columns(cursor) -> bool:
    try:
        cols = {row[1] for row in cursor.execute("PRAGMA table_info(textMap_fts)").fetchall()}
    except Exception:
        return False
    return {"content", "lang", "hash"}.issubset(cols)


def _reset_textmap_fts(cursor):
    cursor.execute("DROP TRIGGER IF EXISTS textMap_fts_ai")
    cursor.execute("DROP TRIGGER IF EXISTS textMap_fts_ad")
    cursor.execute("DROP TRIGGER IF EXISTS textMap_fts_au")
    cursor.execute("DROP TABLE IF EXISTS textMap_fts")
    cursor.execute("DELETE FROM app_meta WHERE k='textmap_fts_built'")
    cursor.execute("DELETE FROM app_meta WHERE k='textmap_fts_tokenizer'")
    cursor.execute("DELETE FROM app_meta WHERE k='textmap_fts_langs'")
    cursor.execute("DELETE FROM app_meta WHERE k='textmap_fts_signature'")


def _create_textmap_fts_objects(
    cursor,
    token_escaped: str,
    langs_sql: str,
    detail_mode: str,
    columnsize: int,
):
    detail_escaped = str(detail_mode).replace("'", "''")
    columnsize_value = 0 if int(columnsize) == 0 else 1
    cursor.execute(
        "CREATE VIRTUAL TABLE IF NOT EXISTS textMap_fts "
        f"USING fts5(content, lang UNINDEXED, hash UNINDEXED, "
        f"content='textMap', content_rowid='id', tokenize='{token_escaped}', "
        f"detail='{detail_escaped}', columnsize={columnsize_value})"
    )

    cursor.execute(
        "CREATE TRIGGER IF NOT EXISTS textMap_fts_ai AFTER INSERT ON textMap "
        f"WHEN new.lang IN ({langs_sql}) BEGIN "
        "INSERT INTO textMap_fts(rowid, content, lang, hash) "
        "VALUES (new.id, gts_fts_content(new.lang, new.content), new.lang, new.hash); "
        "END"
    )
    cursor.execute(
        "CREATE TRIGGER IF NOT EXISTS textMap_fts_ad AFTER DELETE ON textMap BEGIN "
        "INSERT INTO textMap_fts(textMap_fts, rowid, content, lang, hash) "
        "VALUES('delete', old.id, gts_fts_content(old.lang, old.content), old.lang, old.hash); "
        "END"
    )
    cursor.execute(
        "CREATE TRIGGER IF NOT EXISTS textMap_fts_au AFTER UPDATE OF content, lang, hash ON textMap BEGIN "
        "INSERT INTO textMap_fts(textMap_fts, rowid, content, lang, hash) "
        "VALUES('delete', old.id, gts_fts_content(old.lang, old.content), old.lang, old.hash); "
        "INSERT INTO textMap_fts(rowid, content, lang, hash) "
        f"SELECT new.id, gts_fts_content(new.lang, new.content), new.lang, new.hash WHERE new.lang IN ({langs_sql}); "
        "END"
    )


def _ensure_textmap_fts():
    if not _table_exists("textMap"):
        return
    cur = conn.cursor()
    try:
        (
            enabled,
            allow_langs,
            requested_tokenizer,
            tokenizer_args,
            chinese_segmenter,
            jieba_user_dict,
            ext_path,
            ext_entry,
            min_len,
            max_len,
            stopwords,
        ) = _resolve_fts_settings()
        if not enabled:
            _reset_textmap_fts(cur)
            conn.commit()
            return
        _try_load_fts_extension(ext_path, ext_entry)

        token_spec = requested_tokenizer
        if tokenizer_args:
            token_spec = f"{requested_tokenizer} {tokenizer_args}".strip()
        tokenizer_name = token_spec.split()[0] if token_spec else "trigram"

        if not _supports_fts_tokenizer(cur, token_spec):
            if token_spec != "trigram" and _supports_fts_tokenizer(cur, "trigram"):
                token_spec = "trigram"
                tokenizer_name = "trigram"
            else:
                return

        try:
            import fts_tokenizer
        except Exception:
            fts_tokenizer = None

        def _fts_content_sql(lang_code, content):
            try:
                lang_value = int(lang_code)
            except Exception:
                lang_value = 0
            if fts_tokenizer is None:
                return str(content or "")
            return fts_tokenizer.build_fts_index_text(
                str(content or ""),
                lang_value,
                tokenizer_name,
                segmenter_mode=chinese_segmenter,
                user_dict_path=jieba_user_dict,
            )

        try:
            conn.create_function("gts_fts_content", 2, _fts_content_sql)
        except Exception:
            pass

        langs_signature = _fts_langs_signature(allow_langs)
        runtime_signature = _fts_runtime_signature(
            token_spec,
            allow_langs,
            _FTS_DETAIL_MODE,
            _FTS_COLUMNSIZE,
            chinese_segmenter,
            jieba_user_dict,
        )
        existing_row = cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='textMap_fts' LIMIT 1"
        ).fetchone()
        existing_tokenizer = _tokenizer_from_fts_sql(existing_row[0] if existing_row else None)
        existing_langs_row = cur.execute(
            "SELECT v FROM app_meta WHERE k='textmap_fts_langs' LIMIT 1"
        ).fetchone()
        existing_langs_signature = str(existing_langs_row[0]).strip() if existing_langs_row and existing_langs_row[0] else ""
        existing_signature_row = cur.execute(
            "SELECT v FROM app_meta WHERE k='textmap_fts_signature' LIMIT 1"
        ).fetchone()
        existing_signature = str(existing_signature_row[0]).strip() if existing_signature_row and existing_signature_row[0] else ""

        structure_changed = existing_row is not None and not _fts_table_has_required_columns(cur)
        tokenizer_changed = existing_row is not None and existing_tokenizer != token_spec
        langs_changed = existing_row is not None and existing_langs_signature != langs_signature
        signature_changed = existing_signature != runtime_signature
        if tokenizer_changed or structure_changed or langs_changed:
            _reset_textmap_fts(cur)

        token_escaped = token_spec.replace("'", "''")
        langs_sql = ",".join(str(v) for v in sorted(set(allow_langs)))
        try:
            _create_textmap_fts_objects(
                cur,
                token_escaped,
                langs_sql,
                _FTS_DETAIL_MODE,
                _FTS_COLUMNSIZE,
            )
        except Exception:
            return

        marker = cur.execute(
            "SELECT v FROM app_meta WHERE k='textmap_fts_built' LIMIT 1"
        ).fetchone()
        needs_rebuild = (
            tokenizer_changed
            or structure_changed
            or langs_changed
            or signature_changed
            or marker is None
            or marker[0] != "1"
        )
        if not needs_rebuild:
            has_textmap = cur.execute("SELECT 1 FROM textMap LIMIT 1").fetchone() is not None
            has_fts = cur.execute("SELECT 1 FROM textMap_fts LIMIT 1").fetchone() is not None
            needs_rebuild = has_textmap and not has_fts

        if needs_rebuild:
            _reset_textmap_fts(cur)
            _create_textmap_fts_objects(
                cur,
                token_escaped,
                langs_sql,
                _FTS_DETAIL_MODE,
                _FTS_COLUMNSIZE,
            )
            cur.execute(
                f"INSERT INTO textMap_fts(rowid, content, lang, hash) "
                f"SELECT id, gts_fts_content(lang, content), lang, hash FROM textMap WHERE lang IN ({langs_sql})"
            )
            try:
                cur.execute("INSERT INTO textMap_fts(textMap_fts) VALUES('optimize')")
            except Exception:
                pass
        cur.execute(
            "INSERT OR REPLACE INTO app_meta(k, v) VALUES ('textmap_fts_built', '1')"
        )
        cur.execute(
            "INSERT OR REPLACE INTO app_meta(k, v) VALUES ('textmap_fts_tokenizer', ?)",
            (token_spec,),
        )
        cur.execute(
            "INSERT OR REPLACE INTO app_meta(k, v) VALUES ('textmap_fts_langs', ?)",
            (langs_signature,),
        )
        cur.execute(
            "INSERT OR REPLACE INTO app_meta(k, v) VALUES ('textmap_fts_signature', ?)",
            (runtime_signature,),
        )
        conn.commit()
    finally:
        cur.close()


def _ensure_unique_index(table_name: str, index_name: str, columns: tuple[str, ...]):
    if not _table_exists(table_name):
        return
    cols_expr = ", ".join(columns)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            DELETE FROM {table_name}
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM {table_name}
                GROUP BY {cols_expr}
            )
            """
        )
        cur.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name}({cols_expr})"
        )
        conn.commit()
    finally:
        cur.close()


def _ensure_version_dim_table():
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {VERSION_DIM_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_version TEXT NOT NULL UNIQUE,
                version_tag TEXT,
                version_sort_key INTEGER
            )
            """
        )
        conn.commit()
    finally:
        cur.close()
    _ensure_column(VERSION_DIM_TABLE, "version_sort_key", "INTEGER")
    _ensure_index(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {VERSION_DIM_TABLE}_raw_version_uindex "
        f"ON {VERSION_DIM_TABLE}(raw_version)"
    )
    _ensure_index(
        f"CREATE INDEX IF NOT EXISTS {VERSION_DIM_TABLE}_version_tag_index "
        f"ON {VERSION_DIM_TABLE}(version_tag)"
    )
    _ensure_index(
        f"CREATE INDEX IF NOT EXISTS {VERSION_DIM_TABLE}_version_sort_key_index "
        f"ON {VERSION_DIM_TABLE}(version_sort_key)"
    )


def get_or_create_version_id(raw_version: str | None) -> int | None:
    text = str(raw_version or "").strip()
    if not text:
        return None
    _ensure_version_dim_table()
    cur = conn.cursor()
    try:
        # 检查版本是否已存在
        row = cur.execute(
            f"SELECT id FROM {VERSION_DIM_TABLE} WHERE raw_version=? LIMIT 1",
            (text,),
        ).fetchone()
        if row:
            return int(row[0])

        # 获取当前最大ID
        max_id_row = cur.execute(f"SELECT MAX(id) FROM {VERSION_DIM_TABLE}").fetchone()
        new_id = max_id_row[0] + 1 if max_id_row[0] else 1

        # 插入新版本，使用指定的ID
        cur.execute(
            f"""
            INSERT INTO {VERSION_DIM_TABLE}(id, raw_version, version_tag, version_sort_key)
            VALUES (?, ?, ?, ?)
            """,
            (
                new_id,
                text,
                _extract_version_tag(text),
                _version_tag_to_sort_key(_extract_version_tag(text)),
            ),
        )
        conn.commit()
        return new_id
    finally:
        cur.close()


def _backfill_version_sort_keys():
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
        row = cur.execute(
            "SELECT v FROM app_meta WHERE k='version_sort_key_backfilled_v1'"
        ).fetchone()
        if row and row[0] == "1":
            return

        rows = cur.execute(
            f"SELECT id, version_tag FROM {VERSION_DIM_TABLE} WHERE version_sort_key IS NULL"
        ).fetchall()
        payload: list[tuple[int, int]] = []
        for version_id, version_tag in rows:
            sort_key = _version_tag_to_sort_key(version_tag)
            if sort_key is None:
                continue
            payload.append((sort_key, int(version_id)))

        if payload:
            cur.executemany(
                f"UPDATE {VERSION_DIM_TABLE} SET version_sort_key=? WHERE id=?",
                payload,
            )
        cur.execute(
            "INSERT OR REPLACE INTO app_meta(k, v) VALUES ('version_sort_key_backfilled_v1', '1')"
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        cur.close()


def _backfill_version_dim_and_ids():
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
        row = cur.execute(
            "SELECT v FROM app_meta WHERE k='version_dim_backfilled_v2'"
        ).fetchone()
        if row and row[0] == "1":
            return

        for table_name in VERSION_SOURCE_TABLES:
            if not _table_exists(table_name):
                continue
            cols = _table_columns(table_name)
            # 跳过quest表，因为它的更新版本存储在quest_version表中
            if table_name != "quest" and "created_version_id" in cols and "updated_version_id" in cols:
                cur.execute(
                    f"""
                    UPDATE {table_name}
                    SET updated_version_id = created_version_id
                    WHERE updated_version_id IS NULL
                      AND created_version_id IS NOT NULL
                    """
                )

        cur.execute(
            "INSERT OR REPLACE INTO app_meta(k, v) VALUES ('version_dim_backfilled_v2', '1')"
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        cur.close()


def _ensure_updated_version_autofill_rules():
    tables = ("textMap", "readable", "subtitle")
    cur = conn.cursor()
    try:
        for table_name in tables:
            if not _table_exists(table_name):
                continue

            trigger_ai = f"{table_name}_version_autofill_ai"
            trigger_au = f"{table_name}_version_autofill_au"
            cur.execute(f"DROP TRIGGER IF EXISTS {trigger_ai}")
            cur.execute(f"DROP TRIGGER IF EXISTS {trigger_au}")
            cols = _table_columns(table_name)
            has_id_cols = "created_version_id" in cols and "updated_version_id" in cols
            if not has_id_cols:
                continue

            cur.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS {trigger_ai}
                AFTER INSERT ON {table_name}
                FOR EACH ROW
                BEGIN
                    UPDATE {table_name}
                    SET updated_version_id = NEW.created_version_id
                    WHERE rowid = NEW.rowid
                      AND updated_version_id IS NULL
                      AND created_version_id IS NOT NULL;
                END
                """
            )
            cur.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS {trigger_au}
                AFTER UPDATE OF created_version_id, updated_version_id ON {table_name}
                FOR EACH ROW
                BEGIN
                    UPDATE {table_name}
                    SET updated_version_id = NEW.created_version_id
                    WHERE rowid = NEW.rowid
                      AND updated_version_id IS NULL
                      AND created_version_id IS NOT NULL;
                END
                """
            )
            cur.execute(
                f"""
                UPDATE {table_name}
                SET updated_version_id = created_version_id
                WHERE updated_version_id IS NULL
                  AND created_version_id IS NOT NULL
                """
            )
        conn.commit()
    finally:
        cur.close()


def _ensure_version_catalog_table():
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {VERSION_CATALOG_TABLE} (
                source_table TEXT NOT NULL,
                raw_version TEXT NOT NULL,
                version_tag TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (source_table, raw_version)
            )
            """
        )
        conn.commit()
    finally:
        cur.close()

    _ensure_column(VERSION_CATALOG_TABLE, "version_tag", "TEXT")
    _ensure_column(VERSION_CATALOG_TABLE, "updated_at", "TEXT")
    _ensure_index(
        f"CREATE INDEX IF NOT EXISTS {VERSION_CATALOG_TABLE}_source_version_tag_index "
        f"ON {VERSION_CATALOG_TABLE}(source_table, version_tag)"
    )
    _ensure_index(
        f"CREATE INDEX IF NOT EXISTS {VERSION_CATALOG_TABLE}_version_tag_index "
        f"ON {VERSION_CATALOG_TABLE}(version_tag)"
    )


def _ensure_quest_hash_map_table():
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS quest_hash_map (
                questId INTEGER NOT NULL,
                hash INTEGER NOT NULL,
                source_type TEXT NOT NULL,
                PRIMARY KEY (questId, hash, source_type)
            )
            """
        )
        conn.commit()
    finally:
        cur.close()
    _ensure_index("CREATE INDEX IF NOT EXISTS quest_hash_map_hash_index ON quest_hash_map(hash)")
    _ensure_index("CREATE INDEX IF NOT EXISTS quest_hash_map_questId_index ON quest_hash_map(questId)")


def rebuild_version_catalog(
    source_tables: tuple[str, ...] | list[str] | None = None,
) -> dict[str, int]:
    selected_tables = _normalize_version_catalog_tables(source_tables)
    _ensure_version_catalog_table()
    if not selected_tables:
        return {}

    cur = conn.cursor()
    stats: dict[str, int] = {}
    try:
        cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
        for table_name in selected_tables:
            cur.execute(
                f"DELETE FROM {VERSION_CATALOG_TABLE} WHERE source_table=?",
                (table_name,),
            )

            if not _table_exists(table_name):
                stats[table_name] = 0
                continue
            cols = _table_columns(table_name)

            query_parts: list[str] = []
            has_id_cols = "created_version_id" in cols and "updated_version_id" in cols
            if (
                has_id_cols
                and _table_exists(VERSION_DIM_TABLE)
            ):
                # 只记录创建版本
                query_parts.append(
                    f"SELECT vd.raw_version AS v FROM {table_name} t "
                    f"JOIN {VERSION_DIM_TABLE} vd ON vd.id = t.created_version_id "
                    f"WHERE t.created_version_id IS NOT NULL"
                )
                # 只记录与创建版本不同的更新版本
                if table_name != "quest":
                    # 对于非quest表，从表本身的updated_version_id列获取更新版本
                    query_parts.append(
                        f"SELECT vd_u.raw_version AS v FROM {table_name} t "
                        f"JOIN {VERSION_DIM_TABLE} vd_c ON vd_c.id = t.created_version_id "
                        f"JOIN {VERSION_DIM_TABLE} vd_u ON vd_u.id = t.updated_version_id "
                        f"WHERE t.created_version_id IS NOT NULL "
                        f"AND t.updated_version_id IS NOT NULL "
                        f"AND vd_c.raw_version != vd_u.raw_version"
                    )
                else:
                    # 对于quest表，从quest_version表获取更新版本
                    if _table_exists("quest_version"):
                        query_parts.append(
                            f"SELECT vd_u.raw_version AS v FROM quest_version qv "
                            f"JOIN quest q ON q.questId = qv.questId "
                            f"JOIN {VERSION_DIM_TABLE} vd_c ON vd_c.id = q.created_version_id "
                            f"JOIN {VERSION_DIM_TABLE} vd_u ON vd_u.id = qv.updated_version_id "
                            f"WHERE q.created_version_id IS NOT NULL "
                            f"AND qv.updated_version_id IS NOT NULL "
                            f"AND vd_c.raw_version != vd_u.raw_version"
                        )
            if not query_parts:
                stats[table_name] = 0
                continue
            rows = cur.execute(
                f"SELECT DISTINCT v FROM ({' UNION '.join(query_parts)})"
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
                cur.executemany(
                    f"""
                    INSERT OR REPLACE INTO {VERSION_CATALOG_TABLE}
                    (source_table, raw_version, version_tag, updated_at)
                    VALUES (?, ?, ?, datetime('now'))
                    """,
                    payload,
                )
            stats[table_name] = len(payload)

        cur.execute(
            "INSERT OR REPLACE INTO app_meta(k, v) VALUES ('version_catalog_updated_at', datetime('now'))"
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        cur.close()
    return stats


def ensure_version_schema():
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
    conn.commit()
    cur.close()
    _ensure_version_dim_table()
    _backfill_version_sort_keys()
    _ensure_version_catalog_table()
    _ensure_quest_hash_map_table()

    for table_name in ("textMap", "readable", "subtitle"):
        _ensure_column(table_name, "created_version_id", "INTEGER")
        _ensure_column(table_name, "updated_version_id", "INTEGER")
    # 为quest表只添加created_version_id列，updated_version_id现在存储在quest_version表中
    _ensure_column("quest", "created_version_id", "INTEGER")
    _ensure_column("quest", "git_created_version_id", "INTEGER")
    _ensure_column("quest", "source_type", "TEXT")
    _ensure_column("quest", "source_code_raw", "TEXT")
    _ensure_column("questTalk", "coopQuestId", "INTEGER NOT NULL DEFAULT 0")

    _ensure_column("subtitle", "subtitleKey", "TEXT")

    _ensure_unique_index("quest", "quest_questId_uindex", ("questId",))
    _ensure_unique_index("subtitle", "subtitle_subtitleKey_uindex", ("subtitleKey",))
    _ensure_unique_index("avatar", "avatar_avatarId_uindex", ("avatarId",))
    _ensure_unique_index("chapter", "chapter_chapterId_uindex", ("chapterId",))
    _ensure_unique_index("fetters", "fetters_fetterId_uindex", ("fetterId",))
    _ensure_unique_index("fetterStory", "fetterStory_fetterId_uindex", ("fetterId",))
    cur = conn.cursor()
    try:
        cur.execute("DROP INDEX IF EXISTS questTalk_questId_talkId_uindex")
        cur.execute("UPDATE questTalk SET coopQuestId = 0 WHERE coopQuestId IS NULL")
        conn.commit()
    finally:
        cur.close()
    _ensure_unique_index("questTalk", "questTalk_questId_talkId_coopQuestId_uindex", ("questId", "talkId", "coopQuestId"))
    _ensure_unique_index("voice", "voice_dialogueId_voicePath_uindex", ("dialogueId", "voicePath"))

    _ensure_index("CREATE INDEX IF NOT EXISTS readable_lang_index ON readable(lang)")
    _ensure_index("CREATE INDEX IF NOT EXISTS readable_lang_fileName_index ON readable(lang, fileName)")
    _ensure_index("CREATE INDEX IF NOT EXISTS subtitle_lang_index ON subtitle(lang)")
    _ensure_index("CREATE INDEX IF NOT EXISTS subtitle_fileName_lang_startTime_index ON subtitle(fileName, lang, startTime)")
    _ensure_index("CREATE INDEX IF NOT EXISTS subtitle_lang_subtitleId_startTime_index ON subtitle(lang, subtitleId, startTime)")
    _ensure_index("CREATE INDEX IF NOT EXISTS textMap_created_version_id_index ON textMap(created_version_id)")
    _ensure_index("CREATE INDEX IF NOT EXISTS textMap_updated_version_id_index ON textMap(updated_version_id)")
    _ensure_index("CREATE INDEX IF NOT EXISTS quest_created_version_id_index ON quest(created_version_id)")
    _ensure_index("CREATE INDEX IF NOT EXISTS quest_git_created_version_id_index ON quest(git_created_version_id)")
    _ensure_index("CREATE INDEX IF NOT EXISTS quest_source_type_index ON quest(source_type)")
    _ensure_index("CREATE INDEX IF NOT EXISTS questTalk_talkId_coopQuestId_index ON questTalk(talkId, coopQuestId)")
    # 不再为quest表的updated_version_id列创建索引，因为它现在存储在quest_version表中
    _ensure_index("CREATE INDEX IF NOT EXISTS readable_created_version_id_index ON readable(created_version_id)")
    _ensure_index("CREATE INDEX IF NOT EXISTS readable_updated_version_id_index ON readable(updated_version_id)")
    _ensure_index("CREATE INDEX IF NOT EXISTS subtitle_created_version_id_index ON subtitle(created_version_id)")
    _ensure_index("CREATE INDEX IF NOT EXISTS subtitle_updated_version_id_index ON subtitle(updated_version_id)")
    _backfill_version_dim_and_ids()
    _ensure_updated_version_autofill_rules()
    _ensure_textmap_fts()


def resolve_version_label(commit: str, repo_path: str = DATA_PATH) -> str:
    if not commit:
        return "unknown"
    try:
        proc = subprocess.run(
            ["git", "-C", repo_path, "show", "-s", "--format=%s", commit],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if proc.returncode == 0:
            title = (proc.stdout or "").strip()
            if title:
                return normalize_version_label(title) or title
    except Exception:
        pass
    return normalize_version_label(commit) or commit


def get_current_version(default: str = "unknown") -> str:
    cur = conn.cursor()
    row = cur.execute(
        "SELECT v FROM app_meta WHERE k='db_current_commit_title'"
    ).fetchone()
    if not row or not row[0]:
        row = cur.execute("SELECT v FROM app_meta WHERE k='db_current_commit'").fetchone()
    cur.close()
    if row and row[0]:
        normalized = normalize_version_label(row[0])
        return normalized or row[0]
    normalized_default = normalize_version_label(default)
    return normalized_default or default


def set_current_version(
    commit: str,
    remote_ref: str = "origin/master",
    version_label: str | None = None,
):
    label = normalize_version_label(version_label) or resolve_version_label(commit)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
    cur.executemany(
        "INSERT OR REPLACE INTO app_meta(k, v) VALUES (?, ?)",
        [
            ("agd_repo_url", "https://gitlab.com/Dimbreath/AnimeGameData.git"),
            ("agd_remote_ref", remote_ref),
            ("db_current_commit", commit),
            ("db_current_commit_title", label),
        ],
    )
    conn.commit()
    cur.close()


def get_meta(key: str, default: str | None = None) -> str | None:
    cur = conn.cursor()
    row = cur.execute("SELECT v FROM app_meta WHERE k=?", (key,)).fetchone()
    cur.close()
    if row and row[0] is not None:
        return row[0]
    return default


def set_meta(key: str, value: str):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
    cur.execute("INSERT OR REPLACE INTO app_meta(k, v) VALUES (?, ?)", (key, value))
    conn.commit()
    cur.close()

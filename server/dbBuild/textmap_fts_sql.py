def build_textmap_fts_table_sql(
    token_escaped: str,
    detail_mode: str,
    columnsize: int,
) -> str:
    detail_escaped = str(detail_mode).replace("'", "''")
    columnsize_value = 0 if int(columnsize) == 0 else 1
    return (
        "CREATE VIRTUAL TABLE IF NOT EXISTS textMap_fts "
        f"USING fts5(content, lang UNINDEXED, hash UNINDEXED, "
        f"content='textMap', content_rowid='id', tokenize='{token_escaped}', "
        f"detail='{detail_escaped}', columnsize={columnsize_value})"
    )


def build_textmap_fts_ai_trigger_sql(langs_sql: str) -> str:
    return (
        "CREATE TRIGGER IF NOT EXISTS textMap_fts_ai AFTER INSERT ON textMap "
        f"WHEN new.lang IN ({langs_sql}) BEGIN "
        "INSERT INTO textMap_fts(rowid, content, lang, hash) "
        "VALUES (new.id, gts_fts_content(new.lang, new.content), new.lang, new.hash); "
        "END"
    )


def build_textmap_fts_ad_trigger_sql(langs_sql: str) -> str:
    return (
        "CREATE TRIGGER IF NOT EXISTS textMap_fts_ad AFTER DELETE ON textMap BEGIN "
        "INSERT INTO textMap_fts(textMap_fts, rowid, content, lang, hash) "
        f"SELECT 'delete', old.id, gts_fts_content(old.lang, old.content), old.lang, old.hash "
        f"WHERE old.lang IN ({langs_sql}); "
        "END"
    )


def build_textmap_fts_au_trigger_sql(langs_sql: str) -> str:
    return (
        "CREATE TRIGGER IF NOT EXISTS textMap_fts_au AFTER UPDATE OF content, lang, hash ON textMap BEGIN "
        "INSERT INTO textMap_fts(textMap_fts, rowid, content, lang, hash) "
        f"SELECT 'delete', old.id, gts_fts_content(old.lang, old.content), old.lang, old.hash "
        f"WHERE old.lang IN ({langs_sql}); "
        "INSERT INTO textMap_fts(rowid, content, lang, hash) "
        f"SELECT new.id, gts_fts_content(new.lang, new.content), new.lang, new.hash WHERE new.lang IN ({langs_sql}); "
        "END"
    )

from __future__ import annotations

QUEST_TEST_TEXT_PREFIXES: tuple[str, ...] = (
    "（test）",
    "（Test）",
    "(test)",
    "(Test)",
)
QUEST_TEST_TEXT_GLOB_PREFIXES: tuple[str, ...] = tuple(
    f"{prefix}*" for prefix in QUEST_TEST_TEXT_PREFIXES
)
QUEST_TEXT_FILTER_LANG_CODE = "TextMapCHS.json"


def is_excluded_quest_text(content: object) -> bool:
    text = content if isinstance(content, str) else str(content or "")
    return any(text.startswith(prefix) for prefix in QUEST_TEST_TEXT_PREFIXES)


def build_quest_text_excluded_sql(content_expr: str) -> tuple[str, tuple[str, ...]]:
    sql = "(" + " OR ".join(
        f"COALESCE({content_expr}, '') GLOB ?"
        for _ in QUEST_TEST_TEXT_GLOB_PREFIXES
    ) + ")"
    return sql, QUEST_TEST_TEXT_GLOB_PREFIXES


def build_quest_text_not_excluded_sql(content_expr: str) -> tuple[str, tuple[str, ...]]:
    sql = "(" + " AND ".join(
        f"COALESCE({content_expr}, '') NOT GLOB ?"
        for _ in QUEST_TEST_TEXT_GLOB_PREFIXES
    ) + ")"
    return sql, QUEST_TEST_TEXT_GLOB_PREFIXES


def get_quest_text_filter_lang_id(cursor) -> int | None:
    row = cursor.execute(
        "SELECT id FROM langCode WHERE codeName=? LIMIT 1",
        (QUEST_TEXT_FILTER_LANG_CODE,),
    ).fetchone()
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except Exception:
        return None


def is_excluded_quest_text_hash(
    cursor,
    text_hash: int | None,
    *,
    lang_id: int | None = None,
) -> bool:
    try:
        normalized_hash = int(text_hash) if text_hash not in (None, 0) else 0
    except Exception:
        normalized_hash = 0
    if normalized_hash <= 0:
        return False
    resolved_lang_id = lang_id if lang_id is not None else get_quest_text_filter_lang_id(cursor)
    if resolved_lang_id is None:
        return False
    row = cursor.execute(
        "SELECT content FROM textMap WHERE hash=? AND lang=? LIMIT 1",
        (normalized_hash, resolved_lang_id),
    ).fetchone()
    return bool(row and is_excluded_quest_text(row[0]))


def sanitize_quest_text_hash(
    cursor,
    text_hash: int | None,
    *,
    lang_id: int | None = None,
) -> int | None:
    try:
        normalized_hash = int(text_hash) if text_hash not in (None, 0) else 0
    except Exception:
        normalized_hash = 0
    if normalized_hash <= 0:
        return None
    if is_excluded_quest_text_hash(cursor, normalized_hash, lang_id=lang_id):
        return None
    return normalized_hash

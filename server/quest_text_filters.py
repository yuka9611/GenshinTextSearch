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
SHORT_GENERIC_TEXT_MAX_LENGTH = 6
SHORT_GENERIC_TEXT_INTERJECTION_CHARS = "呀啊哇呜嗯唔哦噢欸诶哼哈嘿呵嗷喂呃咦哟哎"
SHORT_GENERIC_TEXT_PUNCTUATION_CHARS = "…？！?!～~。．，、·・—-；:（）()「」『』【】《》〈〉"
_SHORT_GENERIC_TEXT_TRIM_CHARS_SQL = "' ' || char(9) || char(10) || char(13) || '　'"
_SHORT_GENERIC_TEXT_SQL_EXTRA_STRIP_TOKENS: tuple[str, ...] = (
    "char(9)",
    "char(10)",
    "char(13)",
)
_SHORT_GENERIC_TEXT_INTERJECTION_SET = frozenset(SHORT_GENERIC_TEXT_INTERJECTION_CHARS)
_SHORT_GENERIC_TEXT_PUNCTUATION_SET = frozenset(
    SHORT_GENERIC_TEXT_PUNCTUATION_CHARS + " \t\r\n　"
)
_SHORT_GENERIC_TEXT_ALLOWED_SET = frozenset(
    SHORT_GENERIC_TEXT_INTERJECTION_CHARS + SHORT_GENERIC_TEXT_PUNCTUATION_CHARS + " \t\r\n　"
)


def is_excluded_quest_text(content: object) -> bool:
    text = content if isinstance(content, str) else str(content or "")
    return any(text.startswith(prefix) for prefix in QUEST_TEST_TEXT_PREFIXES)


def _normalize_short_generic_text(content: object) -> str:
    text = content if isinstance(content, str) else str(content or "")
    return text.strip(" \t\r\n　")


def is_short_generic_text(content: object) -> bool:
    text = _normalize_short_generic_text(content)
    if not text or len(text) > SHORT_GENERIC_TEXT_MAX_LENGTH:
        return False
    if all(char in _SHORT_GENERIC_TEXT_PUNCTUATION_SET for char in text):
        return True
    return all(char in _SHORT_GENERIC_TEXT_ALLOWED_SET for char in text) and any(
        char in _SHORT_GENERIC_TEXT_INTERJECTION_SET for char in text
    )


def is_excluded_quest_version_dialogue_text(content: object) -> bool:
    return is_excluded_quest_text(content) or is_short_generic_text(content)


def _sql_quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_trimmed_text_sql(content_expr: str) -> str:
    return f"trim(coalesce({content_expr}, ''), {_SHORT_GENERIC_TEXT_TRIM_CHARS_SQL})"


def _build_sql_strip_chars(content_expr: str, chars: str) -> str:
    expr = content_expr
    seen_chars: set[str] = set()
    for char in chars:
        if char in ("\t", "\n", "\r") or char in seen_chars:
            continue
        seen_chars.add(char)
        expr = f"replace({expr}, {_sql_quote_literal(char)}, '')"
    for token in _SHORT_GENERIC_TEXT_SQL_EXTRA_STRIP_TOKENS:
        expr = f"replace({expr}, {token}, '')"
    return expr


def build_short_generic_text_excluded_sql(
    content_expr: str,
) -> tuple[str, tuple[object, ...]]:
    trimmed_expr = _build_trimmed_text_sql(content_expr)
    punctuation_only_expr = _build_sql_strip_chars(
        trimmed_expr,
        SHORT_GENERIC_TEXT_PUNCTUATION_CHARS + " 　",
    )
    allowed_chars_expr = _build_sql_strip_chars(
        trimmed_expr,
        SHORT_GENERIC_TEXT_INTERJECTION_CHARS + SHORT_GENERIC_TEXT_PUNCTUATION_CHARS + " 　",
    )
    has_interjection_sql = " OR ".join(
        f"instr({trimmed_expr}, {_sql_quote_literal(char)}) > 0"
        for char in dict.fromkeys(SHORT_GENERIC_TEXT_INTERJECTION_CHARS)
    )
    sql = (
        "("
        f"{trimmed_expr} <> '' "
        f"AND length({trimmed_expr}) <= {SHORT_GENERIC_TEXT_MAX_LENGTH} "
        "AND ("
        f"{punctuation_only_expr} = '' "
        f"OR ({allowed_chars_expr} = '' AND ({has_interjection_sql}))"
        ")"
        ")"
    )
    return sql, tuple()


def build_short_generic_text_not_excluded_sql(
    content_expr: str,
) -> tuple[str, tuple[object, ...]]:
    excluded_sql, _excluded_params = build_short_generic_text_excluded_sql(content_expr)
    return f"(NOT {excluded_sql})", tuple()


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


def build_quest_version_dialogue_excluded_sql(
    content_expr: str,
) -> tuple[str, tuple[str, ...]]:
    excluded_sql, excluded_params = build_quest_text_excluded_sql(content_expr)
    short_generic_sql, _short_generic_params = build_short_generic_text_excluded_sql(content_expr)
    return f"(({excluded_sql}) OR ({short_generic_sql}))", excluded_params


def build_quest_version_dialogue_not_excluded_sql(
    content_expr: str,
) -> tuple[str, tuple[str, ...]]:
    not_excluded_sql, not_excluded_params = build_quest_text_not_excluded_sql(content_expr)
    short_generic_sql, _short_generic_params = build_short_generic_text_excluded_sql(content_expr)
    return f"(({not_excluded_sql}) AND NOT ({short_generic_sql}))", not_excluded_params


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

import re


_INT_PATTERN = re.compile(r"^[+-]?\d+$")
_HEX_PATTERN = re.compile(r"^[+-]?0x[0-9a-fA-F]+$")
_VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)(?:\.\d+)?")


def _parse_int_keyword(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if _HEX_PATTERN.match(text):
        return int(text, 16)
    if _INT_PATTERN.match(text):
        return int(text, 10)
    return None


def _extract_version_tag(raw_version: str | None) -> str | None:
    if raw_version is None:
        return None
    text = str(raw_version).strip()
    if not text:
        return None
    matches = _VERSION_PATTERN.findall(text)
    if not matches:
        return None
    major, minor = matches[-1]
    return f"{major}.{minor}"


def _normalize_version_filter(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    extracted = _extract_version_tag(text)
    return extracted or text


def _build_version_fields(created_raw: str | None, updated_raw: str | None) -> dict:
    return {
        "createdVersionRaw": created_raw,
        "updatedVersionRaw": updated_raw,
        "createdVersion": _extract_version_tag(created_raw),
        "updatedVersion": _extract_version_tag(updated_raw),
    }


def _entry_version_match(entry: dict, created_filter: str | None, updated_filter: str | None) -> bool:
    if created_filter:
        created_tag = entry.get("createdVersion")
        if created_tag != created_filter:
            return False
    if updated_filter:
        # Updated-version queries should exclude rows whose created/updated are equal.
        created_tag = entry.get("createdVersion")
        updated_tag = entry.get("updatedVersion")
        if created_tag and updated_tag:
            if created_tag == updated_tag:
                return False
        if updated_tag != updated_filter:
            return False
    return True


def _resolve_avatar_query_langs(search_lang: int | None = None) -> tuple[list[int], int, int]:
    import config
    langs = config.getResultLanguages().copy()
    if search_lang and search_lang not in langs:
        langs.append(search_lang)
    source_lang_code = config.getSourceLanguage()
    keyword_lang_code = search_lang if search_lang else source_lang_code
    return langs, source_lang_code, keyword_lang_code


def _normalize_text_map_content(content: str | None, lang_code: int):
    if content is None:
        return None
    if content.startswith("#"):
        import placeholderHandler
        import config
        return placeholderHandler.replace(content, config.getIsMale(), lang_code)[1:]
    return content


def _get_text_map_content_with_fallback(
    text_hash: int | None,
    preferred_lang: int | None = None,
    fallback_langs: list[int] | None = None,
) -> str | None:
    if not text_hash:
        return None

    ordered_langs: list[int] = []
    seen_langs: set[int] = set()

    def _push_lang(lang: int | None):
        if lang is None:
            return
        if lang in seen_langs:
            return
        seen_langs.add(lang)
        ordered_langs.append(lang)

    _push_lang(preferred_lang)
    for lang in (fallback_langs or []):
        _push_lang(lang)

    import databaseHelper
    for lang in ordered_langs:
        content = databaseHelper.getTextMapContent(text_hash, lang)
        normalized = _normalize_text_map_content(content, lang)
        if normalized:
            return normalized

    translations = databaseHelper.selectTextMapFromTextHash(text_hash, None)
    for content, lang in translations:
        normalized = _normalize_text_map_content(content, lang)
        if normalized:
            return normalized
    return None


def _build_text_map_translates(text_hash: int | None, langs: 'list[int]'):
    if not text_hash:
        return None
    import databaseHelper
    translates = databaseHelper.selectTextMapFromTextHash(text_hash, langs)
    if not translates:
        return None
    result = {}
    for content, lang_code in translates:
        result[lang_code] = _normalize_text_map_content(content, lang_code)
    return result if result else None


def _build_lang_str_to_id_map() -> dict[str, int]:
    import databaseHelper
    result: dict[str, int] = {}
    lang_map = databaseHelper.getLangCodeMap()

    def add_alias(alias: str, lang_id: int):
        if not alias:
            return
        result[alias] = lang_id
        result[alias.upper()] = lang_id

    for lang_id, code_name in lang_map.items():
        if not code_name:
            continue
        code_text = str(code_name).strip()
        if not code_text:
            continue
        add_alias(code_text, lang_id)
        m = re.match(r"^Text(?:Map)?([A-Za-z0-9_]+)\.json$", code_text, re.IGNORECASE)
        if m:
            short = m.group(1)
            add_alias(short, lang_id)
            add_alias(f"TextMap{short.upper()}.json", lang_id)
            add_alias(f"Text{short.upper()}.json", lang_id)
        elif re.fullmatch(r"[A-Za-z0-9_]{2,8}", code_text):
            upper = code_text.upper()
            add_alias(f"TextMap{upper}.json", lang_id)
            add_alias(f"Text{upper}.json", lang_id)
    return result
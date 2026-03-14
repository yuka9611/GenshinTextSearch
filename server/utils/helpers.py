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
    import placeholderHandler
    import config

    return placeholderHandler.replace(content, config.getIsMale(), lang_code)


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
    """
    构建语言字符串到语言ID的映射
    - 从数据库获取语言代码映射
    - 为每种语言添加别名，包括大写形式
    - 处理TextMap文件名格式的语言代码
    """
    import databaseHelper
    result: dict[str, int] = {}
    lang_map = databaseHelper.getLangCodeMap()

    def add_alias(alias: str, lang_id: int):
        """
        添加语言别名到映射中
        """
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
        # 处理TextMap文件名格式的语言代码
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


def resource_path(rel_path: str) -> str:
    """
    兼容 PyInstaller 和源码运行的资源路径
    - 打包后资源在 sys._MEIPASS
    - 源码运行时以项目根目录为基准（server/..）
    """
    import os
    import sys
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS  # type: ignore
    else:
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(base_path, rel_path)


def buildResponse(data=None, code=200, msg="ok"):
    from flask import jsonify
    return jsonify({"data": data, "code": code, "msg": msg})


def _has_non_empty(value) -> bool:
    return not (value is None or str(value).strip() == "")


def _to_int_or_default(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _to_positive_int_or_default(value, default: int) -> int:
    result = _to_int_or_default(value, default)
    return result if result > 0 else default


def getLangFromRequest():
    from flask import request
    lang = request.args.get('lang', 'zh-cn', type=str)
    return lang


def normalizeSearchTerm(term: str) -> str:
    # 标准化搜索词，移除多余空格等
    return term.strip().lower()


def getLanguageName(lang_code: str) -> str:
    # 根据语言代码获取语言名称
    lang_names = {
        "zh-cn": "中文",
        "en-us": "English",
        "ja-jp": "日本語",
        "ko-kr": "한국어"
    }
    return lang_names.get(lang_code, lang_code)


def buildErrorResponse(code=400, msg="Bad Request"):
    """
    构建错误响应
    """
    from flask import jsonify
    return jsonify({"data": None, "code": code, "msg": msg})


def buildSuccessResponse(data=None, msg="ok"):
    """
    构建成功响应
    """
    from flask import jsonify
    return jsonify({"data": data, "code": 200, "msg": msg})


def handle_error(e, code=500, msg="Internal Server Error"):
    """
    统一处理错误
    """
    import traceback
    print(f"Error: {e}")
    print(traceback.format_exc())
    from flask import jsonify
    return jsonify({"data": None, "code": code, "msg": f"{msg}: {str(e)}"})

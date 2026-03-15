import io
import re
import zlib
from functools import lru_cache

import databaseHelper
import languagePackReader
import config
import placeholderHandler
from utils.cache import search_cache


@lru_cache(maxsize=1024)
def _count_dialogue_by_talker_keyword_cached(
    speaker_keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countDialogueByTalkerKeyword(
        speaker_keyword,
        lang_code,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=1024)
def _count_dialogue_by_talker_type_cached(
    talker_type: str,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countDialogueByTalkerType(
        talker_type,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=1024)
def _count_dialogue_by_talker_and_keyword_cached(
    speaker_keyword: str,
    keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countDialogueByTalkerAndKeyword(
        speaker_keyword,
        keyword,
        lang_code,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=1024)
def _count_dialogue_by_talker_type_and_keyword_cached(
    talker_type: str,
    keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countDialogueByTalkerTypeAndKeyword(
        talker_type,
        keyword,
        lang_code,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=1024)
def _count_fetter_by_speaker_and_keyword_cached(
    speaker_keyword: str,
    keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countFetterBySpeakerAndKeyword(
        speaker_keyword,
        keyword,
        lang_code,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=2048)
def _count_textmap_from_keyword_cached(
    keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countTextMapFromKeyword(
        keyword,
        lang_code,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=2048)
def _count_textmap_from_keyword_voice_cached(
    keyword: str,
    lang_code: int,
    voice_filter: str,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countTextMapFromKeywordVoice(
        keyword,
        lang_code,
        voice_filter,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=2048)
def _count_readable_from_keyword_cached(
    keyword: str,
    lang_code: int,
    lang_str: str,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countReadableFromKeyword(
        keyword,
        lang_code,
        lang_str,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=2048)
def _count_subtitle_from_keyword_cached(
    keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countSubtitleFromKeyword(
        keyword,
        lang_code,
        created_version,
        updated_version,
    )


def pickAssetDirViaDialog() -> str | None:
    """
    弹出选择文件夹对话框（Windows/macOS/Linux）
    - 返回选中的目录路径
    - 取消返回 None

    注意：这适合桌面发行版。如果你把服务部署到远端服务器，这个对话框不会在用户电脑上出现。
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None

    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        picked = filedialog.askdirectory(title="请选择原神资源目录（包含 StreamingAssets 或 Persistent）")
        root.destroy()
        if not picked:
            return None
        return picked
    except Exception:
        return None


def selectVoicePathFromTextHash(textHash: int):
    return databaseHelper.selectVoicePathFromTextHash(textHash)


def _get_available_voice_langs(voice_path: str | None, langs: list[int] | None) -> list[int]:
    if not voice_path:
        return []

    available_langs = []
    for lang in list(dict.fromkeys(langs or [])):
        if lang not in languagePackReader.langPackages:
            continue
        if languagePackReader.checkAudioBin(voice_path, lang):
            available_langs.append(lang)
    return available_langs


def _attach_voice_metadata(obj: dict, voice_path: str | None, langs: list[int] | None) -> dict:
    obj.setdefault("voicePaths", [])
    obj.setdefault("availableVoiceLangs", [])
    if not voice_path:
        return obj

    if voice_path not in obj["voicePaths"]:
        obj["voicePaths"].append(voice_path)

    known_langs = set(obj["availableVoiceLangs"])
    for lang in _get_available_voice_langs(voice_path, langs):
        if lang not in known_langs:
            obj["availableVoiceLangs"].append(lang)
            known_langs.add(lang)
    return obj


def selectVoiceOriginFromTextHash(textHash: int, langCode: int) -> tuple[str, bool]:
    origin = databaseHelper.getSourceFromDialogue(textHash, langCode)
    if origin is not None:
        return origin, True

    origin = databaseHelper.getSourceFromFetter(textHash, langCode)
    if origin is not None:
        return origin, False

    # TODO 支持更多类型的语音
    return "其他文本", False


def queryTextHashInfo(textHash: int, langs: 'list[int]', sourceLangCode: int, queryOrigin=True):
    """
    查询文本哈希的信息
    - 获取多语言翻译
    - 查询语音路径
    - 获取版本信息
    """
    obj = {'translates': {}, 'voicePaths': [], 'availableVoiceLangs': [], 'hash': textHash}
    # 去重并添加源语言
    lang_list = list(dict.fromkeys(langs or []))
    if sourceLangCode and sourceLangCode not in lang_list:
        lang_list.append(sourceLangCode)

    # 获取翻译
    translates = databaseHelper.selectTextMapFromTextHash(textHash, lang_list)
    if not translates:
        # 回退：如果选择的语言没有翻译，返回至少一种可用语言
        translates = databaseHelper.selectTextMapFromTextHash(textHash, None)
    for translate in translates:
        obj['translates'][str(translate[1])] = _normalize_text_map_content(translate[0], translate[1])

    # 查询来源信息
    if queryOrigin:
        origin, isTalk = selectVoiceOriginFromTextHash(textHash, sourceLangCode)
        obj['isTalk'] = isTalk
        obj['origin'] = origin

    # 查询语音路径
    voicePath = selectVoicePathFromTextHash(textHash)
    _attach_voice_metadata(obj, voicePath, lang_list)

    # 添加版本信息
    created_raw, updated_raw = databaseHelper.getTextMapVersionInfo(textHash, sourceLangCode)
    obj.update(_build_version_fields(created_raw, updated_raw))
    return obj


def _resolve_avatar_query_langs(search_lang: int | None = None) -> tuple[list[int], int, int]:
    """
    解析角色查询的语言设置
    - 获取结果语言列表
    - 添加搜索语言（如果不在结果语言列表中）
    - 确定源语言和关键词语言
    """
    langs = config.getResultLanguages().copy()
    if search_lang and search_lang not in langs:
        langs.append(search_lang)
    source_lang_code = config.getSourceLanguage()
    keyword_lang_code = search_lang if search_lang else source_lang_code
    return langs, source_lang_code, keyword_lang_code


def _normalize_text_map_content(content: str | None, lang_code: int):
    if content is None:
        return None
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


def _build_text_map_translates(text_hash: int | None, langs: 'list[int]'):
    if not text_hash:
        return None
    translates = databaseHelper.selectTextMapFromTextHash(text_hash, langs)
    if not translates:
        return None
    result = {}
    for content, lang_code in translates:
        result[str(lang_code)] = _normalize_text_map_content(content, lang_code)
    return result if result else None


def _build_lang_str_to_id_map() -> dict[str, int]:
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


def _normalize_speaker(value: str, langCode: int) -> str:
    """
    标准化说话者名称
    """
    normalized = value.strip().lower()
    if langCode in databaseHelper.CHINESE_LANG_CODES:
        normalized = "".join(normalized.split())
    return normalized


def _normalize_match_text(value: str | None, langCode: int) -> str:
    text = str(value or "").strip()
    if langCode in databaseHelper.CHINESE_LANG_CODES:
        text = "".join(text.split())
    return text.lower()


def _match_rank(value: str | None, keyword: str, langCode: int) -> int:
    normalized_keyword = _normalize_match_text(keyword, langCode)
    if not normalized_keyword:
        return 0
    normalized_value = _normalize_match_text(value, langCode)
    if not normalized_value:
        return 3
    if normalized_value == normalized_keyword:
        return 0
    if normalized_value.startswith(normalized_keyword):
        return 1
    if normalized_keyword in normalized_value:
        return 2
    return 3


def _best_field_match(values: list[str | None], keyword: str, langCode: int) -> tuple[int, int, int]:
    best = (3, len(values), 10**9)
    for index, value in enumerate(values):
        normalized_value = _normalize_match_text(value, langCode)
        rank = _match_rank(value, keyword, langCode)
        candidate = (rank, index, len(normalized_value))
        if candidate < best:
            best = candidate
    return best


def _sort_entries_by_match(
    entries: list[dict],
    keyword: str,
    langCode: int,
    field_getter,
):
    keyword_trim = (keyword or "").strip()
    if not keyword_trim:
        return entries

    def sort_key(entry: dict):
        return _best_field_match(field_getter(entry), keyword_trim, langCode)

    entries.sort(key=sort_key)
    return entries


def _coerce_sort_int(value, fallback: int = 10**12) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _sort_entries_by_match_with_exact_id(
    entries: list[dict],
    keyword: str,
    langCode: int,
    field_getter,
    id_getter,
    stable_getter=None,
):
    keyword_trim = (keyword or "").strip()
    if not keyword_trim:
        return entries

    def sort_key(entry: dict):
        match_key = _best_field_match(field_getter(entry), keyword_trim, langCode)
        stable_value = stable_getter(entry) if stable_getter else ""
        if match_key[0] == 0:
            return (0, _coerce_sort_int(id_getter(entry)), match_key[1], match_key[2], stable_value)
        return (1, match_key[0], match_key[1], match_key[2], stable_value)

    entries.sort(key=sort_key)
    return entries


def _apply_voice_filter(entries: list[dict], voice_filter: str) -> list[dict]:
    """
    应用语音过滤
    """
    if voice_filter == "with":
        return [entry for entry in entries if len(entry.get('voicePaths', [])) > 0]
    if voice_filter == "without":
        return [entry for entry in entries if len(entry.get('voicePaths', [])) == 0]
    return entries


def _paginate(entries: list[dict], page: int, page_size: int, total: int | None = None) -> tuple[list[dict], int]:
    """
    分页处理
    """
    total_count = total if total is not None else len(entries)
    safe_page = page if page and page > 0 else 1
    safe_size = page_size if page_size and page_size > 0 else 50
    start = (safe_page - 1) * safe_size
    end = start + safe_size
    return entries[start:end], total_count


def _handle_speaker_only_query(speaker_keyword: str, langCode: int, page: int, page_size: int, voice_filter: str, created_version_filter: str | None, updated_version_filter: str | None) -> tuple[list[dict], int]:
    """
    处理仅说话者查询
    """
    ans = []
    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    seen_hashes = set()
    speaker_norm = _normalize_speaker(speaker_keyword, langCode)

    # 查询对话
    dialogue_rows = databaseHelper.selectDialogueByTalkerKeyword(
        speaker_keyword,
        langCode,
        page * page_size * 3,
        created_version_filter,
        updated_version_filter,
    )
    for textHash, talkerType, talkerId, _dialogueId in dialogue_rows:
        if textHash in seen_hashes:
            continue
        seen_hashes.add(textHash)
        obj = queryTextHashInfo(textHash, langs, sourceLangCode)
        obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
        ans.append(obj)

    total = _count_dialogue_by_talker_keyword_cached(
        speaker_keyword,
        langCode,
        created_version_filter,
        updated_version_filter,
    )

    # 查询特殊说话者类型
    for talkerType in ("TALK_ROLE_PLAYER", "TALK_ROLE_MATE_AVATAR"):
        talkerName = databaseHelper.getTalkerName(talkerType, 0, langCode)
        if not talkerName:
            continue
        talker_norm = _normalize_speaker(talkerName, langCode)
        if speaker_norm not in talker_norm:
            continue
        talker_rows = databaseHelper.selectDialogueByTalkerType(
            talkerType,
            page * page_size * 3,
            created_version_filter,
            updated_version_filter,
        )
        for textHash, talkerType, talkerId, _dialogueId in talker_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
            ans.append(obj)
        total += _count_dialogue_by_talker_type_cached(
            talkerType,
            created_version_filter,
            updated_version_filter,
        )

    ans = _apply_voice_filter(ans, voice_filter)
    _sort_entries_by_match(
        ans,
        speaker_keyword,
        langCode,
        lambda entry: [entry.get('talker')],
    )
    return _paginate(ans, page, page_size, total)


def _handle_speaker_and_keyword_query(speaker_keyword: str, keyword_trim: str, langCode: int, page: int, page_size: int, voice_filter: str, created_version_filter: str | None, updated_version_filter: str | None) -> tuple[list[dict], int]:
    """
    处理说话者和关键词查询
    """
    ans = []
    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    seen_hashes = set()

    # 查询对话
    dialogue_rows = databaseHelper.selectDialogueByTalkerAndKeyword(
        speaker_keyword,
        keyword_trim,
        langCode,
        page * page_size * 3,
        created_version_filter,
        updated_version_filter,
    )
    for textHash, talkerType, talkerId, _dialogueId in dialogue_rows:
        if textHash in seen_hashes:
            continue
        seen_hashes.add(textHash)
        obj = queryTextHashInfo(textHash, langs, sourceLangCode)
        obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, langCode)
        ans.append(obj)

    total = _count_dialogue_by_talker_and_keyword_cached(
        speaker_keyword,
        keyword_trim,
        langCode,
        created_version_filter,
        updated_version_filter,
    )

    # 查询特殊说话者类型
    speaker_norm = _normalize_speaker(speaker_keyword, langCode)
    for talkerType in ("TALK_ROLE_PLAYER", "TALK_ROLE_MATE_AVATAR"):
        talkerName = databaseHelper.getTalkerName(talkerType, 0, langCode)
        if not talkerName:
            continue
        talker_norm = _normalize_speaker(talkerName, langCode)
        if speaker_norm not in talker_norm:
            continue
        talker_rows = databaseHelper.selectDialogueByTalkerTypeAndKeyword(
            talkerType,
            keyword_trim,
            langCode,
            page * page_size * 3,
            created_version_filter,
            updated_version_filter,
        )
        for textHash, talkerType, talkerId, _dialogueId in talker_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, langCode)
            ans.append(obj)
        total += _count_dialogue_by_talker_type_and_keyword_cached(
            talkerType,
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
        )

    # 查询角色语音
    fetter_rows = databaseHelper.selectFetterBySpeakerAndKeyword(
        speaker_keyword,
        keyword_trim,
        langCode,
        page * page_size * 3,
        created_version_filter,
        updated_version_filter,
    )
    for textHash, avatarId in fetter_rows:
        if textHash in seen_hashes:
            continue
        seen_hashes.add(textHash)
        obj = queryTextHashInfo(textHash, langs, sourceLangCode)
        obj['talker'] = databaseHelper.getCharterName(avatarId, langCode)
        ans.append(obj)
    total += _count_fetter_by_speaker_and_keyword_cached(
        speaker_keyword,
        keyword_trim,
        langCode,
        created_version_filter,
        updated_version_filter,
    )

    ans = _apply_voice_filter(ans, voice_filter)
    ans.sort(
        key=lambda entry: (
            _best_field_match(
                [entry.get('translates', {}).get(str(langCode))],
                keyword_trim,
                langCode,
            ),
            _best_field_match([entry.get('talker')], speaker_keyword, langCode),
            0 if entry.get('voicePaths') else 1,
        )
    )
    return _paginate(ans, page, page_size, total)


def _handle_keyword_only_query(keyword: str, keyword_trim: str, langCode: int, page: int, page_size: int, voice_filter: str, created_version_filter: str | None, updated_version_filter: str | None) -> tuple[list[dict], int]:
    """
    处理仅关键词查询
    """
    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    hash_value = _parse_int_keyword(keyword_trim)
    is_hash_query = hash_value is not None
    text_hashes_seen = set()
    hash_obj = None
    hash_extra = False

    # 处理哈希查询
    if hash_value is not None:
        hash_obj = queryTextHashInfo(hash_value, langs, sourceLangCode)
        if hash_obj.get('translates'):
            hash_obj['hashMatch'] = True
            if _entry_version_match(hash_obj, created_version_filter, updated_version_filter):
                hash_extra = not databaseHelper.isTextMapHashInKeyword(
                    hash_value, keyword_trim, langCode
                )
            else:
                hash_obj = None

    langMap = databaseHelper.getLangCodeMap()
    langStr = langMap.get(langCode)
    targetLangStrs = []
    for result_lang in langs:
        if result_lang in langMap:
            targetLangStrs.append(langMap[result_lang])
    strToLangId = _build_lang_str_to_id_map()
    prefix_labels = {
        "Book": "书籍",
        "Costume": "衣装",
        "Relic": "圣遗物",
        "Weapon": "武器",
        "Wings": "风之翼",
    }

    safe_page = page if page and page > 0 else 1
    safe_size = page_size if page_size and page > 0 else 50

    if voice_filter == "all":
        return _handle_all_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter)
    else:
        return _handle_specific_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, voice_filter, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter)


def _handle_all_voice_filter(keyword, keyword_trim, langCode, safe_page, safe_size, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter):
    """
    处理所有语音过滤
    """
    # 计算总数
    total_textmap = _count_textmap_from_keyword_cached(
        keyword,
        langCode,
        created_version_filter,
        updated_version_filter,
    )
    total_readable = 0
    if langStr:
        total_readable = _count_readable_from_keyword_cached(
            keyword,
            langCode,
            langStr,
            created_version_filter,
            updated_version_filter,
        )
    total_subtitle = _count_subtitle_from_keyword_cached(
        keyword,
        langCode,
        created_version_filter,
        updated_version_filter,
    )
    total = total_textmap + total_readable + total_subtitle + (1 if hash_extra else 0)

    ans = []
    offset = (safe_page - 1) * safe_size
    remaining = safe_size

    # 处理哈希结果
    if hash_extra and offset == 0 and hash_obj is not None:
        ans.append(hash_obj)
        text_hashes_seen.add(hash_value)
        remaining -= 1

    offset_after_hash = offset - (1 if hash_extra else 0)
    if offset_after_hash < 0:
        offset_after_hash = 0

    # 处理文本映射结果
    if remaining > 0 and offset_after_hash < total_textmap:
        rows = databaseHelper.selectTextMapFromKeywordPaged(
            keyword,
            langCode,
            remaining,
            offset_after_hash,
            hash_value if is_hash_query else None,
            None,
            created_version_filter,
            updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            obj = queryTextHashInfo(text_hash, langs, sourceLangCode)
            ans.append(obj)
        remaining = safe_size - len(ans)
        offset_after_hash = 0
    else:
        offset_after_hash -= total_textmap

    # 处理可读内容结果
    if remaining > 0 and langStr and offset_after_hash < total_readable:
        readableContents = databaseHelper.selectReadableFromKeyword(
            keyword,
            langCode,
            langStr,
            remaining,
            offset_after_hash,
            created_version_filter,
            updated_version_filter,
        )
        for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readableContents:
            ans.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels))
        remaining = safe_size - len(ans)
        offset_after_hash = 0
    else:
        offset_after_hash -= total_readable

    # 处理字幕结果
    if remaining > 0 and offset_after_hash < total_subtitle:
        subtitleContents = databaseHelper.selectSubtitleFromKeyword(
            keyword,
            langCode,
            remaining,
            offset_after_hash,
            created_version_filter,
            updated_version_filter,
        )
        for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitleContents:
            ans.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))

    return ans, total


def _handle_specific_voice_filter(keyword, keyword_trim, langCode, safe_page, safe_size, voice_filter, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter):
    """
    处理特定语音过滤
    """
    # 处理哈希结果
    hash_extra_filtered = False
    if hash_extra and hash_obj is not None:
        hash_has_voice = databaseHelper.hasVoiceForTextHashDb(hash_value)
        if (voice_filter == "with" and hash_has_voice) or (
            voice_filter == "without" and not hash_has_voice
        ):
            hash_extra_filtered = True

    # 计算总数
    total_textmap = _count_textmap_from_keyword_voice_cached(
        keyword,
        langCode,
        voice_filter,
        created_version_filter,
        updated_version_filter,
    )
    total_readable = 0
    total_subtitle = 0
    if voice_filter == "without":
        if langStr:
            total_readable = _count_readable_from_keyword_cached(
                keyword,
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
            )
        total_subtitle = _count_subtitle_from_keyword_cached(
            keyword,
            langCode,
            created_version_filter,
            updated_version_filter,
        )

    total = total_textmap + total_readable + total_subtitle + (1 if hash_extra_filtered else 0)

    ans = []
    offset = (safe_page - 1) * safe_size
    remaining = safe_size

    # 处理哈希结果
    if hash_extra_filtered and offset == 0 and hash_obj is not None:
        ans.append(hash_obj)
        remaining -= 1

    offset_after_hash = offset - (1 if hash_extra_filtered else 0)
    if offset_after_hash < 0:
        offset_after_hash = 0

    # 处理文本映射结果
    if remaining > 0 and offset_after_hash < total_textmap:
        rows = databaseHelper.selectTextMapFromKeywordPaged(
            keyword,
            langCode,
            remaining,
            offset_after_hash,
            hash_value if is_hash_query else None,
            voice_filter,
            created_version_filter,
            updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            obj = queryTextHashInfo(text_hash, langs, sourceLangCode)
            ans.append(obj)
        remaining = safe_size - len(ans)
        offset_after_hash = 0
    else:
        offset_after_hash -= total_textmap

    # 处理无语音的可读内容和字幕
    if voice_filter == "without":
        if remaining > 0 and langStr and offset_after_hash < total_readable:
            readableContents = databaseHelper.selectReadableFromKeyword(
                keyword,
                langCode,
                langStr,
                remaining,
                offset_after_hash,
                created_version_filter,
                updated_version_filter,
            )
            for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readableContents:
                ans.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels))
            remaining = safe_size - len(ans)
            offset_after_hash = 0
        else:
            offset_after_hash -= total_readable

        if remaining > 0 and offset_after_hash < total_subtitle:
            subtitleContents = databaseHelper.selectSubtitleFromKeyword(
                keyword,
                langCode,
                remaining,
                offset_after_hash,
                created_version_filter,
                updated_version_filter,
            )
            for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitleContents:
                ans.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))

    return ans, total


def _handle_all_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter):
    total_textmap = _count_textmap_from_keyword_cached(
        keyword,
        langCode,
        created_version_filter,
        updated_version_filter,
    )
    total_readable = 0
    if langStr:
        total_readable = _count_readable_from_keyword_cached(
            keyword,
            langCode,
            langStr,
            created_version_filter,
            updated_version_filter,
        )
    total_subtitle = _count_subtitle_from_keyword_cached(
        keyword,
        langCode,
        created_version_filter,
        updated_version_filter,
    )
    total = total_textmap + total_readable + total_subtitle + (1 if hash_extra else 0)

    candidate_limit = max(safe_size, safe_page * safe_size)
    candidates = []

    if hash_extra and hash_obj is not None:
        candidates.append(hash_obj)
        if hash_value is not None:
            text_hashes_seen.add(hash_value)

    rows = databaseHelper.selectTextMapFromKeywordPaged(
        keyword,
        langCode,
        candidate_limit,
        0,
        hash_value if is_hash_query else None,
        None,
        created_version_filter,
        updated_version_filter,
    )
    for text_hash, _content, _created_raw, _updated_raw in rows:
        if text_hash in text_hashes_seen:
            continue
        text_hashes_seen.add(text_hash)
        candidates.append(queryTextHashInfo(text_hash, langs, sourceLangCode))

    if langStr:
        readable_contents = databaseHelper.selectReadableFromKeyword(
            keyword,
            langCode,
            langStr,
            candidate_limit,
            None,
            created_version_filter,
            updated_version_filter,
        )
        for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readable_contents:
            candidates.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels))

    subtitle_contents = databaseHelper.selectSubtitleFromKeyword(
        keyword,
        langCode,
        candidate_limit,
        None,
        created_version_filter,
        updated_version_filter,
    )
    for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitle_contents:
        candidates.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))

    _sort_search_results(candidates, keyword_trim, langCode, is_hash_query, hash_value)
    start = (safe_page - 1) * safe_size
    end = start + safe_size
    return candidates[start:end], total


def _handle_specific_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, voice_filter, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter):
    hash_extra_filtered = False
    if hash_extra and hash_obj is not None:
        hash_has_voice = databaseHelper.hasVoiceForTextHashDb(hash_value)
        if (voice_filter == "with" and hash_has_voice) or (
            voice_filter == "without" and not hash_has_voice
        ):
            hash_extra_filtered = True

    total_textmap = _count_textmap_from_keyword_voice_cached(
        keyword,
        langCode,
        voice_filter,
        created_version_filter,
        updated_version_filter,
    )
    total_readable = 0
    total_subtitle = 0
    if voice_filter == "without":
        if langStr:
            total_readable = _count_readable_from_keyword_cached(
                keyword,
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
            )
        total_subtitle = _count_subtitle_from_keyword_cached(
            keyword,
            langCode,
            created_version_filter,
            updated_version_filter,
        )

    total = total_textmap + total_readable + total_subtitle + (1 if hash_extra_filtered else 0)
    candidate_limit = max(safe_size, safe_page * safe_size)
    candidates = []

    if hash_extra_filtered and hash_obj is not None:
        candidates.append(hash_obj)
        if hash_value is not None:
            text_hashes_seen.add(hash_value)

    rows = databaseHelper.selectTextMapFromKeywordPaged(
        keyword,
        langCode,
        candidate_limit,
        0,
        hash_value if is_hash_query else None,
        voice_filter,
        created_version_filter,
        updated_version_filter,
    )
    for text_hash, _content, _created_raw, _updated_raw in rows:
        if text_hash in text_hashes_seen:
            continue
        text_hashes_seen.add(text_hash)
        candidates.append(queryTextHashInfo(text_hash, langs, sourceLangCode))

    if voice_filter == "without":
        if langStr:
            readable_contents = databaseHelper.selectReadableFromKeyword(
                keyword,
                langCode,
                langStr,
                candidate_limit,
                None,
                created_version_filter,
                updated_version_filter,
            )
            for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readable_contents:
                candidates.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels))

        subtitle_contents = databaseHelper.selectSubtitleFromKeyword(
            keyword,
            langCode,
            candidate_limit,
            None,
            created_version_filter,
            updated_version_filter,
        )
        for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitle_contents:
            candidates.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))

    _sort_search_results(candidates, keyword_trim, langCode, is_hash_query, hash_value)
    start = (safe_page - 1) * safe_size
    end = start + safe_size
    return candidates[start:end], total


def _build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels):
    """
    构建可读内容对象
    """
    fileHash = zlib.crc32(fileName.encode('utf-8'))
    origin_label = "阅读物"
    for prefix, label in prefix_labels.items():
        if fileName.startswith(prefix):
            origin_label = label
            break

    title = _get_text_map_content_with_fallback(
        titleTextMapHash,
        sourceLangCode,
        [langCode],
    )
    origin = f"{origin_label}: {title}" if title else f"{origin_label}: {fileName}"

    obj = {
        'translates': {},
        'voicePaths': [],
        'hash': fileHash,
        'isTalk': False,
        'isReadable': True,
        'readableId': readableId,
        'fileName': fileName,
        'origin': origin
    }
    obj.update(_build_version_fields(created_raw, updated_raw))

    translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)
    for transContent, transLangStr in translations:
        if transLangStr in strToLangId:
            lang_id = strToLangId[transLangStr]
            obj['translates'][str(lang_id)] = _normalize_text_map_content(transContent, lang_id)

    return obj


def _build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs):
    """
    构建字幕对象
    """
    fileHash = zlib.crc32(f"{fileName}_{startTime}".encode('utf-8'))
    origin = f"字幕: {fileName}"
    obj = {
        'translates': {},
        'voicePaths': [],
        'availableVoiceLangs': [],
        'hash': fileHash,
        'isTalk': False,
        'origin': origin,
        'isSubtitle': True,
        'fileName': fileName,
        'subtitleId': subtitleId
    }
    obj.update(_build_version_fields(created_raw, updated_raw))
    translations = []
    if subtitleId:
        translations = databaseHelper.selectSubtitleTranslationsBySubtitleId(subtitleId, startTime, langs)
    if not translations:
        translations = databaseHelper.selectSubtitleTranslations(fileName, startTime, langs)
    for transContent, transLangCode in translations:
        obj['translates'][str(transLangCode)] = _normalize_text_map_content(transContent, transLangCode)
    return obj


def _sort_search_results(ans: list[dict], keyword: str, langCode: int, is_hash_query: bool, hash_value: int | None):
    """
    排序搜索结果
    """
    def sort_key(entry: dict) -> tuple[int, int, int, int, int]:
        target_text = entry.get('translates', {}).get(str(langCode))
        hash_rank = 0 if (is_hash_query and entry.get('hash') == hash_value) else 1
        match_rank = _match_rank(target_text, keyword, langCode)
        voice_rank = 0 if entry.get('voicePaths') else 1
        talk_rank = 0 if entry.get('isTalk') else 1
        target_len = len(_normalize_match_text(target_text, langCode))
        return (
            hash_rank,
            match_rank,
            voice_rank,
            talk_rank,
            target_len,
        )

    ans.sort(key=sort_key)
    return ans


def getTranslateObj(
    keyword: str,
    langCode: int,
    speaker: str | None = None,
    page: int = 1,
    page_size: int = 50,
    voice_filter: str = "all",
    created_version: str | None = None,
    updated_version: str | None = None,
):
    """
    获取翻译对象
    """
    speaker_keyword = (speaker or "").strip()
    keyword_trim = keyword.strip()
    created_version_filter = _normalize_version_filter(created_version)
    updated_version_filter = _normalize_version_filter(updated_version)

    # 生成缓存键
    cache_key = (
        keyword_trim,
        langCode,
        speaker_keyword,
        page,
        page_size,
        voice_filter,
        created_version_filter,
        updated_version_filter
    )

    # 尝试从缓存中获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return cached_result

    # 使用原有的查询逻辑
    if keyword_trim == "" and speaker_keyword:
        # 仅说话者查询
        result = _handle_speaker_only_query(speaker_keyword, langCode, page, page_size, voice_filter, created_version_filter, updated_version_filter)
    elif keyword_trim != "" and speaker_keyword:
        # 说话者和关键词查询
        result = _handle_speaker_and_keyword_query(speaker_keyword, keyword_trim, langCode, page, page_size, voice_filter, created_version_filter, updated_version_filter)
    else:
        # 仅关键词查询
        result = _handle_keyword_only_query(keyword, keyword_trim, langCode, page, page_size, voice_filter, created_version_filter, updated_version_filter)

    # 将结果缓存
    search_cache.set(cache_key, result)
    return result


def searchNameEntries(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
):
    quests = []
    readables = []
    keyword_trim = (keyword or "").strip()
    created_version_filter = _normalize_version_filter(created_version)
    updated_version_filter = _normalize_version_filter(updated_version)
    if not keyword_trim and not created_version_filter and not updated_version_filter:
        return {
            "quests": [],
            "readables": []
        }

    def format_chapter_name(chapter_num: str | None, chapter_title: str | None):
        if chapter_num and chapter_title:
            return '{} · {}'.format(chapter_num, chapter_title)
        if chapter_title:
            return chapter_title
        if chapter_num:
            return chapter_num
        return None
    quest_map = {}
    if keyword_trim:
        questMatches = databaseHelper.selectQuestByTitleKeyword(
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
        )
        for questId, questTitle, created_raw, updated_raw in questMatches:
            chapterName = databaseHelper.getQuestChapterName(questId, langCode)
            quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_version_fields(created_raw, updated_raw),
            }

        chapterMatches = databaseHelper.selectQuestByChapterKeyword(
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
        )
        for questId, questTitle, chapterTitle, chapterNum, created_raw, updated_raw in chapterMatches:
            if questId in quest_map:
                continue
            chapterName = format_chapter_name(chapterNum, chapterTitle)
            quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_version_fields(created_raw, updated_raw),
            }
        questIdMatches = databaseHelper.selectQuestByIdContains(
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
        )
        for questId, questTitle, created_raw, updated_raw in questIdMatches:
            if questId in quest_map:
                continue
            chapterName = databaseHelper.getQuestChapterName(questId, langCode)
            quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_version_fields(created_raw, updated_raw),
            }
    else:
        questMatches = databaseHelper.selectQuestByVersion(
            langCode,
            created_version_filter,
            updated_version_filter,
        )
        for questId, questTitle, created_raw, updated_raw in questMatches:
            chapterName = databaseHelper.getQuestChapterName(questId, langCode)
            quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_version_fields(created_raw, updated_raw),
            }

    quests.extend(quest_map.values())

    langMap = databaseHelper.getLangCodeMap()
    if langCode in langMap:
        langStr = langMap[langCode]
        readable_seen = set()
        if keyword_trim:
            readableMatches = databaseHelper.selectReadableByTitleKeyword(
                keyword_trim,
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
            )
            for fileName, readableId, titleTextMapHash, title, created_raw, updated_raw in readableMatches:
                resolved_hash = titleTextMapHash
                if resolved_hash is None:
                    resolved_hash = databaseHelper.resolveReadableTitleHash(readableId, fileName)
                resolved_title = title or _get_text_map_content_with_fallback(
                    resolved_hash,
                    langCode,
                    [config.getSourceLanguage()],
                )
                readables.append({
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": resolved_title or fileName,
                    "titleTextMapHash": resolved_hash,
                    **_build_version_fields(created_raw, updated_raw),
                })
                readable_seen.add((readableId, fileName))
            readableFileMatches = databaseHelper.selectReadableByFileNameContains(
                keyword_trim,
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
            )
            for fileName, readableId, titleTextMapHash, title, created_raw, updated_raw in readableFileMatches:
                key = (readableId, fileName)
                if key in readable_seen:
                    continue
                readable_seen.add(key)
                resolved_hash = titleTextMapHash
                if resolved_hash is None:
                    resolved_hash = databaseHelper.resolveReadableTitleHash(readableId, fileName)
                resolved_title = title or _get_text_map_content_with_fallback(
                    resolved_hash,
                    langCode,
                    [config.getSourceLanguage()],
                )
                readables.append({
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": resolved_title or fileName,
                    "titleTextMapHash": resolved_hash,
                    **_build_version_fields(created_raw, updated_raw),
                })
            readableContentMatches = databaseHelper.selectReadableFromKeyword(
                keyword_trim,
                langCode,
                langStr,
                limit=200,
                offset=None,
                created_version=created_version_filter,
                updated_version=updated_version_filter,
            )
            for fileName, _content, titleTextMapHash, readableId, created_raw, updated_raw in readableContentMatches:
                key = (readableId, fileName)
                if key in readable_seen:
                    continue
                readable_seen.add(key)
                resolved_hash = titleTextMapHash
                if resolved_hash is None:
                    resolved_hash = databaseHelper.resolveReadableTitleHash(readableId, fileName)
                title = _get_text_map_content_with_fallback(
                    resolved_hash,
                    langCode,
                    [config.getSourceLanguage()],
                )
                readables.append({
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": title or fileName,
                    "titleTextMapHash": resolved_hash,
                    **_build_version_fields(created_raw, updated_raw),
                })
        else:
            readableMatches = databaseHelper.selectReadableByVersion(
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
            )
            for fileName, readableId, titleTextMapHash, title, created_raw, updated_raw in readableMatches:
                resolved_hash = titleTextMapHash
                if resolved_hash is None:
                    resolved_hash = databaseHelper.resolveReadableTitleHash(readableId, fileName)
                resolved_title = title or _get_text_map_content_with_fallback(
                    resolved_hash,
                    langCode,
                    [config.getSourceLanguage()],
                )
                readables.append({
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": resolved_title or fileName,
                    "titleTextMapHash": resolved_hash,
                    **_build_version_fields(created_raw, updated_raw),
                })

    _sort_entries_by_match_with_exact_id(
        quests,
        keyword_trim,
        langCode,
        lambda entry: [
            entry.get("title"),
            entry.get("chapterName"),
            str(entry.get("questId", "")),
        ],
        lambda entry: entry.get("questId"),
        lambda entry: str(entry.get("questId", "")),
    )
    _sort_entries_by_match_with_exact_id(
        readables,
        keyword_trim,
        langCode,
        lambda entry: [entry.get("title"), entry.get("fileName")],
        lambda entry: entry.get("readableId"),
        lambda entry: str(entry.get("fileName", "")),
    )

    return {
        "quests": quests,
        "readables": readables
    }


def searchAvatarEntries(keyword: str, langCode: int):
    avatars = []
    matches = databaseHelper.selectAvatarByNameKeyword(keyword, langCode)
    for avatarId, avatarName in matches:
        avatars.append({
            "avatarId": avatarId,
            "name": avatarName
        })
    _sort_entries_by_match(
        avatars,
        keyword,
        langCode,
        lambda entry: [entry.get("name")],
    )
    return {
        "avatars": avatars
    }


def getAvatarVoices(avatarId: int, searchLang: int | None = None):
    langs, sourceLangCode, _keywordLangCode = _resolve_avatar_query_langs(searchLang)

    avatarName = databaseHelper.getCharterName(avatarId, sourceLangCode)
    voice_rows = databaseHelper.selectAvatarVoiceItems(avatarId)

    voices = []
    seen_hashes = set()

    for titleHash, textHash, voicePath in voice_rows:
        if textHash is None:
            continue
        if textHash in seen_hashes:
            continue
        seen_hashes.add(textHash)

        obj = queryTextHashInfo(textHash, langs, sourceLangCode)
        obj['isTalk'] = False
        obj['viewAsTextHash'] = True
        obj['disableDetail'] = True
        if avatarName:
            if titleHash:
                title = _get_text_map_content_with_fallback(titleHash, sourceLangCode, langs)
                if title:
                    obj['origin'] = f"{avatarName} · {title}"
                    obj['voiceTitle'] = title
                else:
                    obj['origin'] = avatarName
                    obj['voiceTitle'] = ""
            else:
                obj['origin'] = avatarName
                obj['voiceTitle'] = ""
        else:
            obj['voiceTitle'] = ""
        created_raw, updated_raw = databaseHelper.getTextMapVersionInfo(textHash, sourceLangCode)
        obj.update(_build_version_fields(created_raw, updated_raw))

        _attach_voice_metadata(obj, voicePath, langs)

        voices.append(obj)

    return {
        "avatarId": avatarId,
        "avatarName": avatarName,
        "voices": voices
    }


def getAvatarStories(avatarId: int, searchLang: int | None = None):
    langs, sourceLangCode, _keywordLangCode = _resolve_avatar_query_langs(searchLang)

    avatarName = databaseHelper.getCharterName(avatarId, sourceLangCode)
    story_rows = databaseHelper.selectAvatarStories(avatarId)

    stories = []
    version_cache: dict[int | None, tuple[str | None, str | None]] = {}

    for (fetterId,
         storyTitleHash,
         storyTitle2Hash,
         storyTitleLockedHash,
         storyContextHash,
         storyContext2Hash) in story_rows:

        per_story_seen = set()
        for context_hash, title_hash in (
            (storyContextHash, storyTitleHash),
            (storyContext2Hash, storyTitle2Hash),
        ):
            translates = _build_text_map_translates(context_hash, langs)
            if not translates:
                continue

            source_text = (translates.get(str(sourceLangCode)) or "").strip()
            if source_text:
                if source_text in per_story_seen:
                    continue
                per_story_seen.add(source_text)

            title = None
            if title_hash:
                title = _get_text_map_content_with_fallback(title_hash, sourceLangCode, langs)
            if not title and storyTitleLockedHash:
                title = _get_text_map_content_with_fallback(storyTitleLockedHash, sourceLangCode, langs)

            if avatarName and title:
                origin = f"{avatarName} · {title}"
            elif avatarName:
                origin = avatarName
            elif title:
                origin = title
            else:
                origin = "角色故事"

            if context_hash in version_cache:
                created_raw, updated_raw = version_cache[context_hash]
            else:
                version_info = databaseHelper.getTextMapVersionInfo(context_hash, sourceLangCode)
                if version_info:
                    created_raw, updated_raw = version_info
                else:
                    created_raw, updated_raw = None, None
                version_cache[context_hash] = (created_raw, updated_raw)

                stories.append({
                    "translates": translates,
                    "voicePaths": [],
                    "availableVoiceLangs": [],
                    "hash": context_hash,
                    "isTalk": False,
                    "viewAsTextHash": True,
                    "disableDetail": True,
                "origin": origin,
                "storyTitle": title or "",
                "fetterId": fetterId,
                "avatarName": avatarName or "",
                **_build_version_fields(created_raw, updated_raw),
            })

    return {
        "avatarId": avatarId,
        "avatarName": avatarName,
        "stories": stories
    }





def searchAvatarVoicesByFilters(
    title_keyword: str | None = None,
    searchLang: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
    limit: int = 1200,
):
    langs, sourceLangCode, keywordLangCode = _resolve_avatar_query_langs(searchLang)

    voice_rows = databaseHelper.selectAvatarVoiceItemsByFilters(
        title_keyword,
        keywordLangCode,
        limit=limit,
        created_version=created_version,
        updated_version=updated_version,
        version_lang_code=sourceLangCode,
    )

    voices = []
    seen_keys = set()
    avatar_name_cache: dict[int, str | None] = {}
    title_cache: dict[int, str | None] = {}

    for avatarId, titleHash, textHash, voicePath in voice_rows:
        if textHash is None:
            continue
        dedupe_key = (avatarId, textHash)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        if avatarId not in avatar_name_cache:
            avatar_name_cache[avatarId] = databaseHelper.getCharterName(avatarId, sourceLangCode)
        avatarName = avatar_name_cache[avatarId]

        if titleHash and titleHash not in title_cache:
            title_cache[titleHash] = _normalize_text_map_content(
                databaseHelper.getTextMapContent(titleHash, sourceLangCode),
                sourceLangCode,
            )
        title = title_cache.get(titleHash) if titleHash else None

        obj = queryTextHashInfo(textHash, langs, sourceLangCode)
        obj['isTalk'] = False
        obj['viewAsTextHash'] = True
        obj['disableDetail'] = True
        if avatarName and title:
            obj['origin'] = f"{avatarName} · {title}"
            obj['voiceTitle'] = title
        elif avatarName:
            obj['origin'] = avatarName
            obj['voiceTitle'] = ""
        elif title:
            obj['origin'] = title
            obj['voiceTitle'] = title
        else:
            obj['origin'] = "角色语音"
            obj['voiceTitle'] = ""
        created_raw, updated_raw = databaseHelper.getTextMapVersionInfo(textHash, sourceLangCode)
        obj.update(_build_version_fields(created_raw, updated_raw))

        obj['avatarId'] = avatarId
        obj['avatarName'] = avatarName or ""

        _attach_voice_metadata(obj, voicePath, langs)

        voices.append(obj)

    _sort_entries_by_match(
        voices,
        title_keyword or "",
        keywordLangCode,
        lambda entry: [
            entry.get("voiceTitle"),
            entry.get("translates", {}).get(str(keywordLangCode)),
        ],
    )
    return {
        "voices": voices
    }


def searchAvatarStoriesByFilters(
    title_keyword: str | None = None,
    searchLang: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
    limit: int = 1600,
):
    langs, sourceLangCode, keywordLangCode = _resolve_avatar_query_langs(searchLang)

    story_rows = databaseHelper.selectAvatarStoryItemsByFilters(
        title_keyword,
        keywordLangCode,
        limit=limit,
        created_version=created_version,
        updated_version=updated_version,
        version_lang_code=sourceLangCode,
    )

    stories = []
    seen_keys = set()
    version_cache: dict[int | None, tuple[str | None, str | None]] = {}
    avatar_name_cache: dict[int, str | None] = {}
    title_cache: dict[int, str | None] = {}

    for avatarId, fetterId, titleHash, lockedTitleHash, contextHash in story_rows:
        if contextHash is None:
            continue
        dedupe_key = (avatarId, contextHash)
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)

        translates = _build_text_map_translates(contextHash, langs)
        if not translates:
            continue

        # 确保只包含当前搜索语言的内容
        if avatarId not in avatar_name_cache:
            avatar_name_cache[avatarId] = databaseHelper.getCharterName(avatarId, sourceLangCode)
        avatarName = avatar_name_cache[avatarId]

        title = None
        if titleHash:
            if titleHash not in title_cache:
                title_cache[titleHash] = _get_text_map_content_with_fallback(
                    titleHash,
                    sourceLangCode,
                    langs,
                )
            title = title_cache.get(titleHash)
        if not title and lockedTitleHash:
            if lockedTitleHash not in title_cache:
                title_cache[lockedTitleHash] = _get_text_map_content_with_fallback(
                    lockedTitleHash,
                    sourceLangCode,
                    langs,
                )
            title = title_cache.get(lockedTitleHash)

        if avatarName and title:
            origin = f"{avatarName} · {title}"
        elif avatarName:
            origin = avatarName
        elif title:
            origin = title
        else:
            origin = "角色故事"

        if contextHash in version_cache:
            created_raw, updated_raw = version_cache[contextHash]
        else:
            version_info = databaseHelper.getTextMapVersionInfo(contextHash, sourceLangCode)
            if version_info:
                created_raw, updated_raw = version_info
            else:
                created_raw, updated_raw = None, None
            version_cache[contextHash] = (created_raw, updated_raw)

        stories.append({
            "translates": translates,
            "voicePaths": [],
            "availableVoiceLangs": [],
            "hash": contextHash,
            "isTalk": False,
            "viewAsTextHash": True,
            "disableDetail": True,
            "origin": origin,
            "storyTitle": title or "",
            "fetterId": fetterId,
            "avatarId": avatarId,
            "avatarName": avatarName or "",
            **_build_version_fields(created_raw, updated_raw),
        })

    _sort_entries_by_match(
        stories,
        title_keyword or "",
        keywordLangCode,
        lambda entry: [
            entry.get("storyTitle"),
            entry.get("translates", {}).get(str(keywordLangCode)),
        ],
    )
    return {
        "stories": stories
    }


def getQuestDialogues(
    questId: int,
    searchLang: int | None = None,
    page: int = 1,
    page_size: int = 200,
):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    questCompleteName = databaseHelper.getQuestName(questId, sourceLangCode)

    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 200

    total = databaseHelper.countQuestDialogues(questId)
    offset = (page - 1) * page_size
    rows = databaseHelper.selectQuestDialoguesPaged(questId, page_size, offset)
    dialogues = []

    for textHash, talkerType, talkerId, dialogueId, talkId in rows:
        obj = queryTextHashInfo(textHash, langs, sourceLangCode, False)
        obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
        obj['dialogueId'] = dialogueId
        obj['talkId'] = talkId
        dialogues.append(obj)

    return {
        "talkQuestName": questCompleteName,
        "talkId": 0,
        "dialogues": dialogues,
    }, total


# 根据hash值查询整个对话的内容
def getTalkFromHash(textHash: int, searchLang: int | None = None):
    # 先查到文本所属的talk，然后查询对话所属的任务的标题，然后查询对话所有的内容，对于每一句话，查询多语言翻译、说话者
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()
    if sourceLangCode and sourceLangCode not in langs:
        langs.append(sourceLangCode)

    talkInfo = databaseHelper.getTalkInfo(textHash)
    if talkInfo is None:
        # Fallback for non-dialogue text hashes, e.g. avatar story entries.
        obj = queryTextHashInfo(textHash, langs, sourceLangCode, False)
        if not obj.get("translates"):
            raise Exception("内容不属于任何对话！")
        obj['talker'] = "文本"
        obj['dialogueId'] = textHash
        return {
            "talkQuestName": "文本详情",
            "talkId": 0,
            "dialogues": [obj]
        }

    talkId, talkerType, talkerId, coopQuestId = talkInfo
    if coopQuestId is None:
        questCompleteName = databaseHelper.getTalkQuestName(talkId, sourceLangCode)
    else:
        questCompleteName = databaseHelper.getCoopTalkQuestName(coopQuestId, sourceLangCode)

    rawDialogues = databaseHelper.getTalkContent(talkId, coopQuestId)
    dialogues = []

    if rawDialogues is None:
        rawDialogues = []

    for rawDialogue in rawDialogues:
        textHash, talkerType, talkerId, dialogueId = rawDialogue
        obj = queryTextHashInfo(textHash, langs, sourceLangCode, False)
        obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
        obj['dialogueId'] = dialogueId
        dialogues.append(obj)

    # 获取版本信息
    created_raw, updated_raw = None, None
    if dialogues:
        # 使用第一个对话的版本信息
        first_dialogue = dialogues[0]
        created_raw = first_dialogue.get("createdVersionRaw")
        updated_raw = first_dialogue.get("updatedVersionRaw")

    ans = {
        "talkQuestName": questCompleteName,
        "talkId": talkId,
        "dialogues": dialogues,
        **_build_version_fields(created_raw, updated_raw),
    }

    return ans


def getReadableContent(readableId: int | None, fileName: str | None, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    langMap = databaseHelper.getLangCodeMap()
    targetLangStrs = []
    for result_lang in langs:
        if result_lang in langMap:
            targetLangStrs.append(langMap[result_lang])

    if readableId:
        readableInfo = databaseHelper.getReadableInfo(readableId, None)
    elif fileName:
        readableInfo = databaseHelper.getReadableInfo(None, fileName)
    else:
        readableInfo = None
    readableTitle = None
    created_raw = None
    updated_raw = None
    if readableInfo:
        fileName, titleTextMapHash, readableId = readableInfo
        readableTitle = _get_text_map_content_with_fallback(titleTextMapHash, sourceLangCode, langs)
        created_raw, updated_raw = databaseHelper.getReadableVersionInfo(readableId, fileName)

    translations = []
    if readableId:
        translations = databaseHelper.selectReadableFromReadableId(readableId, targetLangStrs)
    elif fileName:
        translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)

    strToLangId = _build_lang_str_to_id_map()
    translateMap = {}
    for transContent, transLangStr in translations:
        if transLangStr in strToLangId:
            lang_id = strToLangId[transLangStr]
            translateMap[str(lang_id)] = _normalize_text_map_content(transContent, lang_id)

    return {
        "fileName": fileName,
        "readableId": readableId,
        "readableTitle": readableTitle,
        "translates": translateMap,
        **_build_version_fields(created_raw, updated_raw),
    }

def getSubtitleContext(fileName: str, _subtitleId: int | None = None, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)

    # Always load subtitle context by file base to keep multi-language lines available.
    lines = databaseHelper.selectSubtitleContext(fileName, langs)

    # Subtitle versions are file-level.
    created_raw, updated_raw = databaseHelper.getSubtitleFileVersionInfo(fileName)

    # Group by per-language line index (more stable than timestamp across language variants).
    lines_by_lang: dict[int, list[tuple[str, float, float]]] = {}
    for content, lang, startTime, endTime in lines:
        norm_content = _normalize_text_map_content(content, lang)
        if norm_content is None:
            continue  # Skip lines where normalization failed
        lines_by_lang.setdefault(lang, []).append((norm_content, startTime, endTime))
    for lang in lines_by_lang:
        lines_by_lang[lang].sort(key=lambda x: x[1])

    max_line_count = 0
    for lang_lines in lines_by_lang.values():
        if len(lang_lines) > max_line_count:
            max_line_count = len(lang_lines)

    dialogues = []
    ordered_langs = [lang for lang in langs if lang in lines_by_lang]
    if not ordered_langs:
        ordered_langs = sorted(lines_by_lang.keys())

    for idx in range(max_line_count):
        translates = {}
        start_time = None
        for lang in ordered_langs:
            lang_lines = lines_by_lang.get(lang) or []
            if idx >= len(lang_lines):
                continue
            content, line_start, _line_end = lang_lines[idx]
            translates[str(lang)] = content
            if start_time is None:
                start_time = line_start
        if not translates:
            continue
        dialogues.append({
            'talker': '',
            'translates': translates,
            'voicePaths': [],
            'availableVoiceLangs': [],
            'dialogueId': int((start_time or 0) * 1000) + idx,
            **_build_version_fields(created_raw, updated_raw),
        })

    return {
        "talkQuestName": f"字幕: {fileName}",
        "talkId": 0,
        "dialogues": dialogues,
        **_build_version_fields(created_raw, updated_raw),
    }


def getVoiceBinStream(voicePath, langCode):
    wemBin = languagePackReader.getAudioBin(voicePath, langCode)
    if wemBin is None:
        return None
    return io.BytesIO(wemBin)


def getLoadedVoicePacks():
    from utils.helpers import getLanguageName
    ans = {}
    for packId in languagePackReader.langPackages:
        ans[packId] = getLanguageName(languagePackReader.langCodes[packId])

    return ans


def getImportedTextMapLangs():
    from utils.helpers import getLanguageName
    langs = databaseHelper.getImportedTextMapLangs()
    ans = {}
    for langItem in langs:
        ans[str(langItem[0])] = getLanguageName(langItem[1])

    return ans





def getAvailableVersions():
    """
    获取可用的版本列表
    """
    # 生成缓存键
    cache_key = "available_versions"

    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return cached_result

    versions = databaseHelper.getAllVersionValues()
    # 提取版本标签并去重
    version_tags = set()
    for version in versions:
        tag = _extract_version_tag(version)
        if tag:
            version_tags.add(tag)
    # 按版本号排序
    sorted_versions = sorted(version_tags, key=lambda x: [int(part) for part in x.split('.')], reverse=True)

    # 缓存结果
    search_cache.set(cache_key, sorted_versions)

    return sorted_versions


def getConfig():
    # 返回 config + 额外状态字段（前端可以直接显示“目录是否有效”）
    cfg = dict(config.config)
    cfg["assetDirValid"] = config.isAssetDirValid()
    return cfg


def setDefaultSearchLanguage(newLanguage: int):
    config.setDefaultSearchLanguage(newLanguage)


def setResultLanguages(newLanguages: list[int]):
    config.setResultLanguages(newLanguages)


def saveConfig():
    config.saveConfig()


def setSourceLanguage(newSourceLanguage):
    config.setSourceLanguage(newSourceLanguage)


def setIsMale(isMale):
    config.setIsMale(isMale)


def getAvatarVoices(avatarId: int, searchLang: int | None = None):
    langs, sourceLangCode, _keywordLangCode = _resolve_avatar_query_langs(searchLang)

    avatarName = databaseHelper.getCharterName(avatarId, sourceLangCode)
    voice_rows = databaseHelper.selectAvatarVoiceItems(avatarId)

    voices = []
    voice_map: dict[int, dict] = {}

    for titleHash, textHash, voicePath in voice_rows:
        if textHash is None:
            continue

        obj = voice_map.get(textHash)
        if obj is None:
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['isTalk'] = False
            obj['viewAsTextHash'] = True
            obj['disableDetail'] = True
            if avatarName:
                if titleHash:
                    title = _get_text_map_content_with_fallback(titleHash, sourceLangCode, langs)
                    if title:
                        obj['origin'] = f"{avatarName} ﾂｷ {title}"
                        obj['voiceTitle'] = title
                    else:
                        obj['origin'] = avatarName
                        obj['voiceTitle'] = ""
                else:
                    obj['origin'] = avatarName
                    obj['voiceTitle'] = ""
            else:
                obj['voiceTitle'] = ""
            created_raw, updated_raw = databaseHelper.getTextMapVersionInfo(textHash, sourceLangCode)
            obj.update(_build_version_fields(created_raw, updated_raw))
            voice_map[textHash] = obj
            voices.append(obj)

        _attach_voice_metadata(obj, voicePath, langs)

    return {
        "avatarId": avatarId,
        "avatarName": avatarName,
        "voices": voices
    }


def searchAvatarVoicesByFilters(
    title_keyword: str | None = None,
    searchLang: int | None = None,
    created_version: str | None = None,
    updated_version: str | None = None,
    limit: int = 1200,
):
    langs, sourceLangCode, keywordLangCode = _resolve_avatar_query_langs(searchLang)

    voice_rows = databaseHelper.selectAvatarVoiceItemsByFilters(
        title_keyword,
        keywordLangCode,
        limit=limit,
        created_version=created_version,
        updated_version=updated_version,
        version_lang_code=sourceLangCode,
    )

    voices = []
    voice_map: dict[tuple[int, int], dict] = {}
    avatar_name_cache: dict[int, str | None] = {}
    title_cache: dict[int, str | None] = {}

    for avatarId, titleHash, textHash, voicePath in voice_rows:
        if textHash is None:
            continue
        dedupe_key = (avatarId, textHash)

        obj = voice_map.get(dedupe_key)
        if obj is None:
            if avatarId not in avatar_name_cache:
                avatar_name_cache[avatarId] = databaseHelper.getCharterName(avatarId, sourceLangCode)
            avatarName = avatar_name_cache[avatarId]

            if titleHash and titleHash not in title_cache:
                title_cache[titleHash] = _normalize_text_map_content(
                    databaseHelper.getTextMapContent(titleHash, sourceLangCode),
                    sourceLangCode,
                )
            title = title_cache.get(titleHash) if titleHash else None

            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['isTalk'] = False
            obj['viewAsTextHash'] = True
            obj['disableDetail'] = True
            if avatarName and title:
                obj['origin'] = f"{avatarName} ﾂｷ {title}"
                obj['voiceTitle'] = title
            elif avatarName:
                obj['origin'] = avatarName
                obj['voiceTitle'] = ""
            elif title:
                obj['origin'] = title
                obj['voiceTitle'] = title
            else:
                obj['origin'] = "隗定牡隸ｭ髻ｳ"
                obj['voiceTitle'] = ""
            created_raw, updated_raw = databaseHelper.getTextMapVersionInfo(textHash, sourceLangCode)
            obj.update(_build_version_fields(created_raw, updated_raw))

            obj['avatarId'] = avatarId
            obj['avatarName'] = avatarName or ""

            voice_map[dedupe_key] = obj
            voices.append(obj)

        _attach_voice_metadata(obj, voicePath, langs)

    _sort_entries_by_match(
        voices,
        title_keyword or "",
        keywordLangCode,
        lambda entry: [
            entry.get("voiceTitle"),
            entry.get("translates", {}).get(str(keywordLangCode)),
        ],
    )
    return {
        "voices": voices
    }

from __future__ import annotations

import os
from typing import Mapping

from lang_constants import LANG_CODE_MAP


def _normalize_rel_path(path: str | None) -> str:
    return str(path or "").replace("\\", "/").strip("/")


def _lang_name_from_id(
    lang_id: int | str,
    *,
    lang_code_map: Mapping[str, int] = LANG_CODE_MAP,
) -> str | None:
    try:
        target_id = int(lang_id)
    except Exception:
        return None
    for lang_name, mapped_id in lang_code_map.items():
        if int(mapped_id) == target_id:
            return lang_name
    return None


def normalize_readable_rel_path(rel_path: str) -> tuple[str, str] | None:
    normalized = _normalize_rel_path(rel_path)
    if normalized.startswith("Readable/"):
        normalized = normalized[len("Readable/") :]
    parts = normalized.split("/", 1)
    if len(parts) != 2:
        return None
    lang, file_name = parts
    file_name = _normalize_rel_path(file_name)
    if not lang or not file_name:
        return None
    return lang, file_name


def build_readable_rel_path(file_name: str, lang: str) -> str | None:
    normalized_file_name = _normalize_rel_path(file_name)
    normalized_lang = _normalize_rel_path(lang)
    if not normalized_lang or not normalized_file_name:
        return None
    return f"Readable/{normalized_lang}/{normalized_file_name}"


def build_readable_full_path(
    file_name: str,
    lang: str,
    *,
    readable_root: str,
) -> str | None:
    rel_path = build_readable_rel_path(file_name, lang)
    if rel_path is None:
        return None
    rel_under_root = rel_path[len("Readable/") :]
    return os.path.join(readable_root, rel_under_root.replace("/", os.sep))


def build_readable_rel_path_from_record(record) -> str | None:
    if not isinstance(record, (tuple, list)) or len(record) < 2:
        return None
    file_name, lang = record[:2]
    if not isinstance(file_name, str) or not isinstance(lang, str):
        return None
    return build_readable_rel_path(file_name, lang)


def normalize_subtitle_rel_path(
    rel_path: str,
    *,
    lang_code_map: Mapping[str, int] = LANG_CODE_MAP,
) -> tuple[str, int, str] | None:
    normalized = _normalize_rel_path(rel_path)
    if normalized.startswith("Subtitle/"):
        normalized = normalized[len("Subtitle/") :]
    parts = normalized.split("/", 1)
    if len(parts) != 2:
        return None
    lang_name, rel_under_lang = parts
    lang_id = lang_code_map.get(lang_name)
    if lang_id is None:
        return None
    if not rel_under_lang.lower().endswith(".srt"):
        return None
    clean_file_name = os.path.splitext(rel_under_lang)[0].replace("\\", "/")
    clean_file_name = _normalize_rel_path(clean_file_name)
    if not clean_file_name:
        return None
    return lang_name, int(lang_id), clean_file_name


def build_subtitle_rel_path(clean_file_name: str, lang_name: str) -> str | None:
    normalized_file_name = _normalize_rel_path(clean_file_name)
    normalized_lang_name = _normalize_rel_path(lang_name)
    if normalized_file_name.lower().endswith(".srt"):
        normalized_file_name = normalized_file_name[:-4]
    if not normalized_lang_name or not normalized_file_name:
        return None
    return f"Subtitle/{normalized_lang_name}/{normalized_file_name}.srt"


def build_subtitle_full_path(
    clean_file_name: str,
    lang_name: str,
    *,
    subtitle_root: str,
) -> str | None:
    rel_path = build_subtitle_rel_path(clean_file_name, lang_name)
    if rel_path is None:
        return None
    rel_under_root = rel_path[len("Subtitle/") :]
    return os.path.join(subtitle_root, rel_under_root.replace("/", os.sep))


def build_subtitle_rel_path_from_key(
    subtitle_key: str,
    *,
    lang_code_map: Mapping[str, int] = LANG_CODE_MAP,
) -> str | None:
    if not isinstance(subtitle_key, str) or not subtitle_key:
        return None

    clean_file_name: str | None = None
    lang_part: str | None = None
    if "|" in subtitle_key:
        parts = subtitle_key.split("|")
        if len(parts) != 4:
            return None
        clean_file_name, lang_part = parts[0], parts[1]
    else:
        parts = subtitle_key.split("_")
        if len(parts) < 4:
            return None
        clean_file_name = "_".join(parts[:-3])
        lang_part = parts[-3]

    if not clean_file_name or not lang_part:
        return None

    lang_name = _lang_name_from_id(lang_part, lang_code_map=lang_code_map)
    if lang_name is None:
        return None
    return build_subtitle_rel_path(clean_file_name, lang_name)


def build_subtitle_rel_path_from_record(record) -> str | None:
    if not isinstance(record, (tuple, list)) or not record:
        return None
    subtitle_key = record[0]
    if not isinstance(subtitle_key, str):
        return None
    return build_subtitle_rel_path_from_key(subtitle_key)

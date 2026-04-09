import io
import json
import math
import os
import re
import zlib
from functools import lru_cache
from typing import TypedDict

import databaseHelper
import languagePackReader
import config
import placeholderHandler
from utils.cache import search_cache

_QUEST_SOURCE_TYPE_LABELS = {
    "AQ": "魔神任务",
    "LQ": "传说任务",
    "WQ": "世界任务",
    "EQ": "活动任务",
    "IQ": "委托任务",
    "HANGOUT": "邀约事件",
    "ANECDOTE": "游逸旅闻",
    "UNKNOWN": "未分类",
}


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


@lru_cache(maxsize=1024)
def _count_fetter_by_speaker_keyword_cached(
    speaker_keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countFetterBySpeakerKeyword(
        speaker_keyword,
        lang_code,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=1024)
def _count_story_by_speaker_keyword_cached(
    speaker_keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countAvatarStoryBySpeakerKeyword(
        speaker_keyword,
        lang_code,
        created_version,
        updated_version,
    )


@lru_cache(maxsize=1024)
def _count_story_by_speaker_and_keyword_cached(
    speaker_keyword: str,
    keyword: str,
    lang_code: int,
    created_version: str | None,
    updated_version: str | None,
) -> int:
    return databaseHelper.countAvatarStoryBySpeakerAndKeyword(
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


class AssetDirDialogUnavailableError(RuntimeError):
    """Raised when the native asset-directory picker cannot be opened."""


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
    except ModuleNotFoundError as exc:
        if exc.name == "_tkinter":
            raise AssetDirDialogUnavailableError("_tkinter module is unavailable") from exc
        raise AssetDirDialogUnavailableError("tkinter is unavailable") from exc
    except Exception as exc:
        raise AssetDirDialogUnavailableError("tkinter is unavailable") from exc

    root = None
    try:
        root = tk.Tk()
        root.withdraw()
        try:
            root.attributes("-topmost", True)
        except Exception:
            pass
        picked = filedialog.askdirectory(title="请选择原神资源目录（包含 StreamingAssets 或 Persistent）")
    except Exception as exc:
        raise AssetDirDialogUnavailableError("failed to open directory picker") from exc
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:
                pass

    if not picked:
        return None
    return picked


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


def _select_story_source_from_text_hash(text_hash: int, lang_code: int) -> tuple[dict, str, bool, int] | None:
    story_sources = databaseHelper.selectStorySourcesByTextHash(text_hash)
    if not story_sources:
        return None

    avatar_id, _fetter_id, title_hash, locked_title_hash = story_sources[0]
    avatar_name = databaseHelper.getCharterName(avatar_id, lang_code)
    title = None
    if title_hash:
        title = _get_text_map_content_with_fallback(title_hash, lang_code, [config.getSourceLanguage()])
    if not title and locked_title_hash:
        title = _get_text_map_content_with_fallback(locked_title_hash, lang_code, [config.getSourceLanguage()])

    if avatar_name and title:
        origin = f"{avatar_name} · {title}"
    elif avatar_name:
        origin = avatar_name
    elif title:
        origin = title
    else:
        origin = "角色故事"

    primary = _build_primary_source(
        "story",
        origin,
        "角色故事",
        {"kind": "text", "textHash": text_hash},
    )
    return primary, origin, False, len(story_sources)


def _build_primary_source(
    source_type: str,
    title: str,
    subtitle: str | None = None,
    detail_query: dict[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "sourceType": source_type,
        "title": title,
    }
    if subtitle:
        payload["subtitle"] = subtitle
    if detail_query:
        payload["detailQuery"] = detail_query
    return payload


def _normalize_source_type_filter(source_type: str | None) -> str | None:
    normalized = str(source_type or "").strip().lower()
    if not normalized or normalized == "all":
        return None
    alias_map = {
        "角色语音": "voice",
        "角色故事": "story",
        "奇偶装扮": "costume",
        "装扮套装": "costume",
        "千星奇域": "costume",
        "衣装": "dressing",
        "角色装扮": "dressing",
        "outfit": "dressing",
        "武器": "weapon",
        "圣遗物": "reliquary",
        "怪物": "monster",
        "生物": "creature",
        "小道具": "gadget",
        "道具": "item",
        "材料": "material",
        "食物": "food",
        "摆设": "furnishing",
        "图纸": "blueprint",
        "七圣召唤": "gcg",
        "名片": "namecard",
        "表演诀窍": "performance",
        "角色": "avatar_intro",
        "装扮": "dressing",
        "演奏主题": "music_theme",
        "其他": "other_mat",
        "角色突破素材": "avatar_mat",
        "成就": "achievement",
        "观景点": "viewpoint",
        "秘境": "dungeon",
        "过场提示": "loading_tip",
    }
    return alias_map.get(normalized, normalized)


def _matches_source_type_filter(entry: dict, source_type_filter: str | None) -> bool:
    normalized_filter = _normalize_source_type_filter(source_type_filter)
    if normalized_filter is None:
        return True
    primary_source = entry.get("primarySource") or {}
    source_type = str(primary_source.get("sourceType") or "").strip().lower()
    if normalized_filter == "costume":
        return source_type in {"costume", "suit"}
    return source_type == normalized_filter


def _filter_entries_by_source_type(entries: list[dict], source_type_filter: str | None) -> list[dict]:
    normalized_filter = _normalize_source_type_filter(source_type_filter)
    if normalized_filter is None:
        return entries
    return [entry for entry in entries if _matches_source_type_filter(entry, normalized_filter)]


def _text_hash_matches_source_type(
    text_hash: int | None,
    source_type_filter: str | None,
    lang_code: int,
    entry: dict | None = None,
) -> bool:
    normalized_filter = _normalize_source_type_filter(source_type_filter)
    if normalized_filter is None:
        return True
    if text_hash is None:
        return False
    if normalized_filter == "voice":
        return databaseHelper.getSourceFromFetter(text_hash, lang_code) is not None
    if normalized_filter == "story":
        return bool(databaseHelper.selectStorySourcesByTextHash(text_hash))
    return _matches_source_type_filter(entry or {}, normalized_filter)


# 可以在数据库层直接 JOIN 过滤的来源类型
_DB_FILTERABLE_SOURCE_TYPES_BUILTIN: frozenset[str] = frozenset({
    "dialogue", "voice", "story", "quest",
    "item", "food", "furnishing", "gadget", "material",
    "costume", "suit",
    "weapon", "reliquary", "monster", "creature",
    "blueprint", "gcg", "namecard", "performance",
    "avatar_intro", "dressing", "music_theme", "other_mat",
    "avatar_mat", "achievement", "viewpoint", "dungeon",
    "loading_tip",
})


def _get_db_filterable_source_types() -> frozenset[str]:
    custom = _load_custom_categories()
    extras = {f"custom_{code}" for code in custom.get("source_types", {})}
    if not extras:
        return _DB_FILTERABLE_SOURCE_TYPES_BUILTIN
    return _DB_FILTERABLE_SOURCE_TYPES_BUILTIN | extras


_ENTITY_SOURCE_META = {
    1: ("item", "道具"),
    2: ("food", "食物"),
    3: ("furnishing", "摆设"),
    4: ("gadget", "小道具"),
    5: ("costume", "千星奇域"),
    6: ("suit", "千星奇域"),
    9: ("weapon", "武器"),
    10: ("reliquary", "圣遗物"),
    11: ("monster", "怪物"),
    12: ("creature", "生物"),
    13: ("material", "材料"),
    14: ("blueprint", "图纸"),
    15: ("gcg", "七圣召唤"),
    16: ("namecard", "名片"),
    17: ("performance", "表演诀窍"),
    18: ("avatar_intro", "角色"),
    19: ("dressing", "装扮"),
    20: ("music_theme", "演奏主题"),
    21: ("other_mat", "其他"),
    22: ("avatar_mat", "角色突破素材"),
    23: ("achievement", "成就"),
    24: ("viewpoint", "观景点"),
    25: ("dungeon", "秘境"),
    26: ("loading_tip", "过场提示"),
}

_ENTITY_SOURCE_PRIORITY = {
    2: 1,
    22: 2,
    13: 3,
    14: 4,
    15: 5,
    16: 6,
    17: 7,
    18: 8,
    19: 9,
    20: 10,
    3: 11,
    5: 12,
    6: 15,
    9: 16,
    10: 17,
    11: 18,
    12: 19,
    23: 20,
    24: 21,
    25: 22,
    26: 23,
    21: 24,
    4: 25,
    1: 26,
}


def _get_entity_source_meta(source_type_code: int) -> tuple[str, str]:
    meta = _ENTITY_SOURCE_META.get(int(source_type_code))
    if meta:
        return meta
    custom = _load_custom_categories()
    label = custom.get("source_types", {}).get(str(source_type_code))
    if label:
        return (f"custom_{source_type_code}", label)
    return ("item", "道具")


def _load_custom_categories() -> dict:
    """Load custom categories from material_type_overrides.json."""
    base = os.path.join(os.path.dirname(__file__), "dbBuild", "material_type_overrides.json")
    try:
        with open(base, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"source_types": {}, "sub_categories": {}}
    return {
        "source_types": data.get("_source_types") or {},
        "sub_categories": data.get("_sub_categories") or {},
    }


_SUB_CATEGORY_LABELS: dict[int, str] = {
    0: "",
    1: "任务道具",
    2: "摆设图纸",
    3: "牌面",
    4: "卡牌",
    5: "消耗品",
    6: "角色介绍",
    7: "命之座激活素材",
    8: "摆设套装图纸",
    9: "宝箱",
    10: "活动道具",
    11: "小道具",
    12: "种子",
    13: "牌背",
    14: "活动食物",
    15: "木材",
    16: "经验素材",
    17: "角色装扮",
    18: "游迹",
    19: "风之翼",
    20: "烟花",
    21: "钓竿",
    22: "武器外观",
    23: "头像框",
    24: "头像",
    25: "牌盒",
    26: "鱼饵",
    27: "旋曜玉帛",
    28: "角色突破素材",
    29: "奇偶装扮",
    30: "装扮套装",
}

_CATALOG_OTHER_SUB_CATEGORY_CODE = "0"
_CATALOG_OTHER_SUB_CATEGORY_LABEL = "其他"


def _get_sub_category_label(sub_category_code: int) -> str:
    return _SUB_CATEGORY_LABELS.get(int(sub_category_code), "")


_READABLE_LANG_SUFFIX_RE = re.compile(r"_(CHT|DE|EN|ES|FR|ID|IT|JP|KR|PT|RU|TH|TR|VI)$", re.IGNORECASE)
_ENTITY_EMPTY_BODY_MESSAGE = "暂无可用描述文本"


class _EntityReadableLookup(TypedDict):
    outfit_item_to_skin: dict[int, int]
    reliquary_set_to_id: dict[int, int]
    reliquary_set_piece_to_id: dict[tuple[int, int], int]
    book_material_ids: set[int]


@lru_cache(maxsize=1)
def _load_entity_readable_lookup() -> _EntityReadableLookup:
    asset_dir = str(config.getAssetDir() or "").strip()
    candidate_roots = [asset_dir]
    try:
        project_root = config.project_root()
        candidate_roots.append(str(project_root / "AnimeGameData"))
        candidate_roots.append(str(project_root.parent / "AnimeGameData"))
    except Exception:
        pass

    data_root = ""
    for candidate in candidate_roots:
        if not candidate:
            continue
        excel_dir = os.path.join(candidate, "ExcelBinOutput")
        if os.path.isdir(excel_dir):
            data_root = candidate
            break

    if not data_root:
        return {
            "outfit_item_to_skin": {},
            "reliquary_set_to_id": {},
            "reliquary_set_piece_to_id": {},
            "book_material_ids": set(),
        }

    excel_root = os.path.join(data_root, "ExcelBinOutput")

    def _load_rows(file_name: str) -> list[dict]:
        path = os.path.join(excel_root, file_name)
        try:
            with open(path, encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception:
            return []
        if not isinstance(data, list):
            return []
        return [row for row in data if isinstance(row, dict)]

    outfit_item_to_skin: dict[int, int] = {}
    for row in _load_rows("AvatarCostumeExcelConfigData.json"):
        skin_id = row.get("skinId")
        item_id = row.get("itemId")
        if isinstance(skin_id, int) and skin_id and isinstance(item_id, int) and item_id:
            outfit_item_to_skin[item_id] = skin_id

    reliquary_set_to_id: dict[int, int] = {}
    reliquary_set_piece_to_id: dict[tuple[int, int], int] = {}
    equip_order = {
        "EQUIP_BRACER": 1,
        "EQUIP_NECKLACE": 2,
        "EQUIP_SHOES": 3,
        "EQUIP_RING": 4,
        "EQUIP_DRESS": 5,
    }
    for row in _load_rows("ReliquaryExcelConfigData.json"):
        reliquary_id = row.get("id")
        set_id = row.get("setId")
        equip_type = str(row.get("equipType") or "").strip().upper()
        if not isinstance(reliquary_id, int) or not reliquary_id or not isinstance(set_id, int) or not set_id:
            continue
        reliquary_set_to_id.setdefault(set_id, reliquary_id)
        piece_no = equip_order.get(equip_type)
        if piece_no:
            reliquary_set_piece_to_id.setdefault((set_id, piece_no), reliquary_id)

    return {
        "outfit_item_to_skin": outfit_item_to_skin,
        "reliquary_set_to_id": reliquary_set_to_id,
        "reliquary_set_piece_to_id": reliquary_set_piece_to_id,
        "book_material_ids": _build_book_material_ids(_load_rows("BooksCodexExcelConfigData.json")),
    }


def _build_book_material_ids(rows: list[dict]) -> set[int]:
    ids: set[int] = set()
    for row in rows:
        mid = row.get("materialId")
        if isinstance(mid, int) and mid:
            ids.add(mid)
    return ids


def _build_entity_source_payload(
    source_rows: list[tuple[int, int, int, int]],
    lang_code: int,
    text_hash: int,
) -> tuple[dict, str, int]:
    picked = min(source_rows, key=lambda row: (_ENTITY_SOURCE_PRIORITY.get(row[0], 99), row[1]))
    source_type_code, entity_id, title_hash, extra = picked
    source_type, source_label = _get_entity_source_meta(source_type_code)
    title = _get_text_map_content_with_fallback(title_hash, lang_code, [config.getSourceLanguage()]) or str(entity_id)
    subtitle = f"{source_label} {entity_id}"
    gender_code = 0
    if source_type in ("costume", "suit"):
        if extra in (1, 2):
            gender_code = extra
        else:
            gender_code = (extra >> 8) & 0xFF
    if gender_code in (1, 2):
        subtitle = subtitle + (" · 男" if gender_code == 1 else " · 女")
    origin = f"{source_label}: {title}"
    primary = _build_primary_source(
        source_type,
        title,
        subtitle,
        {"kind": "entity", "sourceTypeCode": source_type_code, "entityId": entity_id, "textHash": text_hash},
    )
    return primary, origin, len(source_rows)


def _resolve_entity_from_readable_file(file_name: str) -> tuple[int, int] | None:
    lookup = _load_entity_readable_lookup()
    stem = os.path.splitext(os.path.basename(str(file_name or "")))[0]
    stem = _READABLE_LANG_SUFFIX_RE.sub("", stem)

    weapon_match = re.fullmatch(r"Weapon(\d+)", stem, re.IGNORECASE)
    if weapon_match:
        return 9, int(weapon_match.group(1))

    wings_match = re.fullmatch(r"Wings(\d+)", stem, re.IGNORECASE)
    if wings_match:
        return 19, int(wings_match.group(1))

    costume_match = re.fullmatch(r"Costume(\d+)", stem, re.IGNORECASE)
    if costume_match:
        raw_id = int(costume_match.group(1))
        outfit_skin_id = lookup["outfit_item_to_skin"].get(raw_id)
        if outfit_skin_id:
            return 19, outfit_skin_id
        return 5, raw_id

    relic_match = re.fullmatch(r"Relic(\d+)(?:_(\d+))?", stem, re.IGNORECASE)
    if relic_match:
        set_id = int(relic_match.group(1))
        piece_no = int(relic_match.group(2)) if relic_match.group(2) else None
        if piece_no is not None:
            reliquary_id = lookup["reliquary_set_piece_to_id"].get((set_id, piece_no))
            if reliquary_id:
                return 10, reliquary_id
        reliquary_id = lookup["reliquary_set_to_id"].get(set_id)
        if reliquary_id:
            return 10, reliquary_id

    return None


def _get_entity_readable_category_filters(source_type: str) -> list[str]:
    mapping = {
        "costume": ["COSTUME"],
        "suit": ["COSTUME"],
        "dressing": ["COSTUME", "WINGS"],
        "weapon": ["WEAPON"],
        "reliquary": ["RELIC"],
    }
    return mapping.get(source_type, [])


def _build_entity_readable_entry(
    file_name: str,
    title_text_hash: int | None,
    readable_id: int | None,
    field_label: str,
    subtitle: str,
    langs: list[int],
    source_lang_code: int,
) -> dict | None:
    lang_map = databaseHelper.getLangCodeMap()
    target_lang_strs = [lang_map[lang] for lang in langs if lang in lang_map]
    str_to_lang_id = _build_lang_str_to_id_map()

    if readable_id:
        translations = databaseHelper.selectReadableFromReadableId(readable_id, target_lang_strs)
    else:
        translations = databaseHelper.selectReadableFromFileName(file_name, target_lang_strs)

    translate_map = {}
    for trans_content, trans_lang_str in translations:
        if trans_lang_str in str_to_lang_id:
            lang_id = str_to_lang_id[trans_lang_str]
            translate_map[str(lang_id)] = _normalize_text_map_content(trans_content, lang_id)

    if not translate_map:
        return None

    created_raw, updated_raw = databaseHelper.getReadableVersionInfo(readable_id, file_name)
    readable_hash = int(title_text_hash or 0)
    return {
        "fieldLabel": field_label,
        "subtitle": subtitle,
        "textHash": readable_hash,
        "titleHash": readable_hash,
        "readableId": readable_id,
        "fileName": file_name,
        "detailQuery": {
            "kind": "readable",
            "readableId": readable_id,
            "fileName": file_name,
            "textHash": readable_hash,
        },
        "text": {
            "translates": translate_map,
            "voicePaths": [],
            "availableVoiceLangs": [],
            "hash": readable_hash,
            **_build_version_fields(created_raw, updated_raw),
        },
    }


def _collect_entity_readable_entries(
    source_type: str,
    entity_id: int,
    title_text_hash: int | None,
    subtitle: str,
    langs: list[int],
    source_lang_code: int,
    sub_category: int = 0,
) -> list[dict]:
    refs = []
    prefix_map = {
        "weapon": [f"Weapon{entity_id}"],
        "costume": [f"Costume{entity_id}"],
    }
    if source_type == "dressing" and sub_category == 19:
        # 风之翼 (SUB_FLYCLOAK_SUB) → Wings readable
        prefix_map[source_type] = [f"Wings{entity_id}"]
    elif source_type == "dressing" and sub_category == 17:
        # 角色装扮 (SUB_COSTUME_DRESS) → Costume readable via reverse lookup
        for raw_item_id, skin_id in _load_entity_readable_lookup()["outfit_item_to_skin"].items():
            if skin_id == entity_id:
                prefix_map[source_type] = [f"Costume{raw_item_id}"]
                break
    if source_type == "reliquary":
        lookup = _load_entity_readable_lookup()
        for set_id, reliquary_id in lookup["reliquary_set_to_id"].items():
            if reliquary_id == entity_id or any(v == entity_id and k[0] == set_id for k, v in lookup["reliquary_set_piece_to_id"].items()):
                prefix_map[source_type] = [f"Relic{set_id}_", f"Relic{set_id}"]
                break

    for prefix in prefix_map.get(source_type, []):
        refs.extend(databaseHelper.selectReadableRefsByFileNamePrefix(prefix))

    category_filters = _get_entity_readable_category_filters(source_type)
    if title_text_hash:
        for category in category_filters:
            refs.extend(databaseHelper.selectReadableRefsByTitleHash(title_text_hash, category))
        if not refs:
            refs.extend(databaseHelper.selectReadableRefsByTitleHash(title_text_hash, None))

    seen_keys: set[int | str] = set()
    entries: list[dict] = []
    if source_type in {"costume", "suit", "dressing", "weapon", "reliquary"}:
        field_label = "故事"
    else:
        field_label = _classify_readable_label(None, title_text_hash)

    for file_name, readable_title_hash, readable_id in refs:
        dedupe_key: int | str
        if readable_id is not None:
            dedupe_key = int(readable_id)
        else:
            dedupe_key = _READABLE_LANG_SUFFIX_RE.sub("", os.path.splitext(str(file_name))[0])
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        resolved_title_hash = int(readable_title_hash or title_text_hash or 0)
        entry = _build_entity_readable_entry(
            str(file_name),
            resolved_title_hash,
            int(readable_id) if readable_id is not None else None,
            field_label,
            subtitle,
            langs,
            source_lang_code,
        )
        if entry is not None:
            entries.append(entry)
    return entries


def _select_primary_source_from_text_hash(text_hash: int, lang_code: int) -> tuple[dict, str, bool, int]:
    talk_info = databaseHelper.getTalkInfo(text_hash)
    if talk_info is not None:
        talk_id, talker_type, talker_id, coop_quest_id = talk_info
        talker_name = databaseHelper.getTalkerName(talker_type, talker_id, lang_code)
        source_lang_code = config.getSourceLanguage()
        if coop_quest_id is None:
            quest_title = databaseHelper.getTalkQuestName(talk_id, source_lang_code)
        else:
            quest_title = databaseHelper.getCoopTalkQuestName(coop_quest_id, source_lang_code)

        origin = quest_title if not talker_name else f"{talker_name}, {quest_title}"
        primary = _build_primary_source(
            "dialogue",
            quest_title,
            talker_name,
            {"kind": "talk", "textHash": text_hash},
        )
        return primary, origin, True, 1

    fetter_origin = databaseHelper.getSourceFromFetter(text_hash, lang_code)
    if fetter_origin is not None:
        primary = _build_primary_source(
            "voice",
            fetter_origin,
            "角色语音",
            {"kind": "text", "textHash": text_hash},
        )
        return primary, fetter_origin, False, 1

    story_source = _select_story_source_from_text_hash(text_hash, lang_code)
    if story_source is not None:
        return story_source

    quest_sources = databaseHelper.selectQuestHashSources(text_hash)
    if quest_sources:
        source_priority = {"title": 0, "desc": 1, "long_desc": 2}
        quest_id, matched_source_type = min(
            quest_sources,
            key=lambda item: (source_priority.get(item[1], 99), item[0]),
        )
        quest_ids = sorted({quest_id for quest_id, _ in quest_sources})
        quest_title = databaseHelper.getQuestName(quest_id, lang_code)
        if matched_source_type == "desc":
            quest_subtitle = f"Quest {quest_id} · 任务简介"
        elif matched_source_type == "long_desc":
            quest_subtitle = f"Quest {quest_id} · 任务长简介"
        else:
            quest_subtitle = f"Quest {quest_id}"
        origin = f"任务: {quest_title}"
        primary = _build_primary_source(
            "quest",
            quest_title,
            quest_subtitle,
            {"kind": "quest", "questId": quest_id},
        )
        return primary, origin, False, len(quest_ids)

    entity_sources = databaseHelper.selectEntitySourcesByTextHash(text_hash)
    if entity_sources:
        primary, origin, source_count = _build_entity_source_payload(entity_sources, lang_code, text_hash)
        return primary, origin, False, source_count

    entity_title_sources = databaseHelper.selectEntitySourcesByTitleHash(text_hash)
    if entity_title_sources:
        primary, origin, source_count = _build_entity_source_payload(entity_title_sources, lang_code, text_hash)
        return primary, origin, False, source_count

    readable_info = databaseHelper.getReadableInfoByTitleHash(text_hash)
    if readable_info:
        file_name, _title_text_map_hash, readable_id = readable_info
        readable_title = _get_text_map_content_with_fallback(text_hash, lang_code, [config.getSourceLanguage()]) or str(file_name)
        readable_refs = databaseHelper.selectReadableRefsByTitleHash(text_hash)
        readable_origin = f"阅读物: {readable_title}"
        readable_primary = _build_primary_source(
            "readable",
            readable_title,
            "阅读物",
            {"kind": "readable", "readableId": readable_id, "fileName": file_name, "textHash": text_hash},
        )
        return readable_primary, readable_origin, False, max(len(readable_refs), 1)

    unknown_origin = "其他文本"
    primary = _build_primary_source(
        "unknown",
        "未归类文本",
        f"TextMap Hash {text_hash}",
        {"kind": "text", "textHash": text_hash},
    )
    return primary, unknown_origin, False, 0


def queryTextHashInfo(textHash: int, langs: 'list[int]', sourceLangCode: int, queryOrigin=True):
    """
    查询文本哈希的信息
    - 获取多语言翻译
    - 查询语音路径
    - 获取版本信息
    - queryOrigin=False 用于搜索阶段，跳过来源查询以大幅减少数据库查询
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
        primary_source, origin, is_talk, source_count = _select_primary_source_from_text_hash(textHash, sourceLangCode)
        obj['isTalk'] = is_talk
        obj['origin'] = origin
        obj['primarySource'] = primary_source
        obj['sourceCount'] = source_count
        if not is_talk:
            obj['viewAsTextHash'] = True
    else:
        # 搜索阶段：跳过昂贵的来源查询，来源筛选在数据库层完成
        obj['origin'] = "其他文本"
        obj['isTalk'] = False

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


def _normalize_preview_text(content: str | None) -> str | None:
    if content is None:
        return None
    text = str(content).replace("\\n", "\n")
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _build_quest_source_type_fields(source_type: str | None) -> dict:
    normalized = str(source_type or "").strip().upper() or "UNKNOWN"
    if normalized not in _QUEST_SOURCE_TYPE_LABELS:
        normalized = "UNKNOWN"
    return {
        "sourceType": normalized,
        "sourceTypeLabel": _QUEST_SOURCE_TYPE_LABELS[normalized],
    }


def _classify_readable_label(file_name: str | None, title_text_hash: int | None = None) -> str:
    """统一阅读物分类：先按 codex（书籍/任务道具），再按文件名前缀。"""
    if title_text_hash:
        lookup = _load_entity_readable_lookup()
        book_ids = lookup.get("book_material_ids", set())
        entity_rows = databaseHelper.selectEntitySourcesByTitleHash(title_text_hash)
        for row in entity_rows:
            source_type_code, entity_id = row[0], row[1]
            if entity_id in book_ids:
                return "书籍"
            sub_rows = databaseHelper.selectEntityTextHashesByEntity(source_type_code, entity_id)
            for sub_row in sub_rows:
                sub_cat = sub_row[3] if len(sub_row) > 3 else 0
                if sub_cat == 1:  # SUB_QUEST_ITEM
                    return "任务道具"
    # 按文件名前缀分类（Book* 前缀不授予书籍标签，仅 codex 才能）
    category = databaseHelper.getReadableCategoryCode(file_name)
    label = databaseHelper.READABLE_CATEGORY_LABELS.get(category, "")
    return label if label and label not in {"其他", "书籍"} else "阅读物"


def _build_readable_category_fields(file_name: str | None, title_text_hash: int | None = None) -> dict:
    category = databaseHelper.getReadableCategoryCode(file_name)
    label = _classify_readable_label(file_name, title_text_hash)
    return {
        "readableCategory": category,
        "readableCategoryLabel": label,
    }


def _has_visible_quest_card_content(entry: dict) -> bool:
    return bool(
        _normalize_preview_text(entry.get("title"))
        or _normalize_preview_text(entry.get("contentPreview"))
    )


def _build_keyword_preview(
    content: str | None,
    keyword: str | None,
    *,
    radius: int = 28,
    fallback_limit: int = 72,
) -> str | None:
    text = _normalize_preview_text(content)
    if not text:
        return None

    keyword_text = _normalize_preview_text(keyword)
    if keyword_text:
        start_idx = text.lower().find(keyword_text.lower())
        if start_idx >= 0:
            end_idx = start_idx + len(keyword_text)
            snippet_start = max(0, start_idx - radius)
            snippet_end = min(len(text), end_idx + radius)
            snippet = text[snippet_start:snippet_end].strip()
            if snippet_start > 0:
                snippet = "..." + snippet
            if snippet_end < len(text):
                snippet = snippet + "..."
            return snippet

    if len(text) <= fallback_limit:
        return text
    return text[: max(0, fallback_limit - 3)].rstrip() + "..."


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


def _select_preferred_primary_source_from_text_hash(
    text_hash: int,
    source_lang_code: int,
    preferred_source_type: str | None,
) -> tuple[dict, str, bool, int] | None:
    normalized = _normalize_source_type_filter(preferred_source_type)
    if normalized == "voice":
        fetter_origin = databaseHelper.getSourceFromFetter(text_hash, source_lang_code)
        if fetter_origin is None:
            return None
        primary = _build_primary_source(
            "voice",
            fetter_origin,
            "角色语音",
            {"kind": "text", "textHash": text_hash},
        )
        return primary, fetter_origin, False, 1
    if normalized == "story":
        return _select_story_source_from_text_hash(text_hash, source_lang_code)
    return None


def _enrich_primary_sources(results: list[dict], source_lang_code: int):
    """
    为搜索结果中缺少 primarySource 的条目补充来源信息。
    仅对分页后的最终结果调用，避免对全部候选做昂贵查询。
    已有 primarySource 的条目（readable/subtitle）会被跳过。
    """
    for entry in results:
        text_hash = entry.get('hash')
        if text_hash is None:
            continue
        preferred_source_type = entry.pop('_preferredSourceType', None)
        preferred_source = _select_preferred_primary_source_from_text_hash(
            text_hash,
            source_lang_code,
            preferred_source_type,
        )
        if preferred_source is not None:
            primary_source, origin, is_talk, source_count = preferred_source
            entry['primarySource'] = primary_source
            entry['origin'] = origin
            entry['isTalk'] = is_talk
            entry['sourceCount'] = source_count
            if not is_talk and not entry.get('isReadable') and not entry.get('isSubtitle'):
                entry['viewAsTextHash'] = True
            continue
        if entry.get('primarySource'):
            continue
        primary_source, origin, is_talk, source_count = _select_primary_source_from_text_hash(text_hash, source_lang_code)
        entry['primarySource'] = primary_source
        entry['origin'] = origin
        entry['isTalk'] = is_talk
        entry['sourceCount'] = source_count
        if not is_talk and not entry.get('isReadable') and not entry.get('isSubtitle'):
            entry['viewAsTextHash'] = True


def _handle_speaker_only_query(speaker_keyword: str, langCode: int, page: int, page_size: int, voice_filter: str, created_version_filter: str | None, updated_version_filter: str | None, source_type_filter: str | None = None) -> tuple[list[dict], int]:
    """
    处理仅说话者查询
    """
    normalized_source_type = _normalize_source_type_filter(source_type_filter)
    if normalized_source_type and normalized_source_type not in ("dialogue", "voice", "story"):
        return [], 0

    ans = []
    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    seen_hashes = set()
    if normalized_source_type == "voice":
        if voice_filter == "without":
            return [], 0
        voice_rows = databaseHelper.selectFetterBySpeakerKeyword(
            speaker_keyword,
            langCode,
            page * page_size * 3,
            created_version_filter,
            updated_version_filter,
        )
        for textHash, avatarId in voice_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['_preferredSourceType'] = "voice"
            obj['talker'] = databaseHelper.getCharterName(avatarId, langCode)
            ans.append(obj)
        total = _count_fetter_by_speaker_keyword_cached(
            speaker_keyword,
            langCode,
            created_version_filter,
            updated_version_filter,
        )
        _sort_entries_by_match(
            ans,
            speaker_keyword,
            langCode,
            lambda entry: [entry.get('talker')],
        )
        return _paginate(ans, page, page_size, total)

    if normalized_source_type == "story":
        if voice_filter == "with":
            return [], 0
        story_rows = databaseHelper.selectAvatarStoryBySpeakerKeyword(
            speaker_keyword,
            langCode,
            page * page_size * 3,
            created_version_filter,
            updated_version_filter,
        )
        for textHash, avatarId in story_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['_preferredSourceType'] = "story"
            obj['talker'] = databaseHelper.getCharterName(avatarId, langCode)
            ans.append(obj)
        total = _count_story_by_speaker_keyword_cached(
            speaker_keyword,
            langCode,
            created_version_filter,
            updated_version_filter,
        )
        _sort_entries_by_match(
            ans,
            speaker_keyword,
            langCode,
            lambda entry: [entry.get('talker')],
        )
        return _paginate(ans, page, page_size, total)

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


def _handle_speaker_and_keyword_query(speaker_keyword: str, keyword_trim: str, langCode: int, page: int, page_size: int, voice_filter: str, created_version_filter: str | None, updated_version_filter: str | None, source_type_filter: str | None = None) -> tuple[list[dict], int]:
    """
    处理说话者和关键词查询
    """
    normalized_source_type = _normalize_source_type_filter(source_type_filter)
    if normalized_source_type and normalized_source_type not in ("dialogue", "voice", "story"):
        return [], 0

    ans = []
    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    seen_hashes = set()

    if normalized_source_type == "voice":
        if voice_filter == "without":
            return [], 0
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
            obj['_preferredSourceType'] = "voice"
            obj['talker'] = databaseHelper.getCharterName(avatarId, langCode)
            ans.append(obj)
        total = _count_fetter_by_speaker_and_keyword_cached(
            speaker_keyword,
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
        )
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

    if normalized_source_type == "story":
        if voice_filter == "with":
            return [], 0
        story_rows = databaseHelper.selectAvatarStoryBySpeakerAndKeyword(
            speaker_keyword,
            keyword_trim,
            langCode,
            page * page_size * 3,
            created_version_filter,
            updated_version_filter,
        )
        for textHash, avatarId in story_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['_preferredSourceType'] = "story"
            obj['talker'] = databaseHelper.getCharterName(avatarId, langCode)
            ans.append(obj)
        total = _count_story_by_speaker_and_keyword_cached(
            speaker_keyword,
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
        )
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

    dialogue_total = _count_dialogue_by_talker_and_keyword_cached(
        speaker_keyword,
        keyword_trim,
        langCode,
        created_version_filter,
        updated_version_filter,
    )
    total = dialogue_total

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
    voice_total = _count_fetter_by_speaker_and_keyword_cached(
        speaker_keyword,
        keyword_trim,
        langCode,
        created_version_filter,
        updated_version_filter,
    )
    total += voice_total

    ans = _apply_voice_filter(ans, voice_filter)
    if normalized_source_type:
        ans = _filter_entries_by_source_type(ans, normalized_source_type)
        if normalized_source_type == "dialogue":
            total = dialogue_total
        elif normalized_source_type == "voice":
            total = voice_total
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


def _handle_keyword_only_query(keyword: str, keyword_trim: str, langCode: int, page: int, page_size: int, voice_filter: str, created_version_filter: str | None, updated_version_filter: str | None, source_type_filter: str | None = None) -> tuple[list[dict], int]:
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
        "Costume": "装扮",
        "Relic": "圣遗物",
        "Weapon": "武器",
        "Wings": "风之翼",
    }

    safe_page = page if page and page > 0 else 1
    safe_size = page_size if page_size and page > 0 else 50

    if voice_filter == "all":
        return _handle_all_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter, source_type_filter)
    else:
        return _handle_specific_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, voice_filter, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter, source_type_filter)


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
            obj = queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False)
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
            ans.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels, isSearchPhase=True))
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
            obj = queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False)
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
                ans.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels, isSearchPhase=True))
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


def _handle_all_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter, source_type_filter=None):
    normalized_source_type = _normalize_source_type_filter(source_type_filter)
    candidate_limit = max(safe_size, safe_page * safe_size)
    candidates = []
    hash_matches_source_type = bool(
        hash_obj is not None and _text_hash_matches_source_type(
            hash_value,
            normalized_source_type,
            sourceLangCode,
            hash_obj,
        )
    )

    if hash_extra and hash_obj is not None and hash_matches_source_type:
        if normalized_source_type in {"voice", "story"}:
            hash_obj['_preferredSourceType'] = normalized_source_type
        candidates.append(hash_obj)
        if hash_value is not None:
            text_hashes_seen.add(hash_value)

    if normalized_source_type == "textmap":
        # textmap 专用路径
        total = _count_textmap_from_keyword_cached(
            keyword, langCode, created_version_filter, updated_version_filter,
        ) + (1 if hash_extra and hash_matches_source_type else 0)

        rows = databaseHelper.selectTextMapFromKeywordPaged(
            keyword, langCode, candidate_limit, 0,
            hash_value if is_hash_query else None, None,
            created_version_filter, updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            candidates.append(queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False))

    elif normalized_source_type in _get_db_filterable_source_types():
        # 数据库层 JOIN 过滤：dialogue, voice, quest, entity types
        db_source_type = normalized_source_type
        if db_source_type is None:
            db_source_type = ""
        total = databaseHelper.countTextMapFromKeywordBySourceType(
            keyword, langCode, db_source_type,
            created_version_filter, updated_version_filter,
        ) + (1 if hash_extra and hash_matches_source_type else 0)

        rows = databaseHelper.selectTextMapFromKeywordBySourceType(
            keyword, langCode, db_source_type, candidate_limit, 0,
            created_version_filter, updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            obj = queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False)
            if db_source_type in {"voice", "story"}:
                obj['_preferredSourceType'] = db_source_type
            candidates.append(obj)

    elif normalized_source_type == "readable":
        # 仅加载 readable
        if langStr:
            total = _count_readable_from_keyword_cached(
                keyword, langCode, langStr, created_version_filter, updated_version_filter,
            )
            readable_contents = databaseHelper.selectReadableFromKeyword(
                keyword, langCode, langStr, candidate_limit, None,
                created_version_filter, updated_version_filter,
            )
            for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readable_contents:
                candidates.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels, isSearchPhase=True))
        else:
            total = 0

    elif normalized_source_type == "subtitle":
        # 仅加载 subtitle
        total = _count_subtitle_from_keyword_cached(
            keyword, langCode, created_version_filter, updated_version_filter,
        )
        subtitle_contents = databaseHelper.selectSubtitleFromKeyword(
            keyword, langCode, candidate_limit, None,
            created_version_filter, updated_version_filter,
        )
        for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitle_contents:
            candidates.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))

    else:
        # 无筛选或 unknown：加载所有来源
        total_textmap = _count_textmap_from_keyword_cached(
            keyword, langCode, created_version_filter, updated_version_filter,
        )
        total_readable = 0
        if langStr:
            total_readable = _count_readable_from_keyword_cached(
                keyword, langCode, langStr, created_version_filter, updated_version_filter,
            )
        total_subtitle = _count_subtitle_from_keyword_cached(
            keyword, langCode, created_version_filter, updated_version_filter,
        )
        total = total_textmap + total_readable + total_subtitle + (1 if hash_extra and hash_matches_source_type else 0)

        rows = databaseHelper.selectTextMapFromKeywordPaged(
            keyword, langCode, candidate_limit, 0,
            hash_value if is_hash_query else None, None,
            created_version_filter, updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            candidates.append(queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False))

        if langStr:
            readable_contents = databaseHelper.selectReadableFromKeyword(
                keyword, langCode, langStr, candidate_limit, None,
                created_version_filter, updated_version_filter,
            )
            for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readable_contents:
                candidates.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels, isSearchPhase=True))

        subtitle_contents = databaseHelper.selectSubtitleFromKeyword(
            keyword, langCode, candidate_limit, None,
            created_version_filter, updated_version_filter,
        )
        for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitle_contents:
            candidates.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))

        if normalized_source_type:
            candidates = _filter_entries_by_source_type(candidates, normalized_source_type)
            total = len(candidates)

    _sort_search_results(candidates, keyword_trim, langCode, is_hash_query, hash_value)
    start = (safe_page - 1) * safe_size
    end = start + safe_size
    return candidates[start:end], total


def _handle_specific_voice_filter_ranked(keyword, keyword_trim, langCode, safe_page, safe_size, voice_filter, hash_value, is_hash_query, hash_obj, hash_extra, text_hashes_seen, langs, sourceLangCode, langStr, targetLangStrs, strToLangId, prefix_labels, created_version_filter, updated_version_filter, source_type_filter=None):
    normalized_source_type = _normalize_source_type_filter(source_type_filter)
    hash_matches_source_type = bool(
        hash_obj is not None and _text_hash_matches_source_type(
            hash_value,
            normalized_source_type,
            sourceLangCode,
            hash_obj,
        )
    )
    hash_extra_filtered = False
    if hash_extra and hash_obj is not None:
        hash_has_voice = databaseHelper.hasVoiceForTextHashDb(hash_value)
        if (voice_filter == "with" and hash_has_voice) or (
            voice_filter == "without" and not hash_has_voice
        ):
            hash_extra_filtered = hash_matches_source_type

    candidate_limit = max(safe_size, safe_page * safe_size)
    candidates = []

    if hash_extra_filtered and hash_obj is not None:
        if normalized_source_type in {"voice", "story"}:
            hash_obj['_preferredSourceType'] = normalized_source_type
        candidates.append(hash_obj)
        if hash_value is not None:
            text_hashes_seen.add(hash_value)

    if normalized_source_type == "textmap":
        total = _count_textmap_from_keyword_voice_cached(
            keyword, langCode, voice_filter, created_version_filter, updated_version_filter,
        ) + (1 if hash_extra_filtered else 0)

        rows = databaseHelper.selectTextMapFromKeywordPaged(
            keyword, langCode, candidate_limit, 0,
            hash_value if is_hash_query else None, voice_filter,
            created_version_filter, updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            candidates.append(queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False))

    elif normalized_source_type in _get_db_filterable_source_types():
        # 数据库层 JOIN 过滤 + 语音过滤
        # 注意：voice_filter 需要通过 selectTextMapFromKeywordPaged 而非 BySourceType 处理
        # 先通过 source type JOIN 获取候选，再应用语音过滤
        db_source_type = normalized_source_type
        if db_source_type is None:
            db_source_type = ""
        rows = databaseHelper.selectTextMapFromKeywordBySourceType(
            keyword, langCode, db_source_type, candidate_limit * 3, 0,
            created_version_filter, updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            obj = queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False)
            if db_source_type in {"voice", "story"}:
                obj['_preferredSourceType'] = db_source_type
            candidates.append(obj)
        candidates = _apply_voice_filter(candidates, voice_filter)
        total = len(candidates) + (1 if hash_extra_filtered else 0)

    elif normalized_source_type == "readable":
        if voice_filter == "without" and langStr:
            total = _count_readable_from_keyword_cached(
                keyword, langCode, langStr, created_version_filter, updated_version_filter,
            )
            readable_contents = databaseHelper.selectReadableFromKeyword(
                keyword, langCode, langStr, candidate_limit, None,
                created_version_filter, updated_version_filter,
            )
            for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readable_contents:
                candidates.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels, isSearchPhase=True))
        else:
            total = 0

    elif normalized_source_type == "subtitle":
        if voice_filter == "without":
            total = _count_subtitle_from_keyword_cached(
                keyword, langCode, created_version_filter, updated_version_filter,
            )
            subtitle_contents = databaseHelper.selectSubtitleFromKeyword(
                keyword, langCode, candidate_limit, None,
                created_version_filter, updated_version_filter,
            )
            for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitle_contents:
                candidates.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))
        else:
            total = 0

    else:
        # 无筛选或 unknown
        total_textmap = _count_textmap_from_keyword_voice_cached(
            keyword, langCode, voice_filter, created_version_filter, updated_version_filter,
        )
        total_readable = 0
        total_subtitle = 0
        if voice_filter == "without":
            if langStr:
                total_readable = _count_readable_from_keyword_cached(
                    keyword, langCode, langStr, created_version_filter, updated_version_filter,
                )
            total_subtitle = _count_subtitle_from_keyword_cached(
                keyword, langCode, created_version_filter, updated_version_filter,
            )
        total = total_textmap + total_readable + total_subtitle + (1 if hash_extra_filtered else 0)

        rows = databaseHelper.selectTextMapFromKeywordPaged(
            keyword, langCode, candidate_limit, 0,
            hash_value if is_hash_query else None, voice_filter,
            created_version_filter, updated_version_filter,
        )
        for text_hash, _content, _created_raw, _updated_raw in rows:
            if text_hash in text_hashes_seen:
                continue
            text_hashes_seen.add(text_hash)
            candidates.append(queryTextHashInfo(text_hash, langs, sourceLangCode, queryOrigin=False))

        if voice_filter == "without":
            if langStr:
                readable_contents = databaseHelper.selectReadableFromKeyword(
                    keyword, langCode, langStr, candidate_limit, None,
                    created_version_filter, updated_version_filter,
                )
                for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readable_contents:
                    candidates.append(_build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels, isSearchPhase=True))

            subtitle_contents = databaseHelper.selectSubtitleFromKeyword(
                keyword, langCode, candidate_limit, None,
                created_version_filter, updated_version_filter,
            )
            for fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw in subtitle_contents:
                candidates.append(_build_subtitle_obj(fileName, content, startTime, endTime, subtitleId, created_raw, updated_raw, langs))

        if normalized_source_type:
            candidates = _filter_entries_by_source_type(candidates, normalized_source_type)
            total = len(candidates)

    _sort_search_results(candidates, keyword_trim, langCode, is_hash_query, hash_value)
    start = (safe_page - 1) * safe_size
    end = start + safe_size
    return candidates[start:end], total


def _build_readable_obj(fileName, content, titleTextMapHash, readableId, created_raw, updated_raw, sourceLangCode, langCode, targetLangStrs, strToLangId, prefix_labels, isSearchPhase=False):
    """
    构建可读内容对象
    isSearchPhase=True 时仅返回基本信息以提高搜索性能
    """
    fileHash = zlib.crc32(fileName.encode('utf-8'))
    readable_category_fields = _build_readable_category_fields(fileName, titleTextMapHash)
    origin_label = readable_category_fields["readableCategoryLabel"]

    # 搜索阶段：返回最小化信息
    if isSearchPhase:
        obj = {
            'translates': {},
            'voicePaths': [],
            'hash': fileHash,
            'isTalk': False,
            'isReadable': True,
            'readableId': readableId,
            'fileName': fileName,
            'origin': f"{origin_label}: {fileName}",
            'primarySource': _build_primary_source(
                "readable",
                fileName,
                origin_label,
                {"kind": "readable", "readableId": readableId, "fileName": fileName},
            ),
            'sourceCount': 1,
            **readable_category_fields,
        }
        obj.update(_build_version_fields(created_raw, updated_raw))

        # 只获取必要的翻译信息
        translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)
        for transContent, transLangStr in translations:
            if transLangStr in strToLangId:
                lang_id = strToLangId[transLangStr]
                obj['translates'][str(lang_id)] = _normalize_text_map_content(transContent, lang_id)
        return obj

    # 详情页面：获取完整源信息
    title = _get_text_map_content_with_fallback(
        titleTextMapHash,
        sourceLangCode,
        [langCode],
    )
    fallback_origin = f"{origin_label}: {title}" if title else f"{origin_label}: {fileName}"
    primary_source = _build_primary_source(
        "readable",
        title or fileName,
        origin_label,
        {"kind": "readable", "readableId": readableId, "fileName": fileName},
    )
    origin = fallback_origin
    source_count = 1

    obj = {
        'translates': {},
        'voicePaths': [],
        'hash': fileHash,
        'isTalk': False,
        'isReadable': True,
        'readableId': readableId,
        'fileName': fileName,
        'origin': origin,
        'primarySource': primary_source,
        'sourceCount': source_count,
        **readable_category_fields,
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
        'subtitleId': subtitleId,
        'primarySource': _build_primary_source(
            "subtitle",
            fileName,
            "字幕",
            {"kind": "subtitle", "fileName": fileName, "subtitleId": subtitleId},
        ),
        'sourceCount': 1,
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
    source_type: str | None = None,
):
    """
    获取翻译对象
    """
    speaker_keyword = (speaker or "").strip()
    keyword_trim = keyword.strip()
    created_version_filter = _normalize_version_filter(created_version)
    updated_version_filter = _normalize_version_filter(updated_version)
    source_type_filter = _normalize_source_type_filter(source_type)

    # 生成缓存键
    cache_key = (
        keyword_trim,
        langCode,
        speaker_keyword,
        page,
        page_size,
        voice_filter,
        created_version_filter,
        updated_version_filter,
        source_type_filter,
    )

    # 尝试从缓存中获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return cached_result

    # 使用原有的查询逻辑
    if keyword_trim == "" and speaker_keyword:
        # 仅说话者查询
        result = _handle_speaker_only_query(speaker_keyword, langCode, page, page_size, voice_filter, created_version_filter, updated_version_filter, source_type_filter)
    elif keyword_trim != "" and speaker_keyword:
        # 说话者和关键词查询
        result = _handle_speaker_and_keyword_query(speaker_keyword, keyword_trim, langCode, page, page_size, voice_filter, created_version_filter, updated_version_filter, source_type_filter)
    else:
        # 仅关键词查询
        result = _handle_keyword_only_query(keyword, keyword_trim, langCode, page, page_size, voice_filter, created_version_filter, updated_version_filter, source_type_filter)

    # 为搜索阶段跳过来源查询的条目补充 primarySource
    contents, total = result
    source_lang_code = config.getSourceLanguage()
    _enrich_primary_sources(contents, source_lang_code)
    result = (contents, total)

    # 将结果缓存
    search_cache.set(cache_key, result)
    return result


def searchNameEntries(
    keyword: str,
    langCode: int,
    created_version: str | None = None,
    updated_version: str | None = None,
    quest_source_type: str | None = None,
    speaker_keyword: str | None = None,
    readable_category: str | None = None,
):
    quests = []
    readables = []
    keyword_trim = (keyword or "").strip()
    speaker_keyword_trim = (speaker_keyword or "").strip()
    created_version_filter = _normalize_version_filter(created_version)
    updated_version_filter = _normalize_version_filter(updated_version)
    quest_source_type_filter = (quest_source_type or "").strip().upper() or None
    readable_category_filter = (readable_category or "").strip().upper() or None
    if (
        not keyword_trim
        and not created_version_filter
        and not updated_version_filter
        and not quest_source_type_filter
        and not speaker_keyword_trim
        and not readable_category_filter
    ):
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

    def build_preview(content: str | None):
        return _build_keyword_preview(content, keyword_trim)

    quest_map = {}
    if keyword_trim:
        questMatches = databaseHelper.selectQuestByTitleKeyword(
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
            quest_source_type_filter,
        )
        for questId, questTitle, source_type, created_raw, updated_raw in questMatches:
            chapterName = databaseHelper.getQuestChapterName(questId, langCode)
            quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_quest_source_type_fields(source_type),
                **_build_version_fields(created_raw, updated_raw),
            }

        chapterMatches = databaseHelper.selectQuestByChapterKeyword(
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
            quest_source_type_filter,
        )
        for questId, questTitle, chapterTitle, chapterNum, source_type, created_raw, updated_raw in chapterMatches:
            if questId in quest_map:
                continue
            chapterName = format_chapter_name(chapterNum, chapterTitle)
            quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_quest_source_type_fields(source_type),
                **_build_version_fields(created_raw, updated_raw),
            }
        questIdMatches = databaseHelper.selectQuestByIdContains(
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
            quest_source_type_filter,
        )
        for questId, questTitle, source_type, created_raw, updated_raw in questIdMatches:
            if questId in quest_map:
                continue
            chapterName = databaseHelper.getQuestChapterName(questId, langCode)
            quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_quest_source_type_fields(source_type),
                **_build_version_fields(created_raw, updated_raw),
            }
        questContentMatches = databaseHelper.selectQuestByContentKeyword(
            keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
            quest_source_type_filter,
        )
        for questId, title_hash, questTitle, matched_text, source_type, created_raw, updated_raw, best_rank in questContentMatches:
            preview = build_preview(_normalize_text_map_content(matched_text, langCode))
            if questId in quest_map:
                if preview and not quest_map[questId].get("contentPreview"):
                    quest_map[questId]["contentPreview"] = preview
                if not quest_map[questId].get("contentMatchType"):
                    quest_map[questId]["contentMatchType"] = "description" if best_rank == 0 else "dialogue"
                continue
            chapterName = databaseHelper.getQuestChapterName(questId, langCode)
            resolved_title = questTitle or _get_text_map_content_with_fallback(
                title_hash,
                langCode,
                [config.getSourceLanguage()],
            )
            quest_map[questId] = {
                "questId": questId,
                "title": resolved_title or str(questId),
                "chapterName": chapterName,
                "contentPreview": preview,
                "contentMatchType": "description" if best_rank == 0 else "dialogue",
                **_build_quest_source_type_fields(source_type),
                **_build_version_fields(created_raw, updated_raw),
            }
    else:
        if created_version_filter or updated_version_filter or quest_source_type_filter:
            questMatches = databaseHelper.selectQuestByVersion(
                langCode,
                created_version_filter,
                updated_version_filter,
                quest_source_type_filter,
            )
            for questId, questTitle, source_type, created_raw, updated_raw in questMatches:
                chapterName = databaseHelper.getQuestChapterName(questId, langCode)
                quest_map[questId] = {
                    "questId": questId,
                    "title": questTitle,
                    "chapterName": chapterName,
                    **_build_quest_source_type_fields(source_type),
                    **_build_version_fields(created_raw, updated_raw),
                }

    if speaker_keyword_trim:
        speaker_quest_map: dict[int, dict] = {}
        speakerMatches = databaseHelper.selectQuestByNpcSpeakerKeyword(
            speaker_keyword_trim,
            langCode,
            created_version_filter,
            updated_version_filter,
            quest_source_type_filter,
        )
        for questId, questTitle, source_type, created_raw, updated_raw in speakerMatches:
            chapterName = databaseHelper.getQuestChapterName(questId, langCode)
            speaker_quest_map[questId] = {
                "questId": questId,
                "title": questTitle,
                "chapterName": chapterName,
                **_build_quest_source_type_fields(source_type),
                **_build_version_fields(created_raw, updated_raw),
            }

        speaker_norm = _normalize_speaker(speaker_keyword_trim, langCode)
        for talkerType in ("TALK_ROLE_PLAYER", "TALK_ROLE_MATE_AVATAR"):
            talkerName = databaseHelper.getTalkerName(talkerType, 0, langCode)
            if not talkerName:
                continue
            if speaker_norm not in _normalize_speaker(talkerName, langCode):
                continue
            specialMatches = databaseHelper.selectQuestByTalkerType(
                talkerType,
                langCode,
                created_version_filter,
                updated_version_filter,
                quest_source_type_filter,
            )
            for questId, questTitle, source_type, created_raw, updated_raw in specialMatches:
                if questId in speaker_quest_map:
                    continue
                chapterName = databaseHelper.getQuestChapterName(questId, langCode)
                speaker_quest_map[questId] = {
                    "questId": questId,
                    "title": questTitle,
                    "chapterName": chapterName,
                    **_build_quest_source_type_fields(source_type),
                    **_build_version_fields(created_raw, updated_raw),
                }

        if keyword_trim or created_version_filter or updated_version_filter or quest_source_type_filter:
            quest_map = {
                questId: entry
                for questId, entry in quest_map.items()
                if questId in speaker_quest_map
            }
        else:
            quest_map = speaker_quest_map

    quests.extend(entry for entry in quest_map.values() if _has_visible_quest_card_content(entry))

    langMap = databaseHelper.getLangCodeMap()
    if (
        not speaker_keyword_trim
        and langCode in langMap
        and (keyword_trim or created_version_filter or updated_version_filter or readable_category_filter)
    ):
        langStr = langMap[langCode]
        readable_seen = set()
        readable_entry_map = {}
        if keyword_trim:
            readableMatches = databaseHelper.selectReadableByTitleKeyword(
                keyword_trim,
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
                readable_category_filter,
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
                entry = {
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": resolved_title or fileName,
                    "titleTextMapHash": resolved_hash,
                    **_build_readable_category_fields(fileName),
                    **_build_version_fields(created_raw, updated_raw),
                }
                readables.append(entry)
                readable_entry_map[(readableId, fileName)] = entry
                readable_seen.add((readableId, fileName))
            readableFileMatches = databaseHelper.selectReadableByFileNameContains(
                keyword_trim,
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
                readable_category_filter,
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
                entry = {
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": resolved_title or fileName,
                    "titleTextMapHash": resolved_hash,
                    **_build_readable_category_fields(fileName),
                    **_build_version_fields(created_raw, updated_raw),
                }
                readables.append(entry)
                readable_entry_map[key] = entry
            readableContentMatches = databaseHelper.selectReadableFromKeyword(
                keyword_trim,
                langCode,
                langStr,
                limit=200,
                offset=None,
                created_version=created_version_filter,
                updated_version=updated_version_filter,
                category=readable_category_filter,
            )
            for fileName, content, titleTextMapHash, readableId, created_raw, updated_raw in readableContentMatches:
                key = (readableId, fileName)
                preview = build_preview(content)
                if key in readable_seen:
                    if preview and key in readable_entry_map and not readable_entry_map[key].get("contentPreview"):
                        readable_entry_map[key]["contentPreview"] = preview
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
                entry = {
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": title or fileName,
                    "titleTextMapHash": resolved_hash,
                    "contentPreview": preview,
                    **_build_readable_category_fields(fileName),
                    **_build_version_fields(created_raw, updated_raw),
                }
                readables.append(entry)
                readable_entry_map[key] = entry
        else:
            readableMatches = databaseHelper.selectReadableByVersion(
                langCode,
                langStr,
                created_version_filter,
                updated_version_filter,
                category=readable_category_filter,
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
                entry = {
                    "fileName": fileName,
                    "readableId": readableId,
                    "title": resolved_title or fileName,
                    "titleTextMapHash": resolved_hash,
                    **_build_readable_category_fields(fileName),
                    **_build_version_fields(created_raw, updated_raw),
                }
                readables.append(entry)
                readable_entry_map[(readableId, fileName)] = entry

    _sort_entries_by_match_with_exact_id(
        quests,
        keyword_trim,
        langCode,
        lambda entry: [
            entry.get("title"),
            entry.get("chapterName"),
            entry.get("contentPreview"),
            str(entry.get("questId", "")),
        ],
        lambda entry: entry.get("questId"),
        lambda entry: str(entry.get("questId", "")),
    )
    _sort_entries_by_match_with_exact_id(
        readables,
        keyword_trim,
        langCode,
        lambda entry: [entry.get("title"), entry.get("fileName"), entry.get("contentPreview")],
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


def _build_dialogue_group_key(
    talk_id: int | None,
    coop_quest_id: int | None,
    dialogue_id_fallback: int | None,
) -> str:
    if dialogue_id_fallback is not None:
        return f"dialogue:{dialogue_id_fallback}"
    coop_key = "null" if coop_quest_id is None else str(coop_quest_id)
    return f"talk:{int(talk_id or 0)}:{coop_key}"


def _build_dialogue_group_preview_lines(
    raw_rows: list[tuple[int, str, int, int]],
    source_lang_code: int,
    *,
    max_lines: int = 3,
) -> list[str]:
    preview_lines: list[str] = []
    for text_hash, talker_type, talker_id, _dialogue_id in raw_rows[:max_lines]:
        text = _normalize_text_map_content(
            databaseHelper.getTextMapContent(text_hash, source_lang_code),
            source_lang_code,
        )
        talker = databaseHelper.getTalkerName(talker_type, talker_id, source_lang_code)
        if not text:
            continue
        line = f"{talker}: {text}" if talker else text
        normalized = _normalize_preview_text(line)
        if not normalized:
            continue
        if len(normalized) > 96:
            normalized = normalized[:93].rstrip() + "..."
        preview_lines.append(normalized)
    return preview_lines


def searchNpcDialogueEntries(
    keyword: str,
    langCode: int,
    npc_created_version: str | None = None,
    npc_updated_version: str | None = None,
):
    entries = []
    matches = databaseHelper.selectNpcDialogueSearchEntries(
        keyword,
        langCode,
        created_version=npc_created_version,
        updated_version=npc_updated_version,
        limit=200,
    )
    for npcName, npc_ids_raw, created_raw, dialogue_updated_raw in matches:
        npc_ids = [
            int(item)
            for item in str(npc_ids_raw or "").split(",")
            if str(item).strip()
        ]
        if not npc_ids:
            continue
        entries.append(
            {
                "npcIds": npc_ids,
                "name": npcName or "",
                **_build_version_fields(created_raw, dialogue_updated_raw),
            }
        )

    _sort_entries_by_match(
        entries,
        keyword,
        langCode,
        lambda entry: [entry.get("name")],
    )
    return {"npcs": entries}


def getNpcDialogues(
    npcId: int | None = None,
    npcIds: list[int] | None = None,
    searchLang: int | None = None,
    dialogue_created_version: str | None = None,
    dialogue_updated_version: str | None = None,
    page: int = 1,
    page_size: int = 20,
):
    source_lang_code = config.getSourceLanguage()
    resolved_npc_ids: list[int] = []
    if npcIds:
        for value in npcIds:
            try:
                resolved = int(value)
            except Exception:
                continue
            if resolved <= 0 or resolved in resolved_npc_ids:
                continue
            resolved_npc_ids.append(resolved)
    elif npcId is not None:
        try:
            resolved = int(npcId)
        except Exception:
            resolved = 0
        if resolved > 0:
            resolved_npc_ids.append(resolved)

    if not resolved_npc_ids:
        raise Exception("未找到对应的 NPC 名称组！")

    npc_name = databaseHelper.getTalkerName("TALK_ROLE_NPC", resolved_npc_ids[0], source_lang_code)

    safe_page_size = max(1, int(page_size) if page_size else 20)
    safe_page = max(1, int(page) if page else 1)

    created_filter = _normalize_version_filter(dialogue_created_version)
    updated_filter = _normalize_version_filter(dialogue_updated_version)
    use_merged_summary = len(resolved_npc_ids) > 1 or bool(created_filter or updated_filter)
    use_version_summary = bool(created_filter or updated_filter)

    if use_merged_summary:
        if use_version_summary:
            summaries = databaseHelper.selectNpcNonTaskDialogueGroupSummariesForNpcIds(
                resolved_npc_ids,
                created_version=dialogue_created_version,
                updated_version=dialogue_updated_version,
            )
            total_groups = len(summaries)
            total_pages = max(1, math.ceil(total_groups / safe_page_size)) if total_groups else 1
            if safe_page > total_pages:
                safe_page = total_pages
            paged_summaries = summaries[(safe_page - 1) * safe_page_size : safe_page * safe_page_size]
        else:
            total_groups = databaseHelper.countNpcNonTaskDialogueGroupsForNpcIds(resolved_npc_ids)
            total_pages = max(1, math.ceil(total_groups / safe_page_size)) if total_groups else 1
            if safe_page > total_pages:
                safe_page = total_pages
            paged_summaries = databaseHelper.selectNpcNonTaskDialogueGroupPageForNpcIds(
                resolved_npc_ids,
                safe_page_size,
                (safe_page - 1) * safe_page_size,
            )
    else:
        single_npc_id = resolved_npc_ids[0]
        total_groups = databaseHelper.countNpcNonTaskDialogueGroups(single_npc_id)
        total_pages = max(1, math.ceil(total_groups / safe_page_size)) if total_groups else 1
        if safe_page > total_pages:
            safe_page = total_pages
        paged_summaries = databaseHelper.selectNpcNonTaskDialogueGroupPage(
            single_npc_id,
            safe_page_size,
            (safe_page - 1) * safe_page_size,
        )

    groups = []
    for summary in paged_summaries:
        if use_version_summary:
            talkId, coopQuestId, dialogueIdFallback, sortDialogueId, lineCount, created_raw, updated_raw = summary
        else:
            talkId, coopQuestId, dialogueIdFallback, sortDialogueId, lineCount = summary
            created_raw, updated_raw = databaseHelper.getDialogueGroupVersionInfo(
                int(talkId or 0),
                coopQuestId,
                dialogueIdFallback,
                source_lang_code,
            )
        preview_rows = databaseHelper.selectDialogueGroupContentPaged(
            int(talkId or 0),
            coopQuestId,
            dialogueIdFallback,
            3,
            0,
        )
        groups.append(
            {
                "groupKey": _build_dialogue_group_key(talkId, coopQuestId, dialogueIdFallback),
                "talkId": int(talkId or 0),
                "coopQuestId": coopQuestId,
                "dialogueIdFallback": dialogueIdFallback,
                "firstDialogueId": int(sortDialogueId or 0),
                "lineCount": int(lineCount or 0),
                "previewLines": _build_dialogue_group_preview_lines(preview_rows, source_lang_code),
                **_build_version_fields(created_raw, updated_raw),
            }
        )

    return {
        "npcId": int(resolved_npc_ids[0]),
        "npcIds": resolved_npc_ids,
        "npcName": npc_name,
        "page": safe_page,
        "pageSize": safe_page_size,
        "totalGroups": total_groups,
        "groups": groups,
    }


def getDialogueGroup(
    talkId: int,
    coopQuestId: int | None = None,
    dialogueIdFallback: int | None = None,
    searchLang: int | None = None,
    page: int = 1,
    page_size: int = 200,
):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()
    if sourceLangCode and sourceLangCode not in langs:
        langs.append(sourceLangCode)

    resolved_talk_id = int(talkId or 0)
    resolved_coop_quest_id = coopQuestId
    if dialogueIdFallback is not None:
        dialogue_info = databaseHelper.getDialogueInfoById(int(dialogueIdFallback))
        if dialogue_info is None:
            raise Exception("未找到对应的非任务对话分组！")
        _text_hash, _talker_type, _talker_id, _dialogue_id, resolved_talk_id, resolved_coop_quest_id = dialogue_info

    safe_page_size = max(1, int(page_size) if page_size else 200)
    total = databaseHelper.countDialogueGroupContent(
        resolved_talk_id,
        resolved_coop_quest_id,
        dialogueIdFallback,
    )
    if total <= 0:
        raise Exception("未找到对应的非任务对话分组！")

    total_pages = max(1, math.ceil(total / safe_page_size))
    safe_page = max(1, int(page) if page else 1)
    if safe_page > total_pages:
        safe_page = total_pages

    raw_dialogues = databaseHelper.selectDialogueGroupContentPaged(
        resolved_talk_id,
        resolved_coop_quest_id,
        dialogueIdFallback,
        safe_page_size,
        (safe_page - 1) * safe_page_size,
    )
    dialogues = []
    for dialogue_text_hash, talkerType, talkerId, dialogueId in raw_dialogues:
        obj = queryTextHashInfo(dialogue_text_hash, langs, sourceLangCode, False)
        obj["talker"] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
        obj["dialogueId"] = dialogueId
        obj["talkId"] = resolved_talk_id
        dialogues.append(obj)

    created_raw, updated_raw = databaseHelper.getDialogueGroupVersionInfo(
        resolved_talk_id,
        resolved_coop_quest_id,
        dialogueIdFallback,
        sourceLangCode,
    )
    return {
        "talkQuestName": "非任务对话",
        "questId": None,
        "talkId": resolved_talk_id,
        "coopQuestId": resolved_coop_quest_id,
        "dialogueIdFallback": dialogueIdFallback,
        "dialogues": dialogues,
        "total": total,
        "page": safe_page,
        "pageSize": safe_page_size,
        **_build_version_fields(created_raw, updated_raw),
    }


def _legacy_getAvatarVoices(avatarId: int, searchLang: int | None = None):
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





def _legacy_searchAvatarVoicesByFilters(
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
    questDescription = databaseHelper.getQuestDescription(questId, sourceLangCode)
    questLongDescription = databaseHelper.getQuestLongDescription(questId, sourceLangCode)
    stepTitleMap = databaseHelper.getQuestStepTitleMap(questId, sourceLangCode)
    created_raw, updated_raw = databaseHelper.getQuestVersionInfo(questId, sourceLangCode)

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
        obj['stepTitle'] = stepTitleMap.get(talkId) or ""
        dialogues.append(obj)

    return {
        "talkQuestName": questCompleteName,
        "questId": questId,
        "questDescription": questDescription,
        "questLongDescription": questLongDescription,
        "talkId": 0,
        "dialogues": dialogues,
        **_build_version_fields(created_raw, updated_raw),
    }, total


# 根据hash值查询整个对话的内容
def getTalkFromHash(
    textHash: int,
    searchLang: int | None = None,
    page: int | None = None,
    page_size: int = 200,
):
    requested_text_hash = textHash
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
            "questId": None,
            "talkId": 0,
            "dialogues": [obj]
        }

    talkId, talkerType, talkerId, coopQuestId = talkInfo
    if coopQuestId is None:
        questId = databaseHelper.getTalkQuestId(talkId)
        questCompleteName = databaseHelper.getTalkQuestName(talkId, sourceLangCode)
    else:
        questId = coopQuestId // 100
        questCompleteName = databaseHelper.getCoopTalkQuestName(coopQuestId, sourceLangCode)

    safe_page_size = max(1, int(page_size) if page_size else 200)
    total = databaseHelper.countTalkContent(talkId, coopQuestId)
    if page is None:
        safe_page = databaseHelper.getTalkContentPageForTextHash(
            requested_text_hash,
            talkId,
            coopQuestId,
            safe_page_size,
        )
    else:
        safe_page = max(1, int(page))

    total_pages = max(1, math.ceil(total / safe_page_size)) if total else 1
    if safe_page > total_pages:
        safe_page = total_pages

    offset = (safe_page - 1) * safe_page_size
    rawDialogues = databaseHelper.selectTalkContentPaged(
        talkId,
        coopQuestId,
        safe_page_size,
        offset,
    )
    dialogues = []

    if rawDialogues is None:
        rawDialogues = []

    for rawDialogue in rawDialogues:
        dialogue_text_hash, talkerType, talkerId, dialogueId = rawDialogue
        obj = queryTextHashInfo(dialogue_text_hash, langs, sourceLangCode, False)
        obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
        obj['dialogueId'] = dialogueId
        obj['isSelectedHash'] = obj.get('hash') == requested_text_hash
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
        "questId": questId,
        "talkId": talkId,
        "dialogues": dialogues,
        "total": total,
        "page": safe_page,
        "pageSize": safe_page_size,
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


def searchCatalog(keyword: str, langCode: int, sourceTypeCode: int | None = None,
                  subCategory: int | None = None, page: int = 1, pageSize: int = 50,
                  createdVersion: str | None = None, updatedVersion: str | None = None):
    import time
    start = time.time()
    offset = (max(1, page) - 1) * pageSize
    rows = databaseHelper.selectCatalogEntities(
        keyword, langCode,
        source_type_code=sourceTypeCode,
        sub_category=subCategory,
        limit=pageSize,
        offset=offset,
        created_version=createdVersion,
        updated_version=updatedVersion,
    )
    total = databaseHelper.countCatalogEntities(
        keyword, langCode,
        source_type_code=sourceTypeCode,
        sub_category=subCategory,
        created_version=createdVersion,
        updated_version=updatedVersion,
    )
    results = []
    for entity_id, stc, title_hash, sub_cat, title_text, created_raw, updated_raw in rows:
        _, source_label = _get_entity_source_meta(stc)
        sub_label = _get_sub_category_label(sub_cat)
        # enrichment: 构建 primarySource
        primary_source = _build_primary_source(
            source_type=str(stc),
            title=title_text or str(entity_id),
            subtitle=sub_label,
            detail_query={
                "kind": "entity",
                "sourceTypeCode": int(stc),
                "entityId": int(entity_id),
            },
        )
        results.append({
            "entityId": int(entity_id),
            "sourceTypeCode": int(stc),
            "sourceTypeLabel": source_label,
            "subCategoryLabel": sub_label,
            "title": title_text or str(entity_id),
            "primarySource": primary_source,
            **_build_version_fields(created_raw, updated_raw),
        })
    elapsed = (time.time() - start) * 1000
    return {
        "contents": results,
        "total": total,
        "page": page,
        "pageSize": pageSize,
        "time": elapsed,
    }


def getCatalogSubCategories():
    """Return the full sub-category mapping for the frontend dropdown."""
    result = {str(k): v for k, v in _SUB_CATEGORY_LABELS.items() if v}
    custom = _load_custom_categories()
    for code, label in custom.get("sub_categories", {}).items():
        result.setdefault(str(code), label)
    return result


def getCatalogSubCategoryGroups():
    """Return available sub-categories for each main category based on catalog data."""
    known_sub_categories = getCatalogSubCategories()
    groups: dict[str, list[str]] = {}
    for source_type_code, sub_category in databaseHelper.selectCatalogCategoryPairs():
        source_key = str(source_type_code)
        sub_key = str(sub_category)
        if sub_key != _CATALOG_OTHER_SUB_CATEGORY_CODE and sub_key not in known_sub_categories:
            continue
        bucket = groups.setdefault(source_key, [])
        if sub_key not in bucket:
            bucket.append(sub_key)
    return groups


def getCatalogUncategorizedSubCategory():
    return {
        "value": _CATALOG_OTHER_SUB_CATEGORY_CODE,
        "label": _CATALOG_OTHER_SUB_CATEGORY_LABEL,
    }


def getCatalogMainCategories():
    """Return main categories that exist in the entity source system."""
    result: dict[str, str] = {}
    seen_labels: set[str] = set()
    for code, (_, label) in _ENTITY_SOURCE_META.items():
        if not label or label in seen_labels:
            continue
        result[str(code)] = label
        seen_labels.add(label)
    custom = _load_custom_categories()
    for code, label in custom.get("source_types", {}).items():
        result.setdefault(str(code), label)
    return result


def getEntityTexts(sourceTypeCode: int, entityId: int, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    source_type_code = int(sourceTypeCode)
    entity_id = int(entityId)
    rows = databaseHelper.selectEntityTextHashesByEntity(source_type_code, entity_id)
    if not rows:
        return {
            "sourceTypeCode": source_type_code,
            "entityId": entity_id,
            "title": None,
            "entries": [],
            "missingBody": False,
            "emptyMessage": "",
            **_build_version_fields(None, None),
        }

    source_type, source_label = _get_entity_source_meta(source_type_code)

    _first_title_hash = rows[0][1]
    _first_sub_category = rows[0][3] if len(rows[0]) > 3 else 0
    sub_category_label = _get_sub_category_label(_first_sub_category)
    title_lang = searchLang or sourceLangCode
    title = _get_text_map_content_with_fallback(_first_title_hash, title_lang, [sourceLangCode])
    if not title:
        title = str(entity_id)

    created_raw, updated_raw = databaseHelper.getCatalogEntityVersionInfo(source_type_code, entity_id, sourceLangCode)
    entries = []
    for row in rows:
        text_hash, title_hash, extra = row[0], row[1], row[2]
        field_code = extra & 0xFF
        gender_code = (extra >> 8) & 0xFF
        if source_type in ("costume", "suit") and extra in (1, 2):
            gender_code = extra
            field_code = 1

        if source_type in ("costume", "suit", "dressing"):
            field_label = "介绍"
        else:
            field_label_map = {
                1: "描述",
                2: "效果",
                3: "特殊",
                4: "类型",
                5: "标题",
                6: "图鉴描述",
            }
            field_label = field_label_map.get(field_code, "描述")

        subtitle = f"{source_label} {entity_id}"
        if gender_code in (1, 2):
            subtitle = subtitle + (" · 男" if gender_code == 1 else " · 女")

        text_obj = queryTextHashInfo(int(text_hash), langs, sourceLangCode, queryOrigin=True)
        if not text_obj or not text_obj.get("translates"):
            continue
        entries.append({
            "fieldLabel": field_label,
            "subtitle": subtitle,
            "textHash": int(text_hash),
            "titleHash": int(title_hash),
            "text": text_obj,
        })

    has_direct_text_entries = len(entries) > 0
    entries.extend(_collect_entity_readable_entries(source_type, entity_id, _first_title_hash, f"{source_label} {entity_id}", langs, sourceLangCode, _first_sub_category))
    missing_body = not has_direct_text_entries and len(entries) == 0

    return {
        "sourceType": source_type,
        "sourceTypeLabel": source_label,
        "subCategoryLabel": sub_category_label,
        "sourceTypeCode": source_type_code,
        "entityId": entity_id,
        "title": title,
        "entries": entries,
        "missingBody": missing_body,
        "emptyMessage": _ENTITY_EMPTY_BODY_MESSAGE if missing_body else "",
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

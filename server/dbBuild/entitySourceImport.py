import json
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from itertools import chain
from typing import Any

from DBConfig import DATA_PATH, conn
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched, load_json_file
from version_control import (
    _build_version_preference_case_sql,
    ensure_version_schema,
    get_current_version,
    get_or_create_version_id,
)

import entity_constants
from entity_constants import (
    # 大分类
    SOURCE_TYPE_ITEM, SOURCE_TYPE_FOOD, SOURCE_TYPE_FURNISHING,
    SOURCE_TYPE_COSTUME, SOURCE_TYPE_SUIT, SOURCE_TYPE_WEAPON, SOURCE_TYPE_RELIQUARY,
    SOURCE_TYPE_MONSTER, SOURCE_TYPE_CREATURE,
    SOURCE_TYPE_DRESSING, SOURCE_TYPE_GCG, SOURCE_TYPE_AVATAR_INTRO,
    SOURCE_TYPE_ACHIEVEMENT, SOURCE_TYPE_VIEWPOINT, SOURCE_TYPE_DUNGEON, SOURCE_TYPE_LOADING_TIP,
    SOURCE_TYPE_QIANXING_EMOJI, SOURCE_TYPE_QIANXING_POSE, SOURCE_TYPE_QIANXING_EFFECT,
    SOURCE_TYPE_QIANXING_HALL,
    # 字段
    FIELD_DESC, FIELD_EFFECT_DESC, FIELD_SPECIAL_DESC, FIELD_TYPE_DESC, FIELD_TITLE, FIELD_CODEX_DESC, FIELD_SKILL_DESC,
    # 二级分类
    SUB_NONE, SUB_CARD,
    SUB_COSTUME_DRESS, SUB_AVATAR_SKILL, SUB_QIANXING_PARADOX, SUB_QIANXING_SUIT,
    SUB_QIANXING_EMOJI, SUB_QIANXING_POSE, SUB_QIANXING_EFFECT, SUB_QIANXING_HALL,
    # 映射与标签
    MATERIAL_TYPE_TO_CATEGORY, SUB_CATEGORY_LABELS, SOURCE_TYPE_LABELS,
    # 工具函数
    append_to_source, _source_type_const_name, _sub_const_name,
)

# ── 外部配置文件路径 ───────────────────────────────────────────────
_DBUILD_DIR = os.path.dirname(os.path.abspath(__file__))
_OVERRIDES_PATH = os.path.join(_DBUILD_DIR, "material_type_overrides.json")


def _load_overrides() -> tuple[dict[str, tuple[int, int]], set[str]]:
    """Load material type overrides from JSON.

    Returns (classified, skip_set):
        classified: materialType → (source_type_code, sub_category)
        skip_set: materialTypes marked as "skip" (use default, don't ask again)
    """
    classified: dict[str, tuple[int, int]] = {}
    skip_set: set[str] = set()
    try:
        with open(_OVERRIDES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return classified, skip_set
    if not isinstance(data, dict):
        return classified, skip_set
    for key, value in data.items():
        if key.startswith("_"):
            continue
        key = str(key).strip()
        if not key:
            continue
        if value == "skip":
            skip_set.add(key)
        elif isinstance(value, list) and len(value) == 2:
            try:
                classified[key] = (int(value[0]), int(value[1]))
            except (ValueError, TypeError):
                pass
    return classified, skip_set


def _save_overrides(classified: dict[str, tuple[int, int]], skip_set: set[str]):
    """Merge and save overrides to JSON (preserves existing entries).

    Output order: materialType entries first, then _source_types / _sub_categories.
    """
    existing_classified, existing_skip = _load_overrides()
    existing_classified.update(classified)
    existing_skip.update(skip_set)
    # preserve _source_types / _sub_categories
    try:
        with open(_OVERRIDES_PATH, "r", encoding="utf-8") as f:
            full = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        full = {}
    reserved = {k: v for k, v in full.items() if k.startswith("_")}
    data: dict[str, Any] = {}
    for key, (code, sub) in sorted(existing_classified.items()):
        data[key] = [code, sub]
    for key in sorted(existing_skip):
        if key not in data:
            data[key] = "skip"
    data.update(reserved)
    with open(_OVERRIDES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_custom_categories() -> tuple[dict[int, str], dict[int, str]]:
    """Load custom source types and sub categories from the overrides JSON.

    Returns (custom_source_types, custom_sub_categories):
        custom_source_types: code → label
        custom_sub_categories: code → label
    """
    custom_st: dict[int, str] = {}
    custom_sub: dict[int, str] = {}
    try:
        with open(_OVERRIDES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return custom_st, custom_sub
    if not isinstance(data, dict):
        return custom_st, custom_sub
    for k, v in (data.get("_source_types") or {}).items():
        try:
            custom_st[int(k)] = str(v)
        except (ValueError, TypeError):
            pass
    for k, v in (data.get("_sub_categories") or {}).items():
        try:
            custom_sub[int(k)] = str(v)
        except (ValueError, TypeError):
            pass
    return custom_st, custom_sub


def _save_custom_categories(source_types: dict[int, str], sub_categories: dict[int, str]):
    """Merge and save custom categories into the overrides JSON."""
    existing_st, existing_sub = _load_custom_categories()
    existing_st.update(source_types)
    existing_sub.update(sub_categories)
    try:
        with open(_OVERRIDES_PATH, "r", encoding="utf-8") as f:
            full = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        full = {}
    full["_source_types"] = {str(k): v for k, v in sorted(existing_st.items())}
    full["_sub_categories"] = {str(k): v for k, v in sorted(existing_sub.items())}
    with open(_OVERRIDES_PATH, "w", encoding="utf-8") as f:
        json.dump(full, f, ensure_ascii=False, indent=2)


def _all_source_type_labels() -> dict[int, str]:
    """Merge built-in and custom source type labels."""
    result = dict(SOURCE_TYPE_LABELS)
    custom_st, _ = _load_custom_categories()
    result.update(custom_st)
    return result


def _all_sub_category_labels() -> dict[int, str]:
    """Merge built-in and custom sub category labels."""
    result = dict(SUB_CATEGORY_LABELS)
    _, custom_sub = _load_custom_categories()
    result.update(custom_sub)
    return result


def _next_source_type_code() -> int:
    all_labels = _all_source_type_labels()
    return max(all_labels.keys(), default=0) + 1


def _next_sub_category_code() -> int:
    all_labels = _all_sub_category_labels()
    return max(all_labels.keys(), default=0) + 1


# ── 未知 materialType 检测 ────────────────────────────────────────

def _scan_unknown_material_types(
    materials: list[dict[str, Any]],
    overrides: dict[str, tuple[int, int]],
    skip_set: set[str],
) -> dict[str, dict]:
    """Scan materials for materialType values not in any known mapping.

    Returns {materialType: {"count": N, "sample_id": int, "sample_name_hash": int}}.
    """
    unknown: dict[str, dict] = {}
    for row in materials:
        # food rows bypass materialType classification
        food_quality = str(row.get("foodQuality") or "").strip()
        if food_quality and food_quality != "FOOD_QUALITY_NONE":
            continue
        material_type = str(row.get("materialType") or "").strip()
        if not material_type:
            continue
        if material_type in MATERIAL_TYPE_TO_CATEGORY:
            continue
        if material_type in overrides:
            continue
        if material_type in skip_set:
            continue
        if material_type not in unknown:
            unknown[material_type] = {
                "count": 0,
                "sample_id": row.get("id"),
                "sample_name_hash": row.get("nameTextMapHash"),
            }
        unknown[material_type]["count"] += 1
    return unknown


# ── 交互式分类提示 ────────────────────────────────────────────────

def _prompt_select_source_type(all_labels: dict[int, str]) -> int:
    """Prompt user to select or create a source type. Returns code."""
    sorted_items = sorted(all_labels.items())
    print("\n  可选大分类:")
    for code, label in sorted_items:
        print(f"    {code:>3} = {label}")
    print("  (输入编号选择已有分类，或输入汉字名称创建新分类)")
    while True:
        ans = input("  大分类> ").strip()
        if not ans:
            return SOURCE_TYPE_ITEM  # default
        # try as number
        try:
            code = int(ans)
            if code in all_labels:
                return code
            print(f"  ⚠ 编号 {code} 不存在，请重新输入")
            continue
        except ValueError:
            pass
        # treat as Chinese label for new category
        new_code = _next_source_type_code()
        new_st = {new_code: ans}
        _save_custom_categories(new_st, {})
        all_labels[new_code] = ans
        print(f"  ✓ 已创建新大分类: {new_code} = {ans}")
        return new_code


def _prompt_select_sub_category(all_labels: dict[int, str]) -> int:
    """Prompt user to select or create a sub category. Returns code."""
    sorted_items = sorted(all_labels.items())
    print("\n  可选二级分类:")
    for code, label in sorted_items:
        display = label if label else "(无)"
        print(f"    {code:>3} = {display}")
    print("  (输入编号选择，输入汉字创建新分类，回车默认为0=无)")
    while True:
        ans = input("  二级分类> ").strip()
        if not ans:
            return SUB_NONE
        try:
            code = int(ans)
            if code in all_labels:
                return code
            print(f"  ⚠ 编号 {code} 不存在，请重新输入")
            continue
        except ValueError:
            pass
        new_code = _next_sub_category_code()
        new_sub = {new_code: ans}
        _save_custom_categories({}, new_sub)
        all_labels[new_code] = ans
        print(f"  ✓ 已创建新二级分类: {new_code} = {ans}")
        return new_code


def _prompt_classify_unknown_types(
    unknown_types: dict[str, dict],
) -> tuple[dict[str, tuple[int, int]], set[str]]:
    """Interactive prompt for classifying unknown materialTypes.

    Returns (classified, skip_set).
    """
    classified: dict[str, tuple[int, int]] = {}
    skip_set: set[str] = set()
    if not unknown_types:
        return classified, skip_set

    st_labels = _all_source_type_labels()
    sub_labels = _all_sub_category_labels()

    print(f"\n{'='*60}")
    print(f"  发现 {len(unknown_types)} 个未知 materialType:")
    print(f"{'='*60}")
    for mt, info in sorted(unknown_types.items(), key=lambda x: -x[1]["count"]):
        print(f"  {mt}  (数量: {info['count']}, 示例ID: {info['sample_id']})")
    print()
    print("  对每个类型，可以选择:")
    print("    - 输入分类 → 保存到配置文件（下次自动使用）")
    print("    - 输入 x   → 标记忽略（使用默认分类，下次不再询问）")
    print("    - 直接回车 → 本次使用默认分类（下次仍会询问）")
    print()

    for mt, info in sorted(unknown_types.items(), key=lambda x: -x[1]["count"]):
        print(f"─── {mt}  (数量: {info['count']}, 示例ID: {info['sample_id']}) ───")
        ans = input("  操作 (回车=默认 / x=忽略 / 其他=分类): ").strip().lower()
        if ans == "x":
            skip_set.add(mt)
            print("  → 已标记为忽略 (将使用默认分类 道具/无)")
            continue
        if not ans:
            print("  → 本次使用默认分类 (道具/无)")
            continue
        # classify
        source_code = _prompt_select_source_type(st_labels)
        sub_code = _prompt_select_sub_category(sub_labels)
        classified[mt] = (source_code, sub_code)
        st_name = st_labels.get(source_code, str(source_code))
        sub_name = sub_labels.get(sub_code, "") or "(无)"
        print(f"  → 分类为: {st_name} / {sub_name}")

    return classified, skip_set


def check_and_classify_interactive(
    materials: list[dict[str, Any]],
    *,
    interactive: bool = True,
) -> dict[str, tuple[int, int]]:
    """Check for unknown materialTypes and optionally prompt for classification.

    Returns the merged override mapping (overrides + newly classified).
    """
    overrides, skip_set = _load_overrides()
    unknown = _scan_unknown_material_types(materials, overrides, skip_set)

    if unknown:
        if interactive and sys.stdin.isatty():
            new_classified, new_skipped = _prompt_classify_unknown_types(unknown)
            if new_classified or new_skipped:
                _save_overrides(new_classified, new_skipped)
                overrides.update(new_classified)
        else:
            print(f"\n⚠ 发现 {len(unknown)} 个未知 materialType（非交互模式，使用默认分类）:")
            for mt, info in sorted(unknown.items(), key=lambda x: -x[1]["count"]):
                print(f"  {mt}  (数量: {info['count']})")
            print()

    return overrides


# ── 数据加载 ──────────────────────────────────────────────────────

_ENTITY_EXCEL_FILES = [
    "MaterialExcelConfigData.json",
    "HomeWorldFurnitureExcelConfigData.json",
    "BeyondCostumeExcelConfigData.json",
    "BeyondCostumeSuitExcelConfigData.json",
    "BeyondEmojiExcelConfigData.json",
    "BeyondPoseExcelConfigData.json",
    "BeyondTransferEffectExcelConfigData.json",
    "BeyondHallExcelConfigData.json",
    "BeyondHallFacilityExcelConfigData.json",
    "AvatarExcelConfigData.json",
    "AvatarSkillDepotExcelConfigData.json",
    "AvatarSkillExcelConfigData.json",
    "AvatarCostumeExcelConfigData.json",
    "WeaponExcelConfigData.json",
    "ReliquaryExcelConfigData.json",
    "AnimalCodexExcelConfigData.json",
    "AnimalDescribeExcelConfigData.json",
    "MonsterDescribeExcelConfigData.json",
    "AchievementExcelConfigData.json",
    "ViewCodexExcelConfigData.json",
    "DungeonExcelConfigData.json",
    "MaterialCodexExcelConfigData.json",
    "LoadingTipsExcelConfigData.json",
    "GCGCardExcelConfigData.json",
    "GCGCharExcelConfigData.json",
    "GCGSkillExcelConfigData.json",
    "GCGKeywordExcelConfigData.json",
    "GCGElementExcelConfigData.json",
]


def _load_all_entity_data(excel_root: str) -> dict[str, Any]:
    """Load all entity-related Excel data and derived maps."""
    materials = _load_rows(os.path.join(excel_root, "MaterialExcelConfigData.json"))
    furnitures = _load_rows(os.path.join(excel_root, "HomeWorldFurnitureExcelConfigData.json"))
    costumes = _load_rows(os.path.join(excel_root, "BeyondCostumeExcelConfigData.json"))
    suits = _load_rows(os.path.join(excel_root, "BeyondCostumeSuitExcelConfigData.json"))
    emojis = _load_rows(os.path.join(excel_root, "BeyondEmojiExcelConfigData.json"))
    poses = _load_rows(os.path.join(excel_root, "BeyondPoseExcelConfigData.json"))
    effects = _load_rows(os.path.join(excel_root, "BeyondTransferEffectExcelConfigData.json"))
    halls = _load_rows(os.path.join(excel_root, "BeyondHallExcelConfigData.json"))
    hall_facilities = _load_rows(os.path.join(excel_root, "BeyondHallFacilityExcelConfigData.json"))
    avatars = _load_rows(os.path.join(excel_root, "AvatarExcelConfigData.json"))
    avatar_skill_depots = _load_rows(os.path.join(excel_root, "AvatarSkillDepotExcelConfigData.json"))
    avatar_skills = _load_rows(os.path.join(excel_root, "AvatarSkillExcelConfigData.json"))
    avatar_costumes = _load_rows(os.path.join(excel_root, "AvatarCostumeExcelConfigData.json"))
    weapons = _load_rows(os.path.join(excel_root, "WeaponExcelConfigData.json"))
    reliquaries = _load_rows(os.path.join(excel_root, "ReliquaryExcelConfigData.json"))
    codex = _load_rows(os.path.join(excel_root, "AnimalCodexExcelConfigData.json"))
    animal_describes = _load_rows(os.path.join(excel_root, "AnimalDescribeExcelConfigData.json"))
    monster_describes = _load_rows(os.path.join(excel_root, "MonsterDescribeExcelConfigData.json"))
    achievements = _load_rows(os.path.join(excel_root, "AchievementExcelConfigData.json"))
    viewpoints = _load_rows(os.path.join(excel_root, "ViewCodexExcelConfigData.json"))
    dungeons = _load_rows(os.path.join(excel_root, "DungeonExcelConfigData.json"))
    material_codex = _load_rows(os.path.join(excel_root, "MaterialCodexExcelConfigData.json"))
    loading_tips = _load_rows(os.path.join(excel_root, "LoadingTipsExcelConfigData.json"))
    gcg_cards = _load_rows(os.path.join(excel_root, "GCGCardExcelConfigData.json"))
    gcg_chars = _load_rows(os.path.join(excel_root, "GCGCharExcelConfigData.json"))
    gcg_skills = _load_rows(os.path.join(excel_root, "GCGSkillExcelConfigData.json"))
    gcg_keywords = _load_rows(os.path.join(excel_root, "GCGKeywordExcelConfigData.json"))
    gcg_elements = _load_rows(os.path.join(excel_root, "GCGElementExcelConfigData.json"))

    describe_title_map = _build_describe_title_map(animal_describes, monster_describes)
    codex_desc_map = _build_codex_desc_map(material_codex)

    return {
        "materials": materials,
        "furnitures": furnitures,
        "costumes": costumes,
        "suits": suits,
        "emojis": emojis,
        "poses": poses,
        "effects": effects,
        "halls": halls,
        "hall_facilities": hall_facilities,
        "avatars": avatars,
        "avatar_skill_depots": avatar_skill_depots,
        "avatar_skills": avatar_skills,
        "avatar_costumes": avatar_costumes,
        "weapons": weapons,
        "reliquaries": reliquaries,
        "codex": codex,
        "achievements": achievements,
        "viewpoints": viewpoints,
        "dungeons": dungeons,
        "loading_tips": loading_tips,
        "gcg_cards": gcg_cards,
        "gcg_chars": gcg_chars,
        "gcg_skills": gcg_skills,
        "gcg_keywords": gcg_keywords,
        "gcg_elements": gcg_elements,
        "describe_title_map": describe_title_map,
        "codex_desc_map": codex_desc_map,
    }


def _print_entity_source_summary(data: dict[str, Any]):
    """Print a summary table of entity source record counts."""
    names = [
        ("materials", "材料"), ("furnitures", "家具"), ("costumes", "装扮"),
        ("suits", "套装"), ("emojis", "表情"), ("poses", "动作"),
        ("effects", "特效"), ("halls", "大厅模板"), ("hall_facilities", "大厅设施"),
        ("avatars", "角色"), ("avatar_skill_depots", "角色技能池"), ("avatar_skills", "角色技能"),
        ("avatar_costumes", "角色装扮"),
        ("weapons", "武器"), ("reliquaries", "圣遗物"), ("codex", "图鉴"),
        ("achievements", "成就"), ("viewpoints", "观景点"), ("dungeons", "秘境"),
        ("loading_tips", "过场提示"), ("gcg_cards", "七圣召唤卡牌"),
        ("gcg_chars", "七圣召唤角色牌"), ("gcg_skills", "七圣召唤技能"),
        ("gcg_keywords", "七圣召唤关键词"), ("gcg_elements", "七圣召唤元素"),
    ]
    parts = []
    for key, label in names:
        rows = data.get(key)
        if rows is not None:
            parts.append(f"{label} {len(rows)}")
    if parts:
        print(f"  实体数据源: {' | '.join(parts)}")


def _build_rows_iter(data: dict[str, Any], overrides: dict[str, tuple[int, int]] | None = None):
    """Build the chained rows iterator from loaded entity data."""
    return chain(
        _iter_material_mappings(data["materials"], data["codex_desc_map"], overrides),
        _pad_sub_category(_iter_furniture_mappings(data["furnitures"])),
        _iter_costume_mappings(data["costumes"]),
        _iter_suit_mappings(data["suits"]),
        _iter_emoji_mappings(data["emojis"]),
        _iter_pose_mappings(data["poses"]),
        _iter_effect_mappings(data["effects"]),
        _iter_hall_template_mappings(data["halls"]),
        _iter_hall_facility_mappings(data["hall_facilities"]),
        _iter_avatar_skill_mappings(
            data.get("avatars", []),
            data.get("avatar_skill_depots", []),
            data.get("avatar_skills", []),
        ),
        _iter_avatar_costume_mappings(data["avatar_costumes"]),
        _pad_sub_category(_iter_weapon_mappings(data["weapons"])),
        _pad_sub_category(_iter_reliquary_mappings(data["reliquaries"])),
        _pad_sub_category(_iter_codex_mappings(data["codex"], data["describe_title_map"])),
        _iter_achievement_mappings(data["achievements"]),
        _iter_viewpoint_mappings(data["viewpoints"]),
        _iter_dungeon_mappings(data["dungeons"]),
        _iter_loading_tip_mappings(data["loading_tips"]),
        _iter_gcg_card_skill_mappings(
            data["materials"],
            data["gcg_cards"],
            data["gcg_chars"],
            data["gcg_skills"],
            data["codex_desc_map"],
            data.get("_gcg_synthetic_hashes"),
        ),
    )


def build_entity_key_set(data: dict[str, Any], overrides: dict[str, tuple[int, int]] | None = None) -> set[tuple[int, int]]:
    """Return the entity identity set represented by loaded entity Excel data."""
    keys: set[tuple[int, int]] = set()
    for row in _build_rows_iter(data, overrides):
        if len(row) < 3:
            continue
        source_type_code = row[1]
        entity_id = row[2]
        if source_type_code is None or entity_id is None:
            continue
        keys.add((int(source_type_code), int(entity_id)))
    return keys


def _as_nonzero_int(value) -> int | None:
    if isinstance(value, int) and value != 0:
        return value
    return None


def _load_rows(path: str) -> list[dict[str, Any]]:
    data = load_json_file(path, default=[])
    if not isinstance(data, list):
        return []
    return [row for row in data if isinstance(row, dict)]


def _ensure_entity_source_schema(cursor):
    ensure_version_schema()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS text_source_entity (
            text_hash INTEGER NOT NULL,
            source_type_code INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            title_hash INTEGER NOT NULL,
            extra INTEGER NOT NULL DEFAULT 0,
            sub_category INTEGER NOT NULL DEFAULT 0,
            created_version_id INTEGER,
            PRIMARY KEY (text_hash, source_type_code, entity_id)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS text_source_entity_text_hash_index ON text_source_entity(text_hash)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS text_source_entity_title_hash_index ON text_source_entity(title_hash)"
    )
    # add sub_category column to existing tables (idempotent)
    try:
        cursor.execute("ALTER TABLE text_source_entity ADD COLUMN sub_category INTEGER NOT NULL DEFAULT 0")
    except Exception:
        pass
    try:
        cursor.execute("ALTER TABLE text_source_entity ADD COLUMN created_version_id INTEGER")
    except Exception:
        pass
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS text_source_entity_created_version_id_index "
        "ON text_source_entity(created_version_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS text_source_entity_entity_version_index "
        "ON text_source_entity(source_type_code, entity_id, created_version_id)"
    )


def _sync_entity_created_versions(cursor):
    cursor.execute(
        f"""
        UPDATE text_source_entity
        SET created_version_id = (
            SELECT chosen.created_version_id
            FROM text_source_entity chosen
            LEFT JOIN version_dim vd ON vd.id = chosen.created_version_id
            WHERE chosen.source_type_code = text_source_entity.source_type_code
              AND chosen.entity_id = text_source_entity.entity_id
              AND chosen.created_version_id IS NOT NULL
            ORDER BY COALESCE(vd.version_sort_key, 2147483647), chosen.created_version_id
            LIMIT 1
        )
        WHERE EXISTS (
            SELECT 1
            FROM text_source_entity chosen
            WHERE chosen.source_type_code = text_source_entity.source_type_code
              AND chosen.entity_id = text_source_entity.entity_id
              AND chosen.created_version_id IS NOT NULL
        )
        """
    )


def _current_entity_version_id() -> int | None:
    version = get_current_version()
    return get_or_create_version_id(version)


def _classify_material(row: dict[str, Any], overrides: dict[str, tuple[int, int]] | None = None) -> tuple[int, int]:
    """Return (source_type_code, sub_category) based on materialType lookup."""
    food_quality = str(row.get("foodQuality") or "").strip()
    if food_quality and food_quality != "FOOD_QUALITY_NONE":
        return SOURCE_TYPE_FOOD, SUB_NONE

    material_type = str(row.get("materialType") or "").strip()
    if overrides and material_type in overrides:
        val = overrides[material_type]
        if val != "skip" and isinstance(val, (list, tuple)) and len(val) == 2:
            return val[0], val[1]
    if material_type in MATERIAL_TYPE_TO_CATEGORY:
        return MATERIAL_TYPE_TO_CATEGORY[material_type]

    # fallback for unknown materialType
    return SOURCE_TYPE_ITEM, SUB_NONE


def _iter_material_mappings(rows: list[dict[str, Any]], codex_desc_map: dict[int, int] | None = None, overrides: dict[str, tuple[int, int]] | None = None):
    for row in rows:
        material_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        if material_id is None or title_hash is None:
            continue

        source_type_code, sub_category = _classify_material(row, overrides)
        mapping = (
            ("descTextMapHash", FIELD_DESC),
            ("effectDescTextMapHash", FIELD_EFFECT_DESC),
            ("specialDescTextMapHash", FIELD_SPECIAL_DESC),
            ("typeDescTextMapHash", FIELD_TYPE_DESC),
        )
        yielded_hashes: set[int] = set()
        for key, field_code in mapping:
            text_hash = _as_nonzero_int(row.get(key))
            if text_hash is None:
                continue
            yielded_hashes.add(text_hash)
            yield (text_hash, source_type_code, material_id, title_hash, _pack_extra(field_code), sub_category)

        # 图鉴描述（与背包描述不同时才额外产出）
        if codex_desc_map:
            codex_desc_hash = codex_desc_map.get(material_id)
            if codex_desc_hash and codex_desc_hash not in yielded_hashes:
                yield (codex_desc_hash, source_type_code, material_id, title_hash, _pack_extra(FIELD_CODEX_DESC), sub_category)


def _parse_nonzero_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value if value != 0 else None
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed != 0 else None
    return None


def _extract_gcg_card_id_from_material(row: dict[str, Any]) -> int | None:
    item_uses = row.get("itemUse")
    if not isinstance(item_uses, list):
        return None
    for item_use in item_uses:
        if not isinstance(item_use, dict):
            continue
        if item_use.get("useOp") != "ITEM_USE_GAIN_GCG_CARD":
            continue
        use_params = item_use.get("useParam")
        if not isinstance(use_params, list) or not use_params:
            continue
        card_id = _parse_nonzero_int(use_params[0])
        if card_id is not None:
            return card_id
    return None


_GCG_SYNTHETIC_TEXT_TABLE = "gcg_synthetic_textmap"
_GCG_TOKEN_RE = re.compile(r"\$\[([^\]]+)\]")
_GCG_PLURAL_RE = re.compile(r"\{PLURAL#([^|{}]+)\|([^|{}]*)\|([^{}]*)\}")
_GCG_DECLARED_VALUE_CACHE: dict[str, dict[str, dict[str, Any]]] = {}
_GCG_TEXT_KIND_SKILL = "skill"
_GCG_TEXT_KIND_CARD = "card"


@dataclass(frozen=True)
class _GcgTextSource:
    material_id: int
    title_hash: int
    card_id: int
    source_kind: str
    source_id: int
    raw_hash: int
    skill_json: str | None = None


def _build_gcg_row_map(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for row in rows:
        row_id = _as_nonzero_int(row.get("id"))
        if row_id is not None:
            result[row_id] = row
    return result


def _build_gcg_keyword_title_map(keyword_rows: list[dict[str, Any]]) -> dict[int, int]:
    result: dict[int, int] = {}
    for row in keyword_rows:
        keyword_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("titleTextMapHash"))
        if keyword_id is not None and title_hash is not None:
            result[keyword_id] = title_hash
    return result


def _build_gcg_element_keyword_map(element_rows: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in element_rows:
        element_type = str(row.get("type") or "").strip()
        keyword_id = _as_nonzero_int(row.get("keywordId"))
        if element_type and keyword_id is not None:
            result[element_type] = keyword_id
    return result


def _build_gcg_char_element_map(char_rows: list[dict[str, Any]]) -> dict[int, str]:
    result: dict[int, str] = {}
    for row in char_rows:
        char_id = _as_nonzero_int(row.get("id"))
        tag_list = row.get("tagList")
        if char_id is None or not isinstance(tag_list, list):
            continue
        for tag in tag_list:
            tag_text = str(tag or "")
            if tag_text.startswith("GCG_TAG_ELEMENT_"):
                result[char_id] = tag_text.replace("GCG_TAG_ELEMENT_", "GCG_ELEMENT_", 1)
                break
    return result


def _iter_gcg_skill_ids(row: dict[str, Any]):
    skill_list = row.get("skillList")
    if not isinstance(skill_list, list):
        return
    for raw_skill_id in skill_list:
        skill_id = _parse_nonzero_int(raw_skill_id)
        if skill_id is not None:
            yield skill_id


def _material_text_hashes(row: dict[str, Any], codex_desc_map: dict[int, int] | None = None) -> set[int]:
    result: set[int] = set()
    for key in ("descTextMapHash", "effectDescTextMapHash", "specialDescTextMapHash", "typeDescTextMapHash"):
        text_hash = _as_nonzero_int(row.get(key))
        if text_hash is not None:
            result.add(text_hash)
    if codex_desc_map:
        material_id = _as_nonzero_int(row.get("id"))
        if material_id is not None:
            codex_hash = codex_desc_map.get(material_id)
            if codex_hash:
                result.add(codex_hash)
    return result


def _iter_gcg_card_text_sources(
    material_rows: list[dict[str, Any]],
    gcg_card_rows: list[dict[str, Any]],
    gcg_char_rows: list[dict[str, Any]],
    gcg_skill_rows: list[dict[str, Any]],
    codex_desc_map: dict[int, int] | None = None,
):
    card_map = _build_gcg_row_map(gcg_card_rows)
    char_map = _build_gcg_row_map(gcg_char_rows)
    skill_map = _build_gcg_row_map(gcg_skill_rows)

    for row in material_rows:
        if row.get("materialType") != "MATERIAL_GCG_CARD":
            continue
        material_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        card_id = _extract_gcg_card_id_from_material(row)
        if material_id is None or title_hash is None or card_id is None:
            continue

        existing_hashes = _material_text_hashes(row, codex_desc_map)
        yielded_sources: set[tuple[str, int, int]] = set()
        char_row = char_map.get(card_id)
        if char_row:
            for skill_id in _iter_gcg_skill_ids(char_row):
                skill_row = skill_map.get(skill_id)
                if not skill_row:
                    continue
                desc_hash = _as_nonzero_int(skill_row.get("descTextMapHash"))
                if desc_hash is None or desc_hash in existing_hashes:
                    continue
                source_key = (_GCG_TEXT_KIND_SKILL, skill_id, desc_hash)
                if source_key in yielded_sources:
                    continue
                yielded_sources.add(source_key)
                skill_json = str(skill_row.get("skillJson") or "").strip() or None
                yield _GcgTextSource(
                    material_id=material_id,
                    title_hash=title_hash,
                    card_id=card_id,
                    source_kind=_GCG_TEXT_KIND_SKILL,
                    source_id=skill_id,
                    raw_hash=desc_hash,
                    skill_json=skill_json,
                )
            continue

        card_row = card_map.get(card_id)
        if not card_row:
            continue
        desc_hash = _as_nonzero_int(card_row.get("descTextMapHash"))
        if desc_hash is None or desc_hash in existing_hashes:
            continue
        source_key = (_GCG_TEXT_KIND_CARD, card_id, desc_hash)
        if source_key in yielded_sources:
            continue
        yielded_sources.add(source_key)
        yield _GcgTextSource(
            material_id=material_id,
            title_hash=title_hash,
            card_id=card_id,
            source_kind=_GCG_TEXT_KIND_CARD,
            source_id=card_id,
            raw_hash=desc_hash,
        )


def _stable_gcg_synthetic_hash(source: _GcgTextSource | str, source_id: int | None = None, raw_hash: int | None = None) -> int:
    if isinstance(source, _GcgTextSource):
        key = f"gcg:{source.source_kind}:{source.source_id}:{source.raw_hash}"
    else:
        key = f"gcg:{source}:{int(source_id or 0)}:{int(raw_hash or 0)}"
    digest = hashlib.blake2s(key.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "big") % 2_000_000_000
    return -(value + 1)


def _pack_gcg_text_extra(source_kind: str, source_id: int) -> int:
    kind_code = 1 if source_kind == _GCG_TEXT_KIND_SKILL else 2
    return ((int(source_id) & 0xFFFFFFFF) << 16) | ((kind_code & 0xFF) << 8) | (FIELD_SKILL_DESC & 0xFF)


def _normalize_declared_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _collect_gcg_declared_values(obj: Any, result: dict[str, dict[str, Any]]):
    if isinstance(obj, dict):
        name = obj.get("MEGMIMEDODJ")
        if isinstance(name, str) and name:
            result[_normalize_declared_key(name)] = obj
        for value in obj.values():
            _collect_gcg_declared_values(value, result)
    elif isinstance(obj, list):
        for value in obj:
            _collect_gcg_declared_values(value, result)


def _load_gcg_declared_values(skill_json: str | None) -> dict[str, dict[str, Any]]:
    if not skill_json:
        return {}
    key = str(skill_json).strip()
    if not key:
        return {}
    if key in _GCG_DECLARED_VALUE_CACHE:
        return _GCG_DECLARED_VALUE_CACHE[key]
    path = os.path.join(DATA_PATH, "BinOutput", "GCG", "Gcg_DeclaredValueSet", f"{key}.json")
    if not os.path.isfile(path):
        _GCG_DECLARED_VALUE_CACHE[key] = {}
        return {}
    data = load_json_file(path, default={})
    result: dict[str, dict[str, Any]] = {}
    _collect_gcg_declared_values(data, result)
    _GCG_DECLARED_VALUE_CACHE[key] = result
    return result


def _format_gcg_declared_number(value: Any) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)
    return None


class _GcgTextResolver:
    def __init__(self, cursor, data: dict[str, Any]):
        self.cursor = cursor
        self.card_map = _build_gcg_row_map(data.get("gcg_cards", []))
        self.char_map = _build_gcg_row_map(data.get("gcg_chars", []))
        self.skill_map = _build_gcg_row_map(data.get("gcg_skills", []))
        self.keyword_title_map = _build_gcg_keyword_title_map(data.get("gcg_keywords", []))
        self.element_keyword_map = _build_gcg_element_keyword_map(data.get("gcg_elements", []))
        self.char_element_map = _build_gcg_char_element_map(data.get("gcg_chars", []))
        self._text_cache: dict[tuple[int, int], str | None] = {}

    def _text(self, text_hash: int | None, lang: int) -> str | None:
        if text_hash is None:
            return None
        key = (int(text_hash), int(lang))
        if key not in self._text_cache:
            row = self.cursor.execute(
                "SELECT content FROM textMap WHERE hash=? AND lang=? LIMIT 1",
                key,
            ).fetchone()
            self._text_cache[key] = row[0] if row and row[0] is not None else None
        return self._text_cache[key]

    def _name_from_row(self, row: dict[str, Any] | None, lang: int) -> str | None:
        if not row:
            return None
        return self._text(_as_nonzero_int(row.get("nameTextMapHash")), lang)

    def _keyword_text(self, keyword_id: int, lang: int) -> str | None:
        return self._text(self.keyword_title_map.get(int(keyword_id)), lang)

    def _element_text(self, element_type: str, lang: int) -> str | None:
        keyword_id = self.element_keyword_map.get(str(element_type or "").strip())
        if keyword_id is None:
            return None
        return self._keyword_text(keyword_id, lang)

    def _declared_value(
        self,
        declared: dict[str, dict[str, Any]],
        key: str,
        lang: int,
        card_id: int | None,
    ) -> str | None:
        normalized = _normalize_declared_key(key)
        entry = declared.get(normalized)
        if entry is None and normalized.startswith("damage"):
            entry = declared.get("damage")
        if entry is None and normalized.startswith("damage"):
            entry = declared.get("indirectdamage")
        if entry is None and normalized.startswith("damage"):
            entry = declared.get("effectnum")
        if entry is None and normalized.startswith("element"):
            entry = declared.get("element")
        if not entry:
            if normalized.startswith("element") and card_id is not None:
                element_type = self.char_element_map.get(int(card_id))
                if element_type:
                    return self._element_text(element_type, lang)
            if normalized.startswith("damage"):
                return "1"
            return None
        number = _format_gcg_declared_number(entry.get("AOJNMJNAEEO"))
        if number is not None:
            return number
        element = entry.get("CAHOPGJMELB")
        if element:
            return self._element_text(str(element), lang)
        return None

    def _resolve_token(
        self,
        token: str,
        lang: int,
        declared: dict[str, dict[str, Any]],
        card_id: int | None,
    ) -> str | None:
        spec = token.split("|", 1)[0].strip()
        if spec.startswith("D__KEY__"):
            return self._declared_value(declared, spec.removeprefix("D__KEY__"), lang, card_id)

        token_type = spec[:1]
        raw_id = spec[1:]
        match = re.search(r"-?\d+", raw_id)
        if not token_type or not match:
            return None
        ref_id = int(match.group(0))
        if token_type == "K":
            return self._keyword_text(ref_id, lang)
        if token_type == "C":
            return self._name_from_row(self.card_map.get(ref_id) or self.char_map.get(ref_id), lang)
        if token_type == "S":
            return self._name_from_row(self.skill_map.get(ref_id), lang)
        if token_type == "A":
            return self._name_from_row(self.char_map.get(ref_id), lang)
        return None

    @staticmethod
    def _resolve_plural(content: str) -> str:
        def repl(match: re.Match) -> str:
            count_text = match.group(1).strip()
            try:
                count = float(count_text)
            except ValueError:
                return match.group(0)
            return match.group(2) if abs(count) == 1 else match.group(3)

        return _GCG_PLURAL_RE.sub(repl, content)

    def resolve(self, content: str, lang: int, skill_json: str | None = None, card_id: int | None = None) -> str:
        declared = _load_gcg_declared_values(skill_json)

        def repl(match: re.Match) -> str:
            resolved = self._resolve_token(match.group(1), lang, declared, card_id)
            return resolved if resolved is not None else match.group(0)

        resolved = _GCG_TOKEN_RE.sub(repl, content)
        return self._resolve_plural(resolved)


def _ensure_gcg_synthetic_text_schema(cursor):
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_GCG_SYNTHETIC_TEXT_TABLE} (
            hash INTEGER PRIMARY KEY,
            source_kind TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            raw_hash INTEGER NOT NULL
        )
        """
    )


def _sqlite_table_exists(cursor, table_name: str) -> bool:
    row = cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _clear_gcg_synthetic_text(cursor):
    _ensure_gcg_synthetic_text_schema(cursor)
    if _sqlite_table_exists(cursor, "textMap"):
        cursor.execute(
            f"""
            DELETE FROM textMap
            WHERE hash IN (SELECT hash FROM {_GCG_SYNTHETIC_TEXT_TABLE})
            """
        )
    cursor.execute(f"DELETE FROM {_GCG_SYNTHETIC_TEXT_TABLE}")


def _clear_gcg_skill_entity_sources(cursor):
    cursor.execute(
        """
        DELETE FROM text_source_entity
        WHERE source_type_code=? AND sub_category=? AND (extra & 255)=?
        """,
        (SOURCE_TYPE_GCG, SUB_CARD, FIELD_SKILL_DESC),
    )


def _refresh_gcg_synthetic_textmap(cursor, data: dict[str, Any], version_id: int | None):
    _clear_gcg_synthetic_text(cursor)
    data["_gcg_synthetic_hashes"] = {}
    if not _sqlite_table_exists(cursor, "textMap"):
        return
    _GCG_DECLARED_VALUE_CACHE.clear()
    resolver = _GcgTextResolver(cursor, data)
    sources = list(
        _iter_gcg_card_text_sources(
            data.get("materials", []),
            data.get("gcg_cards", []),
            data.get("gcg_chars", []),
            data.get("gcg_skills", []),
            data.get("codex_desc_map"),
        )
    )
    insert_text_sql = (
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) "
        "VALUES (?,?,?,?,?) "
        "ON CONFLICT(lang, hash) DO UPDATE SET "
        "content=excluded.content, "
        "created_version_id=excluded.created_version_id, "
        "updated_version_id=excluded.updated_version_id "
        "WHERE NOT (textMap.content IS excluded.content) "
        "OR NOT (textMap.created_version_id IS excluded.created_version_id) "
        "OR NOT (textMap.updated_version_id IS excluded.updated_version_id)"
    )
    insert_meta_sql = (
        f"INSERT OR REPLACE INTO {_GCG_SYNTHETIC_TEXT_TABLE}(hash, source_kind, source_id, raw_hash) "
        "VALUES (?,?,?,?)"
    )
    seen_synthetic: set[int] = set()
    synthetic_hashes: dict[tuple[str, int, int], int] = {}
    for source in sources:
        synthetic_hash = _stable_gcg_synthetic_hash(source)
        if synthetic_hash in seen_synthetic:
            continue
        seen_synthetic.add(synthetic_hash)
        raw_rows = cursor.execute(
            """
            SELECT lang, content, created_version_id, updated_version_id
            FROM textMap
            WHERE hash=? AND content IS NOT NULL AND content!=''
            """,
            (source.raw_hash,),
        ).fetchall()
        if not raw_rows:
            continue
        resolved_rows = [
            (
                int(lang),
                resolver.resolve(str(content), int(lang), source.skill_json, source.card_id),
                created_version_id,
                updated_version_id,
            )
            for lang, content, created_version_id, updated_version_id in raw_rows
        ]
        if not any(resolved != str(content) for (_lang, content, _created, _updated), (_r_lang, resolved, _r_created, _r_updated) in zip(raw_rows, resolved_rows)):
            continue
        synthetic_hashes[(source.source_kind, source.source_id, source.raw_hash)] = synthetic_hash
        cursor.execute(insert_meta_sql, (synthetic_hash, source.source_kind, source.source_id, source.raw_hash))
        for lang, resolved, created_version_id, updated_version_id in resolved_rows:
            row_created = created_version_id if created_version_id is not None else version_id
            row_updated = updated_version_id if updated_version_id is not None else version_id
            cursor.execute(insert_text_sql, (synthetic_hash, resolved, int(lang), row_created, row_updated))
    data["_gcg_synthetic_hashes"] = synthetic_hashes


def _iter_gcg_card_skill_mappings(
    material_rows: list[dict[str, Any]],
    gcg_card_rows: list[dict[str, Any]],
    gcg_char_rows: list[dict[str, Any]],
    gcg_skill_rows: list[dict[str, Any]],
    codex_desc_map: dict[int, int] | None = None,
    synthetic_hashes: dict[tuple[str, int, int], int] | None = None,
):
    for source in _iter_gcg_card_text_sources(material_rows, gcg_card_rows, gcg_char_rows, gcg_skill_rows, codex_desc_map):
        source_key = (source.source_kind, source.source_id, source.raw_hash)
        text_hash = (
            synthetic_hashes.get(source_key, source.raw_hash)
            if synthetic_hashes is not None
            else _stable_gcg_synthetic_hash(source)
        )
        yield (
            text_hash,
            SOURCE_TYPE_GCG,
            source.material_id,
            source.title_hash,
            _pack_gcg_text_extra(source.source_kind, source.source_id),
            SUB_CARD,
        )


def _iter_furniture_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        furniture_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if furniture_id is None or title_hash is None or text_hash is None:
            continue
        yield (text_hash, SOURCE_TYPE_FURNISHING, furniture_id, title_hash, _pack_extra(FIELD_DESC))


def _gender_code(body_types: Any) -> int:
    if isinstance(body_types, str):
        body_types = [body_types]
    if not isinstance(body_types, (list, tuple, set)):
        return 0
    body_type_set = {str(body_type).strip() for body_type in body_types if body_type}
    has_boy = "BODY_BOY" in body_type_set
    has_girl = "BODY_GIRL" in body_type_set
    if has_boy and has_girl:
        return 3
    if has_boy:
        return 1
    if has_girl:
        return 2
    return 0


def _get_body_types(row: dict[str, Any]) -> Any:
    for key in ("BKBPADANEOC", "IAHOEKGIPPJ", "AFAENJLHMOD"):
        if key in row:
            return row.get(key)
    return None


def _extract_first_int(row: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = _as_nonzero_int(row.get(key))
        if value is not None:
            return value
    return None


def _extract_first_str(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str):
            return value
    return ""


def _pack_extra(field_code: int, gender_code: int = 0) -> int:
    if field_code < 0:
        field_code = 0
    if gender_code < 0:
        gender_code = 0
    return ((gender_code & 0xFF) << 8) | (field_code & 0xFF)


def _iter_costume_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        costume_id = _as_nonzero_int(row.get("costumeId"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descriptionTextMapHash"))
        if costume_id is None or title_hash is None or text_hash is None:
            continue
        gender = _gender_code(_get_body_types(row))
        yield (
            text_hash,
            SOURCE_TYPE_COSTUME,
            costume_id,
            title_hash,
            _pack_extra(FIELD_DESC, gender),
            SUB_QIANXING_PARADOX,
        )


def _iter_suit_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        suit_id = _as_nonzero_int(row.get("suitId"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descriptionTextMapHash"))
        if suit_id is None or title_hash is None or text_hash is None:
            continue
        gender = _gender_code(_get_body_types(row))
        yield (
            text_hash,
            SOURCE_TYPE_SUIT,
            suit_id,
            title_hash,
            _pack_extra(FIELD_DESC, gender),
            SUB_QIANXING_SUIT,
        )


def _iter_emoji_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        emoji_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descriptionTextMapHash"))
        if emoji_id is None or title_hash is None or text_hash is None:
            continue
        yield (
            text_hash,
            SOURCE_TYPE_QIANXING_EMOJI,
            emoji_id,
            title_hash,
            _pack_extra(FIELD_DESC),
            SUB_QIANXING_EMOJI,
        )


def _iter_pose_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        pose_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descriptionTextMapHash"))
        if pose_id is None or title_hash is None or text_hash is None:
            continue
        yield (
            text_hash,
            SOURCE_TYPE_QIANXING_POSE,
            pose_id,
            title_hash,
            _pack_extra(FIELD_DESC, _gender_code(row.get("bodyType"))),
            SUB_QIANXING_POSE,
        )


def _iter_effect_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        effect_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descriptionTextMapHash"))
        if effect_id is None or title_hash is None or text_hash is None:
            continue
        yield (
            text_hash,
            SOURCE_TYPE_QIANXING_EFFECT,
            effect_id,
            title_hash,
            _pack_extra(FIELD_DESC),
            SUB_QIANXING_EFFECT,
        )


def _get_hall_style_id(row: dict[str, Any]) -> int | None:
    return _extract_first_int(row, "COGKFPLDLLL", "DCOBMNILGJL", "OCHDBIAAHIO", "CKIGKAIIFFI")


def _get_hall_name_text_hash(row: dict[str, Any]) -> int | None:
    return _extract_first_int(row, "LDCAAIEKMOE", "KMMKMJLOFGC", "CAMAHAEKAIH", "AOGCNHLHJMJ")


def _get_hall_desc_text_hash(row: dict[str, Any]) -> int | None:
    return _extract_first_int(row, "BPKNEMEJEPF", "DKBHBHOOGAP", "PEODHMPDKNF", "PPOAOFDNLDJ")


def _is_public_hall(row: dict[str, Any]) -> bool:
    hall_type = _extract_first_str(row, "PEMNJBEBBOG", "BMIILBDKBIO", "KMDBAGPDKNG", "BNKLMBACEDF")
    return hall_type == "BEYOND_HALL_PUBLIC"


def _get_hall_facility_style_id(row: dict[str, Any]) -> int | None:
    return _extract_first_int(
        row,
        "OJGEAGGJALA", "DGBOKBNOJKE", "LGGBFCPPBBJ",
        "OEFKFFGKKKP", "DOPFMOKHIIC", "JPGMJHBCOGK",
        "FEIJJDIAHFJ", "KJJPGPAKCIF", "BBLFLDMDBNJ",
    )


def _iter_hall_template_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        if _is_public_hall(row):
            continue
        hall_style_id = _get_hall_style_id(row)
        title_hash = _get_hall_name_text_hash(row)
        text_hash = _get_hall_desc_text_hash(row)
        if hall_style_id is None or title_hash is None or text_hash is None:
            continue
        yield (
            text_hash,
            SOURCE_TYPE_QIANXING_HALL,
            hall_style_id,
            title_hash,
            _pack_extra(FIELD_DESC),
            SUB_QIANXING_HALL,
        )


def _iter_hall_facility_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        facility_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descTextMapHash"))
        style_id = _get_hall_facility_style_id(row)
        if facility_id is None or title_hash is None or text_hash is None or style_id is None:
            continue
        yield (
            text_hash,
            SOURCE_TYPE_QIANXING_HALL,
            facility_id,
            title_hash,
            _pack_extra(FIELD_DESC),
            SUB_QIANXING_HALL,
        )


def _iter_avatar_depot_ids(rows: list[dict[str, Any]]) -> set[int]:
    depot_ids: set[int] = set()
    for row in rows:
        if str(row.get("useType") or "").strip() == "AVATAR_TEST":
            continue
        depot_id = _as_nonzero_int(row.get("skillDepotId"))
        if depot_id is not None:
            depot_ids.add(depot_id)
        cand_depot_ids = row.get("candSkillDepotIds")
        if isinstance(cand_depot_ids, list):
            for cand_depot_id in cand_depot_ids:
                parsed = _parse_nonzero_int(cand_depot_id)
                if parsed is not None:
                    depot_ids.add(parsed)
    return depot_ids


def _iter_avatar_skill_ids(
    avatar_skill_depots: list[dict[str, Any]],
    avatar_depot_ids: set[int],
) -> set[int]:
    skill_ids: set[int] = set()
    for row in avatar_skill_depots:
        depot_id = _as_nonzero_int(row.get("id"))
        if depot_id is None or depot_id not in avatar_depot_ids:
            continue
        for key in ("skills", "subSkills"):
            values = row.get(key)
            if not isinstance(values, list):
                continue
            for value in values:
                parsed = _parse_nonzero_int(value)
                if parsed is not None:
                    skill_ids.add(parsed)
        for key in ("energySkill", "attackModeSkill"):
            parsed = _parse_nonzero_int(row.get(key))
            if parsed is not None:
                skill_ids.add(parsed)
    return skill_ids


def _iter_avatar_skill_mappings(
    avatars: list[dict[str, Any]],
    avatar_skill_depots: list[dict[str, Any]],
    avatar_skills: list[dict[str, Any]],
):
    avatar_depot_ids = _iter_avatar_depot_ids(avatars)
    skill_ids = _iter_avatar_skill_ids(avatar_skill_depots, avatar_depot_ids)
    for row in avatar_skills:
        skill_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        if skill_id is None or title_hash is None or skill_id not in skill_ids:
            continue

        yielded_hashes: set[int] = set()
        mapping = (
            (title_hash, FIELD_TITLE),
            (_as_nonzero_int(row.get("descTextMapHash")), FIELD_DESC),
            (_as_nonzero_int(row.get("extraDescTextMapHash")), FIELD_SPECIAL_DESC),
        )
        for text_hash, field_code in mapping:
            if text_hash is None or text_hash in yielded_hashes:
                continue
            yielded_hashes.add(text_hash)
            yield (
                text_hash,
                SOURCE_TYPE_AVATAR_INTRO,
                skill_id,
                title_hash,
                _pack_extra(field_code),
                SUB_AVATAR_SKILL,
            )


def _iter_avatar_costume_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        costume_id = _as_nonzero_int(row.get("skinId"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if costume_id is None or title_hash is None or text_hash is None:
            continue
        yield (text_hash, SOURCE_TYPE_DRESSING, costume_id, title_hash, _pack_extra(FIELD_DESC), SUB_COSTUME_DRESS)


def _iter_weapon_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        weapon_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if weapon_id is None or title_hash is None or text_hash is None:
            continue
        yield (text_hash, SOURCE_TYPE_WEAPON, weapon_id, title_hash, _pack_extra(FIELD_DESC))


def _iter_reliquary_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        reliquary_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        text_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if reliquary_id is None or title_hash is None or text_hash is None:
            continue
        yield (text_hash, SOURCE_TYPE_RELIQUARY, reliquary_id, title_hash, _pack_extra(FIELD_DESC))


def _build_describe_title_map(*rows_groups: list[dict[str, Any]]) -> dict[int, int]:
    mapping: dict[int, int] = {}
    for rows in rows_groups:
        for row in rows:
            describe_id = _as_nonzero_int(row.get("id"))
            title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
            if describe_id is None or title_hash is None:
                continue
            mapping[describe_id] = title_hash
    return mapping


def _classify_codex_source_type(row: dict[str, Any]) -> int:
    codex_type = str(row.get("type") or "").strip().upper()
    if codex_type == "CODEX_MONSTER":
        return SOURCE_TYPE_MONSTER
    return SOURCE_TYPE_CREATURE


def _iter_codex_mappings(rows: list[dict[str, Any]], describe_title_map: dict[int, int]):
    for row in rows:
        codex_id = _as_nonzero_int(row.get("id"))
        describe_id = _as_nonzero_int(row.get("describeId"))
        text_hash = _as_nonzero_int(row.get("descTextMapHash"))
        title_hash = describe_title_map.get(describe_id) if describe_id is not None else None
        title_hash = title_hash or _as_nonzero_int(row.get("nameTextMapHash"))
        if codex_id is None or title_hash is None or text_hash is None:
            continue
        yield (
            text_hash,
            _classify_codex_source_type(row),
            codex_id,
            title_hash,
            _pack_extra(FIELD_DESC),
        )


def _pad_sub_category(rows_iter):
    """Wrap 5-tuple iterators to produce 6-tuples with sub_category=0."""
    for row in rows_iter:
        yield (*row, SUB_NONE)


def _iter_achievement_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        ach_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("titleTextMapHash"))
        desc_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if ach_id is None or title_hash is None:
            continue
        if title_hash:
            yield (title_hash, SOURCE_TYPE_ACHIEVEMENT, ach_id, title_hash, _pack_extra(FIELD_TITLE), SUB_NONE)
        if desc_hash:
            yield (desc_hash, SOURCE_TYPE_ACHIEVEMENT, ach_id, title_hash, _pack_extra(FIELD_DESC), SUB_NONE)


def _iter_viewpoint_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        vp_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        desc_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if vp_id is None or title_hash is None:
            continue
        if desc_hash:
            yield (desc_hash, SOURCE_TYPE_VIEWPOINT, vp_id, title_hash, _pack_extra(FIELD_DESC), SUB_NONE)


def _iter_dungeon_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        dg_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("nameTextMapHash"))
        desc_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if dg_id is None or title_hash is None:
            continue
        if desc_hash:
            yield (desc_hash, SOURCE_TYPE_DUNGEON, dg_id, title_hash, _pack_extra(FIELD_DESC), SUB_NONE)


def _build_codex_desc_map(codex_rows: list[dict[str, Any]]) -> dict[int, int]:
    """Build materialId → codex descTextMapHash mapping."""
    result: dict[int, int] = {}
    for row in codex_rows:
        material_id = _as_nonzero_int(row.get("materialId"))
        desc_hash = _as_nonzero_int(row.get("descTextMapHash"))
        if material_id is not None and desc_hash is not None:
            result[material_id] = desc_hash
    return result


def _build_book_material_ids(book_codex_rows: list[dict[str, Any]]) -> set[int]:
    """Build a set of materialIds that are books (in BooksCodexExcelConfigData)."""
    ids: set[int] = set()
    for row in book_codex_rows:
        material_id = _as_nonzero_int(row.get("materialId"))
        if material_id is not None:
            ids.add(material_id)
    return ids


def _iter_loading_tip_mappings(rows: list[dict[str, Any]]):
    for row in rows:
        tip_id = _as_nonzero_int(row.get("id"))
        title_hash = _as_nonzero_int(row.get("tipsTitleTextMapHash"))
        desc_hash = _as_nonzero_int(row.get("tipsDescTextMapHash"))
        if tip_id is None or title_hash is None:
            continue
        if title_hash:
            yield (title_hash, SOURCE_TYPE_LOADING_TIP, tip_id, title_hash, _pack_extra(FIELD_TITLE), SUB_NONE)
        if desc_hash:
            yield (desc_hash, SOURCE_TYPE_LOADING_TIP, tip_id, title_hash, _pack_extra(FIELD_DESC), SUB_NONE)


def importEntitySources(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE, interactive: bool = True):
    cursor = conn.cursor()
    try:
        _ensure_entity_source_schema(cursor)
        cursor.execute("DELETE FROM text_source_entity")
        version_id = _current_entity_version_id()

        excel_root = os.path.join(DATA_PATH, "ExcelBinOutput")
        data = _load_all_entity_data(excel_root)
        _print_entity_source_summary(data)

        overrides = check_and_classify_interactive(data["materials"], interactive=interactive)
        _refresh_gcg_synthetic_textmap(cursor, data, version_id)

        sql = (
            "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(text_hash, source_type_code, entity_id) DO UPDATE SET "
            "title_hash=excluded.title_hash, extra=excluded.extra, sub_category=excluded.sub_category, "
            "created_version_id="
            + _build_version_preference_case_sql(
                existing_expr="text_source_entity.created_version_id",
                candidate_expr="excluded.created_version_id",
                is_created=True,
            )
        )

        # Full imports rebuild the catalog from the current snapshot, so the
        # entity first-appearance version must be supplied by history replay.
        rows_iter = ((*row, None) for row in _build_rows_iter(data, overrides))
        executemany_batched(cursor, sql, rows_iter, batch_size=batch_size)
        _sync_entity_created_versions(cursor)
    finally:
        cursor.close()
    if commit:
        conn.commit()


def insertEntitySourcesDelta(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE, interactive: bool = True):
    cursor = conn.cursor()
    try:
        _ensure_entity_source_schema(cursor)
        version_id = _current_entity_version_id()

        excel_root = os.path.join(DATA_PATH, "ExcelBinOutput")
        data = _load_all_entity_data(excel_root)
        _print_entity_source_summary(data)

        overrides = check_and_classify_interactive(data["materials"], interactive=interactive)
        _refresh_gcg_synthetic_textmap(cursor, data, version_id)
        _clear_gcg_skill_entity_sources(cursor)

        sql = (
            "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id) "
            "VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(text_hash, source_type_code, entity_id) DO NOTHING"
        )

        rows_iter = ((*row, version_id) for row in _build_rows_iter(data, overrides))
        executemany_batched(cursor, sql, rows_iter, batch_size=batch_size)
        _sync_entity_created_versions(cursor)
    finally:
        cursor.close()
    if commit:
        conn.commit()

import json
import os
import sys
from itertools import chain
from typing import Any

from DBConfig import DATA_PATH, conn
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched, load_json_file

import entity_constants
from entity_constants import (
    # 大分类
    SOURCE_TYPE_ITEM, SOURCE_TYPE_FOOD, SOURCE_TYPE_FURNISHING,
    SOURCE_TYPE_COSTUME, SOURCE_TYPE_WEAPON, SOURCE_TYPE_RELIQUARY,
    SOURCE_TYPE_MONSTER, SOURCE_TYPE_CREATURE,
    SOURCE_TYPE_DRESSING,
    SOURCE_TYPE_ACHIEVEMENT, SOURCE_TYPE_VIEWPOINT, SOURCE_TYPE_DUNGEON, SOURCE_TYPE_LOADING_TIP,
    # 字段
    FIELD_DESC, FIELD_EFFECT_DESC, FIELD_SPECIAL_DESC, FIELD_TYPE_DESC, FIELD_TITLE, FIELD_CODEX_DESC,
    # 二级分类
    SUB_NONE,
    SUB_COSTUME_DRESS, SUB_QIANXING_PARADOX, SUB_QIANXING_SUIT,
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
]


def _load_all_entity_data(excel_root: str) -> dict[str, Any]:
    """Load all entity-related Excel data and derived maps."""
    materials = _load_rows(os.path.join(excel_root, "MaterialExcelConfigData.json"))
    furnitures = _load_rows(os.path.join(excel_root, "HomeWorldFurnitureExcelConfigData.json"))
    costumes = _load_rows(os.path.join(excel_root, "BeyondCostumeExcelConfigData.json"))
    suits = _load_rows(os.path.join(excel_root, "BeyondCostumeSuitExcelConfigData.json"))
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

    describe_title_map = _build_describe_title_map(animal_describes, monster_describes)
    codex_desc_map = _build_codex_desc_map(material_codex)

    return {
        "materials": materials,
        "furnitures": furnitures,
        "costumes": costumes,
        "suits": suits,
        "avatar_costumes": avatar_costumes,
        "weapons": weapons,
        "reliquaries": reliquaries,
        "codex": codex,
        "achievements": achievements,
        "viewpoints": viewpoints,
        "dungeons": dungeons,
        "loading_tips": loading_tips,
        "describe_title_map": describe_title_map,
        "codex_desc_map": codex_desc_map,
    }


def _print_entity_source_summary(data: dict[str, Any]):
    """Print a summary table of entity source record counts."""
    names = [
        ("materials", "材料"), ("furnitures", "家具"), ("costumes", "装扮"),
        ("suits", "套装"), ("avatar_costumes", "角色装扮"),
        ("weapons", "武器"), ("reliquaries", "圣遗物"), ("codex", "图鉴"),
        ("achievements", "成就"), ("viewpoints", "观景点"), ("dungeons", "秘境"),
        ("loading_tips", "过场提示"),
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
        _iter_avatar_costume_mappings(data["avatar_costumes"]),
        _pad_sub_category(_iter_weapon_mappings(data["weapons"])),
        _pad_sub_category(_iter_reliquary_mappings(data["reliquaries"])),
        _pad_sub_category(_iter_codex_mappings(data["codex"], data["describe_title_map"])),
        _iter_achievement_mappings(data["achievements"]),
        _iter_viewpoint_mappings(data["viewpoints"]),
        _iter_dungeon_mappings(data["dungeons"]),
        _iter_loading_tip_mappings(data["loading_tips"]),
    )


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
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS text_source_entity (
            text_hash INTEGER NOT NULL,
            source_type_code INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            title_hash INTEGER NOT NULL,
            extra INTEGER NOT NULL DEFAULT 0,
            sub_category INTEGER NOT NULL DEFAULT 0,
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
    for key in ("BKBPADANEOC", "IAHOEKGIPPJ"):
        if key in row:
            return row.get(key)
    return None


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
            SOURCE_TYPE_COSTUME,
            suit_id,
            title_hash,
            _pack_extra(FIELD_DESC, gender),
            SUB_QIANXING_SUIT,
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

        excel_root = os.path.join(DATA_PATH, "ExcelBinOutput")
        data = _load_all_entity_data(excel_root)
        _print_entity_source_summary(data)

        overrides = check_and_classify_interactive(data["materials"], interactive=interactive)

        sql = (
            "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(text_hash, source_type_code, entity_id) DO UPDATE SET "
            "title_hash=excluded.title_hash, extra=excluded.extra, sub_category=excluded.sub_category"
        )

        rows_iter = _build_rows_iter(data, overrides)
        executemany_batched(cursor, sql, rows_iter, batch_size=batch_size)
    finally:
        cursor.close()
    if commit:
        conn.commit()


def insertEntitySourcesDelta(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE, interactive: bool = True):
    cursor = conn.cursor()
    try:
        _ensure_entity_source_schema(cursor)

        excel_root = os.path.join(DATA_PATH, "ExcelBinOutput")
        data = _load_all_entity_data(excel_root)
        _print_entity_source_summary(data)

        overrides = check_and_classify_interactive(data["materials"], interactive=interactive)

        sql = (
            "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category) "
            "VALUES (?,?,?,?,?,?) "
            "ON CONFLICT(text_hash, source_type_code, entity_id) DO NOTHING"
        )

        rows_iter = _build_rows_iter(data, overrides)
        executemany_batched(cursor, sql, rows_iter, batch_size=batch_size)
    finally:
        cursor.close()
    if commit:
        conn.commit()

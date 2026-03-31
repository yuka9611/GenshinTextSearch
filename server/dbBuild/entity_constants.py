"""实体来源常量：大分类、字段编码、二级分类、materialType 映射及标签。"""

import os
import re


# ── 大分类编码 ────────────────────────────────────────────────────
SOURCE_TYPE_ITEM = 1
SOURCE_TYPE_FOOD = 2
SOURCE_TYPE_FURNISHING = 3
SOURCE_TYPE_GADGET = 4          # legacy, kept for non-material gadget compat
SOURCE_TYPE_COSTUME = 5
SOURCE_TYPE_SUIT = 6
# SOURCE_TYPE_FLYCLOAK = 7  (removed, wind gliders now imported via MATERIAL_FLYCLOAK → DRESSING)
# SOURCE_TYPE_OUTFIT = 8  (removed, avatar costumes now imported under DRESSING)
SOURCE_TYPE_WEAPON = 9
SOURCE_TYPE_RELIQUARY = 10
SOURCE_TYPE_MONSTER = 11
SOURCE_TYPE_CREATURE = 12
SOURCE_TYPE_MATERIAL = 13
SOURCE_TYPE_BLUEPRINT = 14      # 图纸
SOURCE_TYPE_GCG = 15            # 七圣召唤
SOURCE_TYPE_NAMECARD = 16       # 名片
SOURCE_TYPE_PERFORMANCE = 17    # 表演诀窍
SOURCE_TYPE_AVATAR_INTRO = 18   # 角色
SOURCE_TYPE_DRESSING = 19       # 装扮
SOURCE_TYPE_MUSIC_THEME = 20    # 演奏主题
SOURCE_TYPE_OTHER_MAT = 21      # 其他
SOURCE_TYPE_AVATAR_MAT = 22     # 角色突破素材
SOURCE_TYPE_ACHIEVEMENT = 23    # 成就
SOURCE_TYPE_VIEWPOINT = 24      # 观景点
SOURCE_TYPE_DUNGEON = 25        # 秘境
SOURCE_TYPE_LOADING_TIP = 26    # 过场提示

# ── 字段编码 ──────────────────────────────────────────────────────
FIELD_DESC = 1
FIELD_EFFECT_DESC = 2
FIELD_SPECIAL_DESC = 3
FIELD_TYPE_DESC = 4
FIELD_TITLE = 5
FIELD_CODEX_DESC = 6

# ── 二级分类编码 ──────────────────────────────────────────────────
SUB_NONE = 0
SUB_QUEST_ITEM = 1          # 任务道具
SUB_FURN_BLUEPRINT = 2      # 摆设图纸
SUB_CARD_FACE = 3           # 牌面
SUB_CARD = 4                # 卡牌
SUB_CONSUMABLE = 5          # 消耗品
SUB_AVATAR_INTRO = 6        # 角色介绍
SUB_CONSTELLATION = 7       # 命之座激活素材
SUB_SUITE_BLUEPRINT = 8     # 摆设套装图纸
SUB_CHEST = 9               # 宝箱
SUB_EVENT_ITEM = 10         # 活动道具
SUB_WIDGET = 11             # 小道具
SUB_SEED = 12               # 种子
SUB_CARD_BACK = 13          # 牌背
SUB_EVENT_FOOD = 14         # 活动食物
SUB_WOOD = 15               # 木材
SUB_EXP_MATERIAL = 16       # 经验素材
SUB_COSTUME_DRESS = 17      # 衣装
SUB_TRACE = 18              # 游迹
SUB_FLYCLOAK_SUB = 19       # 风之翼
SUB_FIREWORKS = 20          # 烟花
SUB_FISH_ROD = 21           # 钓竿
SUB_WEAPON_SKIN = 22        # 武器外观
SUB_PROFILE_FRAME = 23      # 头像框
SUB_PROFILE_PICTURE = 24    # 头像
SUB_GCG_BOX = 25            # 牌盒
SUB_FISH_BAIT = 26          # 鱼饵
SUB_BGM = 27                # 旋曜玉帛
SUB_BREAKTHROUGH = 28       # 角色突破素材
SUB_QIANXING_PARADOX = 29   # 奇偶装扮
SUB_QIANXING_SUIT = 30      # 装扮套装

# ── materialType → (source_type_code, sub_category_code) ─────────
MATERIAL_TYPE_TO_CATEGORY: dict[str, tuple[int, int]] = {
    "MATERIAL_QUEST":                       (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_FURNITURE_FORMULA":           (SOURCE_TYPE_BLUEPRINT, SUB_FURN_BLUEPRINT),
    "MATERIAL_GCG_CARD_FACE":              (SOURCE_TYPE_GCG, SUB_CARD_FACE),
    "MATERIAL_FOOD":                        (SOURCE_TYPE_OTHER_MAT, SUB_NONE),
    "MATERIAL_GCG_CARD":                    (SOURCE_TYPE_GCG, SUB_CARD),
    "MATERIAL_CONSUME":                     (SOURCE_TYPE_MATERIAL, SUB_CONSUMABLE),
    "MATERIAL_AVATAR_MATERIAL":             (SOURCE_TYPE_AVATAR_MAT, SUB_NONE),
    "MATERIAL_NOTICE_ADD_HP":               (SOURCE_TYPE_FOOD, SUB_NONE),
    "MATERIAL_NAMECARD":                    (SOURCE_TYPE_NAMECARD, SUB_NONE),
    "MATERIAL_EXCHANGE":                    (SOURCE_TYPE_MATERIAL, SUB_NONE),
    "MATERIAL_BGM":                         (SOURCE_TYPE_ITEM, SUB_BGM),
    "MATERIAL_PHOTOGRAPH_POSE":             (SOURCE_TYPE_PERFORMANCE, SUB_NONE),
    "MATERIAL_WIDGET":                      (SOURCE_TYPE_ITEM, SUB_WIDGET),
    "MATERIAL_AVATAR":                      (SOURCE_TYPE_AVATAR_INTRO, SUB_AVATAR_INTRO),
    "MATERIAL_NONE":                        (SOURCE_TYPE_MATERIAL, SUB_NONE),
    "MATERIAL_TALENT":                      (SOURCE_TYPE_MATERIAL, SUB_CONSTELLATION),
    "MATERIAL_AVATAR_TALENT_MATERIAL":      (SOURCE_TYPE_MATERIAL, SUB_CONSTELLATION),
    "MATERIAL_FURNITURE_SUITE_FORMULA":     (SOURCE_TYPE_BLUEPRINT, SUB_SUITE_BLUEPRINT),
    "MATERIAL_CHEST":                       (SOURCE_TYPE_ITEM, SUB_CHEST),
    "MATERIAL_CHANNELLER_SLAB_BUFF":        (SOURCE_TYPE_ITEM, SUB_EVENT_ITEM),
    "MATERIAL_HOME_SEED":                   (SOURCE_TYPE_MATERIAL, SUB_SEED),
    "MATERIAL_GCG_CARD_BACK":              (SOURCE_TYPE_GCG, SUB_CARD_BACK),
    "MATERIAL_SPICE_FOOD":                  (SOURCE_TYPE_FOOD, SUB_EVENT_FOOD),
    "MATERIAL_WOOD":                        (SOURCE_TYPE_MATERIAL, SUB_WOOD),
    "MATERIAL_ADSORBATE":                   (SOURCE_TYPE_MATERIAL, SUB_NONE),
    "MATERIAL_CONSUME_BATCH_USE":           (SOURCE_TYPE_MATERIAL, SUB_CONSUMABLE),
    "MATERIAL_COSTUME":                     (SOURCE_TYPE_DRESSING, SUB_COSTUME_DRESS),
    "MATERIAL_MUSIC_GAME_BOOK_THEME":       (SOURCE_TYPE_MUSIC_THEME, SUB_NONE),
    "MATERIAL_ACTIVITY_ROBOT":              (SOURCE_TYPE_ITEM, SUB_EVENT_ITEM),
    "MATERIAL_AVATAR_TRACE":                (SOURCE_TYPE_DRESSING, SUB_TRACE),
    "MATERIAL_FLYCLOAK":                    (SOURCE_TYPE_DRESSING, SUB_FLYCLOAK_SUB),
    "MATERIAL_FIREWORKS":                   (SOURCE_TYPE_ITEM, SUB_FIREWORKS),
    "MATERIAL_SELECTABLE_CHEST":            (SOURCE_TYPE_ITEM, SUB_CHEST),
    "MATERIAL_WEAPON_SKIN":                 (SOURCE_TYPE_DRESSING, SUB_WEAPON_SKIN),
    "MATERIAL_ELEM_CRYSTAL":                (SOURCE_TYPE_MATERIAL, SUB_NONE),
    "MATERIAL_FISH_BAIT":                   (SOURCE_TYPE_MATERIAL, SUB_FISH_BAIT),
    "MATERIAL_CHEST_BATCH_USE":             (SOURCE_TYPE_ITEM, SUB_CHEST),
    "MATERIAL_FISH_ROD":                    (SOURCE_TYPE_ITEM, SUB_FISH_ROD),
    "MATERIAL_ACTIVITY_GEAR":               (SOURCE_TYPE_ITEM, SUB_EVENT_ITEM),
    "MATERIAL_GCG_FIELD":                   (SOURCE_TYPE_GCG, SUB_GCG_BOX),
    "MATERIAL_ACTIVITY_JIGSAW":             (SOURCE_TYPE_ITEM, SUB_EVENT_ITEM),
    "MATERIAL_QUEST_EVENT_BOOK":            (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_RENAME_ITEM":                 (SOURCE_TYPE_MATERIAL, SUB_CONSUMABLE),
    "MATERIAL_CRICKET":                     (SOURCE_TYPE_OTHER_MAT, SUB_NONE),
    "MATERIAL_RELIQUARY_MATERIAL":          (SOURCE_TYPE_MATERIAL, SUB_NONE),
    "MATERIAL_CHEST_BATCH_USE_WITH_GROUP":  (SOURCE_TYPE_ITEM, SUB_CHEST),
    "MATERIAL_QUEST_ALBUM":                 (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_PROFILE_FRAME":               (SOURCE_TYPE_DRESSING, SUB_PROFILE_FRAME),
    "MATERIAL_FAKE_ABSORBATE":              (SOURCE_TYPE_ITEM, SUB_EVENT_ITEM),
    "MATERIAL_EXP_FRUIT":                   (SOURCE_TYPE_MATERIAL, SUB_EXP_MATERIAL),
    "MATERIAL_WEAPON_EXP_STONE":            (SOURCE_TYPE_MATERIAL, SUB_EXP_MATERIAL),
    "MATERIAL_PROFILE_PICTURE":             (SOURCE_TYPE_DRESSING, SUB_PROFILE_PICTURE),
    "MATERIAL_NATLAN_RACE_ALBUM":           (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_NATLAN_RACE_ENVELOPE":        (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_ROBO_GIFT":                   (SOURCE_TYPE_MATERIAL, SUB_CONSUMABLE),
    "MATERIAL_FIRE_MASTER_AVATAR_TALENT_ITEM": (SOURCE_TYPE_MATERIAL, SUB_CONSTELLATION),
    "MATERIAL_RARE_GROWTH_MATERIAL":        (SOURCE_TYPE_MATERIAL, SUB_BREAKTHROUGH),
    "MATERIAL_GCG_EXCHANGE_ITEM":           (SOURCE_TYPE_MATERIAL, SUB_CONSUMABLE),
    "MATERIAL_BEYOND_COSTUME_SELECTABLE_CHEST": (SOURCE_TYPE_ITEM, SUB_CHEST),
    "MATERIAL_RAINBOW_PRINCE_HAND_BOOK":    (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_PHOTO_DISPLAY_BOOK":          (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_REMUS_MUSIC_BOX":             (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_GREATEFESTIVALV2_INVITE":     (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_PHOTOV5_HAND_BOOK":           (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_NATLAN_RELATION_A":           (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_NATLAN_RELATION_B":           (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_LANV5_PAIMON_GREETING_CARD":  (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_MIKAWA_FLOWER_INVITE":        (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_HOLIDAY_MEMORY_BOOK":         (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_HOLIDAY_RESORT_INVITE":       (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_PHOTOV6_HAND_BOOK":           (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_CLUE_SHOP_HANDBOOK":          (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_MOON_NIGHT_CARD":             (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_MAGIC_STORY_BOOK":            (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_BRONZE_CARRIAGE_BOX":         (SOURCE_TYPE_ITEM, SUB_WIDGET),
    "MATERIAL_BUBBLE_DRAMA_INVITE":         (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_SEA_LAMP":                    (SOURCE_TYPE_ITEM, SUB_WIDGET),
    "MATERIAL_ARANARA":                     (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
    "MATERIAL_DESHRET_MANUAL":              (SOURCE_TYPE_ITEM, SUB_QUEST_ITEM),
}

# ── 二级分类标签（也供 controllers 使用）────────────────────────────
SUB_CATEGORY_LABELS: dict[int, str] = {
    SUB_NONE: "",
    SUB_QUEST_ITEM: "任务道具",
    SUB_FURN_BLUEPRINT: "摆设图纸",
    SUB_CARD_FACE: "牌面",
    SUB_CARD: "卡牌",
    SUB_CONSUMABLE: "消耗品",
    SUB_AVATAR_INTRO: "角色介绍",
    SUB_CONSTELLATION: "命之座激活素材",
    SUB_SUITE_BLUEPRINT: "摆设套装图纸",
    SUB_CHEST: "宝箱",
    SUB_EVENT_ITEM: "活动道具",
    SUB_WIDGET: "小道具",
    SUB_SEED: "种子",
    SUB_CARD_BACK: "牌背",
    SUB_EVENT_FOOD: "活动食物",
    SUB_WOOD: "木材",
    SUB_EXP_MATERIAL: "经验素材",
    SUB_COSTUME_DRESS: "衣装",
    SUB_TRACE: "游迹",
    SUB_FLYCLOAK_SUB: "风之翼",
    SUB_FIREWORKS: "烟花",
    SUB_FISH_ROD: "钓竿",
    SUB_WEAPON_SKIN: "武器外观",
    SUB_PROFILE_FRAME: "头像框",
    SUB_PROFILE_PICTURE: "头像",
    SUB_GCG_BOX: "牌盒",
    SUB_FISH_BAIT: "鱼饵",
    SUB_BGM: "旋曜玉帛",
    SUB_BREAKTHROUGH: "角色突破素材",
    SUB_QIANXING_PARADOX: "奇偶装扮",
    SUB_QIANXING_SUIT: "装扮套装",
}

# ── 内置大分类标签 ─────────────────────────────────────────────────
SOURCE_TYPE_LABELS: dict[int, str] = {
    SOURCE_TYPE_ITEM: "道具",
    SOURCE_TYPE_FOOD: "食物",
    SOURCE_TYPE_FURNISHING: "摆设",
    SOURCE_TYPE_GADGET: "小道具",
    SOURCE_TYPE_COSTUME: "千星奇域",
    SOURCE_TYPE_SUIT: "千星奇域",
    SOURCE_TYPE_WEAPON: "武器",
    SOURCE_TYPE_RELIQUARY: "圣遗物",
    SOURCE_TYPE_MONSTER: "怪物",
    SOURCE_TYPE_CREATURE: "生物",
    SOURCE_TYPE_MATERIAL: "材料",
    SOURCE_TYPE_BLUEPRINT: "图纸",
    SOURCE_TYPE_GCG: "七圣召唤",
    SOURCE_TYPE_NAMECARD: "名片",
    SOURCE_TYPE_PERFORMANCE: "表演诀窍",
    SOURCE_TYPE_AVATAR_INTRO: "角色",
    SOURCE_TYPE_DRESSING: "装扮",
    SOURCE_TYPE_MUSIC_THEME: "演奏主题",
    SOURCE_TYPE_OTHER_MAT: "其他",
    SOURCE_TYPE_AVATAR_MAT: "角色突破素材",
    SOURCE_TYPE_ACHIEVEMENT: "成就",
    SOURCE_TYPE_VIEWPOINT: "观景点",
    SOURCE_TYPE_DUNGEON: "秘境",
    SOURCE_TYPE_LOADING_TIP: "过场提示",
}


# ── 源文件自修改 ──────────────────────────────────────────────────
_THIS_FILE = os.path.abspath(__file__)
_THIS_MODULE = __name__


def _source_type_const_name(code: int) -> str:
    """Return the SOURCE_TYPE_* constant name for a given code."""
    import sys
    mod = sys.modules[_THIS_MODULE]
    for name in dir(mod):
        if name.startswith("SOURCE_TYPE_") and getattr(mod, name) == code:
            return name
    return str(code)


def _sub_const_name(code: int) -> str:
    """Return the SUB_* constant name for a given code."""
    import sys
    mod = sys.modules[_THIS_MODULE]
    for name in dir(mod):
        if name.startswith("SUB_") and not name.startswith("SUB_CATEGORY") and getattr(mod, name) == code:
            return name
    return str(code)


def append_to_source(
    *,
    material_type_entries: dict[str, tuple[str, str]] | None = None,
    source_type_const: tuple[str, int, str] | None = None,
    sub_const: tuple[str, int, str] | None = None,
):
    """Append new entries directly into this source file.

    Args:
        material_type_entries: {"MATERIAL_XXX": ("SOURCE_TYPE_YYY", "SUB_ZZZ"), ...}
        source_type_const: (const_name, code, label) — new SOURCE_TYPE_* constant
        sub_const: (const_name, code, label) — new SUB_* constant
    """
    with open(_THIS_FILE, "r", encoding="utf-8") as f:
        lines = f.read().split("\n")

    if source_type_const:
        name, code, label = source_type_const
        # insert constant before FIELD_DESC
        for i, line in enumerate(lines):
            if line.startswith("FIELD_DESC"):
                lines.insert(i, f"{name} = {code}    # {label}")
                break
        # append to SOURCE_TYPE_LABELS
        for i, line in enumerate(lines):
            if "SOURCE_TYPE_LABELS" in line and "dict[int" in line:
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() == "}":
                        lines.insert(j, f'    {name}: "{label}",')
                        break
                break

    if sub_const:
        name, code, label = sub_const
        # insert constant before "# materialType →" comment (or MATERIAL_TYPE_TO_CATEGORY)
        for i, line in enumerate(lines):
            if "MATERIAL_TYPE_TO_CATEGORY" in line and "dict[str" in line:
                lines.insert(i, f"{name} = {code}    # {label}")
                break
        # append to SUB_CATEGORY_LABELS
        for i, line in enumerate(lines):
            if "SUB_CATEGORY_LABELS" in line and "dict[int" in line:
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() == "}":
                        lines.insert(j, f'    {name}: "{label}",')
                        break
                break

    if material_type_entries:
        for i, line in enumerate(lines):
            if "MATERIAL_TYPE_TO_CATEGORY" in line and "dict[str" in line:
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() == "}":
                        for mt_key, (st_name, sub_name) in sorted(material_type_entries.items()):
                            pad = max(1, 40 - len(f'    "{mt_key}":'))
                            entry = f'    "{mt_key}":{" " * pad}({st_name}, {sub_name}),'
                            lines.insert(j, entry)
                            j += 1
                        break
                break

    with open(_THIS_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def register_source_type(const_name: str, code: int, label: str):
    """Register a new SOURCE_TYPE_* constant at runtime and persist to source."""
    import sys
    mod = sys.modules[_THIS_MODULE]
    setattr(mod, const_name, code)
    SOURCE_TYPE_LABELS[code] = label
    append_to_source(source_type_const=(const_name, code, label))


def register_sub_category(const_name: str, code: int, label: str):
    """Register a new SUB_* constant at runtime and persist to source."""
    import sys
    mod = sys.modules[_THIS_MODULE]
    setattr(mod, const_name, code)
    SUB_CATEGORY_LABELS[code] = label
    append_to_source(sub_const=(const_name, code, label))

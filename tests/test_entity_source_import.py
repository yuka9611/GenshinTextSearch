"""Tests for entity source import helpers."""
import os
import sqlite3
import sys

import pytest


TESTS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.normpath(os.path.join(TESTS_DIR, os.pardir))
DBBUILD_DIR = os.path.join(REPO_ROOT, "server", "dbBuild")
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import entitySourceImport
import history_backfill
import databaseHelper


@pytest.fixture(autouse=True)
def _restore_database_helper_schema_cache():
    table_cache = dict(databaseHelper._CACHE["table"])
    column_cache = dict(databaseHelper._CACHE["column"])
    yield
    databaseHelper._CACHE["table"].clear()
    databaseHelper._CACHE["table"].update(table_cache)
    databaseHelper._CACHE["column"].clear()
    databaseHelper._CACHE["column"].update(column_cache)


def test_gender_code_marks_both_body_types_as_unisex():
    assert entitySourceImport._gender_code(["BODY_GIRL", "BODY_BOY"]) == 3


def test_iter_costume_mappings_supports_obfuscated_body_type_field():
    rows = [
        {
            "costumeId": 260001,
            "nameTextMapHash": 2098177162,
            "descriptionTextMapHash": 33590471,
            "IAHOEKGIPPJ": ["BODY_GIRL", "BODY_BOY"],
        }
    ]

    result = list(entitySourceImport._iter_costume_mappings(rows))

    assert result == [
        (
            33590471,
            entitySourceImport.SOURCE_TYPE_COSTUME,
            260001,
            2098177162,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC, 3),
            entitySourceImport.SUB_QIANXING_PARADOX,
        )
    ]


def test_iter_costume_mappings_supports_6_6_body_type_field():
    rows = [
        {
            "costumeId": 260002,
            "nameTextMapHash": 2098177162,
            "descriptionTextMapHash": 33590471,
            "AFAENJLHMOD": ["BODY_GIRL"],
        }
    ]

    result = list(entitySourceImport._iter_costume_mappings(rows))

    assert result == [
        (
            33590471,
            entitySourceImport.SOURCE_TYPE_COSTUME,
            260002,
            2098177162,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC, 2),
            entitySourceImport.SUB_QIANXING_PARADOX,
        )
    ]


def test_iter_suit_mappings_emits_qianxing_suit_source_type_and_gender():
    rows = [
        {
            "suitId": 265311,
            "nameTextMapHash": 2365153540,
            "descriptionTextMapHash": 338370744,
            "AFAENJLHMOD": ["BODY_GIRL", "BODY_BOY"],
        }
    ]

    result = list(entitySourceImport._iter_suit_mappings(rows))

    assert result == [
        (
            338370744,
            entitySourceImport.SOURCE_TYPE_SUIT,
            265311,
            2365153540,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC, 3),
            entitySourceImport.SUB_QIANXING_SUIT,
        )
    ]


def test_iter_emoji_mappings_emits_qianxing_emoji_desc():
    rows = [
        {
            "id": 1001,
            "nameTextMapHash": 741611961,
            "descriptionTextMapHash": 2762799464,
        }
    ]

    result = list(entitySourceImport._iter_emoji_mappings(rows))

    assert result == [
        (
            2762799464,
            entitySourceImport.SOURCE_TYPE_QIANXING_EMOJI,
            1001,
            741611961,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_EMOJI,
        )
    ]


def test_iter_pose_mappings_marks_gender_from_body_type():
    rows = [
        {
            "id": 611000,
            "nameTextMapHash": 1491722704,
            "descriptionTextMapHash": 4282661314,
            "bodyType": "BODY_GIRL",
        }
    ]

    result = list(entitySourceImport._iter_pose_mappings(rows))

    assert result == [
        (
            4282661314,
            entitySourceImport.SOURCE_TYPE_QIANXING_POSE,
            611000,
            1491722704,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC, 2),
            entitySourceImport.SUB_QIANXING_POSE,
        )
    ]


def test_iter_effect_mappings_emits_qianxing_effect_desc():
    rows = [
        {
            "id": 2001,
            "nameTextMapHash": 3244865603,
            "descriptionTextMapHash": 3531184282,
        }
    ]

    result = list(entitySourceImport._iter_effect_mappings(rows))

    assert result == [
        (
            3531184282,
            entitySourceImport.SOURCE_TYPE_QIANXING_EFFECT,
            2001,
            3244865603,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_EFFECT,
        )
    ]


def test_iter_hall_template_mappings_skips_public_hall_and_supports_fallback_keys():
    rows = [
        {
            "DCOBMNILGJL": 15,
            "KMMKMJLOFGC": 1209931833,
            "DKBHBHOOGAP": 3795564820,
            "BMIILBDKBIO": "BEYOND_HALL_PRIVATE",
        },
        {
            "COGKFPLDLLL": 1015,
            "LDCAAIEKMOE": 1823553705,
            "BPKNEMEJEPF": 4197610804,
            "PEMNJBEBBOG": "BEYOND_HALL_PUBLIC",
        },
    ]

    result = list(entitySourceImport._iter_hall_template_mappings(rows))

    assert result == [
        (
            3795564820,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            15,
            1209931833,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]


def test_iter_hall_template_mappings_supports_6_6_keys():
    rows = [
        {
            "OCHDBIAAHIO": 1,
            "CAMAHAEKAIH": 1209931833,
            "PEODHMPDKNF": 3795564820,
            "KMDBAGPDKNG": "BEYOND_HALL_PRIVATE",
        },
        {
            "OCHDBIAAHIO": 1002,
            "CAMAHAEKAIH": 1823553705,
            "PEODHMPDKNF": 4197610804,
            "KMDBAGPDKNG": "BEYOND_HALL_PUBLIC",
        },
    ]

    result = list(entitySourceImport._iter_hall_template_mappings(rows))

    assert result == [
        (
            3795564820,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            1,
            1209931833,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]


def test_iter_hall_template_mappings_supports_6_7_keys():
    rows = [
        {
            "CKIGKAIIFFI": 11,
            "AOGCNHLHJMJ": 3153252249,
            "PPOAOFDNLDJ": 2073840156,
            "BNKLMBACEDF": "BEYOND_HALL_PRIVATE",
        },
        {
            "CKIGKAIIFFI": 1011,
            "AOGCNHLHJMJ": 4116792297,
            "PPOAOFDNLDJ": 1271560124,
            "BNKLMBACEDF": "BEYOND_HALL_PUBLIC",
        },
    ]

    result = list(entitySourceImport._iter_hall_template_mappings(rows))

    assert result == [
        (
            2073840156,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            11,
            3153252249,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]


def test_iter_hall_facility_mappings_accepts_bwiki_style_id_fallback_keys():
    rows = [
        {
            "id": 351101,
            "nameTextMapHash": 2097032544,
            "descTextMapHash": 3061297604,
            "DGBOKBNOJKE": 1,
        }
    ]

    result = list(entitySourceImport._iter_hall_facility_mappings(rows))

    assert result == [
        (
            3061297604,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            351101,
            2097032544,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]


def test_iter_hall_facility_mappings_supports_6_6_style_keys():
    rows = [
        {
            "id": 351101,
            "nameTextMapHash": 2097032544,
            "descTextMapHash": 3061297604,
            "FEIJJDIAHFJ": 0,
            "KJJPGPAKCIF": 1,
        }
    ]

    result = list(entitySourceImport._iter_hall_facility_mappings(rows))

    assert result == [
        (
            3061297604,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            351101,
            2097032544,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]


def test_iter_hall_facility_mappings_supports_6_7_style_key():
    rows = [
        {
            "id": 351101,
            "nameTextMapHash": 2097033056,
            "descTextMapHash": 3061298116,
            "BBLFLDMDBNJ": 1,
        }
    ]

    result = list(entitySourceImport._iter_hall_facility_mappings(rows))

    assert result == [
        (
            3061298116,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            351101,
            2097033056,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]


def test_iter_avatar_skill_mappings_uses_formal_avatar_depots_only():
    avatars = [
        {
            "id": 10000002,
            "useType": "AVATAR_FORMAL",
            "skillDepotId": 201,
            "candSkillDepotIds": [202],
        },
        {
            "id": 10000001,
            "useType": "AVATAR_TEST",
            "skillDepotId": 101,
            "candSkillDepotIds": [],
        },
    ]
    depots = [
        {
            "id": 201,
            "skills": [10024, 0],
            "subSkills": [10020],
            "energySkill": 10019,
            "attackModeSkill": 0,
        },
        {
            "id": 202,
            "skills": [20024],
            "subSkills": [],
            "energySkill": 0,
            "attackModeSkill": 0,
        },
        {
            "id": 101,
            "skills": [90001],
            "subSkills": [],
            "energySkill": 0,
            "attackModeSkill": 0,
        },
    ]
    skills = [
        {
            "id": 10024,
            "nameTextMapHash": 101,
            "descTextMapHash": 102,
            "extraDescTextMapHash": 103,
        },
        {
            "id": 10020,
            "nameTextMapHash": 201,
            "descTextMapHash": 202,
            "extraDescTextMapHash": 0,
        },
        {
            "id": 10019,
            "nameTextMapHash": 301,
            "descTextMapHash": 302,
            "extraDescTextMapHash": 0,
        },
        {
            "id": 20024,
            "nameTextMapHash": 401,
            "descTextMapHash": 402,
            "extraDescTextMapHash": 0,
        },
        {
            "id": 90001,
            "nameTextMapHash": 901,
            "descTextMapHash": 902,
            "extraDescTextMapHash": 0,
        },
    ]

    result = list(entitySourceImport._iter_avatar_skill_mappings(avatars, depots, skills))

    assert result == [
        (
            101,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            10024,
            101,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_TITLE),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            102,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            10024,
            101,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            103,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            10024,
            101,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_SPECIAL_DESC),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            201,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            10020,
            201,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_TITLE),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            202,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            10020,
            201,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            301,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            10019,
            301,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_TITLE),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            302,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            10019,
            301,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            401,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            20024,
            401,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_TITLE),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
        (
            402,
            entitySourceImport.SOURCE_TYPE_AVATAR_INTRO,
            20024,
            401,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_AVATAR_SKILL,
        ),
    ]


def _create_gcg_text_tables(connection: sqlite3.Connection):
    connection.executescript(
        """
        CREATE TABLE textMap (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash INTEGER,
            content TEXT,
            lang INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER,
            UNIQUE(lang, hash)
        );
        CREATE TABLE text_source_entity (
            text_hash INTEGER NOT NULL,
            source_type_code INTEGER NOT NULL,
            entity_id INTEGER NOT NULL,
            title_hash INTEGER NOT NULL,
            extra INTEGER NOT NULL DEFAULT 0,
            sub_category INTEGER NOT NULL DEFAULT 0,
            created_version_id INTEGER,
            PRIMARY KEY(text_hash, source_type_code, entity_id)
        );
        """
    )


def _insert_gcg_text_source_rows(connection: sqlite3.Connection, data: dict, version_id: int = 1):
    rows = [(*row, version_id) for row in entitySourceImport._build_rows_iter(data, {})]
    connection.executemany(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (?,?,?,?,?,?,?)
        """,
        rows,
    )


def test_gcg_text_resolver_replaces_declared_refs_names_and_plural(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _create_gcg_text_tables(connection)
        connection.executemany(
            "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [
                (1000, "物理伤害", 1, 1, 1),
                (1001, "万能元素", 1, 1, 1),
                (1002, "冰莲", 1, 1, 1),
                (1003, "霜华矢", 1, 1, 1),
                (1004, "铁甲熔火帝皇", 1, 1, 1),
            ],
        )
        monkeypatch.setattr(
            entitySourceImport,
            "_load_gcg_declared_values",
            lambda skill_json: {
                "damage": {"MEGMIMEDODJ": "Damage", "AOJNMJNAEEO": 2},
                "element": {"MEGMIMEDODJ": "Element", "CAHOPGJMELB": "GCG_ELEMENT_PHYSIC"},
            },
        )
        resolver = entitySourceImport._GcgTextResolver(
            connection.cursor(),
            {
                "gcg_cards": [{"id": 111012, "nameTextMapHash": 1002}],
                "gcg_chars": [{"id": 2304, "nameTextMapHash": 1004}],
                "gcg_skills": [{"id": 11011, "nameTextMapHash": 1003}],
                "gcg_keywords": [
                    {"id": 100, "titleTextMapHash": 1000},
                    {"id": 411, "titleTextMapHash": 1001},
                ],
                "gcg_elements": [{"type": "GCG_ELEMENT_PHYSIC", "keywordId": 100}],
            },
        )

        result = resolver.resolve(
            "造成$[D__KEY__DAMAGE]点$[D__KEY__ELEMENT]，生成$[K411]、$[C111012]、$[S11011]、$[A2304]。{PLURAL#2|单数|复数}",
            1,
            "dummy",
        )

        assert result == "造成2点物理伤害，生成万能元素、冰莲、霜华矢、铁甲熔火帝皇。复数"
    finally:
        connection.close()


def test_gcg_text_resolver_falls_back_to_character_element_and_default_damage(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _create_gcg_text_tables(connection)
        connection.executemany(
            "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [
                (1000, "雷元素伤害", 1, 1, 1),
            ],
        )
        monkeypatch.setattr(entitySourceImport, "_load_gcg_declared_values", lambda skill_json: {})
        resolver = entitySourceImport._GcgTextResolver(
            connection.cursor(),
            {
                "gcg_cards": [],
                "gcg_chars": [{"id": 1415, "tagList": ["GCG_TAG_ELEMENT_ELECTRO"]}],
                "gcg_skills": [],
                "gcg_keywords": [{"id": 104, "titleTextMapHash": 1000}],
                "gcg_elements": [{"type": "GCG_ELEMENT_ELECTRO", "keywordId": 104}],
            },
        )

        result = resolver.resolve("造成$[D__KEY__DAMAGE]点$[D__KEY__ELEMENT]。", 1, "empty", card_id=1415)

        assert result == "造成1点雷元素伤害。"
    finally:
        connection.close()


def test_iter_gcg_card_skill_mappings_links_character_card_skills_to_synthetic_hash():
    materials = [
        {
            "id": 330000,
            "nameTextMapHash": 1082201100,
            "descTextMapHash": 2247414261,
            "materialType": "MATERIAL_GCG_CARD",
            "itemUse": [
                {"useOp": "ITEM_USE_GAIN_GCG_CARD", "useParam": ["1101", "", "", "", ""]},
            ],
        }
    ]
    gcg_chars = [{"id": 1101, "skillList": [11011, 11012]}]
    gcg_skills = [
        {"id": 11011, "descTextMapHash": 3920541026},
        {"id": 11012, "descTextMapHash": 2684862714},
    ]

    result = list(entitySourceImport._iter_gcg_card_skill_mappings(materials, [], gcg_chars, gcg_skills))

    assert [row[:4] for row in result] == [
        (
            entitySourceImport._stable_gcg_synthetic_hash("skill", 11011, 3920541026),
            entitySourceImport.SOURCE_TYPE_GCG,
            330000,
            1082201100,
        ),
        (
            entitySourceImport._stable_gcg_synthetic_hash("skill", 11012, 2684862714),
            entitySourceImport.SOURCE_TYPE_GCG,
            330000,
            1082201100,
        ),
    ]
    assert [row[4] & 0xFF for row in result] == [entitySourceImport.FIELD_SKILL_DESC, entitySourceImport.FIELD_SKILL_DESC]
    assert [row[5] for row in result] == [entitySourceImport.SUB_CARD, entitySourceImport.SUB_CARD]


def test_iter_gcg_card_skill_mappings_links_action_card_desc_to_synthetic_hash():
    materials = [
        {
            "id": 332001,
            "nameTextMapHash": 3330146804,
            "materialType": "MATERIAL_GCG_CARD",
            "itemUse": [
                {"useOp": "ITEM_USE_GAIN_GCG_CARD", "useParam": ["321002", "", "", "", ""]},
            ],
        }
    ]
    gcg_cards = [{"id": 321002, "descTextMapHash": 1420628032, "skillList": [3210021, 3210022]}]
    gcg_skills = [
        {"id": 3210021, "descTextMapHash": 253515098},
        {"id": 3210022, "descTextMapHash": 0},
    ]

    result = list(entitySourceImport._iter_gcg_card_skill_mappings(materials, gcg_cards, [], gcg_skills))

    assert result == [
        (
            entitySourceImport._stable_gcg_synthetic_hash("card", 321002, 1420628032),
            entitySourceImport.SOURCE_TYPE_GCG,
            332001,
            3330146804,
            entitySourceImport._pack_gcg_text_extra("card", 321002),
            entitySourceImport.SUB_CARD,
        )
    ]


def test_iter_gcg_card_skill_mappings_skips_invalid_missing_and_duplicate_hashes():
    materials = [
        {
            "id": 332001,
            "nameTextMapHash": 3330146804,
            "descTextMapHash": 253515098,
            "effectDescTextMapHash": 784460031,
            "materialType": "MATERIAL_GCG_CARD",
            "itemUse": [
                {"useOp": "ITEM_USE_GAIN_GCG_CARD", "useParam": ["321002", "", "", "", ""]},
            ],
        },
        {
            "id": 332002,
            "nameTextMapHash": 1,
            "materialType": "MATERIAL_GCG_CARD",
            "itemUse": [
                {"useOp": "ITEM_USE_GAIN_GCG_CARD", "useParam": ["", "", "", "", ""]},
            ],
        },
        {
            "id": 332003,
            "nameTextMapHash": 2,
            "materialType": "MATERIAL_GCG_CARD",
            "itemUse": [
                {"useOp": "ITEM_USE_GAIN_GCG_CARD", "useParam": ["999999", "", "", "", ""]},
            ],
        },
    ]
    gcg_cards = [{"id": 321002, "descTextMapHash": 600}]
    gcg_skills = []

    result = list(entitySourceImport._iter_gcg_card_skill_mappings(materials, gcg_cards, [], gcg_skills))

    assert result == [
        (
            entitySourceImport._stable_gcg_synthetic_hash("card", 321002, 600),
            entitySourceImport.SOURCE_TYPE_GCG,
            332001,
            3330146804,
            entitySourceImport._pack_gcg_text_extra("card", 321002),
            entitySourceImport.SUB_CARD,
        )
    ]


def test_refresh_gcg_synthetic_textmap_replaces_skill_placeholders_and_cleans_stale(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _create_gcg_text_tables(connection)
        cursor = connection.cursor()
        data = {
            "materials": [
                {
                    "id": 330000,
                    "nameTextMapHash": 1082201100,
                    "materialType": "MATERIAL_GCG_CARD",
                    "itemUse": [{"useOp": "ITEM_USE_GAIN_GCG_CARD", "useParam": ["1101"]}],
                }
            ],
            "furnitures": [],
            "costumes": [],
            "suits": [],
            "emojis": [],
            "poses": [],
            "effects": [],
            "halls": [],
            "hall_facilities": [],
            "avatar_costumes": [],
            "weapons": [],
            "reliquaries": [],
            "codex": [],
            "achievements": [],
            "viewpoints": [],
            "dungeons": [],
            "loading_tips": [],
            "gcg_cards": [],
            "gcg_chars": [{"id": 1101, "skillList": [11011]}],
            "gcg_skills": [{"id": 11011, "descTextMapHash": 3920541026, "skillJson": "Effect_Damage_Physic_2"}],
            "gcg_keywords": [{"id": 100, "titleTextMapHash": 1000}],
            "gcg_elements": [{"type": "GCG_ELEMENT_PHYSIC", "keywordId": 100}],
            "describe_title_map": {},
            "codex_desc_map": {},
        }
        connection.executemany(
            "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [
                (3920541026, "造成$[D__KEY__DAMAGE]点$[D__KEY__ELEMENT]。", 1, 1, 1),
                (1000, "物理伤害", 1, 1, 1),
                (-999, "旧合成文本", 1, 1, 1),
            ],
        )
        cursor.execute(
            "CREATE TABLE gcg_synthetic_textmap (hash INTEGER PRIMARY KEY, source_kind TEXT NOT NULL, source_id INTEGER NOT NULL, raw_hash INTEGER NOT NULL)"
        )
        cursor.execute(
            "INSERT INTO gcg_synthetic_textmap(hash, source_kind, source_id, raw_hash) VALUES (-999, 'skill', 999, 999)"
        )
        cursor.execute(
            """
            INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
            VALUES (-999, ?, 330000, 1082201100, ?, ?, 1)
            """,
            (
                entitySourceImport.SOURCE_TYPE_GCG,
                entitySourceImport._pack_extra(entitySourceImport.FIELD_SKILL_DESC),
                entitySourceImport.SUB_CARD,
            ),
        )
        monkeypatch.setattr(
            entitySourceImport,
            "_load_gcg_declared_values",
            lambda skill_json: {
                "damage": {"MEGMIMEDODJ": "Damage", "AOJNMJNAEEO": 2},
                "element": {"MEGMIMEDODJ": "Element", "CAHOPGJMELB": "GCG_ELEMENT_PHYSIC"},
            },
        )

        entitySourceImport._refresh_gcg_synthetic_textmap(cursor, data, 2)
        entitySourceImport._clear_gcg_skill_entity_sources(cursor)
        _insert_gcg_text_source_rows(connection, data, version_id=2)

        synthetic_hash = entitySourceImport._stable_gcg_synthetic_hash("skill", 11011, 3920541026)
        assert connection.execute("SELECT content FROM textMap WHERE hash=-999").fetchone() is None
        assert connection.execute("SELECT content FROM textMap WHERE hash=? AND lang=1", (3920541026,)).fetchone()[0] == "造成$[D__KEY__DAMAGE]点$[D__KEY__ELEMENT]。"
        assert connection.execute("SELECT content FROM textMap WHERE hash=? AND lang=1", (synthetic_hash,)).fetchone()[0] == "造成2点物理伤害。"
        rows = connection.execute(
            "SELECT text_hash, extra & 255 FROM text_source_entity WHERE entity_id=330000"
        ).fetchall()
        assert rows == [(synthetic_hash, entitySourceImport.FIELD_SKILL_DESC)]
    finally:
        connection.close()


def test_refresh_gcg_synthetic_textmap_uses_action_card_desc(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _create_gcg_text_tables(connection)
        cursor = connection.cursor()
        data = {
            "materials": [
                {
                    "id": 332001,
                    "nameTextMapHash": 3330146804,
                    "materialType": "MATERIAL_GCG_CARD",
                    "itemUse": [{"useOp": "ITEM_USE_GAIN_GCG_CARD", "useParam": ["321002"]}],
                }
            ],
            "furnitures": [],
            "costumes": [],
            "suits": [],
            "emojis": [],
            "poses": [],
            "effects": [],
            "halls": [],
            "hall_facilities": [],
            "avatar_costumes": [],
            "weapons": [],
            "reliquaries": [],
            "codex": [],
            "achievements": [],
            "viewpoints": [],
            "dungeons": [],
            "loading_tips": [],
            "gcg_cards": [{"id": 321002, "descTextMapHash": 1420628032, "skillList": [3210021]}],
            "gcg_chars": [],
            "gcg_skills": [{"id": 3210021, "descTextMapHash": 1770233178}],
            "gcg_keywords": [{"id": 411, "titleTextMapHash": 1001}],
            "gcg_elements": [],
            "describe_title_map": {},
            "codex_desc_map": {},
        }
        connection.executemany(
            "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [
                (1420628032, "生成2个万能元素。", 1, 3, 4),
                (1001, "万能元素", 1, 1, 1),
            ],
        )
        monkeypatch.setattr(entitySourceImport, "_load_gcg_declared_values", lambda skill_json: {})

        entitySourceImport._refresh_gcg_synthetic_textmap(cursor, data, 5)
        _insert_gcg_text_source_rows(connection, data, version_id=5)

        assert connection.execute("SELECT content, created_version_id, updated_version_id FROM textMap WHERE hash=? AND lang=1", (1420628032,)).fetchone() == (
            "生成2个万能元素。",
            3,
            4,
        )
        assert connection.execute("SELECT COUNT(*) FROM text_source_entity WHERE text_hash=1420628032").fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM textMap tm JOIN gcg_synthetic_textmap gs ON gs.hash=tm.hash"
        ).fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM text_source_entity WHERE text_hash=1770233178").fetchone()[0] == 0
    finally:
        connection.close()


def test_load_gcg_declared_values_missing_file_returns_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(entitySourceImport, "DATA_PATH", str(tmp_path))
    entitySourceImport._GCG_DECLARED_VALUE_CACHE.clear()

    assert entitySourceImport._load_gcg_declared_values("Char_Skill_17122") == {}
    assert entitySourceImport._GCG_DECLARED_VALUE_CACHE["Char_Skill_17122"] == {}


def _create_entity_version_tables(connection: sqlite3.Connection):
    connection.execute(
        """
        CREATE TABLE version_dim (
            id INTEGER PRIMARY KEY,
            raw_version TEXT NOT NULL,
            version_tag TEXT,
            version_sort_key INTEGER
        )
        """
    )
    connection.executemany(
        "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?,?,?,?)",
        [
            (1, "Version 1.0", "1.0", 1000),
            (2, "Version 2.0", "2.0", 2000),
            (3, "Version 3.0", "3.0", 3000),
        ],
    )
    connection.execute(
        """
        CREATE TABLE text_source_entity (
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


def test_import_entity_sources_leaves_created_version_for_history_replay(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    monkeypatch.setattr(entitySourceImport, "conn", connection)
    monkeypatch.setattr(entitySourceImport, "ensure_version_schema", lambda: None)
    monkeypatch.setattr(entitySourceImport, "get_current_version", lambda: "Version 2.0")
    monkeypatch.setattr(entitySourceImport, "get_or_create_version_id", lambda version: 2)
    monkeypatch.setattr(entitySourceImport, "check_and_classify_interactive", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(entitySourceImport, "_print_entity_source_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        entitySourceImport,
        "_load_all_entity_data",
        lambda _root: {
            "materials": [
                {
                    "id": 1001,
                    "nameTextMapHash": 11,
                    "descTextMapHash": 21,
                    "effectDescTextMapHash": 22,
                    "materialType": "MATERIAL_QUEST",
                }
            ],
            "furnitures": [],
            "costumes": [],
            "suits": [],
            "emojis": [],
            "poses": [],
            "effects": [],
            "halls": [],
            "hall_facilities": [],
            "avatar_costumes": [],
            "weapons": [],
            "reliquaries": [],
            "codex": [],
            "achievements": [],
            "viewpoints": [],
            "dungeons": [],
            "loading_tips": [],
            "gcg_cards": [],
            "gcg_chars": [],
            "gcg_skills": [],
            "describe_title_map": {},
            "codex_desc_map": {},
        },
    )

    entitySourceImport.importEntitySources(commit=False, interactive=False)

    rows = connection.execute(
        """
        SELECT text_hash, source_type_code, entity_id, created_version_id
        FROM text_source_entity
        ORDER BY text_hash
        """
    ).fetchall()
    assert rows == [
        (21, entitySourceImport.SOURCE_TYPE_ITEM, 1001, None),
        (22, entitySourceImport.SOURCE_TYPE_ITEM, 1001, None),
    ]


def test_insert_entity_sources_delta_writes_current_created_version(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    monkeypatch.setattr(entitySourceImport, "conn", connection)
    monkeypatch.setattr(entitySourceImport, "ensure_version_schema", lambda: None)
    monkeypatch.setattr(entitySourceImport, "get_current_version", lambda: "Version 2.0")
    monkeypatch.setattr(entitySourceImport, "get_or_create_version_id", lambda version: 2)
    monkeypatch.setattr(entitySourceImport, "check_and_classify_interactive", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(entitySourceImport, "_print_entity_source_summary", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        entitySourceImport,
        "_load_all_entity_data",
        lambda _root: {
            "materials": [
                {
                    "id": 1001,
                    "nameTextMapHash": 11,
                    "descTextMapHash": 21,
                    "effectDescTextMapHash": 22,
                    "materialType": "MATERIAL_QUEST",
                }
            ],
            "furnitures": [],
            "costumes": [],
            "suits": [],
            "emojis": [],
            "poses": [],
            "effects": [],
            "halls": [],
            "hall_facilities": [],
            "avatar_costumes": [],
            "weapons": [],
            "reliquaries": [],
            "codex": [],
            "achievements": [],
            "viewpoints": [],
            "dungeons": [],
            "loading_tips": [],
            "gcg_cards": [],
            "gcg_chars": [],
            "gcg_skills": [],
            "describe_title_map": {},
            "codex_desc_map": {},
        },
    )

    entitySourceImport.insertEntitySourcesDelta(commit=False, interactive=False)

    rows = connection.execute(
        """
        SELECT text_hash, source_type_code, entity_id, created_version_id
        FROM text_source_entity
        ORDER BY text_hash
        """
    ).fetchall()
    assert rows == [
        (21, entitySourceImport.SOURCE_TYPE_ITEM, 1001, 2),
        (22, entitySourceImport.SOURCE_TYPE_ITEM, 1001, 2),
    ]


def test_catalog_entity_history_backfill_updates_only_new_snapshot_entities(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    connection.executemany(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (?,?,?,?,?,?,?)
        """,
        [
            (21, entitySourceImport.SOURCE_TYPE_ITEM, 1001, 11, 1, 0, None),
            (31, entitySourceImport.SOURCE_TYPE_ITEM, 2002, 12, 1, 0, None),
        ],
    )
    monkeypatch.setattr(history_backfill, "conn", connection)
    monkeypatch.setattr(entitySourceImport, "ensure_version_schema", lambda: None)
    monkeypatch.setattr(entitySourceImport, "_load_overrides", lambda: ({}, set()))

    def fake_git_show_json(_repo_path, commit_sha, rel_path):
        if rel_path != "ExcelBinOutput/MaterialExcelConfigData.json":
            return []
        base_rows = [
            {"id": 1001, "nameTextMapHash": 11, "descTextMapHash": 21, "materialType": "MATERIAL_QUEST"},
        ]
        if commit_sha == "child":
            return [
                *base_rows,
                {"id": 2002, "nameTextMapHash": 12, "descTextMapHash": 31, "materialType": "MATERIAL_QUEST"},
            ]
        return base_rows

    def fake_backfill_versions_from_history(**kwargs):
        kwargs["process_entry_fn"](
            connection.cursor(),
            "/repo",
            "child",
            "parent",
            {"action": "M", "new_path": "ExcelBinOutput/MaterialExcelConfigData.json"},
            2,
            "Version 2.0",
            100,
        )

    monkeypatch.setattr(history_backfill, "_git_show_json", fake_git_show_json)
    monkeypatch.setattr(history_backfill, "_backfill_versions_from_history", fake_backfill_versions_from_history)

    history_backfill.backfill_catalog_entity_versions_from_history(refresh_version_catalog=False)

    rows = connection.execute(
        """
        SELECT entity_id, created_version_id
        FROM text_source_entity
        ORDER BY entity_id
        """
    ).fetchall()
    assert rows == [(1001, None), (2002, 2)]


def test_catalog_entity_history_backfill_overrides_earlier_text_version(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    connection.executemany(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (?,?,?,?,?,?,?)
        """,
        [
            (31, entitySourceImport.SOURCE_TYPE_COSTUME, 2002, 12, 1, entitySourceImport.SUB_QIANXING_PARADOX, 1),
        ],
    )
    monkeypatch.setattr(history_backfill, "conn", connection)
    monkeypatch.setattr(entitySourceImport, "ensure_version_schema", lambda: None)
    monkeypatch.setattr(entitySourceImport, "_load_overrides", lambda: ({}, set()))

    def fake_git_show_json(_repo_path, commit_sha, rel_path):
        if rel_path != "ExcelBinOutput/BeyondCostumeExcelConfigData.json":
            return []
        if commit_sha == "child":
            return [
                {
                    "costumeId": 2002,
                    "nameTextMapHash": 12,
                    "descriptionTextMapHash": 31,
                    "GGBEAGONFJA": ["BODY_GIRL"],
                },
            ]
        return []

    def fake_backfill_versions_from_history(**kwargs):
        kwargs["process_entry_fn"](
            connection.cursor(),
            "/repo",
            "child",
            "parent",
            {"action": "M", "new_path": "ExcelBinOutput/BeyondCostumeExcelConfigData.json"},
            2,
            "Version 2.0",
            100,
        )

    monkeypatch.setattr(history_backfill, "_git_show_json", fake_git_show_json)
    monkeypatch.setattr(history_backfill, "_backfill_versions_from_history", fake_backfill_versions_from_history)

    history_backfill.backfill_catalog_entity_versions_from_history(refresh_version_catalog=False)

    rows = connection.execute(
        """
        SELECT entity_id, created_version_id
        FROM text_source_entity
        ORDER BY entity_id
        """
    ).fetchall()
    assert rows == [(2002, 2)]


def test_catalog_entity_history_backfill_replays_full_when_all_rows_are_target_version(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    connection.executemany(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (?,?,?,?,?,?,?)
        """,
        [
            (21, entitySourceImport.SOURCE_TYPE_ITEM, 1001, 11, 1, 0, 2),
            (31, entitySourceImport.SOURCE_TYPE_ITEM, 2002, 12, 1, 0, 2),
        ],
    )
    monkeypatch.setattr(history_backfill, "conn", connection)
    monkeypatch.setattr(history_backfill, "get_meta", lambda _key, default="": default)
    monkeypatch.setattr(entitySourceImport, "ensure_version_schema", lambda: None)
    monkeypatch.setattr(entitySourceImport, "_load_overrides", lambda: ({}, set()))

    def fake_git_show_json(_repo_path, commit_sha, rel_path):
        if rel_path != "ExcelBinOutput/MaterialExcelConfigData.json":
            return []
        base_rows = [
            {"id": 1001, "nameTextMapHash": 11, "descTextMapHash": 21, "materialType": "MATERIAL_QUEST"},
        ]
        if commit_sha == "child":
            return [
                *base_rows,
                {"id": 2002, "nameTextMapHash": 12, "descTextMapHash": 31, "materialType": "MATERIAL_QUEST"},
            ]
        if commit_sha == "base":
            return base_rows
        return []

    target_snapshot = history_backfill.VersionSnapshot(
        version_tag="2.0",
        version_label="Version 2.0",
        version_id=2,
        commit_sha="child",
        version_sort_key=2000,
    )
    from_snapshot = history_backfill.VersionSnapshot(
        version_tag="1.0",
        version_label="Version 1.0",
        version_id=1,
        commit_sha="base",
        version_sort_key=1000,
    )

    monkeypatch.setattr(
        history_backfill,
        "_resolve_snapshot_replay_range",
        lambda *_args, **_kwargs: history_backfill.SnapshotReplayRange(
            raw_target_commit="child",
            raw_from_commit="base",
            target_snapshot=target_snapshot,
            from_snapshot=from_snapshot,
            base_snapshot=None,
            snapshots=(target_snapshot,),
            resume_scope="base..child",
        ),
    )

    captured = {}

    def fake_backfill_versions_from_history(**kwargs):
        captured.update(kwargs)
        cursor = connection.cursor()
        try:
            kwargs["process_entry_fn"](
                cursor,
                "/repo",
                "base",
                None,
                {"action": "A", "new_path": "ExcelBinOutput/MaterialExcelConfigData.json"},
                1,
                "Version 1.0",
                100,
            )
            kwargs["process_entry_fn"](
                cursor,
                "/repo",
                "child",
                "base",
                {"action": "M", "new_path": "ExcelBinOutput/MaterialExcelConfigData.json"},
                2,
                "Version 2.0",
                100,
            )
            connection.commit()
        finally:
            cursor.close()

    monkeypatch.setattr(history_backfill, "_git_show_json", fake_git_show_json)
    monkeypatch.setattr(history_backfill, "_backfill_versions_from_history", fake_backfill_versions_from_history)

    history_backfill.backfill_catalog_entity_versions_from_history(
        target_commit="child",
        from_commit="base",
        refresh_version_catalog=False,
    )

    rows = connection.execute(
        """
        SELECT entity_id, created_version_id
        FROM text_source_entity
        ORDER BY entity_id
        """
    ).fetchall()
    assert captured["from_commit"] is None
    assert rows == [(1001, 1), (2002, 2)]


def _create_textmap_version_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE textMap (
            hash INTEGER,
            content TEXT,
            lang INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        )
        """
    )


def test_entity_text_version_correction_prefers_title_version():
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    _create_textmap_version_table(connection)
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
        [
            (11, "可靠标题", 1, 1, 1),
            (21, "可靠描述", 1, 2, 2),
        ],
    )
    connection.executemany(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (?,?,?,?,?,?,?)
        """,
        [
            (21, entitySourceImport.SOURCE_TYPE_ITEM, 1001, 11, 1, 0, 3),
            (22, entitySourceImport.SOURCE_TYPE_ITEM, 1001, 11, 1, 0, 3),
        ],
    )

    changed = history_backfill.backfill_catalog_entity_created_versions_from_textmap(connection.cursor())

    rows = connection.execute(
        """
        SELECT text_hash, created_version_id
        FROM text_source_entity
        ORDER BY text_hash
        """
    ).fetchall()
    assert changed == 1
    assert rows == [(21, 1), (22, 1)]


def test_entity_text_version_correction_uses_body_majority_and_earlier_tie():
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    _create_textmap_version_table(connection)
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
        [
            (101, "当前标题", 1, 3, 3),
            (111, "可靠描述甲版本较长", 1, 2, 2),
            (112, "可靠描述乙版本较长", 1, 2, 2),
            (113, "可靠描述丙版本较长", 1, 1, 1),
            (201, "另一个当前标题", 1, 3, 3),
            (211, "同票描述甲版本较长", 1, 2, 2),
            (212, "同票描述乙版本较长", 1, 1, 1),
        ],
    )
    connection.executemany(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (?,?,?,?,?,?,?)
        """,
        [
            (111, entitySourceImport.SOURCE_TYPE_ITEM, 2002, 101, 1, 0, 3),
            (112, entitySourceImport.SOURCE_TYPE_ITEM, 2002, 101, 1, 0, 3),
            (113, entitySourceImport.SOURCE_TYPE_ITEM, 2002, 101, 1, 0, 3),
            (211, entitySourceImport.SOURCE_TYPE_ITEM, 3003, 201, 1, 0, 3),
            (212, entitySourceImport.SOURCE_TYPE_ITEM, 3003, 201, 1, 0, 3),
        ],
    )

    changed = history_backfill.backfill_catalog_entity_created_versions_from_textmap(connection.cursor())

    rows = connection.execute(
        """
        SELECT entity_id, MIN(created_version_id), MAX(created_version_id)
        FROM text_source_entity
        GROUP BY entity_id
        ORDER BY entity_id
        """
    ).fetchall()
    assert changed == 2
    assert rows == [(2002, 2, 2), (3003, 1, 1)]


def test_entity_text_version_correction_ignores_short_and_test_body_texts():
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    _create_textmap_version_table(connection)
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
        [
            (101, "当前标题", 1, 3, 3),
            (111, "食物", 1, 1, 1),
            (201, "另一个当前标题", 1, 3, 3),
            (211, "(test)旧文本", 1, 1, 1),
        ],
    )
    connection.executemany(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (?,?,?,?,?,?,?)
        """,
        [
            (111, entitySourceImport.SOURCE_TYPE_ITEM, 4004, 101, 1, 0, 3),
            (211, entitySourceImport.SOURCE_TYPE_ITEM, 5005, 201, 1, 0, 3),
        ],
    )

    changed = history_backfill.backfill_catalog_entity_created_versions_from_textmap(connection.cursor())

    rows = connection.execute(
        """
        SELECT entity_id, created_version_id
        FROM text_source_entity
        ORDER BY entity_id
        """
    ).fetchall()
    assert changed == 0
    assert rows == [(4004, 3), (5005, 3)]


def test_catalog_queries_prefer_entity_created_version_and_fallback_to_textmap(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
    _create_textmap_version_table(connection)
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
        [
            (11, "测试道具", 1, 2, 2),
            (21, "测试描述", 1, 2, 3),
        ],
    )
    connection.execute(
        """
        INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category, created_version_id)
        VALUES (21, ?, 1001, 11, 1, 0, 1)
        """,
        (entitySourceImport.SOURCE_TYPE_ITEM,),
    )
    monkeypatch.setattr(databaseHelper, "conn", connection)
    databaseHelper._CACHE["column"].clear()
    databaseHelper._CACHE["table"].clear()

    rows = databaseHelper.selectCatalogEntities("", 1)
    assert rows[0][5:7] == ("Version 1.0", "Version 3.0")
    assert databaseHelper.countCatalogEntities("", 1, created_version="1.0") == 1
    assert databaseHelper.countCatalogEntities("", 1, created_version="2.0") == 0
    assert databaseHelper.getCatalogEntityVersionInfo(entitySourceImport.SOURCE_TYPE_ITEM, 1001, 1) == (
        "Version 1.0",
        "Version 3.0",
    )

    connection.execute("UPDATE text_source_entity SET created_version_id=NULL")
    rows = databaseHelper.selectCatalogEntities("", 1)
    assert rows[0][5:7] == ("Version 2.0", "Version 3.0")

"""Tests for entity source import helpers."""
import os
import sqlite3
import sys


TESTS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.normpath(os.path.join(TESTS_DIR, os.pardir))
DBBUILD_DIR = os.path.join(REPO_ROOT, "server", "dbBuild")
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import entitySourceImport
import history_backfill
import databaseHelper


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


def test_import_entity_sources_writes_current_created_version_and_syncs_entity_rows(monkeypatch):
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


def test_catalog_queries_prefer_entity_created_version_and_fallback_to_textmap(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_entity_version_tables(connection)
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

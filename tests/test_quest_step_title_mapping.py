import json
import os
import sqlite3
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import databaseHelper
import questImport
from genshin_data_core.access import FilesystemGameDataAccess
from genshin_data_core.quest import (
    build_step_title_hash_by_talk_id,
    extract_quest_id,
    extract_quest_talk_ids,
    get_step_talk_ids,
)
from genshin_data_core.sources import QuestSourceResolver, SOURCE_TYPE_HANGOUT


class _DummyProgress:
    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def update(self):
        return None


def _create_textmap_and_quest_talk_tables(connection: sqlite3.Connection):
    connection.execute(
        """
        CREATE TABLE questTalk (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            questId INTEGER,
            talkId INTEGER,
            stepTitleTextMapHash INTEGER,
            coopQuestId INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE textMap (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash INTEGER,
            content TEXT,
            lang INTEGER
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE langCode (
            id INTEGER PRIMARY KEY,
            codeName TEXT
        )
        """
    )
    connection.execute(
        "INSERT INTO langCode(id, codeName) VALUES (?, ?)",
        (1, "TextMapCHS.json"),
    )


def _create_backfill_tables(connection: sqlite3.Connection):
    connection.execute(
        """
        CREATE TABLE quest (
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            descTextMapHash INTEGER,
            longDescTextMapHash INTEGER,
            chapterId INTEGER,
            source_type TEXT,
            source_code_raw TEXT
        )
        """
    )
    _create_textmap_and_quest_talk_tables(connection)
    connection.execute(
        """
        CREATE TABLE talk_dialogue_link (
            talkId INTEGER NOT NULL,
            coopQuestId INTEGER NOT NULL DEFAULT 0,
            dialogueId INTEGER NOT NULL,
            PRIMARY KEY (talkId, coopQuestId, dialogueId)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE dialogue (
            dialogueId INTEGER PRIMARY KEY,
            talkId INTEGER,
            textHash INTEGER,
            coopQuestId INTEGER
        )
        """
    )


def test_get_step_talk_ids_supports_finish_plot_and_skips_lua_notify():
    step_obj = {
        "POPHAFEBKIH": [
            {"AAHAKNIPEDM": [1000701, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_COMPLETE_TALK"},
            {"AAHAKNIPEDM": [1000702, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_FINISH_PLOT"},
            {"AAHAKNIPEDM": [0, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_LUA_NOTIFY"},
        ]
    }

    assert get_step_talk_ids(step_obj) == [1000701, 1000702]


def test_shared_quest_parser_supports_6_6_quest_brief_schema():
    obj = {
        "GMOMCKNPBGE": 70065,
        "ALLMCLJBBDM": 2595721583,
        "JILHIMLENJK": 0,
        "EOHJIHHMBAN": [7006501, 7006502],
    }

    assert extract_quest_id(obj) == 70065
    assert extract_quest_talk_ids(obj) == [7006501, 7006502]


def test_shared_quest_parser_supports_6_7_quest_brief_schema():
    obj = {
        "ANKFNLMKOII": 76109,
        "OCCBMCOGDOO": 4183792175,
        "JBDLGLCIOHM": 0,
        "HONEAMECBEN": 1611,
        "GDDPNNHLGBL": [
            {"ANKFNLMKOII": 7610901},
            {"ANKFNLMKOII": 7610902},
        ],
    }

    assert extract_quest_id(obj) == 76109
    assert extract_quest_talk_ids(obj) == [7610901, 7610902]


def test_legacy_190xx_quest_ids_are_hangouts(monkeypatch):
    resolver = QuestSourceResolver(FilesystemGameDataAccess([]))
    monkeypatch.setattr(resolver, "load_quest_source_raw_by_id", lambda: {19001: "LQ"})
    monkeypatch.setattr(resolver, "load_main_coop_ids_by_quest_id", lambda: {})

    assert resolver.resolve_quest_source_fields(19001) == (SOURCE_TYPE_HANGOUT, "LQ")
    assert resolver.resolve_quest_source_fields(19187) == (SOURCE_TYPE_HANGOUT, "UNKNOWN")


def test_build_step_title_hash_by_talk_id_supports_6_6_quest_brief_schema():
    obj = {
        "IKECHKLEFFK": [
            {
                "CBOGAFHNHNI": 70065,
                "LAFBPKMMBHD": 7006501,
                "JDFENJAFCPF": 1374941308,
                "PGELADPAKLA": [
                    {
                        "KFDJJBPNIHG": [7006519, 0],
                        "MEGMIMEDODJ": "QUEST_CONTENT_COMPLETE_TALK",
                    }
                ],
            },
            {
                "CBOGAFHNHNI": 70065,
                "LAFBPKMMBHD": 7006502,
                "JDFENJAFCPF": 2033502460,
                "PGELADPAKLA": [
                    {
                        "KFDJJBPNIHG": [7006520, 0],
                        "MEGMIMEDODJ": "QUEST_CONTENT_FINISH_PLOT",
                    }
                ],
            },
        ]
    }

    mapping = build_step_title_hash_by_talk_id(obj)

    assert mapping[7006519] == 1374941308
    assert mapping[7006520] == 2033502460


def test_build_step_title_hash_by_talk_id_supports_6_7_quest_brief_schema():
    obj = {
        "HLCINEMBGEF": [
            {
                "PHPKOAIPNFO": 76109,
                "NDOFAOCKPGE": 7610901,
                "BMEACBBPBGK": 1090861876,
                "FCBEKGAHMPD": [
                    {
                        "PALPAGCBFDI": [7610901, 0],
                        "BPEHONLLNNK": "QUEST_CONTENT_COMPLETE_TALK",
                    }
                ],
            },
            {
                "PHPKOAIPNFO": 76109,
                "NDOFAOCKPGE": 7610902,
                "BMEACBBPBGK": 1770283964,
                "FCBEKGAHMPD": [
                    {
                        "PALPAGCBFDI": [7610903, 0],
                        "BPEHONLLNNK": "QUEST_CONTENT_FINISH_PLOT",
                    }
                ],
            },
        ],
        "GDDPNNHLGBL": [
            {
                "ANKFNLMKOII": 7610904,
                "BLCEJLFCFPH": [],
                "MPFAEHLBPJE": [
                    {
                        "BPEHONLLNNK": "QUEST_COND_STATE_EQUAL",
                        "PALPAGCBFDI": ["7610902", "2"],
                    }
                ],
            }
        ],
    }

    mapping = build_step_title_hash_by_talk_id(obj)

    assert get_step_talk_ids(obj["HLCINEMBGEF"][0]) == [7610901]
    assert get_step_talk_ids(obj["HLCINEMBGEF"][1]) == [7610903]
    assert mapping[7610901] == 1090861876
    assert mapping[7610903] == 1770283964
    assert mapping[7610904] == 1770283964


def test_build_step_title_hash_by_talk_id_keeps_first_match_for_conflicts():
    obj = {
        "MEGJPCLADOG": [
            {
                "JPBOKMKMHCJ": 10007,
                "AJGGCMPLKHK": 1108051172,
                "POPHAFEBKIH": [
                    {"AAHAKNIPEDM": [1000712, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_COMPLETE_TALK"}
                ],
            },
            {
                "JPBOKMKMHCJ": 10007,
                "AJGGCMPLKHK": 2447604868,
                "POPHAFEBKIH": [
                    {"AAHAKNIPEDM": [1000712, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_FINISH_PLOT"}
                ],
            },
            {
                "JPBOKMKMHCJ": 10007,
                "AJGGCMPLKHK": 1897459020,
                "POPHAFEBKIH": [
                    {"AAHAKNIPEDM": [1000702, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_FINISH_PLOT"}
                ],
            },
        ]
    }

    mapping = build_step_title_hash_by_talk_id(obj)

    assert mapping[1000712] == 1108051172
    assert mapping[1000702] == 1897459020


def test_build_step_title_hash_by_talk_id_fills_top_level_talk_titles():
    obj = {
        "NFFIGDHFAJG": [
            {"NFIEHACCECI": 7310101},
            {
                "NFIEHACCECI": 7310102,
                "MPFAEHLBPJE": [
                    {"_type": "QUEST_COND_STATE_EQUAL", "_param": ["7310101", "2"]},
                ],
            },
            {
                "NFIEHACCECI": 7310104,
                "MPFAEHLBPJE": [
                    {"_type": "QUEST_COND_STATE_EQUAL", "_param": ["7310103", "2"]},
                ],
            },
        ],
        "MEGJPCLADOG": [
            {
                "JPBOKMKMHCJ": 73101,
                "KKMJBEPGLGD": 7310101,
                "AJGGCMPLKHK": 419665676,
                "POPHAFEBKIH": [],
            },
            {
                "JPBOKMKMHCJ": 73101,
                "KKMJBEPGLGD": 7310103,
                "AJGGCMPLKHK": 1953878954,
                "POPHAFEBKIH": [],
            },
        ],
    }

    mapping = build_step_title_hash_by_talk_id(obj)

    assert mapping[7310101] == 419665676
    assert mapping[7310102] == 419665676
    assert mapping[7310104] == 1953878954


def test_build_step_title_hash_by_talk_id_direct_step_mapping_wins_over_top_level_fallback():
    obj = {
        "NFFIGDHFAJG": [
            {
                "NFIEHACCECI": 7310102,
                "MPFAEHLBPJE": [
                    {"_type": "QUEST_COND_STATE_EQUAL", "_param": ["7310101", "2"]},
                ],
            },
        ],
        "MEGJPCLADOG": [
            {
                "JPBOKMKMHCJ": 73101,
                "KKMJBEPGLGD": 7310101,
                "AJGGCMPLKHK": 419665676,
                "POPHAFEBKIH": [
                    {"AAHAKNIPEDM": [7310102, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_COMPLETE_TALK"}
                ],
            },
            {
                "JPBOKMKMHCJ": 73101,
                "KKMJBEPGLGD": 7310102,
                "AJGGCMPLKHK": 1953878954,
                "POPHAFEBKIH": [],
            },
        ],
    }

    mapping = build_step_title_hash_by_talk_id(obj)

    assert mapping[7310102] == 419665676


def test_get_quest_step_title_map_merges_db_rows_with_quest_bin_fallback(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_textmap_and_quest_talk_tables(connection)
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?,?,?)",
        [
            (1108051172, "与温迪合奏", 1),
            (1897459020, "与温迪一同循风前进", 1),
            (2447604868, "击败出现的魔物", 1),
            (419665676, "（test）与兰那罗对话$HIDDEN", 1),
        ],
    )
    connection.executemany(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        [
            (10007, 1000702, None, 0),
            (10007, 1000703, None, 0),
            (10007, 1000712, 1108051172, 0),
        ],
    )

    monkeypatch.setattr(databaseHelper, "conn", connection)
    monkeypatch.setattr(
        databaseHelper,
        "_QUEST_STEP_TALK_MAP_CACHE",
        {},
    )
    monkeypatch.setitem(databaseHelper._CACHE, "column", {})
    monkeypatch.setattr(databaseHelper, "_QUEST_STEP_ROWS_BY_MAIN_ID", {})
    monkeypatch.setattr(
        databaseHelper,
        "_get_quest_bin_output",
        lambda quest_id: {
            "MEGJPCLADOG": [
                {
                    "KKMJBEPGLGD": 1000702,
                    "AJGGCMPLKHK": 1897459020,
                    "DGINIFCGMGL": 1,
                    "POPHAFEBKIH": [
                        {"AAHAKNIPEDM": [1000702, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_FINISH_PLOT"}
                    ],
                },
                {
                    "KKMJBEPGLGD": 1000712,
                    "AJGGCMPLKHK": 2447604868,
                    "DGINIFCGMGL": 11,
                    "POPHAFEBKIH": [
                        {"AAHAKNIPEDM": [1000712, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_FINISH_PLOT"}
                    ],
                },
                {
                    "KKMJBEPGLGD": 1000703,
                    "AJGGCMPLKHK": 419665676,
                    "DGINIFCGMGL": 12,
                    "POPHAFEBKIH": [],
                },
            ],
            "NFFIGDHFAJG": [
                {"NFIEHACCECI": 1000703},
            ]
        },
    )
    monkeypatch.setattr(databaseHelper, "_normalize_output_text", lambda text, lang_code: text)

    result = databaseHelper.getQuestStepTitleMap(10007, 1)

    assert result[1000702] == "与温迪一同循风前进"
    assert result[1000703] == "（test）与兰那罗对话$HIDDEN"
    assert result[1000712] == "与温迪合奏"


def test_backfill_quest_metadata_populates_finish_plot_step_title(monkeypatch, tmp_path):
    connection = sqlite3.connect(":memory:")
    _create_backfill_tables(connection)
    connection.execute(
        "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId, source_type, source_code_raw) VALUES (?,?,?,?,?,?,?)",
        (10007, 123, None, None, 1, "WQ", "WQ"),
    )
    connection.execute(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        (10007, 1000702, None, 0),
    )

    quest_obj = {
        "NFIEHACCECI": 10007,
        "BPNEONFJEEO": 123,
        "BALAIBAGIEL": 1,
        "NFFIGDHFAJG": [
            {"NFIEHACCECI": 1000702},
        ],
        "MEGJPCLADOG": [
            {
                "JPBOKMKMHCJ": 10007,
                "AJGGCMPLKHK": 1897459020,
                "POPHAFEBKIH": [
                    {"AAHAKNIPEDM": [1000702, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_FINISH_PLOT"}
                ],
            }
        ],
    }

    quest_dir = tmp_path / "BinOutput" / "Quest"
    quest_dir.mkdir(parents=True)
    (quest_dir / "10007.json").write_text(json.dumps(quest_obj), encoding="utf-8")

    brief_dir = tmp_path / "BinOutput" / "QuestBrief"
    brief_dir.mkdir(parents=True)
    (brief_dir / "10007.json").write_text(json.dumps(quest_obj), encoding="utf-8")

    monkeypatch.setattr(questImport, "conn", connection)
    monkeypatch.setattr(questImport, "DATA_PATH", str(tmp_path))
    monkeypatch.setattr(questImport, "LightweightProgress", _DummyProgress)
    monkeypatch.setattr(questImport, "_ensure_quest_version_tables", lambda cursor: None)
    monkeypatch.setattr(questImport, "_get_quest_desc_text_map_hash", lambda quest_id: None)
    monkeypatch.setattr(questImport, "_print_skip_summary", lambda *args, **kwargs: None)

    questImport.backfillQuestMetadata(commit=False)

    row = connection.execute(
        "SELECT stepTitleTextMapHash FROM questTalk WHERE questId=? AND talkId=? AND coalesce(coopQuestId, 0)=0",
        (10007, 1000702),
    ).fetchone()
    assert row == (1897459020,)


def test_backfill_quest_metadata_uses_main_quest_chapter_id_for_6_6_schema(monkeypatch, tmp_path):
    connection = sqlite3.connect(":memory:")
    _create_backfill_tables(connection)
    connection.execute(
        "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId, source_type, source_code_raw) VALUES (?,?,?,?,?,?,?)",
        (6034, 4148058431, None, None, 6034, "AQ", "AQ"),
    )

    quest_obj = {
        "GMOMCKNPBGE": 6034,
        "ALLMCLJBBDM": 4148058431,
        "JILHIMLENJK": 6034,
        "EOHJIHHMBAN": [106034],
    }
    main_quest_rows = [
        {
            "id": 6034,
            "chapterId": 1611,
            "descTextMapHash": 3061644630,
        }
    ]

    quest_dir = tmp_path / "BinOutput" / "Quest"
    quest_dir.mkdir(parents=True)
    (quest_dir / "6034.json").write_text(json.dumps(quest_obj), encoding="utf-8")

    excel_dir = tmp_path / "ExcelBinOutput"
    excel_dir.mkdir(parents=True)
    (excel_dir / "MainQuestExcelConfigData.json").write_text(json.dumps(main_quest_rows), encoding="utf-8")

    monkeypatch.setattr(questImport, "conn", connection)
    monkeypatch.setattr(questImport, "DATA_PATH", str(tmp_path))
    monkeypatch.setattr(questImport, "LightweightProgress", _DummyProgress)
    monkeypatch.setattr(questImport, "_ensure_quest_version_tables", lambda cursor: None)
    monkeypatch.setattr(questImport, "_print_skip_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(questImport, "_MAIN_QUEST_DESC_HASH_BY_ID", None)
    monkeypatch.setattr(questImport, "_MAIN_QUEST_CHAPTER_ID_BY_ID", None)

    questImport.backfillQuestMetadata(commit=False)

    row = connection.execute(
        "SELECT descTextMapHash, chapterId FROM quest WHERE questId=?",
        (6034,),
    ).fetchone()
    assert row == (3061644630, 1611)


def test_backfill_quest_metadata_preserves_top_level_talk_title_after_quest_brief(monkeypatch, tmp_path):
    connection = sqlite3.connect(":memory:")
    _create_backfill_tables(connection)
    connection.execute(
        "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId, source_type, source_code_raw) VALUES (?,?,?,?,?,?,?)",
        (73101, 123, None, None, 1, "WQ", "WQ"),
    )
    connection.execute(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        (73101, 7310102, None, 0),
    )

    quest_obj = {
        "NFIEHACCECI": 73101,
        "BPNEONFJEEO": 123,
        "BALAIBAGIEL": 1,
        "NFFIGDHFAJG": [
            {
                "NFIEHACCECI": 7310102,
                "MPFAEHLBPJE": [
                    {"_type": "QUEST_COND_STATE_EQUAL", "_param": ["7310101", "2"]},
                ],
            },
        ],
        "MEGJPCLADOG": [
            {
                "JPBOKMKMHCJ": 73101,
                "KKMJBEPGLGD": 7310101,
                "AJGGCMPLKHK": 419665676,
                "POPHAFEBKIH": [],
            }
        ],
    }

    quest_dir = tmp_path / "BinOutput" / "Quest"
    quest_dir.mkdir(parents=True)
    (quest_dir / "73101.json").write_text(json.dumps(quest_obj), encoding="utf-8")

    brief_dir = tmp_path / "BinOutput" / "QuestBrief"
    brief_dir.mkdir(parents=True)
    (brief_dir / "73101.json").write_text(json.dumps(quest_obj), encoding="utf-8")

    monkeypatch.setattr(questImport, "conn", connection)
    monkeypatch.setattr(questImport, "DATA_PATH", str(tmp_path))
    monkeypatch.setattr(questImport, "LightweightProgress", _DummyProgress)
    monkeypatch.setattr(questImport, "_ensure_quest_version_tables", lambda cursor: None)
    monkeypatch.setattr(questImport, "_get_quest_desc_text_map_hash", lambda quest_id: None)
    monkeypatch.setattr(questImport, "_print_skip_summary", lambda *args, **kwargs: None)

    questImport.backfillQuestMetadata(commit=False)

    row = connection.execute(
        "SELECT stepTitleTextMapHash FROM questTalk WHERE questId=? AND talkId=? AND coalesce(coopQuestId, 0)=0",
        (73101, 7310102),
    ).fetchone()
    assert row == (419665676,)

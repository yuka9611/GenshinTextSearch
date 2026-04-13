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
from quest_source_utils import build_step_title_hash_by_talk_id, get_step_talk_ids


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


def test_get_step_talk_ids_supports_finish_plot_and_skips_lua_notify():
    step_obj = {
        "POPHAFEBKIH": [
            {"AAHAKNIPEDM": [1000701, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_COMPLETE_TALK"},
            {"AAHAKNIPEDM": [1000702, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_FINISH_PLOT"},
            {"AAHAKNIPEDM": [0, 0], "HAHEIAHBPEJ": "QUEST_CONTENT_LUA_NOTIFY"},
        ]
    }

    assert get_step_talk_ids(step_obj) == [1000701, 1000702]


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


def test_get_quest_step_title_map_merges_db_rows_with_quest_bin_fallback(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_textmap_and_quest_talk_tables(connection)
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?,?,?)",
        [
            (1108051172, "与温迪合奏", 1),
            (1897459020, "与温迪一同循风前进", 1),
            (2447604868, "击败出现的魔物", 1),
        ],
    )
    connection.executemany(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        [
            (10007, 1000702, None, 0),
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
            ]
        },
    )
    monkeypatch.setattr(databaseHelper, "_normalize_output_text", lambda text, lang_code: text)

    result = databaseHelper.getQuestStepTitleMap(10007, 1)

    assert result[1000702] == "与温迪一同循风前进"
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

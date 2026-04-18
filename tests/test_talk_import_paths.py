import json
import os
import sqlite3
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import diffUpdate
import databaseHelper
import questImport


class _DummyCursor:
    def execute(self, *_args, **_kwargs):
        return self

    def close(self):
        return None


class _DummyConn:
    def cursor(self):
        return _DummyCursor()

    def commit(self):
        return None

    def rollback(self):
        return None


class _DummyProgress:
    def __init__(self, *_args, **_kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def update(self):
        return None


def _create_dialogue_tables(connection: sqlite3.Connection):
    connection.execute(
        """
        CREATE TABLE dialogue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talkerType TEXT,
            talkerId INTEGER,
            talkId INTEGER,
            textHash INTEGER,
            dialogueId INTEGER UNIQUE,
            coopQuestId INTEGER
        )
        """
    )
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


def _create_quest_tables(connection: sqlite3.Connection):
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


def test_resolve_talk_file_path_supports_windows_and_posix_rel_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(questImport, "DATA_PATH", str(tmp_path))
    expected = os.path.join(str(tmp_path), "BinOutput", "Talk", "Quest", "foo.json")

    assert os.path.normpath(questImport._resolve_talk_file_path("Quest/foo.json")) == os.path.normpath(expected)
    assert os.path.normpath(questImport._resolve_talk_file_path(r"Quest\foo.json")) == os.path.normpath(expected)


def test_import_all_talk_items_uses_posix_logical_paths(monkeypatch, tmp_path):
    talk_dir = tmp_path / "BinOutput" / "Talk" / "Quest"
    talk_dir.mkdir(parents=True)
    (talk_dir / "foo.json").write_text("{}", encoding="utf-8")

    seen: list[str] = []
    monkeypatch.setattr(questImport, "DATA_PATH", str(tmp_path))
    monkeypatch.setattr(questImport, "conn", _DummyConn())
    monkeypatch.setattr(questImport, "LightweightProgress", _DummyProgress)
    monkeypatch.setattr(questImport, "_refresh_quest_hash_map_for_talk_ids", lambda *args, **kwargs: None)
    monkeypatch.setattr(questImport, "_print_skip_summary", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        questImport,
        "importTalk",
        lambda fileName, **kwargs: seen.append(fileName) or 0,
    )

    questImport.importAllTalkItems(commit=False)

    assert seen == ["Quest/foo.json"]


def test_diff_update_analyze_diff_tracks_talk_paths_with_forward_slashes():
    plan = diffUpdate._analyze_diff(
        [
            {
                "action": "M",
                "old_path": r"BinOutput\Talk\Quest\foo.json",
                "new_path": r"BinOutput\Talk\Quest\foo.json",
            }
        ]
    )

    assert plan["talk_changed"] == {"Quest/foo.json"}


def test_diff_update_analyze_diff_marks_new_qianxing_entity_excels():
    plan = diffUpdate._analyze_diff(
        [
            {"action": "M", "old_path": None, "new_path": "ExcelBinOutput/BeyondEmojiExcelConfigData.json"},
            {"action": "M", "old_path": None, "new_path": "ExcelBinOutput/BeyondPoseExcelConfigData.json"},
            {"action": "M", "old_path": None, "new_path": "ExcelBinOutput/BeyondTransferEffectExcelConfigData.json"},
            {"action": "M", "old_path": None, "new_path": "ExcelBinOutput/BeyondHallExcelConfigData.json"},
            {"action": "M", "old_path": None, "new_path": "ExcelBinOutput/BeyondHallFacilityExcelConfigData.json"},
        ]
    )

    assert plan["entity_sources"] is True


def test_diff_update_analyze_diff_marks_readable_meta_refresh_triggers():
    plan = diffUpdate._analyze_diff(
        [
            {"action": "M", "old_path": None, "new_path": "Readable/CHS/Book1039.txt"},
            {"action": "M", "old_path": None, "new_path": "ExcelBinOutput/MaterialExcelConfigData.json"},
            {"action": "M", "old_path": None, "new_path": "ExcelBinOutput/BooksCodexExcelConfigData.json"},
            {"action": "M", "old_path": None, "new_path": "TextMap/TextMapCHS.json"},
        ]
    )

    assert plan["readable_meta"] is True


def test_diff_update_resolve_talk_keys_supports_aadkdkpmgno_schema():
    obj = {
        "AADKDKPMGNO": 7008901,
        "GALIDJOEHOC": [
            {
                "NFIEHACCECI": 700890101,
                "AIGJBMCHCJG": 3728079010,
            }
        ],
    }

    assert diffUpdate._resolve_talk_keys(obj) == "AADKDKPMGNO"


def test_replace_talk_file_from_local_normalizes_rel_path(monkeypatch, tmp_path):
    talk_dir = tmp_path / "BinOutput" / "Talk" / "Quest"
    talk_dir.mkdir(parents=True)
    talk_file = talk_dir / "foo.json"
    talk_file.write_text(
        json.dumps(
            {
                "AADKDKPMGNO": 7008901,
                "GALIDJOEHOC": [
                    {
                        "NFIEHACCECI": 700890101,
                        "AIGJBMCHCJG": 3728079010,
                        "PIBKEGJOJHN": {
                            "_id": "1005",
                            "_type": "TALK_ROLE_NPC",
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    deleted_scopes: list[tuple[int, int | None]] = []
    imported_rels: list[str] = []

    monkeypatch.setattr(diffUpdate, "DATA_PATH", str(tmp_path))
    monkeypatch.setattr(
        diffUpdate,
        "_delete_talk_scope",
        lambda talk_id, coop_quest_id: deleted_scopes.append((talk_id, coop_quest_id)),
    )
    monkeypatch.setattr(
        diffUpdate.DBBuild,
        "importTalk",
        lambda talk_file_rel, **kwargs: imported_rels.append(talk_file_rel),
    )

    changed_talk_id, skipped = diffUpdate._replace_talk_file_from_local(r"Quest\foo.json")

    assert changed_talk_id == 7008901
    assert skipped is False
    assert deleted_scopes == [(7008901, None)]
    assert imported_rels == ["Quest/foo.json"]


def test_import_talk_populates_and_clears_talk_dialogue_links(monkeypatch, tmp_path):
    connection = sqlite3.connect(":memory:")
    _create_dialogue_tables(connection)

    talk_dir = tmp_path / "BinOutput" / "Talk" / "Quest"
    talk_dir.mkdir(parents=True)
    talk_file = talk_dir / "foo.json"
    talk_file.write_text(
        json.dumps(
            {
                "AADKDKPMGNO": 7008901,
                "GALIDJOEHOC": [
                    {
                        "NFIEHACCECI": 700890101,
                        "AIGJBMCHCJG": 3728079010,
                        "PIBKEGJOJHN": {"_id": "1005", "_type": "TALK_ROLE_NPC"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(questImport, "DATA_PATH", str(tmp_path))
    monkeypatch.setattr(questImport, "conn", connection)
    monkeypatch.setattr(questImport, "_refresh_quest_hash_map_for_talk_ids", lambda *args, **kwargs: None)
    questImport._set_talk_dialogue_link_presence(None)

    assert questImport.importTalk("Quest/foo.json", refresh_hash_map=False) == 1
    assert connection.execute(
        "SELECT talkId, coopQuestId, dialogueId FROM talk_dialogue_link"
    ).fetchall() == [(7008901, 0, 700890101)]
    assert connection.execute(
        "SELECT dialogueId, talkId, textHash FROM dialogue"
    ).fetchall() == [(700890101, 7008901, 3728079010)]

    talk_file.write_text(
        json.dumps({"AADKDKPMGNO": 7008901, "GALIDJOEHOC": []}),
        encoding="utf-8",
    )
    assert questImport.importTalk("Quest/foo.json", refresh_hash_map=False) == 0
    assert connection.execute("SELECT * FROM talk_dialogue_link").fetchall() == []
    assert connection.execute("SELECT * FROM dialogue").fetchall() == []


def test_filter_quest_talk_rows_uses_link_table_for_collision_scope_fix_and_drop(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_dialogue_tables(connection)
    cursor = connection.cursor()
    questImport._ensure_talk_dialogue_link_schema(cursor)
    cursor.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
        [
            (100002, 0, 10000201),
            (10000201, 0, 10000201),
            (232, 1905003, 232001),
            (151, 1904705, 151001),
            (151, 1905003, 151002),
        ],
    )
    connection.commit()

    questImport._set_talk_dialogue_link_presence(None)
    filtered = questImport._filter_quest_talk_rows_by_available_dialogues(
        cursor,
        [
            (100002, None, 0),
            (10000201, None, 0),
            (232, None, 1900103),
            (151, None, 1900103),
            (31013, None, 0),
        ],
    )

    assert filtered == [
        (100002, None, 0),
        (10000201, None, 0),
        (232, None, 1905003),
    ]


def test_database_helper_quest_dialogue_queries_use_talk_dialogue_link(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_dialogue_tables(connection)
    _create_quest_tables(connection)
    connection.execute(
        "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId, source_type, source_code_raw) "
        "VALUES (1000, NULL, NULL, NULL, NULL, 'AQ', 'AQ')"
    )
    connection.executemany(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        [
            (1000, 100002, None, 0),
            (1000, 10000201, None, 0),
        ],
    )
    connection.executemany(
        "INSERT INTO dialogue(dialogueId, talkerId, talkerType, talkId, textHash, coopQuestId) VALUES (?,?,?,?,?,?)",
        [
            (10000201, 203601, "TALK_ROLE_NPC", 10000201, 1937334897, None),
        ],
    )
    connection.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
        [
            (100002, 0, 10000201),
            (10000201, 0, 10000201),
        ],
    )
    connection.commit()

    monkeypatch.setattr(databaseHelper, "conn", connection)
    databaseHelper._CACHE["table"].clear()
    databaseHelper._CACHE["column"].clear()

    assert databaseHelper.countQuestDialogues(1000) == 2
    assert databaseHelper.selectQuestDialoguesPaged(1000) == [
        (1937334897, "TALK_ROLE_NPC", 203601, 10000201, 100002),
        (1937334897, "TALK_ROLE_NPC", 203601, 10000201, 10000201),
    ]

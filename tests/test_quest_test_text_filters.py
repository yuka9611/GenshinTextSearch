import hashlib
import os
import sqlite3
import sys


SERVER_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server")
)
DBBUILD_DIR = os.path.join(SERVER_DIR, "dbBuild")
for path in (SERVER_DIR, DBBUILD_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

import databaseHelper
import questImport
import quest_hash_map_utils
import version_control


def _bootstrap_quest_db(connection: sqlite3.Connection):
    connection.executescript(
        """
        CREATE TABLE langCode (
            id INTEGER PRIMARY KEY,
            codeName TEXT,
            imported INTEGER
        );
        CREATE TABLE version_dim (
            id INTEGER PRIMARY KEY,
            raw_version TEXT,
            version_tag TEXT,
            version_sort_key INTEGER
        );
        CREATE TABLE textMap (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lang INTEGER NOT NULL,
            hash INTEGER NOT NULL,
            content TEXT,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE TABLE quest (
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            descTextMapHash INTEGER,
            longDescTextMapHash INTEGER,
            chapterId INTEGER,
            source_type TEXT,
            source_code_raw TEXT,
            created_version_id INTEGER,
            git_created_version_id INTEGER
        );
        CREATE TABLE questTalk (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            questId INTEGER,
            talkId INTEGER,
            stepTitleTextMapHash INTEGER,
            coopQuestId INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dialogue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talkerType TEXT,
            talkerId INTEGER,
            talkId INTEGER,
            textHash INTEGER,
            dialogueId INTEGER UNIQUE,
            coopQuestId INTEGER
        );
        CREATE TABLE talk_dialogue_link (
            talkId INTEGER NOT NULL,
            coopQuestId INTEGER NOT NULL DEFAULT 0,
            dialogueId INTEGER NOT NULL,
            PRIMARY KEY (talkId, coopQuestId, dialogueId)
        );
        CREATE TABLE quest_version (
            questId INTEGER NOT NULL,
            lang INTEGER NOT NULL,
            updated_version_id INTEGER,
            PRIMARY KEY (questId, lang)
        );
        CREATE TABLE quest_text_signature (
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            dialogue_signature TEXT NOT NULL
        );
        CREATE TABLE chapter (
            chapterId INTEGER PRIMARY KEY,
            chapterTitleTextMapHash INTEGER,
            chapterNumTextMapHash INTEGER
        );
        """
    )
    connection.executemany(
        "INSERT INTO langCode(id, codeName, imported) VALUES (?,?,?)",
        [
            (1, "TextMapCHS.json", 1),
            (2, "TextMapEN.json", 1),
        ],
    )
    connection.executemany(
        "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?,?,?,?)",
        [
            (1, "Version 1.0", "1.0", 10),
            (2, "Version 2.0", "2.0", 20),
            (3, "Version 3.0", "3.0", 30),
            (4, "Version 4.0", "4.0", 40),
            (9, "Version 9.0", "9.0", 90),
        ],
    )
    connection.executemany(
        "INSERT INTO chapter(chapterId, chapterTitleTextMapHash, chapterNumTextMapHash) VALUES (?,?,?)",
        [(10, 901, 902)],
    )
    connection.executemany(
        """
        INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id)
        VALUES (?,?,?,?,?)
        """,
        [
            (1, 101, "(test)Quest Title", 9, 9),
            (1, 102, "Normal Quest Title", 3, 4),
            (1, 103, "（test）Quest Description", 9, 9),
            (1, 104, "（test）Quest Long Description", 9, 9),
            (1, 105, "（test）Quest Dialogue", 9, 9),
            (1, 106, "Normal Quest Dialogue", 3, 4),
            (1, 107, "（test）Step Title", 9, 9),
            (1, 901, "Chapter Name", 1, 1),
            (1, 902, "Act I", 1, 1),
            (2, 101, "(test)Quest Title", 9, 9),
            (2, 105, "(test)Quest Dialogue", 9, 9),
            (2, 106, "Normal Quest Dialogue", 3, 4),
        ],
    )
    connection.executemany(
        """
        INSERT INTO quest(
            questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId,
            source_type, source_code_raw, created_version_id, git_created_version_id
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        [
            (1, 101, 103, 104, 10, None, None, 9, 2),
            (2, 102, None, None, 10, None, None, 3, 1),
            (3, 101, 103, 104, 10, None, None, 9, 2),
        ],
    )
    connection.executemany(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        [
            (1, 1001, 107, 0),
            (2, 2001, None, 0),
            (3, 3001, 107, 0),
        ],
    )
    connection.executemany(
        """
        INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId)
        VALUES (?,?,?,?,?,?)
        """,
        [
            (5001, "TALK_ROLE_NPC", 1, 1001, 105, None),
            (5002, "TALK_ROLE_NPC", 1, 1001, 106, None),
            (6001, "TALK_ROLE_NPC", 1, 2001, 106, None),
            (7001, "TALK_ROLE_NPC", 1, 3001, 105, None),
        ],
    )
    connection.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
        [
            (1001, 0, 5001),
            (1001, 0, 5002),
            (2001, 0, 6001),
            (3001, 0, 7001),
        ],
    )
    connection.executemany(
        "INSERT INTO quest_version(questId, lang, updated_version_id) VALUES (?,?,?)",
        [
            (1, 1, 9),
            (1, 2, 9),
            (3, 1, 9),
            (3, 2, 9),
        ],
    )
    connection.commit()


def _patch_modules(monkeypatch, connection: sqlite3.Connection):
    monkeypatch.setattr(databaseHelper, "conn", connection)
    monkeypatch.setattr(questImport, "conn", connection)
    monkeypatch.setattr(version_control, "conn", connection)
    monkeypatch.setattr(databaseHelper.config, "getSourceLanguage", lambda: 1)
    monkeypatch.setattr(databaseHelper, "_normalize_output_text", lambda text, _lang: text)
    databaseHelper._CACHE["table"].clear()
    databaseHelper._CACHE["column"].clear()
    databaseHelper._CACHE["version"].clear()
    version_control._clear_version_sort_key_cache()
    questImport._set_talk_dialogue_link_presence(None)


def test_is_excluded_quest_text_matches_only_prefixes():
    from quest_text_filters import is_excluded_quest_text

    assert is_excluded_quest_text("（test）Quest")
    assert is_excluded_quest_text("（Test）Quest")
    assert is_excluded_quest_text("(test)Quest")
    assert is_excluded_quest_text("(Test)Quest")
    assert not is_excluded_quest_text("Quest（test）suffix")


def test_refresh_quest_hash_map_skips_excluded_hashes(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)

        touched = quest_hash_map_utils.refresh_quest_hash_map_for_quest_ids(connection.cursor(), [1, 2])

        rows = connection.execute(
            "SELECT questId, hash, source_type FROM quest_hash_map ORDER BY questId, hash, source_type"
        ).fetchall()

        assert touched == 2
        assert rows == [
            (1, 106, "dialogue"),
            (2, 102, "title"),
            (2, 106, "dialogue"),
        ]
    finally:
        connection.close()


def test_build_quest_dialogue_signature_ignores_excluded_dialogues(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)

        signature = questImport._build_quest_dialogue_signature(
            connection.cursor(),
            [(1001, 107, 0)],
        )

        assert signature == hashlib.sha1("106:1".encode("utf-8")).hexdigest()
    finally:
        connection.close()


def test_authoritative_quest_backfill_ignores_excluded_texts_and_keeps_git_created(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)

        quest_hash_map_utils.refresh_quest_hash_map_for_quest_ids(connection.cursor(), [3])
        created_rows, updated_rows = version_control.backfill_quest_created_version_from_textmap(
            connection.cursor(),
            quest_ids=[3],
            authoritative=True,
            with_stats=True,
        )
        connection.commit()

        created_version = connection.execute(
            "SELECT created_version_id FROM quest WHERE questId=3"
        ).fetchone()[0]
        quest_versions = connection.execute(
            "SELECT lang, updated_version_id FROM quest_version WHERE questId=3 ORDER BY lang"
        ).fetchall()

        assert created_rows == 1
        assert updated_rows == 0
        assert created_version == 2
        assert quest_versions == []
    finally:
        connection.close()


def test_database_helper_keeps_test_titles_visible_but_filters_non_title_texts(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)

        assert databaseHelper.getQuestName(1, 1) == "Act I · Chapter Name · (test)Quest Title"
        assert databaseHelper.getQuestDescription(1, 1) == ""
        assert databaseHelper.getQuestLongDescription(1, 1) == ""
        assert databaseHelper.getQuestStepTitleMap(1, 1) == {}

        assert [row[0] for row in databaseHelper.selectQuestByTitleKeyword("test", 1)] == [1, 3]
        assert databaseHelper.selectQuestByIdContains("1", 1)[0][1] == "(test)Quest Title"
        assert databaseHelper.selectQuestByVersion(1, limit=10)[0][1] == "(test)Quest Title"

        assert databaseHelper.selectQuestByContentKeyword("test", 1) == []
        content_rows = databaseHelper.selectQuestByContentKeyword("Normal Quest Dialogue", 1)
        assert [row[0] for row in content_rows] == [1, 2]

        assert databaseHelper.countQuestDialogues(1) == 1
        assert databaseHelper.selectQuestDialoguesPaged(1) == [
            (106, "TALK_ROLE_NPC", 1, 5002, 1001),
        ]
    finally:
        connection.close()

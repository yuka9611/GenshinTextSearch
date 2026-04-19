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


def _seed_short_generic_regression_quest(connection: sqlite3.Connection, quest_id: int = 4):
    connection.executemany(
        "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?,?,?,?)",
        [
            (20, "Version 2.0", "2.0", 20),
            (50, "Version 5.0", "5.0", 50),
            (52, "Version 5.2", "5.2", 52),
            (65, "Version 6.5", "6.5", 65),
        ],
    )
    connection.executemany(
        """
        INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id)
        VALUES (?,?,?,?,?)
        """,
        [
            (1, 401, "Quest Title", 50, 50),
            (1, 402, "呀呀！", 65, 65),
            (1, 403, "……", 20, 20),
            (1, 404, "Long meaningful dialogue that should drive the quest version.", 50, 52),
        ],
    )
    connection.execute(
        """
        INSERT INTO quest(
            questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId,
            source_type, source_code_raw, created_version_id, git_created_version_id
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (quest_id, 401, None, None, 10, None, None, 65, 50),
    )
    connection.execute(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        (quest_id, 4001, None, 0),
    )
    connection.executemany(
        """
        INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId)
        VALUES (?,?,?,?,?,?)
        """,
        [
            (8001, "TALK_ROLE_NPC", 1, 4001, 402, None),
            (8002, "TALK_ROLE_NPC", 1, 4001, 403, None),
            (8003, "TALK_ROLE_NPC", 1, 4001, 404, None),
        ],
    )
    connection.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
        [
            (4001, 0, 8001),
            (4001, 0, 8002),
            (4001, 0, 8003),
        ],
    )
    connection.commit()


def _seed_short_generic_title_quest(connection: sqlite3.Connection, quest_id: int = 5):
    connection.executemany(
        """
        INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id)
        VALUES (?,?,?,?,?)
        """,
        [
            (1, 410, "呀！", 3, 3),
            (1, 411, "呀！", 4, 4),
            (1, 412, "Useful dialogue with real semantic meaning.", 3, 4),
        ],
    )
    connection.execute(
        """
        INSERT INTO quest(
            questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId,
            source_type, source_code_raw, created_version_id, git_created_version_id
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (quest_id, 410, None, None, 10, None, None, 3, 3),
    )
    connection.execute(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        (quest_id, 5001, None, 0),
    )
    connection.executemany(
        """
        INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId)
        VALUES (?,?,?,?,?,?)
        """,
        [
            (9001, "TALK_ROLE_NPC", 1, 5001, 411, None),
            (9002, "TALK_ROLE_NPC", 1, 5001, 412, None),
        ],
    )
    connection.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
        [
            (5001, 0, 9001),
            (5001, 0, 9002),
        ],
    )
    connection.commit()


def _seed_broad_short_response_regression_quest(connection: sqlite3.Connection, quest_id: int = 6):
    connection.executemany(
        "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?,?,?,?)",
        [
            (57, "Version 5.7", "5.7", 57),
            (60, "Version 6.0", "6.0", 60),
        ],
    )
    connection.executemany(
        """
        INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id)
        VALUES (?,?,?,?,?)
        """,
        [
            (1, 610, "Quest Title", 57, 57),
            (1, 611, "怎么了？", 4, 4),
            (1, 612, "谢谢。", 2, 2),
            (1, 613, "Useful dialogue with quest-specific meaning.", 57, 60),
        ],
    )
    connection.execute(
        """
        INSERT INTO quest(
            questId, titleTextMapHash, descTextMapHash, longDescTextMapHash, chapterId,
            source_type, source_code_raw, created_version_id, git_created_version_id
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (quest_id, 610, None, None, 10, None, None, 60, 57),
    )
    connection.execute(
        "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
        (quest_id, 6001, None, 0),
    )
    connection.executemany(
        """
        INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId)
        VALUES (?,?,?,?,?,?)
        """,
        [
            (9601, "TALK_ROLE_NPC", 1, 6001, 611, None),
            (9602, "TALK_ROLE_NPC", 1, 6001, 612, None),
            (9603, "TALK_ROLE_NPC", 1, 6001, 613, None),
        ],
    )
    connection.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
        [
            (6001, 0, 9601),
            (6001, 0, 9602),
            (6001, 0, 9603),
        ],
    )
    connection.commit()


def test_is_excluded_quest_text_matches_only_prefixes():
    from quest_text_filters import is_excluded_quest_text

    assert is_excluded_quest_text("（test）Quest")
    assert is_excluded_quest_text("（Test）Quest")
    assert is_excluded_quest_text("(test)Quest")
    assert is_excluded_quest_text("(Test)Quest")
    assert not is_excluded_quest_text("Quest（test）suffix")


def test_is_short_generic_text_matches_expected_examples():
    from quest_text_filters import (
        is_excluded_quest_version_dialogue_text,
        is_short_generic_text,
    )

    for text in (
        "……",
        "呀！",
        "呀呀！",
        "唔…",
        "嗯？",
        "欸？！",
        "怎么了？",
        "没问题。",
        "谢谢。",
        "交给我吧。",
        "可是…",
        "喵。",
        "原来是这样…",
    ):
        assert is_short_generic_text(text)
        assert is_excluded_quest_version_dialogue_text(text)

    for text in ("风起地", "白术", "白垩之章", "请跟我来。"):
        assert not is_short_generic_text(text)

    assert is_excluded_quest_version_dialogue_text("(test)Quest")


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


def test_refresh_quest_hash_map_keeps_short_generic_titles_but_skips_short_generic_dialogues(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)
        _seed_short_generic_title_quest(connection)

        touched = quest_hash_map_utils.refresh_quest_hash_map_for_quest_ids(connection.cursor(), [5])
        rows = connection.execute(
            "SELECT questId, hash, source_type FROM quest_hash_map WHERE questId=5 ORDER BY hash, source_type"
        ).fetchall()

        assert touched == 1
        assert rows == [
            (5, 410, "title"),
            (5, 412, "dialogue"),
        ]
    finally:
        connection.close()


def test_refresh_quest_hash_map_skips_broad_short_response_dialogues(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)
        _seed_broad_short_response_regression_quest(connection)

        touched = quest_hash_map_utils.refresh_quest_hash_map_for_quest_ids(connection.cursor(), [6])
        rows = connection.execute(
            "SELECT questId, hash, source_type FROM quest_hash_map WHERE questId=6 ORDER BY hash, source_type"
        ).fetchall()

        assert touched == 1
        assert rows == [
            (6, 610, "title"),
            (6, 613, "dialogue"),
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
        assert updated_rows >= 0
        assert created_version == 2
        assert quest_versions == [(1, 9), (2, 9)]
    finally:
        connection.close()


def test_authoritative_quest_backfill_ignores_short_generic_dialogues_with_hash_map(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)
        _seed_short_generic_regression_quest(connection)

        quest_hash_map_utils.refresh_quest_hash_map_for_quest_ids(connection.cursor(), [4])
        created_rows, updated_rows = version_control.backfill_quest_created_version_from_textmap(
            connection.cursor(),
            quest_ids=[4],
            authoritative=True,
            with_stats=True,
        )
        connection.commit()

        created_version = connection.execute(
            "SELECT created_version_id FROM quest WHERE questId=4"
        ).fetchone()[0]
        quest_versions = connection.execute(
            "SELECT lang, updated_version_id FROM quest_version WHERE questId=4 ORDER BY lang"
        ).fetchall()

        assert created_rows == 1
        assert updated_rows == 1
        assert created_version == 50
        assert quest_versions == [(1, 52)]
    finally:
        connection.close()


def test_authoritative_quest_backfill_ignores_short_generic_dialogues_without_hash_map(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)
        _seed_short_generic_regression_quest(connection)

        created_rows, updated_rows = version_control.backfill_quest_created_version_from_textmap(
            connection.cursor(),
            quest_ids=[4],
            authoritative=True,
            with_stats=True,
        )
        connection.commit()

        created_version = connection.execute(
            "SELECT created_version_id FROM quest WHERE questId=4"
        ).fetchone()[0]
        quest_versions = connection.execute(
            "SELECT lang, updated_version_id FROM quest_version WHERE questId=4 ORDER BY lang"
        ).fetchall()

        assert created_rows == 1
        assert updated_rows == 1
        assert created_version == 50
        assert quest_versions == [(1, 52)]
    finally:
        connection.close()


def test_authoritative_quest_backfill_ignores_broad_short_response_dialogues(monkeypatch):
    connection = sqlite3.connect(":memory:")
    try:
        _bootstrap_quest_db(connection)
        _patch_modules(monkeypatch, connection)
        _seed_broad_short_response_regression_quest(connection)

        quest_hash_map_utils.refresh_quest_hash_map_for_quest_ids(connection.cursor(), [6])
        created_rows, updated_rows = version_control.backfill_quest_created_version_from_textmap(
            connection.cursor(),
            quest_ids=[6],
            authoritative=True,
            with_stats=True,
        )
        connection.commit()

        created_version = connection.execute(
            "SELECT created_version_id FROM quest WHERE questId=6"
        ).fetchone()[0]
        quest_versions = connection.execute(
            "SELECT lang, updated_version_id FROM quest_version WHERE questId=6 ORDER BY lang"
        ).fetchall()

        assert created_rows == 1
        assert updated_rows == 1
        assert created_version == 57
        assert quest_versions == [(1, 60)]
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

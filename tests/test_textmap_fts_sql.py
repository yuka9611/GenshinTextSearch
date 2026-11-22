"""Regression tests for textMap FTS trigger SQL."""
import os
import sqlite3
import sys

import pytest


DBBUILD_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
DBBUILD_DIR = os.path.normpath(DBBUILD_DIR)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from textmap_fts_sql import (  # noqa: E402
    build_textmap_fts_ad_trigger_sql,
    build_textmap_fts_ai_trigger_sql,
    build_textmap_fts_au_trigger_sql,
    build_textmap_fts_table_sql,
)


def _build_temp_db(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.create_function("gts_fts_content", 2, lambda _lang, content: str(content or ""))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE textMap(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash INTEGER,
            content TEXT,
            lang INTEGER
        )
        """
    )
    langs_sql = "1,4,9"
    cur.execute(build_textmap_fts_table_sql("unicode61", "full", 0))
    cur.execute(build_textmap_fts_ai_trigger_sql(langs_sql))
    cur.execute(build_textmap_fts_ad_trigger_sql(langs_sql))
    cur.execute(build_textmap_fts_au_trigger_sql(langs_sql))
    conn.commit()
    return conn


def test_non_fts_languages_do_not_error_or_touch_fts(tmp_path):
    conn = _build_temp_db(tmp_path / "textmap-non-fts.db")
    cur = conn.cursor()

    cur.execute("INSERT INTO textMap(hash, content, lang) VALUES (?,?,?)", (100, "before", 2))
    conn.commit()
    cur.execute("UPDATE textMap SET content=? WHERE hash=? AND lang=?", ("after", 100, 2))
    conn.commit()
    cur.execute("DELETE FROM textMap WHERE hash=? AND lang=?", (100, 2))
    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM textMap_fts").fetchone()[0]
    assert count == 0
    conn.close()


def test_fts_languages_still_round_trip_through_index(tmp_path):
    conn = _build_temp_db(tmp_path / "textmap-fts.db")
    cur = conn.cursor()

    cur.execute("INSERT INTO textMap(hash, content, lang) VALUES (?,?,?)", (200, "hello world", 4))
    conn.commit()
    assert cur.execute("SELECT COUNT(*) FROM textMap_fts").fetchone()[0] == 1

    cur.execute("UPDATE textMap SET content=? WHERE hash=? AND lang=?", ("hello sqlite", 200, 4))
    conn.commit()
    rows = cur.execute("SELECT rowid, lang, hash FROM textMap_fts").fetchall()
    assert rows == [(1, 4, 200)]

    cur.execute("DELETE FROM textMap WHERE hash=? AND lang=?", (200, 4))
    conn.commit()
    assert cur.execute("SELECT COUNT(*) FROM textMap_fts").fetchone()[0] == 0
    conn.close()


@pytest.fixture
def search_db(monkeypatch):
    import databaseHelper

    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE textMap(id INTEGER PRIMARY KEY, hash INTEGER, content TEXT, lang INTEGER)"
    )
    monkeypatch.setattr(databaseHelper, "conn", connection)
    monkeypatch.setattr(databaseHelper, "_CACHE", {
        "column": {}, "table": {}, "fts": {
            "available": None, "tokenizer": None, "detail": None, "langs": {1, 4, 9},
        },
    })
    monkeypatch.setattr(databaseHelper.config, "getEnableTextMapFts", lambda: True)
    monkeypatch.setattr(databaseHelper.config, "getFtsTokenizer", lambda: "trigram")
    yield databaseHelper, connection
    connection.close()


@pytest.mark.parametrize("detail", ["none", "column", "full"])
@pytest.mark.parametrize("keyword,lang", [("abcdef", 4), ('ab"cd', 4), ("アルハイゼン", 9)])
def test_long_literal_search_uses_trigram_index_and_verifies_substring(search_db, detail, keyword, lang):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", detail, 0))
    scattered = " / ".join(keyword[index:index + 3] for index in range(len(keyword) - 2))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, ?)",
        [(10, f"prefix {keyword} suffix", lang), (20, scattered, lang), (30, "unrelated", lang)],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    statements = []
    connection.set_trace_callback(statements.append)

    rows = helper.selectTextMapFromKeyword(keyword, lang)
    total = helper.countTextMapFromKeyword(keyword, lang)

    assert [row[0] for row in rows] == [10]
    assert total == 1
    searches = [sql for sql in statements if sql.startswith(("select tm.hash", "select count(*) from textMap tm"))]
    assert len(searches) == 2
    assert all(" match " in sql for sql in searches)
    assert helper._get_textmap_fts_detail() == detail


@pytest.mark.parametrize("detail", ["none", "column"])
def test_long_chinese_search_retains_noncontiguous_like_results(search_db, detail):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", detail, 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, 1)",
        [(10, "天空之琴"), (20, "天边的空岛之上有琴声"), (30, "琴之空天")],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")

    assert helper._build_textmap_fts_match("天空之琴", 1) is None
    assert {row[0] for row in helper.selectTextMapFromKeyword("天空之琴", 1)} == {10, 20}
    assert helper.countTextMapFromKeyword("天空之琴", 1) == 2


@pytest.mark.parametrize("lang", [1, 2, 4, 9])
def test_language_filter_does_not_read_external_fts_content_rows(search_db, lang):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", "none", 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, ?)",
        [(100 + language, "abc", language) for language in (1, 2, 4, 9)],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    statements = []
    connection.set_trace_callback(statements.append)

    assert helper.selectTextMapFromKeyword("abc", lang) == [(100 + lang, "abc")]
    assert helper.countTextMapFromKeyword("abc", lang) == 1
    # FTS5 emits internal "SELECT T..." statements when unindexed columns
    # read through its external-content table. Only the outer textMap lookup
    # should load those rows; language filtering still isolates all results.
    assert not any(sql.startswith("--") and "SELECT T." in sql for sql in statements)


@pytest.mark.parametrize("detail", ["none", "column"])
def test_reduced_detail_search_preserves_literal_whitespace_and_wildcards(search_db, detail):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", detail, 0))
    keyword = "abc   %_def"
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, 4)",
        [(10, keyword), (20, "abc %_def"), (30, "abc   xyzdef")],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")

    assert [row[0] for row in helper.selectTextMapFromKeyword(keyword, 4)] == [10, 20]
    assert helper.countTextMapFromKeyword(keyword, 4) == 2


def test_three_character_chinese_search_keeps_existing_fts_behavior(search_db):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", "none", 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, 1)",
        [(10, "凯瑟琳"), (20, "凯、瑟、琳")],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")

    assert helper._build_textmap_fts_match("凯瑟琳", 1) == '"凯瑟琳"'
    assert [row[0] for row in helper.selectTextMapFromKeyword("凯瑟琳", 1)] == [10]


def test_full_detail_chinese_search_retains_contiguous_phrase(search_db):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", "full", 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, 1)",
        [(10, "天空之琴"), (20, "天边的空岛之上有琴声")],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")

    assert helper._build_textmap_fts_match("天空之琴", 1) == '"天空之琴"'
    assert [row[0] for row in helper.selectTextMapFromKeyword("天空之琴", 1)] == [10]


@pytest.mark.parametrize("detail", ["none", "column", "full"])
def test_chinese_space_separated_terms_match_three_char_and_reversed_order(search_db, detail):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", detail, 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, 1)",
        [
            (10, "天空之琴"),
            (11, "天空琴"),
            (12, "天边的空岛之上有琴声"),
            (20, "向着星辰与深渊！"),
            (21, "深渊 星辰"),
            (22, "深境螺旋里闪耀的渊星和星芒照亮辰光"),
        ],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")

    assert helper._build_textmap_fts_match("天空 琴", 1) is None
    rows_sky = helper.selectTextMapFromKeyword("天空 琴", 1)
    assert [row[0] for row in rows_sky] == [11, 10]
    assert helper.countTextMapFromKeyword("天空 琴", 1) == 2

    assert helper._build_textmap_fts_match("深渊 星辰", 1) is None
    rows_abyss = helper.selectTextMapFromKeyword("深渊 星辰", 1)
    assert [row[0] for row in rows_abyss] == [21, 20]
    assert helper.countTextMapFromKeyword("深渊 星辰", 1) == 2


@pytest.mark.parametrize("detail", ["none", "column", "full"])
def test_chinese_multi_term_uses_fts_for_long_terms_and_verifies_short_terms(search_db, detail):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", detail, 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, 1)",
        [
            (10, "你好，旅行者，欢迎来到蒙德城。"),
            (20, "蒙德欢迎你们，随风而来的旅行者。"),
            (30, "旅行者来自璃月"),
        ],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    statements = []
    connection.set_trace_callback(statements.append)

    rows_forward = helper.selectTextMapFromKeyword("旅行者 蒙德", 1)
    rows_reverse = helper.selectTextMapFromKeyword("蒙德 旅行者", 1)

    assert helper._build_textmap_fts_match("旅行者 蒙德", 1) == '"旅行者"'
    assert {row[0] for row in rows_forward} == {10, 20}
    assert {row[0] for row in rows_reverse} == {10, 20}
    assert helper.countTextMapFromKeyword("蒙德 旅行者", 1) == 2
    assert any(" match " in sql for sql in statements)


@pytest.mark.parametrize("detail", ["none", "column", "full"])
@pytest.mark.parametrize(
    "lang,rows,keyword,expected_order,expect_fts",
    [
        (
            4,
            [
                (10, "Ad astra abyssosque!"),
                (11, "Ad abyssosque"),
                (12, "Ad astra only"),
            ],
            "Ad abyssosque",
            [11, 10],
            True,
        ),
        (
            4,
            [
                (20, "Welcome to the Adventurers' Guild!"),
                (21, "Guild greets you"),
            ],
            "Welcome Guild",
            [20],
            True,
        ),
        (
            9,
            [
                (30, "こういう大型の遺跡はまだ多い"),
                (31, "大型の遺跡についてこういう話がある"),
                (32, "大型だけです"),
            ],
            "こういう 大型",
            [30, 31],
            True,
        ),
        (
            9,
            [
                (40, "遥か昔に残された龍の遺跡。大型装置が隠されている。"),
                (41, "大型の遺跡"),
            ],
            "大型　遺跡",
            [41, 40],
            False,
        ),
    ],
)
def test_non_chinese_space_separated_terms_work_with_trigram_fts(
    search_db, detail, lang, rows, keyword, expected_order, expect_fts
):
    helper, connection = search_db
    connection.execute(build_textmap_fts_table_sql("trigram", detail, 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, ?)",
        [(h, c, lang) for h, c in rows],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    statements = []
    connection.set_trace_callback(statements.append)

    matched = helper.selectTextMapFromKeyword(keyword, lang)
    total = helper.countTextMapFromKeyword(keyword, lang)

    assert [row[0] for row in matched] == expected_order
    assert total == len(expected_order)
    if expect_fts:
        assert any(" match " in sql for sql in statements)


def test_unicode61_chinese_multi_term_skips_single_char_fts_term(search_db, monkeypatch):
    helper, connection = search_db
    connection.create_function("gts_fts_content", 2, lambda _lang, content: str(content or ""))
    monkeypatch.setattr(helper.config, "getFtsTokenizer", lambda: "unicode61")
    monkeypatch.setattr(helper.config, "getFtsChineseSegmenter", lambda: "char_bigram")
    connection.execute(build_textmap_fts_table_sql("unicode61", "full", 0))
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, 1)",
        [(10, "天空之琴"), (20, "天空之城")],
    )
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")

    fts_expr = helper._build_textmap_fts_match("天空 琴", 1)
    assert fts_expr is not None
    assert "琴" not in fts_expr
    assert [row[0] for row in helper.selectTextMapFromKeyword("天空 琴", 1)] == [10]
    assert helper.countTextMapFromKeyword("天空 琴", 1) == 1


def test_multi_term_across_readable_subtitle_quest_catalog_voice_story(search_db):
    helper, connection = search_db
    connection.executescript(
        """
        CREATE TABLE readable(
            fileName TEXT,
            content TEXT,
            titleTextMapHash INTEGER,
            readableId INTEGER,
            lang TEXT
        );
        CREATE TABLE subtitle(
            fileName TEXT,
            content TEXT,
            startTime REAL,
            endTime REAL,
            subtitleId INTEGER,
            lang INTEGER
        );
        CREATE TABLE quest(
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            descTextMapHash INTEGER
        );
        CREATE TABLE questTalk(
            questId INTEGER,
            talkId INTEGER,
            coopQuestId INTEGER
        );
        CREATE TABLE dialogue(
            dialogueId INTEGER PRIMARY KEY,
            talkId INTEGER,
            coopQuestId INTEGER,
            talkerType TEXT,
            talkerId INTEGER,
            textHash INTEGER
        );
        CREATE TABLE text_source_entity(
            entity_id INTEGER,
            source_type_code INTEGER,
            title_hash INTEGER,
            text_hash INTEGER,
            sub_category INTEGER
        );
        CREATE TABLE fetters(
            fetterId INTEGER,
            avatarId INTEGER,
            voiceFile INTEGER,
            voiceTitleTextMapHash INTEGER,
            voiceFileTextTextMapHash INTEGER
        );
        CREATE TABLE fetterVoice(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            avatarId INTEGER NOT NULL,
            voiceFile INTEGER NOT NULL,
            voicePath TEXT NOT NULL
        );
        CREATE TABLE fetterStory(
            avatarId INTEGER,
            fetterId INTEGER,
            storyTitleTextMapHash INTEGER,
            storyTitle2TextMapHash INTEGER,
            storyTitleLockedTextMapHash INTEGER,
            storyContextTextMapHash INTEGER,
            storyContext2TextMapHash INTEGER
        );
        """
    )
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, ?)",
        [
            (1001, "Ad astra abyssosque!", 4),
            (1002, "Welcome to the Adventurers' Guild", 4),
            (1003, "Skyward Harp of Mondstadt", 4),
            (1004, "Greeting Voice", 4),
            (1005, "Ad astra, Traveler, abyssosque!", 4),
            (1006, "Character Story 1", 4),
            (1007, "Story Locked", 4),
            (1008, "Katheryne always says Ad astra abyssosque.", 4),
        ],
    )
    connection.execute(
        "INSERT INTO readable VALUES ('Book1_EN', 'Ad astra abyssosque in Teyvat', 1001, 1, 'EN')"
    )
    connection.execute(
        "INSERT INTO subtitle VALUES ('Cutscene_EN', 'Welcome to the Adventurers Guild', 0.0, 2.0, 1, 4)"
    )
    connection.execute("INSERT INTO quest VALUES (500, 1002, 1001)")
    connection.execute(
        "INSERT INTO text_source_entity VALUES (77, 9, 1003, 1003, 1)"
    )
    connection.execute("INSERT INTO fetters VALUES (1, 10000001, 99, 1004, 1005)")
    connection.execute(
        "INSERT INTO fetterStory VALUES (10000001, 2, 1006, NULL, 1007, 1008, NULL)"
    )

    assert len(helper.selectReadableFromKeyword("abyssosque astra", 4, "EN")) == 1
    assert helper.countReadableFromKeyword("abyssosque astra", 4, "EN") == 1
    assert len(helper.selectSubtitleFromKeyword("Guild Welcome", 4)) == 1
    assert helper.countSubtitleFromKeyword("Guild Welcome", 4) == 1
    assert [row[0] for row in helper.selectQuestByTitleKeyword("Guild Welcome", 4)] == [500]
    assert [row[0] for row in helper.selectQuestByContentKeyword("abyssosque astra", 4)] == [500]
    assert [row[0] for row in helper.selectCatalogEntities("Mondstadt Harp", 4)] == [77]
    assert helper.countCatalogEntities("Mondstadt Harp", 4) == 1
    assert len(helper._select_avatar_scoped_voice_items_by_filters("abyssosque astra", 4)) == 1
    assert len(helper.selectAvatarStoryItemsByFilters("abyssosque Katheryne", 4)) == 1

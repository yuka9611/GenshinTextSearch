"""Regression tests for search optimization paths, caches, batching, and query plans."""

from __future__ import annotations

import re
import sqlite3

import pytest

from textmap_fts_sql import build_textmap_fts_table_sql


def _create_full_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE avatar(avatarId INTEGER, nameTextMapHash INTEGER);
        CREATE INDEX avatar_avatarId_index ON avatar(avatarId);

        CREATE TABLE npc(npcId INTEGER, textHash INTEGER, created_version_id INTEGER);
        CREATE INDEX npc_npcId_index ON npc(npcId);

        CREATE TABLE manualTextMap(textMapId TEXT, textHash INTEGER);
        CREATE UNIQUE INDEX manualTextMap_textMapId_uindex ON manualTextMap(textMapId);

        CREATE TABLE chapter(chapterId INTEGER, chapterTitleTextMapHash INTEGER, chapterNumTextMapHash INTEGER);
        CREATE INDEX chapter_chapterId_index ON chapter(chapterId);

        CREATE TABLE langCode(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codeName TEXT UNIQUE,
            displayName TEXT,
            imported INT
        );

        CREATE TABLE quest(
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            descTextMapHash INTEGER,
            chapterId INTEGER,
            source_type TEXT,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE INDEX quest_questId_index ON quest(questId);

        CREATE TABLE quest_version(
            questId INTEGER,
            lang INTEGER,
            updated_version_id INTEGER,
            PRIMARY KEY (questId, lang)
        );

        CREATE TABLE questTalk(questId INTEGER, talkId INTEGER, coopQuestId INTEGER);
        CREATE UNIQUE INDEX questTalk_questId_talkId_coopQuestId_uindex ON questTalk(questId, talkId, coopQuestId);
        CREATE INDEX questTalk_talkId_coopQuestId_index ON questTalk(talkId, coopQuestId);

        CREATE TABLE quest_hash_map(questId INTEGER, hash INTEGER, source_type TEXT);
        CREATE INDEX quest_hash_map_hash_index ON quest_hash_map(hash);
        CREATE INDEX quest_hash_map_questId_index ON quest_hash_map(questId);

        CREATE TABLE dialogue(
            dialogueId INTEGER PRIMARY KEY,
            talkerType TEXT,
            talkerId INTEGER,
            talkId INTEGER,
            textHash INTEGER,
            coopQuestId INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE INDEX dialogue_dialogueId_index ON dialogue(dialogueId);
        CREATE INDEX dialogue_textHash_index ON dialogue(textHash);
        CREATE INDEX dialogue_talkId_coopQuestId_dialogueId_index ON dialogue(talkId, coopQuestId, dialogueId);
        CREATE INDEX dialogue_talkerType_talkerId_talkId_coopQuestId_dialogueId_index
            ON dialogue(talkerType, talkerId, talkId, coopQuestId, dialogueId);

        CREATE TABLE talk_dialogue_link(talkId INTEGER, coopQuestId INTEGER, dialogueId INTEGER);
        CREATE INDEX talk_dialogue_link_dialogueId_index ON talk_dialogue_link(dialogueId);
        CREATE INDEX talk_dialogue_link_talkId_coopQuestId_index ON talk_dialogue_link(talkId, coopQuestId);

        CREATE TABLE voice(dialogueId INTEGER, voicePath TEXT);
        CREATE INDEX voice_dialogueId_index ON voice(dialogueId);

        CREATE TABLE fetters(
            fetterId INTEGER,
            avatarId INTEGER,
            voiceTitleTextMapHash INTEGER,
            voiceFileTextTextMapHash INTEGER,
            voiceFile INTEGER
        );
        CREATE INDEX fetters_voiceFileTextTextMapHash_index ON fetters(voiceFileTextTextMapHash);
        CREATE INDEX fetters_voiceFile_index ON fetters(voiceFile);

        CREATE TABLE fetterVoice(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            avatarId INTEGER NOT NULL,
            voiceFile INTEGER NOT NULL,
            voicePath TEXT NOT NULL
        );
        CREATE INDEX fetterVoice_avatarId_voiceFile_index ON fetterVoice(avatarId, voiceFile);
        CREATE UNIQUE INDEX fetterVoice_avatarId_voiceFile_voicePath_uindex ON fetterVoice(avatarId, voiceFile, voicePath);

        CREATE TABLE fetterStory(
            fetterId INTEGER,
            avatarId INTEGER,
            storyTitleTextMapHash INTEGER,
            storyTitle2TextMapHash INTEGER,
            storyTitleLockedTextMapHash INTEGER,
            storyContextTextMapHash INTEGER,
            storyContext2TextMapHash INTEGER
        );
        CREATE INDEX fetterStory_avatarId_index ON fetterStory(avatarId);

        CREATE TABLE text_source_entity(
            entity_id INTEGER,
            source_type_code INTEGER,
            title_hash INTEGER,
            text_hash INTEGER,
            sub_category INTEGER
        );
        CREATE INDEX text_source_entity_text_hash_index ON text_source_entity(text_hash);
        CREATE INDEX text_source_entity_source_type_code_index ON text_source_entity(source_type_code);

        CREATE TABLE readable(
            fileName TEXT,
            lang TEXT,
            content TEXT,
            titleTextMapHash INTEGER,
            readableId INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE INDEX readable_fileName_index ON readable(fileName);
        CREATE INDEX readable_lang_index ON readable(lang);
        CREATE INDEX readable_titleTextMapHash_index ON readable(titleTextMapHash);

        CREATE TABLE readable_meta(
            normalized_file_name TEXT PRIMARY KEY,
            readable_id INTEGER,
            title_text_map_hash INTEGER,
            readable_category TEXT NOT NULL
        );

        CREATE TABLE subtitle(
            fileName TEXT,
            lang INTEGER,
            startTime REAL,
            endTime REAL,
            content TEXT,
            subtitleId INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE INDEX subtitle_lang_index ON subtitle(lang);
        CREATE INDEX subtitle_fileName_index ON subtitle(fileName);
        CREATE INDEX subtitle_subtitleId_index ON subtitle(subtitleId);

        CREATE TABLE version_dim(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_version TEXT NOT NULL UNIQUE,
            version_tag TEXT,
            version_sort_key INTEGER
        );

        CREATE TABLE textMap(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash INTEGER,
            content TEXT,
            lang INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER,
            UNIQUE (lang, hash)
        );
        CREATE INDEX textMap_hash_index ON textMap(hash);
        CREATE INDEX textMap_lang_index ON textMap(lang);
        """
    )
    connection.execute(build_textmap_fts_table_sql("trigram", "none", 0))


def _seed_sample_data(connection: sqlite3.Connection) -> None:
    connection.executemany(
        "INSERT INTO langCode(id, codeName, displayName, imported) VALUES (?, ?, ?, ?)",
        [(1, "TextMapCHS.json", "简体中文", 1), (4, "TextMapEN.json", "English", 1)],
    )
    connection.executemany(
        "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?, ?, ?, ?)",
        [(1, "1.0", "1.0", 100), (2, "4.0", "4.0", 400)],
    )
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?, ?, ?, ?, ?)",
        [
            # Exact / prefix / substring matches for '旅行' and 'Traveler' with varied voice & source statuses
            (100, "旅行", 1, 1, 1),                # dialogue + voice
            (101, "旅行者", 1, 1, 1),              # fetters + voice
            (102, "旅行记录", 1, 1, 2),            # quest known source, no voice
            (103, "关于旅行的故事", 1, 1, 1),      # unknown source, no voice
            (104, "另一段旅行文本", 1, 2, 2),      # unknown source, no voice
            (105, "旅行开始之前", 1, 1, 1),        # dialogue NPC (no voice)
            (106, "旅行", 4, 1, 1),
            (100, "Traveler line voiced", 4, 1, 1),
            (101, "Traveler voice fetter", 4, 1, 1),
            (103, "Traveler unknown note", 4, 1, 1),
            # Names / speakers / quests / entities
            (2001, "派蒙", 1, 1, 1),
            (2001, "Paimon", 4, 1, 1),
            (2002, "凯瑟琳", 1, 1, 1),
            (2002, "Katheryne", 4, 1, 1),
            (3001, "风神之翼", 1, 1, 1),
            (3002, "天空之风的描述", 1, 1, 1),
            (4001, "捕捉风的异乡人", 1, 1, 1),
            (4002, "风起地的任务描述", 1, 1, 1),
            (4003, "序章", 1, 1, 1),
            (4004, "第一幕", 1, 1, 1),
            (4005, "105", 1, 1, 1),
            (5001, "这本书讲述了旅行的意义", 1, 1, 1),
        ],
    )
    connection.execute("INSERT INTO npc(npcId, textHash) VALUES (1001, 2002)")
    connection.execute("INSERT INTO avatar(avatarId, nameTextMapHash) VALUES (10000005, 2001)")
    connection.executemany(
        "INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (10, "TALK_ROLE_NPC", 1001, 500, 100, 0),
            (11, "TALK_ROLE_NPC", 1001, 0, 105, 0),
            (12, "TALK_ROLE_NPC", 1001, 0, 2002, 0),
        ],
    )
    connection.execute("INSERT INTO voice(dialogueId, voicePath) VALUES (10, 'VO_100.wem')")
    connection.execute("INSERT INTO fetters(fetterId, avatarId, voiceTitleTextMapHash, voiceFileTextTextMapHash, voiceFile) VALUES (1, 10000005, 3001, 101, 9001)")
    connection.execute("INSERT INTO fetterVoice(avatarId, voiceFile, voicePath) VALUES (10000005, 9001, 'VO_FETTER_101.wem')")
    connection.execute("INSERT INTO chapter(chapterId, chapterTitleTextMapHash, chapterNumTextMapHash) VALUES (10, 4003, 4004)")
    connection.execute("INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, chapterId, source_type, created_version_id, updated_version_id) VALUES (700, 4001, 4002, 10, 'AQ', 1, 1)")
    connection.execute("INSERT INTO questTalk(questId, talkId, coopQuestId) VALUES (700, 500, 0)")
    connection.executemany("INSERT INTO quest_hash_map(questId, hash, source_type) VALUES (?, ?, ?)", [(700, 102, "dialog"), (700, 100, "dialog")])
    connection.execute("INSERT INTO text_source_entity(entity_id, source_type_code, title_hash, text_hash, sub_category) VALUES (88, 1, 3001, 3002, 1)")
    connection.execute("INSERT INTO readable(fileName, lang, content, titleTextMapHash, readableId, created_version_id, updated_version_id) VALUES ('Book_Travel_CHS', 'CHS', '旅行游记正文', 5001, 1, 1, 1)")
    connection.execute("INSERT INTO subtitle(fileName, lang, startTime, endTime, content, subtitleId, created_version_id, updated_version_id) VALUES ('Scene_CHS', 1, 1.0, 2.0, '旅行者的字幕', 99, 1, 1)")
    connection.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    connection.commit()


def _bind_helper_to_connection(monkeypatch, connection: sqlite3.Connection):
    import controllers.common as common
    import databaseHelper

    databaseHelper._ensure_runtime_sql_functions(connection)
    monkeypatch.setattr(databaseHelper, "conn", connection)
    monkeypatch.setattr(
        databaseHelper,
        "_CACHE",
        {"column": {}, "table": {}, "fts": {"available": None, "tokenizer": None, "detail": None, "langs": {1, 4, 9}}},
    )
    monkeypatch.setattr(databaseHelper, "_VOICED_HASH_CACHE", {"key": None, "hashes": None})
    monkeypatch.setattr(databaseHelper, "_KNOWN_SOURCE_HASH_CACHE", {"key": None, "hashes": None})
    monkeypatch.setattr(databaseHelper.config, "getEnableTextMapFts", lambda: True)
    monkeypatch.setattr(databaseHelper.config, "getFtsTokenizer", lambda: "trigram")
    monkeypatch.setattr(databaseHelper.config, "getResultLanguages", lambda: [1, 4])
    monkeypatch.setattr(databaseHelper.config, "getSourceLanguage", lambda: 1)
    common.search_cache.clear()
    for fn_name in (
        "_count_dialogue_by_talker_keyword_cached",
        "_count_dialogue_by_talker_type_cached",
        "_count_dialogue_by_talker_and_keyword_cached",
        "_count_dialogue_by_talker_type_and_keyword_cached",
        "_count_fetter_by_speaker_and_keyword_cached",
        "_count_fetter_by_speaker_keyword_cached",
        "_count_story_by_speaker_keyword_cached",
        "_count_story_by_speaker_and_keyword_cached",
        "_count_textmap_from_keyword_cached",
        "_count_textmap_from_keyword_voice_cached",
        "_count_readable_from_keyword_cached",
        "_count_subtitle_from_keyword_cached",
    ):
        fn = getattr(common, fn_name, None)
        if fn is not None and hasattr(fn, "cache_clear"):
            fn.cache_clear()
    return databaseHelper, common


def test_apply_connection_pragmas_does_not_run_pragma_optimize():
    import databaseHelper

    connection = sqlite3.connect(":memory:")
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    databaseHelper._apply_connection_pragmas(connection)
    assert not any("optimize" in sql.lower() for sql in statements)
    connection.close()


def test_voiced_hash_cache_file_db_fallback_on_mutation_and_memory_db(tmp_path, monkeypatch):
    db_file = tmp_path / "voiced.db"
    setup_conn = sqlite3.connect(db_file)
    _create_full_schema(setup_conn)
    _seed_sample_data(setup_conn)
    setup_conn.close()

    file_conn = sqlite3.connect(db_file)
    helper, _common = _bind_helper_to_connection(monkeypatch, file_conn)

    # Disk DB with total_changes == 0 activates cached voiced hash set + gts_has_voice().
    assert helper._can_use_cached_voice_fn() is True
    hashes = helper._get_voiced_text_hash_set(file_conn)
    assert hashes is not None
    assert {100, 101}.issubset(hashes)
    assert helper.hasVoiceForTextHashDb(100) is True
    assert helper.hasVoiceForTextHashDb(101) is True
    assert helper.hasVoiceForTextHashDb(103) is False
    assert file_conn.execute("SELECT gts_has_voice(100), gts_has_voice(103)").fetchone() == (1, 0)

    # Warmup helper populates cache safely without failing.
    monkeypatch.setattr(helper, "_VOICED_HASH_CACHE", {"key": None, "hashes": None})
    helper.warmup_runtime_caches()
    assert helper._VOICED_HASH_CACHE["hashes"] is not None

    # Mutating the open connection (total_changes > 0) disables stale cache usage cleanly.
    file_conn.execute("INSERT INTO voice(dialogueId, voicePath) VALUES (11, 'VO_105.wem')")
    assert file_conn.total_changes > 0
    assert helper._get_connection_db_cache_key(file_conn) is None
    assert helper._can_use_cached_voice_fn() is False
    assert helper.hasVoiceForTextHashDb(105) is True
    file_conn.close()

    # :memory: DBs do not use disk cache key and fall back accurately to SQL EXISTS.
    mem_conn = sqlite3.connect(":memory:")
    _create_full_schema(mem_conn)
    _seed_sample_data(mem_conn)
    _bind_helper_to_connection(monkeypatch, mem_conn)
    assert helper._get_voiced_text_hash_set(mem_conn) is None
    assert helper._can_use_cached_voice_fn() is False
    assert helper.hasVoiceForTextHashDb(100) is True
    assert helper.hasVoiceForTextHashDb(103) is False
    with_rows, with_total = helper.selectTextMapFromKeywordPagedWithCount("旅行", 1, limit=20, offset=0, voice_filter="with")
    without_rows, without_total = helper.selectTextMapFromKeywordPagedWithCount("旅行", 1, limit=20, offset=0, voice_filter="without")
    assert {r[0] for r in with_rows} == {100, 101}
    assert with_total == 2
    assert {r[0] for r in without_rows} == {102, 103, 104, 105, 5001}
    assert without_total == 5
    mem_conn.close()


@pytest.mark.parametrize("voice_filter", [None, "with", "without"])
def test_unknown_textmap_sql_matches_legacy_filter(monkeypatch, voice_filter):
    connection = sqlite3.connect(":memory:")
    _create_full_schema(connection)
    _seed_sample_data(connection)
    helper, common = _bind_helper_to_connection(monkeypatch, connection)

    legacy_entries, legacy_total = common._collect_unknown_textmap_candidates(
        "旅行", 1, None, False, set(), [1, 4], 1, voice_filter, None, None
    )
    sql_rows, sql_total = helper.selectUnknownTextMapFromKeywordPagedWithCount(
        "旅行", 1, limit=50, offset=0, hash_value=None, voice_filter=voice_filter
    )

    assert sql_total == legacy_total
    assert [r[0] for r in sql_rows] == [entry["hash"] for entry in legacy_entries]

    # Out-of-bounds offset still returns accurate total count.
    oob_rows, oob_total = helper.selectUnknownTextMapFromKeywordPagedWithCount(
        "旅行", 1, limit=50, offset=100, hash_value=None, voice_filter=voice_filter
    )
    assert oob_rows == []
    assert oob_total == legacy_total
    connection.close()


@pytest.mark.parametrize("prefs", [None, {"resultLanguages": [4]}])
def test_query_text_hash_info_batch_matches_single_row_output(monkeypatch, prefs):
    import config

    connection = sqlite3.connect(":memory:")
    _create_full_schema(connection)
    _seed_sample_data(connection)
    _helper, common = _bind_helper_to_connection(monkeypatch, connection)

    token = config.push_request_display_preferences(prefs) if prefs is not None else None
    try:
        hashes = [100, 101, 102, 106, 999999]
        langs = config.getResultLanguagesForResponse(1)
        source_lang = config.getSourceLanguage()

        single = [common.queryTextHashInfo(h, langs, source_lang, queryOrigin=False) for h in hashes]
        batched = common.queryTextHashInfoBatch(hashes, langs, source_lang, queryOrigin=False)
        assert batched == single
    finally:
        if token is not None:
            config.reset_request_display_preferences(token)
        connection.close()


@pytest.mark.parametrize("source_type", [None, "textmap", "unknown", "dialogue", "voice", "readable", "subtitle"])
@pytest.mark.parametrize("voice_filter", ["all", "with", "without"])
def test_ranked_paginate_before_hydrate_matches_legacy(tmp_path, monkeypatch, source_type, voice_filter):
    db_file = tmp_path / "ranked.db"
    connection = sqlite3.connect(db_file)
    _create_full_schema(connection)
    _seed_sample_data(connection)
    helper, common = _bind_helper_to_connection(monkeypatch, connection)

    # First run legacy path via monkeypatched identity anchor, then compare against fast path.
    fast_p1, fast_total = common.getTranslateObj(
        "旅行", 1, page=1, page_size=2, voice_filter=voice_filter, source_type=source_type
    )
    common.search_cache.clear()
    fast_p2, fast_total_p2 = common.getTranslateObj(
        "旅行", 1, page=2, page_size=2, voice_filter=voice_filter, source_type=source_type
    )
    assert fast_total == fast_total_p2

    common.search_cache.clear()
    monkeypatch.setattr(common, "_ranked_seams_patched", lambda: True)
    legacy_p1, legacy_total = common.getTranslateObj(
        "旅行", 1, page=1, page_size=2, voice_filter=voice_filter, source_type=source_type
    )
    common.search_cache.clear()
    legacy_p2, legacy_total_p2 = common.getTranslateObj(
        "旅行", 1, page=2, page_size=2, voice_filter=voice_filter, source_type=source_type
    )

    assert fast_total == legacy_total
    assert fast_total_p2 == legacy_total_p2
    assert fast_p1 == legacy_p1
    assert fast_p2 == legacy_p2
    connection.close()


def test_ranked_fast_path_hydrates_only_sliced_page_and_respects_display_preferences(monkeypatch):
    import config

    connection = sqlite3.connect(":memory:")
    _create_full_schema(connection)
    _seed_sample_data(connection)
    helper, common = _bind_helper_to_connection(monkeypatch, connection)

    hydrated_calls: list[list[int]] = []
    orig_batch = helper.selectTextMapFromTextHashesBatch

    def spy_batch(text_hashes, langs=None):
        hydrated_calls.append(list(text_hashes))
        return orig_batch(text_hashes, langs)

    monkeypatch.setattr(helper, "selectTextMapFromTextHashesBatch", spy_batch)

    token = config.push_request_display_preferences({"resultLanguages": [4]})
    try:
        fast_p2, fast_total = common.getTranslateObj("旅行", 1, page=2, page_size=2, source_type="textmap")
        assert len(hydrated_calls) >= 1
        # Only page 2 hashes (at most 2) should be passed to batch translation hydration.
        assert all(len(call_hashes) <= 2 for call_hashes in hydrated_calls)

        common.search_cache.clear()
        monkeypatch.setattr(common, "_ranked_seams_patched", lambda: True)
        legacy_p2, legacy_total = common.getTranslateObj("旅行", 1, page=2, page_size=2, source_type="textmap")
        assert fast_total == legacy_total
        assert fast_p2 == legacy_p2
    finally:
        config.reset_request_display_preferences(token)
        connection.close()


def test_query_plans_resist_poisoned_sqlite_stat1_and_return_expected_rows(tmp_path, monkeypatch):
    db_file = tmp_path / "poisoned_stat1.db"
    setup_conn = sqlite3.connect(db_file)
    _create_full_schema(setup_conn)
    _seed_sample_data(setup_conn)
    setup_conn.execute("ANALYZE")
    setup_conn.execute(
        "UPDATE sqlite_stat1 SET stat = '5000 2001' WHERE tbl = 'textMap' AND idx = 'textMap_lang_index'"
    )
    setup_conn.commit()
    setup_conn.close()

    conn = sqlite3.connect(db_file)
    helper, common = _bind_helper_to_connection(monkeypatch, conn)

    # Check EXPLAIN QUERY PLAN directly via trace capture for catalog / NPC / quest / speaker+keyword.
    captured_sqls: list[tuple[str, str]] = []

    def explain_queries(fn, label: str):
        trace_log: list[str] = []
        conn.set_trace_callback(trace_log.append)
        try:
            res = fn()
        finally:
            conn.set_trace_callback(None)
        selects = [
            s for s in trace_log
            if s.lstrip().upper().startswith(("WITH ", "SELECT "))
            and "sqlite_master" not in s.lower()
            and "sqlite_temp_master" not in s.lower()
            and "pragma_" not in s.lower()
        ]
        assert selects, f"No SELECT captured for {label}"
        plans: list[str] = []
        for sql_stmt in selects:
            plan_rows = conn.execute("EXPLAIN QUERY PLAN " + sql_stmt).fetchall()
            plans.append("\n".join(str(row[3]) for row in plan_rows))
        captured_sqls.append((label, "\n".join(selects), "\n".join(plans)))
        return res

    catalog_rows, catalog_total = explain_queries(
        lambda: helper.selectCatalogEntitiesWithCount("风神", 1, source_type_code=1, limit=20),
        "catalog",
    )
    assert catalog_total == 1 and catalog_rows[0][0] == 88

    npc_rows = explain_queries(lambda: helper.selectNpcDialogueSearchEntries("凯瑟琳", 1), "npc")
    assert len(npc_rows) == 1

    quest_content = explain_queries(lambda: helper.selectQuestByContentKeyword("风起地", 1), "quest_content")
    assert [r[0] for r in quest_content] == [700]

    quest_content_short = explain_queries(lambda: helper.selectQuestByContentKeyword("风起", 1), "quest_content_short")
    assert [r[0] for r in quest_content_short] == [700]

    dlg_rows, dlg_total = explain_queries(
        lambda: helper.selectDialogueByTalkerAndKeywordWithCount("凯瑟琳", "旅行", 1, limit=20),
        "speaker_keyword",
    )
    assert dlg_total == 2 and {r[0] for r in dlg_rows} == {100, 105}

    # Verify structural plan choices and EXPLAIN QUERY PLAN output do not scan textMap by lang.
    sql_map = {label: sql.lower() for label, sql, _plan in captured_sqls}
    plan_map = {label: plan.lower() for label, _sql, plan in captured_sqls}
    assert "as materialized" in sql_map["npc"]
    assert "cross join" in sql_map["quest_content"] or " match " in sql_map["quest_content"]
    assert "cross join" in sql_map["quest_content_short"]
    assert "cross join" in sql_map["speaker_keyword"] or " match " in sql_map["speaker_keyword"]
    assert "textmap_fts" in plan_map["quest_content"]
    for label, plan_text in plan_map.items():
        assert "textmap_lang_index" not in plan_text, f"{label} plan scanned textMap_lang_index:\n{plan_text}"
    conn.close()


def test_search_name_entries_and_search_catalog_batch_and_single_pass_parity(monkeypatch):
    connection = sqlite3.connect(":memory:")
    _create_full_schema(connection)
    _seed_sample_data(connection)
    helper, common = _bind_helper_to_connection(monkeypatch, connection)

    batch_calls: list[list[int]] = []
    orig_chapter_batch = helper.getQuestChapterNamesBatch

    def spy_chapter_batch(quest_ids, lang_code):
        batch_calls.append(list(quest_ids))
        return orig_chapter_batch(quest_ids, lang_code)

    monkeypatch.setattr(helper, "getQuestChapterNamesBatch", spy_chapter_batch)
    batched_names = common.searchNameEntries("风起地", 1)
    assert len(batch_calls) == 1
    assert batched_names["quests"][0]["chapterName"] == "第一幕 · 序章"

    # Compare with monkeypatched getQuestChapterName fallback path.
    orig_get_chapter = helper.getQuestChapterName
    monkeypatch.setattr(helper, "getQuestChapterName", lambda qid, lang: orig_get_chapter(qid, lang))
    fallback_names = common.searchNameEntries("风起地", 1)
    assert batched_names == fallback_names

    # Verify searchCatalog uses single-pass WithCount and matches legacy pair.
    with_count_calls = 0
    orig_with_count = helper.selectCatalogEntitiesWithCount

    def spy_catalog_with_count(*args, **kwargs):
        nonlocal with_count_calls
        with_count_calls += 1
        return orig_with_count(*args, **kwargs)

    monkeypatch.setattr(helper, "selectCatalogEntitiesWithCount", spy_catalog_with_count)
    fast_cat = common.searchCatalog("风神", 1)
    assert with_count_calls == 1
    assert fast_cat["total"] == 1
    assert fast_cat["contents"][0]["entityId"] == 88

    orig_cat = helper.selectCatalogEntities
    monkeypatch.setattr(helper, "selectCatalogEntities", lambda *a, **kw: orig_cat(*a, **kw))
    legacy_cat = common.searchCatalog("风神", 1)
    assert fast_cat["total"] == legacy_cat["total"]
    assert fast_cat["contents"] == legacy_cat["contents"]
    connection.close()


def test_wsgi_warmup_respects_disable_env_and_runs_safely(monkeypatch):
    import controllers.common as common
    import databaseHelper

    monkeypatch.setenv("GTS_DISABLE_WARMUP", "1")
    import wsgi
    called = []
    monkeypatch.setattr(databaseHelper, "warmup_runtime_caches", lambda: called.append("db"))
    monkeypatch.setattr(common, "getAvailableVersionFilters", lambda: called.append("ver"))

    wsgi._start_warmup_thread()
    assert called == []

    wsgi._warmup_runtime()
    assert called == ["db", "ver"]


def test_short_literal_fts_accelerates_two_char_and_multi_term_with_like_parity(tmp_path, monkeypatch):
    from dbBuild.build_short_literal_fts import build as build_short_fts
    import short_literal_cache

    short_literal_cache._clear_meta_cache()
    db_file = tmp_path / "data.db"
    setup_conn = sqlite3.connect(db_file)
    _create_full_schema(setup_conn)
    _seed_sample_data(setup_conn)
    setup_conn.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?, ?, ?, ?, ?)",
        [
            (6001, "天边的空岛之上有琴声", 1, 1, 1),
            (6002, "星辰闪烁在深渊上方", 1, 1, 1),
            (6003, "cat and hat", 4, 1, 1),
            (6004, "旅人の詩", 9, 1, 1),
            (6005, "旅行", 2, 1, 1),
        ],
    )
    setup_conn.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    setup_conn.commit()
    setup_conn.close()

    sidecar_file = tmp_path / "short_literal_fts.db"
    stats = build_short_fts(db_file, sidecar_file)
    assert stats["rows"] > 0

    conn = sqlite3.connect(db_file)
    try:
        helper, common = _bind_helper_to_connection(monkeypatch, conn)
        assert short_literal_cache.warmup_short_fts(conn) is True

        def capture_plans_and_run(fn):
            trace_log: list[str] = []
            conn.set_trace_callback(trace_log.append)
            try:
                res = fn()
            finally:
                conn.set_trace_callback(None)
            selects = [
                s for s in trace_log
                if s.lstrip().upper().startswith(("WITH ", "SELECT "))
                and "sqlite_master" not in s.lower()
                and "pragma_" not in s.lower()
                and "cache_meta" not in s.lower()
            ]
            plans = "\n".join(
                str(row[3]).lower()
                for sql_stmt in selects
                for row in conn.execute("EXPLAIN QUERY PLAN " + sql_stmt).fetchall()
            )
            return res, plans

        # 2-char CHS, EN, JP, space multi-term, unknown, catalog, quest content, speaker+keyword
        queries = [
            ("chs2", lambda: helper.selectTextMapFromKeywordPagedWithCount("旅行", 1, 20, 0)),
            ("en2", lambda: helper.selectTextMapFromKeywordPagedWithCount("at", 4, 20, 0)),
            ("jp2", lambda: helper.selectTextMapFromKeywordPagedWithCount("旅人", 9, 20, 0)),
            ("space2", lambda: helper.selectTextMapFromKeywordPagedWithCount("星辰 深渊", 1, 20, 0)),
            ("unknown2", lambda: helper.selectUnknownTextMapFromKeywordPagedWithCount("旅行", 1, 20, 0)),
            ("catalog2", lambda: helper.selectCatalogEntitiesWithCount("风神", 1, limit=20)),
            ("quest2", lambda: helper.selectQuestByContentKeyword("风起", 1)),
            ("speaker2", lambda: helper.selectDialogueByTalkerAndKeywordWithCount("凯瑟琳", "旅行", 1, limit=20)),
        ]

        fast_results = {}
        for label, fn in queries:
            res, plans = capture_plans_and_run(fn)
            assert "short_fts" in plans, f"{label} did not use genshin_short.short_fts:\n{plans}"
            assert not re.search(r"textmap_lang_index\s*\(\s*lang=\?\s*\)", plans), (
                f"{label} scanned all rows of textMap_lang_index:\n{plans}"
            )
            fast_results[label] = res

        # Verify contiguous single-term rule vs space-enabled fuzzy matching.
        assert helper.selectTextMapFromKeywordPagedWithCount("天琴", 1, 20, 0) == ([], 0)
        assert helper.selectTextMapFromKeywordPagedWithCount("星辰深渊", 1, 20, 0) == ([], 0)
        sky_rows, sky_total = helper.selectTextMapFromKeywordPagedWithCount("天 琴", 1, 20, 0)
        assert sky_total == 1 and sky_rows[0][0] == 6001

        # Compare every result against LIKE-only mode.
        monkeypatch.setattr(helper.config, "getEnableTextMapFts", lambda: False)
        for label, fn in queries:
            assert fn() == fast_results[label], f"Parity mismatch for {label}"

        # Non-indexed language (CHT=2) falls back to LIKE safely even with sidecar attached.
        monkeypatch.setattr(helper.config, "getEnableTextMapFts", lambda: True)
        cht_rows, cht_total = helper.selectTextMapFromKeywordPagedWithCount("旅行", 2, 20, 0)
        assert cht_total == 1 and [r[0] for r in cht_rows] == [6005]
    finally:
        short_literal_cache._clear_meta_cache()
        conn.close()


def test_short_literal_fts_legacy_meta_and_safe_fallbacks(tmp_path, monkeypatch):
    from dbBuild.build_short_literal_fts import build as build_short_fts
    import short_literal_cache

    short_literal_cache._clear_meta_cache()
    db_file = tmp_path / "data.db"
    setup_conn = sqlite3.connect(db_file)
    _create_full_schema(setup_conn)
    _seed_sample_data(setup_conn)
    setup_conn.execute("INSERT INTO textMap(hash, content, lang) VALUES (6010, '旅行', 2)")
    setup_conn.commit()
    setup_conn.close()

    custom_sidecar = tmp_path / "alt" / "custom_short.db"
    build_short_fts(db_file, custom_sidecar)
    # Remove the optional `languages` metadata key to mimic older Bot-built sidecars.
    meta_conn = sqlite3.connect(custom_sidecar)
    meta_conn.execute("DELETE FROM cache_meta WHERE k='languages'")
    meta_conn.commit()
    meta_conn.close()

    monkeypatch.setenv("GTS_SHORT_FTS_PATH", str(custom_sidecar))
    conn = sqlite3.connect(db_file)
    try:
        helper, _common = _bind_helper_to_connection(monkeypatch, conn)

        # Default languages {1, 4, 9} works without `languages` key in cache_meta.
        assert short_literal_cache.resolve_short_fts_query(conn, "旅行", 1) is not None
        assert short_literal_cache.resolve_short_fts_query(conn, "旅行", 2) is None

        # Non-empty WAL triggers fallback & detach.
        wal_file = tmp_path / "data.db-wal"
        wal_file.write_bytes(b"1234")
        assert short_literal_cache.resolve_short_fts_query(conn, "旅行", 1) is None
        assert "genshin_short" not in {r[1] for r in conn.execute("PRAGMA database_list")}
        wal_file.unlink()

        # Re-attaches cleanly after WAL clears, then invalidates if mutated via total_changes > 0.
        assert short_literal_cache.resolve_short_fts_query(conn, "旅行", 1) is not None
        conn.execute("INSERT INTO textMap(hash, content, lang) VALUES (6011, '新旅行', 1)")
        assert conn.total_changes > 0
        assert short_literal_cache.resolve_short_fts_query(conn, "旅行", 1) is None
        assert "genshin_short" not in {r[1] for r in conn.execute("PRAGMA database_list")}
    finally:
        conn.close()

    # Corrupted sidecar with valid cache_meta falls back cleanly in _execute_with_fallback.
    short_literal_cache._clear_meta_cache()
    bad_conn = sqlite3.connect(custom_sidecar)
    bad_conn.execute("DROP TABLE short_fts")
    bad_conn.execute("CREATE TABLE short_fts(grams TEXT)")
    bad_conn.commit()
    bad_conn.close()

    conn2 = sqlite3.connect(db_file)
    try:
        helper, _common = _bind_helper_to_connection(monkeypatch, conn2)
        assert short_literal_cache.resolve_short_fts_query(conn2, "旅行", 1) is None

        # Verify _execute_with_fallback detaches genshin_short when an attached FTS query fails.
        conn2.execute(
            "ATTACH DATABASE ? AS genshin_short",
            (custom_sidecar.as_uri() + "?mode=ro&immutable=1",),
        )
        with conn2:
            pass
        cur = conn2.cursor()
        helper._execute_with_fallback(
            cur,
            "SELECT rowid FROM genshin_short.short_fts WHERE short_fts MATCH ?",
            ['"e69785e8a18c"'],
            "SELECT COUNT(*) FROM textMap WHERE lang=?",
            [1],
        )
        assert "genshin_short" not in {r[1] for r in conn2.execute("PRAGMA database_list")}
        cur.close()

        # Missing sidecar or :memory: DBs degrade cleanly to LIKE without errors.
        custom_sidecar.unlink()
        short_literal_cache._clear_meta_cache()
        assert short_literal_cache.resolve_short_fts_query(conn2, "旅行", 1) is None
        rows, total = helper.selectTextMapFromKeywordPagedWithCount("旅行", 1, 20, 0)
        assert total == 7 and len(rows) == 7
    finally:
        short_literal_cache._clear_meta_cache()
        conn2.close()

    # Mismatched source metadata cleanly disables sidecar.
    build_short_fts(db_file, custom_sidecar)
    tamper = sqlite3.connect(custom_sidecar)
    tamper.execute("UPDATE cache_meta SET v='999' WHERE k='source_size'")
    tamper.commit()
    tamper.close()

    short_literal_cache._clear_meta_cache()
    conn3 = sqlite3.connect(db_file)
    try:
        helper, _common = _bind_helper_to_connection(monkeypatch, conn3)
        assert short_literal_cache.resolve_short_fts_query(conn3, "旅行", 1) is None
        rows, total = helper.selectTextMapFromKeywordPagedWithCount("旅行", 1, 20, 0)
        assert total == 7 and len(rows) == 7
    finally:
        short_literal_cache._clear_meta_cache()
        conn3.close()

    mem_conn = sqlite3.connect(":memory:")
    assert short_literal_cache.resolve_short_fts_query(mem_conn, "旅行", 1) is None
    mem_conn.close()


def test_compact_text_cache_accelerates_single_char_and_multi_term_with_parity(tmp_path, monkeypatch):
    from dbBuild.build_compact_text_cache import build as build_compact_text
    import compact_text_cache

    compact_text_cache._clear_meta_cache()
    db_file = tmp_path / "data.db"
    setup_conn = sqlite3.connect(db_file)
    _create_full_schema(setup_conn)
    _seed_sample_data(setup_conn)
    setup_conn.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?, ?, ?, ?, ?)",
        [
            (7001, "风", 1, 1, 1),
            (7002, "风之翼展开", 1, 1, 2),
            (7003, "微风拂过", 1, 2, 2),
            (7004, "的", 1, 1, 1),
            (7005, "天边的空岛之上有琴声", 1, 1, 1),
            (7006, "Alpha note", 4, 1, 1),
            (7007, "beta line", 4, 1, 2),
            (7008, "旅人の詩", 9, 1, 1),
            (7009, "風の記憶", 9, 1, 1),
            (7010, "风的记忆", 2, 1, 1),
        ],
    )
    setup_conn.execute("INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId) VALUES (70, 'TALK_ROLE_NPC', 1001, 500, 7001, 0)")
    setup_conn.execute("INSERT INTO voice(dialogueId, voicePath) VALUES (70, 'VO_7001.wem')")
    setup_conn.execute("INSERT INTO quest_hash_map(questId, hash, source_type) VALUES (700, 7002, 'dialog')")
    setup_conn.execute("INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (500, 0, 70)")
    setup_conn.execute("INSERT INTO text_source_entity(entity_id, source_type_code, title_hash, text_hash, sub_category) VALUES (9001, 1, 7006, 7007, 0)")
    setup_conn.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    setup_conn.commit()
    setup_conn.close()

    sidecar_file = tmp_path / "compact_text_cache.db"
    stats = build_compact_text(db_file, sidecar_file)
    assert stats["rows"] > 0

    conn = sqlite3.connect(db_file)
    try:
        helper, _common = _bind_helper_to_connection(monkeypatch, conn)
        assert compact_text_cache.warmup_compact_text(conn) is True

        def capture_sql_and_run(fn):
            trace_log: list[str] = []
            conn.set_trace_callback(trace_log.append)
            try:
                res = fn()
            finally:
                conn.set_trace_callback(None)
            selects = [
                s for s in trace_log
                if s.lstrip().upper().startswith(("WITH ", "SELECT "))
                and "sqlite_master" not in s.lower()
                and "pragma_" not in s.lower()
                and "cache_meta" not in s.lower()
            ]
            plans = "\n".join(
                str(row[3]).lower()
                for sql_stmt in selects
                for row in conn.execute("EXPLAIN QUERY PLAN " + sql_stmt).fetchall()
            )
            return res, "\n".join(selects).lower(), plans

        queries = [
            ("chs_wind", lambda: helper.selectTextMapFromKeywordPagedWithCount("风", 1, 2, 0)),
            ("chs_wind_p2", lambda: helper.selectTextMapFromKeywordPagedWithCount("风", 1, 2, 2)),
            ("chs_wind_oob", lambda: helper.selectTextMapFromKeywordPagedWithCount("风", 1, 2, 500)),
            ("chs_de", lambda: helper.selectTextMapFromKeywordPagedWithCount("的", 1, 20, 0)),
            ("chs_1char_multi", lambda: helper.selectTextMapFromKeywordPagedWithCount("天 琴", 1, 20, 0)),
            ("en_a", lambda: helper.selectTextMapFromKeywordPagedWithCount("a", 4, 20, 0)),
            ("jp_no", lambda: helper.selectTextMapFromKeywordPagedWithCount("の", 9, 20, 0)),
            ("voice_with", lambda: helper.selectTextMapFromKeywordPagedWithCount("风", 1, 20, 0, voice_filter="with")),
            ("voice_without", lambda: helper.selectTextMapFromKeywordPagedWithCount("风", 1, 20, 0, voice_filter="without")),
            ("unknown_wind", lambda: helper.selectUnknownTextMapFromKeywordPagedWithCount("风", 1, 20, 0)),
            ("quest_wind", lambda: helper.selectTextMapFromKeywordBySourceTypeWithCount("风", 1, "quest", 20, 0)),
            ("dialogue_wind", lambda: helper.selectTextMapFromKeywordBySourceTypeWithCount("风", 1, "dialogue", 20, 0)),
            ("ver_created", lambda: helper.selectTextMapFromKeywordPagedWithCount("风", 1, 20, 0, created_version="4.0")),
            ("ver_updated", lambda: helper.selectTextMapFromKeywordPagedWithCount("风", 1, 20, 0, updated_version="4.0")),
            ("cnt_all", lambda: helper.countTextMapFromKeyword("风", 1)),
            ("cnt_voice", lambda: helper.countTextMapFromKeywordVoice("风", 1, "with")),
            ("quest_content_1char", lambda: helper.selectQuestByContentKeyword("风", 1)),
            ("catalog_a_en", lambda: helper.selectCatalogEntitiesWithCount("a", 4, limit=20)),
            ("catalog_wind_chs", lambda: helper.selectCatalogEntitiesWithCount("风", 1, limit=20)),
            ("catalog_wind_list", lambda: helper.selectCatalogEntities("风", 1, limit=20)),
            ("catalog_wind_cnt", lambda: helper.countCatalogEntities("风", 1)),
        ]

        fast_results = {}
        for label, fn in queries:
            res, sql_text, plans = capture_sql_and_run(fn)
            assert "genshin_compact.compact_text" in sql_text, f"{label} did not use genshin_compact.compact_text:\n{sql_text}"
            assert "using primary key (lang=?)" in plans or "compact_text" in plans, (
                f"{label} plan missing compact_text primary key scan:\n{plans}"
            )
            assert not re.search(r"textmap_lang_index\s*\(\s*lang=\?\s*\)", plans), (
                f"{label} scanned all rows of textMap_lang_index:\n{plans}"
            )
            fast_results[label] = res
        assert fast_results["catalog_a_en"][1] >= 1
        assert fast_results["catalog_wind_chs"][1] >= 1
        assert fast_results["catalog_wind_cnt"] >= 1

        # Compare every result against Layer 1 main.textMap (sidecar disabled) AND SQL-only path.
        monkeypatch.setattr(helper.config, "getEnableTextMapFts", lambda: False)
        for label, fn in queries:
            assert fn() == fast_results[label], f"Layer 1 parity mismatch for {label}"

        # Disable voiced/known-source in-memory sets to verify against raw SQL path too.
        monkeypatch.setattr(helper, "_get_voiced_text_hash_set", lambda *_a, **_kw: None)
        monkeypatch.setattr(helper, "_get_known_primary_source_hash_set", lambda *_a, **_kw: None)
        for label, fn in queries:
            assert fn() == fast_results[label], f"SQL fallback parity mismatch for {label}"

        # Non-indexed language (CHT=2) falls back to Layer 1 safely even with sidecar attached.
        monkeypatch.setattr(helper.config, "getEnableTextMapFts", lambda: True)
        cht_rows, cht_total = helper.selectTextMapFromKeywordPagedWithCount("风", 2, 20, 0)
        assert cht_total == 1 and [r[0] for r in cht_rows] == [7010]
    finally:
        compact_text_cache._clear_meta_cache()
        conn.close()


def test_compact_text_cache_safe_fallbacks(tmp_path, monkeypatch):
    from dbBuild.build_compact_text_cache import build as build_compact_text
    import compact_text_cache

    compact_text_cache._clear_meta_cache()
    db_file = tmp_path / "data.db"
    setup_conn = sqlite3.connect(db_file)
    _create_full_schema(setup_conn)
    _seed_sample_data(setup_conn)
    setup_conn.execute("INSERT INTO textMap(hash, content, lang) VALUES (7100, '风铃', 1)")
    setup_conn.commit()
    setup_conn.close()

    custom_sidecar = tmp_path / "alt" / "custom_compact.db"
    build_compact_text(db_file, custom_sidecar)
    monkeypatch.setenv("GTS_COMPACT_TEXT_PATH", str(custom_sidecar))

    conn = sqlite3.connect(db_file)
    try:
        helper, _common = _bind_helper_to_connection(monkeypatch, conn)

        assert compact_text_cache.resolve_compact_text_table(conn, 1) == "genshin_compact.compact_text"
        assert compact_text_cache.resolve_compact_text_table(conn, 2) is None

        # Non-empty WAL triggers fallback & detach.
        wal_file = tmp_path / "data.db-wal"
        wal_file.write_bytes(b"1234")
        assert compact_text_cache.resolve_compact_text_table(conn, 1) is None
        assert "genshin_compact" not in {r[1] for r in conn.execute("PRAGMA database_list")}
        wal_file.unlink()

        # Re-attaches cleanly after WAL clears, then invalidates on total_changes > 0.
        assert compact_text_cache.resolve_compact_text_table(conn, 1) == "genshin_compact.compact_text"
        conn.execute("INSERT INTO textMap(hash, content, lang) VALUES (7101, '风声', 1)")
        assert conn.total_changes > 0
        assert compact_text_cache.resolve_compact_text_table(conn, 1) is None
        assert "genshin_compact" not in {r[1] for r in conn.execute("PRAGMA database_list")}
    finally:
        conn.close()

    # Corrupted sidecar missing required columns falls back cleanly in resolve & _execute_with_fallback.
    compact_text_cache._clear_meta_cache()
    bad_conn = sqlite3.connect(custom_sidecar)
    bad_conn.execute("DROP TABLE compact_text")
    bad_conn.execute("CREATE TABLE compact_text(lang INTEGER, content TEXT)")
    bad_conn.commit()
    bad_conn.close()

    conn2 = sqlite3.connect(db_file)
    try:
        helper, _common = _bind_helper_to_connection(monkeypatch, conn2)
        assert compact_text_cache.resolve_compact_text_table(conn2, 1) is None

        conn2.execute(
            "ATTACH DATABASE ? AS genshin_compact",
            (custom_sidecar.as_uri() + "?mode=ro&immutable=1",),
        )
        cur = conn2.cursor()
        helper._execute_with_fallback(
            cur,
            "SELECT hash FROM genshin_compact.compact_text WHERE lang=?",
            [1],
            "SELECT COUNT(*) FROM textMap WHERE lang=?",
            [1],
        )
        assert "genshin_compact" not in {r[1] for r in conn2.execute("PRAGMA database_list")}
        cur.close()

        # Missing sidecar degrades cleanly without error.
        custom_sidecar.unlink()
        compact_text_cache._clear_meta_cache()
        assert compact_text_cache.resolve_compact_text_table(conn2, 1) is None
        rows, total = helper.selectTextMapFromKeywordPagedWithCount("风", 1, 20, 0)
        assert total == 5 and len(rows) == 5
    finally:
        compact_text_cache._clear_meta_cache()
        conn2.close()

    # Tampered metadata (has_fetter_voice != '1' or mismatched source_size) disables sidecar.
    build_compact_text(db_file, custom_sidecar)
    for meta_key, bad_val in (("has_fetter_voice", "0"), ("source_size", "999")):
        tamper = sqlite3.connect(custom_sidecar)
        tamper.execute("UPDATE cache_meta SET v=? WHERE k=?", (bad_val, meta_key))
        tamper.commit()
        tamper.close()

        compact_text_cache._clear_meta_cache()
        conn3 = sqlite3.connect(db_file)
        try:
            helper, _common = _bind_helper_to_connection(monkeypatch, conn3)
            assert compact_text_cache.resolve_compact_text_table(conn3, 1) is None
            rows, total = helper.selectTextMapFromKeywordPagedWithCount("风", 1, 20, 0)
            assert total == 5 and len(rows) == 5
        finally:
            compact_text_cache._clear_meta_cache()
            conn3.close()
        build_compact_text(db_file, custom_sidecar, force=True)

    mem_conn = sqlite3.connect(":memory:")
    assert compact_text_cache.resolve_compact_text_table(mem_conn, 1) is None
    mem_conn.close()


def test_instr_substring_parity_and_special_chars(monkeypatch):
    import databaseHelper as helper

    clause_zh, params_zh = helper._build_keyword_like_clause("tm.content", "风", 1)
    assert "instr(" in clause_zh and params_zh == ["风"]

    clause_en1, params_en1 = helper._build_keyword_like_clause("tm.content", "A", 4)
    assert "instr(" in clause_en1 and params_en1 == ["a", "A"]

    clause_en2, params_en2 = helper._build_keyword_like_clause("tm.content", "ab", 4)
    assert "LIKE" in clause_en2 and params_en2 == ["%ab%"]

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, content TEXT)")
    samples = [
        (1, None),
        (2, ""),
        (3, "100% pure_wind\\path"),
        (4, "Alpha beta"),
        (5, "BETA only"),
        (6, "風の旅人"),
        (7, "风起之地"),
        (8, "Übung test"),
        (9, "übung test"),
    ]
    conn.executemany("INSERT INTO items VALUES (?, ?)", samples)

    for kw, lang in [
        ("风", 1),
        ("の", 9),
        ("a", 4),
        ("A", 4),
        ("%", 4),
        ("_", 4),
        ("\\", 4),
        ("a _%", 4),
        ("风 起", 1),
        ("Ü", 5),
        ("ab", 4),
    ]:
        fast_sql, fast_params = helper._build_keyword_like_clause("content", kw, lang)
        fast_ids = [
            r[0] for r in conn.execute(f"SELECT id FROM items WHERE {fast_sql} ORDER BY id", fast_params)
        ]

        terms = helper.fts_tokenizer.split_search_terms(kw)
        if len(terms) > 1:
            like_sql = "(" + " AND ".join("content LIKE ? ESCAPE '\\'" for _ in terms) + ")"
            like_params = [f"%{helper._escape_like(t)}%" for t in terms]
        else:
            exact, _fuzzy = helper._build_like_patterns(kw, lang)
            like_sql, like_params = "(content LIKE ? ESCAPE '\\')", [exact]
        like_ids = [
            r[0] for r in conn.execute(f"SELECT id FROM items WHERE {like_sql} ORDER BY id", like_params)
        ]
        assert fast_ids == like_ids, f"Mismatch for kw={kw!r}: {fast_ids} vs {like_ids}"
    conn.close()


def test_quest_content_description_first_and_mixed_parity(monkeypatch):
    conn = sqlite3.connect(":memory:")
    _create_full_schema(conn)
    _seed_sample_data(conn)
    helper, _common = _bind_helper_to_connection(monkeypatch, conn)

    # Case 1: mixed description (best_rank=0), dialogue-only (best_rank=1), excluded (test) desc falling back to dialogue.
    conn.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?, ?, 1, 1, 1)",
        [
            (8001, "任务A标题",),
            (8002, "风之描述A",),
            (8003, "风之对白A",),
            (8004, "任务B标题",),
            (8005, "无关描述",),
            (8006, "风之对白B",),
            (8007, "（test）测试风描述",),
        ],
    )
    conn.executemany(
        "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, chapterId, source_type, created_version_id, updated_version_id) VALUES (?, ?, ?, 10, ?, 1, 1)",
        [
            (701, 8001, 8002, "WQ"),
            (702, 8004, 8005, "AQ"),
            (703, 8004, 8007, "WQ"),
        ],
    )
    conn.executemany(
        "INSERT INTO questTalk(questId, talkId, coopQuestId) VALUES (?, ?, 0)",
        [(701, 501), (702, 502), (703, 503)],
    )
    conn.executemany(
        "INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId) VALUES (?, 'TALK_ROLE_NPC', 1001, ?, ?, 0)",
        [(20, 501, 8003), (21, 502, 8006), (22, 503, 8006)],
    )
    conn.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?, 0, ?)",
        [(501, 20), (502, 21), (503, 22)],
    )

    mixed_rows = helper.selectQuestByContentKeyword("风", 1)
    assert [(r[0], r[7]) for r in mixed_rows] == [(700, 0), (701, 0), (702, 1), (703, 1)]
    assert mixed_rows[1][3] == "风之描述A"

    aq_rows = helper.selectQuestByContentKeyword("风", 1, source_type="AQ")
    assert [(r[0], r[7]) for r in aq_rows] == [(700, 0), (702, 1)]

    # Case 2: >=200 description matches triggers early return without scanning dialogues.
    bulk_text = [(90000 + i, f"批量风描述{i}") for i in range(210)]
    bulk_quests = [(1000 + i, 8001, 90000 + i, 10, "WQ", 1, 1) for i in range(210)]
    conn.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?, ?, 1, 1, 1)",
        bulk_text,
    )
    conn.executemany(
        "INSERT INTO quest(questId, titleTextMapHash, descTextMapHash, chapterId, source_type, created_version_id, updated_version_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
        bulk_quests,
    )

    trace_log: list[str] = []
    conn.set_trace_callback(trace_log.append)
    try:
        full_desc_rows = helper.selectQuestByContentKeyword("风", 1)
    finally:
        conn.set_trace_callback(None)
    assert len(full_desc_rows) == 200
    assert all(r[7] == 0 for r in full_desc_rows)
    assert not any("matched_dlg" in sql for sql in trace_log)
    conn.close()


def test_catalog_deferred_version_hydration_parity(monkeypatch):
    conn = sqlite3.connect(":memory:")
    _create_full_schema(conn)
    _seed_sample_data(conn)
    conn.execute("ALTER TABLE text_source_entity ADD COLUMN created_version_id INTEGER")
    # Entity 89: title in v4.0, body in v1.0, entity created_version_id NULL -> min is 1.0, updated is NULL (both created==updated)
    # Entity 90: title v1.0->4.0, entity created_version_id 1 -> created 1.0, updated 4.0
    # Entity 91: multi-title_hash where only one title_hash matches '风'
    conn.executemany(
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) VALUES (?, ?, 1, ?, ?)",
        [
            (9101, "风铃标题", 2, 2),
            (9102, "风铃正文", 1, 1),
            (9103, "风车标题", 1, 2),
            (9104, "风车正文", 1, 1),
            (9105, "风筝有效标题", 1, 2),
            (9106, "风筝正文", 1, 1),
        ],
    )
    conn.executemany(
        "INSERT INTO text_source_entity(entity_id, source_type_code, title_hash, text_hash, sub_category, created_version_id) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (89, 2, 9101, 9102, 1, None),
            (90, 2, 9103, 9104, 1, 1),
            (91, 3, 9105, 9106, 1, 1),
            (91, 3, 999999, 9106, 1, 2),
        ],
    )
    conn.execute("INSERT INTO textMap_fts(textMap_fts) VALUES ('rebuild')")
    helper, _common = _bind_helper_to_connection(monkeypatch, conn)

    deferred_rows, deferred_total = helper.selectCatalogEntitiesWithCount("风", 1, limit=20, offset=0)
    deferred_simple = helper.selectCatalogEntities("风", 1, limit=20, offset=0)
    assert deferred_rows == deferred_simple
    assert deferred_total == 4

    # Compare directly against inline full version-joined base query.
    base_sql, base_params = helper._build_catalog_entity_base_query(
        "风", 1, include_versions=True
    )
    full_sql = (
        base_sql
        + "SELECT base.entity_id, base.source_type_code, base.title_hash, base.sub_category, "
        + "base.title_text, vdc.raw_version, vdu.raw_version "
        + "FROM catalog_final base "
        + "LEFT JOIN version_dim vdc ON vdc.id = base.created_version_id "
        + "LEFT JOIN version_dim vdu ON vdu.id = base.updated_version_id "
        + "ORDER BY length(coalesce(base.title_text, '')), coalesce(base.title_text, '') LIMIT 20 OFFSET 0"
    )
    full_rows = conn.execute(full_sql, base_params).fetchall()
    assert deferred_rows == full_rows
    conn.close()

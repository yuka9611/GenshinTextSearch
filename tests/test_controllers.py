"""Tests for selected pure/internal logic in the controllers package."""
import sqlite3

import pytest

import controllers.common as controllers


def _patch_avatar_story_dependencies(
    monkeypatch,
    *,
    avatar_name="琴",
    story_rows=None,
    search_rows=None,
    title_map=None,
    translates_map=None,
    version_map=None,
):
    monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1])
    monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
    monkeypatch.setattr(controllers.databaseHelper, "getCharterName", lambda avatar_id, lang_code: avatar_name)
    if story_rows is not None:
        monkeypatch.setattr(controllers.databaseHelper, "selectAvatarStories", lambda avatar_id: story_rows)
    if search_rows is not None:
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectAvatarStoryItemsByFilters",
            lambda *args, **kwargs: search_rows,
        )

    resolved_titles = dict(title_map or {})
    resolved_translates = dict(translates_map or {})
    resolved_versions = dict(version_map or {})

    monkeypatch.setattr(
        controllers,
        "_get_text_map_content_with_fallback",
        lambda text_hash, lang_code, langs: resolved_titles.get(text_hash),
    )
    monkeypatch.setattr(
        controllers,
        "_build_text_map_translates",
        lambda text_hash, langs: resolved_translates.get(text_hash, {}),
    )
    monkeypatch.setattr(
        controllers.databaseHelper,
        "getTextMapVersionInfo",
        lambda text_hash, lang_code: resolved_versions.get(text_hash),
    )


# ---------------------------------------------------------------------------
# source_type filter helpers
# ---------------------------------------------------------------------------

class TestSourceTypeFilter:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            (None, None),
            ("", None),
            ("all", None),
            ("  all  ", None),
            ("未归类", "unknown"),
            ("角色语音", "voice"),
            ("角色故事", "story"),
            ("武器", "weapon"),
            ("装扮", "dressing"),
            ("outfit", "dressing"),
            ("costume", "costume"),
        ],
    )
    def test_normalize_source_type_filter(self, raw, expected):
        assert controllers._normalize_source_type_filter(raw) == expected

    def test_matches_source_type_filter_none_always_true(self):
        entry = {"primarySource": {"sourceType": "weapon"}}
        assert controllers._matches_source_type_filter(entry, None) is True

    def test_matches_source_type_filter_exact_match(self):
        entry = {"primarySource": {"sourceType": "weapon"}}
        assert controllers._matches_source_type_filter(entry, "weapon") is True
        assert controllers._matches_source_type_filter(entry, "food") is False

    def test_matches_source_type_filter_story(self):
        entry = {"primarySource": {"sourceType": "story"}}
        assert controllers._matches_source_type_filter(entry, "story") is True
        assert controllers._matches_source_type_filter(entry, "voice") is False

    def test_matches_source_type_filter_costume_supports_suit(self):
        entry1 = {"primarySource": {"sourceType": "costume"}}
        entry2 = {"primarySource": {"sourceType": "suit"}}
        assert controllers._matches_source_type_filter(entry1, "costume") is True
        assert controllers._matches_source_type_filter(entry2, "costume") is True

    @pytest.mark.parametrize(
        "source_type",
        ["qianxing_emoji", "qianxing_pose", "qianxing_effect", "qianxing_hall"],
    )
    def test_matches_source_type_filter_costume_supports_qianxing_internal_types(self, source_type):
        entry = {"primarySource": {"sourceType": source_type}}
        assert controllers._matches_source_type_filter(entry, "costume") is True

    def test_matches_source_type_filter_unknown_uses_known_source_flag(self):
        assert controllers._matches_source_type_filter({"hash": 101}, "unknown") is True
        assert controllers._matches_source_type_filter(
            {"primarySource": {"sourceType": "unknown"}, "_hasKnownPrimarySource": True},
            "unknown",
        ) is False

    def test_filter_entries_by_source_type(self):
        entries = [
            {"id": 1, "primarySource": {"sourceType": "weapon"}},
            {"id": 2, "primarySource": {"sourceType": "food"}},
            {"id": 3, "primarySource": {"sourceType": "weapon"}},
        ]
        filtered = controllers._filter_entries_by_source_type(entries, "weapon")
        assert [x["id"] for x in filtered] == [1, 3]


class TestAvatarStorySources:
    def test_select_story_source_from_text_hash_builds_story_primary_source(self, monkeypatch):
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectStorySourcesByTextHash",
            lambda text_hash: [
                (10000003, 4201, 111, 222),
                (10000003, 4202, 333, 222),
            ],
        )
        monkeypatch.setattr(controllers.databaseHelper, "getCharterName", lambda avatar_id, lang_code: "琴")
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(
            controllers,
            "_get_text_map_content_with_fallback",
            lambda text_hash, lang_code, langs: {111: "故事一", 222: "未解锁故事"}.get(text_hash),
        )

        primary, origin, is_talk, source_count = controllers._select_story_source_from_text_hash(999, 1)

        assert primary["sourceType"] == "story"
        assert primary["title"] == "琴 · 故事一"
        assert primary["subtitle"] == "角色故事"
        assert origin == "琴 · 故事一"
        assert is_talk is False
        assert source_count == 2

    def test_enrich_primary_sources_prefers_story_override(self, monkeypatch):
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectStorySourcesByTextHash",
            lambda text_hash: [(10000003, 4201, 111, 222)],
        )
        monkeypatch.setattr(controllers.databaseHelper, "getCharterName", lambda avatar_id, lang_code: "琴")
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(
            controllers,
            "_get_text_map_content_with_fallback",
            lambda text_hash, lang_code, langs: {111: "故事一", 222: "未解锁故事"}.get(text_hash),
        )

        entry = {
            "hash": 999,
            "primarySource": {"sourceType": "dialogue", "title": "旧来源"},
            "_preferredSourceType": "story",
        }

        controllers._enrich_primary_sources([entry], 1)

        assert entry["primarySource"]["sourceType"] == "story"
        assert entry["origin"] == "琴 · 故事一"
        assert "_preferredSourceType" not in entry


class TestAvatarStoryResultPayloads:
    def test_get_avatar_stories_adds_story_primary_source(self, monkeypatch):
        _patch_avatar_story_dependencies(
            monkeypatch,
            story_rows=[(10, 111, None, 222, 301, None)],
            title_map={111: "故事一", 222: "未解锁故事"},
            translates_map={301: {"1": "故事内容"}},
            version_map={301: ("Version 4.0", "Version 4.2")},
        )

        result = controllers.getAvatarStories(10000003, searchLang=1)

        assert result["avatarId"] == 10000003
        assert result["avatarName"] == "琴"
        assert len(result["stories"]) == 1
        story = result["stories"][0]
        assert story["origin"] == "琴 · 故事一"
        assert story["storyTitle"] == "故事一"
        assert story["primarySource"] == {
            "sourceType": "story",
            "title": "琴 · 故事一",
            "subtitle": "角色故事",
            "detailQuery": {"kind": "text", "textHash": 301},
        }
        assert story["sourceCount"] == 1
        assert story["createdVersion"] == "4.0"
        assert story["updatedVersion"] == "4.2"

    def test_search_avatar_stories_by_filters_uses_locked_title_for_story_primary_source(self, monkeypatch):
        _patch_avatar_story_dependencies(
            monkeypatch,
            search_rows=[(10000003, 10, 111, 222, 301)],
            title_map={111: None, 222: "未解锁故事"},
            translates_map={301: {"1": "未解锁故事内容"}},
            version_map={301: ("Version 4.0", "Version 4.2")},
        )

        result = controllers.searchAvatarStoriesByFilters("未解锁", searchLang=1)

        assert len(result["stories"]) == 1
        story = result["stories"][0]
        assert story["origin"] == "琴 · 未解锁故事"
        assert story["storyTitle"] == "未解锁故事"
        assert story["primarySource"] == {
            "sourceType": "story",
            "title": "琴 · 未解锁故事",
            "subtitle": "角色故事",
            "detailQuery": {"kind": "text", "textHash": 301},
        }
        assert story["sourceCount"] == 1
        assert story["avatarId"] == 10000003
        assert story["avatarName"] == "琴"

    @pytest.mark.parametrize(
        ("avatar_name", "expected_origin"),
        [
            ("琴", "琴"),
            (None, "角色故事"),
        ],
    )
    def test_get_avatar_stories_falls_back_when_story_title_missing(self, monkeypatch, avatar_name, expected_origin):
        _patch_avatar_story_dependencies(
            monkeypatch,
            avatar_name=avatar_name,
            story_rows=[(10, None, None, None, 301, None)],
            translates_map={301: {"1": "故事内容"}},
            version_map={301: ("Version 4.0", "Version 4.2")},
        )

        result = controllers.getAvatarStories(10000003, searchLang=1)

        assert len(result["stories"]) == 1
        story = result["stories"][0]
        assert story["origin"] == expected_origin
        assert story["storyTitle"] == ""
        assert story["primarySource"] == {
            "sourceType": "story",
            "title": expected_origin,
            "subtitle": "角色故事",
            "detailQuery": {"kind": "text", "textHash": 301},
        }
        assert story["sourceCount"] == 1


class TestSpeakerSourceQueries:
    def test_handle_speaker_only_query_voice_without_filter_returns_empty(self):
        result, total = controllers._handle_speaker_only_query("琴", 1, 1, 20, "without", None, None, "voice")

        assert result == []
        assert total == 0

    def test_handle_speaker_only_query_story_with_filter_returns_empty(self):
        result, total = controllers._handle_speaker_only_query("琴", 1, 1, 20, "with", None, None, "story")

        assert result == []
        assert total == 0

    def test_handle_speaker_and_keyword_query_voice_uses_avatar_voice_branch(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectFetterBySpeakerAndKeyword",
            lambda *args, **kwargs: [(101, 10000003)],
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "countFetterBySpeakerAndKeyword",
            lambda *args, **kwargs: 1,
        )
        monkeypatch.setattr(controllers.databaseHelper, "getCharterName", lambda avatar_id, lang_code: "琴")
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda *args, **kwargs: {"hash": 101, "translates": {"1": "早上好"}, "voicePaths": ["vo_101.wem"]},
        )

        result, total = controllers._handle_speaker_and_keyword_query("琴", "早上", 1, 1, 20, "all", None, None, "voice")

        assert total == 1
        assert len(result) == 1
        assert result[0]["talker"] == "琴"
        assert result[0]["_preferredSourceType"] == "voice"

    def test_handle_speaker_and_keyword_query_story_uses_avatar_story_branch(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectAvatarStoryBySpeakerAndKeyword",
            lambda *args, **kwargs: [(202, 10000003)],
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "countAvatarStoryBySpeakerAndKeyword",
            lambda *args, **kwargs: 1,
        )
        monkeypatch.setattr(controllers.databaseHelper, "getCharterName", lambda avatar_id, lang_code: "琴")
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda *args, **kwargs: {"hash": 202, "translates": {"1": "故事内容"}, "voicePaths": []},
        )

        result, total = controllers._handle_speaker_and_keyword_query("琴", "故事", 1, 1, 20, "all", None, None, "story")

        assert total == 1
        assert len(result) == 1
        assert result[0]["talker"] == "琴"
        assert result[0]["_preferredSourceType"] == "story"


# ---------------------------------------------------------------------------
# match/rank helpers
# ---------------------------------------------------------------------------

class TestMatchRanking:
    def test_normalize_match_text_chinese_removes_spaces(self, monkeypatch):
        monkeypatch.setattr(controllers.databaseHelper, "CHINESE_LANG_CODES", {1, 2}, raising=False)
        assert controllers._normalize_match_text("你 好", 1) == "你好"

    def test_normalize_match_text_non_chinese_keeps_spaces_structure(self, monkeypatch):
        monkeypatch.setattr(controllers.databaseHelper, "CHINESE_LANG_CODES", {1, 2}, raising=False)
        assert controllers._normalize_match_text("Hello World", 4) == "hello world"

    def test_match_rank_exact(self, monkeypatch):
        monkeypatch.setattr(controllers.databaseHelper, "CHINESE_LANG_CODES", {1, 2}, raising=False)
        assert controllers._match_rank("hello", "hello", 4) == 0

    def test_match_rank_prefix(self, monkeypatch):
        monkeypatch.setattr(controllers.databaseHelper, "CHINESE_LANG_CODES", {1, 2}, raising=False)
        assert controllers._match_rank("hello world", "hello", 4) == 1

    def test_match_rank_contains(self, monkeypatch):
        monkeypatch.setattr(controllers.databaseHelper, "CHINESE_LANG_CODES", {1, 2}, raising=False)
        assert controllers._match_rank("say hello", "hello", 4) == 2

    def test_match_rank_no_match(self, monkeypatch):
        monkeypatch.setattr(controllers.databaseHelper, "CHINESE_LANG_CODES", {1, 2}, raising=False)
        assert controllers._match_rank("goodbye", "hello", 4) == 3

    def test_best_field_match_prefers_better_rank_then_index(self, monkeypatch):
        monkeypatch.setattr(controllers.databaseHelper, "CHINESE_LANG_CODES", {1, 2}, raising=False)
        values = ["zzz hello", "hello world", "hello"]
        # best should be exact match at index 2 -> rank 0
        best = controllers._best_field_match(values, "hello", 4)
        assert best[0] == 0
        assert best[1] == 2


# ---------------------------------------------------------------------------
# search result sorting / source marking
# ---------------------------------------------------------------------------

class TestSearchResultSorting:
    def test_sort_search_results_keeps_exact_hash_and_best_text_in_top_tier(self):
        entries = [
            {"hash": 200, "translates": {"4": "keyword"}, "voicePaths": ["vo_200"]},
            {"hash": 123, "translates": {"4": "other text"}, "voicePaths": []},
            {"hash": 300, "translates": {"4": "keyword suffix"}, "voicePaths": ["vo_300"], "primarySource": {"sourceType": "weapon"}},
        ]

        controllers._sort_search_results(entries, "keyword", 4, True, 123)

        assert [entry["hash"] for entry in entries] == [200, 123, 300]

    def test_sort_search_results_prefers_voice_then_source_then_hash(self):
        entries = [
            {"hash": 30, "translates": {"4": "keyword"}, "voicePaths": [], "primarySource": {"sourceType": "weapon"}},
            {"hash": 10, "translates": {"4": "keyword"}, "voicePaths": ["vo_10"]},
            {"hash": 20, "translates": {"4": "keyword"}, "voicePaths": ["vo_20"], "_hasKnownPrimarySource": True},
        ]

        controllers._sort_search_results(entries, "keyword", 4, False, None)

        assert [entry["hash"] for entry in entries] == [20, 10, 30]

    def test_sort_search_results_uses_hash_as_final_tiebreaker_and_ignores_talk_and_length(self):
        entries = [
            {"hash": 30, "isTalk": True, "translates": {"4": "alpha key omega"}, "voicePaths": [], "_hasKnownPrimarySource": True},
            {"hash": 10, "isTalk": False, "translates": {"4": "beta key gamma"}, "voicePaths": [], "_hasKnownPrimarySource": True},
            {"hash": 20, "isTalk": True, "translates": {"4": "delta key epsilon zeta"}, "voicePaths": [], "_hasKnownPrimarySource": True},
        ]

        controllers._sort_search_results(entries, "key", 4, False, None)

        assert [entry["hash"] for entry in entries] == [10, 20, 30]

    def test_mark_entries_with_known_primary_source_marks_only_missing_sources(self, monkeypatch):
        calls = []

        def fake_select_text_hashes_with_known_primary_source(text_hashes):
            calls.append(list(text_hashes))
            return {222, 333}

        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextHashesWithKnownPrimarySource",
            fake_select_text_hashes_with_known_primary_source,
        )

        entries = [
            {"hash": 111, "primarySource": {"sourceType": "weapon"}},
            {"hash": 222, "translates": {"4": "foo"}},
            {"hash": "333", "primarySource": {"sourceType": "unknown"}},
            {"hash": "bad", "translates": {"4": "bar"}},
            {"hash": 222, "translates": {"4": "dup"}},
        ]

        controllers._mark_entries_with_known_primary_source(entries)

        assert calls == [[222, 333]]
        assert "_hasKnownPrimarySource" not in entries[0]
        assert entries[1]["_hasKnownPrimarySource"] is True
        assert entries[2]["_hasKnownPrimarySource"] is True
        assert "_hasKnownPrimarySource" not in entries[3]
        assert entries[4]["_hasKnownPrimarySource"] is True

    def test_ranked_handler_marks_sources_before_pagination(self, monkeypatch):
        monkeypatch.setattr(controllers, "_count_textmap_from_keyword_cached", lambda *args, **kwargs: 2)
        monkeypatch.setattr(controllers, "_count_subtitle_from_keyword_cached", lambda *args, **kwargs: 0)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextMapFromKeywordPaged",
            lambda *args, **kwargs: [(1, None, None, None), (2, None, None, None)],
        )
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda text_hash, *args, **kwargs: {
                "hash": text_hash,
                "translates": {"4": "keyword"},
                "voicePaths": [],
            },
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextHashesWithKnownPrimarySource",
            lambda text_hashes: {2},
        )

        result, total = controllers._handle_all_voice_filter_ranked(
            "keyword",
            "keyword",
            4,
            1,
            1,
            None,
            False,
            None,
            False,
            set(),
            [4],
            1,
            None,
            [],
            {},
            {},
            None,
            None,
            None,
        )

        assert total == 2
        assert [entry["hash"] for entry in result] == [2]
        assert result[0]["_hasKnownPrimarySource"] is True

    def test_unknown_filter_matches_missing_source_but_excludes_known_flagged_entries(self):
        entries = [
            {"hash": 10, "translates": {"4": "keyword"}},
            {"hash": 20, "translates": {"4": "keyword"}, "_hasKnownPrimarySource": True},
            {"hash": 30, "translates": {"4": "keyword"}, "primarySource": {"sourceType": "unknown"}},
        ]

        filtered = controllers._filter_entries_by_source_type(entries, "unknown")

        assert [entry["hash"] for entry in filtered] == [10, 30]

    def test_unknown_ranked_handler_scans_beyond_known_prefix_and_keeps_full_total(self, monkeypatch):
        known_hashes = set(range(1, 206))
        all_hashes = list(range(1, 211))

        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextMapFromKeywordPaged",
            lambda *args, **kwargs: [
                (text_hash, None, None, None)
                for text_hash in all_hashes[kwargs.get("offset", args[3]):kwargs.get("offset", args[3]) + kwargs.get("limit", args[2])]
            ],
        )
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda text_hash, *args, **kwargs: {
                "hash": text_hash,
                "translates": {"4": "keyword"},
                "voicePaths": [],
            },
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextHashesWithKnownPrimarySource",
            lambda text_hashes: set(text_hashes) & known_hashes,
        )

        result, total = controllers._handle_all_voice_filter_ranked(
            "keyword",
            "keyword",
            4,
            1,
            1,
            None,
            False,
            None,
            False,
            set(),
            [4],
            1,
            None,
            [],
            {},
            {},
            None,
            None,
            "unknown",
        )

        assert total == 5
        assert [entry["hash"] for entry in result] == [206]

    def test_unknown_ranked_handler_without_voice_reuses_unknown_scan(self, monkeypatch):
        known_hashes = set(range(1, 204))
        all_hashes = list(range(1, 206))
        seen_voice_filters = []

        def fake_select_textmap(*args, **kwargs):
            limit = kwargs.get("limit", args[2])
            offset = kwargs.get("offset", args[3])
            voice_filter = kwargs.get("voice_filter", args[5] if len(args) > 5 else None)
            seen_voice_filters.append(voice_filter)
            return [
                (text_hash, None, None, None)
                for text_hash in all_hashes[offset:offset + limit]
            ]

        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextMapFromKeywordPaged",
            fake_select_textmap,
        )
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda text_hash, *args, **kwargs: {
                "hash": text_hash,
                "translates": {"4": "keyword"},
                "voicePaths": [],
            },
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextHashesWithKnownPrimarySource",
            lambda text_hashes: set(text_hashes) & known_hashes,
        )

        result, total = controllers._handle_specific_voice_filter_ranked(
            "keyword",
            "keyword",
            4,
            1,
            1,
            "without",
            None,
            False,
            None,
            False,
            set(),
            [4],
            1,
            None,
            [],
            {},
            {},
            None,
            None,
            "unknown",
        )

        assert seen_voice_filters == ["without", "without"]
        assert total == 2
        assert [entry["hash"] for entry in result] == [204]

    def test_enrich_primary_sources_removes_internal_known_source_flag(self):
        entry = {
            "hash": 999,
            "primarySource": {"sourceType": "weapon", "title": "武器"},
            "_hasKnownPrimarySource": True,
        }

        controllers._enrich_primary_sources([entry], 1)

        assert "_hasKnownPrimarySource" not in entry


# ---------------------------------------------------------------------------
# database helper known source lookup
# ---------------------------------------------------------------------------

class TestKnownPrimarySourceLookup:
    def test_select_text_hashes_with_known_primary_source_merges_source_tables(self, monkeypatch):
        conn = sqlite3.connect(":memory:")
        table_cache = dict(controllers.databaseHelper._CACHE["table"])
        column_cache = dict(controllers.databaseHelper._CACHE["column"])
        try:
            conn.executescript(
                """
                CREATE TABLE dialogue (textHash INTEGER);
                CREATE TABLE fetters (
                    voiceFileTextTextMapHash INTEGER,
                    avatarId INTEGER,
                    voiceTitleTextMapHash INTEGER,
                    voiceFile INTEGER
                );
                CREATE TABLE fetterStory (
                    avatarId INTEGER,
                    fetterId INTEGER,
                    storyTitleTextMapHash INTEGER,
                    storyTitleLockedTextMapHash INTEGER,
                    storyContextTextMapHash INTEGER,
                    storyTitle2TextMapHash INTEGER,
                    storyContext2TextMapHash INTEGER
                );
                CREATE TABLE quest_hash_map (
                    questId INTEGER,
                    hash INTEGER,
                    source_type TEXT
                );
                CREATE TABLE text_source_entity (
                    text_hash INTEGER,
                    source_type_code INTEGER,
                    entity_id INTEGER,
                    title_hash INTEGER,
                    extra INTEGER,
                    sub_category INTEGER
                );
                CREATE TABLE readable (
                    fileName TEXT,
                    titleTextMapHash INTEGER,
                    readableId INTEGER
                );
                """
            )
            conn.execute("INSERT INTO dialogue(textHash) VALUES (101)")
            conn.execute(
                "INSERT INTO fetters(voiceFileTextTextMapHash, avatarId, voiceTitleTextMapHash, voiceFile) VALUES (202, 1, 2, 3)"
            )
            conn.execute(
                "INSERT INTO fetterStory(avatarId, fetterId, storyTitleTextMapHash, storyTitleLockedTextMapHash, storyContextTextMapHash, storyTitle2TextMapHash, storyContext2TextMapHash) "
                "VALUES (1, 10, 11, 12, 303, 13, NULL)"
            )
            conn.execute(
                "INSERT INTO quest_hash_map(questId, hash, source_type) VALUES (1, 404, 'title')"
            )
            conn.execute(
                "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category) VALUES (505, 1, 1, 606, 0, 0)"
            )
            conn.execute(
                "INSERT INTO readable(fileName, titleTextMapHash, readableId) VALUES ('book.json', 707, 1)"
            )

            monkeypatch.setattr(controllers.databaseHelper, "conn", conn)
            controllers.databaseHelper._CACHE["table"].clear()
            controllers.databaseHelper._CACHE["column"].clear()

            result = controllers.databaseHelper.selectTextHashesWithKnownPrimarySource(
                [101, 202, 303, 404, 505, 606, 707, 999]
            )

            assert result == {101, 202, 303, 404, 505, 606, 707}
        finally:
            controllers.databaseHelper._CACHE["table"].clear()
            controllers.databaseHelper._CACHE["table"].update(table_cache)
            controllers.databaseHelper._CACHE["column"].clear()
            controllers.databaseHelper._CACHE["column"].update(column_cache)
            conn.close()


# ---------------------------------------------------------------------------
# paginate
# ---------------------------------------------------------------------------

class TestPaginate:
    def test_paginate_basic(self):
        entries = [{"id": i} for i in range(1, 21)]
        page_entries, total = controllers._paginate(entries, page=2, page_size=5)
        assert total == 20
        assert [x["id"] for x in page_entries] == [6, 7, 8, 9, 10]

    def test_paginate_invalid_page_defaults_to_1(self):
        entries = [{"id": i} for i in range(1, 6)]
        page_entries, total = controllers._paginate(entries, page=0, page_size=2)
        assert total == 5
        assert [x["id"] for x in page_entries] == [1, 2]

    def test_paginate_invalid_page_size_defaults_to_50(self):
        entries = [{"id": i} for i in range(1, 6)]
        page_entries, _ = controllers._paginate(entries, page=1, page_size=0)
        assert len(page_entries) == 5

    def test_paginate_with_external_total(self):
        entries = [{"id": 1}, {"id": 2}]
        page_entries, total = controllers._paginate(entries, page=1, page_size=1, total=999)
        assert len(page_entries) == 1
        assert total == 999


# ---------------------------------------------------------------------------
# version helpers
# ---------------------------------------------------------------------------

class TestVersionHelpers:
    def test_extract_version_tag(self):
        assert controllers._extract_version_tag("Version 4.7.0") == "4.7"
        assert controllers._extract_version_tag(" 2.0 ") == "2.0"

    def test_extract_version_tag_invalid(self):
        assert controllers._extract_version_tag(None) is None
        assert controllers._extract_version_tag("") is None
        assert controllers._extract_version_tag("no version") is None

    def test_normalize_version_filter(self):
        assert controllers._normalize_version_filter("Version 4.8.1") == "4.8"
        assert controllers._normalize_version_filter("custom-tag") == "custom-tag"
        assert controllers._normalize_version_filter(None) is None

    def test_build_version_fields(self):
        fields = controllers._build_version_fields("Version 1.2", "Version 2.3")
        assert fields["createdVersion"] == "1.2"
        assert fields["updatedVersion"] == "2.3"

    def test_entry_version_match_created(self):
        entry = {"createdVersion": "4.0", "updatedVersion": "4.1"}
        assert controllers._entry_version_match(entry, "4.0", None) is True
        assert controllers._entry_version_match(entry, "4.2", None) is False

    def test_entry_version_match_updated_excludes_equal_created_updated(self):
        entry = {"createdVersion": "4.0", "updatedVersion": "4.0"}
        assert controllers._entry_version_match(entry, None, "4.0") is False

    def test_entry_version_match_updated_normal(self):
        entry = {"createdVersion": "4.0", "updatedVersion": "4.1"}
        assert controllers._entry_version_match(entry, None, "4.1") is True
        assert controllers._entry_version_match(entry, None, "4.2") is False


class TestQuestDialogueVersions:
    def test_get_quest_dialogues_includes_quest_version_fields(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(controllers.databaseHelper, "getQuestName", lambda quest_id, lang_code: "风起鹤归")
        monkeypatch.setattr(controllers.databaseHelper, "getQuestDescription", lambda quest_id, lang_code: "任务描述")
        monkeypatch.setattr(controllers.databaseHelper, "getQuestStepTitleMap", lambda quest_id, lang_code: {1001: "第一幕"})
        version_calls = []
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getQuestVersionInfo",
            lambda quest_id, lang_code=None: version_calls.append((quest_id, lang_code)) or ("Version 4.4", "Version 4.7"),
        )
        monkeypatch.setattr(controllers.databaseHelper, "countQuestDialogues", lambda quest_id: 1)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectQuestDialoguesPaged",
            lambda quest_id, page_size, offset: [(123456, "NPC", 2001, 3001, 1001)],
        )
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda *args, **kwargs: {
                "hash": 123456,
                "translates": {"1": "测试对白"},
                "createdVersion": "9.9",
                "updatedVersion": "9.9",
            },
        )
        monkeypatch.setattr(controllers.databaseHelper, "getTalkerName", lambda *args, **kwargs: "派蒙")

        result, total = controllers.getQuestDialogues(42, searchLang=1, page=1, page_size=20)

        assert total == 1
        assert version_calls == [(42, 1)]
        assert result["createdVersionRaw"] == "Version 4.4"
        assert result["updatedVersionRaw"] == "Version 4.7"
        assert result["createdVersion"] == "4.4"
        assert result["updatedVersion"] == "4.7"


# ---------------------------------------------------------------------------
# content normalization
# ---------------------------------------------------------------------------

class TestNormalizeTextMapContent:
    def test_normalize_text_map_content_calls_placeholder_handler(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getIsMale", lambda: True)

        calls = []

        def _fake_replace(content, is_male, lang_code):
            calls.append((content, is_male, lang_code))
            return "已替换文本"

        monkeypatch.setattr(controllers.placeholderHandler, "replace", _fake_replace)
        result = controllers._normalize_text_map_content("原始文本", 1)
        assert result == "已替换文本"
        assert calls == [("原始文本", True, 1)]

    def test_normalize_text_map_content_none(self):
        assert controllers._normalize_text_map_content(None, 1) is None


class TestCatalogEntityVersionInfo:
    def test_get_catalog_entity_version_info_aggregates_title_and_body(self, monkeypatch):
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE text_source_entity (
                    text_hash INTEGER NOT NULL,
                    source_type_code INTEGER NOT NULL,
                    entity_id INTEGER NOT NULL,
                    title_hash INTEGER NOT NULL,
                    extra INTEGER NOT NULL DEFAULT 0,
                    sub_category INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE textMap (
                    hash INTEGER,
                    lang INTEGER,
                    created_version_id INTEGER,
                    updated_version_id INTEGER
                );
                CREATE TABLE version_dim (
                    id INTEGER PRIMARY KEY,
                    raw_version TEXT,
                    version_tag TEXT,
                    version_sort_key INTEGER
                );
                """
            )
            conn.executemany(
                "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?, ?, ?, ?)",
                [
                    (1, "Version 1.0", "1.0", 100),
                    (2, "Version 2.0", "2.0", 200),
                    (3, "Version 3.0", "3.0", 300),
                ],
            )
            conn.executemany(
                "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (200, 5, 264106, 100, 1, 29),
                    (201, 5, 264106, 100, 2, 29),
                ],
            )
            conn.executemany(
                "INSERT INTO textMap(hash, lang, created_version_id, updated_version_id) VALUES (?, ?, ?, ?)",
                [
                    (100, 4, 1, 3),
                    (200, 4, 2, 2),
                    (201, 4, 2, 2),
                    (100, 1, 3, 3),
                    (200, 1, 3, 3),
                ],
            )

            monkeypatch.setattr(controllers.databaseHelper, "conn", conn)
            monkeypatch.setattr(controllers.databaseHelper, "_has_version_dim", lambda: True)
            monkeypatch.setattr(controllers.databaseHelper, "_has_version_id_columns", lambda table_name: table_name == "textMap")
            monkeypatch.setattr(controllers.databaseHelper.config, "getSourceLanguage", lambda: 4)

            created_raw, updated_raw = controllers.databaseHelper.getCatalogEntityVersionInfo(5, 264106)

            assert created_raw == "Version 1.0"
            assert updated_raw == "Version 3.0"
        finally:
            conn.close()

    def test_get_catalog_entity_version_info_strictly_uses_source_language(self, monkeypatch):
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE text_source_entity (
                    text_hash INTEGER NOT NULL,
                    source_type_code INTEGER NOT NULL,
                    entity_id INTEGER NOT NULL,
                    title_hash INTEGER NOT NULL,
                    extra INTEGER NOT NULL DEFAULT 0,
                    sub_category INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE textMap (
                    hash INTEGER,
                    lang INTEGER,
                    created_version_id INTEGER,
                    updated_version_id INTEGER
                );
                CREATE TABLE version_dim (
                    id INTEGER PRIMARY KEY,
                    raw_version TEXT,
                    version_tag TEXT,
                    version_sort_key INTEGER
                );
                """
            )
            conn.execute(
                "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category) VALUES (?, ?, ?, ?, ?, ?)",
                (200, 5, 264106, 100, 1, 29),
            )
            conn.executemany(
                "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?, ?, ?, ?)",
                [
                    (1, "Version 1.0", "1.0", 100),
                    (2, "Version 2.0", "2.0", 200),
                ],
            )
            conn.executemany(
                "INSERT INTO textMap(hash, lang, created_version_id, updated_version_id) VALUES (?, ?, ?, ?)",
                [
                    (100, 1, 1, 2),
                    (200, 1, 1, 2),
                ],
            )

            monkeypatch.setattr(controllers.databaseHelper, "conn", conn)
            monkeypatch.setattr(controllers.databaseHelper, "_has_version_dim", lambda: True)
            monkeypatch.setattr(controllers.databaseHelper, "_has_version_id_columns", lambda table_name: table_name == "textMap")
            monkeypatch.setattr(controllers.databaseHelper.config, "getSourceLanguage", lambda: 4)

            created_raw, updated_raw = controllers.databaseHelper.getCatalogEntityVersionInfo(5, 264106)

            assert created_raw is None
            assert updated_raw is None
        finally:
            conn.close()


class TestCatalogMeta:
    def test_database_helper_build_source_type_join_costume_includes_qianxing_codes(self):
        join_clause, params = controllers.databaseHelper._build_source_type_join("costume")

        assert "text_source_entity" in join_clause
        assert "IN" in join_clause
        assert params == [5, 6, 27, 28, 29, 30]

    def test_database_helper_select_catalog_category_pairs_merges_qianxing_codes(self, monkeypatch):
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE text_source_entity (
                    text_hash INTEGER NOT NULL,
                    source_type_code INTEGER NOT NULL,
                    entity_id INTEGER NOT NULL,
                    title_hash INTEGER NOT NULL,
                    extra INTEGER NOT NULL DEFAULT 0,
                    sub_category INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            conn.executemany(
                "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (101, 5, 260001, 201, 1, 29),
                    (102, 6, 265001, 202, 1, 30),
                    (103, 27, 1001, 203, 1, 31),
                    (104, 28, 611000, 204, 1, 32),
                    (105, 29, 2001, 205, 1, 33),
                    (106, 30, 351101, 206, 1, 34),
                ],
            )

            monkeypatch.setattr(controllers.databaseHelper, "conn", conn)

            assert controllers.databaseHelper.selectCatalogCategoryPairs() == [
                (5, 29),
                (5, 30),
                (5, 31),
                (5, 32),
                (5, 33),
                (5, 34),
            ]
        finally:
            conn.close()

    def test_database_helper_select_catalog_entities_aggregates_qianxing_filter(self, monkeypatch):
        conn = sqlite3.connect(":memory:")
        try:
            conn.executescript(
                """
                CREATE TABLE text_source_entity (
                    text_hash INTEGER NOT NULL,
                    source_type_code INTEGER NOT NULL,
                    entity_id INTEGER NOT NULL,
                    title_hash INTEGER NOT NULL,
                    extra INTEGER NOT NULL DEFAULT 0,
                    sub_category INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE textMap (
                    hash INTEGER,
                    lang INTEGER,
                    content TEXT
                );
                """
            )
            conn.executemany(
                "INSERT INTO text_source_entity(text_hash, source_type_code, entity_id, title_hash, extra, sub_category) VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (101, 5, 260001, 201, 1, 29),
                    (102, 6, 265001, 202, 1, 30),
                    (103, 27, 1001, 203, 1, 31),
                    (104, 30, 351101, 204, 1, 34),
                ],
            )
            conn.executemany(
                "INSERT INTO textMap(hash, lang, content) VALUES (?, ?, ?)",
                [
                    (201, 1, "凝脂白"),
                    (202, 1, "套装展示"),
                    (203, 1, "微笑"),
                    (204, 1, "童话剧场猫猫小屋"),
                ],
            )

            monkeypatch.setattr(controllers.databaseHelper, "conn", conn)
            monkeypatch.setattr(controllers.databaseHelper, "_has_version_dim", lambda: False)
            monkeypatch.setattr(controllers.databaseHelper, "_has_version_id_columns", lambda _table_name: False)

            rows = controllers.databaseHelper.selectCatalogEntities("", 1, source_type_code=5, limit=10, offset=0)
            total = controllers.databaseHelper.countCatalogEntities("", 1, source_type_code=5)

            assert total == 4
            assert {(row[0], row[1], row[3], row[4]) for row in rows} == {
                (260001, 5, 29, "凝脂白"),
                (265001, 6, 30, "套装展示"),
                (1001, 27, 31, "微笑"),
                (351101, 30, 34, "童话剧场猫猫小屋"),
            }
        finally:
            conn.close()

    def test_get_catalog_sub_category_groups_keeps_known_subcategories_and_other(self, monkeypatch):
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectCatalogCategoryPairs",
            lambda: [(1, 0), (1, 1), (1, 1), (2, 999), (3, 0)],
        )
        monkeypatch.setattr(
            controllers,
            "getCatalogSubCategories",
            lambda: {"1": "任务道具", "2": "摆设图纸"},
        )

        result = controllers.getCatalogSubCategoryGroups()

        assert result == {
            "1": ["0", "1"],
            "3": ["0"],
        }

    def test_get_catalog_uncategorized_sub_category(self):
        assert controllers.getCatalogUncategorizedSubCategory() == {
            "value": "0",
            "label": "其他",
        }


class TestEntityTexts:
    def test_get_entity_texts_returns_missing_body_with_top_level_versions(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1, 4])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 4)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectEntityTextHashesByEntity",
            lambda source_type_code, entity_id: [(1700897759, 1192942058, 257, 29)],
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getCatalogEntityVersionInfo",
            lambda source_type_code, entity_id, version_lang_code=None: ("Version 1.0", "Version 2.0"),
        )
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("costume", "千星奇域"))
        monkeypatch.setattr(controllers, "_get_sub_category_label", lambda code: "奇偶装扮")
        monkeypatch.setattr(controllers, "_get_text_map_content_with_fallback", lambda *args, **kwargs: "凝脂白")
        monkeypatch.setattr(controllers, "queryTextHashInfo", lambda *args, **kwargs: {"translates": {}})
        monkeypatch.setattr(controllers, "_collect_entity_readable_entries", lambda *args, **kwargs: [])

        result = controllers.getEntityTexts(5, 264106, searchLang=1)

        assert result["title"] == "凝脂白"
        assert result["entries"] == []
        assert result["missingBody"] is True
        assert result["emptyMessage"] == "暂无可用描述文本"
        assert result["createdVersion"] == "1.0"
        assert result["updatedVersion"] == "2.0"

    def test_get_entity_texts_includes_entry_title_and_unisex_subtitle(self, monkeypatch):
        title_map = {
            1192942058: "凝脂白",
        }
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1, 4])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 4)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectEntityTextHashesByEntity",
            lambda source_type_code, entity_id: [(1700897759, 1192942058, 769, 29)],
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getCatalogEntityVersionInfo",
            lambda source_type_code, entity_id, version_lang_code=None: ("Version 1.0", "Version 2.0"),
        )
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("costume", "千星奇域"))
        monkeypatch.setattr(controllers, "_get_sub_category_label", lambda code: "奇偶装扮")
        monkeypatch.setattr(
            controllers,
            "_get_text_map_content_with_fallback",
            lambda text_hash, *args, **kwargs: title_map.get(text_hash),
        )
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda *args, **kwargs: {"translates": {"1": "衣装介绍"}},
        )
        monkeypatch.setattr(controllers, "_collect_entity_readable_entries", lambda *args, **kwargs: [])

        result = controllers.getEntityTexts(5, 264106, searchLang=1)

        assert result["title"] == "凝脂白"
        assert result["entries"] == [
            {
                "entryTitle": "凝脂白",
                "fieldLabel": "介绍",
                "subtitle": "千星奇域 264106 · 通用",
                "textHash": 1700897759,
                "titleHash": 1192942058,
                "text": {"translates": {"1": "衣装介绍"}},
            }
        ]

    def test_build_entity_source_payload_marks_unisex_costume(self, monkeypatch):
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("costume", "千星奇域"))
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(controllers, "_get_text_map_content_with_fallback", lambda *args, **kwargs: "凝脂白")

        primary, origin, source_count = controllers._build_entity_source_payload(
            [(5, 264106, 1192942058, 769)],
            1,
            1700897759,
        )

        assert primary == {
            "sourceType": "costume",
            "title": "凝脂白",
            "subtitle": "千星奇域 264106 · 通用",
            "detailQuery": {
                "kind": "entity",
                "sourceTypeCode": 5,
                "entityId": 264106,
                "textHash": 1700897759,
            },
        }
        assert origin == "千星奇域: 凝脂白"
        assert source_count == 1

    def test_build_entity_source_payload_recovers_legacy_costume_title_hash(self, monkeypatch):
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("costume", "千星奇域"))
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(
            controllers,
            "_get_text_map_content_with_fallback",
            lambda text_hash, *args, **kwargs: "尖齿短袖衫·海蓝" if text_hash == 2098177162 else None,
        )

        primary, origin, source_count = controllers._build_entity_source_payload(
            [(5, 260001, 2098177674, 513)],
            1,
            33590471,
        )

        assert primary == {
            "sourceType": "costume",
            "title": "尖齿短袖衫·海蓝",
            "subtitle": "千星奇域 260001 · 女",
            "detailQuery": {
                "kind": "entity",
                "sourceTypeCode": 5,
                "entityId": 260001,
                "textHash": 33590471,
            },
        }
        assert origin == "千星奇域: 尖齿短袖衫·海蓝"
        assert source_count == 1

    def test_build_entity_source_payload_keeps_internal_qianxing_code_for_detail_query(self, monkeypatch):
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("qianxing_pose", "千星奇域"))
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(controllers, "_get_text_map_content_with_fallback", lambda *args, **kwargs: "升炼姿态")

        primary, origin, source_count = controllers._build_entity_source_payload(
            [(28, 611000, 1491722704, 513)],
            1,
            4282661314,
        )

        assert primary == {
            "sourceType": "qianxing_pose",
            "title": "升炼姿态",
            "subtitle": "千星奇域 611000 · 女",
            "detailQuery": {
                "kind": "entity",
                "sourceTypeCode": 28,
                "entityId": 611000,
                "textHash": 4282661314,
            },
        }
        assert origin == "千星奇域: 升炼姿态"
        assert source_count == 1

    def test_get_entity_texts_includes_item_linked_readable_entries(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectEntityTextHashesByEntity",
            lambda source_type_code, entity_id: [(91001, 5002, 1, 1)],
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getCatalogEntityVersionInfo",
            lambda source_type_code, entity_id, version_lang_code=None: (None, None),
        )
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("item", "道具"))
        monkeypatch.setattr(controllers, "_get_sub_category_label", lambda code: "任务道具")
        monkeypatch.setattr(
            controllers,
            "_get_text_map_content_with_fallback",
            lambda text_hash, *args, **kwargs: {
                5002: "莱茵多特的「礼物」",
                3377011063: "莱茵多特的「礼物」",
                2842036365: "「黄金」莱茵多特赠给阿贝多的礼物。",
            }.get(text_hash),
        )
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda text_hash, langs, source_lang_code, queryOrigin=False: {"translates": {}, "hash": text_hash},
        )
        monkeypatch.setattr(
            controllers,
            "_load_entity_readable_lookup",
            lambda: {
                "outfit_item_to_skin": {},
                "reliquary_set_to_id": {},
                "reliquary_set_piece_to_id": {},
                "book_material_ids": set(),
                "codex_readable_ids": set(),
                "codex_title_hashes": set(),
                "item_readable_ids_by_item_id": {121414: [201140]},
                "item_ids_by_readable_id": {201140: 121414},
                "item_ids_by_title_hash": {3377011063: 121414},
                "item_name_hash_by_item_id": {121414: 3377011063},
                "item_desc_hash_by_item_id": {121414: 2842036365},
            },
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getReadableInfo",
            lambda readable_id, lang_code=None: ("Book1140.txt", 3377011063, readable_id),
        )
        monkeypatch.setattr(controllers.databaseHelper, "selectReadableRefsByFileNamePrefix", lambda prefix: [])
        monkeypatch.setattr(controllers.databaseHelper, "selectReadableRefsByTitleHash", lambda *args, **kwargs: [])
        monkeypatch.setattr(controllers.databaseHelper, "getLangCodeMap", lambda: {1: "CHS"})
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectReadableFromReadableId",
            lambda readable_id, target_lang_strs: [("正文内容", "CHS")],
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectReadableFromFileName",
            lambda file_name, target_lang_strs: [],
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getReadableVersionInfo",
            lambda readable_id, file_name: ("Version 4.1", None),
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getReadableCategoryCode",
            lambda file_name: "ITEM",
        )

        result = controllers.getEntityTexts(1, 121414, searchLang=1)

        assert result["title"] == "莱茵多特的「礼物」"
        assert result["missingBody"] is False
        assert len(result["entries"]) == 1
        entry = result["entries"][0]
        assert entry["fieldLabel"] == "道具"
        assert entry["readableCategory"] == "ITEM"
        assert entry["fileName"] == "Book1140.txt"
        assert entry["readableId"] == 201140
        assert entry["detailQuery"] == {
            "kind": "readable",
            "readableId": 201140,
            "fileName": "Book1140.txt",
            "textHash": 3377011063,
        }
        assert entry["text"]["translates"] == {"1": "正文内容"}
        assert entry["text"]["createdVersion"] == "4.1"


class TestReadableCategoryLabels:
    @pytest.mark.parametrize(
        ("file_name", "category"),
        [
            ("Book1140.txt", "ITEM"),
            ("Book1039.txt", "READABLE"),
            ("Book2000.txt", "BOOK"),
            ("Weapon11431_2.txt", "WEAPON"),
        ],
    )
    def test_build_readable_category_fields_delegates_to_database_helper(self, monkeypatch, file_name, category):
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getReadableCategoryCode",
            lambda raw_file_name: {
                "Book1140.txt": "ITEM",
                "Book1039.txt": "READABLE",
                "Book2000.txt": "BOOK",
                "Weapon11431_2.txt": "WEAPON",
            }[raw_file_name],
        )

        result = controllers._build_readable_category_fields(file_name, 0, None)

        assert result == {
            "readableCategory": category,
        }


def _patch_search_name_empty_quest_dependencies(monkeypatch):
    monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
    monkeypatch.setattr(controllers.databaseHelper, "selectQuestByTitleKeyword", lambda *args, **kwargs: [])
    monkeypatch.setattr(controllers.databaseHelper, "selectQuestByChapterKeyword", lambda *args, **kwargs: [])
    monkeypatch.setattr(controllers.databaseHelper, "selectQuestByIdContains", lambda *args, **kwargs: [])
    monkeypatch.setattr(controllers.databaseHelper, "selectQuestByContentKeyword", lambda *args, **kwargs: [])
    monkeypatch.setattr(controllers.databaseHelper, "selectQuestByVersion", lambda *args, **kwargs: [])
    monkeypatch.setattr(controllers.databaseHelper, "getLangCodeMap", lambda: {1: "CHS"})
    monkeypatch.setattr(controllers.databaseHelper, "resolveReadableTitleHash", lambda readable_id, file_name: None)


class TestSearchNameReadableCategoryFilters:
    def test_search_name_entries_passes_book_filter_directly_to_db(self, monkeypatch):
        _patch_search_name_empty_quest_dependencies(monkeypatch)

        calls = []

        def fake_select_readable_by_title(
            keyword,
            lang_code,
            lang_str,
            created_version=None,
            updated_version=None,
            category=None,
            limit=200,
            offset=None,
        ):
            calls.append((category, limit, offset))
            return [("BookCodex.txt", 999, 9999, "Codex Title", None, None)]

        monkeypatch.setattr(controllers.databaseHelper, "selectReadableByTitleKeyword", fake_select_readable_by_title)
        monkeypatch.setattr(controllers.databaseHelper, "selectReadableByFileNameContains", lambda *args, **kwargs: [])
        monkeypatch.setattr(controllers.databaseHelper, "selectReadableFromKeyword", lambda *args, **kwargs: [])
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getReadableCategoryCode",
            lambda file_name: "BOOK",
        )

        result = controllers.searchNameEntries("book", 1, readable_category="BOOK")

        assert calls == [("BOOK", 200, None)]
        assert result["quests"] == []
        assert result["readables"] == [
            {
                "fileName": "BookCodex.txt",
                "readableId": 999,
                "title": "Codex Title",
                "titleTextMapHash": 9999,
                "readableCategory": "BOOK",
                "createdVersion": None,
                "updatedVersion": None,
                "createdVersionRaw": None,
                "updatedVersionRaw": None,
            }
        ]

    @pytest.mark.parametrize(
        ("filter_value", "expected_ids"),
        [
            ("ITEM", [11]),
            ("READABLE", [22, 33]),
        ],
    )
    def test_search_name_entries_filters_semantic_readable_categories(self, monkeypatch, filter_value, expected_ids):
        _patch_search_name_empty_quest_dependencies(monkeypatch)
        calls = []

        def fake_select_readable_by_title(*args, **kwargs):
            calls.append(kwargs.get("category") if "category" in kwargs else args[5])
            category = kwargs.get("category") if "category" in kwargs else args[5]
            if category == "ITEM":
                return [("BookItem.txt", 11, 1011, "Item Title", None, None)]
            if category == "READABLE":
                return [
                    ("BookReadable.txt", 22, 1022, "Readable Title", None, None),
                    ("Poster1.txt", 33, 1033, "Poster Title", None, None),
                ]
            return []

        monkeypatch.setattr(controllers.databaseHelper, "selectReadableByTitleKeyword", fake_select_readable_by_title)
        monkeypatch.setattr(controllers.databaseHelper, "selectReadableByFileNameContains", lambda *args, **kwargs: [])
        monkeypatch.setattr(controllers.databaseHelper, "selectReadableFromKeyword", lambda *args, **kwargs: [])
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getReadableCategoryCode",
            lambda file_name: {
                "BookItem.txt": "ITEM",
                "BookReadable.txt": "READABLE",
                "Poster1.txt": "READABLE",
            }[file_name],
        )

        result = controllers.searchNameEntries("readable", 1, readable_category=filter_value)

        assert calls == [filter_value]
        assert [entry["readableId"] for entry in result["readables"]] == expected_ids
        assert all(entry["readableCategory"] == filter_value for entry in result["readables"])


class TestEntitySourceFiltering:
    def test_select_primary_source_skips_entities_without_visible_text(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(controllers.databaseHelper, "getTalkInfo", lambda text_hash: None)
        monkeypatch.setattr(controllers.databaseHelper, "getSourceFromFetter", lambda text_hash, lang_code: None)
        monkeypatch.setattr(controllers, "_select_story_source_from_text_hash", lambda text_hash, lang_code: None)
        monkeypatch.setattr(controllers.databaseHelper, "selectQuestHashSources", lambda text_hash: [])
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectEntitySourcesByTextHash",
            lambda text_hash: [(5, 100, 5001, 0), (5, 200, 5002, 0)],
        )
        monkeypatch.setattr(controllers.databaseHelper, "selectEntitySourcesByTitleHash", lambda text_hash: [])
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectEntityTextHashesByEntity",
            lambda source_type_code, entity_id: {
                (5, 100): [(91001, 5001, 1, 0)],
                (5, 200): [(91002, 5002, 1, 0)],
            }.get((source_type_code, entity_id), []),
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextMapFromTextHash",
            lambda text_hash, langs=None: [] if int(text_hash) == 91001 else [("可展示文本", 1)],
        )
        monkeypatch.setattr(controllers, "_collect_entity_readable_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(controllers.databaseHelper, "getReadableInfoByTitleHash", lambda text_hash: None)
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("costume", "千星奇域"))
        monkeypatch.setattr(
            controllers,
            "_get_entity_title_with_fallback",
            lambda source_type, title_hash, lang_code, fallbacks: {5001: "空实体", 5002: "有效实体"}.get(title_hash),
        )

        primary, origin, is_talk, source_count = controllers._select_primary_source_from_text_hash(778899, 1)

        assert is_talk is False
        assert source_count == 1
        assert origin == "千星奇域: 有效实体"
        assert primary["detailQuery"]["entityId"] == 200

    def test_get_text_entity_sources_returns_only_valid_groups(self, monkeypatch):
        monkeypatch.setattr(controllers.config, "getResultLanguages", lambda: [1])
        monkeypatch.setattr(controllers.config, "getSourceLanguage", lambda: 1)
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectEntitySourcesByTextHash",
            lambda text_hash: [(5, 100, 5001, 0), (5, 200, 5002, 0)],
        )
        monkeypatch.setattr(controllers.databaseHelper, "selectEntitySourcesByTitleHash", lambda text_hash: [])
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectEntityTextHashesByEntity",
            lambda source_type_code, entity_id: {
                (5, 100): [(91001, 5001, 1, 0)],
                (5, 200): [(91002, 5002, 1, 0)],
            }.get((source_type_code, entity_id), []),
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "selectTextMapFromTextHash",
            lambda text_hash, langs=None: [] if int(text_hash) == 91001 else [("可展示文本", 1)],
        )
        monkeypatch.setattr(controllers, "_collect_entity_readable_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(controllers, "_get_entity_source_meta", lambda code: ("costume", "千星奇域"))
        monkeypatch.setattr(controllers, "_get_sub_category_label", lambda code: "")
        monkeypatch.setattr(
            controllers,
            "_get_entity_title_with_fallback",
            lambda source_type, title_hash, lang_code, fallbacks: {5001: "空实体", 5002: "有效实体"}.get(title_hash),
        )
        monkeypatch.setattr(
            controllers,
            "queryTextHashInfo",
            lambda text_hash, langs, source_lang_code, queryOrigin=False: {
                "translates": {"1": f"文本-{text_hash}"},
                "hash": text_hash,
            } if int(text_hash) == 91002 else {"translates": {}, "hash": text_hash},
        )
        monkeypatch.setattr(
            controllers.databaseHelper,
            "getCatalogEntityVersionInfo",
            lambda source_type_code, entity_id, version_lang_code=None: (None, None),
        )

        result = controllers.getTextEntitySources(778899, searchLang=1)

        assert result["sourceCount"] == 1
        assert len(result["groups"]) == 1
        assert result["groups"][0]["entityId"] == 200
        assert result["groups"][0]["entries"] == [
            {
                "entryTitle": "有效实体",
                "fieldLabel": "介绍",
                "subtitle": "千星奇域 200 · 男",
                "textHash": 91002,
                "titleHash": 5002,
                "text": {"translates": {"1": "文本-91002"}, "hash": 91002},
            }
        ]

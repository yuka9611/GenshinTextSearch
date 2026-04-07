"""Tests for selected pure/internal logic in server/controllers.py."""
import importlib.util
import sqlite3
from pathlib import Path

import pytest


_CONTROLLERS_FILE = Path(__file__).resolve().parents[1] / "server" / "controllers.py"
_SPEC = importlib.util.spec_from_file_location("controllers_file_module", _CONTROLLERS_FILE)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Cannot load controllers module from {_CONTROLLERS_FILE}")
controllers = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(controllers)


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

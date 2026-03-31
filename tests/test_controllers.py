"""Tests for selected pure/internal logic in server/controllers.py."""
import importlib.util
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

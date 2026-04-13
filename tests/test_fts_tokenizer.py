"""Tests for server/fts_tokenizer.py — pure function tests, no DB dependency."""
from typing import cast

import pytest

from fts_tokenizer import (
    normalize_segmenter_name,
    normalize_search_keyword,
    _compact_spaces,
    _unique_tokens,
    _segment_with_char_bigram,
    segment_chinese_tokens,
    build_fts_index_text,
    build_fts_query_terms,
)


# ---------------------------------------------------------------------------
# normalize_search_keyword
# ---------------------------------------------------------------------------

class TestNormalizeSearchKeyword:
    def test_returns_empty_for_empty_input(self):
        assert normalize_search_keyword("") == ""

    def test_strips_ascii_quotes(self):
        assert normalize_search_keyword('"莉奈娅的"') == "莉奈娅的"

    def test_strips_chinese_quotes(self):
        assert normalize_search_keyword("「莉奈娅的」") == "莉奈娅的"

    def test_strips_multiple_quote_layers(self):
        assert normalize_search_keyword("“「莉奈娅的」”") == "莉奈娅的"

    def test_strips_unbalanced_edge_quotes(self):
        assert normalize_search_keyword("“莉奈娅的") == "莉奈娅的"

    def test_preserves_inner_quotes(self):
        assert normalize_search_keyword('莉奈娅说“你好”') == '莉奈娅说“你好”'


# ---------------------------------------------------------------------------
# normalize_segmenter_name
# ---------------------------------------------------------------------------

class TestNormalizeSegmenterName:
    @pytest.mark.parametrize("value", ["auto", "jieba", "char_bigram", "none"])
    def test_valid_values(self, value):
        assert normalize_segmenter_name(value) == value

    def test_none_returns_auto(self):
        assert normalize_segmenter_name(None) == "auto"

    def test_empty_string_returns_auto(self):
        assert normalize_segmenter_name("") == "auto"

    def test_invalid_returns_auto(self):
        assert normalize_segmenter_name("unknown_mode") == "auto"

    def test_whitespace_stripped(self):
        assert normalize_segmenter_name("  jieba  ") == "jieba"

    def test_case_insensitive(self):
        assert normalize_segmenter_name("JIEBA") == "jieba"


# ---------------------------------------------------------------------------
# _compact_spaces
# ---------------------------------------------------------------------------

class TestCompactSpaces:
    def test_no_spaces(self):
        assert _compact_spaces("hello") == "hello"

    def test_with_spaces(self):
        assert _compact_spaces("he llo wo rld") == "helloworld"

    def test_empty_string(self):
        assert _compact_spaces("") == ""

    def test_none_input(self):
        assert _compact_spaces(cast(str, None)) == ""

    def test_tabs_and_newlines(self):
        assert _compact_spaces("a\tb\nc") == "abc"


# ---------------------------------------------------------------------------
# _unique_tokens
# ---------------------------------------------------------------------------

class TestUniqueTokens:
    def test_removes_duplicates(self):
        assert _unique_tokens(["a", "b", "a", "c"]) == ["a", "b", "c"]

    def test_preserves_order(self):
        assert _unique_tokens(["c", "b", "a"]) == ["c", "b", "a"]

    def test_strips_whitespace(self):
        assert _unique_tokens(["  x ", " y"]) == ["x", "y"]

    def test_filters_empty(self):
        assert _unique_tokens(["", "  ", "a", ""]) == ["a"]

    def test_empty_list(self):
        assert _unique_tokens([]) == []


# ---------------------------------------------------------------------------
# _segment_with_char_bigram
# ---------------------------------------------------------------------------

class TestSegmentWithCharBigram:
    def test_empty_string(self):
        assert _segment_with_char_bigram("") == []

    def test_single_char(self):
        assert _segment_with_char_bigram("你") == ["你"]

    def test_two_chars(self):
        result = _segment_with_char_bigram("你好")
        assert "你好" in result
        assert len(result) == 2  # bigram + full text

    def test_three_chars(self):
        result = _segment_with_char_bigram("你好吗")
        assert "你好" in result
        assert "好吗" in result
        assert "你好吗" in result

    def test_ascii_text(self):
        result = _segment_with_char_bigram("abc")
        assert "ab" in result
        assert "bc" in result
        assert "abc" in result


# ---------------------------------------------------------------------------
# segment_chinese_tokens
# ---------------------------------------------------------------------------

class TestSegmentChineseTokens:
    def test_empty_string(self):
        assert segment_chinese_tokens("") == []

    def test_none_mode_defaults_to_auto(self):
        tokens = segment_chinese_tokens("你好世界", segmenter_mode="none")
        # In "none" mode, only the compact text is returned
        assert "你好世界" in tokens

    def test_char_bigram_mode(self):
        tokens = segment_chinese_tokens("你好世界", segmenter_mode="char_bigram")
        assert "你好" in tokens
        assert "好世" in tokens
        assert "世界" in tokens
        assert "你好世界" in tokens

    def test_full_text_always_in_tokens(self):
        tokens = segment_chinese_tokens("测试文本", segmenter_mode="char_bigram")
        assert "测试文本" in tokens

    def test_spaces_are_compacted(self):
        tokens = segment_chinese_tokens("你 好", segmenter_mode="char_bigram")
        assert "你好" in tokens


# ---------------------------------------------------------------------------
# build_fts_index_text
# ---------------------------------------------------------------------------

class TestBuildFtsIndexText:
    def test_non_chinese_returns_as_is(self):
        # lang_code 4 = English
        result = build_fts_index_text("hello world", lang_code=4, tokenizer_name="unicode61")
        assert result == "hello world"

    def test_chinese_trigram_returns_as_is(self):
        result = build_fts_index_text("你好世界", lang_code=1, tokenizer_name="trigram")
        assert result == "你好世界"

    def test_chinese_non_trigram_segments(self):
        result = build_fts_index_text(
            "你好世界", lang_code=1, tokenizer_name="unicode61",
            segmenter_mode="char_bigram",
        )
        # Should contain space-separated tokens
        assert " " in result
        assert "你好" in result

    def test_none_content(self):
        result = build_fts_index_text(None, lang_code=1, tokenizer_name="unicode61")
        assert result == ""

    def test_empty_content(self):
        result = build_fts_index_text("", lang_code=1, tokenizer_name="unicode61")
        assert result == ""


# ---------------------------------------------------------------------------
# build_fts_query_terms
# ---------------------------------------------------------------------------

class TestBuildFtsQueryTerms:
    def test_empty_keyword(self):
        assert build_fts_query_terms("", lang_code=1, tokenizer_name="unicode61") == []

    def test_trigram_returns_single_term(self):
        result = build_fts_query_terms("hello", lang_code=1, tokenizer_name="trigram")
        assert result == ["hello"]

    def test_english_split_by_space(self):
        result = build_fts_query_terms("hello world", lang_code=4, tokenizer_name="unicode61")
        assert result == ["hello", "world"]

    def test_chinese_char_bigram_segmentation(self):
        result = build_fts_query_terms(
            "你好世界", lang_code=1, tokenizer_name="unicode61",
            segmenter_mode="char_bigram",
        )
        assert len(result) > 0
        # Full text should NOT be in the result when there are multiple tokens
        # (the function strips the compact form when len(tokens) > 1)
        if len(result) > 1:
            assert "你好世界" not in result

    def test_whitespace_only(self):
        assert build_fts_query_terms("   ", lang_code=4, tokenizer_name="unicode61") == []

    def test_none_keyword(self):
        assert build_fts_query_terms(cast(str, None), lang_code=1, tokenizer_name="unicode61") == []

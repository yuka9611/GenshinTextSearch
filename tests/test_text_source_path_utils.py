import os
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import diffUpdate
import history_backfill
import text_source_path_utils


def test_normalize_readable_rel_path_supports_nested_paths_and_backslashes():
    parsed = text_source_path_utils.normalize_readable_rel_path(r"EN\books\archive.txt")

    assert parsed == ("EN", "books/archive.txt")


def test_normalize_readable_rel_path_rejects_missing_language_segment():
    assert text_source_path_utils.normalize_readable_rel_path("archive.txt") is None


def test_build_readable_rel_path_from_record_matches_history_and_diff_users():
    record = ("books/archive.txt", "EN")
    expected = "Readable/EN/books/archive.txt"

    assert text_source_path_utils.build_readable_rel_path_from_record(record) == expected
    assert history_backfill._build_readable_record_rel_path(record) == expected
    assert diffUpdate._build_readable_record_rel_path(record) == expected


def test_normalize_subtitle_rel_path_supports_nested_paths_and_lang_resolution():
    parsed = text_source_path_utils.normalize_subtitle_rel_path(r"EN\story\cutscene.srt")

    assert parsed == ("EN", 4, "story/cutscene")


def test_build_subtitle_rel_path_from_key_supports_current_and_legacy_formats():
    expected = "Subtitle/EN/story/cutscene.srt"

    assert (
        text_source_path_utils.build_subtitle_rel_path_from_key("story/cutscene|4|1.000|2.000")
        == expected
    )
    assert (
        text_source_path_utils.build_subtitle_rel_path_from_key("story/cutscene_4_1.000_2.000")
        == expected
    )


def test_build_subtitle_rel_path_from_record_matches_history_and_diff_users():
    record = ("story/cutscene|4|1.000|2.000",)
    expected = "Subtitle/EN/story/cutscene.srt"

    assert text_source_path_utils.build_subtitle_rel_path_from_record(record) == expected
    assert history_backfill._build_subtitle_record_rel_path(record) == expected
    assert diffUpdate._build_subtitle_record_rel_path(record) == expected

import os
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from textmap_match_utils import (
    TEXTMAP_MATCH_KIND_CROSS_HASH_SIMILAR,
    TEXTMAP_MATCH_KIND_NEW,
    TextMapLineageState,
    allocate_textmap_current_matches,
    build_textmap_content_index,
    match_textmap_lineage_to_previous,
)


def test_match_textmap_lineage_to_previous_keeps_similar_cross_hash_matching_when_pair_budget_is_exceeded():
    previous_obj = {
        100: "是歌尘浪世真君所收的弟子吧。虽是凡人之躯，但毅力和灵性倒是可圈可点。",
    }
    previous_obj.update({1000 + idx: f"noise row {idx}" for idx in range(32)})

    matches = match_textmap_lineage_to_previous(
        {
            200: TextMapLineageState(
                snapshot_hash=200,
                content="是歌尘浪市真君所收的弟子吧。虽是凡人之躯，但毅力和灵性倒是可圈可点。",
            )
        },
        previous_obj,
        max_similarity_pairs=10,
    )

    assert matches[200].match_kind == TEXTMAP_MATCH_KIND_CROSS_HASH_SIMILAR
    assert matches[200].predecessor_hash == 100


def test_allocate_textmap_current_matches_keeps_same_hash_for_short_generic_text():
    matches = allocate_textmap_current_matches(
        build_textmap_content_index({200: "呀！"}),
        build_textmap_content_index({200: "呀！"}),
    )

    assert matches == {200: 200}


def test_allocate_textmap_current_matches_skips_cross_hash_same_content_for_short_generic_text():
    matches = allocate_textmap_current_matches(
        build_textmap_content_index({200: "呀！"}),
        build_textmap_content_index({100: "呀！"}),
    )

    assert matches == {}


def test_match_textmap_lineage_to_previous_skips_cross_hash_same_content_for_short_generic_text():
    matches = match_textmap_lineage_to_previous(
        {
            200: TextMapLineageState(
                snapshot_hash=200,
                content="呀！",
            )
        },
        {
            100: "呀！",
        },
    )

    assert matches[200].match_kind == TEXTMAP_MATCH_KIND_NEW
    assert matches[200].predecessor_hash is None


def test_match_textmap_lineage_to_previous_skips_cross_hash_same_content_for_broad_short_response():
    matches = match_textmap_lineage_to_previous(
        {
            200: TextMapLineageState(
                snapshot_hash=200,
                content="谢谢。",
            )
        },
        {
            100: "谢谢。",
        },
    )

    assert matches[200].match_kind == TEXTMAP_MATCH_KIND_NEW
    assert matches[200].predecessor_hash is None


def test_match_textmap_lineage_to_previous_skips_similar_cross_hash_matching_for_short_generic_text():
    matches = match_textmap_lineage_to_previous(
        {
            200: TextMapLineageState(
                snapshot_hash=200,
                content="呀呀！",
            )
        },
        {
            100: "呀！",
        },
    )

    assert matches[200].match_kind == TEXTMAP_MATCH_KIND_NEW
    assert matches[200].predecessor_hash is None

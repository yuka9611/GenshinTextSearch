import os
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from textmap_match_utils import (
    TEXTMAP_MATCH_KIND_CROSS_HASH_SIMILAR,
    TextMapLineageState,
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

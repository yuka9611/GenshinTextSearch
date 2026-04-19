import os
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import textMapImport


def test_build_versioned_textmap_row_plan_inherits_versions_across_hash_change():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={3642337407: "same text"},
        existing_rows_by_hash={
            3642337919: ("same text", 70, 70),
        },
        version_id=86,
    )

    assert row_plan == {
        3642337407: ("same text", 70, 70),
    }


def test_build_versioned_textmap_row_plan_does_not_reuse_short_generic_versions_across_hash_change():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={100: "呀！"},
        existing_rows_by_hash={
            200: ("呀！", 10, 10),
        },
        version_id=30,
    )

    assert row_plan == {
        100: ("呀！", 30, 30),
    }


def test_build_versioned_textmap_row_plan_keeps_same_hash_same_content_versions():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={100: "same text"},
        existing_rows_by_hash={
            100: ("same text", 10, 12),
            200: ("other text", 20, 20),
        },
        version_id=30,
    )

    assert row_plan == {
        100: ("same text", 10, 12),
    }


def test_build_versioned_textmap_row_plan_keeps_same_hash_versions_for_short_generic_text():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={100: "呀！"},
        existing_rows_by_hash={
            100: ("呀！", 10, 12),
        },
        version_id=30,
    )

    assert row_plan == {
        100: ("呀！", 10, 12),
    }


def test_build_versioned_textmap_row_plan_inherits_created_for_same_hash_text_change():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={100: "new text"},
        existing_rows_by_hash={
            100: ("old text", 10, 10),
        },
        version_id=30,
    )

    assert row_plan == {
        100: ("new text", 10, 30),
    }


def test_build_versioned_textmap_row_plan_uses_stable_multiset_matching_for_duplicates():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={5: "same text", 20: "same text"},
        existing_rows_by_hash={
            20: ("same text", 10, 10),
        },
        version_id=30,
    )

    assert row_plan == {
        5: ("same text", 30, 30),
        20: ("same text", 10, 10),
    }


def test_build_versioned_textmap_row_plan_can_reuse_versions_from_another_hash_with_same_content():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={100: "same text"},
        existing_rows_by_hash={
            100: ("old text", 8, 8),
            200: ("same text", 10, 10),
        },
        version_id=30,
    )

    assert row_plan == {
        100: ("same text", 10, 10),
    }


def test_build_versioned_textmap_row_plan_does_not_reuse_broad_short_responses_across_hash_change():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={100: "谢谢。"},
        existing_rows_by_hash={
            200: ("谢谢。", 10, 10),
        },
        version_id=30,
    )

    assert row_plan == {
        100: ("谢谢。", 30, 30),
    }


def test_build_versioned_textmap_row_plan_inherits_created_from_similar_cross_hash_predecessor():
    row_plan = textMapImport._build_versioned_textmap_row_plan(
        current_obj={200: "I wanted to test it out, so I took a related order."},
        existing_rows_by_hash={
            100: ("I wanted to test it, so I took a related order.", 10, 10),
        },
        version_id=30,
    )

    assert row_plan == {
        200: ("I wanted to test it out, so I took a related order.", 10, 30),
    }

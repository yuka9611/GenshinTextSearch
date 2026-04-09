import os
import sqlite3
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import history_backfill


def test_build_textmap_history_update_rows_ignores_unrelated_group_changes():
    rows = history_backfill._build_textmap_history_update_rows(
        snapshot_obj={"100": "same text"},
        previous_snapshot_obj={"100": "same text"},
        current_obj={"100": "same text"},
        lang_id=1,
        version_id=30,
        existing_map={100: (10, None)},
    )

    assert rows == []


def test_build_textmap_history_update_rows_marks_updated_when_hash_text_changes():
    rows = history_backfill._build_textmap_history_update_rows(
        snapshot_obj={"100": "current text"},
        previous_snapshot_obj={"100": "old text"},
        current_obj={"100": "current text"},
        lang_id=1,
        version_id=30,
        existing_map={100: (10, None)},
    )

    assert rows == [(10, 30, 1, 100)]


def test_build_textmap_history_update_rows_treats_hash_reappearance_as_update():
    rows = history_backfill._build_textmap_history_update_rows(
        snapshot_obj={"100": "current text"},
        previous_snapshot_obj={},
        current_obj={"100": "current text"},
        lang_id=1,
        version_id=30,
        existing_map={100: (10, None)},
    )

    assert rows == [(10, 30, 1, 100)]


def test_load_textmap_version_cache_for_current_group_reads_requested_hashes():
    conn = sqlite3.connect(":memory:")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE textMap (
                hash INTEGER,
                lang INTEGER,
                created_version_id INTEGER,
                updated_version_id INTEGER
            )
            """
        )
        cursor.executemany(
            "INSERT INTO textMap(hash, lang, created_version_id, updated_version_id) VALUES (?, ?, ?, ?)",
            [
                (100, 1, 10, 20),
                (200, 1, 11, 21),
                (300, 2, 12, 22),
            ],
        )

        rows = history_backfill._load_textmap_version_cache_for_current_group(
            cursor,
            lang_id=1,
            current_obj={"100": "a", "200": "b", "999": "c"},
            batch_size=2,
        )

        assert rows == {
            100: (10, 20),
            200: (11, 21),
        }
    finally:
        conn.close()

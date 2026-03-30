import sqlite3
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import history_backfill
from history_backfill import VersionSnapshot
from versioning import _version_tag_to_sort_key


def _should_update_version(existing_id, new_id, is_created):
    if new_id is None:
        return False
    if existing_id is None:
        return True
    if is_created:
        return int(new_id) < int(existing_id)
    return int(new_id) > int(existing_id)


def _executemany_batched(cursor, sql, rows, batch_size=1000):
    del batch_size
    cursor.executemany(sql, rows)


def _no_op(*args, **kwargs):
    del args, kwargs
    return 0


def _empty_exception_report(*args, **kwargs):
    del args, kwargs
    return {"created_after_updated": 0}


class SnapshotReplayRangeTests(unittest.TestCase):
    def setUp(self):
        history_backfill._git_cache.clear()
        history_backfill._snapshot_metadata_cache.clear()
        history_backfill._snapshot_textmap_file_groups_cache.clear()
        history_backfill._snapshot_textmap_group_cache.clear()

    def test_build_snapshot_specs_ignores_unversioned_and_keeps_last_commit_per_version(self):
        commit_rows = [
            ("c0", "bootstrap"),
            ("c1", "release 1.0"),
            ("c2", "follow-up without version"),
            ("c3", "release 1.0 hotfix"),
            ("c4", "release 3.2"),
            ("c5", "release 3.0 rerun"),
        ]

        commit_to_version_tag, snapshot_specs = history_backfill._build_snapshot_specs_from_commit_rows(
            commit_rows
        )

        self.assertIsNone(commit_to_version_tag["c0"])
        self.assertEqual(commit_to_version_tag["c2"], "1.0")
        self.assertEqual(commit_to_version_tag["c5"], "3.0")
        self.assertEqual(
            [(version_tag, commit_sha) for version_tag, _label, commit_sha, _sort_key in snapshot_specs],
            [("1.0", "c3"), ("3.0", "c5"), ("3.2", "c4")],
        )

    def test_resolve_snapshot_replay_range_noops_when_commits_fold_to_same_version(self):
        snapshots = (
            VersionSnapshot("1.0", "release 1.0", 1, "s1", _version_tag_to_sort_key("1.0") or 0),
            VersionSnapshot("1.1", "release 1.1", 2, "s2", _version_tag_to_sort_key("1.1") or 0),
        )
        metadata = {
            "snapshots": snapshots,
            "snapshot_by_tag": {snapshot.version_tag: snapshot for snapshot in snapshots},
            "commit_to_version_tag": {
                "c1": "1.0",
                "c2": "1.0",
                "c3": "1.1",
            },
        }

        with ExitStack() as stack:
            stack.enter_context(patch.object(history_backfill, "_resolve_commit", side_effect=lambda _repo, rev: rev))
            stack.enter_context(
                patch.object(history_backfill, "_get_version_snapshot_metadata", return_value=metadata)
            )
            replay_range = history_backfill._resolve_snapshot_replay_range(
                "/repo",
                target_commit="c2",
                from_commit="c1",
            )

        self.assertEqual(replay_range.target_snapshot.version_tag, "1.0")
        self.assertEqual(replay_range.from_snapshot.version_tag, "1.0")
        self.assertEqual(replay_range.base_snapshot.version_tag, "1.0")
        self.assertEqual(replay_range.snapshots, ())


class TableSnapshotReplayTests(unittest.TestCase):
    def setUp(self):
        history_backfill._git_cache.clear()
        history_backfill._snapshot_metadata_cache.clear()
        history_backfill._snapshot_textmap_file_groups_cache.clear()
        history_backfill._snapshot_textmap_group_cache.clear()

    def test_textmap_uses_snapshot_group_state_for_created_and_updated(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE textMap (lang INTEGER, hash INTEGER, created_version_id INTEGER, updated_version_id INTEGER)"
            )
            conn.execute(
                "INSERT INTO textMap (lang, hash, created_version_id, updated_version_id) VALUES (1, 100, NULL, NULL)"
            )

            snapshot_groups = {
                "c1": {"100": "old"},
                "c2": {"100": "mid"},
                "c3": {"100": "final"},
            }

            def fake_backfill(**kwargs):
                cursor = conn.cursor()
                try:
                    process_entry = kwargs["process_entry_fn"]
                    process_entry(
                        cursor,
                        "/repo",
                        "c1",
                        None,
                        {"action": "A", "old_path": None, "new_path": "TextMap/TextMapCHS.json"},
                        1,
                        "1.0",
                        100,
                    )
                    process_entry(
                        cursor,
                        "/repo",
                        "c2",
                        "c1",
                        {"action": "M", "old_path": None, "new_path": "TextMap/TextMapCHS.json"},
                        2,
                        "1.1",
                        100,
                    )
                    process_entry(
                        cursor,
                        "/repo",
                        "c3",
                        "c2",
                        {"action": "D", "old_path": "TextMap/TextMapCHS_1.json", "new_path": None},
                        3,
                        "1.2",
                        100,
                    )
                    conn.commit()
                finally:
                    cursor.close()

            with ExitStack() as stack:
                stack.enter_context(patch.object(history_backfill, "conn", conn))
                stack.enter_context(patch.object(history_backfill, "_backfill_versions_from_history", new=fake_backfill))
                stack.enter_context(
                    patch.object(history_backfill, "_get_textmap_lang_id_map", return_value={"TextMapCHS.json": 1})
                )
                stack.enter_context(
                    patch.object(history_backfill, "_load_worktree_textmap_group", return_value={"100": "final"})
                )
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_load_snapshot_textmap_group",
                        side_effect=lambda _repo, commit_sha, _base_name: snapshot_groups.get(commit_sha),
                    )
                )
                stack.enter_context(patch.object(history_backfill, "should_update_version", new=_should_update_version))
                stack.enter_context(patch.object(history_backfill, "executemany_batched", new=_executemany_batched))
                stack.enter_context(patch.object(history_backfill, "_backfill_textmap_git_versions", new=_no_op))
                stack.enter_context(
                    patch.object(history_backfill, "analyze_textmap_version_exceptions", new=_empty_exception_report)
                )
                stack.enter_context(patch.object(history_backfill, "report_version_exceptions", new=_no_op))
                stack.enter_context(patch.object(history_backfill, "fix_created_after_updated_versions", new=_no_op))
                history_backfill.backfill_textmap_versions_from_history()

            row = conn.execute(
                "SELECT created_version_id, updated_version_id FROM textMap WHERE lang = 1 AND hash = 100"
            ).fetchone()
            self.assertEqual(row, (1, 3))
        finally:
            conn.close()

    def test_readable_resets_created_version_after_delete_and_reappear(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE readable (fileName TEXT, lang TEXT, created_version_id INTEGER, updated_version_id INTEGER)"
            )
            conn.execute(
                "INSERT INTO readable (fileName, lang, created_version_id, updated_version_id) VALUES ('book.txt', 'EN', NULL, NULL)"
            )

            text_by_commit = {
                ("c1", "Readable/EN/book.txt"): "old",
                ("c3", "Readable/EN/book.txt"): "final",
            }

            def fake_backfill(**kwargs):
                cursor = conn.cursor()
                try:
                    process_entry = kwargs["process_entry_fn"]
                    process_entry(
                        cursor,
                        "/repo",
                        "c1",
                        None,
                        {"action": "A", "old_path": None, "new_path": "Readable/EN/book.txt"},
                        1,
                        "1.0",
                        100,
                    )
                    process_entry(
                        cursor,
                        "/repo",
                        "c2",
                        "c1",
                        {"action": "D", "old_path": "Readable/EN/book.txt", "new_path": None},
                        2,
                        "1.1",
                        100,
                    )
                    process_entry(
                        cursor,
                        "/repo",
                        "c3",
                        "c2",
                        {"action": "A", "old_path": None, "new_path": "Readable/EN/book.txt"},
                        3,
                        "1.2",
                        100,
                    )
                    conn.commit()
                finally:
                    cursor.close()

            with ExitStack() as stack:
                stack.enter_context(patch.object(history_backfill, "conn", conn))
                stack.enter_context(patch.object(history_backfill, "_backfill_versions_from_history", new=fake_backfill))
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_git_show_text",
                        side_effect=lambda _repo, commit_sha, rel_path: text_by_commit.get((commit_sha, rel_path)),
                    )
                )
                stack.enter_context(patch.object(history_backfill, "_read_worktree_text", return_value="final"))
                stack.enter_context(patch.object(history_backfill, "should_update_version", new=_should_update_version))
                stack.enter_context(patch.object(history_backfill, "_backfill_git_versions", new=_no_op))
                stack.enter_context(
                    patch.object(history_backfill, "analyze_readable_version_exceptions", new=_empty_exception_report)
                )
                stack.enter_context(patch.object(history_backfill, "report_version_exceptions", new=_no_op))
                stack.enter_context(patch.object(history_backfill, "fix_created_after_updated_versions", new=_no_op))
                history_backfill.backfill_readable_versions_from_history()

            row = conn.execute(
                "SELECT created_version_id, updated_version_id FROM readable WHERE fileName = 'book.txt' AND lang = 'EN'"
            ).fetchone()
            self.assertEqual(row, (3, 3))
        finally:
            conn.close()

    def test_subtitle_only_advances_updated_when_text_changes(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE subtitle (subtitleKey TEXT PRIMARY KEY, fileName TEXT, lang INTEGER, created_version_id INTEGER, updated_version_id INTEGER)"
            )
            conn.execute(
                "INSERT INTO subtitle (subtitleKey, fileName, lang, created_version_id, updated_version_id) "
                "VALUES ('scene_4_0_1', 'scene', 4, NULL, NULL)"
            )

            text_by_commit = {
                ("c1", "Subtitle/EN/scene.srt"): "old",
                ("c2", "Subtitle/EN/scene.srt"): "final",
                ("c3", "Subtitle/EN/scene.srt"): "timing-only",
            }
            rows_by_text = {
                "old": {"scene_4_0_1": "old"},
                "final": {"scene_4_0_1": "final"},
                "timing-only": {"scene_4_0_1": "final"},
            }

            def fake_backfill(**kwargs):
                cursor = conn.cursor()
                try:
                    process_entry = kwargs["process_entry_fn"]
                    for commit_sha, parent_sha, version_id in (("c1", None, 1), ("c2", "c1", 2), ("c3", "c2", 3)):
                        process_entry(
                            cursor,
                            "/repo",
                            commit_sha,
                            parent_sha,
                            {"action": "M", "old_path": None, "new_path": "Subtitle/EN/scene.srt"},
                            version_id,
                            f"1.{version_id - 1}",
                            100,
                        )
                    conn.commit()
                finally:
                    cursor.close()

            with ExitStack() as stack:
                stack.enter_context(patch.object(history_backfill, "conn", conn))
                stack.enter_context(patch.object(history_backfill, "_backfill_versions_from_history", new=fake_backfill))
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_git_show_text",
                        side_effect=lambda _repo, commit_sha, rel_path: text_by_commit.get((commit_sha, rel_path)),
                    )
                )
                stack.enter_context(patch.object(history_backfill, "_read_worktree_text", return_value="final"))
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_parse_srt_rows",
                        side_effect=lambda text, _lang_id, _rel: rows_by_text.get(text, {}),
                    )
                )
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_subtitle_text_changed_keys",
                        side_effect=lambda history_rows, current_rows: {
                            key
                            for key, value in history_rows.items()
                            if current_rows.get(key) != value
                        },
                    )
                )
                stack.enter_context(patch.object(history_backfill, "should_update_version", new=_should_update_version))
                stack.enter_context(patch.object(history_backfill, "executemany_batched", new=_executemany_batched))
                stack.enter_context(patch.object(history_backfill, "_backfill_git_versions", new=_no_op))
                stack.enter_context(
                    patch.object(history_backfill, "analyze_subtitle_version_exceptions", new=_empty_exception_report)
                )
                stack.enter_context(patch.object(history_backfill, "report_version_exceptions", new=_no_op))
                stack.enter_context(patch.object(history_backfill, "fix_created_after_updated_versions", new=_no_op))
                history_backfill.backfill_subtitle_versions_from_history()

            row = conn.execute(
                "SELECT created_version_id, updated_version_id FROM subtitle WHERE subtitleKey = 'scene_4_0_1'"
            ).fetchone()
            self.assertEqual(row, (1, 2))
        finally:
            conn.close()

    def test_npc_reappearance_is_treated_as_new_creation_in_snapshot_mode(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute("CREATE TABLE npc (npcId INTEGER PRIMARY KEY, created_version_id INTEGER)")
            conn.execute("INSERT INTO npc (npcId, created_version_id) VALUES (7, NULL)")

            npc_rows = {
                ("c1", "ExcelBinOutput/NpcExcelConfigData.json"): [{"id": 7}],
                ("c2", "ExcelBinOutput/NpcExcelConfigData.json"): [],
                ("c3", "ExcelBinOutput/NpcExcelConfigData.json"): [{"id": 7}],
            }

            def fake_backfill(**kwargs):
                cursor = conn.cursor()
                try:
                    process_entry = kwargs["process_entry_fn"]
                    for commit_sha, parent_sha, version_id in (("c1", None, 1), ("c2", "c1", 2), ("c3", "c2", 3)):
                        process_entry(
                            cursor,
                            "/repo",
                            commit_sha,
                            parent_sha,
                            {
                                "action": "M",
                                "old_path": None,
                                "new_path": "ExcelBinOutput/NpcExcelConfigData.json",
                            },
                            version_id,
                            f"1.{version_id - 1}",
                            100,
                        )
                    conn.commit()
                finally:
                    cursor.close()

            with ExitStack() as stack:
                stack.enter_context(patch.object(history_backfill, "conn", conn))
                stack.enter_context(patch.object(history_backfill, "_backfill_versions_from_history", new=fake_backfill))
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_git_show_json",
                        side_effect=lambda _repo, commit_sha, rel_path: npc_rows.get((commit_sha, rel_path)),
                    )
                )
                history_backfill.backfill_npc_versions_from_history()

            row = conn.execute("SELECT created_version_id FROM npc WHERE npcId = 7").fetchone()
            self.assertEqual(row, (3,))
        finally:
            conn.close()

    def test_quest_snapshot_helper_backfills_normal_anecdote_and_hangout(self):
        conn = sqlite3.connect(":memory:")
        try:
            conn.execute(
                "CREATE TABLE quest (questId INTEGER PRIMARY KEY, created_version_id INTEGER, git_created_version_id INTEGER)"
            )
            conn.executemany(
                "INSERT INTO quest (questId, created_version_id, git_created_version_id) VALUES (?, NULL, NULL)",
                [(101,), (202,), (303,)],
            )
            cursor = conn.cursor()

            quest_payloads = {
                ("c5", "BinOutput/Quest/101.json"): {"id": 101},
                ("c5", history_backfill.ANECDOTE_CONFIG_PATH): [{"questId": 202}],
                ("c5", history_backfill.MAIN_COOP_CONFIG_PATH): [{"id": 30300}],
            }

            with ExitStack() as stack:
                stack.enter_context(patch.object(history_backfill, "conn", conn))
                stack.enter_context(patch.object(history_backfill, "should_update_version", new=_should_update_version))
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_git_show_json",
                        side_effect=lambda _repo, commit_sha, rel_path: quest_payloads.get((commit_sha, rel_path)),
                    )
                )
                stack.enter_context(
                    patch.object(history_backfill, "_extract_quest_row", side_effect=lambda obj: (obj["id"],))
                )
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_extract_anecdote_history_row",
                        side_effect=lambda row: (row["questId"],),
                    )
                )
                stack.enter_context(
                    patch.object(
                        history_backfill,
                        "_load_hangout_history_payload",
                        side_effect=lambda _repo, _commit_sha, quest_id: {"id": quest_id} if quest_id == 303 else None,
                    )
                )

                history_backfill._backfill_quest_version_by_commit_entry(
                    cursor,
                    repo_path="/repo",
                    commit_sha="c5",
                    parent_sha=None,
                    entry={"action": "A", "old_path": None, "new_path": "BinOutput/Quest/101.json"},
                    version_id=5,
                )
                history_backfill._backfill_quest_version_by_commit_entry(
                    cursor,
                    repo_path="/repo",
                    commit_sha="c5",
                    parent_sha=None,
                    entry={"action": "M", "old_path": None, "new_path": history_backfill.ANECDOTE_CONFIG_PATH},
                    version_id=5,
                )
                history_backfill._backfill_quest_version_by_commit_entry(
                    cursor,
                    repo_path="/repo",
                    commit_sha="c5",
                    parent_sha=None,
                    entry={"action": "M", "old_path": None, "new_path": history_backfill.MAIN_COOP_CONFIG_PATH},
                    version_id=5,
                )

            rows = conn.execute(
                "SELECT questId, created_version_id, git_created_version_id FROM quest ORDER BY questId"
            ).fetchall()
            self.assertEqual(
                rows,
                [
                    (101, 5, 5),
                    (202, 5, 5),
                    (303, 5, 5),
                ],
            )
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    unittest.main()

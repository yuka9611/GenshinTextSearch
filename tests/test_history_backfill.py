import os
import sqlite3
import sys
from contextlib import nullcontext
from types import SimpleNamespace


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import history_backfill
import textmap_match_utils


def test_extract_history_version_parts_applies_manual_overrides():
    assert history_backfill._extract_history_version_parts(
        "OSRELWin3.0.0_R11806263_S11787575_D11806263",
        commit_sha="1a6597f5a67382119494beae22a4039a1cefc8e1",
    ) == ("3.3", "3.3")
    assert history_backfill._extract_history_version_parts(
        "OSRELWin4.0.1_R17742988_S17600751_D177772935",
        commit_sha="4f872fefab5ed8c6c6b72899e47bcb0344416a4f",
    ) == ("4.1", "4.1")


def test_build_snapshot_specs_from_commit_rows_uses_last_commit_for_each_version_tag():
    commit_to_version_tag, snapshot_specs = history_backfill._build_snapshot_specs_from_commit_rows(
        [
            ("sha-63-1", "OSRELWin6.3.0_R123456_S123456_D123456"),
            ("sha-63-2", "follow-up fix without tag"),
            ("sha-64-1", "OSRELWin6.4.0_R123456_S123456_D123456"),
        ]
    )

    assert commit_to_version_tag == {
        "sha-63-1": "6.3",
        "sha-63-2": "6.3",
        "sha-64-1": "6.4",
    }
    assert [spec[0] for spec in snapshot_specs] == ["6.3", "6.4"]
    assert [spec[2] for spec in snapshot_specs] == ["sha-63-2", "sha-64-1"]


def test_build_snapshot_specs_from_commit_rows_applies_version_aliases():
    commit_to_version_tag, snapshot_specs = history_backfill._build_snapshot_specs_from_commit_rows(
        [
            ("sha-30-old", "OSRELWin3.0.0_R9624836_S9598838_D9617080"),
            ("sha-31", "OSRELWin3.1.0_R10457664_S10289512_D10457664"),
            ("sha-32", "OSRELWin3.2.0_R11078128_S10998085_D11077560"),
            ("1a6597f5a67382119494beae22a4039a1cefc8e1", "OSRELWin3.0.0_R11806263_S11787575_D11806263"),
            ("sha-33-tail", "Merge branch 'master'"),
            ("sha-34", "OSRELWin3.4.0_R12591909_S12591017_D12591425"),
            ("sha-40", "OSRELWin4.0.0_R17100363_S17041295_D17098349"),
            ("sha-41", "OSRELWin4.0.1_R17742988_S17600751_D177772935"),
            ("sha-41-tail", "Hi?"),
            ("sha-42", "OSRELWin4.2.0_R18963989_S18955874_D18963950"),
        ]
    )

    assert commit_to_version_tag["1a6597f5a67382119494beae22a4039a1cefc8e1"] == "3.3"
    assert commit_to_version_tag["sha-33-tail"] == "3.3"
    assert commit_to_version_tag["sha-41"] == "4.1"
    assert commit_to_version_tag["sha-41-tail"] == "4.1"
    assert [spec[0] for spec in snapshot_specs] == ["3.0", "3.1", "3.2", "3.3", "3.4", "4.0", "4.1", "4.2"]
    assert [spec[2] for spec in snapshot_specs] == [
        "sha-30-old",
        "sha-31",
        "sha-32",
        "sha-33-tail",
        "sha-34",
        "sha-40",
        "sha-41-tail",
        "sha-42",
    ]


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


def test_build_textmap_history_update_rows_backfills_earlier_versions_across_hash_change():
    rows = history_backfill._build_textmap_history_update_rows(
        snapshot_obj={"200": "current text"},
        previous_snapshot_obj={},
        current_obj={"100": "current text"},
        lang_id=1,
        version_id=70,
        existing_map={100: (86, 86)},
    )

    assert rows == [(70, 70, 1, 100)]


def test_build_textmap_history_update_rows_does_not_mark_file_only_hash_migration_as_update():
    rows = history_backfill._build_textmap_history_update_rows(
        snapshot_obj={"100": "current text"},
        previous_snapshot_obj={"200": "current text"},
        current_obj={"100": "current text"},
        lang_id=1,
        version_id=86,
        existing_map={100: (70, 70)},
    )

    assert rows == []


def test_build_textmap_history_update_rows_allocates_duplicate_texts_stably():
    rows = history_backfill._build_textmap_history_update_rows(
        snapshot_obj={"150": "same text"},
        previous_snapshot_obj={},
        current_obj={"100": "same text", "200": "same text"},
        lang_id=1,
        version_id=30,
        existing_map={
            100: (86, 86),
            200: (86, 86),
        },
    )

    assert rows == [(30, 30, 1, 100)]


def test_compute_textmap_group_authoritative_versions_prefers_same_content_predecessor_over_reused_hash(monkeypatch):
    history_backfill._clear_history_runtime_caches()
    snapshots = (
        history_backfill.VersionSnapshot("6.0", "6.0", 60, "sha60", 60),
        history_backfill.VersionSnapshot("6.4", "6.4", 64, "sha64", 64),
        history_backfill.VersionSnapshot("6.5", "6.5", 65, "sha65", 65),
    )
    snapshot_payloads = {
        ("sha64", "TextMapEN.json"): {
            "36805634": "It... It was Paimon...",
            "36806146": "I wanted to test it out, so I took a related order.",
        },
        ("sha60", "TextMapEN.json"): {
            "36806146": "I wanted to test it out, so I took a related order.",
        },
    }

    monkeypatch.setattr(
        history_backfill,
        "_load_snapshot_textmap_group",
        lambda repo_path, commit_sha, base_name: snapshot_payloads.get((commit_sha, base_name)),
    )

    version_plan = history_backfill._compute_textmap_group_authoritative_versions(
        repo_path="/tmp/fake-repo",
        base_name="TextMapEN.json",
        current_obj={
            "36805634": "I wanted to test it out, so I took a related order.",
        },
        target_hashes=[36805634],
        snapshots=snapshots,
    )

    assert version_plan == {
        36805634: (60, 60),
    }


def test_compute_textmap_group_authoritative_versions_does_not_reuse_short_generic_same_content_across_hash(monkeypatch):
    history_backfill._clear_history_runtime_caches()
    snapshots = (
        history_backfill.VersionSnapshot("5.0", "5.0", 50, "sha50", 50),
        history_backfill.VersionSnapshot("6.5", "6.5", 65, "sha65", 65),
    )
    snapshot_payloads = {
        ("sha50", "TextMapCHS.json"): {
            "100": "呀！",
        },
    }

    monkeypatch.setattr(
        history_backfill,
        "_load_snapshot_textmap_group",
        lambda repo_path, commit_sha, base_name: snapshot_payloads.get((commit_sha, base_name)),
    )

    version_plan = history_backfill._compute_textmap_group_authoritative_versions(
        repo_path="/tmp/fake-repo",
        base_name="TextMapCHS.json",
        current_obj={
            "200": "呀！",
        },
        target_hashes=[200],
        snapshots=snapshots,
    )

    assert version_plan == {
        200: (65, 65),
    }


def test_compute_textmap_group_authoritative_versions_does_not_reuse_short_generic_similar_text(monkeypatch):
    history_backfill._clear_history_runtime_caches()
    snapshots = (
        history_backfill.VersionSnapshot("5.0", "5.0", 50, "sha50", 50),
        history_backfill.VersionSnapshot("6.5", "6.5", 65, "sha65", 65),
    )
    snapshot_payloads = {
        ("sha50", "TextMapCHS.json"): {
            "100": "呀！",
        },
    }

    monkeypatch.setattr(
        history_backfill,
        "_load_snapshot_textmap_group",
        lambda repo_path, commit_sha, base_name: snapshot_payloads.get((commit_sha, base_name)),
    )

    version_plan = history_backfill._compute_textmap_group_authoritative_versions(
        repo_path="/tmp/fake-repo",
        base_name="TextMapCHS.json",
        current_obj={
            "200": "呀呀！",
        },
        target_hashes=[200],
        snapshots=snapshots,
    )

    assert version_plan == {
        200: (65, 65),
    }


def test_compute_textmap_group_authoritative_versions_marks_updated_when_hash_migration_and_text_change_happen_together(monkeypatch):
    history_backfill._clear_history_runtime_caches()
    snapshots = (
        history_backfill.VersionSnapshot("6.0", "6.0", 60, "sha60", 60),
        history_backfill.VersionSnapshot("6.5", "6.5", 65, "sha65", 65),
    )
    snapshot_payloads = {
        ("sha60", "TextMapEN.json"): {
            "100": "I wanted to test it, so I took a related order.",
        },
    }

    monkeypatch.setattr(
        history_backfill,
        "_load_snapshot_textmap_group",
        lambda repo_path, commit_sha, base_name: snapshot_payloads.get((commit_sha, base_name)),
    )

    version_plan = history_backfill._compute_textmap_group_authoritative_versions(
        repo_path="/tmp/fake-repo",
        base_name="TextMapEN.json",
        current_obj={
            "200": "I wanted to test it out, so I took a related order.",
        },
        target_hashes=[200],
        snapshots=snapshots,
    )

    assert version_plan == {
        200: (60, 65),
    }


def test_compute_textmap_group_authoritative_versions_recovers_created_version_from_similar_predecessor_in_large_group(monkeypatch):
    history_backfill._clear_history_runtime_caches()
    snapshots = (
        history_backfill.VersionSnapshot("6.3", "6.3", 63, "sha63", 63),
        history_backfill.VersionSnapshot("6.5", "6.5", 65, "sha65", 65),
    )
    previous_payload = {
        "3642337919": "是歌尘浪世真君所收的弟子吧。虽是凡人之躯，但毅力和灵性倒是可圈可点。",
    }
    previous_payload.update({str(500000 + idx): f"noise row {idx}" for idx in range(32)})
    snapshot_payloads = {
        ("sha63", "TextMapCHS.json"): previous_payload,
    }

    monkeypatch.setattr(
        history_backfill,
        "_load_snapshot_textmap_group",
        lambda repo_path, commit_sha, base_name: snapshot_payloads.get((commit_sha, base_name)),
    )
    monkeypatch.setattr(
        history_backfill,
        "match_textmap_lineage_to_previous",
        lambda current_states, previous_obj=None, previous_index=None: textmap_match_utils.match_textmap_lineage_to_previous(
            current_states,
            previous_obj,
            previous_index=previous_index,
            max_similarity_pairs=10,
        ),
    )

    version_plan = history_backfill._compute_textmap_group_authoritative_versions(
        repo_path="/tmp/fake-repo",
        base_name="TextMapCHS.json",
        current_obj={
            "3642337407": "是歌尘浪市真君所收的弟子吧。虽是凡人之躯，但毅力和灵性倒是可圈可点。",
        },
        target_hashes=[3642337407],
        snapshots=snapshots,
    )

    assert version_plan == {
        3642337407: (63, 65),
    }


def test_load_textmap_version_cache_for_current_group_reads_requested_hashes():
    conn = sqlite3.connect(":memory:")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE textMap (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            100: (1, 10, 20),
            200: (2, 11, 21),
        }
    finally:
        conn.close()


def test_load_textmap_version_cache_for_current_group_respects_target_hashes():
    conn = sqlite3.connect(":memory:")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE textMap (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                (300, 1, 12, 22),
            ],
        )

        rows = history_backfill._load_textmap_version_cache_for_current_group(
            cursor,
            lang_id=1,
            current_obj={"100": "a", "200": "b"},
            target_hashes={200, 300, 999},
            batch_size=2,
        )

        assert rows == {
            200: (2, 11, 21),
        }
    finally:
        conn.close()


def test_backfill_textmap_versions_from_history_replays_only_scoped_hashes(monkeypatch):
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE textMap (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash INTEGER,
                lang INTEGER,
                created_version_id INTEGER,
                updated_version_id INTEGER
            )
            """
        )
        conn.executemany(
            "INSERT INTO textMap(hash, lang, created_version_id, updated_version_id) VALUES (?, ?, ?, ?)",
            [
                (100, 1, 10, 10),
                (200, 1, 10, 10),
                (300, 2, 10, 10),
            ],
        )
        conn.commit()

        snapshot = history_backfill.VersionSnapshot("6.5", "6.5", 65, "sha65", 65)
        seen = []

        monkeypatch.setattr(history_backfill, "conn", conn)
        monkeypatch.setattr(
            history_backfill,
            "_resolve_snapshot_replay_range",
            lambda *_args, **_kwargs: SimpleNamespace(
                target_snapshot=snapshot,
                raw_target_commit="sha65",
            ),
        )
        monkeypatch.setattr(
            history_backfill,
            "_get_version_snapshot_metadata",
            lambda _repo_path: {"snapshots": (snapshot,)},
        )
        monkeypatch.setattr(
            history_backfill,
            "_get_textmap_lang_id_map",
            lambda: {"TextMapCHS.json": 1, "TextMapEN.json": 2},
        )
        monkeypatch.setattr(
            history_backfill,
            "_load_worktree_textmap_group",
            lambda _repo_path, base_name: {
                "TextMapCHS.json": {"100": "a", "200": "b"},
                "TextMapEN.json": {"300": "c"},
            }.get(base_name),
        )

        def fake_compute(**kwargs):
            seen.append((kwargs["base_name"], set(kwargs["target_hashes"])))
            return {hash_value: (65, 65) for hash_value in kwargs["target_hashes"]}

        monkeypatch.setattr(
            history_backfill,
            "_compute_textmap_group_authoritative_versions",
            fake_compute,
        )
        monkeypatch.setattr(history_backfill, "fast_import_pragmas", lambda *_args, **_kwargs: nullcontext())
        monkeypatch.setattr(history_backfill, "_backfill_textmap_git_versions", lambda *_args, **_kwargs: 0)
        monkeypatch.setattr(
            history_backfill,
            "analyze_textmap_version_exceptions",
            lambda _cursor: {"created_after_updated": 0},
        )
        monkeypatch.setattr(history_backfill, "report_version_exceptions", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(history_backfill, "rebuild_version_catalog", lambda *_args, **_kwargs: {})

        history_backfill.backfill_textmap_versions_from_history(
            target_commit="sha65",
            from_commit="sha64",
            refresh_version_catalog=False,
            target_hashes_by_base={"TextMapCHS.json": {200, 999}},
        )

        assert seen == [("TextMapCHS.json", {200})]
        assert conn.execute(
            "SELECT hash, created_version_id, updated_version_id FROM textMap ORDER BY hash"
        ).fetchall() == [
            (100, 10, 10),
            (200, 65, 65),
            (300, 10, 10),
        ]
    finally:
        conn.close()


def test_evict_snapshot_textmap_group_artifacts_releases_snapshot_caches():
    history_backfill._clear_history_runtime_caches()
    repo_path = "/tmp/fake-repo"
    commit_sha = "sha65"
    base_name = "TextMapCHS.json"
    cache_key = (repo_path, commit_sha, base_name)
    text_key = (commit_sha, "TextMap/TextMapCHS.json")

    history_backfill._snapshot_textmap_file_groups_cache[(repo_path, commit_sha)] = {
        base_name: ["TextMap/TextMapCHS.json"],
    }
    history_backfill._snapshot_textmap_group_cache[cache_key] = {"100": "a"}
    history_backfill._snapshot_textmap_group_index_cache[cache_key] = (
        textmap_match_utils.build_textmap_content_index({"100": "a"})
    )
    history_backfill._git_show_text_cache[text_key] = '{"100":"a"}'
    history_backfill._git_show_json_cache[text_key] = {"100": "a"}
    history_backfill._git_show_text_cache_order[:] = [text_key]

    history_backfill._evict_snapshot_textmap_group_artifacts(repo_path, commit_sha, base_name)

    assert cache_key not in history_backfill._snapshot_textmap_group_cache
    assert cache_key not in history_backfill._snapshot_textmap_group_index_cache
    assert text_key not in history_backfill._git_show_text_cache
    assert text_key not in history_backfill._git_show_json_cache
    assert text_key not in history_backfill._git_show_text_cache_order


def test_apply_textmap_version_patch_rows_updates_rows_by_id():
    conn = sqlite3.connect(":memory:")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE textMap (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            ],
        )

        changed = history_backfill._apply_textmap_version_patch_rows(
            cursor,
            [(1, 30, 40), (2, 31, 41)],
            batch_size=10,
        )

        assert changed == 2
        assert cursor.execute(
            "SELECT id, created_version_id, updated_version_id FROM textMap ORDER BY id"
        ).fetchall() == [
            (1, 30, 40),
            (2, 31, 41),
        ]
    finally:
        conn.close()

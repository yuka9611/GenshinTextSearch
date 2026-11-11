import os
import sqlite3
import sys

import pytest


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import subtitleImport
import diffUpdate
import version_control
from subtitle_utils import subtitle_key


def _subtitle_row(file_name, lang, start, end, content, version=1):
    return (
        file_name,
        lang,
        start,
        end,
        content,
        9,
        subtitle_key(file_name, lang, start, end),
        version,
        version,
    )


def _setup_subtitle_import(tmp_path, monkeypatch):
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE subtitle (
            fileName TEXT, lang INTEGER, startTime REAL, endTime REAL,
            content TEXT, subtitleId INTEGER, subtitleKey TEXT PRIMARY KEY,
            created_version_id INTEGER, updated_version_id INTEGER
        );
        CREATE TABLE version_dim (id INTEGER PRIMARY KEY, version_sort_key INTEGER);
        INSERT INTO version_dim VALUES (1, 7000), (2, 7001);
        """
    )
    monkeypatch.setattr(subtitleImport, "conn", connection)
    monkeypatch.setattr(subtitleImport, "DATA_PATH", str(tmp_path))
    monkeypatch.setattr(subtitleImport, "ensure_version_schema", lambda: None)
    monkeypatch.setattr(subtitleImport, "get_or_create_version_id", lambda _version: 2)
    monkeypatch.setattr(
        subtitleImport,
        "_load_subtitle_filename_map",
        lambda: {"scene": {"subtitleId": 10}},
    )
    monkeypatch.setattr(version_control, "conn", connection)
    monkeypatch.setattr(version_control, "_version_sort_key_cache", {})
    monkeypatch.setattr(version_control, "_version_sort_key_cache_primed", False)
    source_dir = tmp_path / "Subtitle" / "EN"
    source_dir.mkdir(parents=True)
    return connection, source_dir / "scene.srt"


def test_changed_subtitle_file_prunes_old_timestamps_and_keeps_versions(tmp_path, monkeypatch):
    connection, source_file = _setup_subtitle_import(tmp_path, monkeypatch)
    connection.executemany(
        "INSERT INTO subtitle VALUES (?,?,?,?,?,?,?,?,?)",
        [
            _subtitle_row("scene", 4, 1, 2, "unchanged"),
            _subtitle_row("scene", 4, 2, 3, "moved"),
            _subtitle_row("scene", 4, 3, 4, "removed"),
            ("scene", 4, 8, 9, "legacy keyless", 9, None, 1, 1),
            _subtitle_row("scene", 4, 4, 5, "old text"),
            _subtitle_row("scene", 4, 6, 7, "old text and time"),
            _subtitle_row("other", 4, 3, 4, "unrelated"),
            _subtitle_row("scene", 1, 3, 4, "other language"),
        ],
    )
    connection.commit()
    source_file.write_text(
        "1\n00:00:01,000 --> 00:00:02,000\nunchanged\n\n"
        "2\n00:00:02,500 --> 00:00:03,500\nmoved\n\n"
        "3\n00:00:04,000 --> 00:00:05,000\nnew text\n\n"
        "4\n00:00:06,500 --> 00:00:07,500\nnew text and time\n",
        encoding="utf-8",
    )

    for _ in range(2):
        subtitleImport.importSubtitlesByFiles(
            ["Subtitle/EN/scene.srt"], [], current_version="7.1"
        )
        rows = connection.execute(
            "SELECT subtitleKey, content, created_version_id, updated_version_id "
            "FROM subtitle ORDER BY subtitleKey"
        ).fetchall()
        assert rows == [
            (subtitle_key("other", 4, 3, 4), "unrelated", 1, 1),
            (subtitle_key("scene", 1, 3, 4), "other language", 1, 1),
            (subtitle_key("scene", 4, 1, 2), "unchanged", 1, 1),
            (subtitle_key("scene", 4, 2.5, 3.5), "moved", 1, 1),
            (subtitle_key("scene", 4, 4, 5), "new text", 1, 2),
            (subtitle_key("scene", 4, 6.5, 7.5), "new text and time", 2, 2),
        ]
    connection.close()


def test_changed_empty_subtitle_file_removes_only_its_old_rows(tmp_path, monkeypatch):
    connection, source_file = _setup_subtitle_import(tmp_path, monkeypatch)
    connection.executemany(
        "INSERT INTO subtitle VALUES (?,?,?,?,?,?,?,?,?)",
        [
            _subtitle_row("scene", 4, 1, 2, "old"),
            _subtitle_row("scene", 1, 1, 2, "other language"),
        ],
    )
    connection.commit()
    source_file.write_text("", encoding="utf-8")

    subtitleImport.importSubtitlesByFiles(
        ["Subtitle/EN/scene.srt"], [], current_version="7.1"
    )

    assert connection.execute(
        "SELECT subtitleKey FROM subtitle ORDER BY subtitleKey"
    ).fetchall() == [(subtitle_key("scene", 1, 1, 2),)]
    connection.close()


def test_changed_subtitle_parse_error_keeps_old_rows(tmp_path, monkeypatch):
    connection, source_file = _setup_subtitle_import(tmp_path, monkeypatch)
    old_row = _subtitle_row("scene", 4, 1, 2, "old")
    connection.execute("INSERT INTO subtitle VALUES (?,?,?,?,?,?,?,?,?)", old_row)
    connection.commit()
    source_file.write_text("changed", encoding="utf-8")

    def fail_parse(_content):
        raise ValueError("invalid subtitle")

    monkeypatch.setattr(subtitleImport, "iter_srt_entries", fail_parse)
    with pytest.raises(RuntimeError, match="Subtitle diff import incomplete"):
        subtitleImport.importSubtitlesByFiles(
            ["Subtitle/EN/scene.srt"], [], current_version="7.1"
        )

    assert connection.execute("SELECT * FROM subtitle").fetchall() == [old_row]
    connection.close()


def test_missing_changed_subtitle_file_rolls_back_other_file_updates(tmp_path, monkeypatch):
    connection, _source_file = _setup_subtitle_import(tmp_path, monkeypatch)
    old_row = _subtitle_row("other", 4, 1, 2, "old")
    connection.execute("INSERT INTO subtitle VALUES (?,?,?,?,?,?,?,?,?)", old_row)
    connection.commit()
    (tmp_path / "Subtitle" / "EN" / "other.srt").write_text(
        "1\n00:00:01,000 --> 00:00:02,000\nnew\n", encoding="utf-8"
    )

    with pytest.raises(RuntimeError, match="1 skipped paths"):
        subtitleImport.importSubtitlesByFiles(
            ["Subtitle/EN/other.srt", "Subtitle/EN/scene.srt"],
            [],
            current_version="7.1",
        )

    assert connection.execute("SELECT * FROM subtitle").fetchall() == [old_row]
    connection.close()


@pytest.mark.parametrize("path_style", ["git", "relative", "plan"])
def test_subtitle_file_creation_uses_earlier_majority_text_version(monkeypatch, path_style):
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE version_dim (
            id INTEGER PRIMARY KEY, raw_version TEXT, version_sort_key INTEGER
        );
        INSERT INTO version_dim VALUES
            (1, '5.5', 5005), (2, '5.6', 5006), (3, '5.7', 5007);
        CREATE TABLE subtitle (
            fileName TEXT, lang INTEGER, created_version_id INTEGER
        );
        CREATE TABLE source_file_version (
            path TEXT PRIMARY KEY, created_version TEXT,
            last_updated_version TEXT, updated_at TEXT
        );
        INSERT INTO source_file_version VALUES
            ('Subtitle/EN/older-text.srt', '5.7', '5.7', 'before'),
            ('Subtitle/EN/older-file.srt', '5.5', '5.7', 'before');
        INSERT INTO subtitle VALUES
            ('older-text', 4, 2), ('older-text', 4, 2),
            ('older-text', 4, 3), ('older-file', 4, 2);
        """
    )
    monkeypatch.setattr(diffUpdate, "conn", connection)
    paths = {"Subtitle/EN/older-text.srt", "Subtitle/EN/older-file.srt"}
    if path_style == "relative":
        paths = {path.removeprefix("Subtitle/") for path in paths}
    elif path_style == "plan":
        plan = diffUpdate._analyze_diff([
            {"action": "M", "old_path": None, "new_path": path} for path in paths
        ])
        paths = plan["subtitle_changed"]
        assert paths == {"EN\\older-text.srt", "EN\\older-file.srt"}

    assert diffUpdate._align_subtitle_file_created_versions(paths) == 1
    assert diffUpdate._align_subtitle_file_created_versions(paths) == 0
    assert connection.execute(
        "SELECT path, created_version, last_updated_version FROM source_file_version "
        "ORDER BY path"
    ).fetchall() == [
        ("Subtitle/EN/older-file.srt", "5.5", "5.7"),
        ("Subtitle/EN/older-text.srt", "5.6", "5.7"),
    ]
    assert connection.execute(
        "SELECT fileName, created_version_id, count(*) FROM subtitle "
        "GROUP BY fileName, created_version_id ORDER BY fileName, created_version_id"
    ).fetchall() == [
        ("older-file", 2, 1),
        ("older-text", 2, 2),
        ("older-text", 3, 1),
    ]
    connection.close()


def test_7_2_subtitle_localization_aliases_and_non_subtitle_exclusion():
    from localization_utils import build_subtitle_filename_map
    rows = [
        {"id": 9, "BFDDHOKGBOH": "Subtitle/CHS/Cs_a.srt", "OKIHBKFCDND": "Subtitle/CHS/Cs_b.srt"},
        {"id": 10, "BFDDHOKGBOH": "Readable/CHS/book.txt"},
    ]
    assert build_subtitle_filename_map(rows) == {"Cs_a": {"subtitleId": 9}, "Cs_b": {"subtitleId": 9}}


def test_7_2_document_localization_ids_keep_titles(tmp_path):
    import json
    from localization_utils import load_document_loc_title_hash
    excel = tmp_path / "ExcelBinOutput"
    excel.mkdir()
    (excel / "DocumentExcelConfigData.json").write_text(json.dumps([
        {"id": 100236, "titleTextMapHash": 1210651911, "PLOJOMIPJGH": [300008]},
        {"id": 100110, "titleTextMapHash": 272183383, "questIDList": [200001]},
    ]))
    assert load_document_loc_title_hash(str(tmp_path)) == {300008: 1210651911, 200001: 272183383}

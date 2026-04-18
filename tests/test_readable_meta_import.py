import json
import os
import sqlite3
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import readableMetaImport
import databaseHelper


def _write_excel_json(tmp_path, file_name: str, rows) -> None:
    excel_dir = tmp_path / "ExcelBinOutput"
    excel_dir.mkdir(parents=True, exist_ok=True)
    (excel_dir / file_name).write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")


def _create_readable_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE readable (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fileName TEXT,
            lang TEXT,
            content TEXT,
            titleTextMapHash INTEGER,
            readableId INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE TABLE textMap (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash INTEGER,
            content TEXT,
            lang INTEGER
        );
        """
    )


def _seed_readable_meta_fixture(connection: sqlite3.Connection, tmp_path) -> None:
    _create_readable_tables(connection)
    connection.executemany(
        """
        INSERT INTO readable(fileName, lang, content, titleTextMapHash, readableId, created_version_id, updated_version_id)
        VALUES (?, ?, ?, ?, ?, NULL, NULL)
        """,
        [
            ("Book2000.txt", "CHS", "书籍正文", 99887766, 200001),
            ("Book1140.txt", "CHS", "道具正文", 3377011063, 201140),
            ("Book1039.txt", "CHS", "阅读物正文", 297568295, 201039),
            ("Book1039_EN.txt", "EN", "Readable body", 297568295, 201039),
            ("Weapon11431_2.txt", "CHS", "武器故事", None, None),
        ],
    )
    connection.executemany(
        "INSERT INTO textMap(hash, content, lang) VALUES (?, ?, ?)",
        [
            (99887766, "提瓦特游览指南", 1),
            (3377011063, "莱茵多特的「礼物」", 1),
            (297568295, "审议之庭-至高领主共识评议会记录", 1),
            (359516492, "审议之庭-至高领主共识评议会记录", 1),
            (2842036365, "Gift description", 4),
        ],
    )
    readableMetaImport.ensure_readable_meta_schema(connection)
    connection.execute(
        "INSERT INTO readable_meta(normalized_file_name, readable_id, title_text_map_hash, readable_category) "
        "VALUES ('Old.txt', NULL, NULL, 'READABLE')"
    )
    connection.commit()

    _write_excel_json(
        tmp_path,
        "MaterialExcelConfigData.json",
        [
            {"id": 121001, "nameTextMapHash": 10001, "descTextMapHash": 10002},
            {"id": 121414, "nameTextMapHash": 3377011063, "descTextMapHash": 2842036365},
            {"id": 121221, "nameTextMapHash": 359516492},
        ],
    )
    _write_excel_json(
        tmp_path,
        "BooksCodexExcelConfigData.json",
        [{"materialId": 121001}],
    )
    _write_excel_json(
        tmp_path,
        "LocalizationExcelConfigData.json",
        [
            {"id": 200001, "enPath": "Readable/EN/Book2000.txt"},
            {"id": 201140, "enPath": "Readable/EN/Book1140.txt"},
            {"id": 201039, "enPath": "Readable/EN/Book1039_EN.txt"},
        ],
    )
    _write_excel_json(
        tmp_path,
        "DocumentExcelConfigData.json",
        [
            {"id": 121001, "titleTextMapHash": 99887766, "questIDList": [200001]},
            {"id": 121414, "titleTextMapHash": 3377011063, "questIDList": [201140]},
            {"id": 121221, "titleTextMapHash": 297568295, "questIDList": [201039]},
        ],
    )


def test_ensure_readable_meta_schema_creates_table_and_indexes():
    connection = sqlite3.connect(":memory:")

    readableMetaImport.ensure_readable_meta_schema(connection)

    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    indexes = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }

    assert "readable_meta" in tables
    assert "readable_meta_readable_category_index" in indexes
    assert "readable_meta_readable_id_index" in indexes
    assert "readable_meta_title_text_map_hash_index" in indexes


def test_refresh_readable_meta_rebuilds_categories_and_cleans_stale_rows(tmp_path):
    connection = sqlite3.connect(":memory:")
    _seed_readable_meta_fixture(connection, tmp_path)

    rows = readableMetaImport.refresh_readable_meta(connection=connection, data_path=str(tmp_path))

    assert rows == [
        ("Book1039.txt", 201039, 297568295, "READABLE"),
        ("Book1140.txt", 201140, 3377011063, "ITEM"),
        ("Book2000.txt", 200001, 99887766, "BOOK"),
        ("Weapon11431_2.txt", None, None, "WEAPON"),
    ]
    stored_rows = connection.execute(
        """
        SELECT normalized_file_name, readable_id, title_text_map_hash, readable_category
        FROM readable_meta
        ORDER BY normalized_file_name
        """
    ).fetchall()
    assert stored_rows == rows
    assert connection.execute(
        "SELECT COUNT(*) FROM readable_meta WHERE normalized_file_name='Old.txt'"
    ).fetchone()[0] == 0


def test_refresh_readable_meta_raises_on_normalized_file_name_collision(tmp_path):
    connection = sqlite3.connect(":memory:")
    _create_readable_tables(connection)
    connection.executemany(
        """
        INSERT INTO readable(fileName, lang, content, titleTextMapHash, readableId, created_version_id, updated_version_id)
        VALUES (?, ?, ?, ?, ?, NULL, NULL)
        """,
        [
            ("Book1039.txt", "CHS", "a", 297568295, 201039),
            ("Book1039_EN.txt", "EN", "b", 297568295, 999999),
        ],
    )
    _write_excel_json(tmp_path, "MaterialExcelConfigData.json", [])
    _write_excel_json(tmp_path, "BooksCodexExcelConfigData.json", [])
    _write_excel_json(tmp_path, "LocalizationExcelConfigData.json", [])
    _write_excel_json(tmp_path, "DocumentExcelConfigData.json", [])

    try:
        readableMetaImport.refresh_readable_meta(connection=connection, data_path=str(tmp_path))
    except RuntimeError as exc:
        assert "collision" in str(exc).lower()
    else:
        raise AssertionError("expected readable meta refresh to fail on normalized file name collision")


def test_database_helper_uses_readable_meta_for_sql_category_filters(tmp_path, monkeypatch):
    connection = sqlite3.connect(":memory:")
    _seed_readable_meta_fixture(connection, tmp_path)
    readableMetaImport.refresh_readable_meta(connection=connection, data_path=str(tmp_path))

    monkeypatch.setattr(databaseHelper, "conn", connection)
    databaseHelper._CACHE["table"].clear()
    databaseHelper._CACHE["column"].clear()

    item_rows = databaseHelper.selectReadableByVersion(1, "CHS", category="ITEM")
    readable_rows = databaseHelper.selectReadableByTitleKeyword("记录", 1, "CHS", category="READABLE")
    book_rows = databaseHelper.selectReadableFromKeyword("书籍", 1, "CHS", category="BOOK")

    assert [row[1] for row in item_rows] == [201140]
    assert [row[1] for row in readable_rows] == [201039]
    assert [row[3] for row in book_rows] == [200001]

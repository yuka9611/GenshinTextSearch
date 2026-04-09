"""Regression tests for textMap FTS trigger SQL."""
import os
import sqlite3
import sys


DBBUILD_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
DBBUILD_DIR = os.path.normpath(DBBUILD_DIR)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from textmap_fts_sql import (  # noqa: E402
    build_textmap_fts_ad_trigger_sql,
    build_textmap_fts_ai_trigger_sql,
    build_textmap_fts_au_trigger_sql,
    build_textmap_fts_table_sql,
)


def _build_temp_db(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.create_function("gts_fts_content", 2, lambda _lang, content: str(content or ""))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE textMap(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash INTEGER,
            content TEXT,
            lang INTEGER
        )
        """
    )
    langs_sql = "1,4,9"
    cur.execute(build_textmap_fts_table_sql("unicode61", "full", 0))
    cur.execute(build_textmap_fts_ai_trigger_sql(langs_sql))
    cur.execute(build_textmap_fts_ad_trigger_sql(langs_sql))
    cur.execute(build_textmap_fts_au_trigger_sql(langs_sql))
    conn.commit()
    return conn


def test_non_fts_languages_do_not_error_or_touch_fts(tmp_path):
    conn = _build_temp_db(tmp_path / "textmap-non-fts.db")
    cur = conn.cursor()

    cur.execute("INSERT INTO textMap(hash, content, lang) VALUES (?,?,?)", (100, "before", 2))
    conn.commit()
    cur.execute("UPDATE textMap SET content=? WHERE hash=? AND lang=?", ("after", 100, 2))
    conn.commit()
    cur.execute("DELETE FROM textMap WHERE hash=? AND lang=?", (100, 2))
    conn.commit()

    count = cur.execute("SELECT COUNT(*) FROM textMap_fts").fetchone()[0]
    assert count == 0
    conn.close()


def test_fts_languages_still_round_trip_through_index(tmp_path):
    conn = _build_temp_db(tmp_path / "textmap-fts.db")
    cur = conn.cursor()

    cur.execute("INSERT INTO textMap(hash, content, lang) VALUES (?,?,?)", (200, "hello world", 4))
    conn.commit()
    assert cur.execute("SELECT COUNT(*) FROM textMap_fts").fetchone()[0] == 1

    cur.execute("UPDATE textMap SET content=? WHERE hash=? AND lang=?", ("hello sqlite", 200, 4))
    conn.commit()
    rows = cur.execute("SELECT rowid, lang, hash FROM textMap_fts").fetchall()
    assert rows == [(1, 4, 200)]

    cur.execute("DELETE FROM textMap WHERE hash=? AND lang=?", (200, 4))
    conn.commit()
    assert cur.execute("SELECT COUNT(*) FROM textMap_fts").fetchone()[0] == 0
    conn.close()

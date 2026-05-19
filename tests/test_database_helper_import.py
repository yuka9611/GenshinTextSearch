import os
import sqlite3
import subprocess
import sys
import textwrap


def test_database_helper_import_keeps_dbbuild_versioning_importable():
    repo_root = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))
    script = textwrap.dedent(
        f"""
        import os
        import sys

        repo_root = {repo_root!r}
        server_dir = os.path.join(repo_root, "server")
        dbbuild_dir = os.path.join(server_dir, "dbBuild")
        if server_dir not in sys.path:
            sys.path.insert(0, server_dir)

        import databaseHelper  # noqa: F401

        if dbbuild_dir not in sys.path:
            sys.path.insert(0, dbbuild_dir)

        import history_backfill  # noqa: F401
        import versioning

        assert versioning.VERSION_DIM_TABLE == "version_dim"
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_version_filter_values_preserve_created_updated_split(monkeypatch):
    import databaseHelper

    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE version_dim (
            id INTEGER PRIMARY KEY,
            raw_version TEXT NOT NULL,
            version_tag TEXT,
            version_sort_key INTEGER
        );
        CREATE TABLE textMap (
            hash INTEGER,
            lang INTEGER,
            content TEXT,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE INDEX textMap_created_version_id_index ON textMap(created_version_id);
        CREATE INDEX textMap_updated_version_id_index ON textMap(updated_version_id);
        CREATE TABLE quest (
            questId INTEGER PRIMARY KEY,
            created_version_id INTEGER
        );
        CREATE TABLE quest_version (
            questId INTEGER,
            lang INTEGER,
            updated_version_id INTEGER,
            PRIMARY KEY (questId, lang)
        );
        CREATE INDEX quest_version_updated_version_id_index ON quest_version(updated_version_id);
        CREATE TABLE subtitle (
            subtitleId INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE TABLE readable (
            readableId INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE TABLE npc (
            npcId INTEGER,
            created_version_id INTEGER
        );
        """
    )
    connection.executemany(
        "INSERT INTO version_dim(id, raw_version, version_tag, version_sort_key) VALUES (?, ?, ?, ?)",
        [
            (1, "4.0", "4.0", 100400),
            (2, "4.1", "4.1", 100401),
            (3, "4.2", "4.2", 100402),
            (4, "", None, None),
        ],
    )
    connection.executemany(
        "INSERT INTO textMap(hash, lang, content, created_version_id, updated_version_id) VALUES (?, ?, ?, ?, ?)",
        [
            (100, 1, "created only", 1, 2),
            (101, 1, "duplicate created", 1, 2),
            (102, 1, "updated only", 3, 3),
            (103, 1, "blank ignored", 4, 4),
        ],
    )
    connection.execute("INSERT INTO quest(questId, created_version_id) VALUES (1, 1)")
    connection.execute("INSERT INTO quest_version(questId, lang, updated_version_id) VALUES (1, 1, 3)")
    connection.execute("INSERT INTO subtitle(subtitleId, created_version_id, updated_version_id) VALUES (1, 2, 3)")
    connection.execute("INSERT INTO readable(readableId, created_version_id, updated_version_id) VALUES (1, 1, 2)")
    connection.execute("INSERT INTO npc(npcId, created_version_id) VALUES (1, 2)")

    monkeypatch.setattr(databaseHelper, "conn", connection)
    for cache in databaseHelper._CACHE.values():
        if isinstance(cache, dict):
            cache.clear()

    filters = databaseHelper.getVersionFilterValues()

    assert sorted(filters["created"]) == ["4.0", "4.1", "4.2"]
    assert sorted(filters["updated"]) == ["4.1", "4.2"]

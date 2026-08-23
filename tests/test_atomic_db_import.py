import json
import sqlite3
import sys


DBBUILD_DIR = "/Users/yuka9/Downloads/GenshinTextSearch/server/dbBuild"
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import atomic_db_import


def _make_source(path):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE quest (questId INTEGER PRIMARY KEY, created_version_id INTEGER);
        CREATE TABLE quest_created_version_override (
            questId INTEGER PRIMARY KEY,
            locked_created_version_id INTEGER,
            current_created_version_id INTEGER,
            candidate_created_version_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            reason TEXT NOT NULL
        );
        CREATE TABLE quest_version_override_audit (
            audit_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL
        );
        """
    )
    connection.execute("INSERT INTO quest VALUES (303, 7)")
    connection.execute(
        "INSERT INTO quest_created_version_override VALUES (303, 7, 7, 25, 'audit', 'manual')"
    )
    connection.execute("INSERT INTO quest_version_override_audit VALUES (1, 'prepared')")
    connection.commit()
    connection.close()


def test_sqlite_backup_carries_manual_override_and_audit_rows(tmp_path):
    source = tmp_path / "source.db"
    backup = tmp_path / "backup.db"
    temporary = tmp_path / "data.db.tmp"
    _make_source(source)

    atomic_db_import._sqlite_backup(source, backup, "test")
    temporary.write_bytes(backup.read_bytes())

    connection = sqlite3.connect(temporary)
    assert connection.execute(
        "SELECT questId, locked_created_version_id, candidate_created_version_id "
        "FROM quest_created_version_override"
    ).fetchone() == (303, 7, 25)
    assert connection.execute(
        "SELECT status FROM quest_version_override_audit"
    ).fetchone() == ("prepared",)
    connection.close()


def test_integrity_check_rejects_no_corruption_and_summary_reads_provenance(tmp_path):
    source = tmp_path / "source.db"
    _make_source(source)

    assert atomic_db_import._integrity_check(source) == "ok"
    summary = atomic_db_import._database_summary(source)
    assert summary["manual_locked_quest_count"] == 1
    assert summary["quest_count"] == 1


def test_verify_prepared_provenance_checks_locked_and_quest_values(tmp_path):
    source = tmp_path / "source.db"
    audit = tmp_path / "audit.json"
    _make_source(source)
    audit.write_text(
        json.dumps(
            {
                "records": [
                    {
                        "questId": 303,
                        "status": "manual_difference",
                        "current_created_version_id": 7,
                        "candidate_created_version_id": 25,
                        "final_created_version_id": 7,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    assert atomic_db_import._verify_prepared_provenance(source, audit) == {
        "expected_locked": 1,
        "prepared_audit_rows": 1,
    }


def test_rebase_audit_preserves_existing_manual_locks_and_new_differences(tmp_path):
    source = tmp_path / "source.db"
    audit = tmp_path / "audit.json"
    rebased = tmp_path / "rebased.json"
    _make_source(source)
    audit.write_text(
        json.dumps(
            {
                "audit_kind": "fresh",
                "summary": {"candidate_count": 2, "difference_count": 1},
                "records": [
                    {
                        "questId": 303,
                        "status": "same",
                        "current_created_version_id": 7,
                        "candidate_created_version_id": 7,
                        "final_created_version_id": 7,
                    },
                    {
                        "questId": 404,
                        "status": "manual_difference",
                        "current_created_version_id": 8,
                        "candidate_created_version_id": 9,
                        "final_created_version_id": 8,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    result = atomic_db_import._rebase_audit_with_existing_manual_locks(
        source, audit, rebased
    )
    payload = json.loads(result.read_text(encoding="utf-8"))
    records = {row["questId"]: row for row in payload["records"]}

    assert records[303]["status"] == "manual_difference"
    assert records[303]["candidate_created_version_id"] == 25
    assert records[303]["pre_rebase_status"] == "same"
    assert records[404]["candidate_created_version_id"] == 9
    assert payload["summary"]["difference_count"] == 2

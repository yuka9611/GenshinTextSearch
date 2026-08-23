import json
import os
import sqlite3
import sys


SERVER_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir, "server"))
DBBUILD_DIR = os.path.join(SERVER_DIR, "dbBuild")
for path in (SERVER_DIR, DBBUILD_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

import quest_hash_map_utils
import version_control
from quest_version_provenance import (
    QUEST_CREATED_VERSION_OVERRIDE_TABLE,
    QUEST_TEXT_VERSION_TABLE,
    count_manual_locked_quests,
    calculate_quest_text_version_audit,
    load_manual_created_version_audit,
    manual_created_version_audit_is_prepared,
    mark_manual_created_version_audit_prepared,
    persist_manual_created_version_overrides,
    refresh_quest_text_versions,
)


def _db() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        """
        CREATE TABLE langCode(id INTEGER PRIMARY KEY, codeName TEXT, imported INTEGER);
        CREATE TABLE version_dim(
            id INTEGER PRIMARY KEY, raw_version TEXT, version_tag TEXT, version_sort_key INTEGER
        );
        CREATE TABLE textMap(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lang INTEGER NOT NULL,
            hash INTEGER NOT NULL,
            content TEXT,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE TABLE quest(
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            descTextMapHash INTEGER,
            longDescTextMapHash INTEGER,
            chapterId INTEGER,
            source_type TEXT,
            source_code_raw TEXT,
            created_version_id INTEGER,
            git_created_version_id INTEGER
        );
        CREATE TABLE chapter(
            chapterId INTEGER PRIMARY KEY,
            chapterTitleTextMapHash INTEGER,
            chapterNumTextMapHash INTEGER
        );
        CREATE TABLE questTalk(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            questId INTEGER,
            talkId INTEGER,
            stepTitleTextMapHash INTEGER,
            coopQuestId INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE dialogue(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            talkerType TEXT,
            talkerId INTEGER,
            talkId INTEGER,
            textHash INTEGER,
            dialogueId INTEGER UNIQUE,
            coopQuestId INTEGER
        );
        CREATE TABLE talk_dialogue_link(
            talkId INTEGER NOT NULL,
            coopQuestId INTEGER NOT NULL DEFAULT 0,
            dialogueId INTEGER NOT NULL,
            PRIMARY KEY(talkId, coopQuestId, dialogueId)
        );
        CREATE TABLE quest_hash_map(
            questId INTEGER NOT NULL,
            hash INTEGER NOT NULL,
            source_type TEXT NOT NULL,
            PRIMARY KEY(questId, hash, source_type)
        );
        CREATE TABLE quest_text_signature(
            questId INTEGER PRIMARY KEY,
            titleTextMapHash INTEGER,
            dialogue_signature TEXT NOT NULL
        );
        CREATE TABLE quest_version(
            questId INTEGER NOT NULL,
            lang INTEGER NOT NULL,
            updated_version_id INTEGER,
            PRIMARY KEY(questId, lang)
        );
        INSERT INTO langCode VALUES (1, 'TextMapCHS.json', 1);
        INSERT INTO langCode VALUES (2, 'TextMapEN.json', 1);
        INSERT INTO version_dim VALUES (1, 'Version 1.0', '1.0', 10);
        INSERT INTO version_dim VALUES (2, 'Version 2.0', '2.0', 20);
        INSERT INTO version_dim VALUES (3, 'Version 3.0', '3.0', 30);
        INSERT INTO version_dim VALUES (4, 'Version 4.0', '4.0', 40);
        INSERT INTO chapter VALUES (10, 901, 902);
        INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id)
        VALUES (1, 901, 'Chapter', 1, 1), (1, 902, 'Act', 1, 1);
        """
    )
    return connection


def _add_quest(
    connection: sqlite3.Connection,
    quest_id: int,
    title_hash: int,
    created_version_id: int | None,
    git_created_version_id: int | None,
    *,
    source_type: str | None = None,
    talk_id: int | None = None,
    step_hash: int | None = None,
    coop_quest_id: int = 0,
):
    connection.execute(
        "INSERT INTO quest VALUES (?,?,?,?,?,?,?,?,?)",
        (
            quest_id,
            title_hash,
            None,
            None,
            10,
            source_type,
            source_type,
            created_version_id,
            git_created_version_id,
        ),
    )
    connection.execute(
        "INSERT INTO quest_hash_map VALUES (?,?,?)",
        (quest_id, title_hash, "title"),
    )
    connection.execute(
        "INSERT INTO quest_text_signature VALUES (?,?,?)",
        (quest_id, title_hash, "signature"),
    )
    if talk_id is not None:
        connection.execute(
            "INSERT INTO questTalk(questId, talkId, stepTitleTextMapHash, coopQuestId) VALUES (?,?,?,?)",
            (quest_id, talk_id, step_hash, coop_quest_id),
        )


def test_manual_difference_same_and_unresolved_are_distinct_and_idempotent(monkeypatch):
    connection = _db()
    try:
        monkeypatch.setattr(version_control, "conn", connection)
        connection.executemany(
            "INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [
                (1, 1001, "Quest one", 2, 2),
                (1, 1002, "Quest two", 2, 2),
            ],
        )
        _add_quest(connection, 1, 1001, 1, 1)
        _add_quest(connection, 2, 1002, 2, 2)
        _add_quest(connection, 3, 1003, None, None)
        connection.commit()

        candidates = version_control.calculate_quest_created_version_candidates(
            connection.cursor()
        )
        assert candidates[1]["status"] == "manual_difference"
        assert candidates[1]["current_created_version_id"] == 1
        assert candidates[1]["candidate_created_version_id"] == 2
        assert candidates[1]["final_created_version_id"] == 1
        assert candidates[2]["status"] == "same"
        assert candidates[2]["final_created_version_id"] == 2
        assert candidates[3]["status"] == "unresolved"
        assert candidates[3]["candidate_created_version_id"] is None

        cursor = connection.cursor()
        stats = load_manual_created_version_audit
        del stats  # The file-backed path is exercised in the next test.
        from quest_version_provenance import persist_manual_created_version_overrides

        persisted = persist_manual_created_version_overrides(cursor, candidates)
        assert persisted["difference_count"] == 1
        assert persisted["locked_count"] == 1
        assert count_manual_locked_quests(cursor) == 1
        persisted_again = persist_manual_created_version_overrides(cursor, candidates)
        assert persisted_again["inserted_or_existing_locked_count"] == 1
        assert persisted_again["locked_count"] == 1
        assert count_manual_locked_quests(cursor) == 1
        assert connection.execute(
            f"SELECT locked_created_version_id, candidate_created_version_id FROM {QUEST_CREATED_VERSION_OVERRIDE_TABLE} WHERE questId=1"
        ).fetchone() == (1, 2)

        persisted_new_audit_path = persist_manual_created_version_overrides(
            cursor,
            candidates,
            source="later-equivalent-audit",
            reason="same-lock-new-audit-source",
        )
        assert persisted_new_audit_path["locked_count"] == 1
    finally:
        connection.close()


def test_manual_locked_created_version_survives_backfill_and_unlocked_row_updates(monkeypatch):
    connection = _db()
    try:
        monkeypatch.setattr(version_control, "conn", connection)
        connection.executemany(
            "INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [(1, 1101, "Locked", 2, 2), (1, 1102, "Unlocked", 2, 2)],
        )
        _add_quest(connection, 11, 1101, 1, 1)
        _add_quest(connection, 12, 1102, 3, 3)
        cursor = connection.cursor()
        mark_manual_created_version_audit_prepared(cursor, candidate_count=2, difference_count=1)
        from quest_version_provenance import persist_manual_created_version_overrides

        persist_manual_created_version_overrides(
            cursor,
            {
                11: {
                    "status": "manual_difference",
                    "current_created_version_id": 1,
                    "candidate_created_version_id": 2,
                }
            },
        )
        connection.commit()

        created_rows, _updated_rows = version_control.backfill_quest_created_version_from_textmap(
            cursor,
            quest_ids=[11, 12],
            authoritative=True,
            with_stats=True,
        )
        connection.commit()
        assert created_rows == 1
        assert connection.execute("SELECT created_version_id FROM quest WHERE questId=11").fetchone()[0] == 1
        assert connection.execute("SELECT created_version_id FROM quest WHERE questId=12").fetchone()[0] == 2
    finally:
        connection.close()


def test_file_audit_load_is_stale_checked_and_repeatable(tmp_path):
    connection = _db()
    try:
        connection.execute(
            "INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            (1, 1201, "Audited", 2, 2),
        )
        _add_quest(connection, 21, 1201, 1, 1)
        records = [
            {
                "questId": 21,
                "current_created_version_id": 1,
                "candidate_created_version_id": 2,
                "status": "manual_difference",
                "final_created_version_id": 1,
            }
        ]
        path = tmp_path / "quest-version-audit.json"
        path.write_text(json.dumps({"records": records}), encoding="utf-8")
        cursor = connection.cursor()
        first = load_manual_created_version_audit(cursor, str(path))
        second = load_manual_created_version_audit(cursor, str(path))
        connection.commit()
        assert first["difference_count"] == 1
        assert second["difference_count"] == 1
        assert manual_created_version_audit_is_prepared(cursor)
        assert count_manual_locked_quests(cursor) == 1
        assert connection.execute("SELECT created_version_id FROM quest WHERE questId=21").fetchone()[0] == 1
    finally:
        connection.close()


def test_task_text_versions_align_without_overwriting_global_shared_hashes():
    connection = _db()
    try:
        connection.executemany(
            "INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [
                (1, 1301, "Shared", 1, 1),
                (2, 1301, "Shared", 2, 2),
                (1, 1302, "Step", 1, 1),
                (1, 1303, "Dialogue", 1, 1),
                (1, 1304, "Hangout", 3, 3),
            ],
        )
        _add_quest(connection, 31, 1301, 2, 2, talk_id=3101, step_hash=1302)
        _add_quest(connection, 32, 1301, 3, 3)
        _add_quest(connection, 33, 1304, 3, 3, source_type="HANGOUT", talk_id=3301, coop_quest_id=330100)
        connection.execute(
            "INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId) VALUES (?,?,?,?,?,?)",
            (9301, "TALK_ROLE_NPC", 1, 3101, 1303, None),
        )
        connection.execute(
            "INSERT INTO dialogue(dialogueId, talkerType, talkerId, talkId, textHash, coopQuestId) VALUES (?,?,?,?,?,?)",
            (9302, "TALK_ROLE_NPC", 1, 3301, 1304, 330100),
        )
        connection.executemany(
            "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
            [(3101, 0, 9301), (3301, 330100, 9302)],
        )
        cursor = connection.cursor()
        mark_manual_created_version_audit_prepared(cursor, candidate_count=3, difference_count=1)
        persist_manual_created_version_overrides(
            cursor,
            {
                31: {
                    "status": "manual_difference",
                    "current_created_version_id": 2,
                    "candidate_created_version_id": 1,
                }
            },
        )
        before = connection.execute(
            "SELECT lang, created_version_id FROM textMap WHERE hash=1301 ORDER BY lang"
        ).fetchall()

        first = refresh_quest_text_versions(cursor)
        second = refresh_quest_text_versions(cursor)
        after = connection.execute(
            "SELECT lang, created_version_id FROM textMap WHERE hash=1301 ORDER BY lang"
        ).fetchall()
        rows = connection.execute(
            f"SELECT questId, hash, created_version_id, alignment_status, relation_types "
            f"FROM {QUEST_TEXT_VERSION_TABLE} ORDER BY questId, hash"
        ).fetchall()
        connection.commit()

        assert before == after
        # The shared chapter title/number are also task text, so they are
        # correctly reported alongside the shared quest title.
        assert first["conflict_hashes"] == 3
        assert second["conflict_hashes"] == 3
        assert first["association_rows"] == second["association_rows"]
        assert any(row[0] == 31 and row[1] == 1301 and row[2] == 2 and row[3] == "shared_conflict_task_scoped" for row in rows)
        assert connection.execute(
            f"SELECT locked_created_version_id FROM {QUEST_CREATED_VERSION_OVERRIDE_TABLE} WHERE questId=31"
        ).fetchone() == (2,)
        assert any(row[0] == 32 and row[1] == 1301 and row[2] == 3 and row[3] == "shared_conflict_task_scoped" for row in rows)
        step_row = next(row for row in rows if row[0] == 31 and row[1] == 1302)
        assert "step_title" in step_row[4]
        dialogue_row = next(row for row in rows if row[0] == 31 and row[1] == 1303)
        assert "linked_dialogue" in dialogue_row[4]
        hangout_row = next(row for row in rows if row[0] == 33 and row[1] == 1304)
        assert "linked_dialogue" in hangout_row[4]
    finally:
        connection.close()


def test_task_text_unresolved_values_are_not_silently_aligned():
    connection = _db()
    try:
        _add_quest(connection, 41, 1401, None, None, source_type="ANECDOTE")
        _add_quest(connection, 42, 1402, 2, 2)
        cursor = connection.cursor()
        mark_manual_created_version_audit_prepared(cursor, candidate_count=2, difference_count=0)
        stats = refresh_quest_text_versions(cursor)
        statuses = dict(
            connection.execute(
                f"SELECT questId, alignment_status FROM {QUEST_TEXT_VERSION_TABLE}"
            ).fetchall()
        )
        assert stats["unresolved_rows"] == 4
        assert statuses[41] == "unresolved_quest_version"
        assert statuses[42] == "unresolved_textmap_hash"
    finally:
        connection.close()


def test_read_only_task_text_audit_reports_final_versions_without_persistent_writes():
    connection = _db()
    try:
        connection.executemany(
            "INSERT INTO textMap(lang, hash, content, created_version_id, updated_version_id) VALUES (?,?,?,?,?)",
            [
                (1, 1501, "Shared", 1, 1),
                (2, 1501, "Shared", 2, 2),
                (1, 1502, "Step", 1, 1),
            ],
        )
        _add_quest(connection, 51, 1501, 2, 2, talk_id=5101, step_hash=1502)
        _add_quest(connection, 52, 1501, 3, 3)
        before = connection.execute(
            "SELECT lang, hash, created_version_id FROM textMap ORDER BY lang, hash"
        ).fetchall()

        audit = calculate_quest_text_version_audit(
            connection.cursor(),
            {51: 2, 52: 3},
        )
        records = {row["questId"]: row for row in audit["records"]}
        after = connection.execute(
            "SELECT lang, hash, created_version_id FROM textMap ORDER BY lang, hash"
        ).fetchall()

        assert before == after
        assert records[51]["final_created_version_id"] == 2
        assert records[51]["associated_hash_count"] >= 3
        assert records[51]["shared_conflict_count"] >= 1
        assert records[51]["text_version_adjustment_count"] >= 1
        assert audit["summary"]["shared_conflict_hash_count"] >= 1
        assert connection.execute(
            "SELECT 1 FROM sqlite_temp_master WHERE type='table' AND name='quest_text_version'"
        ).fetchone() is None
    finally:
        connection.close()

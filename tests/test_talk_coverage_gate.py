import json
import os
import sqlite3
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from talk_coverage_gate import (  # noqa: E402
    assert_talk_dialogue_coverage,
    audit_talk_dialogue_coverage,
)


def _create_talk_tables(connection):
    connection.execute(
        """
        CREATE TABLE talk_dialogue_content (
            talkId INTEGER NOT NULL,
            coopQuestId INTEGER NOT NULL DEFAULT 0,
            dialogueId INTEGER NOT NULL,
            textHash INTEGER NOT NULL,
            talkerId INTEGER,
            talkerType TEXT,
            PRIMARY KEY (talkId, coopQuestId, dialogueId, textHash)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE talk_dialogue_link (
            talkId INTEGER NOT NULL,
            coopQuestId INTEGER NOT NULL DEFAULT 0,
            dialogueId INTEGER NOT NULL,
            PRIMARY KEY (talkId, coopQuestId, dialogueId)
        )
        """
    )


def test_talk_coverage_gate_accepts_duplicate_source_scopes_and_detects_missing_rows(tmp_path):
    talk_root = tmp_path / "BinOutput" / "Talk"
    for folder, dialogue_id, text_hash in (
        ("Activity", 400670701, 1183839098),
        ("FreeGroup", 400679917, 3376835474),
    ):
        directory = talk_root / folder
        directory.mkdir(parents=True)
        (directory / "4006707.json").write_text(
            json.dumps(
                {
                    "IOKNFDJFGDH": 4006707,
                    "PFALHAKIILD": [
                        {
                            "OIFGMOHKPOI": dialogue_id,
                            "OACNIBLFFDI": text_hash,
                            "LFGCLNLPAPB": {"_id": "1005", "_type": "TALK_ROLE_NPC"},
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

    connection = sqlite3.connect(":memory:")
    _create_talk_tables(connection)
    connection.executemany(
        "INSERT INTO talk_dialogue_content"
        "(talkId, coopQuestId, dialogueId, textHash) VALUES (?,?,?,?)",
        [
            (4006707, 0, 400670701, 1183839098),
            (4006707, 0, 400679917, 3376835474),
        ],
    )
    connection.executemany(
        "INSERT INTO talk_dialogue_link(talkId, coopQuestId, dialogueId) VALUES (?,?,?)",
        [(4006707, 0, 400670701), (4006707, 0, 400679917)],
    )
    connection.commit()

    report = assert_talk_dialogue_coverage(connection.cursor(), str(tmp_path))
    assert report["missing_content_rows"] == 0
    assert report["missing_exact_links"] == 0
    assert report["source_unique_content_rows"] == 2

    connection.execute(
        "DELETE FROM talk_dialogue_content WHERE dialogueId=?", (400679917,)
    )
    failed = audit_talk_dialogue_coverage(connection.cursor(), str(tmp_path))
    assert failed["missing_content_rows"] == 1
    try:
        assert_talk_dialogue_coverage(connection.cursor(), str(tmp_path))
    except RuntimeError as exc:
        assert "missing_content_rows" in str(exc)
    else:
        raise AssertionError("coverage gate unexpectedly accepted a missing source row")

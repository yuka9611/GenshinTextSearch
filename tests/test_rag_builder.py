from __future__ import annotations

import json
import sqlite3

import rag_builder


def _create_source_database(path):
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE app_meta(k TEXT PRIMARY KEY, v TEXT NOT NULL);
        CREATE TABLE version_dim(
            id INTEGER PRIMARY KEY,
            version_tag TEXT
        );
        CREATE TABLE textMap(
            id INTEGER PRIMARY KEY,
            hash INTEGER,
            content TEXT,
            lang INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE TABLE npc(
            id INTEGER PRIMARY KEY,
            npcId INTEGER,
            textHash INTEGER
        );
        CREATE TABLE quest(
            id INTEGER PRIMARY KEY,
            questId INTEGER,
            titleTextMapHash INTEGER,
            source_type TEXT,
            created_version_id INTEGER
        );
        CREATE TABLE questTalk(
            id INTEGER PRIMARY KEY,
            questId INTEGER,
            talkId INTEGER,
            stepTitleTextMapHash INTEGER,
            coopQuestId INTEGER
        );
        CREATE TABLE talk_dialogue_content(
            talkId INTEGER,
            coopQuestId INTEGER,
            dialogueId INTEGER,
            textHash INTEGER,
            talkerId INTEGER,
            talkerType TEXT
        );
        CREATE TABLE readable(
            id INTEGER PRIMARY KEY,
            fileName TEXT,
            lang TEXT,
            content TEXT,
            titleTextMapHash INTEGER,
            readableId INTEGER,
            created_version_id INTEGER,
            updated_version_id INTEGER
        );
        CREATE TABLE readable_meta(
            normalized_file_name TEXT PRIMARY KEY,
            readable_id INTEGER,
            title_text_map_hash INTEGER,
            readable_category TEXT
        );
        CREATE TABLE avatar(
            id INTEGER PRIMARY KEY,
            avatarId INTEGER,
            nameTextMapHash INTEGER
        );
        CREATE TABLE fetterStory(
            id INTEGER PRIMARY KEY,
            fetterId INTEGER,
            avatarId INTEGER,
            storyTitleTextMapHash INTEGER,
            storyTitle2TextMapHash INTEGER,
            storyContextTextMapHash INTEGER,
            storyContext2TextMapHash INTEGER
        );
        CREATE TABLE fetters(
            id INTEGER PRIMARY KEY,
            fetterId INTEGER,
            avatarId INTEGER,
            voiceTitleTextMapHash INTEGER,
            voiceFileTextTextMapHash INTEGER,
            voiceFile INTEGER
        );
        """
    )
    connection.execute(
        "INSERT INTO app_meta(k, v) VALUES ('db_current_commit', ?)",
        ("a" * 40,),
    )
    connection.execute("INSERT INTO version_dim VALUES (1, '1.0')")
    text_rows = [
        (1, 100, "测试任务", 1, 1, 1),
        (2, 101, "关于契约", 1, 1, 1),
        (3, 200, "派蒙", 1, 1, 1),
        (4, 300, "测试读物", 1, 1, 1),
        (5, 400, "钟离", 1, 1, 1),
        (6, 401, "角色故事", 1, 1, 1),
        (7, 402, "这是关于契约与璃月历史的角色故事。", 1, 1, 1),
        (8, 403, "关于契约", 1, 1, 1),
        (9, 404, "契约一旦订立，就必须履行。", 1, 1, 1),
    ]
    for index in range(14):
        text_rows.append(
            (1000 + index, 1000 + index, f"第{index + 1}句关于契约的测试对白。", 1, 1, 1)
        )
    connection.executemany(
        "INSERT INTO textMap VALUES (?,?,?,?,?,?)",
        text_rows,
    )
    connection.execute("INSERT INTO npc VALUES (1, 10, 200)")
    connection.execute("INSERT INTO quest VALUES (1, 500, 100, 'AQ', 1)")
    connection.execute("INSERT INTO questTalk VALUES (1, 500, 700, 101, 0)")
    connection.executemany(
        "INSERT INTO talk_dialogue_content VALUES (?,?,?,?,?,?)",
        [
            (700, 0, 800 + index, 1000 + index, 10, "TALK_ROLE_NPC")
            for index in range(14)
        ],
    )
    readable_text = (
        "这是第一段关于契约的可读物内容。" * 15
        + "\n\n"
        + "这是第二段关于璃月历史的可读物内容。" * 15
    )
    connection.execute(
        "INSERT INTO readable VALUES (1, 'Book1', 'CHS', ?, 300, 900, 1, 1)",
        (readable_text,),
    )
    connection.execute("INSERT INTO readable_meta VALUES ('Book1', 900, 300, 'BOOK')")
    connection.execute("INSERT INTO avatar VALUES (1, 10000030, 400)")
    connection.execute(
        "INSERT INTO fetterStory VALUES (1, 4201, 10000030, 401, NULL, 402, NULL)"
    )
    connection.execute(
        "INSERT INTO fetters VALUES (1, 1002, 10000030, 403, 404, 24001)"
    )
    connection.commit()
    connection.close()


def _read_chunks(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_build_artifacts_is_commit_bound_searchable_and_repeatable(tmp_path):
    database = tmp_path / "source.db"
    _create_source_database(database)
    first = tmp_path / "first"
    second = tmp_path / "second"

    manifest = rag_builder.build_artifacts(database, first)
    repeated = rag_builder.build_artifacts(database, second)

    assert manifest["sourceDbCommit"] == "a" * 40
    assert manifest["embeddingDimension"] == 1024
    assert manifest["countsByDocType"]["quest_dialogue"] >= 2
    assert set(manifest["countsByDocType"]) == {
        "avatar_story",
        "avatar_voice",
        "quest_dialogue",
        "readable",
    }
    assert manifest["chunkCount"] == repeated["chunkCount"]

    chunks = _read_chunks(first / "genshin_rag_chunks.jsonl")
    repeated_chunks = _read_chunks(second / "genshin_rag_chunks.jsonl")
    assert [chunk["chunkId"] for chunk in chunks] == [
        chunk["chunkId"] for chunk in repeated_chunks
    ]
    assert all(len(chunk["content"]) <= rag_builder.MAX_CHARS for chunk in chunks)
    assert any(chunk["title"] == "关于契约" for chunk in chunks)
    assert any("派蒙：" in chunk["content"] for chunk in chunks)

    dialogue_chunks = [chunk for chunk in chunks if chunk["docType"] == "quest_dialogue"]
    assert len(dialogue_chunks) >= 2
    first_ids = dialogue_chunks[0]["metadata"]["dialogueIds"]
    second_ids = dialogue_chunks[1]["metadata"]["dialogueIds"]
    assert first_ids[-2:] == second_ids[:2]
    assert all(chunk["sourceKey"] == "quest:500:talk:700:coop:0" for chunk in dialogue_chunks)

    sidecar = sqlite3.connect(first / "genshin_rag_fts.db")
    try:
        assert sidecar.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        matched = sidecar.execute(
            """
            SELECT c.chunk_id
            FROM chunk_fts f JOIN chunk c ON c.rowid=f.rowid
            WHERE chunk_fts MATCH '关于契约'
            """
        ).fetchall()
        assert matched
        stored_commit = json.loads(
            sidecar.execute(
                "SELECT value FROM manifest WHERE key='sourceDbCommit'"
            ).fetchone()[0]
        )
        assert stored_commit == "a" * 40
    finally:
        sidecar.close()

    assert not list(first.glob("*.tmp"))


def test_prose_windows_preserve_text_when_overlap_would_exceed_limit():
    text = "甲" * 350 + "\n\n" + "乙" * 350
    windows = rag_builder._prose_windows(text)

    assert windows[0] == "甲" * 350
    assert windows[1] == "乙" * 350
    assert all(len(window) <= 350 for window in windows)

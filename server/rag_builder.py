from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from itertools import groupby
from pathlib import Path
from typing import Any, Iterable, Iterator


CHUNK_SCHEMA_VERSION = 1
EMBEDDING_MODEL = "Qwen/Qwen3-Embedding-0.6B"
EMBEDDING_DIMENSION = 1024
CHS_TEXTMAP_LANG = 1
CHS_READABLE_LANG = "CHS"
MAX_CHARS = 350
PROSE_OVERLAP_CHARS = 60
DIALOGUE_MAX_LINES = 12
DIALOGUE_OVERLAP_LINES = 2


@dataclass(frozen=True, slots=True)
class RagChunk:
    chunkId: str
    docType: str
    sourceKey: str
    ordinal: int
    title: str
    content: str
    createdVersion: str
    updatedVersion: str
    metadata: dict[str, Any]
    contentSha256: str


def _normalize_text(value: Any) -> str:
    text = html.unescape(str(value or ""))
    text = text.replace("\\r\\n", "\n").replace("\\n", "\n")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(?:color|size|i|b)(?:=[^>]*)?>", "", text, flags=re.I)
    text = re.sub(r"[ \t\u3000]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _chunk_id(
    doc_type: str,
    source_key: str,
    ordinal: int,
    content_sha256: str,
) -> str:
    identity = (
        f"{CHUNK_SCHEMA_VERSION}\0{doc_type}\0{source_key}\0"
        f"{ordinal}\0{content_sha256}"
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _make_chunk(
    *,
    doc_type: str,
    source_key: str,
    ordinal: int,
    title: str,
    content: str,
    created_version: str = "",
    updated_version: str = "",
    metadata: dict[str, Any] | None = None,
) -> RagChunk | None:
    normalized = _normalize_text(content)
    if not normalized:
        return None
    content_sha256 = _sha256_text(normalized)
    return RagChunk(
        chunkId=_chunk_id(doc_type, source_key, ordinal, content_sha256),
        docType=doc_type,
        sourceKey=source_key,
        ordinal=ordinal,
        title=_normalize_text(title),
        content=normalized,
        createdVersion=str(created_version or ""),
        updatedVersion=str(updated_version or ""),
        metadata=metadata or {},
        contentSha256=content_sha256,
    )


def _paragraph_units(text: str, max_chars: int = MAX_CHARS) -> list[str]:
    normalized = _normalize_text(text)
    if not normalized:
        return []
    paragraphs = [part.strip() for part in re.split(r"\n\s*\n", normalized) if part.strip()]
    units: list[str] = []
    for paragraph in paragraphs or [normalized]:
        while len(paragraph) > max_chars:
            split_at = max(
                paragraph.rfind(mark, 0, max_chars + 1)
                for mark in ("。", "！", "？", "；", "\n")
            )
            if split_at < max_chars // 2:
                split_at = max_chars
            else:
                split_at += 1
            units.append(paragraph[:split_at].strip())
            paragraph = paragraph[split_at:].strip()
        if paragraph:
            units.append(paragraph)
    return units


def _prose_windows(
    text: str,
    *,
    max_chars: int = MAX_CHARS,
    overlap_chars: int = PROSE_OVERLAP_CHARS,
) -> list[str]:
    units = _paragraph_units(text, max_chars=max_chars)
    if not units:
        return []
    windows: list[str] = []
    current = ""
    for unit in units:
        candidate = unit if not current else f"{current}\n\n{unit}"
        if current and len(candidate) > max_chars:
            windows.append(current)
            overlap = current[-overlap_chars:].lstrip() if overlap_chars else ""
            if overlap:
                available_overlap = max(0, max_chars - len(unit) - 2)
                overlap = overlap[-available_overlap:] if available_overlap else ""
            current = f"{overlap}\n\n{unit}".strip() if overlap else unit
        else:
            current = candidate
    if current:
        windows.append(current)
    return windows


def _dialogue_windows(lines: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    expanded: list[dict[str, Any]] = []
    for line in lines:
        speaker = _normalize_text(line.get("speaker"))
        body = _normalize_text(line.get("text"))
        prefix = f"{speaker}：" if speaker else ""
        available = max(1, MAX_CHARS - len(prefix))
        parts = _paragraph_units(body, max_chars=available) or [body]
        for part in parts:
            copied = dict(line)
            copied["rendered"] = f"{prefix}{part}".strip()
            expanded.append(copied)

    windows: list[list[dict[str, Any]]] = []
    start = 0
    while start < len(expanded):
        end = start
        char_count = 0
        while end < len(expanded) and end - start < DIALOGUE_MAX_LINES:
            rendered = str(expanded[end]["rendered"])
            addition = len(rendered) + (1 if end > start else 0)
            if end > start and char_count + addition > MAX_CHARS:
                break
            char_count += addition
            end += 1
        if end == start:
            end += 1
        windows.append(expanded[start:end])
        if end >= len(expanded):
            break
        start = max(start + 1, end - DIALOGUE_OVERLAP_LINES)
    return windows


class RagCorpusBuilder:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        self.connection.row_factory = sqlite3.Row
        self.versions = {
            int(row["id"]): str(row["version_tag"] or "")
            for row in self.connection.execute(
                "SELECT id, version_tag FROM version_dim"
            )
        }
        self.npc_names = {
            str(row["npcId"]): _normalize_text(row["content"])
            for row in self.connection.execute(
                """
                SELECT n.npcId, tm.content
                FROM npc n
                LEFT JOIN textMap tm ON tm.hash=n.textHash AND tm.lang=?
                ORDER BY n.id
                """,
                (CHS_TEXTMAP_LANG,),
            )
            if row["content"]
        }

    def _version(self, value: Any) -> str:
        try:
            return self.versions.get(int(value), "")
        except (TypeError, ValueError):
            return ""

    def _speaker(self, talker_type: Any, talker_id: Any) -> str:
        role = str(talker_type or "")
        if role == "TALK_ROLE_PLAYER":
            return "旅行者"
        if role == "TALK_ROLE_MATE_AVATAR":
            return "{荧/空}"
        if role == "TALK_ROLE_NPC":
            return self.npc_names.get(str(talker_id), "")
        return ""

    def iter_chunks(self) -> Iterator[RagChunk]:
        yield from self._dialogue_chunks()
        yield from self._readable_chunks()
        yield from self._story_chunks()
        yield from self._voice_chunks()

    def _dialogue_source_table(self) -> str:
        row = self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='talk_dialogue_content'"
        ).fetchone()
        return "talk_dialogue_content" if row else "dialogue"

    def _dialogue_chunks(self) -> Iterator[RagChunk]:
        source_table = self._dialogue_source_table()
        rows = self.connection.execute(
            f"""
            SELECT qt.questId, qt.talkId, qt.coopQuestId,
                   q.titleTextMapHash, q.source_type, q.created_version_id AS quest_created,
                   qt.stepTitleTextMapHash,
                   d.dialogueId, d.textHash, d.talkerType, d.talkerId,
                   body.content, body.created_version_id AS body_created,
                   body.updated_version_id AS body_updated,
                   qtitle.content AS quest_title, stitle.content AS step_title
            FROM questTalk qt
            JOIN quest q ON q.questId=qt.questId
            JOIN {source_table} d
              ON d.talkId=qt.talkId
             AND COALESCE(d.coopQuestId, 0)=COALESCE(qt.coopQuestId, 0)
            JOIN textMap body ON body.hash=d.textHash AND body.lang=?
            LEFT JOIN textMap qtitle ON qtitle.hash=q.titleTextMapHash AND qtitle.lang=?
            LEFT JOIN textMap stitle ON stitle.hash=qt.stepTitleTextMapHash AND stitle.lang=?
            WHERE TRIM(COALESCE(body.content, ''))<>''
            ORDER BY qt.questId, qt.talkId, qt.coopQuestId, d.dialogueId, d.textHash
            """,
            (CHS_TEXTMAP_LANG, CHS_TEXTMAP_LANG, CHS_TEXTMAP_LANG),
        )
        key_fn = lambda row: (
            int(row["questId"]),
            int(row["talkId"]),
            int(row["coopQuestId"] or 0),
        )
        for (quest_id, talk_id, coop_id), group in groupby(rows, key=key_fn):
            group_rows = list(group)
            title = _normalize_text(group_rows[0]["step_title"] or group_rows[0]["quest_title"])
            quest_title = _normalize_text(group_rows[0]["quest_title"])
            lines = [
                {
                    "dialogueId": int(row["dialogueId"]),
                    "textHash": str(row["textHash"]),
                    "speaker": self._speaker(row["talkerType"], row["talkerId"]),
                    "text": row["content"],
                    "createdVersion": self._version(row["body_created"]),
                    "updatedVersion": self._version(row["body_updated"]),
                }
                for row in group_rows
            ]
            source_key = f"quest:{quest_id}:talk:{talk_id}:coop:{coop_id}"
            for ordinal, window in enumerate(_dialogue_windows(lines)):
                dialogue_ids = list(dict.fromkeys(int(line["dialogueId"]) for line in window))
                text_hashes = list(dict.fromkeys(str(line["textHash"]) for line in window))
                created = next(
                    (str(line["createdVersion"]) for line in window if line["createdVersion"]),
                    self._version(group_rows[0]["quest_created"]),
                )
                updated = next(
                    (str(line["updatedVersion"]) for line in reversed(window) if line["updatedVersion"]),
                    created,
                )
                chunk = _make_chunk(
                    doc_type="quest_dialogue",
                    source_key=source_key,
                    ordinal=ordinal,
                    title=title or quest_title or f"任务 {quest_id}",
                    content="\n".join(str(line["rendered"]) for line in window),
                    created_version=created,
                    updated_version=updated,
                    metadata={
                        "questId": quest_id,
                        "talkId": talk_id,
                        "coopQuestId": coop_id,
                        "dialogueIds": dialogue_ids,
                        "textHashes": text_hashes,
                        "questTitle": quest_title,
                        "sourceType": str(group_rows[0]["source_type"] or ""),
                    },
                )
                if chunk:
                    yield chunk

    def _readable_chunks(self) -> Iterator[RagChunk]:
        rows = self.connection.execute(
            """
            SELECT r.*, title.content AS title, rm.readable_category
            FROM readable r
            LEFT JOIN textMap title ON title.hash=r.titleTextMapHash AND title.lang=?
            LEFT JOIN readable_meta rm ON rm.normalized_file_name=r.fileName
            WHERE UPPER(r.lang)=? AND TRIM(COALESCE(r.content, ''))<>''
            ORDER BY r.fileName, r.readableId
            """,
            (CHS_TEXTMAP_LANG, CHS_READABLE_LANG),
        )
        for row in rows:
            readable_id = row["readableId"]
            source_key = f"readable:{readable_id or row['fileName']}"
            title = _normalize_text(row["title"] or row["fileName"])
            for ordinal, content in enumerate(_prose_windows(row["content"])):
                chunk = _make_chunk(
                    doc_type="readable",
                    source_key=source_key,
                    ordinal=ordinal,
                    title=title,
                    content=content,
                    created_version=self._version(row["created_version_id"]),
                    updated_version=self._version(row["updated_version_id"]),
                    metadata={
                        "readableId": readable_id,
                        "fileName": str(row["fileName"] or ""),
                        "sourceType": "readable",
                        "readableCategory": str(row["readable_category"] or ""),
                        "textHashes": [str(row["titleTextMapHash"])] if row["titleTextMapHash"] else [],
                    },
                )
                if chunk:
                    yield chunk

    def _story_chunks(self) -> Iterator[RagChunk]:
        rows = self.connection.execute(
            """
            SELECT fs.*, a.nameTextMapHash, avatar_name.content AS avatar_name,
                   t1.content AS title1, t2.content AS title2,
                   c1.content AS content1, c2.content AS content2,
                   c1.created_version_id AS created1, c1.updated_version_id AS updated1,
                   c2.created_version_id AS created2, c2.updated_version_id AS updated2
            FROM fetterStory fs
            LEFT JOIN avatar a ON a.avatarId=fs.avatarId
            LEFT JOIN textMap avatar_name ON avatar_name.hash=a.nameTextMapHash AND avatar_name.lang=?
            LEFT JOIN textMap t1 ON t1.hash=fs.storyTitleTextMapHash AND t1.lang=?
            LEFT JOIN textMap t2 ON t2.hash=fs.storyTitle2TextMapHash AND t2.lang=?
            LEFT JOIN textMap c1 ON c1.hash=fs.storyContextTextMapHash AND c1.lang=?
            LEFT JOIN textMap c2 ON c2.hash=fs.storyContext2TextMapHash AND c2.lang=?
            ORDER BY fs.avatarId, fs.fetterId
            """,
            (CHS_TEXTMAP_LANG,) * 5,
        )
        for row in rows:
            avatar_name = _normalize_text(row["avatar_name"])
            for entry_index in (1, 2):
                content = row[f"content{entry_index}"]
                if not _normalize_text(content):
                    continue
                text_hash = row[
                    "storyContextTextMapHash" if entry_index == 1 else "storyContext2TextMapHash"
                ]
                title_hash = row[
                    "storyTitleTextMapHash" if entry_index == 1 else "storyTitle2TextMapHash"
                ]
                source_key = f"avatar:{row['avatarId']}:story:{row['fetterId']}:{entry_index}"
                title = " · ".join(
                    part for part in (avatar_name, _normalize_text(row[f"title{entry_index}"])) if part
                )
                for ordinal, part in enumerate(_prose_windows(content)):
                    chunk = _make_chunk(
                        doc_type="avatar_story",
                        source_key=source_key,
                        ordinal=ordinal,
                        title=title,
                        content=part,
                        created_version=self._version(row[f"created{entry_index}"]),
                        updated_version=self._version(row[f"updated{entry_index}"]),
                        metadata={
                            "avatarId": int(row["avatarId"]),
                            "avatarName": avatar_name,
                            "fetterId": int(row["fetterId"]),
                            "entryIndex": entry_index,
                            "textHashes": [str(text_hash)] if text_hash else [],
                            "titleTextHash": str(title_hash) if title_hash else "",
                            "sourceType": "story",
                        },
                    )
                    if chunk:
                        yield chunk

    def _voice_chunks(self) -> Iterator[RagChunk]:
        rows = self.connection.execute(
            """
            SELECT f.*, a.nameTextMapHash, avatar_name.content AS avatar_name,
                   title.content AS title, body.content AS content,
                   body.created_version_id, body.updated_version_id
            FROM fetters f
            LEFT JOIN avatar a ON a.avatarId=f.avatarId
            LEFT JOIN textMap avatar_name ON avatar_name.hash=a.nameTextMapHash AND avatar_name.lang=?
            LEFT JOIN textMap title ON title.hash=f.voiceTitleTextMapHash AND title.lang=?
            LEFT JOIN textMap body ON body.hash=f.voiceFileTextTextMapHash AND body.lang=?
            WHERE TRIM(COALESCE(body.content, ''))<>''
            ORDER BY f.avatarId, f.voiceFile, f.fetterId
            """,
            (CHS_TEXTMAP_LANG,) * 3,
        )
        for row in rows:
            avatar_name = _normalize_text(row["avatar_name"])
            source_key = f"avatar:{row['avatarId']}:voice:{row['voiceFile']}:{row['fetterId']}"
            title = " · ".join(
                part for part in (avatar_name, _normalize_text(row["title"])) if part
            )
            for ordinal, content in enumerate(_prose_windows(row["content"])):
                chunk = _make_chunk(
                    doc_type="avatar_voice",
                    source_key=source_key,
                    ordinal=ordinal,
                    title=title,
                    content=content,
                    created_version=self._version(row["created_version_id"]),
                    updated_version=self._version(row["updated_version_id"]),
                    metadata={
                        "avatarId": int(row["avatarId"]),
                        "avatarName": avatar_name,
                        "fetterId": int(row["fetterId"]),
                        "voiceFile": int(row["voiceFile"]),
                        "textHashes": [str(row["voiceFileTextTextMapHash"])],
                        "titleTextHash": str(row["voiceTitleTextMapHash"] or ""),
                        "sourceType": "voice",
                    },
                )
                if chunk:
                    yield chunk


def _database_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _open_source_database(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def _source_commit(connection: sqlite3.Connection) -> str:
    row = connection.execute(
        "SELECT v FROM app_meta WHERE k='db_current_commit' LIMIT 1"
    ).fetchone()
    commit = str(row[0] or "").strip() if row else ""
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise RuntimeError("app_meta.db_current_commit is missing or invalid")
    return commit


def _create_sidecar(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA synchronous=FULL;
        CREATE TABLE manifest (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE chunk (
            chunk_id TEXT PRIMARY KEY,
            doc_type TEXT NOT NULL,
            source_key TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            created_version TEXT NOT NULL,
            updated_version TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            content_sha256 TEXT NOT NULL
        );
        CREATE INDEX chunk_doc_type_index ON chunk(doc_type);
        CREATE INDEX chunk_source_key_index ON chunk(source_key);
        CREATE INDEX chunk_created_version_index ON chunk(created_version);
        CREATE INDEX chunk_updated_version_index ON chunk(updated_version);
        CREATE VIRTUAL TABLE chunk_fts USING fts5(
            title,
            content,
            content='chunk',
            content_rowid='rowid',
            tokenize='trigram'
        );
        """
    )
    return connection


def build_artifacts(database: Path, output_dir: Path) -> dict[str, Any]:
    database = database.resolve()
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    if not database.is_file():
        raise FileNotFoundError(database)

    jsonl_path = output_dir / "genshin_rag_chunks.jsonl"
    sidecar_path = output_dir / "genshin_rag_fts.db"
    manifest_path = output_dir / "genshin_rag_manifest.json"
    temporary_paths = [Path(f"{path}.tmp") for path in (jsonl_path, sidecar_path, manifest_path)]
    for path in temporary_paths:
        if path.exists():
            path.unlink()

    source_connection = _open_source_database(database)
    sidecar = _create_sidecar(temporary_paths[1])
    counts: Counter[str] = Counter()
    chunk_count = 0
    try:
        source_commit = _source_commit(source_connection)
        with temporary_paths[0].open("w", encoding="utf-8", newline="\n") as jsonl:
            for chunk in RagCorpusBuilder(source_connection).iter_chunks():
                payload = asdict(chunk)
                jsonl.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
                cursor = sidecar.execute(
                    """
                    INSERT INTO chunk(
                        chunk_id, doc_type, source_key, ordinal, title, content,
                        created_version, updated_version, metadata_json, content_sha256
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        chunk.chunkId,
                        chunk.docType,
                        chunk.sourceKey,
                        chunk.ordinal,
                        chunk.title,
                        chunk.content,
                        chunk.createdVersion,
                        chunk.updatedVersion,
                        json.dumps(chunk.metadata, ensure_ascii=False, sort_keys=True),
                        chunk.contentSha256,
                    ),
                )
                sidecar.execute(
                    "INSERT INTO chunk_fts(rowid, title, content) VALUES (?,?,?)",
                    (cursor.lastrowid, chunk.title, chunk.content),
                )
                counts[chunk.docType] += 1
                chunk_count += 1

        manifest = {
            "chunkSchemaVersion": CHUNK_SCHEMA_VERSION,
            "sourceDbCommit": source_commit,
            "sourceDbSha256": _database_sha256(database),
            "sourceDbSize": database.stat().st_size,
            "embeddingModel": EMBEDDING_MODEL,
            "embeddingDimension": EMBEDDING_DIMENSION,
            "chunkCount": chunk_count,
            "countsByDocType": dict(sorted(counts.items())),
            "builtAtUtc": datetime.now(timezone.utc).isoformat(),
        }
        sidecar.executemany(
            "INSERT INTO manifest(key, value) VALUES (?,?)",
            [
                (key, json.dumps(value, ensure_ascii=False, sort_keys=True))
                for key, value in manifest.items()
            ],
        )
        sidecar.commit()
        integrity = sidecar.execute("PRAGMA integrity_check").fetchone()[0]
        fts_count = sidecar.execute("SELECT COUNT(*) FROM chunk_fts").fetchone()[0]
        if integrity != "ok" or fts_count != chunk_count or not chunk_count:
            raise RuntimeError(
                f"sidecar validation failed: integrity={integrity!r}, "
                f"chunks={chunk_count}, fts={fts_count}"
            )
        with temporary_paths[2].open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
    except Exception:
        sidecar.close()
        source_connection.close()
        for path in temporary_paths:
            if path.exists():
                path.unlink()
        raise
    else:
        sidecar.close()
        source_connection.close()

    for temporary, final in zip(temporary_paths, (jsonl_path, sidecar_path, manifest_path)):
        os.replace(temporary, final)
    return manifest


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build commit-bound Genshin RAG chunks and an FTS5 sidecar."
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=Path(__file__).resolve().parent / "data.db",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    manifest = build_artifacts(args.database, args.output_dir)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

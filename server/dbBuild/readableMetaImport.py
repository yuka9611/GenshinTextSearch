from __future__ import annotations

import os
import re
import unicodedata
from collections import Counter
from contextlib import closing
from typing import Any, TypedDict

from import_utils import load_json_file
from lang_constants import LANG_CODE_MAP


READABLE_META_CATEGORY_CODES = (
    "BOOK",
    "ITEM",
    "READABLE",
    "COSTUME",
    "RELIC",
    "WEAPON",
    "WINGS",
)
_READABLE_LANG_SUFFIX_RE = re.compile(r"_(CHS|CHT|DE|EN|ES|FR|ID|IT|JP|KR|PT|RU|TH|TR|VI)$", re.IGNORECASE)
_READABLE_ITEM_MATCH_STRIP_RE = re.compile(r"[\s\"'“”‘’《》「」『』\(\)（）\[\]【】<>〈〉·・]")
_SOURCE_LANG_CODE = int(LANG_CODE_MAP.get("CHS", 1))


class _ReadableMetaLookup(TypedDict):
    codex_readable_ids: set[int]
    codex_title_hashes: set[int]
    item_ids_by_readable_id: dict[int, int]
    item_ids_by_title_hash: dict[int, int]
    item_name_hash_by_item_id: dict[int, int]
    item_desc_hash_by_item_id: dict[int, int]


def normalize_readable_file_name(file_name: str | None) -> str:
    normalized = str(file_name or "").replace("\\", "/").strip()
    base_name = os.path.basename(normalized)
    if not base_name:
        return ""
    root, ext = os.path.splitext(base_name)
    return _READABLE_LANG_SUFFIX_RE.sub("", root) + ext


def ensure_readable_meta_schema(connection, *, commit: bool = True) -> None:
    with closing(connection.cursor()) as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS readable_meta (
                normalized_file_name TEXT PRIMARY KEY,
                readable_id INTEGER,
                title_text_map_hash INTEGER,
                readable_category TEXT NOT NULL
                    CHECK (readable_category IN ('BOOK', 'ITEM', 'READABLE', 'COSTUME', 'RELIC', 'WEAPON', 'WINGS'))
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS readable_meta_readable_category_index "
            "ON readable_meta (readable_category)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS readable_meta_readable_id_index "
            "ON readable_meta (readable_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS readable_meta_title_text_map_hash_index "
            "ON readable_meta (title_text_map_hash)"
        )
    if commit:
        connection.commit()


def _default_connection_and_data_path():
    from DBConfig import DATA_PATH, conn

    return conn, DATA_PATH


def _load_rows(data_path: str, file_name: str) -> list[dict[str, Any]]:
    path = os.path.join(data_path, "ExcelBinOutput", file_name)
    rows = load_json_file(path, default=[])
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _normalize_item_match_text(text: str | None) -> str:
    normalized = unicodedata.normalize("NFKC", str(text or "")).strip().lower()
    return _READABLE_ITEM_MATCH_STRIP_RE.sub("", normalized)


def _build_lookup(data_path: str) -> _ReadableMetaLookup:
    material_rows = _load_rows(data_path, "MaterialExcelConfigData.json")
    material_ids = {
        int(row["id"])
        for row in material_rows
        if isinstance(row.get("id"), int) and row.get("id")
    }
    item_name_hash_by_item_id: dict[int, int] = {}
    item_desc_hash_by_item_id: dict[int, int] = {}
    for row in material_rows:
        item_id = row.get("id")
        if not isinstance(item_id, int) or not item_id:
            continue
        name_hash = row.get("nameTextMapHash")
        desc_hash = row.get("descTextMapHash")
        if isinstance(name_hash, int) and name_hash:
            item_name_hash_by_item_id[item_id] = name_hash
        if isinstance(desc_hash, int) and desc_hash:
            item_desc_hash_by_item_id[item_id] = desc_hash

    book_material_ids = {
        int(row["materialId"])
        for row in _load_rows(data_path, "BooksCodexExcelConfigData.json")
        if isinstance(row.get("materialId"), int) and row.get("materialId")
    }

    readable_localization_ids: set[int] = set()
    for row in _load_rows(data_path, "LocalizationExcelConfigData.json"):
        loc_id = row.get("id")
        if not isinstance(loc_id, int) or not loc_id:
            continue
        if any(isinstance(value, str) and "Readable" in value for value in row.values()):
            readable_localization_ids.add(loc_id)

    codex_readable_ids: set[int] = set()
    codex_title_hashes: set[int] = set()
    item_ids_by_readable_id: dict[int, int] = {}
    item_ids_by_title_hash: dict[int, int] = {}
    for row in _load_rows(data_path, "DocumentExcelConfigData.json"):
        item_id = row.get("id")
        if not isinstance(item_id, int) or not item_id or item_id not in material_ids:
            continue

        readable_ids: list[int] = []
        quest_id_list = row.get("questIDList")
        if isinstance(quest_id_list, list):
            for loc_id in quest_id_list:
                if not isinstance(loc_id, int) or loc_id not in readable_localization_ids:
                    continue
                readable_ids.append(loc_id)
                item_ids_by_readable_id.setdefault(loc_id, item_id)

        if not readable_ids:
            continue

        title_hash = row.get("titleTextMapHash")
        if isinstance(title_hash, int) and title_hash:
            item_ids_by_title_hash.setdefault(title_hash, item_id)
            if item_id in book_material_ids:
                codex_title_hashes.add(title_hash)
        if item_id in book_material_ids:
            codex_readable_ids.update(readable_ids)

    return {
        "codex_readable_ids": codex_readable_ids,
        "codex_title_hashes": codex_title_hashes,
        "item_ids_by_readable_id": item_ids_by_readable_id,
        "item_ids_by_title_hash": item_ids_by_title_hash,
        "item_name_hash_by_item_id": item_name_hash_by_item_id,
        "item_desc_hash_by_item_id": item_desc_hash_by_item_id,
    }


def _load_text_hash_state(connection, hashes: set[int]) -> tuple[dict[int, str], set[int]]:
    if not hashes:
        return {}, set()
    preferred_text_by_hash: dict[int, str] = {}
    fallback_text_by_hash: dict[int, str] = {}
    visible_hashes: set[int] = set()
    hash_list = sorted(int(value) for value in hashes)
    with closing(connection.cursor()) as cursor:
        for start in range(0, len(hash_list), 500):
            batch = hash_list[start : start + 500]
            placeholders = ",".join(["?"] * len(batch))
            rows = cursor.execute(
                f"SELECT hash, lang, content FROM textMap WHERE hash IN ({placeholders})",
                batch,
            ).fetchall()
            for raw_hash, raw_lang, raw_content in rows:
                if raw_hash is None:
                    continue
                hash_value = int(raw_hash)
                content = str(raw_content or "").strip()
                if not content:
                    continue
                visible_hashes.add(hash_value)
                lang_value = int(raw_lang or 0)
                if lang_value == _SOURCE_LANG_CODE and hash_value not in preferred_text_by_hash:
                    preferred_text_by_hash[hash_value] = content
                fallback_text_by_hash.setdefault(hash_value, content)
    for hash_value, content in fallback_text_by_hash.items():
        preferred_text_by_hash.setdefault(hash_value, content)
    return preferred_text_by_hash, visible_hashes


def _resolve_item_id(
    lookup: _ReadableMetaLookup,
    readable_id: int | None,
    title_text_map_hash: int | None,
) -> int | None:
    if readable_id is not None:
        item_id = lookup["item_ids_by_readable_id"].get(int(readable_id))
        if item_id:
            return int(item_id)
    if title_text_map_hash is not None:
        item_id = lookup["item_ids_by_title_hash"].get(int(title_text_map_hash))
        if item_id:
            return int(item_id)
    return None


def resolve_readable_category(
    file_name: str | None,
    *,
    readable_id: int | None = None,
    title_text_map_hash: int | None = None,
    lookup: _ReadableMetaLookup,
    preferred_text_by_hash: dict[int, str],
    visible_hashes: set[int],
) -> str:
    normalized_file_name = normalize_readable_file_name(file_name)
    stem = os.path.splitext(normalized_file_name)[0]
    if stem.startswith("Costume"):
        return "COSTUME"
    if stem.startswith("Relic"):
        return "RELIC"
    if stem.startswith("Weapon"):
        return "WEAPON"
    if stem.startswith("Wings"):
        return "WINGS"
    if not stem.startswith("Book"):
        return "READABLE"

    if readable_id is not None and int(readable_id) in lookup["codex_readable_ids"]:
        return "BOOK"
    if title_text_map_hash is not None and int(title_text_map_hash) in lookup["codex_title_hashes"]:
        return "BOOK"

    item_id = _resolve_item_id(lookup, readable_id, title_text_map_hash)
    if item_id is None:
        return "READABLE"

    item_name_hash = lookup["item_name_hash_by_item_id"].get(item_id)
    item_desc_hash = lookup["item_desc_hash_by_item_id"].get(item_id)
    item_name = preferred_text_by_hash.get(int(item_name_hash)) if item_name_hash else None
    readable_title = preferred_text_by_hash.get(int(title_text_map_hash)) if title_text_map_hash else None
    normalized_item_name = _normalize_item_match_text(item_name)
    normalized_readable_title = _normalize_item_match_text(readable_title)
    if (
        normalized_item_name
        and normalized_readable_title
        and normalized_item_name != normalized_readable_title
    ):
        return "ITEM"

    if item_desc_hash is None or int(item_desc_hash) not in visible_hashes:
        return "READABLE"
    item_desc = preferred_text_by_hash.get(int(item_desc_hash))
    normalized_item_desc = _normalize_item_match_text(item_desc)
    if (
        normalized_item_desc
        and (not normalized_readable_title or normalized_item_desc != normalized_readable_title)
        and (not normalized_item_name or normalized_item_desc != normalized_item_name)
    ):
        return "ITEM"
    return "READABLE"


def _load_canonical_readable_rows(connection) -> list[tuple[str, int | None, int | None]]:
    with closing(connection.cursor()) as cursor:
        rows = cursor.execute(
            "SELECT fileName, readableId, titleTextMapHash FROM readable ORDER BY fileName, lang"
        ).fetchall()

    grouped: dict[str, list[tuple[str, int | None, int | None]]] = {}
    for file_name, readable_id, title_text_map_hash in rows:
        normalized_file_name = normalize_readable_file_name(file_name)
        if not normalized_file_name:
            continue
        grouped.setdefault(normalized_file_name, []).append(
            (
                str(file_name),
                int(readable_id) if readable_id is not None else None,
                int(title_text_map_hash) if title_text_map_hash is not None else None,
            )
        )

    canonical_rows: list[tuple[str, int | None, int | None]] = []
    for normalized_file_name, variants in sorted(grouped.items()):
        readable_ids = {
            int(readable_id)
            for _file_name, readable_id, _title_hash in variants
            if readable_id is not None
        }
        title_hashes = {
            int(title_hash)
            for _file_name, _readable_id, title_hash in variants
            if title_hash is not None
        }
        if len(readable_ids) > 1 or len(title_hashes) > 1:
            raise RuntimeError(
                "Readable meta collision detected for "
                f"{normalized_file_name}: readable_ids={sorted(readable_ids)}, "
                f"title_text_map_hashes={sorted(title_hashes)}"
            )
        canonical_rows.append(
            (
                normalized_file_name,
                next(iter(readable_ids)) if readable_ids else None,
                next(iter(title_hashes)) if title_hashes else None,
            )
        )
    return canonical_rows


def refresh_readable_meta(
    *,
    connection=None,
    data_path: str | None = None,
    commit: bool = True,
) -> list[tuple[str, int | None, int | None, str]]:
    if connection is None or data_path is None:
        default_connection, default_data_path = _default_connection_and_data_path()
        connection = connection or default_connection
        data_path = data_path or default_data_path

    ensure_readable_meta_schema(connection, commit=False)
    lookup = _build_lookup(str(data_path))
    canonical_rows = _load_canonical_readable_rows(connection)
    text_hashes: set[int] = set()
    for _file_name, readable_id, title_hash in canonical_rows:
        if title_hash is not None:
            text_hashes.add(int(title_hash))
        item_id = _resolve_item_id(lookup, readable_id, title_hash)
        if item_id is None:
            continue
        item_name_hash = lookup["item_name_hash_by_item_id"].get(item_id)
        item_desc_hash = lookup["item_desc_hash_by_item_id"].get(item_id)
        if item_name_hash is not None:
            text_hashes.add(int(item_name_hash))
        if item_desc_hash is not None:
            text_hashes.add(int(item_desc_hash))

    preferred_text_by_hash, visible_hashes = _load_text_hash_state(connection, text_hashes)
    meta_rows = [
        (
            normalized_file_name,
            readable_id,
            title_hash,
            resolve_readable_category(
                normalized_file_name,
                readable_id=readable_id,
                title_text_map_hash=title_hash,
                lookup=lookup,
                preferred_text_by_hash=preferred_text_by_hash,
                visible_hashes=visible_hashes,
            ),
        )
        for normalized_file_name, readable_id, title_hash in canonical_rows
    ]

    try:
        with closing(connection.cursor()) as cursor:
            cursor.execute("DELETE FROM readable_meta")
            cursor.executemany(
                """
                INSERT INTO readable_meta(
                    normalized_file_name,
                    readable_id,
                    title_text_map_hash,
                    readable_category
                ) VALUES (?,?,?,?)
                """,
                meta_rows,
            )
        if commit:
            connection.commit()
    except Exception:
        if commit:
            connection.rollback()
        raise

    category_counter = Counter(category for _name, _readable_id, _title_hash, category in meta_rows)
    summary = ", ".join(
        f"{category}={category_counter[category]}"
        for category in READABLE_META_CATEGORY_CODES
        if category_counter.get(category)
    )
    print(f"Readable meta refreshed: total={len(meta_rows)}{', ' + summary if summary else ''}")
    return meta_rows

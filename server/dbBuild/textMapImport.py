import os
import json
from lightweight_progress import LightweightProgress

from DBConfig import conn, LANG_PATH
from import_utils import (
    DEFAULT_BATCH_SIZE,
    drop_temp_table,
    executemany_batched,
    iter_batches,
    print_skip_summary as _print_skip_summary,
    print_summary as _print_issue_summary,
    reset_temp_table,
    to_hash_value,
)
from textmap_match_utils import (
    TEXTMAP_MATCH_KIND_NEW,
    TEXTMAP_MATCH_KIND_SAME_CONTENT,
    build_textmap_lineage_states,
    match_textmap_lineage_to_previous,
    textmap_values_match,
)
from version_control import ensure_version_schema, get_current_version, get_or_create_version_id
from textmap_name_utils import parse_textmap_file_name, textmap_file_sort_key
def _load_existing_textmap_content_by_hash(
    cursor,
    lang_id: int,
    hashes: list,
    *,
    chunk_size: int = 900,
) -> dict:
    """
    Load existing textMap content for target hashes in chunks.
    SQLite has a bound parameter limit, so keep IN list size conservative.
    """
    existing: dict = {}
    if not hashes:
        return existing

    dedup_hashes = list(dict.fromkeys(hashes))
    safe_chunk_size = max(1, min(int(chunk_size), 900))
    for batch in iter_batches(dedup_hashes, safe_chunk_size):
        placeholders = ",".join(["?"] * len(batch))
        sql = f"SELECT hash, content FROM textMap WHERE lang=? AND hash IN ({placeholders})"
        cursor.execute(sql, (lang_id, *batch))
        for row_hash, row_content in cursor.fetchall():
            existing[row_hash] = row_content
    return existing


def _load_existing_textmap_rows(cursor, lang_id: int) -> dict[int, tuple[object, int | None, int | None]]:
    rows = cursor.execute(
        "SELECT hash, content, created_version_id, updated_version_id FROM textMap WHERE lang=?",
        (lang_id,),
    ).fetchall()
    return {
        int(row_hash): (row_content, created_version_id, updated_version_id)
        for row_hash, row_content, created_version_id, updated_version_id in rows
    }


def _build_plain_textmap_upsert_sql() -> str:
    return (
        "INSERT INTO textMap(hash, content, lang) VALUES (?,?,?) "
        "ON CONFLICT(lang, hash) DO UPDATE SET "
        "content=excluded.content "
        "WHERE NOT (textMap.content IS excluded.content)"
    )


def _build_versioned_textmap_upsert_sql() -> str:
    return (
        "INSERT INTO textMap(hash, content, lang, created_version_id, updated_version_id) "
        "VALUES (?,?,?,?,?) "
        "ON CONFLICT(lang, hash) DO UPDATE SET "
        "content=excluded.content, "
        "created_version_id=excluded.created_version_id, "
        "updated_version_id=excluded.updated_version_id "
        "WHERE NOT (textMap.content IS excluded.content) "
        "OR NOT (textMap.created_version_id IS excluded.created_version_id) "
        "OR NOT (textMap.updated_version_id IS excluded.updated_version_id)"
    )


def _build_versioned_textmap_row_plan(
    *,
    current_obj: dict[object, object],
    existing_rows_by_hash: dict[int, tuple[object, int | None, int | None]],
    version_id: int,
) -> dict[int, tuple[object, int | None, int | None]]:
    current_states = build_textmap_lineage_states(current_obj)
    existing_content_by_hash = {
        hash_value: row[0]
        for hash_value, row in existing_rows_by_hash.items()
    }
    predecessor_matches = match_textmap_lineage_to_previous(
        current_states,
        existing_content_by_hash,
    )

    row_plan: dict[int, tuple[object, int | None, int | None]] = {}
    for hash_value in sorted(current_states.keys()):
        current_content = current_states[hash_value].content
        predecessor = predecessor_matches.get(hash_value)
        predecessor_hash = predecessor.predecessor_hash if predecessor is not None else None
        if predecessor_hash is not None:
            _old_content, old_created_version, old_updated_version = existing_rows_by_hash[predecessor_hash]
            created_version = old_created_version if old_created_version is not None else version_id
            if predecessor.match_kind == TEXTMAP_MATCH_KIND_SAME_CONTENT:
                updated_version = old_updated_version
                if updated_version is None:
                    updated_version = created_version if created_version is not None else version_id
            else:
                updated_version = version_id
        else:
            created_version = version_id
            updated_version = version_id

        if predecessor is not None and predecessor.match_kind == TEXTMAP_MATCH_KIND_NEW:
            created_version = version_id
            updated_version = version_id
        row_plan[hash_value] = (current_content, created_version, updated_version)

    return row_plan


def _import_textmap(
    baseMapName: str,
    fileList: list[str],
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    force_reimport: bool = False,
    prune_missing: bool = True,
    version_id: int | None = None,
):
    """
    :param baseMapName: Base language config entry name, e.g. TextMapRU.json
    :param fileList: Split files for that base map, e.g. [TextMapRU_0.json, TextMapRU_1.json]
    """
    ensure_version_schema()
    cursor = conn.cursor()
    sql_lang = "select id, imported from langCode where codeName = ?"
    cursor.execute(sql_lang, (baseMapName,))
    ans2 = cursor.fetchall()
    if len(ans2) == 0:
        print(f"{baseMapName} (Base for {fileList}) not found in langCode table")
        cursor.close()
        return

    langId = ans2[0][0]
    imported = ans2[0][1]

    if imported == 1 and not force_reimport:
        ans = input(
            f"{baseMapName} already imported. Reimport and overwrite this language? (y/n): "
        )
        if ans != "y":
            cursor.close()
            return

    print(f"Reimporting {baseMapName} (ID: {langId})...")
    reset_temp_table(
        cursor,
        "CREATE TEMP TABLE IF NOT EXISTS _seen_textmap_hash(hash INTEGER PRIMARY KEY)",
        "_seen_textmap_hash",
    )

    sql_seen = "INSERT OR IGNORE INTO _seen_textmap_hash(hash) VALUES (?)"
    missing_files: list[str] = []
    import_errors: list[str] = []
    compared_hash_count = 0
    changed_hash_count = 0
    merged_textmap: dict[object, object] = {}

    with LightweightProgress(len(fileList), desc=f"{baseMapName} files", unit="files") as pbar:
        for fileName in fileList:
            filePath = os.path.join(LANG_PATH, fileName)
            if not os.path.exists(filePath):
                missing_files.append(fileName)
                pbar.update()
                continue

            try:
                with open(filePath, encoding="utf-8") as f:
                    textMap = json.load(f)
                if not isinstance(textMap, dict):
                    raise TypeError("TextMap payload must be a JSON object")
                merged_textmap.update(textMap)

            except Exception as e:
                import_errors.append(f"{fileName} ({e})")
            finally:
                pbar.update()

    parsed_rows: list[tuple[int, object]] = []
    parsed_hashes: list[int] = []
    for hashVal, content in merged_textmap.items():
        parsed_hash = int(to_hash_value(hashVal))
        parsed_rows.append((parsed_hash, content))
        parsed_hashes.append(parsed_hash)
    compared_hash_count = len(parsed_rows)

    if version_id is None:
        sql_upsert = _build_plain_textmap_upsert_sql()
        existing_map = _load_existing_textmap_content_by_hash(
            cursor,
            langId,
            parsed_hashes,
        )
        changed_rows = []
        for row_hash, content in parsed_rows:
            old_content = existing_map.get(row_hash)
            if not textmap_values_match(old_content, content):
                changed_rows.append((row_hash, content, langId))
        changed_hash_count += executemany_batched(
            cursor,
            sql_upsert,
            changed_rows,
            batch_size=batch_size,
        )
    else:
        sql_upsert = _build_versioned_textmap_upsert_sql()
        existing_rows_by_hash = _load_existing_textmap_rows(cursor, langId)
        row_plan = _build_versioned_textmap_row_plan(
            current_obj=merged_textmap,
            existing_rows_by_hash=existing_rows_by_hash,
            version_id=version_id,
        )
        changed_rows = []
        for row_hash, content, created_version_id, updated_version_id in (
            (hash_value, *row_plan[hash_value]) for hash_value in sorted(row_plan.keys())
        ):
            existing_row = existing_rows_by_hash.get(row_hash)
            if existing_row is not None:
                old_content, old_created_version, old_updated_version = existing_row
                if (
                    textmap_values_match(old_content, content)
                    and old_created_version == created_version_id
                    and old_updated_version == updated_version_id
                ):
                    continue
            changed_rows.append(
                (row_hash, content, langId, created_version_id, updated_version_id)
            )
        changed_hash_count += executemany_batched(
            cursor,
            sql_upsert,
            changed_rows,
            batch_size=batch_size,
        )

    seen_iter = ((row_hash,) for row_hash in parsed_hashes)
    executemany_batched(cursor, sql_seen, seen_iter, batch_size=batch_size)

    if prune_missing:
        cursor.execute(
            "DELETE FROM textMap WHERE lang=? AND hash NOT IN (SELECT hash FROM _seen_textmap_hash)",
            (langId,),
        )
    cursor.execute("update langCode set imported=1 where id=?", (langId,))
    drop_temp_table(cursor, "_seen_textmap_hash")
    cursor.close()
    _print_issue_summary(f"textmap missing files ({baseMapName})", missing_files)
    _print_issue_summary(f"textmap import errors ({baseMapName})", import_errors)

    if commit:
        conn.commit()
    print(
        f"TextMap diff summary ({baseMapName}): "
        f"checked={compared_hash_count}, changed={changed_hash_count}"
    )
    print(f"Done importing {baseMapName}.")


def importTextMap(
    baseMapName: str,
    fileList: list[str],
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    force_reimport: bool = False,
    prune_missing: bool = True,
):
    _import_textmap(
        baseMapName,
        fileList,
        commit=commit,
        batch_size=batch_size,
        force_reimport=force_reimport,
        prune_missing=prune_missing,
    )


def importTextMapForDiff(
    baseMapName: str,
    fileList: list[str],
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    force_reimport: bool = False,
    prune_missing: bool = True,
    current_version: str,
):
    version = current_version or get_current_version()
    version_id = get_or_create_version_id(version)
    _import_textmap(
        baseMapName,
        fileList,
        commit=commit,
        batch_size=batch_size,
        force_reimport=force_reimport,
        prune_missing=prune_missing,
        version_id=version_id,
    )


def importAllTextMap(
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    prune_missing: bool = True,
):
    if not os.path.exists(LANG_PATH):
        print(f"TextMap directory not found: {LANG_PATH}")
        return

    files = os.listdir(LANG_PATH)
    file_groups: dict[str, list[str]] = {}
    unsupported_files: list[str] = []

    for fileName in files:
        if not fileName.endswith(".json"):
            continue

        parsed = parse_textmap_file_name(fileName)
        if parsed is not None:
            base_name, _split_part = parsed
            file_groups.setdefault(base_name, []).append(fileName)
        else:
            unsupported_files.append(fileName)

    _print_skip_summary("textmap unsupported filename", unsupported_files)

    for baseMapName, fileList in file_groups.items():
        fileList.sort(key=textmap_file_sort_key)
        importTextMap(
            baseMapName,
            fileList,
            commit=False,
            batch_size=batch_size,
            prune_missing=prune_missing,
        )

    if commit:
        conn.commit()


def importAllTextMapForDiff(
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    prune_missing: bool = True,
    current_version: str,
):
    if not os.path.exists(LANG_PATH):
        print(f"TextMap directory not found: {LANG_PATH}")
        return

    files = os.listdir(LANG_PATH)
    file_groups: dict[str, list[str]] = {}
    unsupported_files: list[str] = []

    for fileName in files:
        if not fileName.endswith(".json"):
            continue

        parsed = parse_textmap_file_name(fileName)
        if parsed is not None:
            base_name, _split_part = parsed
            file_groups.setdefault(base_name, []).append(fileName)
        else:
            unsupported_files.append(fileName)

    _print_skip_summary("textmap unsupported filename", unsupported_files)

    for baseMapName, fileList in file_groups.items():
        fileList.sort(key=textmap_file_sort_key)
        importTextMapForDiff(
            baseMapName,
            fileList,
            commit=False,
            batch_size=batch_size,
            prune_missing=prune_missing,
            current_version=current_version,
        )

    if commit:
        conn.commit()


if __name__ == "__main__":
    importAllTextMap()

import os
import json
from lightweight_progress import LightweightProgress

from DBConfig import conn, LANG_PATH
from import_utils import (
    DEFAULT_BATCH_SIZE,
    build_versioned_upsert_sql,
    drop_temp_table,
    executemany_batched,
    iter_batches,
    reset_temp_table,
    to_hash_value,
)
from version_control import ensure_version_schema, get_current_version, get_or_create_version_id
from textmap_name_utils import parse_textmap_file_name, textmap_file_sort_key


def _print_issue_summary(title: str, items: list[str], sample_size: int = 10):
    if not items:
        return
    samples = items[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(items) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SUMMARY] {title}: {len(items)}. samples: {sample_text}")


def _print_skip_summary(title: str, skipped_files: list[str], sample_size: int = 10):
    if not skipped_files:
        return
    _print_issue_summary(title, skipped_files, sample_size=sample_size)


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


def _build_plain_textmap_upsert_sql() -> str:
    return (
        "INSERT INTO textMap(hash, content, lang) VALUES (?,?,?) "
        "ON CONFLICT(lang, hash) DO UPDATE SET "
        "content=excluded.content "
        "WHERE NOT (textMap.content IS excluded.content)"
    )


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
    if version_id is None:
        sql_upsert = _build_plain_textmap_upsert_sql()
    else:
        sql_upsert = build_versioned_upsert_sql(
            table="textMap",
            insert_columns=["hash", "content", "lang", "created_version_id", "updated_version_id"],
            conflict_columns=["lang", "hash"],
            update_columns=["content"],
            compare_columns=["content"],
        )
    missing_files: list[str] = []
    import_errors: list[str] = []
    compared_hash_count = 0
    changed_hash_count = 0

    with LightweightProgress(len(fileList), desc=f"{baseMapName} files", unit="files") as pbar:
        for i, fileName in enumerate(fileList):
            filePath = os.path.join(LANG_PATH, fileName)
            if not os.path.exists(filePath):
                missing_files.append(fileName)
                pbar.update()
                continue

            try:
                with open(filePath, encoding="utf-8") as f:
                    textMap = json.load(f)

                parsed_rows: list[tuple] = []
                parsed_hashes: list = []
                for hashVal, content in textMap.items():
                    parsed_hash = to_hash_value(hashVal)
                    parsed_rows.append((parsed_hash, content))
                    parsed_hashes.append(parsed_hash)

                existing_map = _load_existing_textmap_content_by_hash(
                    cursor,
                    langId,
                    parsed_hashes,
                )
                compared_hash_count += len(parsed_rows)

                # 版本预审查：只处理需要更新的行
                changed_rows = []
                for row_hash, content in parsed_rows:
                    old_content = existing_map.get(row_hash)
                    if old_content != content:
                        # 检查版本是否需要更新
                        # 由于 textMap 使用 SQL 层面的版本控制，这里只需要检查内容变化
                        if version_id is None:
                            changed_rows.append((row_hash, content, langId))
                        else:
                            changed_rows.append((row_hash, content, langId, version_id, version_id))

                changed_hash_count += executemany_batched(
                    cursor,
                    sql_upsert,
                    changed_rows,
                    batch_size=batch_size,
                )

                seen_iter = ((row_hash,) for row_hash in parsed_hashes)
                executemany_batched(cursor, sql_seen, seen_iter, batch_size=batch_size)

            except Exception as e:
                import_errors.append(f"{fileName} ({e})")
            finally:
                pbar.update()

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

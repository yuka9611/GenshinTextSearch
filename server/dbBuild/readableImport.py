import os
import sys
from lightweight_progress import LightweightProgress

from DBConfig import conn, READABLE_PATH, DATA_PATH
from import_utils import (
    DEFAULT_BATCH_SIZE,
    BufferedExecutemany,
    build_versioned_upsert_sql,
    drop_temp_table,
    reset_temp_table,
)
from localization_utils import (
    build_readable_filename_map,
    load_document_loc_title_hash,
    load_localization_entries,
)
from readable_version_utils import assign_readable_versions_by_text
from versioning import ensure_version_schema, get_current_version, get_or_create_version_id
from textmap_name_utils import analyze_readable_exceptions, report_exceptions, delete_empty_readable_entries


def _print_summary(title: str, items: list[str], sample_size: int = 10):
    if not items:
        return
    samples = items[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(items) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SUMMARY] {title}: {len(items)}. samples: {sample_text}")


def _load_readable_filename_map() -> dict:
    print("Loading document and localization configs...")
    loc_id_to_title_hash = load_document_loc_title_hash(DATA_PATH)
    localization_entries = load_localization_entries(DATA_PATH)
    filename_to_info = build_readable_filename_map(localization_entries, loc_id_to_title_hash)
    print(f"Loaded {len(filename_to_info)} readable file mappings.")
    return filename_to_info


def _build_readable_upsert_sql() -> str:
    return build_versioned_upsert_sql(
        table="readable",
        insert_columns=[
            "fileName",
            "lang",
            "content",
            "titleTextMapHash",
            "readableId",
            "created_version_id",
            "updated_version_id",
        ],
        conflict_columns=["fileName", "lang"],
        update_columns=["content", "titleTextMapHash", "readableId"],
    )


def _normalize_readable_rel_path(rel_path: str) -> tuple[str, str, str] | None:
    normalized = rel_path.replace("\\", "/").strip("/")
    parts = normalized.split("/", 1)
    if len(parts) != 2:
        return None
    lang, rel_under_lang = parts
    if not lang or not rel_under_lang:
        return None
    file_name = os.path.basename(rel_under_lang)
    if not file_name:
        return None
    full_path = os.path.join(READABLE_PATH, lang, rel_under_lang.replace("/", os.sep))
    return lang, file_name, full_path


def importReadableByFiles(
    changed_files: list[str] | set[str],
    deleted_files: list[str] | set[str],
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    current_version: str | None = None,
    refresh_mapping: bool = False,
    write_versions: bool = True,
):
    ensure_version_schema()
    version_id: int | None = None
    if write_versions:
        version = current_version or get_current_version()
        version_id = get_or_create_version_id(version)

    changed_list = sorted({p for p in (changed_files or []) if p})
    deleted_list = sorted({p for p in (deleted_files or []) if p})
    if not changed_list and not deleted_list and not refresh_mapping:
        return

    filename_to_info = _load_readable_filename_map() if (changed_list or refresh_mapping) else {}
    cursor = conn.cursor()
    upsert_sql = _build_readable_upsert_sql()
    flush_size = max(50, batch_size)
    writer = BufferedExecutemany(cursor, upsert_sql, flush_size=flush_size)

    read_errors: list[str] = []
    skipped_paths: list[str] = []
    changed_tasks: list[tuple[str, str, str]] = []
    for rel_path in changed_list:
        parsed = _normalize_readable_rel_path(rel_path)
        if parsed is None:
            skipped_paths.append(rel_path)
            continue
        changed_tasks.append(parsed)

    print(
        "Readable diff import: "
        f"changed={len(changed_tasks)}, deleted={len(deleted_list)}, remap={'yes' if refresh_mapping else 'no'}"
    )

    with LightweightProgress(len(changed_tasks), desc="Readable diff files", unit="files") as pbar:
        for lang, file_name, full_path in changed_tasks:
            if not os.path.isfile(full_path):
                skipped_paths.append(f"{lang}/{file_name} (missing)")
                pbar.update()
                continue
            name_without_ext = os.path.splitext(file_name)[0]
            info = filename_to_info.get(name_without_ext) or filename_to_info.get(file_name)
            title_hash = info["titleHash"] if info else None
            readable_id = info["readableId"] if info else None
            try:
                with open(full_path, "r", encoding="utf-8") as f:
                    content = f.read().replace("\n", "\\n")
                existing_row = cursor.execute(
                    """
                    SELECT content, created_version_id, updated_version_id
                    FROM readable
                    WHERE fileName=? AND lang=?
                    LIMIT 1
                    """,
                    (file_name, lang),
                ).fetchone()
                created_id, updated_id = assign_readable_versions_by_text(
                    existing_row,
                    content,
                    version_id,
                )
                writer.add((file_name, lang, content, title_hash, readable_id, created_id, updated_id))
            except Exception as e:
                read_errors.append(f"{lang}/{file_name} ({e})")
            finally:
                pbar.update()
    writer.flush()

    delete_rows: list[tuple[str, str]] = []
    for rel_path in deleted_list:
        parsed = _normalize_readable_rel_path(rel_path)
        if parsed is None:
            skipped_paths.append(rel_path)
            continue
        lang, file_name, _ = parsed
        delete_rows.append((file_name, lang))
    if delete_rows:
        cursor.executemany("DELETE FROM readable WHERE fileName=? AND lang=?", delete_rows)

    mapping_updated_rows = 0
    if refresh_mapping and not filename_to_info:
        print("Readable mapping refresh skipped: mapping table is empty.")
    elif refresh_mapping:
        mapping_rows: list[tuple[int | None, int | None, str, int | None, int | None]] = []
        for (file_name,) in cursor.execute("SELECT DISTINCT fileName FROM readable"):
            name_without_ext = os.path.splitext(file_name)[0]
            info = filename_to_info.get(name_without_ext) or filename_to_info.get(file_name)
            title_hash = info["titleHash"] if info else None
            readable_id = info["readableId"] if info else None
            mapping_rows.append((title_hash, readable_id, file_name, title_hash, readable_id))
        if mapping_rows:
            before = conn.total_changes
            cursor.executemany(
                """
                UPDATE readable
                SET titleTextMapHash=?, readableId=?
                WHERE fileName=?
                  AND (
                    NOT (titleTextMapHash IS ?)
                    OR NOT (readableId IS ?)
                  )
                """,
                mapping_rows,
            )
            mapping_updated_rows = conn.total_changes - before
    cursor.close()
    _print_summary("readable diff read errors", read_errors)
    _print_summary("readable diff skipped paths", skipped_paths)
    if refresh_mapping:
        print(f"Readable mapping refresh rows: {mapping_updated_rows}")

    if commit:
        conn.commit()


def importReadable(
    *,
    commit: bool = True,
    reset: bool = False,
    prune_missing: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    current_version: str | None = None,
    write_versions: bool = True,
):
    ensure_version_schema()
    version_id: int | None = None
    if write_versions:
        version = current_version or get_current_version()
        version_id = get_or_create_version_id(version)

    filename_to_info = _load_readable_filename_map()

    cursor = conn.cursor()
    if reset:
        cursor.execute("DELETE FROM readable")

    reset_temp_table(
        cursor,
        "CREATE TEMP TABLE IF NOT EXISTS _seen_readable(fileName TEXT, lang TEXT, PRIMARY KEY(fileName, lang))",
        "_seen_readable",
    )

    upsert_sql = _build_readable_upsert_sql()
    seen_sql = "INSERT OR IGNORE INTO _seen_readable(fileName, lang) VALUES (?,?)"

    if not os.path.exists(READABLE_PATH):
        print(f"Readable path not found: {READABLE_PATH}")
        cursor.close()
        return

    flush_size = max(50, batch_size)
    writer = BufferedExecutemany(
        cursor,
        upsert_sql,
        flush_size=flush_size,
        secondary_sql=seen_sql,
    )

    file_tasks: list[tuple[str, str, str]] = []
    for lang in sorted(os.listdir(READABLE_PATH)):
        langPath = os.path.join(READABLE_PATH, lang)
        if not os.path.isdir(langPath):
            continue
        for root, _, files in os.walk(langPath):
            for fileName in sorted(files):
                filePath = os.path.join(root, fileName)
                if os.path.isfile(filePath):
                    # 计算相对路径，保持与Subtitle导入逻辑一致
                    rel_path = os.path.relpath(filePath, langPath)
                    clean_file_name = rel_path.replace(os.sep, "/")
                    file_tasks.append((lang, clean_file_name, filePath))

    print(f"Importing readable files ({len(file_tasks)})...")
    read_errors: list[str] = []
    readable_data: list[tuple[str, str, str]] = []
    with LightweightProgress(len(file_tasks), desc="Readable files", unit="files") as pbar:
        for lang, clean_file_name, filePath in file_tasks:
            name_without_ext = os.path.splitext(clean_file_name)[0]
            # 尝试使用不同的键来查找信息
            base_name = os.path.basename(clean_file_name)
            base_name_without_ext = os.path.splitext(base_name)[0]
            info = filename_to_info.get(name_without_ext) or filename_to_info.get(base_name) or filename_to_info.get(base_name_without_ext)

            title_hash = info["titleHash"] if info else None
            readable_id = info["readableId"] if info else None

            try:
                with open(filePath, "r", encoding="utf-8") as f:
                    content = f.read().replace("\n", "\\n")
                writer.add(
                    (clean_file_name, lang, content, title_hash, readable_id, version_id, version_id),
                    (clean_file_name, lang),
                )
                readable_data.append((clean_file_name, lang, content))
            except Exception as e:
                read_errors.append(f"{lang}/{clean_file_name} ({e})")
            finally:
                pbar.update()

    writer.flush()
    
    # 分析并报告异常情况
    if readable_data:
        exception_data = analyze_readable_exceptions(readable_data)
        report_exceptions(exception_data, "Readable")
        
        # 删除源文件和数据库中内容均为空白的条目
        print("删除空白Readable条目...")
        delete_count = delete_empty_readable_entries(cursor, READABLE_PATH)
        print(f"已删除 {delete_count} 个空白Readable条目")
    if prune_missing:
        cursor.execute(
            """
            DELETE FROM readable
            WHERE (fileName, lang) NOT IN (
                SELECT fileName, lang FROM _seen_readable
            )
            """
        )
    drop_temp_table(cursor, "_seen_readable")
    cursor.close()
    _print_summary("readable read errors", read_errors)

    if commit:
        conn.commit()

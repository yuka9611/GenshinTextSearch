import os
import sys
from lightweight_progress import LightweightProgress

from DBConfig import conn, DATA_PATH
from import_utils import (
    DEFAULT_BATCH_SIZE,
    BufferedExecutemany,
    build_versioned_upsert_sql,
    drop_temp_table,
    reset_temp_table,
)
from lang_constants import LANG_CODE_MAP
from localization_utils import build_subtitle_filename_map, load_localization_entries
from subtitle_utils import iter_srt_entries, subtitle_key
from subtitle_version_utils import assign_subtitle_versions_by_text
from versioning import ensure_version_schema, get_current_version, get_or_create_version_id
from textmap_name_utils import analyze_subtitle_exceptions, report_exceptions, delete_empty_subtitle_entries


def _print_summary(title: str, items: list[str], sample_size: int = 10):
    if not items:
        return
    samples = items[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(items) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SUMMARY] {title}: {len(items)}. samples: {sample_text}")


def _load_subtitle_filename_map() -> dict:
    print("Loading localization configs for subtitles...")
    localization_entries = load_localization_entries(DATA_PATH)
    filename_to_info = build_subtitle_filename_map(localization_entries)
    print(f"Loaded {len(filename_to_info)} subtitle file mappings.")
    return filename_to_info


def _build_subtitle_upsert_sql() -> str:
    return build_versioned_upsert_sql(
        table="subtitle",
        insert_columns=[
            "fileName",
            "lang",
            "startTime",
            "endTime",
            "content",
            "subtitleId",
            "subtitleKey",
            "created_version_id",
            "updated_version_id",
        ],
        conflict_columns=["subtitleKey"],
        update_columns=["fileName", "lang", "startTime", "endTime", "content", "subtitleId"],
    )


def _normalize_subtitle_rel_path(rel_path: str) -> tuple[str, int, str, str] | None:
    normalized = rel_path.replace("\\", "/").strip("/")
    parts = normalized.split("/", 1)
    if len(parts) != 2:
        return None
    lang_name, rel_under_lang = parts
    if not lang_name or not rel_under_lang:
        return None
    lang_id = LANG_CODE_MAP.get(lang_name)
    if lang_id is None:
        return None
    if not rel_under_lang.lower().endswith(".srt"):
        return None
    clean_file_name = os.path.splitext(rel_under_lang)[0].replace("\\", "/")
    full_path = os.path.join(DATA_PATH, "Subtitle", lang_name, rel_under_lang.replace("/", os.sep))
    return lang_name, lang_id, clean_file_name, full_path


def importSubtitlesByFiles(
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

    filename_to_info = _load_subtitle_filename_map() if (changed_list or refresh_mapping) else {}
    cursor = conn.cursor()
    upsert_sql = _build_subtitle_upsert_sql()
    flush_size = max(100, batch_size)
    writer = BufferedExecutemany(cursor, upsert_sql, flush_size=flush_size)

    process_errors: list[str] = []
    skipped_paths: list[str] = []
    changed_tasks: list[tuple[str, int, str, str]] = []
    for rel_path in changed_list:
        parsed = _normalize_subtitle_rel_path(rel_path)
        if parsed is None:
            skipped_paths.append(rel_path)
            continue
        changed_tasks.append(parsed)

    print(
        "Subtitle diff import: "
        f"changed={len(changed_tasks)}, deleted={len(deleted_list)}, remap={'yes' if refresh_mapping else 'no'}"
    )

    with LightweightProgress(len(changed_tasks), desc="Subtitle diff files", unit="files") as pbar:
        for lang_name, lang_id, clean_file_name, full_path in changed_tasks:
            if not os.path.isfile(full_path):
                skipped_paths.append(f"{lang_name}/{clean_file_name}.srt (missing)")
                pbar.update()
                continue
            base_stem = os.path.splitext(os.path.basename(full_path))[0]
            info = filename_to_info.get(base_stem)
            subtitle_id = info["subtitleId"] if info else None
            try:
                existing_rows = cursor.execute(
                    """
                    SELECT subtitleKey, content, created_version_id, updated_version_id
                    FROM subtitle
                    WHERE fileName=? AND lang=?
                    """,
                    (clean_file_name, lang_id),
                ).fetchall()
                with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                parsed_rows: list[tuple[str, float, float, str]] = []
                for start_time, end_time, text_content in iter_srt_entries(content):
                    key = subtitle_key(clean_file_name, lang_id, start_time, end_time)
                    parsed_rows.append((key, start_time, end_time, text_content))

                assigned_rows = assign_subtitle_versions_by_text(
                    existing_rows,
                    parsed_rows,
                    version_id,
                )

                cursor.execute("DELETE FROM subtitle WHERE fileName=? AND lang=?", (clean_file_name, lang_id))
                for key, start_time, end_time, text_content, created_id, updated_id in assigned_rows:
                    writer.add(
                        (
                            clean_file_name,
                            lang_id,
                            start_time,
                            end_time,
                            text_content,
                            subtitle_id,
                            key,
                            created_id,
                            updated_id,
                        )
                    )
            except Exception as e:
                process_errors.append(f"{lang_name}/{os.path.basename(full_path)} ({e})")
            finally:
                pbar.update()
    writer.flush()

    delete_rows: list[tuple[str, int]] = []
    for rel_path in deleted_list:
        parsed = _normalize_subtitle_rel_path(rel_path)
        if parsed is None:
            skipped_paths.append(rel_path)
            continue
        _, lang_id, clean_file_name, _ = parsed
        delete_rows.append((clean_file_name, lang_id))
    if delete_rows:
        cursor.executemany("DELETE FROM subtitle WHERE fileName=? AND lang=?", delete_rows)

    mapping_updated_rows = 0
    if refresh_mapping and not filename_to_info:
        print("Subtitle mapping refresh skipped: mapping table is empty.")
    elif refresh_mapping:
        mapping_rows: list[tuple[int | None, str, int | None]] = []
        for (file_name,) in cursor.execute("SELECT DISTINCT fileName FROM subtitle"):
            base_stem = os.path.splitext(os.path.basename(file_name))[0]
            info = filename_to_info.get(base_stem)
            subtitle_id = info["subtitleId"] if info else None
            mapping_rows.append((subtitle_id, file_name, subtitle_id))
        if mapping_rows:
            before = conn.total_changes
            cursor.executemany(
                """
                UPDATE subtitle
                SET subtitleId=?
                WHERE fileName=?
                  AND NOT (subtitleId IS ?)
                """,
                mapping_rows,
            )
            mapping_updated_rows = conn.total_changes - before

    cursor.close()
    _print_summary("subtitle diff parse/import errors", process_errors)
    _print_summary("subtitle diff skipped paths", skipped_paths)
    if refresh_mapping:
        print(f"Subtitle mapping refresh rows: {mapping_updated_rows}")

    if commit:
        conn.commit()


def importSubtitles(
    *,
    commit: bool = True,
    reset: bool = False,
    prune_missing: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    current_version: str | None = None,
):
    ensure_version_schema()

    filename_to_info = _load_subtitle_filename_map()

    print("Importing Subtitles (srt)...")
    cursor = conn.cursor()
    if reset:
        cursor.execute("DELETE FROM subtitle")

    reset_temp_table(
        cursor,
        "CREATE TEMP TABLE IF NOT EXISTS _seen_subtitle_key(subtitleKey TEXT PRIMARY KEY)",
        "_seen_subtitle_key",
    )

    subtitle_root = os.path.join(DATA_PATH, "Subtitle")
    if not os.path.exists(subtitle_root):
        print(f"Subtitle path not found: {subtitle_root}")
        cursor.close()
        return

    upsert_sql = _build_subtitle_upsert_sql()
    seen_sql = "INSERT OR IGNORE INTO _seen_subtitle_key(subtitleKey) VALUES (?)"

    flush_size = max(100, batch_size)
    writer = BufferedExecutemany(
        cursor,
        upsert_sql,
        flush_size=flush_size,
        secondary_sql=seen_sql,
    )

    subtitle_tasks: list[tuple[str, int, str, str]] = []
    for lang_name, lang_id in LANG_CODE_MAP.items():
        lang_path = os.path.join(subtitle_root, lang_name)
        if not os.path.exists(lang_path):
            continue
        for root, _, files in os.walk(lang_path):
            for file_name in files:
                if file_name.endswith(".srt"):
                    subtitle_tasks.append((lang_name, lang_id, lang_path, os.path.join(root, file_name)))

    print(f"Processing subtitle files ({len(subtitle_tasks)})...")
    process_errors: list[str] = []
    subtitle_data: list[tuple[str, int, str, float, float, str]] = []
    with LightweightProgress(len(subtitle_tasks), desc="Subtitle files", unit="files") as pbar:
        for lang_name, lang_id, lang_path, full_path in subtitle_tasks:
            file_name = os.path.basename(full_path)
            name_without_ext = os.path.splitext(file_name)[0]

            rel_path = os.path.relpath(full_path, lang_path)
            cleanFileName = os.path.splitext(rel_path)[0].replace(os.sep, "/")

            info = filename_to_info.get(name_without_ext)
            subtitle_id = info["subtitleId"] if info else None

            try:
                with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()

                for start_time, end_time, text_content in iter_srt_entries(content):
                    key = subtitle_key(cleanFileName, lang_id, start_time, end_time)
                    writer.add(
                        (
                            cleanFileName,
                            lang_id,
                            start_time,
                            end_time,
                            text_content,
                            subtitle_id,
                            key,
                            None,
                            None,
                        ),
                        (key,),
                    )
                subtitle_data.append((cleanFileName, lang_id, key, start_time, end_time, text_content))

            except Exception as e:
                process_errors.append(f"{lang_name}/{file_name} ({e})")
            finally:
                pbar.update()

    writer.flush()
    
    # 分析并报告异常情况
    if subtitle_data:
        exception_data = analyze_subtitle_exceptions(subtitle_data)
        report_exceptions(exception_data, "Subtitle")
        
        # 删除源文件和数据库中内容均为空白的条目
        print("删除空白Subtitle条目...")
        subtitle_root = os.path.join(DATA_PATH, "Subtitle")
        delete_count = delete_empty_subtitle_entries(cursor, subtitle_root)
        print(f"已删除 {delete_count} 个空白Subtitle条目")
    if prune_missing:
        cursor.execute(
            """
            DELETE FROM subtitle
            WHERE subtitleKey IS NULL
               OR subtitleKey NOT IN (SELECT subtitleKey FROM _seen_subtitle_key)
            """
        )
    drop_temp_table(cursor, "_seen_subtitle_key")
    cursor.close()
    _print_summary("subtitle parse/import errors", process_errors)

    if commit:
        conn.commit()

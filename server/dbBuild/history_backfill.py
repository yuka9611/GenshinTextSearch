import json
import os
import sqlite3
import subprocess
import sys

from DBConfig import conn, DATA_PATH
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched, to_hash_value as _to_hash_value
from lang_constants import LANG_CODE_MAP
from quest_hash_map_utils import (
    count_unresolved_quest_versions as _count_unresolved_quest_versions,
    refresh_all_quest_hash_map as _refresh_all_quest_hash_map,
    unresolved_created_quest_ids as _unresolved_created_quest_ids,
)
from quest_utils import extract_quest_row as _extract_quest_row
from quest_version_utils import backfill_quest_created_version_from_textmap as _backfill_quest_created_version_from_textmap
from readable_version_utils import readable_text_changed as _readable_text_changed
from subtitle_utils import parse_srt_rows as _parse_srt_rows
from subtitle_version_utils import subtitle_text_changed_keys as _subtitle_text_changed_keys
from textmap_name_utils import parse_textmap_file_name
from versioning import (
    ensure_version_schema,
    get_meta,
    get_or_create_version_id,
    normalize_version_label,
    rebuild_version_catalog,
    set_meta,
)
from tqdm import tqdm


RELEVANT_PATHS = [
    "TextMap",
    "Readable",
    "Subtitle",
    "BinOutput/Quest",
    "ExcelBinOutput/LocalizationExcelConfigData.json",
    "ExcelBinOutput/DocumentExcelConfigData.json",
]
TEXTMAP_ONLY_PATHS = ["TextMap"]
READABLE_ONLY_PATHS = ["Readable"]
SUBTITLE_ONLY_PATHS = ["Subtitle"]


def _table_columns(table_name: str) -> set[str]:
    cur = conn.cursor()
    try:
        rows = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
    finally:
        cur.close()
    return {row[1] for row in rows}


def _table_exists(table_name: str) -> bool:
    cur = conn.cursor()
    try:
        row = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None
    finally:
        cur.close()


def _resolve_version_id(version_label: str) -> int | None:
    return get_or_create_version_id(normalize_version_label(version_label) or version_label)

def _run_git(repo_path: str, args: list[str], check: bool = True) -> str:
    proc = subprocess.run(
        ["git", "-C", repo_path] + args,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git command failed")
    return (proc.stdout or "").strip()


def _resolve_commit(repo_path: str, rev: str) -> str:
    return _run_git(repo_path, ["rev-parse", rev], check=True)


def _resolve_commit_title(repo_path: str, commit_sha: str) -> str:
    title = _run_git(repo_path, ["show", "-s", "--format=%s", commit_sha], check=False).strip()
    return title or commit_sha


def _list_commits(
    repo_path: str,
    target_commit: str,
    *,
    from_commit: str | None = None,
) -> list[tuple[str, str]]:
    out = _run_git(
        repo_path,
        ["log", "--reverse", "--format=%H%x1f%s", target_commit],
        check=True,
    )
    commits = []
    for line in out.splitlines():
        if not line:
            continue
        if "\x1f" in line:
            sha, title = line.split("\x1f", 1)
        else:
            sha, title = line, ""
        commits.append((sha.strip(), title.strip()))
    if from_commit:
        from_idx = None
        for idx, (sha, _title) in enumerate(commits):
            if sha == from_commit:
                from_idx = idx
                break
        if from_idx is None:
            raise RuntimeError(
                f"from_commit {from_commit} is not in target history {target_commit}"
            )
        commits = commits[from_idx:]
    return commits


def _resolve_parent_commit(repo_path: str, commit_sha: str) -> str | None:
    out = _run_git(
        repo_path,
        ["rev-list", "--parents", "-n", "1", commit_sha],
        check=False,
    )
    parts = out.split()
    if len(parts) >= 2:
        return parts[1]
    return None


def _resolve_first_parent_sha(
    repo_path: str,
    commits: list[tuple[str, str]],
    resolved_from: str | None,
) -> str | None:
    if not resolved_from or not commits:
        return None
    return _resolve_parent_commit(repo_path, commits[0][0])


def _resolve_commit_version(repo_path: str, commit_sha: str, commit_title: str) -> tuple[str, int | None]:
    version_label = normalize_version_label(commit_title or _resolve_commit_title(repo_path, commit_sha)) or commit_sha
    return version_label, _resolve_version_id(version_label)


def _latest_commit_meta_title(commits: list[tuple[str, str]]) -> str | None:
    if not commits:
        return None
    return normalize_version_label(commits[-1][1]) or normalize_version_label(commits[-1][0]) or commits[-1][0]


def _build_guarded_created_updated_sql(table_name: str, key_predicate_sql: str) -> str:
    return (
        f"UPDATE {table_name} SET "
        "created_version_id=CASE "
        "WHEN created_version_id IS NULL OR created_version_id > ? THEN ? "
        "ELSE created_version_id END, "
        "updated_version_id=? "
        f"WHERE {key_predicate_sql} "
        "AND (updated_version_id IS NULL OR updated_version_id <= ?)"
    )


def _build_guarded_updated_only_sql(table_name: str, key_predicate_sql: str) -> str:
    return (
        f"UPDATE {table_name} SET updated_version_id=? "
        f"WHERE {key_predicate_sql} "
        "AND (updated_version_id IS NULL OR updated_version_id <= ?)"
    )


def _initial_entries(repo_path: str, commit: str, include_paths: list[str] | None = None) -> list[dict]:
    pathspec = include_paths or RELEVANT_PATHS
    out = _run_git(
        repo_path,
        ["ls-tree", "-r", "--name-only", commit, "--"] + pathspec,
        check=True,
    )
    entries = []
    for line in out.splitlines():
        if line:
            entries.append({"action": "A", "old_path": None, "new_path": line})
    return entries


def _diff_entries(
    repo_path: str,
    parent: str,
    commit: str,
    include_paths: list[str] | None = None,
) -> list[dict]:
    pathspec = include_paths or RELEVANT_PATHS
    out = _run_git(
        repo_path,
        ["diff", "--name-status", "--find-renames", parent, commit, "--"] + pathspec,
        check=True,
    )
    entries = []
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        action = parts[0]
        if action.startswith("R") and len(parts) >= 3:
            entries.append({"action": action, "old_path": parts[1], "new_path": parts[2]})
        elif action == "D":
            entries.append({"action": action, "old_path": parts[1], "new_path": None})
        else:
            entries.append({"action": action, "old_path": None, "new_path": parts[1]})
    return entries


def _git_show_text(repo_path: str, commit: str, rel_path: str) -> str | None:
    proc = subprocess.run(
        ["git", "-C", repo_path, "show", f"{commit}:{rel_path}"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if proc.returncode != 0:
        return None
    return proc.stdout


def _git_show_json(repo_path: str, commit: str, rel_path: str):
    raw = _git_show_text(repo_path, commit, rel_path)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def _get_textmap_lang_id_map() -> dict[str, int]:
    cur = conn.cursor()
    rows = cur.execute("SELECT id, codeName FROM langCode").fetchall()
    cur.close()
    mapping: dict[str, int] = {}
    fallback_hits: list[str] = []
    for lang_id, code_name in rows:
        if not code_name:
            continue
        parsed = parse_textmap_file_name(str(code_name).strip())
        if parsed is None:
            continue
        base_name, _split_part = parsed
        try:
            mapping[base_name] = int(lang_id)
        except Exception:
            continue

    # Fallback for old/malformed langCode rows: keep canonical TextMap->lang id map usable.
    for lang_code, lang_id in LANG_CODE_MAP.items():
        base_name = f"TextMap{lang_code}.json"
        if base_name not in mapping:
            mapping[base_name] = int(lang_id)
            fallback_hits.append(base_name)

    if fallback_hits:
        print(
            "[WARN] history backfill: langCode mapping incomplete, "
            f"using fallback ids for {len(fallback_hits)} entries."
        )
    return mapping


def _quest_text_signature(row):
    if row is None:
        return None
    # Quest versioning should only follow title text changes, not chapter remapping.
    return row[0], row[1]


def _backfill_quest_version_by_commit_entry(
    cursor,
    *,
    repo_path: str,
    commit_sha: str,
    parent_sha: str | None,
    entry: dict,
    version_id: int,
    target_quest_ids: set[int] | None = None,
) -> int:
    action = entry["action"]
    if action == "D":
        return 0
    old_path = entry.get("old_path")
    new_path = entry.get("new_path")
    rel_path = (new_path or old_path or "").replace("\\", "/")
    if not (rel_path.startswith("BinOutput/Quest/") and rel_path.endswith(".json")):
        return 0

    if target_quest_ids is not None:
        file_name = os.path.basename(rel_path)
        stem, _ext = os.path.splitext(file_name)
        if stem.isdigit():
            if int(stem) not in target_quest_ids:
                return 0

    new_obj = _git_show_json(repo_path, commit_sha, rel_path)
    if not isinstance(new_obj, dict):
        return 0
    new_row = _extract_quest_row(new_obj)
    if new_row is None:
        return 0
    if target_quest_ids is not None:
        try:
            if int(new_row[0]) not in target_quest_ids:
                return 0
        except Exception:
            return 0

    old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
    old_obj = _git_show_json(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
    old_row = _extract_quest_row(old_obj) if isinstance(old_obj, dict) else None
    if _quest_text_signature(old_row) == _quest_text_signature(new_row):
        return 0

    # Commit fallback write:
    # only write for quests whose created_version_id is still missing.
    cursor.execute(
        """
        UPDATE quest
        SET created_version_id=?,
            updated_version_id=CASE
                WHEN updated_version_id IS NULL OR updated_version_id < ? THEN ?
                ELSE updated_version_id
            END
        WHERE questId=?
          AND created_version_id IS NULL
        """,
        (version_id, version_id, version_id, new_row[0]),
    )
    return 1 if cursor.rowcount > 0 else 0


def _update_quest_versions_by_changed_hashes(
    cursor,
    version_id: int,
    changed_hashes: set,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    show_progress: bool = False,
    progress_position: int = 2,
):
    stats = apply_quest_version_delta_from_textmap(
        cursor,
        version_id=version_id,
        changed_hashes=changed_hashes,
        batch_size=batch_size,
        show_progress=show_progress,
        progress_position=progress_position,
    )
    return int(stats.get("quest_updated_by_textmap", 0))


def _update_quest_versions_by_existing_textmap(cursor, version_id: int):
    stats = apply_quest_version_delta_from_textmap(
        cursor,
        version_id=version_id,
        changed_hashes=None,
        show_progress=False,
    )
    return int(stats.get("quest_updated_by_textmap", 0))


def _update_quest_versions_by_existing_textmap_with_progress(
    cursor,
    version_id: int,
    *,
    fetch_size: int = 50000,
    progress_position: int = 1,
    show_progress: bool = True,
) -> tuple[int, int]:
    stats = apply_quest_version_delta_from_textmap(
        cursor,
        version_id=version_id,
        changed_hashes=None,
        fetch_size=fetch_size,
        show_progress=show_progress,
        progress_position=progress_position,
    )
    return int(stats.get("looked_up_hashes", 0)), int(stats.get("quest_updated_by_textmap", 0))


def apply_quest_version_delta_from_textmap(
    cursor,
    *,
    version_id: int,
    changed_hashes: set[int] | None = None,
    version_label: str | None = None,
    quest_scope: set[int] | list[int] | tuple[int, ...] | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fetch_size: int = 50000,
    show_progress: bool = False,
    progress_position: int = 1,
) -> dict[str, int]:
    stats = {
        "looked_up_hashes": 0,
        "quest_updated_by_textmap": 0,
        "quest_created_backfilled": 0,
        "quest_updated_backfilled": 0,
    }
    if version_id is None:
        return stats

    cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _changed_textmap_hash(hash INTEGER PRIMARY KEY)")
    cursor.execute("DELETE FROM _changed_textmap_hash")

    if quest_scope is not None:
        cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _target_quest_id(questId INTEGER PRIMARY KEY)")
        cursor.execute("DELETE FROM _target_quest_id")
        normalized_qids: list[tuple[int]] = []
        seen = set()
        for raw in quest_scope:
            try:
                qid = int(raw)
            except Exception:
                continue
            if qid in seen:
                continue
            seen.add(qid)
            normalized_qids.append((qid,))
        if not normalized_qids:
            return stats
        executemany_batched(
            cursor,
            "INSERT OR IGNORE INTO _target_quest_id(questId) VALUES (?)",
            (row for row in normalized_qids),
            batch_size=batch_size,
        )
        quest_scope_filter = " AND questId IN (SELECT questId FROM _target_quest_id)"
    else:
        quest_scope_filter = ""

    if changed_hashes is not None:
        normalized_hashes = []
        seen_hash = set()
        for raw in changed_hashes:
            try:
                h = int(raw)
            except Exception:
                continue
            if h in seen_hash:
                continue
            seen_hash.add(h)
            normalized_hashes.append((h,))
        stats["looked_up_hashes"] = len(normalized_hashes)
        if not normalized_hashes:
            return stats
        if show_progress:
            with tqdm(
                total=len(normalized_hashes),
                desc="textMap hash sync",
                unit="hash",
                leave=False,
                position=progress_position,
                dynamic_ncols=True,
                file=sys.stdout,
            ) as hash_pbar:
                pending = []
                for row in normalized_hashes:
                    pending.append(row)
                    if len(pending) >= batch_size:
                        cursor.executemany(
                            "INSERT OR IGNORE INTO _changed_textmap_hash(hash) VALUES (?)",
                            pending,
                        )
                        hash_pbar.update(len(pending))
                        pending = []
                if pending:
                    cursor.executemany(
                        "INSERT OR IGNORE INTO _changed_textmap_hash(hash) VALUES (?)",
                        pending,
                    )
                    hash_pbar.update(len(pending))
        else:
            executemany_batched(
                cursor,
                "INSERT OR IGNORE INTO _changed_textmap_hash(hash) VALUES (?)",
                (row for row in normalized_hashes),
                batch_size=batch_size,
            )
    else:
        total_hashes_row = cursor.execute(
            "SELECT COUNT(DISTINCT hash) FROM textMap WHERE updated_version_id=?",
            (version_id,),
        ).fetchone()
        total_hashes = int(total_hashes_row[0] or 0) if total_hashes_row else 0
        stats["looked_up_hashes"] = total_hashes
        if total_hashes <= 0:
            return stats
        select_cur = cursor.execute(
            "SELECT DISTINCT hash FROM textMap WHERE updated_version_id=?",
            (version_id,),
        )
        if show_progress:
            with tqdm(
                total=total_hashes,
                desc="textMap DB lookup",
                unit="hash",
                leave=False,
                position=progress_position,
                dynamic_ncols=True,
                file=sys.stdout,
            ) as hash_pbar:
                while True:
                    rows = select_cur.fetchmany(fetch_size)
                    if not rows:
                        break
                    cursor.executemany(
                        "INSERT OR IGNORE INTO _changed_textmap_hash(hash) VALUES (?)",
                        rows,
                    )
                    hash_pbar.update(len(rows))
        else:
            while True:
                rows = select_cur.fetchmany(fetch_size)
                if not rows:
                    break
                cursor.executemany(
                    "INSERT OR IGNORE INTO _changed_textmap_hash(hash) VALUES (?)",
                    rows,
                )

    has_quest_hash_map_rows = False
    if _table_exists("quest_hash_map"):
        row = cursor.execute("SELECT 1 FROM quest_hash_map LIMIT 1").fetchone()
        has_quest_hash_map_rows = row is not None

    if has_quest_hash_map_rows:
        cursor.execute(
            f"""
            UPDATE quest
            SET updated_version_id=?
            WHERE COALESCE(updated_version_id, -1) <> ?
              AND (updated_version_id IS NULL OR updated_version_id <= ?)
              {quest_scope_filter}
              AND questId IN (
                SELECT DISTINCT qhm.questId
                FROM quest_hash_map qhm
                JOIN _changed_textmap_hash c ON c.hash = qhm.hash
              )
            """,
            (version_id, version_id, version_id),
        )
    else:
        cursor.execute(
            f"""
            UPDATE quest
            SET updated_version_id=?
            WHERE COALESCE(updated_version_id, -1) <> ?
              AND (updated_version_id IS NULL OR updated_version_id <= ?)
              {quest_scope_filter}
              AND (
                titleTextMapHash IN (SELECT hash FROM _changed_textmap_hash)
                OR questId IN (
                    SELECT DISTINCT qt.questId
                    FROM questTalk qt
                    JOIN dialogue d ON d.talkId = qt.talkId
                    JOIN _changed_textmap_hash c ON c.hash = d.textHash
                )
              )
            """,
            (version_id, version_id, version_id),
        )
    stats["quest_updated_by_textmap"] = int(cursor.rowcount or 0)

    if version_label:
        backfill_result = _backfill_quest_created_version_from_textmap(
            cursor,
            quest_updated_version=version_label,
            quest_ids=quest_scope,
            overwrite_existing=False,
            with_stats=True,
        )
        if isinstance(backfill_result, dict):
            stats["quest_created_backfilled"] = int(backfill_result.get("created_rows", 0))
            stats["quest_updated_backfilled"] = int(backfill_result.get("updated_rows", 0))
        elif isinstance(backfill_result, tuple) and len(backfill_result) >= 2:
            stats["quest_created_backfilled"] = int(backfill_result[0])
            stats["quest_updated_backfilled"] = int(backfill_result[1])
        else:
            stats["quest_created_backfilled"] = 0
            stats["quest_updated_backfilled"] = 0
    return stats


def _has_any_version_data(table_name: str) -> bool:
    cols = _table_columns(table_name)
    predicates: list[str] = []
    if "created_version_id" in cols:
        predicates.append("created_version_id IS NOT NULL")
    if "updated_version_id" in cols:
        predicates.append("updated_version_id IS NOT NULL")
    if not predicates:
        return False
    cur = conn.cursor()
    try:
        row = cur.execute(
            f"SELECT 1 FROM {table_name} WHERE {' OR '.join(predicates)} LIMIT 1"
        ).fetchone()
        return row is not None
    finally:
        cur.close()


def _prune_unseen_rows_by_version(cursor, table_name: str) -> int:
    cols = _table_columns(table_name)
    predicates: list[str] = []
    if "created_version_id" in cols:
        predicates.append("created_version_id IS NULL")
    if "updated_version_id" in cols:
        predicates.append("updated_version_id IS NULL")
    if not predicates:
        return 0
    cursor.execute(
        f"DELETE FROM {table_name} WHERE {' AND '.join(predicates)}"
    )
    return cursor.rowcount


def _extract_quest_backfill_stats(backfill_result) -> tuple[int, int]:
    if isinstance(backfill_result, dict):
        return (
            int(backfill_result.get("created_rows", 0)),
            int(backfill_result.get("updated_rows", 0)),
        )
    if isinstance(backfill_result, tuple) and len(backfill_result) >= 2:
        return int(backfill_result[0]), int(backfill_result[1])
    return 0, 0


def _backfill_quest_phase1_with_progress(
    cursor,
    *,
    chunk_size: int = 2000,
) -> tuple[int, int]:
    total_row = cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
    total_quests = int(total_row[0] or 0) if total_row else 0
    if total_quests <= 0:
        return 0, 0

    created_total = 0
    updated_total = 0
    quest_cursor = cursor.execute("SELECT questId FROM quest ORDER BY questId")
    with tqdm(
        total=total_quests,
        desc="Quest history phase-1 (local textMap DB)",
        unit="quest",
        leave=False,
        position=0,
        dynamic_ncols=True,
        file=sys.stdout,
    ) as phase1_pbar:
        while True:
            rows = quest_cursor.fetchmany(max(1, int(chunk_size)))
            if not rows:
                break
            quest_ids = [int(row[0]) for row in rows]
            backfill_result = _backfill_quest_created_version_from_textmap(
                cursor,
                quest_ids=quest_ids,
                overwrite_existing=False,
                overwrite_updated_existing=True,
                with_stats=True,
            )
            created_rows, updated_rows = _extract_quest_backfill_stats(backfill_result)
            created_total += created_rows
            updated_total += updated_rows
            phase1_pbar.update(len(quest_ids))
            phase1_pbar.set_postfix_str(
                f"q_tm_create={created_total} q_tm_upfill={updated_total}"
            )
    return created_total, updated_total


def _prepare_resume_for_commits(
    *,
    resume_target_key: str,
    resume_done_key: str,
    resolved_target: str,
    commits: list[tuple[str, str]],
    force: bool,
    label: str,
) -> int:
    total_commits = len(commits)
    commit_index = {sha: idx for idx, (sha, _title) in enumerate(commits)}

    if force:
        set_meta(resume_target_key, "")
        set_meta(resume_done_key, "")

    start_idx = 0
    resume_target = get_meta(resume_target_key, "")
    resume_done = get_meta(resume_done_key, "")
    if not force and resume_target == resolved_target and resume_done:
        done_idx = commit_index.get(resume_done)
        if done_idx is not None:
            start_idx = done_idx + 1
            if start_idx < total_commits:
                print(
                    f"{label} resume: continue from commit {start_idx + 1}/{total_commits} "
                    f"(last done: {resume_done[:8]})"
                )
            else:
                print(f"{label} resume: all commits already processed; finalizing metadata only.")
        else:
            print(f"{label} resume checkpoint not found in current history; restart from scratch.")
            start_idx = 0
    return start_idx


def reset_history_version_marks(*, scope: str = "all"):
    """
    Explicitly reset version marks to NULL for selected history-backfill scope.
    This is intentionally separate from --force so normal backfill runs do not clear data.
    """
    ensure_version_schema()
    normalized_scope = (scope or "all").strip().lower()
    scope_map: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
        "all": (
            ("textMap", "readable", "subtitle", "quest"),
            (
                "db_history_versions_commit",
                "db_history_versions_commit_title",
                "db_history_versions_commit_resume_target",
                "db_history_versions_commit_resume_done",
                "db_history_versions_commit_textmap",
                "db_history_versions_commit_title_textmap",
                "db_history_versions_commit_textmap_resume_target",
                "db_history_versions_commit_textmap_resume_done",
                "db_history_versions_commit_readable",
                "db_history_versions_commit_title_readable",
                "db_history_versions_commit_readable_resume_target",
                "db_history_versions_commit_readable_resume_done",
                "db_history_versions_commit_subtitle",
                "db_history_versions_commit_title_subtitle",
                "db_history_versions_commit_subtitle_resume_target",
                "db_history_versions_commit_subtitle_resume_done",
                "db_history_versions_commit_quest",
                "db_history_versions_commit_title_quest",
                "db_history_versions_commit_quest_resume_target",
                "db_history_versions_commit_quest_resume_done",
            ),
        ),
        "textmap": (
            ("textMap",),
            (
                "db_history_versions_commit_textmap",
                "db_history_versions_commit_title_textmap",
                "db_history_versions_commit_textmap_resume_target",
                "db_history_versions_commit_textmap_resume_done",
            ),
        ),
        "readable": (
            ("readable",),
            (
                "db_history_versions_commit_readable",
                "db_history_versions_commit_title_readable",
                "db_history_versions_commit_readable_resume_target",
                "db_history_versions_commit_readable_resume_done",
            ),
        ),
        "subtitle": (
            ("subtitle",),
            (
                "db_history_versions_commit_subtitle",
                "db_history_versions_commit_title_subtitle",
                "db_history_versions_commit_subtitle_resume_target",
                "db_history_versions_commit_subtitle_resume_done",
            ),
        ),
        "quest": (
            ("quest",),
            (
                "db_history_versions_commit_quest",
                "db_history_versions_commit_title_quest",
                "db_history_versions_commit_quest_resume_target",
                "db_history_versions_commit_quest_resume_done",
            ),
        ),
    }
    if normalized_scope not in scope_map:
        raise ValueError(f"Unsupported reset scope: {scope}")

    table_names, meta_keys = scope_map[normalized_scope]
    cursor = conn.cursor()
    try:
        for table_name in table_names:
            cursor.execute(f"UPDATE {table_name} SET created_version_id=NULL, updated_version_id=NULL")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        cursor.close()

    for key in meta_keys:
        set_meta(key, "")
    rebuild_version_catalog(list(table_names))
    print(
        "History version reset finished: "
        f"scope={normalized_scope}, tables={','.join(table_names)}"
    )


def backfill_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    prune_missing: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    ensure_version_schema()
    repo_path = DATA_PATH
    resolved_target = _resolve_commit(repo_path, target_commit)
    resolved_from = _resolve_commit(repo_path, from_commit) if from_commit else None
    resume_scope = f"{resolved_from or ''}..{resolved_target}"
    resume_target_key = "db_history_versions_commit_resume_target"
    resume_done_key = "db_history_versions_commit_resume_done"
    if not force and resolved_from is None and get_meta("db_history_versions_commit") == resolved_target:
        version_tables = ("textMap", "readable", "subtitle", "quest")
        if all(_has_any_version_data(t) for t in version_tables):
            print(f"History backfill already done for {resolved_target}, skipping.")
            return
        print(
            f"History backfill meta indicates {resolved_target}, "
            "but version columns are empty/incomplete; rerunning."
        )

    textmap_lang_map = _get_textmap_lang_id_map()
    sql_textmap = _build_guarded_created_updated_sql("textMap", "lang=? AND hash=?")
    sql_readable = _build_guarded_created_updated_sql("readable", "fileName=? AND lang=?")
    sql_subtitle = _build_guarded_created_updated_sql("subtitle", "subtitleKey=?")

    commits = _list_commits(repo_path, resolved_target, from_commit=resolved_from)
    if not commits:
        print("History backfill: no commits to process.")
        return
    first_parent_sha = _resolve_first_parent_sha(repo_path, commits, resolved_from)
    total_commits = len(commits)
    start_idx = _prepare_resume_for_commits(
        resume_target_key=resume_target_key,
        resume_done_key=resume_done_key,
        resolved_target=resume_scope,
        commits=commits,
        force=force,
        label="History backfill",
    )
    if start_idx == 0:
        if resolved_from:
            print(
                "History backfill start: "
                f"{total_commits} commits (from: {resolved_from[:8]}, target: {resolved_target})"
            )
        else:
            print(f"History backfill start: {total_commits} commits (target: {resolved_target})")
    else:
        print(
            "History backfill continue: "
            f"{total_commits - start_idx} remaining / {total_commits} total commits "
            f"(target: {resolved_target})"
        )

    cursor = conn.cursor()
    refreshed_qhm = _refresh_all_quest_hash_map(cursor, batch_size=batch_size)
    unresolved_created_scope = _unresolved_created_quest_ids(cursor)
    print(f"Quest hash map refresh before history replay: quests={refreshed_qhm}")
    commit_pbar = tqdm(
        total=total_commits,
        initial=start_idx,
        desc="History backfill (textMap-first)",
        unit="commit",
        leave=False,
        position=0,
        dynamic_ncols=True,
        file=sys.stdout,
    )
    try:
        for idx in range(start_idx, total_commits):
            commit_sha, commit_title = commits[idx]
            parent_sha = commits[idx - 1][0] if idx > 0 else first_parent_sha
            version_label, version_id = _resolve_commit_version(repo_path, commit_sha, commit_title)
            if version_id is None:
                continue
            entries = _initial_entries(repo_path, commit_sha) if parent_sha is None else _diff_entries(repo_path, parent_sha, commit_sha)

            textmap_entries = []
            other_entries = []
            for entry in entries:
                action = entry["action"]
                old_path = entry.get("old_path")
                new_path = entry.get("new_path")
                rel_path = (new_path or old_path or "").replace("\\", "/")
                if rel_path.startswith("TextMap/") and rel_path.endswith(".json") and action != "D":
                    textmap_entries.append(entry)
                else:
                    other_entries.append(entry)
            commit_pbar.set_postfix_str(
                f"{idx + 1}/{total_commits} {commit_sha[:8]} "
                f"tm_files={len(textmap_entries)} other={len(other_entries)}"
            )

            with tqdm(
                total=len(entries),
                desc=f"{commit_sha[:8]} files (pass1 tm, pass2 other)",
                unit="file",
                leave=False,
                position=1,
                dynamic_ncols=True,
                file=sys.stdout,
            ) as entry_pbar:
                changed_text_hashes = set()
                quest_updated_by_textmap = 0
                quest_created_by_textmap = 0
                quest_updated_backfilled_by_textmap = 0
                quest_created_by_commit = 0
                unresolved_count = 0

                # Pass 1: textMap first.
                entry_pbar.set_postfix_str("phase=1/2 textMap")
                for entry in textmap_entries:
                    try:
                        action = entry["action"]
                        old_path = entry.get("old_path")
                        new_path = entry.get("new_path")
                        rel_path = (new_path or old_path or "").replace("\\", "/")
                        if not rel_path:
                            continue

                        file_name = rel_path.split("/", 1)[1]
                        parsed = parse_textmap_file_name(file_name)
                        if parsed is None:
                            continue
                        base_name, _split_part = parsed
                        lang_id = textmap_lang_map.get(base_name)
                        if lang_id is None:
                            continue

                        new_obj = _git_show_json(repo_path, commit_sha, rel_path)
                        if not isinstance(new_obj, dict):
                            continue

                        old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
                        old_obj = _git_show_json(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
                        if not isinstance(old_obj, dict):
                            old_obj = {}

                        changed_rows = []
                        total_items = len(new_obj)
                        for i, (raw_hash, content) in enumerate(new_obj.items(), start=1):
                            if old_obj.get(raw_hash, None) == content:
                                continue
                            parsed_hash = _to_hash_value(raw_hash)
                            changed_rows.append((version_id, version_id, version_id, lang_id, parsed_hash, version_id))
                            changed_text_hashes.add(parsed_hash)
                            if i % 50000 == 0:
                                entry_pbar.set_postfix_str(f"textmap {base_name} {i}/{total_items}")
                                entry_pbar.refresh()
                        if changed_rows:
                            executemany_batched(cursor, sql_textmap, changed_rows, batch_size=batch_size)
                    finally:
                        entry_pbar.update(1)

                if changed_text_hashes:
                    q_delta_stats = apply_quest_version_delta_from_textmap(
                        cursor,
                        version_id=version_id,
                        changed_hashes=changed_text_hashes,
                        version_label=version_label,
                        batch_size=batch_size,
                        show_progress=True,
                        progress_position=2,
                    )
                    quest_updated_by_textmap = int(q_delta_stats.get("quest_updated_by_textmap", 0))
                    quest_created_by_textmap = int(q_delta_stats.get("quest_created_backfilled", 0))
                    quest_updated_backfilled_by_textmap = int(q_delta_stats.get("quest_updated_backfilled", 0))

                # Pass 2: other datasets and quest hash remaps.
                entry_pbar.set_postfix_str("phase=2/2 other")
                for entry in other_entries:
                    try:
                        action = entry["action"]
                        old_path = entry.get("old_path")
                        new_path = entry.get("new_path")
                        rel_path = (new_path or old_path or "").replace("\\", "/")
                        if not rel_path:
                            continue

                        if rel_path == "ExcelBinOutput/DocumentExcelConfigData.json":
                            # Do not blanket-update readable versions by config file changes.
                            # It would incorrectly stamp all current rows with the same early version.
                            continue
                        if rel_path == "ExcelBinOutput/LocalizationExcelConfigData.json":
                            # Keep subtitle/readable versioning driven by per-entry file changes only.
                            continue

                        if rel_path.startswith("Readable/") and action != "D":
                            parts = rel_path.split("/", 2)
                            if len(parts) < 3:
                                continue
                            lang = parts[1]
                            file_name = os.path.basename(parts[2])

                            new_text = _git_show_text(repo_path, commit_sha, rel_path)
                            old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
                            old_text = _git_show_text(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
                            if new_text is None or not _readable_text_changed(old_text, new_text):
                                continue
                            cursor.execute(sql_readable, (version_id, version_id, version_id, file_name, lang, version_id))
                            continue

                        if rel_path.startswith("Subtitle/") and rel_path.endswith(".srt") and action != "D":
                            parts = rel_path.split("/", 2)
                            if len(parts) < 3:
                                continue
                            lang_name = parts[1]
                            lang_id = LANG_CODE_MAP.get(lang_name)
                            if lang_id is None:
                                continue

                            rel_under_lang = parts[2]
                            new_text = _git_show_text(repo_path, commit_sha, rel_path)
                            if new_text is None:
                                continue
                            new_rows = _parse_srt_rows(new_text, lang_id, rel_under_lang)

                            old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
                            old_text = _git_show_text(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
                            old_rows = _parse_srt_rows(old_text, lang_id, rel_under_lang) if old_text else {}

                            changed_keys = _subtitle_text_changed_keys(old_rows, new_rows)
                            if changed_keys:
                                executemany_batched(
                                    cursor,
                                    sql_subtitle,
                                    (
                                        (version_id, version_id, version_id, key, version_id)
                                        for key in changed_keys
                                    ),
                                    batch_size=batch_size,
                                )
                            continue

                        if (
                            rel_path.startswith("BinOutput/Quest/")
                            and rel_path.endswith(".json")
                            and action != "D"
                            and unresolved_created_scope
                        ):
                            quest_created_by_commit += _backfill_quest_version_by_commit_entry(
                                cursor,
                                repo_path=repo_path,
                                commit_sha=commit_sha,
                                parent_sha=parent_sha,
                                entry=entry,
                                version_id=version_id,
                                target_quest_ids=unresolved_created_scope,
                            )
                    finally:
                        entry_pbar.update(1)
            if unresolved_created_scope and (
                quest_created_by_commit > 0 or quest_created_by_textmap > 0
            ):
                unresolved_created_scope = _unresolved_created_quest_ids(cursor)
            unresolved_row = cursor.execute(
                "SELECT COUNT(*) FROM quest WHERE created_version_id IS NULL OR updated_version_id IS NULL"
            ).fetchone()
            unresolved_count = int(unresolved_row[0] or 0) if unresolved_row else 0
            commit_pbar.set_postfix_str(
                f"{commit_sha[:8]} "
                f"tm_hash={len(changed_text_hashes)} "
                f"q_tm_upd={quest_updated_by_textmap} "
                f"q_tm_create={quest_created_by_textmap} "
                f"q_tm_upfill={quest_updated_backfilled_by_textmap} "
                f"q_commit_create={quest_created_by_commit} "
                f"unresolved={unresolved_count}"
            )
            conn.commit()
            set_meta(resume_target_key, resume_scope)
            set_meta(resume_done_key, commit_sha)
            commit_pbar.update(1)
        if prune_missing:
            _prune_unseen_rows_by_version(cursor, "textMap")
            _prune_unseen_rows_by_version(cursor, "readable")
            _prune_unseen_rows_by_version(cursor, "subtitle")
            _prune_unseen_rows_by_version(cursor, "quest")
            cursor.execute("DELETE FROM questTalk WHERE questId NOT IN (SELECT questId FROM quest)")
            try:
                cursor.execute("DELETE FROM quest_text_signature WHERE questId NOT IN (SELECT questId FROM quest)")
            except sqlite3.OperationalError:
                pass
        conn.commit()
    except BaseException:
        conn.rollback()
        print(
            "History backfill interrupted; checkpoint saved, rerun to continue.",
            file=sys.stderr,
        )
        raise
    finally:
        cursor.close()
        commit_pbar.close()

    if resolved_from is None:
        set_meta("db_history_versions_commit", resolved_target)
        if commits:
            set_meta(
                "db_history_versions_commit_title",
                _latest_commit_meta_title(commits) or commits[-1][0],
            )
    set_meta(resume_target_key, "")
    set_meta(resume_done_key, "")
    rebuild_version_catalog(["textMap", "quest", "subtitle", "readable"])
    print(f"History backfill finished at commit {resolved_target}")


def backfill_textmap_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    prune_missing: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    ensure_version_schema()
    repo_path = DATA_PATH
    resolved_target = _resolve_commit(repo_path, target_commit)
    resolved_from = _resolve_commit(repo_path, from_commit) if from_commit else None
    resume_scope = f"{resolved_from or ''}..{resolved_target}"
    meta_commit_key = "db_history_versions_commit_textmap"
    meta_title_key = "db_history_versions_commit_title_textmap"
    resume_target_key = "db_history_versions_commit_textmap_resume_target"
    resume_done_key = "db_history_versions_commit_textmap_resume_done"
    if not force and resolved_from is None and get_meta(meta_commit_key) == resolved_target:
        if _has_any_version_data("textMap"):
            print(f"TextMap history backfill already done for {resolved_target}, skipping.")
            return
        print(
            f"TextMap history backfill meta indicates {resolved_target}, "
            "but textMap versions are empty/incomplete; rerunning."
        )

    textmap_lang_map = _get_textmap_lang_id_map()
    sql_textmap = _build_guarded_created_updated_sql("textMap", "lang=? AND hash=?")

    commits = _list_commits(repo_path, resolved_target, from_commit=resolved_from)
    if not commits:
        print("TextMap history backfill: no commits to process.")
        return
    first_parent_sha = _resolve_first_parent_sha(repo_path, commits, resolved_from)
    total_commits = len(commits)
    start_idx = _prepare_resume_for_commits(
        resume_target_key=resume_target_key,
        resume_done_key=resume_done_key,
        resolved_target=resume_scope,
        commits=commits,
        force=force,
        label="TextMap history backfill",
    )
    if start_idx == 0:
        if resolved_from:
            print(
                "TextMap history backfill start: "
                f"{total_commits} commits (from: {resolved_from[:8]}, target: {resolved_target})"
            )
        else:
            print(f"TextMap history backfill start: {total_commits} commits (target: {resolved_target})")
    else:
        print(
            "TextMap history backfill continue: "
            f"{total_commits - start_idx} remaining / {total_commits} total commits "
            f"(target: {resolved_target})"
        )

    cursor = conn.cursor()
    commit_pbar = tqdm(
        total=total_commits,
        initial=start_idx,
        desc="TextMap history backfill",
        unit="commit",
        leave=False,
        position=0,
        dynamic_ncols=True,
        file=sys.stdout,
    )
    try:
        for idx in range(start_idx, total_commits):
            commit_sha, commit_title = commits[idx]
            parent_sha = commits[idx - 1][0] if idx > 0 else first_parent_sha
            version_label, version_id = _resolve_commit_version(repo_path, commit_sha, commit_title)
            if version_id is None:
                continue
            entries = (
                _initial_entries(repo_path, commit_sha, include_paths=TEXTMAP_ONLY_PATHS)
                if parent_sha is None
                else _diff_entries(repo_path, parent_sha, commit_sha, include_paths=TEXTMAP_ONLY_PATHS)
            )
            commit_pbar.set_postfix_str(f"{idx + 1}/{total_commits} {commit_sha[:8]} entries={len(entries)}")

            with tqdm(
                total=len(entries),
                desc=f"{commit_sha[:8]} textmap files",
                unit="file",
                leave=False,
                position=1,
                dynamic_ncols=True,
                file=sys.stdout,
            ) as entry_pbar:
                for entry in entries:
                    try:
                        action = entry["action"]
                        if action == "D":
                            continue
                        old_path = entry.get("old_path")
                        new_path = entry.get("new_path")
                        rel_path = (new_path or old_path or "").replace("\\", "/")
                        if not rel_path:
                            continue
                        if not (rel_path.startswith("TextMap/") and rel_path.endswith(".json")):
                            continue

                        file_name = rel_path.split("/", 1)[1]
                        parsed = parse_textmap_file_name(file_name)
                        if parsed is None:
                            continue
                        base_name, _split_part = parsed
                        lang_id = textmap_lang_map.get(base_name)
                        if lang_id is None:
                            continue

                        new_obj = _git_show_json(repo_path, commit_sha, rel_path)
                        if not isinstance(new_obj, dict):
                            continue

                        old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
                        old_obj = _git_show_json(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
                        if not isinstance(old_obj, dict):
                            old_obj = {}

                        changed_rows = []
                        total_items = len(new_obj)
                        for i, (raw_hash, content) in enumerate(new_obj.items(), start=1):
                            old_content = old_obj.get(raw_hash, None)
                            if old_content == content:
                                continue
                            changed_rows.append((version_id, version_id, version_id, lang_id, _to_hash_value(raw_hash), version_id))
                            if i % 50000 == 0:
                                entry_pbar.set_postfix_str(f"textmap {base_name} {i}/{total_items}")
                                entry_pbar.refresh()
                        if changed_rows:
                            executemany_batched(cursor, sql_textmap, changed_rows, batch_size=batch_size)
                    finally:
                        entry_pbar.update(1)

            conn.commit()
            set_meta(resume_target_key, resume_scope)
            set_meta(resume_done_key, commit_sha)
            commit_pbar.update(1)

        if prune_missing:
            _prune_unseen_rows_by_version(cursor, "textMap")
        conn.commit()
    except BaseException:
        conn.rollback()
        print(
            "TextMap history backfill interrupted; checkpoint saved, rerun to continue.",
            file=sys.stderr,
        )
        raise
    finally:
        cursor.close()
        commit_pbar.close()

    if resolved_from is None:
        set_meta(meta_commit_key, resolved_target)
        if commits:
            set_meta(meta_title_key, _latest_commit_meta_title(commits) or commits[-1][0])
    set_meta(resume_target_key, "")
    set_meta(resume_done_key, "")
    rebuild_version_catalog(["textMap"])
    print(f"TextMap history backfill finished at commit {resolved_target}")


def backfill_readable_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    prune_missing: bool = True,
):
    ensure_version_schema()
    repo_path = DATA_PATH
    resolved_target = _resolve_commit(repo_path, target_commit)
    resolved_from = _resolve_commit(repo_path, from_commit) if from_commit else None
    resume_scope = f"{resolved_from or ''}..{resolved_target}"
    meta_commit_key = "db_history_versions_commit_readable"
    meta_title_key = "db_history_versions_commit_title_readable"
    resume_target_key = "db_history_versions_commit_readable_resume_target"
    resume_done_key = "db_history_versions_commit_readable_resume_done"
    if not force and resolved_from is None and get_meta(meta_commit_key) == resolved_target:
        if _has_any_version_data("readable"):
            print(f"Readable history backfill already done for {resolved_target}, skipping.")
            return
        print(
            f"Readable history backfill meta indicates {resolved_target}, "
            "but readable versions are empty/incomplete; rerunning."
        )

    sql_readable = _build_guarded_created_updated_sql("readable", "fileName=? AND lang=?")
    commits = _list_commits(repo_path, resolved_target, from_commit=resolved_from)
    if not commits:
        print("Readable history backfill: no commits to process.")
        return
    first_parent_sha = _resolve_first_parent_sha(repo_path, commits, resolved_from)
    total_commits = len(commits)
    start_idx = _prepare_resume_for_commits(
        resume_target_key=resume_target_key,
        resume_done_key=resume_done_key,
        resolved_target=resume_scope,
        commits=commits,
        force=force,
        label="Readable history backfill",
    )
    if start_idx == 0:
        if resolved_from:
            print(
                "Readable history backfill start: "
                f"{total_commits} commits (from: {resolved_from[:8]}, target: {resolved_target})"
            )
        else:
            print(f"Readable history backfill start: {total_commits} commits (target: {resolved_target})")
    else:
        print(
            "Readable history backfill continue: "
            f"{total_commits - start_idx} remaining / {total_commits} total commits "
            f"(target: {resolved_target})"
        )

    cursor = conn.cursor()
    commit_pbar = tqdm(
        total=total_commits,
        initial=start_idx,
        desc="Readable history backfill",
        unit="commit",
        leave=False,
        position=0,
        dynamic_ncols=True,
        file=sys.stdout,
    )
    try:
        for idx in range(start_idx, total_commits):
            commit_sha, commit_title = commits[idx]
            parent_sha = commits[idx - 1][0] if idx > 0 else first_parent_sha
            version_label, version_id = _resolve_commit_version(repo_path, commit_sha, commit_title)
            if version_id is None:
                continue
            entries = (
                _initial_entries(repo_path, commit_sha, include_paths=READABLE_ONLY_PATHS)
                if parent_sha is None
                else _diff_entries(repo_path, parent_sha, commit_sha, include_paths=READABLE_ONLY_PATHS)
            )
            commit_pbar.set_postfix_str(f"{idx + 1}/{total_commits} {commit_sha[:8]} entries={len(entries)}")

            with tqdm(
                total=len(entries),
                desc=f"{commit_sha[:8]} readable files",
                unit="file",
                leave=False,
                position=1,
                dynamic_ncols=True,
                file=sys.stdout,
            ) as entry_pbar:
                for entry in entries:
                    try:
                        action = entry["action"]
                        if action == "D":
                            continue
                        old_path = entry.get("old_path")
                        new_path = entry.get("new_path")
                        rel_path = (new_path or old_path or "").replace("\\", "/")
                        if not rel_path:
                            continue
                        if not rel_path.startswith("Readable/"):
                            continue
                        parts = rel_path.split("/", 2)
                        if len(parts) < 3:
                            continue
                        lang = parts[1]
                        file_name = os.path.basename(parts[2])

                        new_text = _git_show_text(repo_path, commit_sha, rel_path)
                        if new_text is None:
                            continue
                        old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
                        old_text = _git_show_text(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
                        if not _readable_text_changed(old_text, new_text):
                            continue
                        cursor.execute(sql_readable, (version_id, version_id, version_id, file_name, lang, version_id))
                    finally:
                        entry_pbar.update(1)

            conn.commit()
            set_meta(resume_target_key, resume_scope)
            set_meta(resume_done_key, commit_sha)
            commit_pbar.update(1)
    except BaseException:
        conn.rollback()
        print(
            "Readable history backfill interrupted; checkpoint saved, rerun to continue.",
            file=sys.stderr,
        )
        raise
    finally:
        cursor.close()
        commit_pbar.close()

    if prune_missing:
        prune_cursor = conn.cursor()
        _prune_unseen_rows_by_version(prune_cursor, "readable")
        prune_cursor.close()
        conn.commit()

    if resolved_from is None:
        set_meta(meta_commit_key, resolved_target)
        if commits:
            set_meta(meta_title_key, _latest_commit_meta_title(commits) or commits[-1][0])
    set_meta(resume_target_key, "")
    set_meta(resume_done_key, "")
    rebuild_version_catalog(["readable"])
    print(f"Readable history backfill finished at commit {resolved_target}")


def backfill_subtitle_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    prune_missing: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    ensure_version_schema()
    repo_path = DATA_PATH
    resolved_target = _resolve_commit(repo_path, target_commit)
    resolved_from = _resolve_commit(repo_path, from_commit) if from_commit else None
    resume_scope = f"{resolved_from or ''}..{resolved_target}"
    meta_commit_key = "db_history_versions_commit_subtitle"
    meta_title_key = "db_history_versions_commit_title_subtitle"
    resume_target_key = "db_history_versions_commit_subtitle_resume_target"
    resume_done_key = "db_history_versions_commit_subtitle_resume_done"
    if not force and resolved_from is None and get_meta(meta_commit_key) == resolved_target:
        if _has_any_version_data("subtitle"):
            print(f"Subtitle history backfill already done for {resolved_target}, skipping.")
            return
        print(
            f"Subtitle history backfill meta indicates {resolved_target}, "
            "but subtitle versions are empty/incomplete; rerunning."
        )

    sql_subtitle = _build_guarded_created_updated_sql("subtitle", "subtitleKey=?")
    commits = _list_commits(repo_path, resolved_target, from_commit=resolved_from)
    if not commits:
        print("Subtitle history backfill: no commits to process.")
        return
    first_parent_sha = _resolve_first_parent_sha(repo_path, commits, resolved_from)
    total_commits = len(commits)
    start_idx = _prepare_resume_for_commits(
        resume_target_key=resume_target_key,
        resume_done_key=resume_done_key,
        resolved_target=resume_scope,
        commits=commits,
        force=force,
        label="Subtitle history backfill",
    )
    if start_idx == 0:
        if resolved_from:
            print(
                "Subtitle history backfill start: "
                f"{total_commits} commits (from: {resolved_from[:8]}, target: {resolved_target})"
            )
        else:
            print(f"Subtitle history backfill start: {total_commits} commits (target: {resolved_target})")
    else:
        print(
            "Subtitle history backfill continue: "
            f"{total_commits - start_idx} remaining / {total_commits} total commits "
            f"(target: {resolved_target})"
        )

    cursor = conn.cursor()
    commit_pbar = tqdm(
        total=total_commits,
        initial=start_idx,
        desc="Subtitle history backfill",
        unit="commit",
        leave=False,
        position=0,
        dynamic_ncols=True,
        file=sys.stdout,
    )
    try:
        for idx in range(start_idx, total_commits):
            commit_sha, commit_title = commits[idx]
            parent_sha = commits[idx - 1][0] if idx > 0 else first_parent_sha
            version_label, version_id = _resolve_commit_version(repo_path, commit_sha, commit_title)
            if version_id is None:
                continue
            entries = (
                _initial_entries(repo_path, commit_sha, include_paths=SUBTITLE_ONLY_PATHS)
                if parent_sha is None
                else _diff_entries(repo_path, parent_sha, commit_sha, include_paths=SUBTITLE_ONLY_PATHS)
            )
            commit_pbar.set_postfix_str(f"{idx + 1}/{total_commits} {commit_sha[:8]} entries={len(entries)}")

            with tqdm(
                total=len(entries),
                desc=f"{commit_sha[:8]} subtitle files",
                unit="file",
                leave=False,
                position=1,
                dynamic_ncols=True,
                file=sys.stdout,
            ) as entry_pbar:
                for entry in entries:
                    try:
                        action = entry["action"]
                        if action == "D":
                            continue
                        old_path = entry.get("old_path")
                        new_path = entry.get("new_path")
                        rel_path = (new_path or old_path or "").replace("\\", "/")
                        if not rel_path:
                            continue
                        if not (rel_path.startswith("Subtitle/") and rel_path.endswith(".srt")):
                            continue
                        parts = rel_path.split("/", 2)
                        if len(parts) < 3:
                            continue
                        lang_name = parts[1]
                        lang_id = LANG_CODE_MAP.get(lang_name)
                        if lang_id is None:
                            continue

                        rel_under_lang = parts[2]
                        new_text = _git_show_text(repo_path, commit_sha, rel_path)
                        if new_text is None:
                            continue
                        new_rows = _parse_srt_rows(new_text, lang_id, rel_under_lang)

                        old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
                        old_text = _git_show_text(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
                        old_rows = _parse_srt_rows(old_text, lang_id, rel_under_lang) if old_text else {}

                        changed_keys = _subtitle_text_changed_keys(old_rows, new_rows)
                        if changed_keys:
                            executemany_batched(
                                cursor,
                                sql_subtitle,
                                (
                                    (version_id, version_id, version_id, key, version_id)
                                    for key in changed_keys
                                ),
                                batch_size=batch_size,
                            )
                    finally:
                        entry_pbar.update(1)

            conn.commit()
            set_meta(resume_target_key, resume_scope)
            set_meta(resume_done_key, commit_sha)
            commit_pbar.update(1)
    except BaseException:
        conn.rollback()
        print(
            "Subtitle history backfill interrupted; checkpoint saved, rerun to continue.",
            file=sys.stderr,
        )
        raise
    finally:
        cursor.close()
        commit_pbar.close()

    if prune_missing:
        prune_cursor = conn.cursor()
        _prune_unseen_rows_by_version(prune_cursor, "subtitle")
        prune_cursor.close()
        conn.commit()

    if resolved_from is None:
        set_meta(meta_commit_key, resolved_target)
        if commits:
            set_meta(meta_title_key, _latest_commit_meta_title(commits) or commits[-1][0])
    set_meta(resume_target_key, "")
    set_meta(resume_done_key, "")
    rebuild_version_catalog(["subtitle"])
    print(f"Subtitle history backfill finished at commit {resolved_target}")


def backfill_quest_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    prune_missing: bool = True,
    unresolved_ratio_threshold: float = 0.05,
) -> dict[str, int | str]:
    ensure_version_schema()
    repo_path = DATA_PATH
    resolved_target = _resolve_commit(repo_path, target_commit)
    resolved_from = _resolve_commit(repo_path, from_commit) if from_commit else None
    resume_scope = f"{resolved_from or ''}..{resolved_target}"
    meta_commit_key = "db_history_versions_commit_quest"
    meta_title_key = "db_history_versions_commit_title_quest"
    resume_target_key = "db_history_versions_commit_quest_resume_target"
    resume_done_key = "db_history_versions_commit_quest_resume_done"
    if not force and resolved_from is None and get_meta(meta_commit_key) == resolved_target:
        if _has_any_version_data("quest"):
            print(f"Quest history backfill already done for {resolved_target}, skipping.")
            return {
                "replay_mode": "skip",
                "total_quests": 0,
                "unresolved_quests": 0,
                "phase1_created_backfilled": 0,
                "phase1_updated_backfilled": 0,
                "phase2_commit_created_backfilled": 0,
            }
        print(
            f"Quest history backfill meta indicates {resolved_target}, "
            "but quest versions are empty/incomplete; rerunning."
        )

    commits = _list_commits(repo_path, resolved_target, from_commit=resolved_from)
    if not commits:
        print("Quest history backfill: no commits to process.")
        return {
            "replay_mode": "none",
            "total_quests": 0,
            "unresolved_quests": 0,
            "phase1_created_backfilled": 0,
            "phase1_updated_backfilled": 0,
            "phase2_commit_created_backfilled": 0,
        }
    first_parent_sha = _resolve_first_parent_sha(repo_path, commits, resolved_from)
    total_commits = len(commits)
    start_idx = _prepare_resume_for_commits(
        resume_target_key=resume_target_key,
        resume_done_key=resume_done_key,
        resolved_target=resume_scope,
        commits=commits,
        force=force,
        label="Quest history backfill",
    )

    if start_idx == 0:
        if resolved_from:
            print(
                "Quest history backfill start: "
                f"{total_commits} commits (from: {resolved_from[:8]}, target: {resolved_target})"
            )
        else:
            print(f"Quest history backfill start: {total_commits} commits (target: {resolved_target})")
    else:
        print(
            "Quest history backfill continue: "
            f"{total_commits - start_idx} remaining / {total_commits} total commits "
            f"(target: {resolved_target})"
        )

    cursor = conn.cursor()
    phase2_commit_created_backfilled = 0
    replay_mode = "none"
    prefilled_created_rows = 0
    prefilled_updated_rows = 0
    final_total = 0
    final_unresolved_count = 0
    try:
        refreshed_qhm = _refresh_all_quest_hash_map(cursor)
        print(f"Quest hash map refresh before phase-1: quests={refreshed_qhm}")
        # Resume from phase-2 checkpoint: skip expensive phase-1 rerun.
        if start_idx > 0:
            print(
                "Quest history phase-1 skipped on resume: "
                f"continue phase-2 from commit {start_idx + 1}/{total_commits}"
            )
        else:
            # Phase 1: infer created_version_id from local textMap DB first.
            # Commit replay below only fills unresolved rows.
            print("Quest history phase-1: local textMap DB infer (create+update)")
            prefilled_created_rows, prefilled_updated_rows = _backfill_quest_phase1_with_progress(
                cursor
            )
            print(
                "Quest history phase-1 done: "
                f"created_version rows backfilled={prefilled_created_rows}, "
                f"updated_version rows backfilled={prefilled_updated_rows}"
            )
        total_quests, unresolved_quests = _count_unresolved_quest_versions(cursor)
        unresolved_created_ids = _unresolved_created_quest_ids(cursor)
        unresolved_created_quests = len(unresolved_created_ids)
        unresolved_ratio = (
            (float(unresolved_created_quests) / float(total_quests))
            if total_quests > 0
            else 0.0
        )

        if unresolved_created_quests <= 0:
            replay_mode = "none"
            print(
                "Quest history phase-2 skipped: "
                f"created_null=0 (unresolved_total={unresolved_quests}/{total_quests})"
            )
        elif force:
            replay_mode = "full"
            print(
                "Quest history phase-2 mode: full (force enabled), "
                f"created_null={unresolved_created_quests}/{total_quests}, "
                f"unresolved_total={unresolved_quests}/{total_quests}"
            )
        elif unresolved_ratio <= max(0.0, float(unresolved_ratio_threshold)):
            replay_mode = "targeted"
            print(
                "Quest history phase-2 mode: targeted, "
                f"created_null={unresolved_created_quests}/{total_quests} "
                f"({unresolved_ratio * 100:.2f}%)"
            )
        else:
            replay_mode = "full"
            print(
                "Quest history phase-2 mode: full (auto fallback), "
                f"created_null={unresolved_created_quests}/{total_quests} "
                f"({unresolved_ratio * 100:.2f}% > threshold {unresolved_ratio_threshold * 100:.2f}%)"
            )

        if replay_mode in ("targeted", "full"):
            commit_pbar = tqdm(
                total=total_commits,
                initial=start_idx,
                desc=f"Quest history backfill (phase2 {replay_mode})",
                unit="commit",
                leave=False,
                position=0,
                dynamic_ncols=True,
                file=sys.stdout,
            )
            try:
                target_quest_ids = unresolved_created_ids if replay_mode == "targeted" else None
                for idx in range(start_idx, total_commits):
                    commit_sha, commit_title = commits[idx]
                    parent_sha = commits[idx - 1][0] if idx > 0 else first_parent_sha
                    _version_label, version_id = _resolve_commit_version(repo_path, commit_sha, commit_title)
                    if version_id is None:
                        continue

                    quest_backfilled_by_commit = 0
                    quest_entries = (
                        _initial_entries(repo_path, commit_sha, include_paths=["BinOutput/Quest"])
                        if parent_sha is None
                        else _diff_entries(repo_path, parent_sha, commit_sha, include_paths=["BinOutput/Quest"])
                    )
                    with tqdm(
                        total=len(quest_entries),
                        desc=f"{commit_sha[:8]} quest files",
                        unit="file",
                        leave=False,
                        position=1,
                        dynamic_ncols=True,
                        file=sys.stdout,
                    ) as file_pbar:
                        for entry in quest_entries:
                            quest_backfilled_by_commit += _backfill_quest_version_by_commit_entry(
                                cursor,
                                repo_path=repo_path,
                                commit_sha=commit_sha,
                                parent_sha=parent_sha,
                                entry=entry,
                                version_id=version_id,
                                target_quest_ids=target_quest_ids,
                            )
                            file_pbar.update(1)
                            file_pbar.set_postfix_str(
                                f"q_commit_create={quest_backfilled_by_commit}"
                            )
                    phase2_commit_created_backfilled += quest_backfilled_by_commit

                    unresolved_row = cursor.execute(
                        "SELECT COUNT(*) FROM quest WHERE created_version_id IS NULL"
                    ).fetchone()
                    current_unresolved_created = int(unresolved_row[0] or 0) if unresolved_row else 0
                    commit_pbar.set_postfix_str(
                        f"{commit_sha[:8]} q_commit_create={quest_backfilled_by_commit} created_null={current_unresolved_created}"
                    )

                    conn.commit()
                    set_meta(resume_target_key, resume_scope)
                    set_meta(resume_done_key, commit_sha)
                    commit_pbar.update(1)
            finally:
                commit_pbar.close()

        final_total, final_unresolved_count = _count_unresolved_quest_versions(cursor)
        print(
            "Quest history summary: "
            f"mode={replay_mode}, unresolved={final_unresolved_count}/{final_total}, "
            f"phase2_commit_created={phase2_commit_created_backfilled}"
        )
    finally:
        cursor.close()

    if prune_missing:
        prune_cursor = conn.cursor()
        _prune_unseen_rows_by_version(prune_cursor, "quest")
        prune_cursor.execute("DELETE FROM questTalk WHERE questId NOT IN (SELECT questId FROM quest)")
        try:
            prune_cursor.execute("DELETE FROM quest_text_signature WHERE questId NOT IN (SELECT questId FROM quest)")
        except sqlite3.OperationalError:
            pass
        prune_cursor.close()
        conn.commit()

    if resolved_from is None:
        set_meta(meta_commit_key, resolved_target)
        if commits:
            set_meta(meta_title_key, _latest_commit_meta_title(commits) or commits[-1][0])
    set_meta(resume_target_key, "")
    set_meta(resume_done_key, "")
    rebuild_version_catalog(["quest"])
    print(f"Quest history backfill finished at commit {resolved_target}")
    return {
        "replay_mode": replay_mode,
        "total_quests": int(final_total),
        "unresolved_quests": int(final_unresolved_count),
        "phase1_created_backfilled": int(prefilled_created_rows),
        "phase1_updated_backfilled": int(prefilled_updated_rows),
        "phase2_commit_created_backfilled": int(phase2_commit_created_backfilled),
    }

import gc
import json
import os
import logging
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterable, Mapping

try:
    import pygit2 as _pygit2  # type: ignore[import-untyped]
except ImportError:
    _pygit2 = None  # type: ignore[assignment]

# 日志配置

# Git 命令缓存


from DBConfig import conn, DATA_PATH
from git_utils import resolve_commit as _resolve_commit_impl, run_git as _run_git_impl
from import_utils import (
    DEFAULT_BATCH_SIZE,
    executemany_batched,
    fast_import_pragmas,
    normalize_unique_ints,
    to_hash_value as _to_hash_value,
)
from lang_constants import LANG_CODE_MAP
from quest_hash_map_utils import (
    count_unresolved_quest_versions as _count_unresolved_quest_versions,
    refresh_all_quest_hash_map as _refresh_all_quest_hash_map,
    unresolved_created_quest_ids as _unresolved_created_quest_ids,
)
from quest_source_utils import (
    SOURCE_TYPE_ANECDOTE,
    SOURCE_TYPE_HANGOUT,
    extract_anecdote_core_fields,
    extract_main_coop_ids,
    extract_storyboard_group_talk_ids,
)
from quest_utils import extract_quest_row as _extract_quest_row
from version_control import backfill_quest_created_version_from_textmap as _backfill_quest_created_version_from_textmap
from subtitle_utils import parse_srt_rows as _parse_srt_rows
from text_source_path_utils import (
    build_readable_rel_path_from_record as _build_readable_record_rel_path,
    build_subtitle_rel_path,
    build_subtitle_rel_path_from_record as _build_subtitle_record_rel_path,
    normalize_subtitle_rel_path,
)
from version_control import subtitle_text_changed_keys as _subtitle_text_changed_keys
from textmap_name_utils import parse_textmap_file_name, analyze_readable_version_exceptions, analyze_subtitle_version_exceptions, analyze_textmap_version_exceptions, report_version_exceptions
from version_control import (
    _build_version_preference_case_sql,
    _version_precedes_sql,
    ensure_version_schema,
    get_or_create_version_id,
    should_update_version,
)
from versioning import (
    _extract_version_tag,
    _version_tag_to_sort_key,
    _table_exists,
    get_meta,
    normalize_version_label,
    rebuild_version_catalog,
    set_meta,
)
from lightweight_progress import LightweightProgress

try:
    reconfigure = getattr(sys.stderr, "reconfigure", None)
    if callable(reconfigure):
        reconfigure(encoding="utf-8", errors="backslashreplace")
except Exception:
    pass

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('history_backfill.log', encoding="utf-8"),
        logging.StreamHandler(sys.stderr)
    ]
)
logger = logging.getLogger(__name__)
if _pygit2 is not None:
    logger.info(f"pygit2 {_pygit2.__version__} 可用，将使用进程内 git 读取")
else:
    logger.info("pygit2 不可用，将使用 subprocess 后端")
_first_commit_version_cache: dict[tuple[str, str], tuple[str | None, int | None]] = {}
DEFAULT_HISTORY_COMMIT_BATCH_SIZE = 50
DEFAULT_GIT_BACKFILL_CHECKPOINT_EVERY = 100
DEFAULT_FIX_COMMIT_BATCH_SIZE = 1000
_git_cache = {}
_snapshot_metadata_cache: dict[str, dict[str, object]] = {}
_snapshot_textmap_file_groups_cache: dict[tuple[str, str], dict[str, list[str]]] = {}
_snapshot_textmap_group_cache: dict[tuple[str, str, str], dict[str, object] | None] = {}
_GIT_SHOW_TEXT_CACHE_MAX = int(os.environ.get("GTS_GIT_SHOW_TEXT_CACHE_MAX", "1024"))
_git_show_text_cache: dict[tuple[str, str], str | None] = {}
_git_show_text_cache_order: list[tuple[str, str]] = []
_git_show_json_cache: dict[tuple[str, str], object] = {}
_git_show_text_cache_stats = {"hits": 0, "misses": 0}
_pygit2_repo_cache: dict[str, object] = {}


@contextmanager
def _suspend_automatic_gc(label: str):
    was_enabled = gc.isenabled()
    if was_enabled:
        gc.disable()
        logger.info(f"{label}: 已临时关闭 Python 自动 GC，避免大对象图遍历卡顿")
    try:
        yield
    finally:
        if was_enabled:
            collected = gc.collect()
            gc.enable()
            logger.info(f"{label}: 已恢复 Python 自动 GC，手动回收 {collected} 个对象")


def _clear_history_runtime_caches(
    *,
    clear_git_command_cache: bool = False,
    clear_snapshot_metadata: bool = False,
    clear_repo_cache: bool = False,
):
    _snapshot_textmap_file_groups_cache.clear()
    _snapshot_textmap_group_cache.clear()
    _git_show_text_cache.clear()
    _git_show_text_cache_order.clear()
    _git_show_json_cache.clear()
    _git_show_text_cache_stats["hits"] = 0
    _git_show_text_cache_stats["misses"] = 0
    if clear_git_command_cache:
        _git_cache.clear()
        _first_commit_version_cache.clear()
    if clear_snapshot_metadata:
        _snapshot_metadata_cache.clear()
    if clear_repo_cache:
        _pygit2_repo_cache.clear()

# Git 命令缓存


# 历史回放涉及的路径
RELEVANT_PATHS = [
    "TextMap",
    "Readable",
    "Subtitle",
    "ExcelBinOutput/NpcExcelConfigData.json",
    "BinOutput/Quest",
    "ExcelBinOutput/AnecdoteExcelConfigData.json",
    "ExcelBinOutput/MainCoopExcelConfigData.json",
    "ExcelBinOutput/CoopExcelConfigData.json",
    "BinOutput/Coop",
    "BinOutput/Talk/Coop",
    "BinOutput/Talk/StoryboardGroup",
    "ExcelBinOutput/LocalizationExcelConfigData.json",
    "ExcelBinOutput/DocumentExcelConfigData.json",
]
TEXTMAP_ONLY_PATHS = ["TextMap"]
READABLE_ONLY_PATHS = ["Readable"]
SUBTITLE_ONLY_PATHS = ["Subtitle"]
ANECDOTE_CONFIG_PATH = "ExcelBinOutput/AnecdoteExcelConfigData.json"
MAIN_COOP_CONFIG_PATH = "ExcelBinOutput/MainCoopExcelConfigData.json"


@dataclass(frozen=True)
class VersionSnapshot:
    version_tag: str
    version_label: str
    version_id: int
    commit_sha: str
    version_sort_key: int


@dataclass(frozen=True)
class SnapshotReplayRange:
    raw_target_commit: str
    raw_from_commit: str | None
    target_snapshot: VersionSnapshot | None
    from_snapshot: VersionSnapshot | None
    base_snapshot: VersionSnapshot | None
    snapshots: tuple[VersionSnapshot, ...]
    resume_scope: str



def ensure_breakpoint_schema():
    """确保断点表存在。"""
    cur = conn.cursor()
    cur.execute('''
    CREATE TABLE IF NOT EXISTS breakpoint (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stage_name TEXT UNIQUE,
        status TEXT DEFAULT 'pending',
        start_time TEXT,
        end_time TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()


def get_breakpoint_status(stage_name):
    """读取阶段断点状态。"""
    cur = conn.cursor()
    cur.execute("SELECT status FROM breakpoint WHERE stage_name = ?", (stage_name,))
    result = cur.fetchone()
    return result[0] if result else 'pending'


def update_breakpoint_status(stage_name, status, start_time=None, end_time=None):
    """更新阶段断点状态。"""
    cur = conn.cursor()
    if status == 'in_progress':
        cur.execute('''
        INSERT OR REPLACE INTO breakpoint (stage_name, status, start_time, end_time)
        VALUES (?, ?, ?, ?)
        ''', (stage_name, status, start_time, end_time))
    elif status == 'completed':
        cur.execute('''
        UPDATE breakpoint
        SET status = ?, end_time = ?
        WHERE stage_name = ?
        ''', (status, end_time, stage_name))
    conn.commit()


def _run_history_stage(stage_name, fn, *args, skip_asking=False, **kwargs):
    """执行单个历史回填阶段并处理断点。"""
    ensure_breakpoint_schema()
    try:
        status = get_breakpoint_status(stage_name)
    except Exception as e:
        logger.error(f"读取阶段断点状态失败 {stage_name}: {e}")
        status = 'pending'

    if not skip_asking:
        try:
            if status == 'completed':
                ans = input(f"{stage_name} 已完成，是否重新执行？(y/n): ")
                if ans != 'y':
                    logger.info(f"跳过 {stage_name}（已完成）...")
                    return True
            elif status == 'in_progress':
                ans = input(f"{stage_name} 正在执行，是否继续？(y/n): ")
                if ans != 'y':
                    logger.info(f"跳过 {stage_name}（执行中）...")
                    return True
            else:
                ans = input(f"是否跳过 {stage_name}？(y/n): ")
                if ans == 'y':
                    logger.info(f"跳过 {stage_name}...")
                    return True
        except KeyboardInterrupt:
            logger.info(f"用户中断，跳过 {stage_name}")
            return True
        except Exception as e:
            logger.error(f"读取用户输入失败: {e}")

    logger.info(f"开始执行 {stage_name}...")
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        update_breakpoint_status(stage_name, 'in_progress', start_time)
    except Exception as e:
        logger.error(f"更新阶段断点状态失败 {stage_name}: {e}")

    try:
        fn(*args, **kwargs)
        end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        try:
            update_breakpoint_status(stage_name, 'completed', start_time, end_time)
        except Exception as e:
            logger.error(f"将阶段断点状态更新为完成失败: {e}")
        logger.info(f"{stage_name} 执行完成")
        return False
    except KeyboardInterrupt:
        logger.error(f"用户中断 {stage_name}")
        raise
    except Exception as e:
        logger.error(f"{stage_name} 执行出错: {e}", exc_info=True)
        raise

def _run_git(repo_path: str, args: list[str], check: bool = True) -> str:
    return _run_git_impl(
        repo_path,
        args,
        check=check,
        cache=_git_cache,
        logger=logger,
        low_priority=True,
    )
    """执行 Git 命令并缓存结果。"""
    cache_key = (repo_path, tuple(args))

    if cache_key in _git_cache:
        return _git_cache[cache_key]

    try:
        logger.debug(f"执行 Git 命令: git -C {repo_path} {' '.join(args)}")
        creationflags = 0
        if sys.platform == "win32":
            try:
                CREATE_LOW_PRIORITY_CLASS = getattr(subprocess, 'CREATE_LOW_PRIORITY_CLASS', 0)
                creationflags = CREATE_LOW_PRIORITY_CLASS
            except Exception:
                pass
        proc = subprocess.run(
            ["git", "-C", repo_path] + args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=creationflags
        )

        if check and proc.returncode != 0:
            error_msg = proc.stderr.strip() or "git 命令执行失败"
            logger.error(f"Git 命令执行失败: {error_msg}")
            raise RuntimeError(error_msg)

        result = (proc.stdout or "").strip()

        _git_cache[cache_key] = result
        return result
    except Exception as e:
        logger.error(f"执行 Git 命令失败: {e}", exc_info=True)
        raise


def _resolve_commit(repo_path: str, rev: str) -> str:
    return _resolve_commit_impl(
        repo_path,
        rev,
        cache=_git_cache,
        logger=logger,
        low_priority=True,
    )
    """解析提交引用。"""
    return _run_git(repo_path, ["rev-parse", rev], check=True)


def _resolve_commit_title(repo_path: str, commit_sha: str) -> str:
    """读取提交标题。"""
    title = _run_git(repo_path, ["show", "-s", "--format=%s", commit_sha], check=False).strip()
    return title or commit_sha


def _list_commits(
    repo_path: str,
    target_commit: str,
    *,
    from_commit: str | None = None,
) -> list[tuple[str, str]]:
    """按顺序列出提交及标题。"""
    cmd_args = ["log", "--reverse", "--format=%H%x1f%s"]

    if from_commit:
        cmd_args.extend([f"{from_commit}..{target_commit}"])
    else:
        cmd_args.append(target_commit)

    out = _run_git(repo_path, cmd_args, check=True)

    commits = []
    for line in out.splitlines():
        if not line:
            continue
        if "\x1f" in line:
            sha, title = line.split("\x1f", 1)
        else:
            sha, title = line, ""
        commits.append((sha.strip(), title.strip()))

    if from_commit and commits and commits[0][0] != from_commit:
        found = False
        for idx, (sha, _title) in enumerate(commits):
            if sha == from_commit:
                found = True
                commits = commits[idx:]
                break
        if not found:
            raise RuntimeError(
                f"from_commit {from_commit} is not in target history {target_commit}"
            )

    return commits


def _resolve_parent_commit(repo_path: str, commit_sha: str) -> str | None:
    """获取提交的父提交。"""
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
    """获取回放起点的父提交。"""
    if not resolved_from or not commits:
        return None
    return _resolve_parent_commit(repo_path, commits[0][0])


def _resolve_commit_version(repo_path: str, commit_sha: str, commit_title: str) -> tuple[str, int | None]:
    """解析提交的版本信息。"""
    try:
        if commit_title:
            version_label = normalize_version_label(commit_title)
            if version_label:
                version_id = _resolve_version_id(version_label)
                if version_id:
                    return version_label, version_id

        version_label = normalize_version_label(commit_sha)
        if version_label:
            version_id = _resolve_version_id(version_label)
            if version_id:
                return version_label, version_id

        version_id = _resolve_version_id(commit_sha)
        return commit_sha, version_id
    except Exception as e:
        logger.error(f"解析提交版本失败 {commit_sha[:8]}: {e}")
        return commit_sha, None


def _extract_history_version_parts(commit_title: str | None) -> tuple[str | None, str | None]:
    normalized = normalize_version_label(commit_title) if commit_title else None
    if not normalized:
        return None, None
    version_tag = _extract_version_tag(normalized)
    if not version_tag:
        return None, None
    return normalized, version_tag


def _build_snapshot_specs_from_commit_rows(
    commit_rows: list[tuple[str, str]],
) -> tuple[dict[str, str | None], list[tuple[str, str, str, int]]]:
    commit_to_version_tag: dict[str, str | None] = {}
    canonical_by_tag: dict[str, tuple[str, str]] = {}
    current_tag: str | None = None

    for commit_sha, commit_title in commit_rows:
        version_label, explicit_tag = _extract_history_version_parts(commit_title)
        if explicit_tag:
            current_tag = explicit_tag
            canonical_by_tag[explicit_tag] = (
                version_label or explicit_tag,
                commit_sha,
            )
        commit_to_version_tag[commit_sha] = current_tag

    snapshot_specs: list[tuple[str, str, str, int]] = []
    for version_tag, (version_label, commit_sha) in canonical_by_tag.items():
        sort_key = _version_tag_to_sort_key(version_tag)
        if sort_key is None:
            continue
        snapshot_specs.append((version_tag, version_label, commit_sha, sort_key))

    snapshot_specs.sort(key=lambda item: (item[3], item[0], item[2]))
    return commit_to_version_tag, snapshot_specs


def _get_version_snapshot_metadata(repo_path: str) -> dict[str, object]:
    cached = _snapshot_metadata_cache.get(repo_path)
    if cached is not None:
        return cached

    commit_rows = _list_commits(repo_path, "HEAD")
    commit_to_version_tag, snapshot_specs = _build_snapshot_specs_from_commit_rows(commit_rows)

    snapshots: list[VersionSnapshot] = []
    snapshot_by_tag: dict[str, VersionSnapshot] = {}
    for version_tag, version_label, commit_sha, sort_key in snapshot_specs:
        version_id = _resolve_version_id(version_label)
        if version_id is None:
            continue
        snapshot = VersionSnapshot(
            version_tag=version_tag,
            version_label=version_label,
            version_id=version_id,
            commit_sha=commit_sha,
            version_sort_key=sort_key,
        )
        snapshots.append(snapshot)
        snapshot_by_tag[version_tag] = snapshot

    metadata = {
        "snapshots": tuple(snapshots),
        "snapshot_by_tag": snapshot_by_tag,
        "commit_to_version_tag": commit_to_version_tag,
    }
    _snapshot_metadata_cache[repo_path] = metadata
    return metadata


def _resolve_commit_snapshot(
    repo_path: str,
    commit_sha: str | None,
    *,
    metadata: dict[str, object] | None = None,
) -> VersionSnapshot | None:
    if not commit_sha:
        return None
    snapshot_metadata = metadata or _get_version_snapshot_metadata(repo_path)
    commit_to_version_tag = snapshot_metadata.get("commit_to_version_tag", {})
    snapshot_by_tag = snapshot_metadata.get("snapshot_by_tag", {})
    if not isinstance(commit_to_version_tag, dict) or not isinstance(snapshot_by_tag, dict):
        return None
    version_tag = commit_to_version_tag.get(commit_sha)
    if version_tag is None:
        return None
    snapshot = snapshot_by_tag.get(version_tag)
    return snapshot if isinstance(snapshot, VersionSnapshot) else None


def _resolve_snapshot_replay_range(
    repo_path: str,
    *,
    target_commit: str,
    from_commit: str | None = None,
) -> SnapshotReplayRange:
    raw_target_commit = _resolve_commit(repo_path, target_commit)
    raw_from_commit = _resolve_commit(repo_path, from_commit) if from_commit else None
    metadata = _get_version_snapshot_metadata(repo_path)
    raw_snapshots = metadata.get("snapshots")
    snapshots: tuple[VersionSnapshot, ...] = tuple()
    if isinstance(raw_snapshots, Iterable):
        snapshots = tuple(
            snapshot for snapshot in raw_snapshots if isinstance(snapshot, VersionSnapshot)
        )

    target_snapshot = _resolve_commit_snapshot(
        repo_path,
        raw_target_commit,
        metadata=metadata,
    )
    from_snapshot = _resolve_commit_snapshot(
        repo_path,
        raw_from_commit,
        metadata=metadata,
    ) if raw_from_commit else None

    snapshot_index = {snapshot.version_tag: idx for idx, snapshot in enumerate(snapshots)}
    target_idx = snapshot_index.get(target_snapshot.version_tag) if target_snapshot else None
    if target_idx is None:
        return SnapshotReplayRange(
            raw_target_commit=raw_target_commit,
            raw_from_commit=raw_from_commit,
            target_snapshot=target_snapshot,
            from_snapshot=from_snapshot,
            base_snapshot=None,
            snapshots=tuple(),
            resume_scope="",
        )

    start_idx = 0
    if from_snapshot is not None:
        start_idx = snapshot_index.get(from_snapshot.version_tag, -1) + 1
    if start_idx < 0:
        start_idx = 0

    if start_idx > target_idx:
        replay_snapshots: tuple[VersionSnapshot, ...] = tuple()
    else:
        replay_snapshots = snapshots[start_idx : target_idx + 1]
    base_snapshot = snapshots[start_idx - 1] if start_idx > 0 else None
    resume_scope = (
        f"{from_snapshot.commit_sha if from_snapshot else ''}.."
        f"{target_snapshot.commit_sha if target_snapshot else ''}"
    )
    return SnapshotReplayRange(
        raw_target_commit=raw_target_commit,
        raw_from_commit=raw_from_commit,
        target_snapshot=target_snapshot,
        from_snapshot=from_snapshot,
        base_snapshot=base_snapshot,
        snapshots=replay_snapshots,
        resume_scope=resume_scope,
    )


def _snapshot_display_label(snapshot: VersionSnapshot | None) -> str:
    if snapshot is None:
        return "unknown"
    return f"{snapshot.version_tag} ({snapshot.commit_sha[:8]})"

def _resolve_first_version_for_path(repo_path: str, file_path: str) -> tuple[str | None, int | None]:
    """解析文件首个提交及版本。"""
    cache_key = (repo_path, file_path)
    cached = _first_commit_version_cache.get(cache_key)
    if cached is not None:
        return cached

    out = _run_git(
        repo_path,
        ["log", "--reverse", "--format=%H", "-n", "1", "--", file_path],
        check=False,
    )
    first_commit = out.strip() if out.strip() else None
    version_id: int | None = None
    if first_commit:
        first_commit_title = _resolve_commit_title(repo_path, first_commit)
        _version_label, version_id = _resolve_commit_version(repo_path, first_commit, first_commit_title)
    result = (first_commit, version_id)
    _first_commit_version_cache[cache_key] = result
    return result


def _commit_and_checkpoint(db_conn, resume_key: str | None = None, resume_value: str | None = None):
    """提交事务并写入断点。"""
    db_conn.commit()
    if resume_key is not None and resume_value is not None:
        set_meta(resume_key, resume_value)


def _backfill_git_versions(
    cursor,
    table_name: str,
    select_sql: str,
    build_file_path_fn,
    update_sql: str,
    desc: str,
    unit: str = "records",
    checkpoint_every: int = DEFAULT_GIT_BACKFILL_CHECKPOINT_EVERY,
) -> int:
    """通用 Git 回溯回填辅助函数。"""
    backfilled_count = 0
    checkpoint_every = max(1, int(checkpoint_every))
    try:
        cursor.execute(select_sql)
        no_version_records = cursor.fetchall()

        if no_version_records:
            print(f"{table_name} 需要 Git 回溯回填：{len(no_version_records)} 条")
            total_records = len(no_version_records)
            resume_key = f"git_backfill_{table_name}_resume"
            start_idx = 0

            try:
                resume_value = get_meta(resume_key, "")
                if resume_value:
                    try:
                        start_idx = int(resume_value)
                        if start_idx < 0 or start_idx >= total_records:
                            start_idx = 0
                        else:
                            print(f"{table_name} Git 回溯从第 {start_idx + 1} 条继续")
                    except ValueError:
                        start_idx = 0
            except Exception as e:
                logger.error(f"读取断点失败: {e}")
                start_idx = 0

            record_groups: dict[str, list[tuple[int, tuple]]] = {}
            skipped_records = 0
            for i, record in enumerate(no_version_records[start_idx:], start=start_idx):
                file_path = build_file_path_fn(record)
                if not file_path:
                    skipped_records += 1
                    continue
                record_groups.setdefault(file_path, []).append((i, record))

            pbar = LightweightProgress(total_records, desc=desc, unit=unit, initial_print=False)
            pbar.current = start_idx + skipped_records
            pbar.update(0)
            processed_records = skipped_records
            last_resume_value = str(start_idx)
            with pbar:
                for file_path, records_for_path in record_groups.items():
                    try:
                        _first_commit, first_version_id = _resolve_first_version_for_path(DATA_PATH, file_path)
                        if first_version_id:
                            rows = (
                                (first_version_id, first_version_id) + record
                                for _idx, record in records_for_path
                            )
                            backfilled_count += executemany_batched(cursor, update_sql, rows)
                    except Exception as e:
                        print(f"[ERROR] {table_name} 的 Git 回溯失败 {file_path}: {e}")
                    finally:
                        for record_idx, record in records_for_path:
                            processed_records += 1
                            last_resume_value = str(record_idx + 1)
                            pbar.update(postfix=f"处理 {record}")
                            if processed_records % checkpoint_every == 0:
                                try:
                                    _commit_and_checkpoint(conn, resume_key, last_resume_value)
                                except Exception as e:
                                    logger.error(f"保存断点失败: {e}")

            _commit_and_checkpoint(conn)
            try:
                set_meta(resume_key, "")
            except Exception as e:
                logger.error(f"清理断点失败: {e}")
            print(f"{table_name} Git 回溯完成：更新 {backfilled_count} 条")
            return backfilled_count

        print(f"{table_name} 没有需要 Git 回溯的记录")
    except Exception as e:
        print(f"[ERROR] {table_name} Git 回溯失败: {e}")
        backfilled_count = 0

    return backfilled_count

def _backfill_textmap_git_versions(
    cursor,
    textmap_lang_map: dict[str, int],
    desc: str = "TextMap Git 回溯",
    unit: str = "records"
) -> int:
    """
    Git-based TextMap backfill helper.
    """
    backfilled_count = 0
    checkpoint_every = max(1, int(DEFAULT_GIT_BACKFILL_CHECKPOINT_EVERY))
    try:
        cursor.execute("SELECT lang, hash FROM textMap WHERE created_version_id IS NULL")
        no_version_records = cursor.fetchall()

        if no_version_records:
            print(f"TextMap 需要 Git 回溯回填：{len(no_version_records)} 条")
            total_records = len(no_version_records)
            resume_key = "git_backfill_textmap_resume"
            start_idx = 0

            try:
                resume_value = get_meta(resume_key, "")
                if resume_value:
                    try:
                        start_idx = int(resume_value)
                        if start_idx < 0 or start_idx >= total_records:
                            start_idx = 0
                        else:
                            print(f"TextMap Git 回溯从第 {start_idx + 1} 个文件继续")
                    except ValueError:
                        start_idx = 0
            except Exception as e:
                logger.error(f"读取断点失败: {e}")
                start_idx = 0

            pbar = LightweightProgress(total_records, desc=desc, unit=unit, initial_print=False)
            pbar.current = start_idx
            pbar.update(0)
            with pbar:
                textmap_groups = _list_snapshot_textmap_groups(DATA_PATH, "HEAD")
                textmap_files: list[str] = []
                for base_name in sorted(textmap_lang_map.keys()):
                    group_files = textmap_groups.get(base_name)
                    if group_files:
                        textmap_files.extend(group_files)
                    else:
                        textmap_files.append(f"TextMap/{base_name}")
                total_files = len(textmap_files)
                file_start_idx = start_idx if 0 <= start_idx < total_files else 0
                pending_by_hash: dict[str, list[tuple[int, int]]] = {}
                for pending_lang, pending_hash_value in no_version_records:
                    pending_by_hash.setdefault(str(pending_hash_value), []).append((pending_lang, pending_hash_value))

                update_textmap_sql = (
                    "UPDATE textMap SET "
                    "created_version_id=CASE "
                    "WHEN created_version_id IS NULL OR "
                    f"{_version_precedes_sql('?1', 'created_version_id')} THEN ?1 "
                    "ELSE created_version_id END, "
                    "updated_version_id=CASE "
                    "WHEN updated_version_id IS NULL "
                    "AND (created_version_id IS NULL OR "
                    f"{_version_precedes_sql('?1', 'created_version_id')}) THEN ?1 "
                    "WHEN updated_version_id = created_version_id "
                    "AND "
                    f"{_version_precedes_sql('?1', 'created_version_id')} THEN ?1 "
                    "ELSE updated_version_id END "
                    "WHERE lang = ?2 AND hash = ?3 "
                    "AND (created_version_id IS NULL OR "
                    f"{_version_precedes_sql('?1', 'created_version_id')})"
                )

                resolved_hashes: set[str] = set()
                with LightweightProgress(total_files, desc=desc, unit="files", initial_print=False) as file_pbar:
                    file_pbar.current = file_start_idx
                    file_pbar.update(0)
                    for file_idx in range(file_start_idx, total_files):
                        file_path = textmap_files[file_idx]
                        remaining_hashes = set(pending_by_hash.keys())
                        try:
                            out = _run_git(
                                DATA_PATH,
                                ["log", "--reverse", "--format=%H", "--", file_path],
                                check=False
                            )
                            commits = out.splitlines() if out else []
                            for commit_sha in commits:
                                if not remaining_hashes:
                                    break
                                content = _git_show_json(DATA_PATH, commit_sha, file_path)
                                if not isinstance(content, dict):
                                    continue
                                content_keys = {str(key) for key in content.keys()}
                                matched_hashes = remaining_hashes.intersection(content_keys)
                                if not matched_hashes:
                                    continue

                                commit_title = _resolve_commit_title(DATA_PATH, commit_sha)
                                _version_label, version_id = _resolve_commit_version(DATA_PATH, commit_sha, commit_title)
                                if not version_id:
                                    continue

                                update_rows = []
                                for matched_hash in matched_hashes:
                                    if matched_hash not in resolved_hashes:
                                        backfilled_count += len(pending_by_hash[matched_hash])
                                        resolved_hashes.add(matched_hash)
                                    for matched_lang, matched_hash_value in pending_by_hash[matched_hash]:
                                        update_rows.append(
                                            (
                                                version_id,
                                                matched_lang,
                                                matched_hash_value,
                                            )
                                        )
                                if update_rows:
                                    executemany_batched(cursor, update_textmap_sql, update_rows)
                                remaining_hashes.difference_update(matched_hashes)
                        except Exception as e:
                            print(f"[ERROR] TextMap 的 Git 回溯失败 {file_path}: {e}")
                        finally:
                            file_pbar.update(postfix=file_path)
                            if (file_idx + 1) % checkpoint_every == 0:
                                try:
                                    _commit_and_checkpoint(conn, resume_key, str(file_idx + 1))
                                except Exception as e:
                                    logger.error(f"保存断点失败: {e}")

            _commit_and_checkpoint(conn)
            try:
                set_meta(resume_key, "")
            except Exception as e:
                logger.error(f"清理断点失败: {e}")
            print(f"TextMap Git 回溯完成：更新 {backfilled_count} 条")
            return backfilled_count

        print("TextMap 没有需要 Git 回溯的记录")
    except Exception as e:
        print(f"[ERROR] TextMap Git 回溯失败: {e}")
        backfilled_count = 0

    return backfilled_count

def _latest_commit_meta_title(commits: list[tuple[str, str]]) -> str | None:
    """提取最新提交对应的版本标题。"""
    if not commits:
        return None
    return normalize_version_label(commits[-1][1]) or normalize_version_label(commits[-1][0]) or commits[-1][0]


def _initial_entries(repo_path: str, commit: str, include_paths: list[str] | None = None) -> list[dict]:
    """列出初始提交的文件条目。"""
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
    """列出两个提交之间的变更条目。"""
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


def _snapshot_entries(
    repo_path: str,
    snapshot: VersionSnapshot,
    previous_snapshot: VersionSnapshot | None,
    include_paths: list[str] | None = None,
) -> list[dict]:
    if previous_snapshot is None:
        return _initial_entries(repo_path, snapshot.commit_sha, include_paths=include_paths)
    return _diff_entries(
        repo_path,
        previous_snapshot.commit_sha,
        snapshot.commit_sha,
        include_paths=include_paths,
    )


def _get_pygit2_repo(repo_path: str):
    """获取或创建 pygit2 Repository 句柄（缓存复用）。"""
    cached = _pygit2_repo_cache.get(repo_path)
    if cached is not None:
        return cached
    repo = _pygit2.Repository(repo_path)
    _pygit2_repo_cache[repo_path] = repo
    return repo


def _git_show_text_pygit2(repo_path: str, commit: str, rel_path: str) -> str | None:
    """通过 pygit2 进程内读取 blob，无 subprocess 开销。"""
    try:
        repo = _get_pygit2_repo(repo_path)
        commit_obj = repo.revparse_single(commit)
        if commit_obj.type == _pygit2.GIT_OBJECT_TAG:  # type: ignore[union-attr]
            commit_obj = commit_obj.peel(_pygit2.Commit)  # type: ignore[union-attr]
        tree = commit_obj.peel(_pygit2.Tree)  # type: ignore[union-attr]
        entry = tree[rel_path]
        blob = repo[entry.id]
        if blob.type != _pygit2.GIT_OBJECT_BLOB:  # type: ignore[union-attr]
            return None
        return blob.data.decode("utf-8", errors="replace")
    except KeyError:
        logger.debug(f"pygit2: path not found {commit[:8]}:{rel_path}")
        return None
    except Exception as e:
        logger.debug(f"pygit2 读取失败 {commit[:8]}:{rel_path}: {e}")
        return None


def _git_show_text_subprocess(repo_path: str, commit: str, rel_path: str) -> str | None:
    """读取指定提交中的文本文件（无缓存，直接 subprocess）。"""
    try:
        logger.debug(f"Getting text from git: {commit}:{rel_path}")
        proc = subprocess.run(
            ["git", "-C", repo_path, "show", f"{commit}:{rel_path}"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if proc.returncode != 0:
            logger.debug(f"Git show failed for {commit}:{rel_path}, return code: {proc.returncode}")
            return None
        return proc.stdout
    except Exception as e:
        logger.error(f"读取 Git 文本失败: {e}")
        return None


def _git_show_text_uncached(repo_path: str, commit: str, rel_path: str) -> str | None:
    """读取指定提交中的文本文件（自动选择 pygit2 或 subprocess 后端）。"""
    if _pygit2 is not None:
        return _git_show_text_pygit2(repo_path, commit, rel_path)
    return _git_show_text_subprocess(repo_path, commit, rel_path)


def _git_show_text(repo_path: str, commit: str, rel_path: str) -> str | None:
    """读取指定提交中的文本文件（带 LRU 缓存）。"""
    cache_key = (commit, rel_path)
    if cache_key in _git_show_text_cache:
        _git_show_text_cache_stats["hits"] += 1
        return _git_show_text_cache[cache_key]
    _git_show_text_cache_stats["misses"] += 1
    result = _git_show_text_uncached(repo_path, commit, rel_path)
    _git_show_text_cache[cache_key] = result
    _git_show_text_cache_order.append(cache_key)
    if len(_git_show_text_cache_order) > _GIT_SHOW_TEXT_CACHE_MAX:
        evict_key = _git_show_text_cache_order.pop(0)
        _git_show_text_cache.pop(evict_key, None)
        _git_show_json_cache.pop(evict_key, None)
    return result


def _prefetch_via_pygit2(
    repo_path: str,
    commit: str,
    uncached_paths: list[str],
) -> None:
    """通过 pygit2 批量读取 blob 并注入缓存（无 subprocess）。"""
    try:
        repo = _get_pygit2_repo(repo_path)
        commit_obj = repo.revparse_single(commit)
        if commit_obj.type == _pygit2.GIT_OBJECT_TAG:  # type: ignore[union-attr]
            commit_obj = commit_obj.peel(_pygit2.Commit)  # type: ignore[union-attr]
        tree = commit_obj.peel(_pygit2.Tree)  # type: ignore[union-attr]
    except Exception as e:
        logger.warning(f"pygit2 预取失败 (commit {commit[:8]}): {e}")
        for p in uncached_paths:
            _git_show_text(repo_path, commit, p)
        return

    for rel_path in uncached_paths:
        cache_key = (commit, rel_path)
        try:
            entry = tree[rel_path]
            blob = repo[entry.id]
            if blob.type != _pygit2.GIT_OBJECT_BLOB:  # type: ignore[union-attr]
                _git_show_text_cache[cache_key] = None
            else:
                _git_show_text_cache[cache_key] = blob.data.decode("utf-8", errors="replace")
        except KeyError:
            _git_show_text_cache[cache_key] = None
        except Exception:
            _git_show_text_cache[cache_key] = None
        _git_show_text_cache_order.append(cache_key)

    while len(_git_show_text_cache_order) > _GIT_SHOW_TEXT_CACHE_MAX:
        evict_key = _git_show_text_cache_order.pop(0)
        _git_show_text_cache.pop(evict_key, None)
        _git_show_json_cache.pop(evict_key, None)


def _prefetch_via_git_archive(
    repo_path: str,
    commit: str,
    uncached_paths: list[str],
) -> None:
    """通过 git archive 批量预取并注入缓存（subprocess 方式）。"""
    import io
    import tarfile

    try:
        creationflags = 0
        if sys.platform == "win32":
            try:
                creationflags = getattr(subprocess, "CREATE_LOW_PRIORITY_CLASS", 0)
            except Exception:
                pass
        proc = subprocess.run(
            ["git", "-C", repo_path, "archive", "--format=tar", commit, "--"] + uncached_paths,
            capture_output=True,
            creationflags=creationflags,
        )
        if proc.returncode != 0:
            for p in uncached_paths:
                _git_show_text(repo_path, commit, p)
            return

        fetched: set[str] = set()
        with tarfile.open(fileobj=io.BytesIO(proc.stdout), mode="r:") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                rel = member.name.replace("\\", "/")
                fobj = tar.extractfile(member)
                if fobj is None:
                    continue
                raw_bytes = fobj.read()
                try:
                    text = raw_bytes.decode("utf-8", errors="replace")
                except Exception:
                    text = raw_bytes.decode("latin-1", errors="replace")
                cache_key = (commit, rel)
                _git_show_text_cache[cache_key] = text
                _git_show_text_cache_order.append(cache_key)
                fetched.add(rel)

        for p in uncached_paths:
            if p not in fetched:
                cache_key = (commit, p)
                _git_show_text_cache[cache_key] = None
                _git_show_text_cache_order.append(cache_key)

        while len(_git_show_text_cache_order) > _GIT_SHOW_TEXT_CACHE_MAX:
            evict_key = _git_show_text_cache_order.pop(0)
            _git_show_text_cache.pop(evict_key, None)
            _git_show_json_cache.pop(evict_key, None)

    except Exception as e:
        logger.warning(f"git archive 批量预取失败，回退到逐文件读取: {e}")
        for p in uncached_paths:
            _git_show_text(repo_path, commit, p)


def _prefetch_git_show_texts(
    repo_path: str,
    commit: str,
    rel_paths: list[str],
) -> None:
    """批量预取同一提交下的多个文件，将结果注入缓存。

    优先使用 pygit2 直接读取 blob（零 subprocess 开销），
    缺失时回退到 ``git archive`` 方式。
    已在缓存中的路径会被自动跳过。
    """
    if not rel_paths:
        return
    uncached_paths = [p for p in rel_paths if (commit, p) not in _git_show_text_cache]
    if not uncached_paths:
        return

    if _pygit2 is not None:
        _prefetch_via_pygit2(repo_path, commit, uncached_paths)
    else:
        _prefetch_via_git_archive(repo_path, commit, uncached_paths)


def _log_git_show_text_cache_stats():
    """输出缓存命中率统计。"""
    hits = _git_show_text_cache_stats["hits"]
    misses = _git_show_text_cache_stats["misses"]
    total = hits + misses
    if total > 0:
        rate = hits / total * 100
        logger.info(f"_git_show_text cache stats: {hits} hits, {misses} misses, {rate:.1f}% hit rate")


def _read_worktree_text(repo_path: str, rel_path: str) -> str | None:
    """Read the current working-tree file under AnimeGameData."""
    try:
        file_path = os.path.join(repo_path, rel_path.replace("/", os.sep))
        if not os.path.isfile(file_path):
            return None
        with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
            return handle.read()
    except Exception as e:
        logger.error(f"Read local file failed {rel_path}: {e}")
        return None


def _normalize_text_for_compare(text: str | None) -> str:
    if text is None:
        return ""
    return text.replace("\r\n", "\n").replace("\r", "\n")


_TEXTMAP_COMPARE_MISSING = object()


def _normalize_textmap_value_for_compare(value: object) -> object:
    if isinstance(value, str):
        return _normalize_text_for_compare(value)
    return value


def _textmap_values_match(left: object, right: object) -> bool:
    if left is _TEXTMAP_COMPARE_MISSING or right is _TEXTMAP_COMPARE_MISSING:
        return left is right
    return (
        _normalize_textmap_value_for_compare(left)
        == _normalize_textmap_value_for_compare(right)
    )


def _iter_textmap_common_items(
    snapshot_obj: Mapping[str, object],
    current_obj: Mapping[str, object],
):
    if len(snapshot_obj) <= len(current_obj):
        for raw_hash, snapshot_content in snapshot_obj.items():
            current_content = current_obj.get(raw_hash, _TEXTMAP_COMPARE_MISSING)
            if current_content is _TEXTMAP_COMPARE_MISSING:
                continue
            yield raw_hash, snapshot_content, current_content
        return

    for raw_hash, current_content in current_obj.items():
        snapshot_content = snapshot_obj.get(raw_hash, _TEXTMAP_COMPARE_MISSING)
        if snapshot_content is _TEXTMAP_COMPARE_MISSING:
            continue
        yield raw_hash, snapshot_content, current_content


def _textmap_snapshot_has_current_matches(
    snapshot_obj: Mapping[str, object],
    current_obj: Mapping[str, object],
) -> bool:
    for _raw_hash, snapshot_content, current_content in _iter_textmap_common_items(
        snapshot_obj,
        current_obj,
    ):
        if _textmap_values_match(snapshot_content, current_content):
            return True
    return False


def _should_backfill_textmap_updated_version(
    previous_content: object,
    snapshot_content: object,
    current_content: object,
) -> bool:
    """
    Only treat a snapshot as the updated version when this hash itself changes
    into the current text at that snapshot.
    """
    if not _textmap_values_match(snapshot_content, current_content):
        return False
    return not _textmap_values_match(previous_content, current_content)


def _build_textmap_history_update_rows(
    *,
    snapshot_obj: Mapping[str, object],
    previous_snapshot_obj: Mapping[str, object] | None,
    current_obj: Mapping[str, object],
    lang_id: int,
    version_id: int,
    existing_map: Mapping[int, tuple[int | None, int | None]],
) -> list[tuple[int | None, int | None, int, int]]:
    previous_snapshot = previous_snapshot_obj or {}
    update_rows: list[tuple[int | None, int | None, int, int]] = []

    for raw_hash, snapshot_content, current_content in _iter_textmap_common_items(
        snapshot_obj,
        current_obj,
    ):
        try:
            hash_value = int(_to_hash_value(raw_hash))
        except Exception:
            continue

        version_info = existing_map.get(hash_value)
        if version_info is None:
            continue

        existing_created_version, existing_updated_version = version_info
        created_version = existing_created_version
        updated_version = existing_updated_version

        if should_update_version(existing_created_version, version_id, is_created=True):
            created_version = version_id

        previous_content = previous_snapshot.get(raw_hash, _TEXTMAP_COMPARE_MISSING)
        if _should_backfill_textmap_updated_version(
            previous_content,
            snapshot_content,
            current_content,
        ) and (
            existing_updated_version is None
            or should_update_version(existing_updated_version, version_id, is_created=True)
        ):
            updated_version = version_id

        if (
            created_version != existing_created_version
            or updated_version != existing_updated_version
        ):
            update_rows.append((created_version, updated_version, lang_id, hash_value))

    return update_rows


def _load_textmap_version_cache_for_current_group(
    cursor,
    *,
    lang_id: int,
    current_obj: Mapping[str, object],
    batch_size: int,
) -> dict[int, tuple[int | None, int | None]]:
    hash_values: list[int] = []
    seen_hashes: set[int] = set()
    for raw_hash in current_obj.keys():
        try:
            hash_value = int(_to_hash_value(raw_hash))
        except Exception:
            continue
        if hash_value in seen_hashes:
            continue
        seen_hashes.add(hash_value)
        hash_values.append(hash_value)

    if not hash_values:
        return {}

    version_map: dict[int, tuple[int | None, int | None]] = {}
    chunk_size = max(1, int(batch_size))
    for idx in range(0, len(hash_values), chunk_size):
        chunk = hash_values[idx : idx + chunk_size]
        placeholders = ",".join(["?"] * len(chunk))
        params = [lang_id, *chunk]
        rows = cursor.execute(
            f"SELECT hash, created_version_id, updated_version_id "
            f"FROM textMap WHERE lang=? AND hash IN ({placeholders})",
            params,
        ).fetchall()
        for row_hash, created_version_id, updated_version_id in rows:
            version_map[int(row_hash)] = (created_version_id, updated_version_id)

    return version_map


def _merge_textmap_version_updates_into_cache(
    version_map: dict[int, tuple[int | None, int | None]],
    update_rows: Iterable[tuple[int | None, int | None, int, int]],
) -> None:
    for created_version, updated_version, _lang_id, hash_value in update_rows:
        version_map[int(hash_value)] = (created_version, updated_version)


def _load_worktree_textmap_group(repo_path: str, base_name: str) -> dict[str, object] | None:
    """Load current AnimeGameData TextMap files related to one canonical base name."""
    textmap_dir = os.path.join(repo_path, "TextMap")
    if not os.path.isdir(textmap_dir):
        return None

    merged: dict[str, object] = {}
    matched = False
    try:
        for entry in os.listdir(textmap_dir):
            parsed = parse_textmap_file_name(entry)
            if parsed is None:
                continue
            canonical_name, _split_part = parsed
            if canonical_name != base_name:
                continue

            file_path = os.path.join(textmap_dir, entry)
            if not os.path.isfile(file_path):
                continue

            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                payload = json.load(handle)
            if not isinstance(payload, dict):
                continue

            merged.update(payload)
            matched = True
    except Exception as e:
        logger.error(f"Load local TextMap failed {base_name}: {e}")
        return None

    return merged if matched else None


def _list_snapshot_textmap_groups(
    repo_path: str,
    commit_sha: str,
) -> dict[str, list[str]]:
    cache_key = (repo_path, commit_sha)
    cached = _snapshot_textmap_file_groups_cache.get(cache_key)
    if cached is not None:
        return cached

    groups: dict[str, list[str]] = {}
    out = _run_git(
        repo_path,
        ["ls-tree", "-r", "--name-only", commit_sha, "--", "TextMap"],
        check=False,
    )
    for rel_path in out.splitlines():
        rel_path = rel_path.strip()
        if not rel_path:
            continue
        file_name = os.path.basename(rel_path)
        parsed = parse_textmap_file_name(file_name)
        if parsed is None:
            continue
        base_name, _split_part = parsed
        groups.setdefault(base_name, []).append(rel_path)

    for base_name in groups:
        groups[base_name].sort()
    _snapshot_textmap_file_groups_cache[cache_key] = groups
    return groups


def _load_snapshot_textmap_group(
    repo_path: str,
    commit_sha: str,
    base_name: str,
) -> dict[str, object] | None:
    cache_key = (repo_path, commit_sha, base_name)
    cached = _snapshot_textmap_group_cache.get(cache_key)
    if cached is not None or cache_key in _snapshot_textmap_group_cache:
        return cached

    merged: dict[str, object] = {}
    matched = False
    groups = _list_snapshot_textmap_groups(repo_path, commit_sha)
    group_paths = groups.get(base_name, [])
    if group_paths:
        _prefetch_git_show_texts(repo_path, commit_sha, group_paths)
    for rel_path in group_paths:
        payload = _git_show_json(repo_path, commit_sha, rel_path)
        if not isinstance(payload, dict):
            continue
        merged.update(payload)
        matched = True

    result = merged if matched else None
    _snapshot_textmap_group_cache[cache_key] = result
    return result


def _git_show_json(repo_path: str, commit: str, rel_path: str):
    """读取并解析指定提交中的 JSON（带缓存）。"""
    json_cache_key = (commit, rel_path)
    cached = _git_show_json_cache.get(json_cache_key)
    if cached is not None:
        return cached
    try:
        raw = _git_show_text(repo_path, commit, rel_path)
        if raw is None:
            return None
        try:
            parsed = json.loads(raw)
        except Exception as e:
            logger.debug(f"解析 JSON 失败 {commit}:{rel_path}: {e}")
            return None
        _git_show_json_cache[json_cache_key] = parsed
        return parsed
    except Exception as e:
        logger.error(f"读取 Git JSON 失败: {e}")
        return None



def _resolve_version_id(version_label: str) -> int | None:
    """解析并获取版本 ID。"""
    return get_or_create_version_id(normalize_version_label(version_label) or version_label)

def _extract_anecdote_history_row(
    row: dict,
) -> tuple[int, int | None, int | None, int | None, list[int]] | None:
    core_fields = extract_anecdote_core_fields(row)
    if core_fields is None:
        return None
    return (
        core_fields["quest_id"],
        core_fields["title_text_map_hash"],
        core_fields["desc_text_map_hash"],
        core_fields["long_desc_text_map_hash"],
        core_fields["group_ids"],
    )


def _load_anecdote_history_payload(repo_path: str, commit_sha: str, anecdote_id: int) -> dict | None:
    rows = _git_show_json(repo_path, commit_sha, ANECDOTE_CONFIG_PATH)
    if not isinstance(rows, list):
        return None
    matched_row = None
    for row in rows:
        extracted = _extract_anecdote_history_row(row)
        if extracted is None:
            continue
        if extracted[0] == anecdote_id:
            matched_row = extracted
            break
    if matched_row is None:
        return None

    _quest_id, title_hash, desc_hash, long_desc_hash, group_ids = matched_row
    talk_ids: list[int] = []
    seen: set[int] = set()
    for group_id in group_ids:
        group_obj = _git_show_json(repo_path, commit_sha, f"BinOutput/Talk/StoryboardGroup/{group_id}.json")
        if not isinstance(group_obj, dict):
            continue
        for talk_id in extract_storyboard_group_talk_ids(group_obj):
            if talk_id in seen:
                continue
            seen.add(talk_id)
            talk_ids.append(talk_id)
    return {
        "quest_id": anecdote_id,
        "title_hash": title_hash,
        "desc_hash": desc_hash,
        "long_desc_hash": long_desc_hash,
        "talk_ids": talk_ids,
    }

def _load_hangout_history_payload(repo_path: str, commit_sha: str, quest_id: int) -> dict | None:
    rows = _git_show_json(repo_path, commit_sha, MAIN_COOP_CONFIG_PATH)
    main_coop_ids = extract_main_coop_ids(rows, quest_id)
    if not main_coop_ids:
        return None
    existing_main_coop_ids: list[int] = []
    for main_coop_id in main_coop_ids:
        coop_obj = _git_show_json(repo_path, commit_sha, f"BinOutput/Coop/Coop{main_coop_id}.json")
        if isinstance(coop_obj, dict):
            existing_main_coop_ids.append(main_coop_id)
    if not existing_main_coop_ids:
        return None
    return {
        "quest_id": quest_id,
        "main_coop_ids": existing_main_coop_ids,
    }


def _get_quest_source_type(cursor, quest_id: int) -> str | None:
    row = cursor.execute("SELECT source_type FROM quest WHERE questId = ?", (quest_id,)).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0]).strip().upper() or None


def _get_quest_source_fields(cursor, quest_id: int) -> tuple[str | None, str | None]:
    row = cursor.execute(
        "SELECT source_type, source_code_raw FROM quest WHERE questId = ?",
        (quest_id,),
    ).fetchone()
    if not row:
        return None, None
    source_type = str(row[0]).strip().upper() if row[0] is not None else None
    source_code_raw = str(row[1]).strip().upper() if row[1] is not None else None
    return source_type or None, source_code_raw or None


def _build_quest_history_include_paths(repo_path: str) -> list[str]:
    include_paths = [
        "BinOutput/Quest",
        ANECDOTE_CONFIG_PATH,
        MAIN_COOP_CONFIG_PATH,
        "ExcelBinOutput/CoopExcelConfigData.json",
        "BinOutput/Coop",
        "BinOutput/Talk/Coop",
        "BinOutput/Talk/StoryboardGroup",
    ]
    excel_dir = os.path.join(repo_path, "ExcelBinOutput")
    if os.path.isdir(excel_dir):
        for entry in sorted(os.listdir(excel_dir)):
            if entry.startswith("TalkExcelConfigData") and entry.endswith(".json"):
                include_paths.append(f"ExcelBinOutput/{entry}")
    return include_paths


def _find_anecdote_first_commit(repo_path: str, anecdote_id: int) -> str | None:
    out = _run_git(
        repo_path,
        ["log", "--reverse", "--format=%H", "--", ANECDOTE_CONFIG_PATH],
        check=False,
    )
    if not out:
        return None
    for commit_sha in out.splitlines():
        commit_sha = commit_sha.strip()
        if not commit_sha:
            continue
        payload = _load_anecdote_history_payload(repo_path, commit_sha, anecdote_id)
        if payload is not None:
            return commit_sha
    return None


def _find_hangout_first_commit(repo_path: str, quest_id: int) -> str | None:
    out = _run_git(
        repo_path,
        ["log", "--reverse", "--format=%H", "--", MAIN_COOP_CONFIG_PATH, "BinOutput/Coop"],
        check=False,
    )
    if not out:
        return None
    for commit_sha in out.splitlines():
        commit_sha = commit_sha.strip()
        if not commit_sha:
            continue
        payload = _load_hangout_history_payload(repo_path, commit_sha, quest_id)
        if payload is not None:
            return commit_sha
    return None


def _prepare_resume_for_commits(
    *,
    resume_target_key: str,
    resume_done_key: str,
    resolved_target: str,
    commits: list[tuple[str, str]],
    force: bool,
    label: str,
) -> int:
    """根据断点计算提交起点。"""
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
                    f"{label} 断点续跑：从第 {start_idx + 1}/{total_commits} 个提交继续 "
                    f"(上次完成: {resume_done[:8]})"
                )
            else:
                print(f"{label} 断点续跑：提交已全部处理，仅执行元数据收尾。")
        else:
            print(f"{label} 的断点不在当前历史中，已从头开始。")
            start_idx = 0
    return start_idx


def _prepare_resume_for_snapshots(
    *,
    resume_target_key: str,
    resume_done_key: str,
    resolved_target: str,
    snapshots: tuple[VersionSnapshot, ...],
    force: bool,
    label: str,
) -> int:
    """根据断点计算版本快照起点。"""
    total_snapshots = len(snapshots)
    snapshot_index = {snapshot.commit_sha: idx for idx, snapshot in enumerate(snapshots)}

    if force:
        set_meta(resume_target_key, "")
        set_meta(resume_done_key, "")

    start_idx = 0
    resume_target = get_meta(resume_target_key, "")
    resume_done = get_meta(resume_done_key, "")
    if not force and resume_target == resolved_target and resume_done:
        done_idx = snapshot_index.get(resume_done)
        if done_idx is not None:
            start_idx = done_idx + 1
            if start_idx < total_snapshots:
                done_snapshot = snapshots[done_idx]
                print(
                    f"{label} 断点续跑：从第 {start_idx + 1}/{total_snapshots} 个版本快照继续 "
                    f"(上次完成: {done_snapshot.version_tag} / {resume_done[:8]})"
                )
            else:
                print(f"{label} 断点续跑：版本快照已全部处理，仅执行元数据收尾。")
        else:
            print(f"{label} 的断点不在当前快照链中，已从头开始。")
            start_idx = 0
    return start_idx


def _extract_quest_backfill_stats(backfill_result) -> tuple[int, int]:
    """标准化任务回填统计结果。"""
    if isinstance(backfill_result, dict):
        return (
            int(backfill_result.get("created_rows", 0)),
            int(backfill_result.get("updated_rows", 0)),
        )
    if isinstance(backfill_result, tuple) and len(backfill_result) >= 2:
        return int(backfill_result[0]), int(backfill_result[1])
    return 0, 0



def _sync_created_version_from_git(cursor) -> int:
    """Use git_created_version_id as the floor for created_version_id."""
    cursor.execute(
        f"""
        UPDATE quest
        SET created_version_id = git_created_version_id
        WHERE git_created_version_id IS NOT NULL
          AND (
              created_version_id IS NULL
              OR {_version_precedes_sql('git_created_version_id', 'created_version_id')}
          )
        """
    )
    return int(cursor.rowcount or 0)


def _quest_text_signature(row):
    """提取任务文本签名。"""
    if row is None:
        return None
    # Quest versioning should only follow title text changes, not chapter remapping.
    return row[0], row[1]


def find_quest_first_commit(
    cursor,
    *,
    repo_path: str,
    quest_id: int,
    commit_sha: str,
    parent_sha: str | None,
    version_id: int,
) -> tuple[str | None, int | None]:
    """解析任务在指定提交中的创建版本信息。"""
    cursor.execute(
        "SELECT git_created_version_id FROM quest WHERE questId = ?",
        (quest_id,)
    )
    cached_version = cursor.fetchone()
    existing_git_created_version = cached_version[0] if cached_version else None

    if should_update_version(existing_git_created_version, version_id, is_created=True):
        quest_file_path = f"BinOutput/Quest/{quest_id}.json"

        new_obj = _git_show_json(repo_path, commit_sha, quest_file_path)
        if not isinstance(new_obj, dict):
            return None, None
        new_row = _extract_quest_row(new_obj)
        if new_row is None:
            return None, None

        return commit_sha, version_id
    else:
        return None, None


def _update_quest_created_git_versions(cursor, quest_id: int, version_id: int) -> tuple[bool, bool]:
    cursor.execute(
        "SELECT created_version_id, git_created_version_id FROM quest WHERE questId = ?",
        (quest_id,),
    )
    version_info = cursor.fetchone()
    existing_created_version = version_info[0] if version_info else None
    existing_git_created_version = version_info[1] if version_info else None

    created_updated = False
    git_updated = False
    if should_update_version(existing_created_version, version_id, is_created=True):
        cursor.execute(
            """
            UPDATE quest
            SET created_version_id=?
            WHERE questId=?
            """,
            (version_id, quest_id),
        )
        created_updated = cursor.rowcount > 0
    if should_update_version(existing_git_created_version, version_id, is_created=True):
        cursor.execute(
            """
            UPDATE quest
            SET git_created_version_id=?
            WHERE questId=?
            """,
            (version_id, quest_id),
        )
        git_updated = cursor.rowcount > 0
    return created_updated, git_updated


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
    """按提交条目回填单个任务版本。"""
    action = entry["action"]
    if action == "D":
        return 0
    old_path = entry.get("old_path")
    new_path = entry.get("new_path")
    rel_path = (new_path or old_path or "").replace("\\", "/")
    if rel_path == ANECDOTE_CONFIG_PATH:
        rows = _git_show_json(repo_path, commit_sha, rel_path)
        if not isinstance(rows, list):
            return 0
        updated_total = 0
        for row in rows:
            extracted = _extract_anecdote_history_row(row)
            if extracted is None:
                continue
            quest_id = extracted[0]
            if target_quest_ids is not None and quest_id not in target_quest_ids:
                continue
            if version_id <= 0:
                logger.warning(f"Invalid version ID {version_id} for anecdote {quest_id}, skipping")
                continue
            created_updated, git_updated = _update_quest_created_git_versions(cursor, quest_id, version_id)
            if created_updated or git_updated:
                updated_total += 1
        return updated_total

    if rel_path == MAIN_COOP_CONFIG_PATH:
        rows = _git_show_json(repo_path, commit_sha, rel_path)
        if not isinstance(rows, list):
            return 0
        updated_total = 0
        seen_quest_ids: set[int] = set()
        for raw_id in extract_main_coop_ids(rows):
            quest_id = raw_id // 100
            if quest_id in seen_quest_ids:
                continue
            seen_quest_ids.add(quest_id)
            if target_quest_ids is not None and quest_id not in target_quest_ids:
                continue
            if _load_hangout_history_payload(repo_path, commit_sha, quest_id) is None:
                continue
            created_updated, git_updated = _update_quest_created_git_versions(cursor, quest_id, version_id)
            if created_updated or git_updated:
                updated_total += 1
        return updated_total

    if rel_path.startswith("BinOutput/Coop/Coop") and rel_path.endswith(".json"):
        file_name = os.path.basename(rel_path)
        match = re.match(r"^Coop(\d+)\.json$", file_name)
        if not match:
            return 0
        quest_id = int(match.group(1)) // 100
        if target_quest_ids is not None and quest_id not in target_quest_ids:
            return 0
        if _load_hangout_history_payload(repo_path, commit_sha, quest_id) is None:
            return 0
        created_updated, git_updated = _update_quest_created_git_versions(cursor, quest_id, version_id)
        return 1 if (created_updated or git_updated) else 0

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

    if version_id <= 0:
        logger.warning(f"Invalid version ID {version_id} for quest {new_row[0]}, skipping")
        return 0

    created_updated, git_updated = _update_quest_created_git_versions(cursor, int(new_row[0]), version_id)
    if created_updated:
        logger.debug(f"Updated created_version_id for quest {new_row[0]} to {version_id}")
    if git_updated:
        logger.debug(f"Updated git_created_version_id for quest {new_row[0]} to {version_id}")

    return 1 if (created_updated or git_updated) else 0


def _replay_quest_snapshot_entries(
    cursor,
    *,
    repo_path: str,
    snapshots: tuple[VersionSnapshot, ...],
    base_snapshot: VersionSnapshot | None,
    target_quest_ids: set[int] | None = None,
    start_idx: int = 0,
    pbar_desc: str = "任务回放",
    checkpoint_every: int = DEFAULT_HISTORY_COMMIT_BATCH_SIZE,
    resume_target_key: str | None = None,
    resume_done_key: str | None = None,
    resume_scope: str | None = None,
) -> int:
    if not snapshots:
        return 0

    checkpoint_every = max(1, int(checkpoint_every))
    quest_history_include_paths = _build_quest_history_include_paths(repo_path)
    updated_total = 0
    processed_snapshots = 0
    last_snapshot: VersionSnapshot | None = None

    pbar = LightweightProgress(len(snapshots), desc=pbar_desc, unit="snapshots", initial_print=False)
    pbar.current = start_idx
    pbar.update(0)
    with pbar:
        for idx in range(start_idx, len(snapshots)):
            snapshot = snapshots[idx]
            last_snapshot = snapshot
            previous_snapshot = snapshots[idx - 1] if idx > 0 else base_snapshot
            postfix = f"{snapshot.version_tag} ({snapshot.commit_sha[:8]})"

            entries = _snapshot_entries(
                repo_path,
                snapshot,
                previous_snapshot,
                include_paths=quest_history_include_paths,
            )
            # 批量预取该快照所有条目涉及的文件内容
            quest_prefetch: dict[str, list[str]] = {}
            quest_parent_sha = previous_snapshot.commit_sha if previous_snapshot else None
            for entry in entries:
                rel = (entry.get("new_path") or entry.get("old_path") or "").replace("\\", "/")
                if not rel:
                    continue
                quest_prefetch.setdefault(snapshot.commit_sha, []).append(rel)
                if quest_parent_sha:
                    quest_prefetch.setdefault(quest_parent_sha, []).append(rel)
            for pfetch_commit, pfetch_paths in quest_prefetch.items():
                _prefetch_git_show_texts(repo_path, pfetch_commit, pfetch_paths)

            for entry in entries:
                try:
                    updated_total += _backfill_quest_version_by_commit_entry(
                        cursor,
                        repo_path=repo_path,
                        commit_sha=snapshot.commit_sha,
                        parent_sha=previous_snapshot.commit_sha if previous_snapshot else None,
                        entry=entry,
                        version_id=snapshot.version_id,
                        target_quest_ids=target_quest_ids,
                    )
                except Exception as e:
                    print(f"处理任务快照条目失败: {e}")

            pbar.update(postfix=postfix)
            processed_snapshots += 1
            if (
                resume_target_key
                and resume_done_key
                and resume_scope
                and processed_snapshots % checkpoint_every == 0
            ):
                conn.commit()
                set_meta(resume_target_key, resume_scope)
                set_meta(resume_done_key, snapshot.commit_sha)

    if processed_snapshots > 0 and last_snapshot is not None:
        conn.commit()
        if resume_target_key and resume_done_key and resume_scope:
            set_meta(resume_target_key, resume_scope)
            set_meta(resume_done_key, last_snapshot.commit_sha)
    _log_git_show_text_cache_stats()
    return updated_total


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
    """按批次回放任务历史记录并返回统计信息。"""
    stats = {
        "looked_up_hashes": 0,
        "quest_updated_by_textmap": 0,
        "quest_created_backfilled": 0,
        "quest_updated_backfilled": 0,
    }
    if version_id is None:
        return stats

    try:
        cursor.execute("CREATE TEMP TABLE IF NOT EXISTS _changed_textmap_hash(hash INTEGER PRIMARY KEY)")
        cursor.execute("DELETE FROM _changed_textmap_hash")

        if quest_scope is not None:
            try:
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
                quest_scope_filter = "q.questId IN (SELECT questId FROM _target_quest_id)"
            except Exception as e:
                logger.error(f"Error processing quest scope: {e}")
                quest_scope_filter = "1=1"
        else:
            quest_scope_filter = "1=1"

        if changed_hashes is not None:
            try:
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
                executemany_batched(
                    cursor,
                    "INSERT OR IGNORE INTO _changed_textmap_hash(hash) VALUES (?)",
                    (row for row in normalized_hashes),
                    batch_size=batch_size,
                )
            except Exception as e:
                logger.error(f"Error processing changed hashes: {e}")
                return stats
        else:
            try:
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
                def fetch_hashes():
                    while True:
                        rows = select_cur.fetchmany(fetch_size)
                        if not rows:
                            break
                        for row in rows:
                            yield row
                executemany_batched(
                    cursor,
                    "INSERT OR IGNORE INTO _changed_textmap_hash(hash) VALUES (?)",
                    fetch_hashes(),
                    batch_size=batch_size,
                )
            except Exception as e:
                logger.error(f"Error fetching hashes from textMap: {e}")
                return stats

        try:
            has_quest_hash_map_rows = False
            if _table_exists("quest_hash_map"):
                row = cursor.execute("SELECT 1 FROM quest_hash_map LIMIT 1").fetchone()
                has_quest_hash_map_rows = row is not None
        except Exception as e:
            logger.error(f"Error checking quest_hash_map table: {e}")
            has_quest_hash_map_rows = False

        try:
            lang_rows = cursor.execute("SELECT id FROM langCode WHERE imported=1").fetchall()
            languages = [row[0] for row in lang_rows]
        except Exception as e:
            logger.error(f"Error fetching languages: {e}")
            languages = []

        for lang in languages:
            try:
                if has_quest_hash_map_rows:
                    cursor.execute(
                        f"""
                        INSERT INTO quest_version(questId, lang, updated_version_id)
                        SELECT DISTINCT qhm.questId, ?, ?
                        FROM quest_hash_map qhm
                        JOIN _changed_textmap_hash c ON c.hash = qhm.hash
                        JOIN quest q ON q.questId = qhm.questId
                        WHERE qhm.source_type IN ('title', 'dialogue')
                          AND {quest_scope_filter}
                        ON CONFLICT(questId, lang) DO UPDATE SET
                        updated_version_id={_build_version_preference_case_sql(
                            existing_expr="quest_version.updated_version_id",
                            candidate_expr="excluded.updated_version_id",
                            is_created=False,
                        )}
                        """,
                        (lang, version_id),
                    )
                else:
                    cursor.execute(
                        f"""
                        INSERT INTO quest_version(questId, lang, updated_version_id)
                        SELECT DISTINCT q.questId, ?, ?
                        FROM quest q
                        WHERE {quest_scope_filter}
                        AND (
                            titleTextMapHash IN (SELECT hash FROM _changed_textmap_hash)
                            OR questId IN (
                                SELECT DISTINCT qt.questId
                                FROM questTalk qt
                                JOIN dialogue d ON d.talkId = qt.talkId
                                   AND (
                                       (coalesce(qt.coopQuestId, 0) = 0 AND d.coopQuestId IS NULL)
                                       OR (coalesce(qt.coopQuestId, 0) > 0 AND d.coopQuestId = qt.coopQuestId)
                                   )
                                JOIN _changed_textmap_hash c ON c.hash = d.textHash
                            )
                        )
                        ON CONFLICT(questId, lang) DO UPDATE SET
                        updated_version_id={_build_version_preference_case_sql(
                            existing_expr="quest_version.updated_version_id",
                            candidate_expr="excluded.updated_version_id",
                            is_created=False,
                        )}
                        """,
                        (lang, version_id),
                    )
                stats["quest_updated_by_textmap"] += int(cursor.rowcount or 0)
            except Exception as e:
                logger.error(f"Error updating quest versions for language {lang}: {e}")
        if version_label:
            try:
                backfill_result = _backfill_quest_created_version_from_textmap(
                    cursor,
                    quest_updated_version=version_label,
                    quest_ids=quest_scope,
                    overwrite_existing=False,
                    with_stats=True,
                )
                created_rows, updated_rows = _extract_quest_backfill_stats(backfill_result)
                stats["quest_created_backfilled"] = created_rows
                stats["quest_updated_backfilled"] = updated_rows
            except Exception as e:
                logger.error(f"Error backfilling quest created versions: {e}")
        return stats
    except Exception as e:
        logger.error(f"Error in apply_quest_version_delta_from_textmap: {e}", exc_info=True)
        return stats


def _backfill_quest_phase1_with_progress(
    cursor,
    *,
    chunk_size: int = 2000,
) -> tuple[int, int]:
    """带进度执行任务首轮回填。"""
    total_row = cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
    total_quests = int(total_row[0] or 0) if total_row else 0
    if total_quests <= 0:
        return 0, 0

    created_total = 0
    updated_total = 0
    # 使用独立 cursor 遍历 questId，避免 backfill 内部的 SQL 操作
    # 污染迭代状态导致 fetchmany 提前返回空结果。
    iter_cursor = conn.cursor()
    try:
        iter_cursor.execute("SELECT questId FROM quest ORDER BY questId")

        with LightweightProgress(total_quests, desc="阶段 1 回填", unit="quests") as pbar:
            print(f"正在处理 {total_quests} 个任务的阶段 1 回填...")

            while True:
                rows = iter_cursor.fetchmany(max(1, int(chunk_size)))
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
                pbar.update(len(quest_ids))
    finally:
        iter_cursor.close()

    print(f"阶段 1 回填完成：新增 {created_total}，更新 {updated_total}")
    return created_total, updated_total


def _get_textmap_lang_id_map() -> dict[str, int]:
    """构建 TextMap 语言映射。"""
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

    # 为旧数据或异常 langCode 记录补齐默认映射。
    for lang_code, lang_id in LANG_CODE_MAP.items():
        base_name = f"TextMap{lang_code}.json"
        if base_name not in mapping:
            mapping[base_name] = int(lang_id)
            fallback_hits.append(base_name)

    if fallback_hits:
        print(
            "[WARN] 历史回填: langCode 映射不完整，"
            f"已为 {len(fallback_hits)} 项使用兜底 ID。"
        )
    return mapping



def _has_any_version_data(table_name: str) -> bool:
    if not _table_exists(table_name):
        return False

    cursor = conn.cursor()
    try:
        cols = {
            row[1]
            for row in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
            if len(row) > 1
        }
        if table_name == "quest":
            if "created_version_id" not in cols or not _table_exists("quest_version"):
                return False
            quest_version_cols = {
                row[1]
                for row in cursor.execute("PRAGMA table_info(quest_version)").fetchall()
                if len(row) > 1
            }
            if "updated_version_id" not in quest_version_cols:
                return False
            quest_row = cursor.execute(
                "SELECT 1 FROM quest WHERE created_version_id IS NOT NULL LIMIT 1"
            ).fetchone()
            if quest_row is None:
                return False
            quest_version_row = cursor.execute(
                "SELECT 1 FROM quest_version WHERE updated_version_id IS NOT NULL LIMIT 1"
            ).fetchone()
            return quest_version_row is not None
        if table_name == "npc":
            if "created_version_id" not in cols:
                return False
            row = cursor.execute(
                "SELECT 1 FROM npc WHERE created_version_id IS NOT NULL LIMIT 1"
            ).fetchone()
            return row is not None

        if not {"created_version_id", "updated_version_id"}.issubset(cols):
            return False
        row = cursor.execute(
            f"SELECT 1 FROM {table_name} "
            "WHERE created_version_id IS NOT NULL "
            "AND updated_version_id IS NOT NULL "
            "LIMIT 1"
        ).fetchone()
        return row is not None
    except Exception:
        return False
    finally:
        cursor.close()


def _backfill_versions_from_snapshots(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
    include_paths: list[str] | None = None,
    table_name: str,
    meta_commit_key: str,
    meta_title_key: str,
    resume_target_key: str,
    resume_done_key: str,
    process_entry_fn,
    pbar_desc: str,
    commit_batch_size: int = DEFAULT_HISTORY_COMMIT_BATCH_SIZE,
    refresh_version_catalog: bool = True,
):
    """通用版本快照回放入口，按版本快照批次处理指定资源。"""
    commit_batch_size = max(1, int(commit_batch_size))

    try:
        ensure_version_schema()
    except Exception as e:
        logger.error(f"Error ensuring version schema: {e}")
        raise

    repo_path = DATA_PATH
    try:
        replay_range = _resolve_snapshot_replay_range(
            repo_path,
            target_commit=target_commit,
            from_commit=from_commit,
        )
    except Exception as e:
        logger.error(f"Error resolving commits: {e}")
        raise

    target_snapshot = replay_range.target_snapshot
    from_snapshot = replay_range.from_snapshot
    base_snapshot = replay_range.base_snapshot
    snapshots = replay_range.snapshots
    resolved_target = target_snapshot.commit_sha if target_snapshot else replay_range.raw_target_commit
    resolved_from = from_snapshot.commit_sha if from_snapshot else None
    resume_scope = replay_range.resume_scope
    gc_was_enabled = gc.isenabled()
    if gc_was_enabled:
        gc.disable()
        logger.info(f"{table_name} 版本快照回放: 已临时关闭 Python 自动 GC，避免大对象图遍历卡顿")

    try:
        if target_snapshot is None:
            logger.info(
                f"{table_name} 版本快照回放：目标提交 {replay_range.raw_target_commit[:8]} "
                "未映射到任何版本快照，已跳过。"
            )
            return

        if not force and resolved_from is None and get_meta(meta_commit_key) == resolved_target:
            if _has_any_version_data(table_name):
                logger.info(f"{table_name} 版本快照回放已完成于 {target_snapshot.version_tag}，跳过。")
                return
            logger.info(
                f"{table_name} 版本快照回放元数据指向 {target_snapshot.version_tag}，"
                f"但 {table_name} 版本列为空或不完整；将重新执行。"
            )
    except Exception as e:
        logger.error(f"Error checking backfill status: {e}")
    try:
        if not snapshots:
            if from_snapshot and target_snapshot and from_snapshot.version_tag == target_snapshot.version_tag:
                logger.info(
                    f"{table_name} 版本快照回放：起止提交均折算到 {target_snapshot.version_tag}，无需处理。"
                )
            else:
                logger.info(f"{table_name} 版本快照回放：没有需要处理的版本快照。")
            return
        if target_snapshot is None:
            logger.warning(f"{table_name} 版本快照回放：目标快照为空，跳过。")
            return
        total_snapshots = len(snapshots)
        try:
            start_idx = _prepare_resume_for_snapshots(
                resume_target_key=resume_target_key,
                resume_done_key=resume_done_key,
                resolved_target=resume_scope,
                snapshots=snapshots,
                force=force,
                label=f"{table_name} 版本快照回放",
            )
        except Exception as e:
            logger.error(f"Error preparing resume for snapshots: {e}")
            start_idx = 0

        if start_idx == 0:
            if from_snapshot:
                logger.info(
                    f"{table_name} 版本快照回放开始："
                    f"{total_snapshots} 个快照 (起点: {from_snapshot.version_tag}, 目标: {target_snapshot.version_tag})"
                )
            else:
                logger.info(
                    f"{table_name} 版本快照回放开始："
                    f"{total_snapshots} 个快照 (目标: {target_snapshot.version_tag})"
                )
        else:
            logger.info(
                f"{table_name} 版本快照回放继续："
                f"剩余 {total_snapshots - start_idx} / 总计 {total_snapshots} 个快照 "
                f"(目标: {target_snapshot.version_tag})"
            )

        cursor = conn.cursor()
        logger.info(f"正在处理 {total_snapshots} 个 {table_name} 版本快照...")
        last_snapshot: VersionSnapshot | None = None
        processed_snapshots = 0
        try:
            pbar = LightweightProgress(total_snapshots, desc=pbar_desc, unit="snapshots", initial_print=False)
            pbar.current = start_idx
            pbar.update(0)
            with pbar:
                for idx in range(start_idx, total_snapshots):
                    postfix = f"Snapshot {idx}"
                    try:
                        snapshot = snapshots[idx]
                        previous_snapshot = snapshots[idx - 1] if idx > 0 else base_snapshot
                        last_snapshot = snapshot
                        postfix = f"{snapshot.version_tag} ({snapshot.commit_sha[:8]})"

                        try:
                            entries = _snapshot_entries(
                                repo_path,
                                snapshot,
                                previous_snapshot,
                                include_paths=include_paths,
                            )
                        except Exception as e:
                            logger.error(f"Error getting entries for snapshot {snapshot.version_tag}: {e}")
                            pbar.update(postfix=postfix)
                            continue

                        # 批量预取该快照所有条目涉及的文件内容
                        prefetch_paths: dict[str, list[str]] = {}
                        parent_sha = previous_snapshot.commit_sha if previous_snapshot else None
                        for entry in entries:
                            rel = (entry.get("new_path") or entry.get("old_path") or "").replace("\\", "/")
                            if not rel:
                                continue
                            prefetch_paths.setdefault(snapshot.commit_sha, []).append(rel)
                            if parent_sha:
                                prefetch_paths.setdefault(parent_sha, []).append(rel)
                        for pfetch_commit, pfetch_paths in prefetch_paths.items():
                            _prefetch_git_show_texts(repo_path, pfetch_commit, pfetch_paths)

                        entry_errors = 0
                        for entry in entries:
                            try:
                                process_entry_fn(
                                    cursor,
                                    repo_path,
                                    snapshot.commit_sha,
                                    previous_snapshot.commit_sha if previous_snapshot else None,
                                    entry,
                                    snapshot.version_id,
                                    snapshot.version_label,
                                    batch_size,
                                )
                            except Exception as e:
                                logger.error(f"Error processing {table_name} entry: {e}")
                                entry_errors += 1
                        if entry_errors > 0:
                                logger.warning(
                                    f"处理快照 {snapshot.version_tag} ({snapshot.commit_sha[:8]}) "
                                    f"的条目时出现 {entry_errors} 个错误"
                                )

                        pbar.update(postfix=postfix)
                        processed_snapshots += 1

                        if processed_snapshots % commit_batch_size == 0:
                            try:
                                conn.commit()
                                set_meta(resume_target_key, resume_scope)
                                set_meta(resume_done_key, snapshot.commit_sha)
                                logger.debug(f"已批量提交到快照 {snapshot.version_tag} ({snapshot.commit_sha[:8]})")
                            except Exception as e:
                                logger.error(f"Error committing changes for batch: {e}")
                    except Exception as e:
                        logger.error(f"Error processing snapshot {idx}: {e}")
                        pbar.update(postfix=postfix)
                    finally:
                        # Snapshot-scoped caches can balloon during force replay, especially
                        # for TextMap. Drop them eagerly to keep memory bounded.
                        _clear_history_runtime_caches()
                        gc.collect()
                if processed_snapshots > 0 and last_snapshot is not None:
                    try:
                        conn.commit()
                        set_meta(resume_target_key, resume_scope)
                        set_meta(resume_done_key, last_snapshot.commit_sha)
                        logger.info(
                            f"最终快照已完成到 {last_snapshot.version_tag} ({last_snapshot.commit_sha[:8]})"
                        )
                    except Exception as e:
                        logger.error(f"Error finalizing commit: {e}")

        except BaseException:
            conn.rollback()
            logger.error(
                f"{table_name} 版本快照回放被中断；已保存断点，重新运行可继续。",
                exc_info=True
            )
            if last_snapshot is not None:
                try:
                    set_meta(resume_target_key, resume_scope)
                    set_meta(resume_done_key, last_snapshot.commit_sha)
                    logger.info(
                        f"已在快照 {last_snapshot.version_tag} ({last_snapshot.commit_sha[:8]}) 保存断点"
                    )
                except Exception as save_error:
                    logger.error(f"Error saving checkpoint: {save_error}")
            raise
        finally:
            try:
                cursor.close()
            except Exception as e:
                logger.error(f"Error closing cursor: {e}")

        try:
            if resolved_from is None:
                set_meta(meta_commit_key, resolved_target)
                set_meta(meta_title_key, target_snapshot.version_label)
            set_meta(resume_target_key, "")
            set_meta(resume_done_key, "")
            if refresh_version_catalog:
                rebuild_version_catalog([table_name])
        except Exception as e:
            logger.error(f"Error finalizing metadata: {e}")

        _log_git_show_text_cache_stats()
    except Exception as e:
        logger.error(f"{table_name} 版本快照回放失败: {e}", exc_info=True)
        raise
    finally:
        if gc_was_enabled:
            collected = gc.collect()
            gc.enable()
            logger.info(f"{table_name} 版本快照回放: 已恢复 Python 自动 GC，手动回收 {collected} 个对象")


def _backfill_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
    include_paths: list[str] | None = None,
    table_name: str,
    meta_commit_key: str,
    meta_title_key: str,
    resume_target_key: str,
    resume_done_key: str,
    process_entry_fn,
    pbar_desc: str,
    commit_batch_size: int = DEFAULT_HISTORY_COMMIT_BATCH_SIZE,
    refresh_version_catalog: bool = True,
):
    """兼容旧入口名，内部改走版本快照回放。"""
    _backfill_versions_from_snapshots(
        target_commit=target_commit,
        from_commit=from_commit,
        force=force,
        batch_size=batch_size,
        verbose=verbose,
        include_paths=include_paths,
        table_name=table_name,
        meta_commit_key=meta_commit_key,
        meta_title_key=meta_title_key,
        resume_target_key=resume_target_key,
        resume_done_key=resume_done_key,
        process_entry_fn=process_entry_fn,
        pbar_desc=pbar_desc,
        commit_batch_size=commit_batch_size,
        refresh_version_catalog=refresh_version_catalog,
    )


def backfill_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    fast_db_write: bool = False,
    verbose: bool = False,
):
    """执行全量历史版本回填。"""
    try:
        ensure_version_schema()
    except Exception as e:
        logger.error(f"Error ensuring version schema: {e}")
        raise

    repo_path = DATA_PATH
    try:
        replay_range = _resolve_snapshot_replay_range(
            repo_path,
            target_commit=target_commit,
            from_commit=from_commit,
        )
        target_snapshot = replay_range.target_snapshot
        from_snapshot = replay_range.from_snapshot
        if target_snapshot is None:
            logger.info(
                f"版本快照回放：目标提交 {replay_range.raw_target_commit[:8]} 未映射到任何版本快照，已跳过。"
            )
            return
        resolved_target = target_snapshot.commit_sha if target_snapshot else replay_range.raw_target_commit
        resolved_from = from_snapshot.commit_sha if from_snapshot else None
        resume_target_key = "db_history_versions_commit_resume_target"
        resume_done_key = "db_history_versions_commit_resume_done"
    except Exception as e:
        logger.error(f"Error resolving commits: {e}")
        raise

    try:
        if not force and resolved_from is None and get_meta("db_history_versions_commit") == resolved_target:
            version_tables = ("textMap", "readable", "subtitle", "quest", "npc")
            try:
                if all(_has_any_version_data(t) for t in version_tables):
                    logger.info(
                        f"版本快照回放已完成于 "
                        f"{target_snapshot.version_tag if target_snapshot else resolved_target}，跳过。"
                    )
                    return
            except Exception as e:
                logger.error(f"Error checking version data: {e}")
            logger.info(
                f"版本快照回放元数据指向 "
                f"{target_snapshot.version_tag if target_snapshot else resolved_target}，"
                "但版本列为空或不完整；将重新执行。"
            )
    except Exception as e:
        logger.error(f"Error checking backfill status: {e}")
    backfill_functions = [
        ("TextMap", backfill_textmap_versions_from_history),
        ("Readable", backfill_readable_versions_from_history),
        ("Subtitle", backfill_subtitle_versions_from_history),
        ("Npc", backfill_npc_versions_from_history),
        ("Quest", backfill_quest_versions_from_history),
    ]

    # 预留顶层批量写入开关，供后续调优使用。
    if fast_db_write:
        logger.info("版本快照回放：启用快速 SQLite pragma 以加速批量写入")
    with fast_import_pragmas(conn, enabled=fast_db_write):
        for name, func in backfill_functions:
            try:
                logger.info(f"开始执行 {name} 版本快照回放...")
                with _suspend_automatic_gc(f"{name} 版本快照回放"):
                    func(
                        target_commit=target_commit,
                        from_commit=from_commit,
                        force=force,
                        batch_size=batch_size,
                        verbose=verbose,
                    )
                logger.info(f"{name} 版本快照回放完成")
            except Exception as e:
                logger.error(f"{name} 版本快照回放失败: {e}", exc_info=True)
            finally:
                _clear_history_runtime_caches(
                    clear_git_command_cache=True,
                    clear_snapshot_metadata=True,
                    clear_repo_cache=True,
                )
                gc.collect()
    try:
        if resolved_from is None:
            set_meta("db_history_versions_commit", resolved_target)
        set_meta(resume_target_key, "")
        set_meta(resume_done_key, "")
        rebuild_version_catalog(["textMap", "quest", "subtitle", "readable", "npc"])
    except Exception as e:
        logger.error(f"Error finalizing metadata: {e}")
    logger.info("版本快照回放元数据已刷新，版本目录已重建")
    cursor = conn.cursor()
    try:
        try:
            readable_exceptions = analyze_readable_version_exceptions(cursor)
            report_version_exceptions(readable_exceptions, "Readable")
        except Exception as e:
            logger.error(f"Error analyzing Readable version exceptions: {e}")

        try:
            subtitle_exceptions = analyze_subtitle_version_exceptions(cursor)
            report_version_exceptions(subtitle_exceptions, "Subtitle")
        except Exception as e:
            logger.error(f"Error analyzing Subtitle version exceptions: {e}")
    finally:
        try:
            cursor.close()
        except Exception as e:
            logger.error(f"Error closing cursor: {e}")

    logger.info(
        f"版本快照回放完成于 "
        f"{target_snapshot.version_tag if target_snapshot else resolved_target}"
    )


def backfill_textmap_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
    refresh_version_catalog: bool = True,
):
    """回填 TextMap 历史版本。"""
    textmap_lang_map = _get_textmap_lang_id_map()
    local_textmap_cache: dict[str, dict[str, object] | None] = {}
    textmap_version_cache: dict[str, dict[int, tuple[int | None, int | None]]] = {}
    processed_snapshot_groups: set[tuple[str, str]] = set()

    def process_textmap_entry(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size):
        """处理单个 TextMap 版本快照条目。"""
        old_path = entry.get("old_path")
        new_path = entry.get("new_path")
        rel_path = (new_path or old_path or "").replace("\\", "/")
        if not rel_path:
            return
        if not (rel_path.startswith("TextMap/") and rel_path.endswith(".json")):
            return

        file_name = rel_path.split("/", 1)[1]
        parsed = parse_textmap_file_name(file_name)
        if parsed is None:
            return
        base_name, _split_part = parsed
        lang_id = textmap_lang_map.get(base_name)
        if lang_id is None:
            return
        processed_key = (commit_sha, base_name)
        if processed_key in processed_snapshot_groups:
            return
        processed_snapshot_groups.add(processed_key)

        snapshot_obj = _load_snapshot_textmap_group(repo_path, commit_sha, base_name)
        if snapshot_obj is None:
            snapshot_obj = {}

        if base_name not in local_textmap_cache:
            local_textmap_cache[base_name] = _load_worktree_textmap_group(repo_path, base_name)
        current_obj = local_textmap_cache[base_name]
        if not isinstance(current_obj, dict):
            return

        if base_name not in textmap_version_cache:
            textmap_version_cache[base_name] = _load_textmap_version_cache_for_current_group(
                cursor,
                lang_id=lang_id,
                current_obj=current_obj,
                batch_size=batch_size,
            )
        existing_map = textmap_version_cache[base_name]

        previous_snapshot_obj = None
        if parent_sha and _textmap_snapshot_has_current_matches(snapshot_obj, current_obj):
            previous_snapshot_obj = _load_snapshot_textmap_group(repo_path, parent_sha, base_name)

        update_rows = _build_textmap_history_update_rows(
            snapshot_obj=snapshot_obj,
            previous_snapshot_obj=previous_snapshot_obj,
            current_obj=current_obj,
            lang_id=lang_id,
            version_id=version_id,
            existing_map=existing_map,
        )

        if update_rows:
            executemany_batched(
                cursor,
                "UPDATE textMap SET created_version_id = ?, updated_version_id = ? "
                "WHERE lang = ? AND hash = ?",
                update_rows,
                batch_size=batch_size,
            )
            _merge_textmap_version_updates_into_cache(existing_map, update_rows)

    _backfill_versions_from_history(
        target_commit=target_commit,
        from_commit=from_commit,
        force=force,
        batch_size=batch_size,
        verbose=verbose,
        include_paths=TEXTMAP_ONLY_PATHS,
        table_name="textMap",
        meta_commit_key="db_history_versions_commit_textmap",
        meta_title_key="db_history_versions_commit_title_textmap",
        resume_target_key="db_history_versions_commit_textmap_resume_target",
        resume_done_key="db_history_versions_commit_textmap_resume_done",
        process_entry_fn=process_textmap_entry,
        pbar_desc="TextMap backfill",
        commit_batch_size=5,
        refresh_version_catalog=refresh_version_catalog,
    )

    print("TextMap history phase-1.5: Git history backfill for textmap without version data")
    cursor = conn.cursor()
    try:
        _backfill_textmap_git_versions(cursor, textmap_lang_map)
    finally:
        cursor.close()

    check_cursor = conn.cursor()
    try:
        exception_data = analyze_textmap_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "TextMap")

        if exception_data['created_after_updated'] > 0:
            print(f"Fixing {exception_data['created_after_updated']} TextMap rows where created_version_id is later than updated_version_id...")
            fixed = fix_created_after_updated_versions(check_cursor, "textMap", ["lang", "hash"])
            conn.commit()
            print(f"TextMap exception fix complete: {fixed} row(s) updated.")
    finally:
        check_cursor.close()


def backfill_readable_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
    refresh_version_catalog: bool = True,
):
    """回填 Readable 历史版本。"""

    local_text_cache: dict[str, str | None] = {}

    def process_readable_entry(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size):
        """处理单个 Readable 版本快照条目。"""
        action = entry["action"]
        old_path = entry.get("old_path")
        new_path = entry.get("new_path")
        rel_path = (new_path or old_path or "").replace("\\", "/")
        if not rel_path:
            return
        if not rel_path.startswith("Readable/"):
            return
        parts = rel_path.split("/", 2)
        if len(parts) < 3:
            return
        lang = parts[1]
        full_path = os.path.join(repo_path, rel_path)
        lang_path = os.path.join(repo_path, "Readable", lang)
        rel_path_file = os.path.relpath(full_path, lang_path)
        clean_file_name = rel_path_file.replace(os.sep, "/")

        new_text = _git_show_text(repo_path, commit_sha, rel_path) if action != "D" else None

        current_rel_path = f"Readable/{lang}/{clean_file_name}"
        if current_rel_path not in local_text_cache:
            current_text = _read_worktree_text(repo_path, current_rel_path)
            local_text_cache[current_rel_path] = (
                _normalize_text_for_compare(current_text)
                if current_text is not None
                else None
            )
        local_text = local_text_cache[current_rel_path]
        if local_text is None or new_text is None:
            return

        cursor.execute(
            "SELECT created_version_id, updated_version_id FROM readable WHERE fileName=? AND lang=?",
            (clean_file_name, lang),
        )
        version_info = cursor.fetchone()
        if version_info is None:
            return
        existing_created_version, existing_updated_version = version_info

        previous_text = _git_show_text(repo_path, parent_sha, rel_path) if parent_sha else None
        was_alive = previous_text is not None
        created_version = version_id if not was_alive else existing_created_version
        updated_version = existing_updated_version

        if was_alive and should_update_version(existing_created_version, version_id, is_created=True):
            created_version = version_id

        if _normalize_text_for_compare(new_text) == local_text:
            if not was_alive:
                updated_version = version_id
            elif (
                existing_updated_version is None
                or should_update_version(existing_updated_version, version_id, is_created=True)
            ):
                updated_version = version_id
        if (
            created_version != existing_created_version
            or updated_version != existing_updated_version
        ):
            cursor.execute(
                "UPDATE readable SET created_version_id = ?, updated_version_id = ? WHERE fileName = ? AND lang = ?",
                (created_version, updated_version, clean_file_name, lang),
            )

    _backfill_versions_from_history(
        target_commit=target_commit,
        from_commit=from_commit,
        force=force,
        batch_size=batch_size,
        verbose=verbose,
        include_paths=READABLE_ONLY_PATHS,
        table_name="readable",
        meta_commit_key="db_history_versions_commit_readable",
        meta_title_key="db_history_versions_commit_title_readable",
        resume_target_key="db_history_versions_commit_readable_resume_target",
        resume_done_key="db_history_versions_commit_readable_resume_done",
        process_entry_fn=process_readable_entry,
        pbar_desc="Readable backfill",
        commit_batch_size=10,
        refresh_version_catalog=refresh_version_catalog,
    )

    print("Readable 历史阶段 1.5：为缺少版本数据的 Readable 执行 Git 回溯")
    cursor = conn.cursor()
    try:
        _backfill_git_versions(
            cursor,
            "Readable",
            "SELECT fileName, lang FROM readable WHERE created_version_id IS NULL",
            _build_readable_record_rel_path,
            "UPDATE readable SET created_version_id = ?, updated_version_id = ? WHERE fileName = ? AND lang = ?",
            "Readable Git 回溯",
        )
    finally:
        cursor.close()

    check_cursor = conn.cursor()
    try:
        exception_data = analyze_readable_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "Readable")

        if exception_data['created_after_updated'] > 0:
            print(f"正在修复 {exception_data['created_after_updated']} 条 created_version_id 晚于 updated_version_id 的 Readable 记录...")
            fixed = fix_created_after_updated_versions(check_cursor, "readable", ["fileName", "lang"])
            conn.commit()
            print(f"Readable 异常修复完成：更新 {fixed} 条。")
    finally:
        check_cursor.close()


def backfill_subtitle_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
    refresh_version_catalog: bool = True,
):
    """回填 Subtitle 历史版本。"""
    local_rows_cache: dict[str, dict[str, str] | None] = {}

    def process_subtitle_entry(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size):
        """处理单个 Subtitle 版本快照条目。"""
        action = entry["action"]
        old_path = entry.get("old_path")
        new_path = entry.get("new_path")
        rel_path = (new_path or old_path or "").replace("\\", "/")
        parsed = normalize_subtitle_rel_path(rel_path)
        if parsed is None:
            return
        lang_name, lang_id, clean_file_name = parsed
        rel_under_lang = f"{clean_file_name}.srt"
        new_text = _git_show_text(repo_path, commit_sha, rel_path) if action != "D" else None
        history_rows = _parse_srt_rows(new_text, lang_id, rel_under_lang) if new_text is not None else {}
        current_rel_path = build_subtitle_rel_path(clean_file_name, lang_name)
        if current_rel_path is None:
            return
        if current_rel_path not in local_rows_cache:
            current_text = _read_worktree_text(repo_path, current_rel_path)
            local_rows_cache[current_rel_path] = (
                _parse_srt_rows(current_text, lang_id, rel_under_lang)
                if current_text is not None
                else None
            )
        current_rows = local_rows_cache[current_rel_path]
        if current_rows is None:
            return

        matching_current_keys = set(current_rows.keys())
        for changed_key in _subtitle_text_changed_keys(history_rows, current_rows):
            matching_current_keys.discard(changed_key)

        cursor.execute(
            "SELECT subtitleKey, created_version_id, updated_version_id "
            "FROM subtitle WHERE fileName=? AND lang=?",
            (clean_file_name, lang_id),
        )
        version_rows = cursor.fetchall()
        if not version_rows:
            return

        update_rows = []
        for subtitle_key, existing_created_version, existing_updated_version in version_rows:
            created_version = existing_created_version
            updated_version = existing_updated_version

            if should_update_version(existing_created_version, version_id, is_created=True):
                created_version = version_id

            if subtitle_key in matching_current_keys and (
                existing_updated_version is None
                or should_update_version(existing_updated_version, version_id, is_created=True)
            ):
                updated_version = version_id

            if (
                created_version != existing_created_version
                or updated_version != existing_updated_version
            ):
                update_rows.append((created_version, updated_version, subtitle_key))

        if update_rows:
            executemany_batched(
                cursor,
                "UPDATE subtitle SET created_version_id = ?, updated_version_id = ? WHERE subtitleKey = ?",
                update_rows,
                batch_size=batch_size,
            )

    _backfill_versions_from_history(
        target_commit=target_commit,
        from_commit=from_commit,
        force=force,
        batch_size=batch_size,
        verbose=verbose,
        include_paths=SUBTITLE_ONLY_PATHS,
        table_name="subtitle",
        meta_commit_key="db_history_versions_commit_subtitle",
        meta_title_key="db_history_versions_commit_title_subtitle",
        resume_target_key="db_history_versions_commit_subtitle_resume_target",
        resume_done_key="db_history_versions_commit_subtitle_resume_done",
        process_entry_fn=process_subtitle_entry,
        pbar_desc="Subtitle backfill",
        commit_batch_size=10,
        refresh_version_catalog=refresh_version_catalog,
    )

    print("Subtitle 历史阶段 1.5：为缺少版本数据的 Subtitle 执行 Git 回溯")
    cursor = conn.cursor()
    try:
        _backfill_git_versions(
            cursor,
            "Subtitle",
            "SELECT subtitleKey FROM subtitle WHERE created_version_id IS NULL",
            _build_subtitle_record_rel_path,
            "UPDATE subtitle SET created_version_id = ?, updated_version_id = ? WHERE subtitleKey = ?",
            "Subtitle Git 回溯",
        )
    finally:
        cursor.close()

    check_cursor = conn.cursor()
    try:
        exception_data = analyze_subtitle_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "Subtitle")

        if exception_data['created_after_updated'] > 0:
            print(f"正在修复 {exception_data['created_after_updated']} 条 created_version_id 晚于 updated_version_id 的 Subtitle 记录...")
            fixed = fix_created_after_updated_versions(check_cursor, "subtitle", ["subtitleKey"])
            conn.commit()
            print(f"Subtitle 异常修复完成：更新 {fixed} 条。")
    finally:
        check_cursor.close()


def backfill_npc_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
    refresh_version_catalog: bool = True,
):
    """回填 NPC 在 NpcExcelConfigData.json 中首次出现的版本。"""
    current_npc_ids: set[int] | None = None

    def _get_current_npc_ids() -> set[int]:
        nonlocal current_npc_ids
        if current_npc_ids is not None:
            return current_npc_ids
        cursor = conn.cursor()
        try:
            rows = cursor.execute("SELECT npcId FROM npc").fetchall()
        finally:
            cursor.close()
        current_npc_ids = {int(row[0]) for row in rows if row and row[0] is not None}
        return current_npc_ids

    def process_npc_entry(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size):
        action = entry["action"]
        old_path = entry.get("old_path")
        new_path = entry.get("new_path")
        rel_path = (new_path or old_path or "").replace("\\", "/")
        if rel_path != "ExcelBinOutput/NpcExcelConfigData.json":
            return

        history_rows = _git_show_json(repo_path, commit_sha, rel_path) if action != "D" else []
        if not isinstance(history_rows, list):
            history_rows = []

        valid_ids = _get_current_npc_ids()
        if not valid_ids or version_id is None:
            return

        current_snapshot_ids: set[int] = set()
        previous_snapshot_ids: set[int] = set()
        seen_ids: set[int] = set()
        for row in history_rows:
            if not isinstance(row, dict):
                continue
            raw_npc_id = row.get("id")
            if raw_npc_id is None:
                continue
            try:
                npc_id = int(raw_npc_id)
            except Exception:
                continue
            if npc_id not in valid_ids or npc_id in seen_ids:
                continue
            seen_ids.add(npc_id)
            current_snapshot_ids.add(npc_id)

        previous_rows = _git_show_json(repo_path, parent_sha, rel_path) if parent_sha else []
        seen_ids.clear()
        if isinstance(previous_rows, list):
            for row in previous_rows:
                if not isinstance(row, dict):
                    continue
                raw_npc_id = row.get("id")
                if raw_npc_id is None:
                    continue
                try:
                    npc_id = int(raw_npc_id)
                except Exception:
                    continue
                if npc_id not in valid_ids or npc_id in seen_ids:
                    continue
                seen_ids.add(npc_id)
                previous_snapshot_ids.add(npc_id)

        candidate_ids = sorted(current_snapshot_ids - previous_snapshot_ids)
        if not candidate_ids:
            return

        chunk_size = max(1, int(batch_size))
        update_rows: list[tuple[int, int]] = []
        for idx in range(0, len(candidate_ids), chunk_size):
            chunk = candidate_ids[idx : idx + chunk_size]
            placeholders = ",".join(["?"] * len(chunk))
            rows = cursor.execute(
                f"SELECT npcId, created_version_id FROM npc WHERE npcId IN ({placeholders})",
                chunk,
            ).fetchall()
            for npc_id, existing_created_version in rows:
                if existing_created_version != version_id:
                    update_rows.append((version_id, int(npc_id)))

        if update_rows:
            executemany_batched(
                cursor,
                "UPDATE npc SET created_version_id = ? WHERE npcId = ?",
                update_rows,
                batch_size=batch_size,
            )

    _backfill_versions_from_history(
        target_commit=target_commit,
        from_commit=from_commit,
        force=force,
        batch_size=batch_size,
        verbose=verbose,
        include_paths=["ExcelBinOutput/NpcExcelConfigData.json"],
        table_name="npc",
        meta_commit_key="db_history_versions_commit_npc",
        meta_title_key="db_history_versions_commit_title_npc",
        resume_target_key="db_history_versions_commit_npc_resume_target",
        resume_done_key="db_history_versions_commit_npc_resume_done",
        process_entry_fn=process_npc_entry,
        pbar_desc="NPC backfill",
        commit_batch_size=10,
        refresh_version_catalog=refresh_version_catalog,
    )


def fix_created_after_updated_versions(
    cursor,
    table_name: str,
    id_columns: list[str],
    max_fixes: int = 100000
) -> int:
    """Fix rows where created_version_id is later than updated_version_id."""
    where_clause = " AND ".join([f"{col} = ?" for col in id_columns])

    mismatch_predicate = (
        "created_version_id IS NOT NULL "
        "AND updated_version_id IS NOT NULL "
        f"AND {_version_precedes_sql('updated_version_id', 'created_version_id')}"
    )
    update_sql = (
        f"UPDATE {table_name} "
        f"SET created_version_id = updated_version_id "
        f"WHERE {where_clause} "
        f"AND {mismatch_predicate}"
    )

    select_sql = (
        f"SELECT {', '.join(id_columns)} "
        f"FROM {table_name} "
        f"WHERE {mismatch_predicate}"
    )

    if max_fixes <= 0:
        return 0

    count_row = cursor.execute(
        f"SELECT COUNT(*) FROM {table_name} WHERE {mismatch_predicate}"
    ).fetchone()
    mismatch_count = int(count_row[0]) if count_row and count_row[0] is not None else 0
    if mismatch_count == 0:
        return 0
    if mismatch_count <= max_fixes:
        cursor.execute(
            f"UPDATE {table_name} "
            f"SET created_version_id = updated_version_id "
            f"WHERE {mismatch_predicate}"
        )
        return mismatch_count

    cursor.execute(f"{select_sql} LIMIT ?", (max_fixes,))
    records = cursor.fetchall()

    fixed_count = 0
    for record in records:
        if fixed_count >= max_fixes:
            break

        cursor.execute(update_sql, tuple(record[: len(id_columns)]))
        if cursor.rowcount > 0:
            fixed_count += 1

    return fixed_count


def validate_quest_versions(
    *,
    repo_path: str | None = None,
    fix: bool = False,
    max_fixes: int = 100000,
    fix_commit_batch_size: int = DEFAULT_FIX_COMMIT_BATCH_SIZE,
    db_conn=None,
) -> dict[str, int]:
    """Validate quest version anomalies and optionally repair them."""
    ensure_version_schema()
    if repo_path is None:
        repo_path = DATA_PATH

    use_conn = db_conn if db_conn else conn
    cursor = use_conn.cursor()
    try:
        cursor.execute(
            "SELECT questId, created_version_id, git_created_version_id FROM quest WHERE created_version_id IS NULL"
        )
        no_created_version = cursor.fetchall()

        cursor.execute(
            "SELECT questId, created_version_id, git_created_version_id FROM quest WHERE git_created_version_id IS NULL"
        )
        no_git_version = cursor.fetchall()

        cursor.execute(
            "SELECT questId, created_version_id, git_created_version_id FROM quest WHERE created_version_id <= 0 OR git_created_version_id <= 0"
        )
        invalid_version = cursor.fetchall()

        cursor.execute(
            "SELECT qv.questId, qv.lang, qv.updated_version_id FROM quest_version qv LEFT JOIN quest q ON qv.questId = q.questId WHERE q.questId IS NULL"
        )
        quest_version_no_quest = cursor.fetchall()

        cursor.execute(
            "SELECT questId, lang, updated_version_id FROM quest_version WHERE updated_version_id IS NULL"
        )
        quest_version_no_updated = cursor.fetchall()

        cursor.execute(
            "SELECT questId, lang, updated_version_id FROM quest_version WHERE updated_version_id <= 0"
        )
        quest_version_invalid = cursor.fetchall()

        cursor.execute(
            "SELECT questId, MIN(updated_version_id) as min_updated_version FROM quest_version GROUP BY questId"
        )
        min_updated_versions = {row[0]: row[1] for row in cursor.fetchall()}

        quest_created_versions = {
            row[0]: row[1]
            for row in cursor.execute(
                "SELECT questId, created_version_id FROM quest WHERE created_version_id IS NOT NULL"
            ).fetchall()
        }
        quest_version_older = []
        for quest_id, min_updated in min_updated_versions.items():
            current_created_version = quest_created_versions.get(quest_id)
            if current_created_version is not None and should_update_version(min_updated, current_created_version, is_created=False):
                quest_version_older.append((quest_id, None, min_updated, current_created_version))

        total_abnormal = (
            len(no_created_version)
            + len(no_git_version)
            + len(invalid_version)
            + len(quest_version_no_quest)
            + len(quest_version_no_updated)
            + len(quest_version_invalid)
            + len(quest_version_older)
        )

        print("Quest version validation summary:")
        print(f"- Missing created version: {len(no_created_version)}")
        print(f"- Missing Git version: {len(no_git_version)}")
        print(f"- Invalid version values: {len(invalid_version)}")
        print(f"- quest_version rows without quest: {len(quest_version_no_quest)}")
        print(f"- quest_version rows without updated_version_id: {len(quest_version_no_updated)}")
        print(f"- quest_version rows with invalid updated_version_id: {len(quest_version_invalid)}")
        print(f"- quest rows older than quest_version minimum update: {len(quest_version_older)}")
        print(f"- Total anomalies: {total_abnormal}")

        fixed_count = 0
        if fix:
            print("开始修复任务版本异常...")
            processed = 0
            fix_commit_batch_size = max(1, int(fix_commit_batch_size))

            all_tasks = []
            all_tasks.extend([("no_created_version", quest) for quest in no_created_version])
            all_tasks.extend([("no_git_version", quest) for quest in no_git_version])
            all_tasks.extend([("invalid_version", quest) for quest in invalid_version])
            all_tasks.extend([("quest_version_no_quest", quest) for quest in quest_version_no_quest])
            all_tasks.extend([("quest_version_no_updated", quest) for quest in quest_version_no_updated])
            all_tasks.extend([("quest_version_invalid", quest) for quest in quest_version_invalid])
            all_tasks.extend([("quest_version_older", quest) for quest in quest_version_older])

            total_tasks = len(all_tasks)
            pending_writes = 0

            def _flush_fix_batch():
                nonlocal pending_writes
                if pending_writes > 0:
                    use_conn.commit()
                    pending_writes = 0

            def _mark_fix_write():
                nonlocal pending_writes
                pending_writes += 1
                if pending_writes >= fix_commit_batch_size:
                    _flush_fix_batch()

            def _resolve_quest_first_version_id(quest_id: int) -> int | None:
                source_type, source_code_raw = _get_quest_source_fields(cursor, int(quest_id))
                if source_type == SOURCE_TYPE_ANECDOTE:
                    first_commit = _find_anecdote_first_commit(repo_path, int(quest_id))
                elif source_type == SOURCE_TYPE_HANGOUT and source_code_raw == SOURCE_TYPE_HANGOUT:
                    first_commit = _find_hangout_first_commit(repo_path, int(quest_id))
                else:
                    first_commit = None
                if first_commit:
                    _label, version_id = _resolve_commit_version(
                        repo_path,
                        first_commit,
                        _resolve_commit_title(repo_path, first_commit),
                    )
                    return version_id
                if source_type == SOURCE_TYPE_HANGOUT and source_code_raw == SOURCE_TYPE_HANGOUT:
                    return None
                _first_commit, version_id = _resolve_first_version_for_path(
                    repo_path,
                    f"BinOutput/Quest/{quest_id}.json",
                )
                return version_id

            print(f"Tasks queued for fixes: {total_tasks}")
            with LightweightProgress(min(total_tasks, max_fixes), desc="Quest version fix", unit="task") as pbar:
                for task_type, quest in all_tasks:
                    if processed >= max_fixes:
                        break

                    if task_type == "no_created_version":
                        quest_id, _created_version, git_version = quest
                        if git_version:
                            cursor.execute(
                                "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                                (git_version, quest_id)
                            )
                            fixed_count += 1
                            processed += 1
                            _mark_fix_write()
                            pbar.update(1)
                        else:
                            version_id = _resolve_quest_first_version_id(quest_id)
                            if version_id:
                                cursor.execute(
                                    "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                                    (version_id, quest_id)
                                )
                                fixed_count += 1
                                processed += 1
                                _mark_fix_write()
                                pbar.update(1)

                    elif task_type == "no_git_version":
                        quest_id, _created_version, _git_version = quest
                        version_id = _resolve_quest_first_version_id(quest_id)
                        if version_id:
                            cursor.execute(
                                "UPDATE quest SET git_created_version_id = ? WHERE questId = ?",
                                (version_id, quest_id)
                            )
                            fixed_count += 1
                            processed += 1
                            _mark_fix_write()
                            pbar.update(1)

                    elif task_type == "quest_version_no_quest":
                        quest_id, lang, _updated_version = quest
                        cursor.execute(
                            "DELETE FROM quest_version WHERE questId = ? AND lang = ?",
                            (quest_id, lang)
                        )
                        fixed_count += 1
                        processed += 1
                        _mark_fix_write()
                        pbar.update(1)

                    elif task_type == "quest_version_no_updated":
                        quest_id, lang, _updated_version = quest
                        cursor.execute(
                            "DELETE FROM quest_version WHERE questId = ? AND lang = ?",
                            (quest_id, lang)
                        )
                        fixed_count += 1
                        processed += 1
                        _mark_fix_write()
                        pbar.update(1)

                    elif task_type == "invalid_version":
                        quest_id, created_version, git_version = quest
                        version_id = _resolve_quest_first_version_id(quest_id)
                        if version_id and version_id > 0:
                            if created_version <= 0:
                                cursor.execute(
                                    "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                                    (version_id, quest_id)
                                )
                            if git_version <= 0:
                                cursor.execute(
                                    "UPDATE quest SET git_created_version_id = ? WHERE questId = ?",
                                    (version_id, quest_id)
                                )
                            fixed_count += 1
                            processed += 1
                            _mark_fix_write()
                            pbar.update(1)

                    elif task_type == "quest_version_invalid":
                        quest_id, lang, _updated_version = quest
                        cursor.execute(
                            "DELETE FROM quest_version WHERE questId = ? AND lang = ?",
                            (quest_id, lang)
                        )
                        fixed_count += 1
                        processed += 1
                        _mark_fix_write()
                        pbar.update(1)

                    elif task_type == "quest_version_older":
                        quest_id, _lang, min_updated_version, _current_created_version = quest
                        cursor.execute(
                            "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                            (min_updated_version, quest_id)
                        )
                        fixed_count += 1
                        processed += 1
                        _mark_fix_write()
                        pbar.update(1)

            print(f"Processed {processed} anomaly task(s); fixed {fixed_count} row(s).")
            _flush_fix_batch()
            synced_created_from_git = _sync_created_version_from_git(cursor)
            if synced_created_from_git > 0:
                use_conn.commit()
                fixed_count += synced_created_from_git
                print(
                    "Synchronized created_version_id from git_created_version_id "
                    f"for {synced_created_from_git} quest row(s)."
                )
            if processed >= max_fixes:
                print(f"Reached max_fixes limit: {max_fixes}")

        return {
            "total_abnormal": total_abnormal,
            "no_created_version": len(no_created_version),
            "no_git_version": len(no_git_version),
            "invalid_version": len(invalid_version),
            "quest_version_no_quest": len(quest_version_no_quest),
            "quest_version_no_updated": len(quest_version_no_updated),
            "quest_version_invalid": len(quest_version_invalid),
            "quest_version_older": len(quest_version_older),
            "fixed_count": fixed_count if fix else 0,
        }
    except Exception as e:
        logger.error(f"校验任务版本失败: {e}")
        use_conn.rollback()
        return {
            "total_abnormal": 0,
            "no_created_version": 0,
            "no_git_version": 0,
            "invalid_version": 0,
            "quest_version_no_quest": 0,
            "quest_version_no_updated": 0,
            "quest_version_invalid": 0,
            "quest_version_older": 0,
            "fixed_count": 0,
        }
    finally:
        cursor.close()


def backfill_quest_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    unresolved_ratio_threshold: float = 0.05,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
    refresh_version_catalog: bool = True,
) -> dict[str, int | str]:
    """执行任务版本快照回放。"""
    ensure_version_schema()
    repo_path = DATA_PATH
    replay_range = _resolve_snapshot_replay_range(
        repo_path,
        target_commit=target_commit,
        from_commit=from_commit,
    )
    target_snapshot = replay_range.target_snapshot
    from_snapshot = replay_range.from_snapshot
    base_snapshot = replay_range.base_snapshot
    snapshots = replay_range.snapshots
    resolved_target = target_snapshot.commit_sha if target_snapshot else replay_range.raw_target_commit
    resolved_from = from_snapshot.commit_sha if from_snapshot else None
    resume_scope = replay_range.resume_scope
    meta_commit_key = "db_history_versions_commit_quest"
    meta_title_key = "db_history_versions_commit_title_quest"
    resume_target_key = "db_history_versions_commit_quest_resume_target"
    resume_done_key = "db_history_versions_commit_quest_resume_done"

    if target_snapshot is None:
        print(
            f"任务版本快照回放：目标提交 {replay_range.raw_target_commit[:8]} 未映射到任何版本，跳过。"
        )
        return {
            "replay_mode": "none",
            "total_quests": 0,
            "unresolved_quests": 0,
            "phase1_created_backfilled": 0,
            "phase1_updated_backfilled": 0,
            "phase2_commit_created_backfilled": 0,
        }

    if not force and resolved_from is None and get_meta(meta_commit_key) == resolved_target:
        if _has_any_version_data("quest"):
            print(f"任务版本快照回放已完成于 {target_snapshot.version_tag}，跳过。")
            return {
                "replay_mode": "skip",
                "total_quests": 0,
                "unresolved_quests": 0,
                "phase1_created_backfilled": 0,
                "phase1_updated_backfilled": 0,
                "phase2_commit_created_backfilled": 0,
            }
        print(
            f"任务版本快照回放元数据指向 {target_snapshot.version_tag}，"
            "但任务版本数据为空或不完整；将重新执行。"
        )

    if not snapshots:
        if from_snapshot and from_snapshot.version_tag == target_snapshot.version_tag:
            print(f"任务版本快照回放：起止提交均折算到 {target_snapshot.version_tag}，跳过。")
        else:
            print("任务版本快照回放：没有需要处理的版本快照。")
        return {
            "replay_mode": "none",
            "total_quests": 0,
            "unresolved_quests": 0,
            "phase1_created_backfilled": 0,
            "phase1_updated_backfilled": 0,
            "phase2_commit_created_backfilled": 0,
        }
    total_snapshots = len(snapshots)
    start_idx = _prepare_resume_for_snapshots(
        resume_target_key=resume_target_key,
        resume_done_key=resume_done_key,
        resolved_target=resume_scope,
        snapshots=snapshots,
        force=force,
        label="任务版本快照回放",
    )

    if start_idx == 0:
        if from_snapshot:
            print(
                "任务版本快照回放开始："
                f"{total_snapshots} 个快照 (起点: {from_snapshot.version_tag}, 目标: {target_snapshot.version_tag})"
            )
        else:
            print(
                "任务版本快照回放开始："
                f"{total_snapshots} 个快照 (目标: {target_snapshot.version_tag})"
            )
    else:
        print(
            "任务版本快照回放继续："
            f"剩余 {total_snapshots - start_idx} / 总计 {total_snapshots} 个快照 "
            f"(目标: {target_snapshot.version_tag})"
        )

    cursor = conn.cursor()
    phase2_commit_created_backfilled = 0
    replay_mode = "none"
    prefilled_created_rows = 0
    prefilled_updated_rows = 0
    final_total = 0
    final_unresolved_count = 0

    try:
        refreshed_qhm = _refresh_all_quest_hash_map(cursor, batch_size=batch_size)
        print(f"阶段 1 前刷新任务哈希映射：quests={refreshed_qhm}")
        # Resume from phase-2 checkpoint: skip expensive phase-1 rerun.
        if start_idx > 0:
            print(
                "任务历史阶段 1 在断点续跑时跳过："
                f"从第 {start_idx + 1}/{total_snapshots} 个快照继续阶段 2"
            )
        else:
            # 阶段 1 先基于本地 textMap 数据推断版本。
            # 下面的快照回放只补未解决的记录。
            print("任务历史阶段 1：基于本地 textMap 推断（创建+更新）")
            prefilled_created_rows, prefilled_updated_rows = _backfill_quest_phase1_with_progress(
                cursor
            )
            print(
                "任务历史阶段 1 完成："
                f"回填 created_version 行数={prefilled_created_rows}, "
                f"回填 updated_version 行数={prefilled_updated_rows}"
            )

        total_quests, unresolved_quests = _count_unresolved_quest_versions(cursor)

        print("任务历史阶段 1.5：按版本快照回放任务首次出现")
        if force:
            print("强制模式：为所有任务回放 Git 创建版本...")
            cursor.execute("SELECT questId FROM quest")
        else:
            print("标准模式：为缺少 Git 版本的任务执行快照回放...")
            cursor.execute("SELECT questId FROM quest WHERE git_created_version_id IS NULL")
        quest_ids_to_backfill = {int(row[0]) for row in cursor.fetchall()}
        git_backfilled_count = 0

        if quest_ids_to_backfill:
            print(f"需要快照回放 Git 版本的任务数量：{len(quest_ids_to_backfill)}")
            git_backfilled_count = _replay_quest_snapshot_entries(
                cursor,
                repo_path=repo_path,
                snapshots=snapshots,
                base_snapshot=base_snapshot,
                target_quest_ids=quest_ids_to_backfill,
                start_idx=0,
                pbar_desc="任务 Git 快照回放",
            )
            print(f"任务历史阶段 1.5 完成：已通过快照回放更新 {git_backfilled_count} 个任务")
        else:
            print("所有任务都已有 Git 版本，跳过阶段 1.5。")

        synced_created_from_git = _sync_created_version_from_git(cursor)
        if synced_created_from_git > 0:
            conn.commit()
            print(
                "任务历史阶段 1.5 后同步："
                f"已用 git_created_version_id 修正 {synced_created_from_git} 条 created_version_id"
            )

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
                "任务历史阶段 2 已跳过："
                f"created_null=0 (未解决总数={unresolved_quests}/{total_quests})"
            )
        elif force:
            replay_mode = "full"
            print(
                "任务历史阶段 2 模式：全量（已启用 force），"
                f"created_null={unresolved_created_quests}/{total_quests}, "
                f"unresolved_total={unresolved_quests}/{total_quests}"
            )
        elif unresolved_ratio <= max(0.0, float(unresolved_ratio_threshold)):
            replay_mode = "targeted"
            print(
                "任务历史阶段 2 模式：定向，"
                f"created_null={unresolved_created_quests}/{total_quests} "
                f"({unresolved_ratio * 100:.2f}%)"
            )
        else:
            replay_mode = "full"
            print(
                "任务历史阶段 2 模式：全量（自动回退），"
                f"created_null={unresolved_created_quests}/{total_quests} "
                f"({unresolved_ratio * 100:.2f}% > 阈值 {unresolved_ratio_threshold * 100:.2f}%)"
            )

        if replay_mode in ("targeted", "full"):
            print(f"正在以 {replay_mode} 模式处理 {total_snapshots} 个任务版本快照...")
            try:
                target_quest_ids = unresolved_created_ids if replay_mode == "targeted" else None
                phase2_commit_created_backfilled = _replay_quest_snapshot_entries(
                    cursor,
                    repo_path=repo_path,
                    snapshots=snapshots,
                    base_snapshot=base_snapshot,
                    target_quest_ids=target_quest_ids,
                    start_idx=start_idx,
                    pbar_desc="任务快照回放",
                    checkpoint_every=DEFAULT_HISTORY_COMMIT_BATCH_SIZE,
                    resume_target_key=resume_target_key,
                    resume_done_key=resume_done_key,
                    resume_scope=resume_scope,
                )
            except BaseException:
                conn.rollback()
                print(
                    "任务版本快照回放被中断；已保存断点，重新运行可继续。",
                    file=sys.stderr,
                )
                raise

        final_total, final_unresolved_count = _count_unresolved_quest_versions(cursor)

    except BaseException:
        conn.rollback()
        raise
    finally:
        cursor.close()

    if resolved_from is None:
        set_meta(meta_commit_key, resolved_target)
        set_meta(meta_title_key, target_snapshot.version_label)
    set_meta(resume_target_key, "")
    set_meta(resume_done_key, "")
    if refresh_version_catalog:
        rebuild_version_catalog(["quest"])

    print("\n=== Quest Version Validation ===")
    validate_quest_versions(fix=True)

    return {
        "replay_mode": replay_mode,
        "total_quests": final_total,
        "unresolved_quests": final_unresolved_count,
        "phase1_created_backfilled": prefilled_created_rows,
        "phase1_updated_backfilled": prefilled_updated_rows,
        "phase2_commit_created_backfilled": phase2_commit_created_backfilled,
    }


def reset_history_version_marks(*, scope: str = "all"):
    """
    将所选范围的版本标记重置为 NULL。
    该操作与 --force 分离，避免普通重跑时清空数据。
    """
    ensure_version_schema()
    normalized_scope = (scope or "all").strip().lower()
    scope_map: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
        "all": (
            ("textMap", "readable", "subtitle", "quest", "npc"),
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
                "db_history_versions_commit_npc",
                "db_history_versions_commit_title_npc",
                "db_history_versions_commit_npc_resume_target",
                "db_history_versions_commit_npc_resume_done",
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
        "npc": (
            ("npc",),
            (
                "db_history_versions_commit_npc",
                "db_history_versions_commit_title_npc",
                "db_history_versions_commit_npc_resume_target",
                "db_history_versions_commit_npc_resume_done",
            ),
        ),
    }
    if normalized_scope not in scope_map:
        raise ValueError(f"Unsupported reset scope: {scope}")

    table_names, meta_keys = scope_map[normalized_scope]
    cursor = conn.cursor()
    try:
        for table_name in table_names:
            if table_name == 'quest':
                cursor.execute(f"UPDATE {table_name} SET created_version_id=NULL, git_created_version_id=NULL")
                cursor.execute("DELETE FROM quest_version")
            elif table_name == 'npc':
                cursor.execute(f"UPDATE {table_name} SET created_version_id=NULL")
            else:
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

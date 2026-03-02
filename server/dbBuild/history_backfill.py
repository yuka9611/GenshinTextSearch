import json
import os
import sqlite3
import subprocess
import sys
import time
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('history_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Git 命令缓存
_git_cache = {}


from DBConfig import conn, DATA_PATH
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched, to_hash_value as _to_hash_value
from lang_constants import LANG_CODE_MAP
from quest_hash_map_utils import (
    count_unresolved_quest_versions as _count_unresolved_quest_versions,
    refresh_all_quest_hash_map as _refresh_all_quest_hash_map,
    unresolved_created_quest_ids as _unresolved_created_quest_ids,
)
from quest_utils import extract_quest_row as _extract_quest_row
from version_control import backfill_quest_created_version_from_textmap as _backfill_quest_created_version_from_textmap
from version_control import readable_text_changed as _readable_text_changed
from subtitle_utils import parse_srt_rows as _parse_srt_rows
from version_control import subtitle_text_changed_keys as _subtitle_text_changed_keys
from textmap_name_utils import parse_textmap_file_name, analyze_readable_version_exceptions, analyze_subtitle_version_exceptions, analyze_textmap_version_exceptions, report_version_exceptions
from version_control import (
    ensure_version_schema,
    get_meta,
    get_or_create_version_id,
    normalize_version_label,
    rebuild_version_catalog,
    set_meta,
    build_guarded_created_updated_sql,
    should_update_version,
)
from lightweight_progress import LightweightProgress


# 公共路径常量
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


# ==================== 断点管理 ====================

def ensure_breakpoint_schema():
    """创建断点表结构"""
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
    """获取指定阶段的断点状态"""
    cur = conn.cursor()
    cur.execute("SELECT status FROM breakpoint WHERE stage_name = ?", (stage_name,))
    result = cur.fetchone()
    return result[0] if result else 'pending'


def update_breakpoint_status(stage_name, status, start_time=None, end_time=None):
    """更新断点状态"""
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
    """运行历史回填阶段，支持跳过询问和断点缓存"""
    # 确保断点表结构存在
    try:
        ensure_breakpoint_schema()
    except Exception as e:
        logger.error(f"Error ensuring breakpoint schema: {e}")
        raise

    # 检查断点记录
    try:
        status = get_breakpoint_status(stage_name)
    except Exception as e:
        logger.error(f"Error getting breakpoint status for {stage_name}: {e}")
        # 继续执行，使用默认状态
        status = 'pending'

    # 如果已经选择不跳过某个阶段，则不询问
    if not skip_asking:
        if status == 'completed':
            # 如果阶段已完成，询问是否重新执行
            try:
                ans = input(f"{stage_name} already completed. Re-execute? (y/n): ")
                if ans != 'y':
                    logger.info(f"Skipping {stage_name} (already completed)...")
                    return True  # 返回 True 表示跳过了该阶段
            except KeyboardInterrupt:
                logger.info(f"User interrupted, skipping {stage_name}")
                return True
            except Exception as e:
                logger.error(f"Error during user input: {e}")
                # 继续执行
        elif status == 'in_progress':
            # 如果阶段正在进行中，询问是否继续执行
            try:
                ans = input(f"{stage_name} is in progress. Continue? (y/n): ")
                if ans != 'y':
                    logger.info(f"Skipping {stage_name} (in progress)...")
                    return True  # 返回 True 表示跳过了该阶段
            except KeyboardInterrupt:
                logger.info(f"User interrupted, skipping {stage_name}")
                return True
            except Exception as e:
                logger.error(f"Error during user input: {e}")
                # 继续执行
        else:
            # 如果没有断点记录，询问是否跳过
            try:
                ans = input(f"Skip {stage_name}? (y/n): ")
                if ans == 'y':
                    logger.info(f"Skipping {stage_name}...")
                    return True  # 返回 True 表示跳过了该阶段
            except KeyboardInterrupt:
                logger.info(f"User interrupted, skipping {stage_name}")
                return True
            except Exception as e:
                logger.error(f"Error during user input: {e}")
                # 继续执行

    logger.info(f"Running {stage_name}...")
    # 记录开始时间和状态
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        update_breakpoint_status(stage_name, 'in_progress', start_time)
    except Exception as e:
        logger.error(f"Error updating breakpoint status for {stage_name}: {e}")

    try:
        result = fn(*args, **kwargs)
        # 记录完成时间和状态
        end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        try:
            update_breakpoint_status(stage_name, 'completed', start_time, end_time)
        except Exception as e:
            logger.error(f"Error updating breakpoint status to completed: {e}")
        logger.info(f"Completed {stage_name}")
        return False  # 返回 False 表示没有跳过该阶段
    except KeyboardInterrupt:
        logger.error(f"User interrupted {stage_name}")
        # 不更新断点状态，保持为 in_progress
        raise
    except Exception as e:
        logger.error(f"Error in {stage_name}: {e}", exc_info=True)
        # 不更新断点状态，保持为 in_progress
        raise


# ==================== 数据库工具函数 ====================

def _table_columns(table_name: str) -> set[str]:
    """获取表的列名集合"""
    cur = conn.cursor()
    try:
        rows = cur.execute(f"PRAGMA table_info({table_name})").fetchall()
    finally:
        cur.close()
    return {row[1] for row in rows}


def _table_exists(table_name: str) -> bool:
    """检查表是否存在"""
    cur = conn.cursor()
    try:
        row = cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (table_name,),
        ).fetchone()
        return row is not None
    finally:
        cur.close()


def _has_any_version_data(table_name: str) -> bool:
    """检查表是否有版本数据"""
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


# ==================== Git 工具函数 ====================

def _run_git(repo_path: str, args: list[str], check: bool = True) -> str:
    """执行Git命令并缓存结果"""
    # 生成缓存键
    cache_key = (repo_path, tuple(args))

    # 检查缓存
    if cache_key in _git_cache:
        return _git_cache[cache_key]

    # 执行 Git 命令
    try:
        logger.debug(f"Running git command: git -C {repo_path} {' '.join(args)}")
        # 优化：使用低优先级进程，减少对系统资源的占用
        creationflags = 0
        if sys.platform == "win32":
            try:
                # 尝试获取 CREATE_LOW_PRIORITY_CLASS，如果不可用则使用默认值
                CREATE_LOW_PRIORITY_CLASS = getattr(subprocess, 'CREATE_LOW_PRIORITY_CLASS', 0)
                creationflags = CREATE_LOW_PRIORITY_CLASS
            except Exception:
                # 如果获取失败，使用默认值
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
            error_msg = proc.stderr.strip() or "git command failed"
            logger.error(f"Git command failed: {error_msg}")
            raise RuntimeError(error_msg)

        result = (proc.stdout or "").strip()

        # 缓存结果
        _git_cache[cache_key] = result
        return result
    except Exception as e:
        logger.error(f"Error running git command: {e}", exc_info=True)
        raise


def _resolve_commit(repo_path: str, rev: str) -> str:
    """解析提交哈希"""
    return _run_git(repo_path, ["rev-parse", rev], check=True)


def _resolve_commit_title(repo_path: str, commit_sha: str) -> str:
    """获取提交标题"""
    title = _run_git(repo_path, ["show", "-s", "--format=%s", commit_sha], check=False).strip()
    return title or commit_sha


def _list_commits(
    repo_path: str,
    target_commit: str,
    *,
    from_commit: str | None = None,
) -> list[tuple[str, str]]:
    """列出提交历史"""
    # 构建 Git 命令参数
    cmd_args = ["log", "--reverse", "--format=%H%x1f%s"]

    # 如果指定了 from_commit，使用范围查询以减少返回的数据量
    if from_commit:
        cmd_args.extend([f"{from_commit}..{target_commit}"])
    else:
        cmd_args.append(target_commit)

    # 执行 Git 命令
    out = _run_git(repo_path, cmd_args, check=True)

    # 流式处理结果，减少内存使用
    commits = []
    # 逐行处理，避免一次性加载所有内容到内存
    for line in out.splitlines():
        if not line:
            continue
        if "\x1f" in line:
            sha, title = line.split("\x1f", 1)
        else:
            sha, title = line, ""
        commits.append((sha.strip(), title.strip()))

    # 如果 from_commit 不在结果中，说明它不在目标历史中
    if from_commit and commits and commits[0][0] != from_commit:
        # 检查 from_commit 是否存在于结果中
        found = False
        for idx, (sha, _title) in enumerate(commits):
            if sha == from_commit:
                found = True
                # 只保留 from_commit 之后的提交
                commits = commits[idx:]
                break
        if not found:
            raise RuntimeError(
                f"from_commit {from_commit} is not in target history {target_commit}"
            )

    return commits


def _resolve_parent_commit(repo_path: str, commit_sha: str) -> str | None:
    """获取父提交"""
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
    """获取第一个父提交的哈希"""
    if not resolved_from or not commits:
        return None
    return _resolve_parent_commit(repo_path, commits[0][0])


def _resolve_commit_version(repo_path: str, commit_sha: str, commit_title: str) -> tuple[str, int | None]:
    """解析提交的版本信息"""
    try:
        # 首先尝试使用提交标题
        if commit_title:
            version_label = normalize_version_label(commit_title)
            if version_label:
                version_id = _resolve_version_id(version_label)
                if version_id:
                    return version_label, version_id

        # 如果提交标题中没有有效的版本信息，尝试使用提交哈希
        version_label = normalize_version_label(commit_sha)
        if version_label:
            version_id = _resolve_version_id(version_label)
            if version_id:
                return version_label, version_id

        # 作为最后的尝试，直接使用提交哈希
        version_id = _resolve_version_id(commit_sha)
        return commit_sha, version_id
    except Exception as e:
        logger.error(f"Error resolving commit version for {commit_sha[:8]}: {e}")
        # 出错时返回提交哈希和None
        return commit_sha, None


def _backfill_git_versions(
    cursor,
    table_name: str,
    select_sql: str,
    build_file_path_fn,
    update_sql: str,
    desc: str,
    unit: str = "records"
) -> int:
    """
    通用Git版本回溯函数

    Args:
        cursor: 数据库游标
        table_name: 表名
        select_sql: 查询没有版本数据的记录的SQL
        build_file_path_fn: 构建文件路径的函数
        update_sql: 更新版本信息的SQL
        desc: 进度条描述
        unit: 进度条单位

    Returns:
        回填的记录数量
    """
    backfilled_count = 0
    try:
        # 查找没有创建版本的记录
        cursor.execute(select_sql)
        no_version_records = cursor.fetchall()

        if no_version_records:
            print(f"需要Git回溯的{table_name}记录数量: {len(no_version_records)}")
            total_records = len(no_version_records)

            # 缓存点管理
            resume_key = f"git_backfill_{table_name}_resume"
            start_idx = 0

            # 尝试从缓存点恢复
            try:
                resume_value = get_meta(resume_key, "")
                if resume_value:
                    try:
                        start_idx = int(resume_value)
                        if start_idx < 0 or start_idx >= total_records:
                            start_idx = 0
                        else:
                            print(f"从缓存点继续: 从第 {start_idx + 1} 条记录开始")
                    except ValueError:
                        start_idx = 0
            except Exception as e:
                logger.error(f"Error getting resume point: {e}")
                start_idx = 0

            with LightweightProgress(total_records, desc=desc, unit=unit) as pbar:
                # 跳过已经处理的记录
                for i in range(start_idx):
                    pbar.update()

                for i, record in enumerate(no_version_records[start_idx:], start=start_idx):
                    # 设置进度条信息
                    pbar.set_postfix_str(f"Processing {record}")
                    try:
                        # 构建文件路径
                        file_path = build_file_path_fn(record)
                        if not file_path:
                            pbar.update()
                            continue

                        # 使用Git命令获取该文件的第一个提交
                        out = _run_git(
                            DATA_PATH,
                            ["log", "--reverse", "--format=%H", "-n", "1", "--", file_path],
                            check=False
                        )
                        first_commit = out.strip() if out.strip() else None

                        if first_commit:
                            # 获取第一个提交的版本信息
                            first_commit_title = _run_git(
                                DATA_PATH,
                                ["show", "-s", "--format=%s", first_commit],
                                check=False
                            ).strip()
                            first_version_label, first_version_id = _resolve_commit_version(DATA_PATH, first_commit, first_commit_title)

                            if first_version_id:
                                # 构建更新参数
                                update_params = (first_version_id, first_version_id) + record
                                # 更新版本信息
                                cursor.execute(update_sql, update_params)
                                backfilled_count += 1
                    except Exception as e:
                        print(f"[ERROR] Git回溯失败 for {table_name} {record}: {e}")
                        pass
                    finally:
                        pbar.update()
                        # 每处理10条记录更新一次缓存点
                        if (i + 1) % 10 == 0:
                            try:
                                set_meta(resume_key, str(i + 1))
                                conn.commit()
                            except Exception as e:
                                logger.error(f"Error saving resume point: {e}")

            conn.commit()
            # 完成后清除缓存点
            try:
                set_meta(resume_key, "")
            except Exception as e:
                logger.error(f"Error clearing resume point: {e}")
            print(f"{table_name} Git回溯完成: 回填了 {backfilled_count} 条记录")
        else:
            print(f"所有{table_name}记录都已有版本数据，跳过Git回溯")
    except Exception as e:
        print(f"[ERROR] {table_name} Git回溯过程中发生错误: {e}")
        backfilled_count = 0

    return backfilled_count


def _backfill_textmap_git_versions(
    cursor,
    textmap_lang_map: dict[str, int],
    desc: str = "TextMap Git backfill",
    unit: str = "records"
) -> int:
    """
    TextMap专用Git版本回溯函数

    Args:
        cursor: 数据库游标
        textmap_lang_map: TextMap语言ID映射
        desc: 进度条描述
        unit: 进度条单位

    Returns:
        回填的记录数量
    """
    backfilled_count = 0
    try:
        # 查找没有创建版本的TextMap记录
        cursor.execute("SELECT lang, hash FROM textMap WHERE created_version_id IS NULL")
        no_version_records = cursor.fetchall()

        if no_version_records:
            print(f"需要Git回溯的TextMap记录数量: {len(no_version_records)}")
            total_records = len(no_version_records)

            # 缓存点管理
            resume_key = "git_backfill_textmap_resume"
            start_idx = 0

            # 尝试从缓存点恢复
            try:
                resume_value = get_meta(resume_key, "")
                if resume_value:
                    try:
                        start_idx = int(resume_value)
                        if start_idx < 0 or start_idx >= total_records:
                            start_idx = 0
                        else:
                            print(f"从缓存点继续: 从第 {start_idx + 1} 条记录开始")
                    except ValueError:
                        start_idx = 0
            except Exception as e:
                logger.error(f"Error getting resume point: {e}")
                start_idx = 0

            with LightweightProgress(total_records, desc=desc, unit=unit) as pbar:
                # 跳过已经处理的记录
                for i in range(start_idx):
                    pbar.update()

                for i, (lang, hash_value) in enumerate(no_version_records[start_idx:], start=start_idx):
                    pbar.set_postfix_str(f"Lang {lang}, Hash {hash_value}")
                    try:
                        # 查找包含该hash的TextMap文件
                        textmap_files = []
                        # 遍历可能的TextMap文件
                        for textmap_file in textmap_lang_map.keys():
                            textmap_files.append(f"TextMap/{textmap_file}.json")

                        # 使用Git命令查找包含该hash的提交
                        for textmap_file in textmap_files:
                            # 构建文件路径
                            file_path = textmap_file

                            # 使用Git命令获取该文件的所有提交
                            out = _run_git(
                                DATA_PATH,
                                ["log", "--reverse", "--format=%H", "--", file_path],
                                check=False
                            )
                            commits = out.strip().split('\n') if out.strip() else []

                            for commit_sha in commits:
                                if not commit_sha:
                                    continue

                                # 获取该提交中的文件内容
                                content = _git_show_json(DATA_PATH, commit_sha, file_path)
                                if not isinstance(content, dict):
                                    continue

                                # 检查是否包含该hash
                                if str(hash_value) in content:
                                    # 获取该提交的版本信息
                                    commit_title = _run_git(
                                        DATA_PATH,
                                        ["show", "-s", "--format=%s", commit_sha],
                                        check=False
                                    ).strip()
                                    version_label, version_id = _resolve_commit_version(DATA_PATH, commit_sha, commit_title)

                                    if version_id:
                                        # 更新版本信息
                                        cursor.execute(
                                            "UPDATE textMap SET created_version_id = ?, updated_version_id = ? WHERE lang = ? AND hash = ?",
                                            (version_id, version_id, lang, hash_value)
                                        )
                                        backfilled_count += 1
                                        break
                            if backfilled_count > i:
                                break
                    except Exception as e:
                        print(f"[ERROR] Git回溯失败 for textmap {lang}:{hash_value}: {e}")
                        pass
                    finally:
                        pbar.update()
                        # 每处理10条记录更新一次缓存点
                        if (i + 1) % 10 == 0:
                            try:
                                set_meta(resume_key, str(i + 1))
                                conn.commit()
                            except Exception as e:
                                logger.error(f"Error saving resume point: {e}")

            conn.commit()
            # 完成后清除缓存点
            try:
                set_meta(resume_key, "")
            except Exception as e:
                logger.error(f"Error clearing resume point: {e}")
            print(f"TextMap Git回溯完成: 回填了 {backfilled_count} 条记录")
        else:
            print("所有TextMap记录都已有版本数据，跳过Git回溯")
    except Exception as e:
        print(f"[ERROR] TextMap Git回溯过程中发生错误: {e}")
        backfilled_count = 0

    return backfilled_count


def _latest_commit_meta_title(commits: list[tuple[str, str]]) -> str | None:
    """获取最新提交的元数据标题"""
    if not commits:
        return None
    return normalize_version_label(commits[-1][1]) or normalize_version_label(commits[-1][0]) or commits[-1][0]


def _initial_entries(repo_path: str, commit: str, include_paths: list[str] | None = None) -> list[dict]:
    """获取初始提交的文件条目"""
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
    """获取两个提交之间的差异文件条目"""
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
    """获取指定提交中文件的文本内容"""
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
        logger.error(f"Error getting text from git: {e}")
        return None


def _git_show_json(repo_path: str, commit: str, rel_path: str):
    """获取指定提交中文件的JSON内容"""
    try:
        raw = _git_show_text(repo_path, commit, rel_path)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception as e:
            logger.debug(f"Error parsing JSON from {commit}:{rel_path}: {e}")
            return None
    except Exception as e:
        logger.error(f"Error getting JSON from git: {e}")
        return None


# ==================== 版本处理工具函数 ====================

def _resolve_version_id(version_label: str) -> int | None:
    """解析版本ID"""
    return get_or_create_version_id(normalize_version_label(version_label) or version_label)


def _prepare_resume_for_commits(
    *,
    resume_target_key: str,
    resume_done_key: str,
    resolved_target: str,
    commits: list[tuple[str, str]],
    force: bool,
    label: str,
) -> int:
    """准备提交恢复点"""
    total_commits = len(commits)
    commit_index = {sha: idx for idx, (sha, _title) in enumerate(commits)}

    if force:
        # 强制更新仅清除缓存点，不影响版本数据
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


def _extract_quest_backfill_stats(backfill_result) -> tuple[int, int]:
    """提取任务回填统计信息"""
    if isinstance(backfill_result, dict):
        return (
            int(backfill_result.get("created_rows", 0)),
            int(backfill_result.get("updated_rows", 0)),
        )
    if isinstance(backfill_result, tuple) and len(backfill_result) >= 2:
        return int(backfill_result[0]), int(backfill_result[1])
    return 0, 0


# ==================== 任务相关工具函数 ====================

def _quest_text_signature(row):
    """获取任务文本签名"""
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
    """
    使用当前commit作为任务的git创建版本

    Args:
        cursor: 数据库游标
        repo_path: Git仓库路径
        quest_id: 任务ID
        commit_sha: 当前提交哈希
        parent_sha: 父提交哈希
        version_id: 当前版本ID

    Returns:
        (commit_sha, version_id): 提交哈希和对应的版本ID
    """
    # 优先查询缓存结果
    cursor.execute(
        "SELECT git_created_version_id FROM quest WHERE questId = ?",
        (quest_id,)
    )
    cached_version = cursor.fetchone()
    existing_git_created_version = cached_version[0] if cached_version else None

    # 只有当git创建版本为空或小于当前版本时才更新
    if existing_git_created_version is None or version_id < existing_git_created_version:
        # 构建任务文件路径
        quest_file_path = f"BinOutput/Quest/{quest_id}.json"

        # 获取当前提交的任务内容
        new_obj = _git_show_json(repo_path, commit_sha, quest_file_path)
        if not isinstance(new_obj, dict):
            return None, None
        new_row = _extract_quest_row(new_obj)
        if new_row is None:
            return None, None

        # 获取父提交的任务内容
        old_obj = _git_show_json(repo_path, parent_sha, quest_file_path) if parent_sha else None
        old_row = _extract_quest_row(old_obj) if isinstance(old_obj, dict) else None

        # 检查内容是否相同
        content_same = (_quest_text_signature(old_row) == _quest_text_signature(new_row))

        # 返回当前提交作为git创建版本
        return commit_sha, version_id
    else:
        # 如果已有git创建版本且不小于当前版本，返回None
        return None, None


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
    """通过提交条目回填任务版本"""
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

    # 检查内容是否相同
    content_same = (_quest_text_signature(old_row) == _quest_text_signature(new_row))

    # 检查是否已经有创建版本
    cursor.execute("SELECT created_version_id, git_created_version_id FROM quest WHERE questId = ?", (new_row[0],))
    version_info = cursor.fetchone()
    existing_created_version = version_info[0] if version_info else None
    existing_git_created_version = version_info[1] if version_info else None

    # 版本验证：确保版本号有效
    if version_id <= 0:
        logger.warning(f"Invalid version ID {version_id} for quest {new_row[0]}, skipping")
        return 0

    # 只有当创建版本为空或小于当前版本时才更新
    if existing_created_version is None or version_id < existing_created_version:
        # 更新created_version_id
        cursor.execute(
            """
            UPDATE quest
            SET created_version_id=?
            WHERE questId=?
              AND (created_version_id IS NULL OR created_version_id > ?)
            """,
            (version_id, new_row[0], version_id),
        )
        created_updated = cursor.rowcount > 0
        if created_updated:
            logger.debug(f"Updated created_version_id for quest {new_row[0]} to {version_id}")
    else:
        created_updated = False

    # 只有当git创建版本为空或小于当前版本时才更新
    if existing_git_created_version is None or version_id < existing_git_created_version:
        # 更新git_created_version_id
        cursor.execute(
            """
            UPDATE quest
            SET git_created_version_id=?
            WHERE questId=?
              AND (git_created_version_id IS NULL OR git_created_version_id > ?)
            """,
            (version_id, new_row[0], version_id),
        )
        git_updated = cursor.rowcount > 0
        if git_updated:
            logger.debug(f"Updated git_created_version_id for quest {new_row[0]} to {version_id}")
    else:
        git_updated = False

    return 1 if (created_updated or git_updated) else 0


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
    """
    从TextMap应用任务版本增量

    Args:
        cursor: 数据库游标
        version_id: 版本ID
        changed_hashes: 变更的哈希集合
        version_label: 版本标签
        quest_scope: 任务范围
        batch_size: 批处理大小
        fetch_size: 获取大小
        show_progress: 是否显示进度
        progress_position: 进度位置

    Returns:
        统计信息
    """
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
                quest_scope_filter = " AND questId IN (SELECT questId FROM _target_quest_id)"
            except Exception as e:
                logger.error(f"Error processing quest scope: {e}")
                quest_scope_filter = ""
        else:
            quest_scope_filter = ""

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
                # 使用 executemany_batched 函数处理批处理插入
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
                # 使用生成器函数来逐批获取数据，避免一次性加载所有数据到内存
                def fetch_hashes():
                    while True:
                        rows = select_cur.fetchmany(fetch_size)
                        if not rows:
                            break
                        for row in rows:
                            yield row
                # 使用 executemany_batched 函数处理批处理插入
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

        # 获取所有可用的语言
        try:
            lang_rows = cursor.execute("SELECT id FROM langCode WHERE imported=1").fetchall()
            languages = [row[0] for row in lang_rows]
        except Exception as e:
            logger.error(f"Error fetching languages: {e}")
            languages = []

        for lang in languages:
            # 为每种语言单独计算更新版本
            try:
                if has_quest_hash_map_rows:
                    # 使用quest_hash_map表
                    cursor.execute(
                        f"""
                        INSERT INTO quest_version(questId, lang, updated_version_id)
                        SELECT DISTINCT qhm.questId, ?, ?
                        FROM quest_hash_map qhm
                        JOIN _changed_textmap_hash c ON c.hash = qhm.hash
                        JOIN quest q ON q.questId = qhm.questId
                        WHERE {quest_scope_filter}
                        ON CONFLICT(questId, lang) DO UPDATE SET
                        updated_version_id=CASE
                        WHEN excluded.updated_version_id > quest_version.updated_version_id THEN excluded.updated_version_id
                        ELSE quest_version.updated_version_id
                        END
                        """,
                        (lang, version_id),
                    )
                else:
                    # 不使用quest_hash_map表
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
                                JOIN _changed_textmap_hash c ON c.hash = d.textHash
                            )
                        )
                        ON CONFLICT(questId, lang) DO UPDATE SET
                        updated_version_id=CASE
                        WHEN excluded.updated_version_id > quest_version.updated_version_id THEN excluded.updated_version_id
                        ELSE quest_version.updated_version_id
                        END
                        """,
                        (lang, version_id),
                    )
                stats["quest_updated_by_textmap"] += int(cursor.rowcount or 0)
            except Exception as e:
                logger.error(f"Error updating quest versions for language {lang}: {e}")
                # 继续处理下一种语言，不中断整个过程

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
    """执行任务第一阶段回填并显示进度"""
    total_row = cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
    total_quests = int(total_row[0] or 0) if total_row else 0
    if total_quests <= 0:
        return 0, 0

    created_total = 0
    updated_total = 0
    quest_cursor = cursor.execute("SELECT questId FROM quest ORDER BY questId")

    with LightweightProgress(total_quests, desc="Phase-1 backfill", unit="quests") as pbar:
        print(f"Processing {total_quests} quests for phase-1 backfill...")

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
            pbar.update(len(quest_ids))

    print(f"Phase-1 backfill completed: {created_total} created, {updated_total} updated")
    return created_total, updated_total


def _get_textmap_lang_id_map() -> dict[str, int]:
    """获取TextMap语言ID映射"""
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


# ==================== 历史回填核心函数 ====================

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
    commit_batch_size: int = 10,
):
    """
    通用历史回填函数

    Args:
        target_commit: 目标提交
        from_commit: 起始提交
        force: 是否强制重新执行
        batch_size: 批处理大小
        verbose: 是否详细输出
        include_paths: 包含的路径
        table_name: 表名
        meta_commit_key: 元数据提交键
        meta_title_key: 元数据标题键
        resume_target_key: 恢复目标键
        resume_done_key: 恢复完成键
        process_entry_fn: 处理条目的函数
        pbar_desc: 进度条描述
        commit_batch_size: 提交批次大小，每处理多少个提交后进行一次数据库提交
    """
    try:
        ensure_version_schema()
    except Exception as e:
        logger.error(f"Error ensuring version schema: {e}")
        raise

    repo_path = DATA_PATH
    try:
        resolved_target = _resolve_commit(repo_path, target_commit)
        resolved_from = _resolve_commit(repo_path, from_commit) if from_commit else None
        resume_scope = f"{resolved_from or ''}..{resolved_target}"
    except Exception as e:
        logger.error(f"Error resolving commits: {e}")
        raise

    # 检查是否已经执行过历史回填
    try:
        if not force and resolved_from is None and get_meta(meta_commit_key) == resolved_target:
            if _has_any_version_data(table_name):
                logger.info(f"{table_name} history backfill already done for {resolved_target}, skipping.")
                return
            logger.info(
                f"{table_name} history backfill meta indicates {resolved_target}, "
                f"but {table_name} versions are empty/incomplete; rerunning."
            )
    except Exception as e:
        logger.error(f"Error checking backfill status: {e}")
        # 继续执行，不影响主流程

    try:
        commits = _list_commits(repo_path, resolved_target, from_commit=resolved_from)
        if not commits:
            logger.info(f"{table_name} history backfill: no commits to process.")
            return
        first_parent_sha = _resolve_first_parent_sha(repo_path, commits, resolved_from)
        total_commits = len(commits)
        try:
            start_idx = _prepare_resume_for_commits(
                resume_target_key=resume_target_key,
                resume_done_key=resume_done_key,
                resolved_target=resume_scope,
                commits=commits,
                force=force,
                label=f"{table_name} history backfill",
            )
        except Exception as e:
            logger.error(f"Error preparing resume for commits: {e}")
            start_idx = 0

        if start_idx == 0:
            if resolved_from:
                logger.info(
                    f"{table_name} history backfill start: "
                    f"{total_commits} commits (from: {resolved_from[:8]}, target: {resolved_target})"
                )
            else:
                logger.info(f"{table_name} history backfill start: {total_commits} commits (target: {resolved_target})")
        else:
            logger.info(
                f"{table_name} history backfill continue: "
                f"{total_commits - start_idx} remaining / {total_commits} total commits "
                f"(target: {resolved_target})"
            )

        cursor = conn.cursor()
        # 简化进度显示，只显示主要进度
        logger.info(f"Processing {total_commits} {table_name} commits...")
        last_commit_sha = None
        processed_commits = 0
        try:
            with LightweightProgress(total_commits, desc=pbar_desc, unit="commits") as pbar:
                for idx in range(start_idx, total_commits):
                    try:
                        commit_sha, commit_title = commits[idx]
                        last_commit_sha = commit_sha
                        parent_sha = commits[idx - 1][0] if idx > 0 else first_parent_sha

                        # 版本解析错误处理
                        try:
                            version_label, version_id = _resolve_commit_version(repo_path, commit_sha, commit_title)
                            if version_id is None:
                                logger.warning(f"Failed to resolve version for commit {commit_sha[:8]}, skipping")
                                pbar.update()
                                continue
                        except Exception as e:
                            logger.error(f"Error resolving version for commit {commit_sha}: {e}")
                            pbar.update()
                            continue

                        pbar.set_postfix_str(f"Commit {commit_sha[:8]}")

                        try:
                            entries = (
                                _initial_entries(repo_path, commit_sha, include_paths=include_paths)
                                if parent_sha is None
                                else _diff_entries(repo_path, parent_sha, commit_sha, include_paths=include_paths)
                            )
                        except Exception as e:
                            logger.error(f"Error getting entries for commit {commit_sha}: {e}")
                            pbar.update()
                            continue

                        # 处理条目时的错误处理
                        entry_errors = 0
                        for entry in entries:
                            try:
                                process_entry_fn(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size)
                            except Exception as e:
                                logger.error(f"Error processing {table_name} entry: {e}")
                                entry_errors += 1
                                # 继续处理下一个条目，不中断整个过程

                        if entry_errors > 0:
                            logger.warning(f"Encountered {entry_errors} errors while processing entries for commit {commit_sha[:8]}")

                        pbar.update()
                        processed_commits += 1

                        # 每处理一定数量的提交后进行一次数据库提交，减少提交次数
                        if processed_commits % commit_batch_size == 0:
                            # 批量提交并更新缓存点
                            try:
                                conn.commit()
                                set_meta(resume_target_key, resume_scope)
                                set_meta(resume_done_key, commit_sha)
                                logger.debug(f"Committed batch up to commit {commit_sha[:8]}")
                            except Exception as e:
                                logger.error(f"Error committing changes for batch: {e}")
                                # 继续执行，不中断整个过程

                    except Exception as e:
                        logger.error(f"Error processing commit {idx}: {e}")
                        pbar.update()
                        # 继续处理下一个提交，不中断整个过程

                # 处理完所有提交后，确保进行最后一次提交
                if processed_commits > 0 and last_commit_sha:
                    try:
                        conn.commit()
                        set_meta(resume_target_key, resume_scope)
                        set_meta(resume_done_key, last_commit_sha)
                        logger.info(f"Final commit completed up to {last_commit_sha[:8]}")
                    except Exception as e:
                        logger.error(f"Error finalizing commit: {e}")

        except BaseException as e:
            conn.rollback()
            logger.error(
                f"{table_name} history backfill interrupted; checkpoint saved, rerun to continue.",
                exc_info=True
            )
            # 保存当前进度，以便下次可以继续
            if last_commit_sha:
                try:
                    set_meta(resume_target_key, resume_scope)
                    set_meta(resume_done_key, last_commit_sha)
                    logger.info(f"Checkpoint saved at commit {last_commit_sha[:8]}")
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
                if commits:
                    title = _latest_commit_meta_title(commits) or commits[-1][0]
                    set_meta(meta_title_key, title if title is not None else "")
            # 只有在完全处理完所有提交后才清空缓存点
            # 这样在处理过程中中断时，下次运行可以从上次的位置继续
            if start_idx >= total_commits:
                set_meta(resume_target_key, "")
                set_meta(resume_done_key, "")
            rebuild_version_catalog([table_name])
        except Exception as e:
            logger.error(f"Error finalizing metadata: {e}")
            # 继续执行，不影响主流程
    except Exception as e:
        logger.error(f"Error in {table_name} history backfill: {e}", exc_info=True)
        raise


def backfill_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
):
    """执行所有类型的历史回填"""
    try:
        ensure_version_schema()
    except Exception as e:
        logger.error(f"Error ensuring version schema: {e}")
        raise

    repo_path = DATA_PATH
    try:
        resolved_target = _resolve_commit(repo_path, target_commit)
        resolved_from = _resolve_commit(repo_path, from_commit) if from_commit else None
        resume_scope = f"{resolved_from or ''}..{resolved_target}"
        resume_target_key = "db_history_versions_commit_resume_target"
        resume_done_key = "db_history_versions_commit_resume_done"
    except Exception as e:
        logger.error(f"Error resolving commits: {e}")
        raise

    # 检查是否已经执行过历史回填
    try:
        if not force and resolved_from is None and get_meta("db_history_versions_commit") == resolved_target:
            version_tables = ("textMap", "readable", "subtitle", "quest")
            try:
                if all(_has_any_version_data(t) for t in version_tables):
                    logger.info(f"History backfill already done for {resolved_target}, skipping.")
                    return
            except Exception as e:
                logger.error(f"Error checking version data: {e}")
                # 继续执行，不影响主流程
            logger.info(
                f"History backfill meta indicates {resolved_target}, "
                "but version columns are empty/incomplete; rerunning."
            )
    except Exception as e:
        logger.error(f"Error checking backfill status: {e}")
        # 继续执行，不影响主流程

    # 依次执行各个类型的历史回填
    backfill_functions = [
        ("TextMap", backfill_textmap_versions_from_history),
        ("Readable", backfill_readable_versions_from_history),
        ("Subtitle", backfill_subtitle_versions_from_history),
        ("Quest", backfill_quest_versions_from_history),
    ]

    for name, func in backfill_functions:
        try:
            logger.info(f"Starting {name} history backfill...")
            func(
                target_commit=target_commit,
                from_commit=from_commit,
                force=force,
                batch_size=batch_size,
                verbose=verbose,
            )
            logger.info(f"Completed {name} history backfill")
        except Exception as e:
            logger.error(f"Error in {name} history backfill: {e}", exc_info=True)
            # 继续执行下一个类型的回填，不中断整个流程

    try:
        if resolved_from is None:
            set_meta("db_history_versions_commit", resolved_target)
            # 不需要设置标题，因为各个子函数已经设置了各自的标题
        # 只有在完全处理完所有提交后才清空缓存点
        # 这样在处理过程中中断时，下次运行可以从上次的位置继续
        set_meta(resume_target_key, "")
        set_meta(resume_done_key, "")
        rebuild_version_catalog(["textMap", "quest", "subtitle", "readable"])
    except Exception as e:
        logger.error(f"Error finalizing metadata: {e}")
        # 继续执行，不影响主流程

    # 使用现有的异常验证函数分析版本异常
    logger.info("\n=== 版本异常验证 ===")
    cursor = conn.cursor()
    try:
        # 分析Readable版本异常
        try:
            readable_exceptions = analyze_readable_version_exceptions(cursor)
            report_version_exceptions(readable_exceptions, "Readable")
        except Exception as e:
            logger.error(f"Error analyzing Readable version exceptions: {e}")

        # 分析Subtitle版本异常
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

    logger.info(f"History backfill finished at commit {resolved_target}")


def backfill_textmap_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
):
    """执行TextMap历史回填"""
    textmap_lang_map = _get_textmap_lang_id_map()
    sql_textmap = build_guarded_created_updated_sql("textMap", "lang=? AND hash=?")

    def process_textmap_entry(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size):
        """处理TextMap条目"""
        action = entry["action"]
        if action == "D":
            return
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

        new_obj = _git_show_json(repo_path, commit_sha, rel_path)
        if not isinstance(new_obj, dict):
            return

        old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
        old_obj = _git_show_json(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
        if not isinstance(old_obj, dict):
            old_obj = {}

        changed_rows = []
        for raw_hash, content in new_obj.items():
            old_content = old_obj.get(raw_hash, None)
            if old_content == content:
                continue
            changed_rows.append((version_id, version_id, version_id, lang_id, _to_hash_value(raw_hash), version_id))
        if changed_rows:
            executemany_batched(cursor, sql_textmap, changed_rows, batch_size=batch_size)

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
        commit_batch_size=10,
    )

    # 新增步骤：为没有版本数据的TextMap记录执行Git回溯，获取真实的版本信息
    print("TextMap history phase-1.5: Git history backfill for textmap without version data")
    cursor = conn.cursor()
    try:
        _backfill_textmap_git_versions(cursor, textmap_lang_map)
    finally:
        cursor.close()

    # 执行版本异常检验
    check_cursor = conn.cursor()
    try:
        exception_data = analyze_textmap_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "TextMap")

        # 修复创建版本晚于更新版本的记录
        if exception_data['created_after_updated'] > 0:
            print(f"修复 TextMap 中创建版本晚于更新版本的 {exception_data['created_after_updated']} 条记录...")
            fixed = fix_created_after_updated_versions(check_cursor, "textMap", ["lang", "hash"])
            conn.commit()
            print(f"修复完成，共修复 {fixed} 条记录")
    finally:
        check_cursor.close()


def backfill_readable_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
):
    """执行Readable历史回填"""
    sql_readable = build_guarded_created_updated_sql("readable", "fileName=? AND lang=?")

    def process_readable_entry(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size):
        """处理Readable条目"""
        action = entry["action"]
        if action == "D":
            return
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
        # 计算相对路径，保持与importReadable函数一致
        rel_under_lang = parts[2]
        # 构建完整路径以计算相对路径
        full_path = os.path.join(repo_path, rel_path)
        lang_path = os.path.join(repo_path, "Readable", lang)
        # 计算相对路径
        rel_path_file = os.path.relpath(full_path, lang_path)
        # 将路径分隔符替换为"/"
        clean_file_name = rel_path_file.replace(os.sep, "/")

        new_text = _git_show_text(repo_path, commit_sha, rel_path)
        if new_text is None:
            return

        # 检查是否已经有创建版本
        cursor.execute("SELECT created_version_id, updated_version_id FROM readable WHERE fileName=? AND lang=?", (clean_file_name, lang))
        version_info = cursor.fetchone()
        existing_created_version = version_info[0] if version_info else None
        existing_updated_version = version_info[1] if version_info else None

        # 只有当创建版本为空或小于当前版本时才更新
        if existing_created_version is None or version_id < existing_created_version:
            # 检查内容是否相同
            old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
            old_text = _git_show_text(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
            # 确定更新版本：内容相同则写入当前版本，不同则为空
            updated_version = version_id if not _readable_text_changed(old_text, new_text) else None

            cursor.execute(sql_readable, (version_id, version_id, updated_version, clean_file_name, lang, version_id))

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
    )

    # 新增步骤：为没有版本数据的Readable记录执行Git回溯，获取真实的版本信息
    print("Readable history phase-1.5: Git history backfill for readable without version data")
    cursor = conn.cursor()
    try:
        # 定义构建文件路径的函数
        def build_readable_file_path(record):
            file_name, lang = record
            return f"Readable/{lang}/{file_name}"

        # 使用通用Git版本回溯函数
        _backfill_git_versions(
            cursor,
            "Readable",
            "SELECT fileName, lang FROM readable WHERE created_version_id IS NULL",
            build_readable_file_path,
            "UPDATE readable SET created_version_id = ?, updated_version_id = ? WHERE fileName = ? AND lang = ?",
            "Readable Git backfill"
        )
    finally:
        cursor.close()

    # 执行版本异常检验
    check_cursor = conn.cursor()
    try:
        exception_data = analyze_readable_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "Readable")

        # 修复创建版本晚于更新版本的记录
        if exception_data['created_after_updated'] > 0:
            print(f"修复 Readable 中创建版本晚于更新版本的 {exception_data['created_after_updated']} 条记录...")
            fixed = fix_created_after_updated_versions(check_cursor, "readable", ["fileName", "lang"])
            conn.commit()
            print(f"修复完成，共修复 {fixed} 条记录")
    finally:
        check_cursor.close()


def backfill_subtitle_versions_from_history(
    *,
    target_commit: str = "HEAD",
    from_commit: str | None = None,
    force: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    verbose: bool = False,
):
    """执行Subtitle历史回填"""
    sql_subtitle = build_guarded_created_updated_sql("subtitle", "subtitleKey=?")

    def process_subtitle_entry(cursor, repo_path, commit_sha, parent_sha, entry, version_id, version_label, batch_size):
        """处理Subtitle条目"""
        action = entry["action"]
        if action == "D":
            return
        old_path = entry.get("old_path")
        new_path = entry.get("new_path")
        rel_path = (new_path or old_path or "").replace("\\", "/")
        if not rel_path:
            return
        if not (rel_path.startswith("Subtitle/") and rel_path.endswith(".srt")):
            return
        parts = rel_path.split("/", 2)
        if len(parts) < 3:
            return
        lang_name = parts[1]
        lang_id = LANG_CODE_MAP.get(lang_name)
        if lang_id is None:
            return

        rel_under_lang = parts[2]
        new_text = _git_show_text(repo_path, commit_sha, rel_path)
        if new_text is None:
            return
        new_rows = _parse_srt_rows(new_text, lang_id, rel_under_lang)

        old_path_for_compare = old_path if action.startswith("R") and old_path else rel_path
        old_text = _git_show_text(repo_path, parent_sha, old_path_for_compare) if parent_sha else None
        old_rows = _parse_srt_rows(old_text, lang_id, rel_under_lang) if old_text else {}

        changed_keys = _subtitle_text_changed_keys(old_rows, new_rows)
        if changed_keys:
            valid_keys = []
            updated_versions = []
            for key in changed_keys:
                # 检查是否已经有创建版本
                cursor.execute("SELECT created_version_id, updated_version_id FROM subtitle WHERE subtitleKey=?", (key,))
                version_info = cursor.fetchone()
                existing_created_version = version_info[0] if version_info else None

                # 只有当创建版本为空或小于当前版本时才更新
                if existing_created_version is None or version_id < existing_created_version:
                    # 检查内容是否相同
                    old_content = old_rows.get(key, None)
                    new_content = new_rows.get(key, None)
                    # 确定更新版本：内容相同则写入当前版本，不同则为空
                    updated_version = version_id if old_content == new_content else None
                    valid_keys.append(key)
                    updated_versions.append(updated_version)

            if valid_keys:
                executemany_batched(
                    cursor,
                    sql_subtitle,
                    (
                        (version_id, version_id, updated_version, key, version_id)
                        for key, updated_version in zip(valid_keys, updated_versions)
                    ),
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
    )

    # 新增步骤：为没有版本数据的Subtitle记录执行Git回溯，获取真实的版本信息
    print("Subtitle history phase-1.5: Git history backfill for subtitle without version data")
    cursor = conn.cursor()
    try:
        # 定义构建文件路径的函数
        def build_subtitle_file_path(record):
            subtitle_key, = record
            # 解析subtitleKey获取文件路径信息
            # subtitleKey格式通常为：文件名_语言_开始时间_结束时间
            parts = subtitle_key.split('_')
            if len(parts) < 4:
                return None

            # 重建文件路径
            file_name = '_'.join(parts[:-3])
            lang_part = parts[-3]
            # 查找对应的语言目录
            lang_dir = None
            for lang_name, lang_id in LANG_CODE_MAP.items():
                if str(lang_id) == lang_part:
                    lang_dir = lang_name
                    break
            if not lang_dir:
                return None

            return f"Subtitle/{lang_dir}/{file_name}.srt"

        # 使用通用Git版本回溯函数
        _backfill_git_versions(
            cursor,
            "Subtitle",
            "SELECT subtitleKey FROM subtitle WHERE created_version_id IS NULL",
            build_subtitle_file_path,
            "UPDATE subtitle SET created_version_id = ?, updated_version_id = ? WHERE subtitleKey = ?",
            "Subtitle Git backfill"
        )
    finally:
        cursor.close()

    # 执行版本异常检验
    check_cursor = conn.cursor()
    try:
        exception_data = analyze_subtitle_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "Subtitle")

        # 修复创建版本晚于更新版本的记录
        if exception_data['created_after_updated'] > 0:
            print(f"修复 Subtitle 中创建版本晚于更新版本的 {exception_data['created_after_updated']} 条记录...")
            fixed = fix_created_after_updated_versions(check_cursor, "subtitle", ["subtitleKey"])
            conn.commit()
            print(f"修复完成，共修复 {fixed} 条记录")
    finally:
        check_cursor.close()


def fix_created_after_updated_versions(
    cursor,
    table_name: str,
    id_columns: list[str],
    max_fixes: int = 100000
) -> int:
    """
    修复创建版本晚于更新版本的记录

    Args:
        cursor: 数据库游标
        table_name: 表名
        id_columns: 主键列名列表
        max_fixes: 最大修复数量

    Returns:
        修复的记录数量
    """
    # 构建WHERE子句
    where_clause = " AND ".join([f"{col} = ?" for col in id_columns])

    # 构建UPDATE语句
    update_sql = f"UPDATE {table_name} SET created_version_id = updated_version_id WHERE created_version_id > updated_version_id"

    # 构建SELECT语句
    select_sql = f"SELECT {', '.join(id_columns)}, created_version_id, updated_version_id FROM {table_name} WHERE created_version_id > updated_version_id"

    # 执行查询
    cursor.execute(select_sql)
    records = cursor.fetchall()

    fixed_count = 0
    for record in records:
        if fixed_count >= max_fixes:
            break

        # 执行更新
        cursor.execute(update_sql)
        fixed_count += 1

    return fixed_count


def validate_quest_versions(
    *,
    repo_path: str | None = None,
    fix: bool = False,
    max_fixes: int = 100000,  # 增加默认值，确保能处理所有任务
    db_conn=None,
) -> dict[str, int]:
    """
    验证任务版本的合理性，检测并修复版本异常的任务

    Args:
        repo_path: Git仓库路径
        fix: 是否自动修复异常版本
        max_fixes: 最大修复数量，避免一次性处理过多任务导致数据库锁定
        db_conn: 数据库连接，默认使用全局的 conn

    Returns:
        验证结果统计
    """
    ensure_version_schema()
    if repo_path is None:
        repo_path = DATA_PATH

    # 使用提供的数据库连接，否则使用全局的 conn
    use_conn = db_conn if db_conn else conn
    cursor = use_conn.cursor()
    try:
        # 检测版本异常的任务
        # 1. 没有创建版本的任务
        cursor.execute(
            "SELECT questId, created_version_id, git_created_version_id FROM quest WHERE created_version_id IS NULL"
        )
        no_created_version = cursor.fetchall()

        # 2. 没有Git版本的任务
        cursor.execute(
            "SELECT questId, created_version_id, git_created_version_id FROM quest WHERE git_created_version_id IS NULL"
        )
        no_git_version = cursor.fetchall()

        # 3. 版本号无效的任务（负数或零）
        cursor.execute(
            "SELECT questId, created_version_id, git_created_version_id FROM quest WHERE created_version_id <= 0 OR git_created_version_id <= 0"
        )
        invalid_version = cursor.fetchall()

        # 4. 版本号差异过大的任务
        cursor.execute(
            "SELECT questId, created_version_id, git_created_version_id FROM quest WHERE created_version_id IS NOT NULL AND git_created_version_id IS NOT NULL AND ABS(created_version_id - git_created_version_id) > 20"
        )
        large_version_diff = cursor.fetchall()

        # 5. 检测quest_version表中的异常
        # 5.1 没有对应quest的记录
        cursor.execute(
            "SELECT qv.questId, qv.lang, qv.updated_version_id FROM quest_version qv LEFT JOIN quest q ON qv.questId = q.questId WHERE q.questId IS NULL"
        )
        quest_version_no_quest = cursor.fetchall()

        # 5.2 没有更新版本的记录
        cursor.execute(
            "SELECT questId, lang, updated_version_id FROM quest_version WHERE updated_version_id IS NULL"
        )
        quest_version_no_updated = cursor.fetchall()

        # 5.3 quest_version表中版本号无效的记录
        cursor.execute(
            "SELECT questId, lang, updated_version_id FROM quest_version WHERE updated_version_id <= 0"
        )
        quest_version_invalid = cursor.fetchall()

        # 5.4 quest_version表中更新版本早于创建版本的记录
        # 首先找出每个任务的最小updated_version_id
        cursor.execute(
            "SELECT questId, MIN(updated_version_id) as min_updated_version FROM quest_version GROUP BY questId"
        )
        min_updated_versions = {row[0]: row[1] for row in cursor.fetchall()}

        # 然后找出创建版本大于最小更新版本的任务
        quest_version_older = []
        for quest_id, min_updated in min_updated_versions.items():
            cursor.execute(
                "SELECT created_version_id FROM quest WHERE questId = ? AND created_version_id > ?",
                (quest_id, min_updated)
            )
            result = cursor.fetchone()
            if result:
                # 添加到异常列表，格式为(questId, lang, updated_version_id, created_version_id)
                # 这里lang设为None，因为我们是按任务ID分组的
                quest_version_older.append((quest_id, None, min_updated, result[0]))

        # 统计异常任务数量
        total_abnormal = len(no_created_version) + len(no_git_version) + len(invalid_version) + len(large_version_diff) + len(quest_version_no_quest) + len(quest_version_no_updated) + len(quest_version_invalid) + len(quest_version_older)

        print(f"版本验证结果:")
        print(f"- 没有创建版本的任务: {len(no_created_version)}")
        print(f"- 没有Git版本的任务: {len(no_git_version)}")
        print(f"- 版本号无效的任务: {len(invalid_version)}")
        print(f"- 版本号差异过大的任务: {len(large_version_diff)}")
        print(f"- quest_version表中没有对应quest的记录: {len(quest_version_no_quest)}")
        print(f"- quest_version表中没有更新版本的记录: {len(quest_version_no_updated)}")
        print(f"- quest_version表中版本号无效的记录: {len(quest_version_invalid)}")
        print(f"- quest_version表中更新版本早于创建版本的记录: {len(quest_version_older)}")
        print(f"- 总异常任务数: {total_abnormal}")

        # 自动修复异常版本
        fixed_count = 0
        if fix:
            print("开始修复异常版本...")
            processed = 0

            # 合并所有需要处理的任务
            all_tasks = []
            all_tasks.extend([("no_created_version", quest) for quest in no_created_version])
            all_tasks.extend([("no_git_version", quest) for quest in no_git_version])
            all_tasks.extend([("invalid_version", quest) for quest in invalid_version])
            all_tasks.extend([("large_version_diff", quest) for quest in large_version_diff])
            all_tasks.extend([("quest_version_no_quest", quest) for quest in quest_version_no_quest])
            all_tasks.extend([("quest_version_no_updated", quest) for quest in quest_version_no_updated])
            all_tasks.extend([("quest_version_invalid", quest) for quest in quest_version_invalid])
            all_tasks.extend([("quest_version_older", quest) for quest in quest_version_older])

            total_tasks = len(all_tasks)
            print(f"总任务数: {total_tasks}")

            # 使用LightweightProgress添加进度条
            with LightweightProgress(min(total_tasks, max_fixes), desc="修复异常版本", unit="task") as pbar:
                for task_type, quest in all_tasks:
                    if processed >= max_fixes:
                        break

                    if task_type == "no_created_version":
                        quest_id, created_version, git_version = quest
                        if git_version:
                            # 使用Git版本作为创建版本参考
                            cursor.execute(
                                "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                                (git_version, quest_id)
                            )
                            fixed_count += 1
                            processed += 1
                            # 每次修复后立即提交
                            use_conn.commit()
                            pbar.update(1)
                        else:
                            # 尝试Git回溯获取版本参考
                            # 构建任务文件路径
                            quest_file_path = f"BinOutput/Quest/{quest_id}.json"

                            # 使用Git命令获取该文件的第一个提交
                            out = _run_git(
                                repo_path,
                                ["log", "--reverse", "--format=%H", "-n", "1", "--", quest_file_path],
                                check=False
                            )
                            first_commit = out.strip() if out.strip() else None

                            version_id = None
                            if first_commit:
                                # 获取第一个提交的版本信息
                                first_commit_title = _run_git(
                                    repo_path,
                                    ["show", "-s", "--format=%s", first_commit],
                                    check=False
                                ).strip()
                                _, version_id = _resolve_commit_version(repo_path, first_commit, first_commit_title)
                            if version_id:
                                cursor.execute(
                                    "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                                    (version_id, quest_id)
                                )
                                fixed_count += 1
                                processed += 1
                                # 每次修复后立即提交
                                use_conn.commit()
                                pbar.update(1)

                    elif task_type == "no_git_version":
                        quest_id, created_version, git_version = quest
                        # 构建任务文件路径
                        quest_file_path = f"BinOutput/Quest/{quest_id}.json"

                        # 使用Git命令获取该文件的第一个提交
                        out = _run_git(
                            repo_path,
                            ["log", "--reverse", "--format=%H", "-n", "1", "--", quest_file_path],
                            check=False
                        )
                        first_commit = out.strip() if out.strip() else None

                        version_id = None
                        if first_commit:
                            # 获取第一个提交的版本信息
                            first_commit_title = _run_git(
                                repo_path,
                                ["show", "-s", "--format=%s", first_commit],
                                check=False
                            ).strip()
                            _, version_id = _resolve_commit_version(repo_path, first_commit, first_commit_title)
                        # 只更新git_created_version_id字段，不修改created_version_id
                        if version_id:
                            cursor.execute(
                                "UPDATE quest SET git_created_version_id = ? WHERE questId = ?",
                                (version_id, quest_id)
                            )
                            fixed_count += 1
                            processed += 1
                            # 每次修复后立即提交
                            use_conn.commit()
                            pbar.update(1)

                    elif task_type == "quest_version_no_quest":
                        # 删除没有对应quest的quest_version记录
                        quest_id, lang, _ = quest
                        cursor.execute(
                            "DELETE FROM quest_version WHERE questId = ? AND lang = ?",
                            (quest_id, lang)
                        )
                        fixed_count += 1
                        processed += 1
                        # 每次修复后立即提交
                        use_conn.commit()
                        pbar.update(1)

                    elif task_type == "quest_version_no_updated":
                        # 删除没有更新版本的quest_version记录
                        quest_id, lang, _ = quest
                        cursor.execute(
                            "DELETE FROM quest_version WHERE questId = ? AND lang = ?",
                            (quest_id, lang)
                        )
                        fixed_count += 1
                        processed += 1
                        # 每次修复后立即提交
                        use_conn.commit()
                        pbar.update(1)

                    elif task_type == "invalid_version":
                        # 修复版本号无效的任务
                        quest_id, created_version, git_version = quest
                        # 尝试使用Git回溯获取正确的版本
                        quest_file_path = f"BinOutput/Quest/{quest_id}.json"
                        out = _run_git(
                            repo_path,
                            ["log", "--reverse", "--format=%H", "-n", "1", "--", quest_file_path],
                            check=False
                        )
                        first_commit = out.strip() if out.strip() else None

                        version_id = None
                        if first_commit:
                            first_commit_title = _run_git(
                                repo_path,
                                ["show", "-s", "--format=%s", first_commit],
                                check=False
                            ).strip()
                            _, version_id = _resolve_commit_version(repo_path, first_commit, first_commit_title)

                        if version_id and version_id > 0:
                            # 更新无效的版本号
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
                            use_conn.commit()
                            pbar.update(1)

                    elif task_type == "large_version_diff":
                        # 处理版本号差异过大的任务
                        quest_id, textmap_version, git_version = quest
                        # 使用较小的版本作为创建版本
                        final_version = min(textmap_version, git_version)
                        cursor.execute(
                            "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                            (final_version, quest_id)
                        )
                        fixed_count += 1
                        processed += 1
                        use_conn.commit()
                        pbar.update(1)

                    elif task_type == "quest_version_invalid":
                        # 删除quest_version表中版本号无效的记录
                        quest_id, lang, _ = quest
                        cursor.execute(
                            "DELETE FROM quest_version WHERE questId = ? AND lang = ?",
                            (quest_id, lang)
                        )
                        fixed_count += 1
                        processed += 1
                        use_conn.commit()
                        pbar.update(1)

                    elif task_type == "quest_version_older":
                        # 将创建版本改成最小的updated version id
                        quest_id, lang, min_updated_version, current_created_version = quest
                        cursor.execute(
                            "UPDATE quest SET created_version_id = ? WHERE questId = ?",
                            (min_updated_version, quest_id)
                        )
                        fixed_count += 1
                        processed += 1
                        # 每次修复后立即提交
                        use_conn.commit()
                        pbar.update(1)

            print(f"修复完成，共修复 {fixed_count} 个任务的版本")
            if processed >= max_fixes:
                print(f"已达到最大修复数量 {max_fixes}，请多次运行以完成所有修复")

        return {
            "total_abnormal": total_abnormal,
            "no_created_version": len(no_created_version),
            "no_git_version": len(no_git_version),
            "invalid_version": len(invalid_version),
            "large_version_diff": len(large_version_diff),
            "quest_version_no_quest": len(quest_version_no_quest),
            "quest_version_no_updated": len(quest_version_no_updated),
            "quest_version_invalid": len(quest_version_invalid),
            "quest_version_older": len(quest_version_older),
            "fixed_count": fixed_count if fix else 0
        }
    except Exception as e:
        print(f"[ERROR] 验证任务版本时出错: {e}")
        use_conn.rollback()
        return {
            "total_abnormal": 0,
            "no_created_version": 0,
            "no_git_version": 0,
            "invalid_version": 0,
            "large_version_diff": 0,
            "quest_version_no_quest": 0,
            "quest_version_no_updated": 0,
            "quest_version_invalid": 0,
            "quest_version_older": 0,
            "fixed_count": 0
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
) -> dict[str, int | str]:
    """
    执行任务历史回填

    Args:
        target_commit: 目标提交
        from_commit: 起始提交
        force: 是否强制重新执行
        unresolved_ratio_threshold: 未解决任务比例阈值
        batch_size: 批处理大小
        verbose: 是否详细输出

    Returns:
        回填结果统计
    """
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
        refreshed_qhm = _refresh_all_quest_hash_map(cursor, batch_size=batch_size)
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

        # 新增步骤：为没有Git版本的任务执行Git回溯，获取真实的创建版本
        # 在force模式下，处理所有任务
        print("Quest history phase-1.5: Git history backfill for quests")
        if force:
            print("Force mode: backfilling git versions for all quests...")
            cursor.execute("SELECT questId FROM quest")
        else:
            print("Standard mode: backfilling git versions for quests without git version...")
            cursor.execute("SELECT questId FROM quest WHERE git_created_version_id IS NULL")
        quest_ids_to_backfill = [row[0] for row in cursor.fetchall()]
        git_backfilled_count = 0

        if quest_ids_to_backfill:
            print(f"需要Git回溯的任务数量: {len(quest_ids_to_backfill)}")
            total_quests = len(quest_ids_to_backfill)
            with LightweightProgress(total_quests, desc="Git backfill", unit="quests") as pbar:
                for i, quest_id in enumerate(quest_ids_to_backfill):
                    pbar.set_postfix_str(f"Quest {quest_id}")
                    try:
                        # 构建任务文件路径
                        quest_file_path = f"BinOutput/Quest/{quest_id}.json"

                        # 使用Git命令获取该文件的第一个提交
                        out = _run_git(
                            repo_path,
                            ["log", "--reverse", "--format=%H", "-n", "1", "--", quest_file_path],
                            check=False
                        )
                        first_commit = out.strip() if out.strip() else None

                        if first_commit:
                            # 获取第一个提交的版本信息
                            first_commit_title = _run_git(
                                repo_path,
                                ["show", "-s", "--format=%s", first_commit],
                                check=False
                            ).strip()
                            first_version_label, first_version_id = _resolve_commit_version(repo_path, first_commit, first_commit_title)

                            if first_version_id:
                                # 检查是否已经有git创建版本
                                cursor.execute(
                                    "SELECT git_created_version_id FROM quest WHERE questId = ?",
                                    (quest_id,)
                                )
                                existing_version = cursor.fetchone()
                                existing_git_created_version = existing_version[0] if existing_version else None

                                # 获取当前数据库中的任务内容和版本信息
                                cursor.execute(
                                    "SELECT questId, titleTextMapHash, created_version_id FROM quest WHERE questId = ?",
                                    (quest_id,)
                                )
                                db_row = cursor.fetchone()
                                if not db_row:
                                    continue

                                existing_created_version = db_row[2] if len(db_row) >= 3 else None

                                # 只有当git创建版本为空或小于当前版本，或者创建版本为空或git版本小于当前创建版本时才更新
                                if (existing_git_created_version is None or first_version_id < existing_git_created_version) or \
                                   (existing_created_version is None or first_version_id < existing_created_version):
                                    # 获取第一个提交的任务内容
                                    first_obj = _git_show_json(repo_path, first_commit, quest_file_path)
                                    if not isinstance(first_obj, dict):
                                        continue
                                    first_row = _extract_quest_row(first_obj)
                                    if first_row is None:
                                        continue

                                    # 检查内容是否相同
                                    content_same = (_quest_text_signature(db_row) == _quest_text_signature(first_row))

                                    # 更新Git版本和创建版本到数据库
                                    cursor.execute(
                                        "UPDATE quest SET git_created_version_id = ?, created_version_id = ? WHERE questId = ?",
                                        (first_version_id, first_version_id, quest_id)
                                    )
                                    git_backfilled_count += 1
                    except Exception as e:
                        print(f"[ERROR] Git回溯失败 for quest {quest_id}: {e}")
                        pass
                    finally:
                        pbar.update()

            conn.commit()
            print(f"Quest history phase-1.5 done: Git backfilled {git_backfilled_count} quests")
        else:
            print("所有任务都已有Git版本，跳过Git回溯")

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
            # 简化进度显示，只显示主要进度
            print(f"Processing {total_commits} Quest commits in {replay_mode} mode...")
            try:
                target_quest_ids = unresolved_created_ids if replay_mode == "targeted" else None
                commit_batch_size = 10
                processed_commits = 0
                last_commit_sha = None
                with LightweightProgress(total_commits, desc="Quest backfill", unit="commits") as pbar:
                    for idx in range(start_idx, total_commits):
                        commit_sha, commit_title = commits[idx]
                        last_commit_sha = commit_sha
                        parent_sha = commits[idx - 1][0] if idx > 0 else first_parent_sha
                        version_label, version_id = _resolve_commit_version(repo_path, commit_sha, commit_title)
                        if version_id is None:
                            pbar.update()
                            continue

                        pbar.set_postfix_str(f"Commit {commit_sha[:8]}")

                        entries = (
                            _initial_entries(repo_path, commit_sha, include_paths=["BinOutput/Quest"])
                            if parent_sha is None
                            else _diff_entries(repo_path, parent_sha, commit_sha, include_paths=["BinOutput/Quest"])
                        )

                        for entry in entries:
                            try:
                                updated = _backfill_quest_version_by_commit_entry(
                                    cursor,
                                    repo_path=repo_path,
                                    commit_sha=commit_sha,
                                    parent_sha=parent_sha,
                                    entry=entry,
                                    version_id=version_id,
                                    target_quest_ids=target_quest_ids,
                                )
                                phase2_commit_created_backfilled += updated
                            except Exception as e:
                                print(f"Error processing quest entry: {e}")

                        pbar.update()
                        processed_commits += 1

                        # 每处理一定数量的提交后进行一次数据库提交，减少提交次数
                        if processed_commits % commit_batch_size == 0 and last_commit_sha:
                            # 批量提交并更新缓存点
                            conn.commit()
                            set_meta(resume_target_key, resume_scope)
                            set_meta(resume_done_key, last_commit_sha)

                    # 处理完所有提交后，确保进行最后一次提交
                    if processed_commits > 0 and last_commit_sha:
                        conn.commit()
                        set_meta(resume_target_key, resume_scope)
                        set_meta(resume_done_key, last_commit_sha)

            except BaseException:
                conn.rollback()
                print(
                    "Quest history backfill interrupted; checkpoint saved, rerun to continue.",
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
        if commits:
            set_meta(meta_title_key, _latest_commit_meta_title(commits) or commits[-1][0])
    # 只有在完全处理完所有提交后才清空缓存点
    # 这样在处理过程中中断时，下次运行可以从上次的位置继续
    if start_idx >= total_commits:
        set_meta(resume_target_key, "")
        set_meta(resume_done_key, "")
    rebuild_version_catalog(["quest"])

    # 执行任务版本异常验证
    print("\n=== 任务版本异常验证 ===")
    validate_quest_versions(fix=False)

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
    显式重置版本标记为NULL，用于选定的历史回填范围
    这是有意与--force分开的，以便正常的回填运行不会清除数据
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
            if table_name == 'quest':
                # 对于quest表，重置created_version_id和git_created_version_id，因为updated_version_id现在存储在quest_version表中
                cursor.execute(f"UPDATE {table_name} SET created_version_id=NULL, git_created_version_id=NULL")
                # 同时清空quest_version表
                cursor.execute("DELETE FROM quest_version")
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

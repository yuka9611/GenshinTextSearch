import os
import re
import json
import subprocess
from datetime import datetime, timezone

from lightweight_progress import LightweightProgress

from DBConfig import conn, DATA_PATH, LANG_PATH
import DBBuild
import voiceItemImport
import readableImport
import subtitleImport
import textMapImport
from git_utils import resolve_commit as _resolve_commit, run_git as _run_git
from import_utils import print_skip_summary as _print_skip_summary
from textmap_name_utils import parse_textmap_file_name, textmap_file_sort_key, analyze_textmap_version_exceptions, analyze_readable_version_exceptions, analyze_subtitle_version_exceptions, report_version_exceptions
from version_control import (
    ensure_version_schema,
    get_or_create_version_id,
    rebuild_version_catalog,
    set_current_version,
)
from versioning import resolve_version_label


SOURCE_REPO_URL = "https://gitlab.com/Dimbreath/AnimeGameData.git"
DIFF_RESUME_RANGE_KEY = "db_diffupdate_resume_range"
DIFF_RESUME_STAGE_KEY = "db_diffupdate_resume_stage"
def _print_anomaly_summary(anomalies: list[str]):
    if not anomalies:
        print("[ANOMALY] no non-fatal anomalies detected.")
        return
    print("[ANOMALY] summary:")
    for idx, message in enumerate(anomalies, start=1):
        print(f"  {idx}. {message}")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def _list_remotes(repo_path: str) -> set[str]:
    out = _run_git(repo_path, ["remote"], check=False)
    remotes = {line.strip() for line in out.splitlines() if line.strip()}
    if not remotes:
        remotes.add("origin")
    return remotes


def _normalize_remote_ref(repo_path: str, remote_ref: str) -> tuple[str, str, str | None]:
    """
    Normalize user input remote ref to a resolvable target revision.
    Returns (normalized_ref, remote_name, remote_branch_for_pull).
    """
    remotes = _list_remotes(repo_path)
    raw = (remote_ref or "").strip()
    if not raw:
        raw = "origin/master"

    if raw.startswith("refs/remotes/"):
        # refs/remotes/origin/master -> origin/master
        rest = raw[len("refs/remotes/") :]
        if "/" in rest:
            remote_name, branch = rest.split("/", 1)
            return f"{remote_name}/{branch}", remote_name, branch
        return raw, "origin", None

    if raw.startswith("refs/heads/"):
        # refs/heads/master -> origin/master
        branch = raw[len("refs/heads/") :]
        return f"origin/{branch}", "origin", branch

    if raw.startswith("refs/"):
        # tags or other fully qualified refs
        return raw, "origin", None

    if "/" in raw:
        head, tail = raw.split("/", 1)
        if head in remotes:
            # e.g. origin/master
            return raw, head, tail
        # e.g. branch name containing slash
        return f"origin/{raw}", "origin", raw

    # e.g. "master"/"main" -> "origin/master"/"origin/main"
    return f"origin/{raw}", "origin", raw


def _try_resolve_commit(repo_path: str, rev: str) -> str | None:
    try:
        return _resolve_commit(repo_path, rev)
    except Exception:
        return None


def _pull_remote(repo_path: str, remote_name: str, remote_branch: str | None = None):
    cmd = ["git", "-C", repo_path, "pull", "--ff-only", "--progress", "--prune", remote_name]
    if remote_branch:
        cmd.append(remote_branch)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    last_progress = 0
    generic_phase = 0
    generic_last_percent = -1
    output_lines: list[str] = []

    def _apply_progress_mapped(mapped: int, status: str = ""):
        nonlocal last_progress
        mapped = max(0, min(100, int(mapped)))
        if mapped > last_progress:
            pbar.update(mapped - last_progress, postfix=status or None)
            last_progress = mapped
        elif status:
            pbar.set_postfix_str(status)

    def _apply_progress(stage: str, percent: int):
        if stage == "Receiving objects":
            mapped = min(70, int(percent * 0.7))
        elif stage == "Resolving deltas":
            mapped = 70 + int(percent * 0.3)
        else:
            # Counting/Compressing stages run before Receiving; keep them below 35%.
            mapped = min(35, int(percent * 0.35))
        _apply_progress_mapped(mapped, f"{stage} {percent}%")

    def _apply_generic_progress(percent: int):
        nonlocal generic_phase, generic_last_percent
        # Localized git output may still contain percentages but not English stage names.
        # Detect phase boundaries by large percent drops (e.g. 100 -> 0).
        if generic_last_percent >= 0 and percent + 5 < generic_last_percent and generic_phase < 3:
            generic_phase += 1
        generic_last_percent = percent
        bases = (0, 15, 35, 70)
        spans = (15, 20, 35, 30)
        mapped = bases[generic_phase] + int(percent * spans[generic_phase] / 100)
        _apply_progress_mapped(mapped, f"{percent}%")

    def _consume_progress_line(line: str):
        if not line:
            return
        output_lines.append(line)
        stage_match = re.search(
            r"(Counting objects|Compressing objects|Receiving objects|Resolving deltas):\s+(\d+)%",
            line,
        )
        if stage_match:
            _apply_progress(stage_match.group(1), int(stage_match.group(2)))
            return
        percent_match = re.search(r"(\d{1,3})%", line)
        if percent_match:
            percent = max(0, min(100, int(percent_match.group(1))))
            _apply_generic_progress(percent)

    with LightweightProgress(100, desc=f"git pull {remote_name}", unit="%") as pbar:
        buffer = ""
        while True:
            chunk = proc.stdout.read(1) if proc.stdout else ""
            if chunk == "":
                break
            if chunk in ("\r", "\n"):
                line = buffer.strip()
                buffer = ""
                _consume_progress_line(line)
            else:
                buffer += chunk

        line = buffer.strip()
        _consume_progress_line(line)

        return_code = proc.wait()
        if return_code == 0 and last_progress < 100:
            pbar.update(100 - last_progress)

    if return_code != 0:
        err_text = "\n".join(output_lines[-20:]).strip()
        raise RuntimeError(err_text or "git pull failed")


def _diff_name_status(repo_path: str, from_commit: str, to_commit: str) -> list[dict]:
    out = _run_git(
        repo_path,
        ["diff", "--name-status", "--find-renames", f"{from_commit}..{to_commit}"],
        check=True,
    )
    if not out:
        return []

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


def _ensure_version_tables():
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS source_file_version (
            path TEXT PRIMARY KEY,
            created_version TEXT NOT NULL,
            last_updated_version TEXT NOT NULL,
            last_change_type TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    cur.close()


def _meta_get(key: str, default: str | None = None) -> str | None:
    cur = conn.cursor()
    row = cur.execute("SELECT v FROM app_meta WHERE k=?", (key,)).fetchone()
    cur.close()
    return row[0] if row else default


def _meta_set_many(kv: dict[str, str]):
    cur = conn.cursor()
    rows = [(k, v) for k, v in kv.items()]
    cur.executemany("INSERT OR REPLACE INTO app_meta(k, v) VALUES (?, ?)", rows)
    conn.commit()
    cur.close()


def _clear_diff_resume_state():
    _meta_set_many(
        {
            DIFF_RESUME_RANGE_KEY: "",
            DIFF_RESUME_STAGE_KEY: "",
        }
    )


def _record_source_file_versions(diff_entries: list[dict], version_label: str):
    if not diff_entries:
        return

    cur = conn.cursor()
    now_iso = _utc_now_iso()
    upsert_sql = (
        "INSERT INTO source_file_version(path, created_version, last_updated_version, last_change_type, updated_at) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(path) DO UPDATE SET "
        "last_updated_version=excluded.last_updated_version, "
        "last_change_type=excluded.last_change_type, "
        "updated_at=excluded.updated_at "
        "WHERE "
        "NOT (source_file_version.last_updated_version IS excluded.last_updated_version) "
        "OR NOT (source_file_version.last_change_type IS excluded.last_change_type)"
    )

    # 批量处理
    batch_size = 100
    batch = []
    for entry in diff_entries:
        action = entry["action"]
        path = entry["old_path"] if action == "D" else entry["new_path"]
        if not path:
            continue
        batch.append((path, version_label, version_label, action, now_iso))

        # 达到批量大小，执行批量插入
        if len(batch) >= batch_size:
            cur.executemany(upsert_sql, batch)
            batch = []

    # 处理剩余的记录
    if batch:
        cur.executemany(upsert_sql, batch)

    conn.commit()
    cur.close()


def _normalize_path(p: str | None) -> str | None:
    if p is None:
        return None
    return p.replace("\\", "/")


def _resolve_talk_keys(obj: dict):
    if not isinstance(obj, dict):
        return None
    if DBBuild._is_non_dialog_talk_obj(obj):
        return None
    if "talkId" in obj and "dialogList" in obj:
        return "talkId"
    if "ADHLLDAPKCM" in obj and "MOEOFGCKILF" in obj:
        return "ADHLLDAPKCM"
    if "FEOACBMDCKJ" in obj and "AAOAAFLLOJI" in obj:
        return "FEOACBMDCKJ"
    if "LBPGKDMGFBN" in obj and "LOJEOMAPIIM" in obj:
        return "LBPGKDMGFBN"
    return None


def _extract_talk_scope(file_name: str, obj: dict):
    talk_key = _resolve_talk_keys(obj)
    if talk_key is None:
        return None
    talk_id = obj.get(talk_key)
    if talk_id is None:
        return None

    coop_match = re.match(r"^Coop[\\/]([0-9]+)_[0-9]+.json$", file_name)
    coop_quest_id = int(coop_match.group(1)) if coop_match else None
    return talk_id, coop_quest_id


def _delete_talk_scope(talk_id: int, coop_quest_id: int | None):
    cur = conn.cursor()
    if coop_quest_id is None:
        cur.execute("DELETE FROM dialogue WHERE talkId=? AND coopQuestId IS NULL", (talk_id,))
    else:
        cur.execute("DELETE FROM dialogue WHERE talkId=? AND coopQuestId=?", (talk_id, coop_quest_id))
    conn.commit()
    cur.close()


def _get_blob_json(repo_path: str, commit: str, rel_git_path: str):
    try:
        text = _run_git(repo_path, ["show", f"{commit}:{rel_git_path}"], check=True)
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            print(f"[WARN] Failed to parse JSON from {rel_git_path} at commit {commit}: {str(e)}")
            return None
    except Exception as e:
        print(f"[WARN] Failed to get blob {rel_git_path} at commit {commit}: {str(e)}")
        return None


def _delete_talk_file_from_old_commit(repo_path: str, from_commit: str, talk_file_rel: str):
    git_path = f"BinOutput/Talk/{talk_file_rel.replace('\\', '/')}"
    obj = _get_blob_json(repo_path, from_commit, git_path)
    if not isinstance(obj, dict):
        return None
    scope = _extract_talk_scope(talk_file_rel, obj)
    if scope is None:
        return None
    _delete_talk_scope(scope[0], scope[1])
    return scope[0]


def _replace_talk_file_from_local(talk_file_rel: str):
    full_path = os.path.join(DATA_PATH, "BinOutput", "Talk", talk_file_rel)
    if not os.path.isfile(full_path):
        return None, False

    with open(full_path, encoding="utf-8") as f:
        obj = json.load(f)
    if DBBuild._is_non_dialog_talk_obj(obj):
        return None, False
    scope = _extract_talk_scope(talk_file_rel, obj)
    if scope is not None:
        _delete_talk_scope(scope[0], scope[1])
    talk_skipped: list[str] = []
    DBBuild.importTalk(
        talk_file_rel,
        skip_collector=talk_skipped,
        log_skip=False,
        refresh_hash_map=False,
    )
    if talk_skipped:
        return scope[0] if scope is not None else None, True
    return scope[0] if scope is not None else None, False


def _collect_textmap_groups() -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    if not os.path.isdir(LANG_PATH):
        return groups
    for file_name in os.listdir(LANG_PATH):
        parsed = parse_textmap_file_name(file_name)
        if parsed is None:
            continue
        base_name, _split_part = parsed
        groups.setdefault(base_name, []).append(file_name)
    for base_name in groups:
        groups[base_name].sort(key=textmap_file_sort_key)
    return groups


def _analyze_diff(diff_entries: list[dict]) -> dict:
    """
    分析Git diff结果，生成更新计划

    Args:
        diff_entries: Git diff结果列表

    Returns:
        包含各种文件变更信息的字典
    """
    # 初始化更新计划
    plan = {
        "talk_changed": set(),      # 变更的talk文件
        "talk_deleted": set(),      # 删除的talk文件
        "quest_changed": set(),     # 变更的quest文件
        "quest_deleted": set(),     # 删除的quest文件
        "quest_added": set(),       # 新增的quest文件
        "quest_related": False,     # 是否有quest相关变更
        "avatar": False,            # 是否有avatar变更
        "npc": False,               # 是否有npc变更
        "manual": False,            # 是否有manual变更
        "fetters": False,           # 是否有fetters变更
        "fetter_story": False,      # 是否有fetter_story变更
        "chapter": False,           # 是否有chapter变更
        "voice": False,             # 是否有voice变更
        "readable_changed": set(),  # 变更的readable文件
        "readable_deleted": set(),  # 删除的readable文件
        "readable_mapping_changed": False,  # 是否有readable映射变更
        "subtitle_changed": set(),  # 变更的subtitle文件
        "subtitle_deleted": set(),  # 删除的subtitle文件
        "subtitle_mapping_changed": False,  # 是否有subtitle映射变更
        "textmap_bases": set(),     # 变更的textmap基础名称
    }

    def handle(action: str, rel: str, old_side: bool):
        """
        处理单个文件的变更

        Args:
            action: 变更类型 (A: 新增, D: 删除, M: 修改, R: 重命名)
            rel: 文件相对路径
            old_side: 是否是旧路径（重命名时使用）
        """
        # 处理Talk文件
        if rel.startswith("BinOutput/Talk/") and rel.endswith(".json"):
            file_rel = rel[len("BinOutput/Talk/") :].replace("/", "\\")
            if action == "D" or (action.startswith("R") and old_side):
                plan["talk_deleted"].add(file_rel)
            elif not old_side:
                plan["talk_changed"].add(file_rel)
            return

        # 处理Quest文件
        if rel.startswith("BinOutput/Quest/") and rel.endswith(".json"):
            file_rel = rel[len("BinOutput/Quest/") :].replace("/", "\\")
            if action == "D" or (action.startswith("R") and old_side):
                plan["quest_deleted"].add(file_rel)
            elif not old_side:
                plan["quest_changed"].add(file_rel)
                if action == "A":
                    plan["quest_added"].add(file_rel)
            plan["quest_related"] = True
            return

        # 处理QuestBrief文件
        if rel.startswith("BinOutput/QuestBrief/"):
            plan["quest_related"] = True
            return

        if rel == "ExcelBinOutput/AnecdoteExcelConfigData.json":
            plan["quest_related"] = True
            return
        if rel.startswith("ExcelBinOutput/TalkExcelConfigData") and rel.endswith(".json"):
            plan["quest_related"] = True
            return
        if rel == "ExcelBinOutput/MainCoopExcelConfigData.json":
            plan["quest_related"] = True
            return
        if rel == "ExcelBinOutput/CoopExcelConfigData.json":
            plan["quest_related"] = True
            return
        if rel.startswith("BinOutput/Coop/") and rel.endswith(".json"):
            plan["quest_related"] = True
            return
        if rel.startswith("BinOutput/Talk/Coop/") and rel.endswith(".json"):
            plan["quest_related"] = True
            return
        if rel.startswith("BinOutput/Talk/StoryboardGroup/") and rel.endswith(".json"):
            plan["quest_related"] = True
            return

        # 处理Excel配置文件
        if rel == "ExcelBinOutput/AvatarExcelConfigData.json":
            plan["avatar"] = True
            plan["voice"] = True
            return
        if rel == "ExcelBinOutput/NpcExcelConfigData.json":
            plan["npc"] = True
            return
        if rel == "ExcelBinOutput/ManualTextMapConfigData.json":
            plan["manual"] = True
            return
        if rel == "ExcelBinOutput/FettersExcelConfigData.json":
            plan["fetters"] = True
            return
        if rel == "ExcelBinOutput/FetterStoryExcelConfigData.json":
            plan["fetter_story"] = True
            return
        if rel == "ExcelBinOutput/ChapterExcelConfigData.json":
            plan["chapter"] = True
            return

        # 处理Voice文件
        if rel.startswith("BinOutput/Voice/Items/") or rel.startswith("BinOutput/Avatar/"):
            plan["voice"] = True
            return

        # 处理Readable文件
        if rel.startswith("Readable/"):
            file_rel = rel[len("Readable/") :].replace("/", "\\")
            if action == "D" or (action.startswith("R") and old_side):
                plan["readable_deleted"].add(file_rel)
            elif not old_side:
                plan["readable_changed"].add(file_rel)
            return

        # 处理Readable映射文件
        if rel == "ExcelBinOutput/DocumentExcelConfigData.json":
            plan["readable_mapping_changed"] = True
            return

        # 处理Subtitle文件
        if rel.startswith("Subtitle/"):
            file_rel = rel[len("Subtitle/") :].replace("/", "\\")
            if action == "D" or (action.startswith("R") and old_side):
                plan["subtitle_deleted"].add(file_rel)
            elif not old_side:
                plan["subtitle_changed"].add(file_rel)
            return

        # 处理Localization文件（影响subtitle和readable映射）
        if rel == "ExcelBinOutput/LocalizationExcelConfigData.json":
            plan["subtitle_mapping_changed"] = True
            plan["readable_mapping_changed"] = True
            return

        # 处理TextMap文件
        if rel.startswith("TextMap/") and rel.endswith(".json"):
            file_name = rel[len("TextMap/") :]
            parsed = parse_textmap_file_name(file_name)
            if parsed is not None:
                plan["textmap_bases"].add(parsed[0])

    # 处理所有diff条目
    for entry in diff_entries:
        action = entry["action"]
        old_path = _normalize_path(entry.get("old_path"))
        new_path = _normalize_path(entry.get("new_path"))
        if old_path:
            handle(action, old_path, True)
        if new_path:
            handle(action, new_path, False)

    return plan


def _init_diff_update():
    """
    初始化diff更新，检查数据库结构
    """
    _ensure_version_tables()
    ensure_version_schema()
    cur = conn.cursor()
    required_tables = ("dialogue", "quest", "textMap", "readable", "subtitle")
    existing = {
        row[0]
        for row in cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?,?,?)",
            required_tables,
        ).fetchall()
    }
    cur.close()
    missing = [t for t in required_tables if t not in existing]
    if missing:
        raise RuntimeError(
            "Database schema is not initialized. Missing tables: "
            + ", ".join(missing)
            + ". Run DBInit.py + a full DBBuild.py once first."
        )


def _prepare_git_operation(repo_path, remote_ref, fetch_remote):
    """
    准备Git操作，包括拉取代码和解析提交
    """
    normalized_remote_ref, remote_name, remote_branch = _normalize_remote_ref(repo_path, remote_ref)
    if normalized_remote_ref != remote_ref:
        print(f"Normalized remote ref: {remote_ref} -> {normalized_remote_ref}")

    if fetch_remote:
        before_head = _try_resolve_commit(repo_path, "HEAD")
        _pull_remote(repo_path, remote_name, remote_branch=remote_branch)
        after_head = _try_resolve_commit(repo_path, "HEAD")
        if before_head and after_head:
            if before_head != after_head:
                print(f"Pulled local HEAD: {before_head[:8]} -> {after_head[:8]}")
            else:
                print(f"Pulled local HEAD: no change ({after_head[:8]})")

    return normalized_remote_ref, remote_name, remote_branch


def _resolve_commits(repo_path, from_commit, to_commit):
    """
    解析提交信息
    """
    target_commit = _resolve_commit(repo_path, to_commit or "HEAD")
    target_version = resolve_version_label(target_commit, repo_path=repo_path)
    base_input = from_commit or _meta_get("db_current_commit") or _meta_get("agd_last_commit")
    if not base_input:
        raise RuntimeError(
            "Missing baseline commit. Use --from-commit or run a full build once to initialize app_meta.db_current_commit."
        )
    base_commit = _resolve_commit(repo_path, base_input)

    return base_commit, target_commit, target_version


def _handle_no_changes(target_commit, normalized_remote_ref, target_version):
    """
    处理无变更的情况
    """
    print(f"No upstream changes. commit={target_commit}")
    set_current_version(target_commit, remote_ref=normalized_remote_ref, version_label=target_version)
    _meta_set_many(
        {
            "agd_last_checked_at": _utc_now_iso(),
        }
    )
    _clear_diff_resume_state()
    _print_anomaly_summary([])


def _process_textmap_stage(plan, prune_missing, target_version):
    """
    处理textmap阶段
    """
    if plan["textmap_bases"]:
        groups = _collect_textmap_groups()
        for base_name in sorted(plan["textmap_bases"]):
            files = groups.get(base_name, [])
            if files:
                textMapImport.importTextMapForDiff(
                    base_name,
                    files,
                    force_reimport=True,
                    prune_missing=prune_missing,
                    current_version=target_version,
                )


def _process_talk_stage(plan, repo_path, base_commit):
    """
    处理talk阶段
    """
    talk_skipped_files: list[str] = []
    touched_talk_ids: set[int] = set()
    anomalies = []

    # 处理删除的talk文件
    for talk_file in sorted(plan["talk_deleted"]):
        deleted_talk_id = _delete_talk_file_from_old_commit(repo_path, base_commit, talk_file)
        if deleted_talk_id is not None:
            try:
                touched_talk_ids.add(int(deleted_talk_id))
            except (ValueError, TypeError) as e:
                anomalies.append(
                    f"Invalid talk ID format in deleted file {talk_file}: {str(e)}"
                )
        else:
            anomalies.append(
                f"Failed to process deleted talk file {talk_file}"
            )

    # 处理变更的talk文件
    for talk_file in sorted(plan["talk_changed"]):
        changed_talk_id, skipped = _replace_talk_file_from_local(talk_file)
        if changed_talk_id is not None:
            try:
                touched_talk_ids.add(int(changed_talk_id))
            except (ValueError, TypeError) as e:
                anomalies.append(
                    f"Invalid talk ID format in changed file {talk_file}: {str(e)}"
                )
        else:
            anomalies.append(
                f"Failed to process changed talk file {talk_file}"
            )
        if skipped:
            talk_skipped_files.append(talk_file)

    # 刷新quest哈希映射
    if touched_talk_ids:
        try:
            DBBuild.refreshQuestHashMapByTalkIds(touched_talk_ids, commit=True)
        except Exception as e:
            anomalies.append(
                f"Failed to refresh quest hash map: {str(e)}"
            )

    _print_skip_summary("diffupdate talk", talk_skipped_files)

    if talk_skipped_files:
        anomalies.append(
            "Talk files were skipped during diff import; talk schema mapping may need update."
        )

    return anomalies


def _process_quest_stage(plan, target_version, prune_missing):
    """
    处理quest阶段
    """
    quest_file_total = len(plan["quest_changed"]) + len(plan["quest_deleted"])
    if plan["quest_related"]:
        print(
            "Quest files: "
            f"{quest_file_total} (changed={len(plan['quest_changed'])}, deleted={len(plan['quest_deleted'])})"
        )
    else:
        print("Quest files: 0")

    anomalies = []
    if plan["quest_related"]:
        quest_stats = DBBuild.importAllQuestsForDiff(
            current_version=target_version,
            sync_delete=prune_missing,
        ) or {}
        DBBuild.importQuestBriefs()
        hangout_stats = DBBuild.importAllHangoutsForDiff(
            current_version=target_version,
            sync_delete=prune_missing,
        ) or {}
        anecdote_stats = DBBuild.importAllAnecdotesForDiff(
            current_version=target_version,
            sync_delete=prune_missing,
        ) or {}
        quest_added_count = len(plan["quest_added"])
        new_quest_count = int(quest_stats.get("new_quest_count", 0) or 0)
        skipped_quest_count = int(quest_stats.get("skipped_file_count", 0) or 0)
        missing_title_count = int(quest_stats.get("missing_title_count", 0) or 0)
        no_talk_count = int(quest_stats.get("no_talk_count", 0) or 0)
        hangout_missing_title_count = int(hangout_stats.get("missing_title_count", 0) or 0)
        hangout_no_talk_count = int(hangout_stats.get("no_talk_count", 0) or 0)
        hangout_missing_coop_count = int(hangout_stats.get("missing_coop_count", 0) or 0)
        anecdote_missing_title_count = int(anecdote_stats.get("missing_title_count", 0) or 0)
        anecdote_no_talk_count = int(anecdote_stats.get("no_talk_count", 0) or 0)
        anecdote_missing_group_count = int(anecdote_stats.get("missing_group_count", 0) or 0)

        if skipped_quest_count > 0:
            anomalies.append(
                f"Quest import skipped {skipped_quest_count} files; quest schema mapping may need update."
            )
        if missing_title_count > 0:
            anomalies.append(
                f"Quest import found {missing_title_count} rows without titleTextMapHash."
            )
        if no_talk_count > 0:
            anomalies.append(
                f"Quest import found {no_talk_count} rows without talk ids."
            )
        if hangout_missing_title_count > 0:
            anomalies.append(
                f"Hangout import found {hangout_missing_title_count} rows without titleTextMapHash."
            )
        if hangout_no_talk_count > 0:
            anomalies.append(
                f"Hangout import found {hangout_no_talk_count} rows without coop talk ids."
            )
        if hangout_missing_coop_count > 0:
            anomalies.append(
                f"Hangout import found {hangout_missing_coop_count} missing coop config files."
            )
        if anecdote_missing_title_count > 0:
            anomalies.append(
                f"Anecdote import found {anecdote_missing_title_count} rows without titleTextMapHash."
            )
        if anecdote_no_talk_count > 0:
            anomalies.append(
                f"Anecdote import found {anecdote_no_talk_count} rows without storyboard talk ids."
            )
        if anecdote_missing_group_count > 0:
            anomalies.append(
                f"Anecdote import found {anecdote_missing_group_count} missing storyboard groups."
            )
        # Heuristic anomaly detection for possible mapping mismatch.
        if quest_added_count >= 3 and new_quest_count == 0:
            anomalies.append(
                f"Quest files added in diff={quest_added_count}, but new quest rows=0; please verify quest mapping."
            )
        elif quest_added_count >= 10 and new_quest_count < max(1, int(quest_added_count * 0.2)):
            anomalies.append(
                f"Quest new rows look unusually low: added_files={quest_added_count}, new_rows={new_quest_count}; possible mapping/schema change."
            )

    return anomalies


def _process_quest_by_textmap_stage(plan, target_version):
    """
    处理quest_by_textmap阶段
    """
    # 统一获取quest统计信息的函数
    def get_quest_stats(cursor):
        # 一次执行多个查询，减少数据库访问次数
        total_row = cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
        unresolved_row = cursor.execute(
            "SELECT COUNT(*) FROM quest WHERE created_version_id IS NULL"
        ).fetchone()
        total_quest = int(total_row[0] or 0) if total_row else 0
        unresolved_quest = int(unresolved_row[0] or 0) if unresolved_row else 0
        return total_quest, unresolved_quest

    if plan["textmap_bases"] or plan["quest_related"] or plan["talk_changed"] or plan["talk_deleted"]:
        import history_backfill

        version_id = get_or_create_version_id(target_version)
        if version_id is not None:
            q_cursor = conn.cursor()
            quest_delta_stats = history_backfill.apply_quest_version_delta_from_textmap(
                q_cursor,
                version_id=version_id,
                changed_hashes=None,
                version_label=target_version,
                show_progress=False,
            )
            total_quest, unresolved_quest = get_quest_stats(q_cursor)
            q_cursor.close()
            conn.commit()
            _meta_set_many(
                {
                    "quest_version_total_last_count": str(total_quest),
                    "quest_version_unresolved_last_count": str(unresolved_quest),
                    "quest_version_unresolved_created_last_count": str(unresolved_quest),
                    "quest_version_last_fastpath_at": _utc_now_iso(),
                    "quest_version_last_replay_mode": "pending",
                }
            )
            print(
                "Quest textMap fast sync: "
                f"q_tm_upd={int(quest_delta_stats.get('quest_updated_by_textmap', 0))}, "
                f"q_tm_create={int(quest_delta_stats.get('quest_created_backfilled', 0))}, "
                f"q_tm_upfill={int(quest_delta_stats.get('quest_updated_backfilled', 0))}, "
                f"unresolved={unresolved_quest}/{total_quest}, "
                f"created_null={unresolved_quest}"
            )
    else:
        q_cursor = conn.cursor()
        total_quest, unresolved_quest = get_quest_stats(q_cursor)
        q_cursor.close()
        _meta_set_many(
            {
                "quest_version_total_last_count": str(total_quest),
                "quest_version_unresolved_last_count": str(unresolved_quest),
                "quest_version_unresolved_created_last_count": str(unresolved_quest),
                "quest_version_last_fastpath_at": _utc_now_iso(),
                "quest_version_last_replay_mode": "none",
            }
        )


def _process_core_tables_stage(plan):
    """
    处理核心表阶段
    """
    if plan["avatar"]:
        DBBuild.importAvatars()
    if plan["npc"]:
        DBBuild.importNPCs()
    if plan["manual"]:
        DBBuild.importManualTextMap()
    if plan["fetters"]:
        DBBuild.importFetters()
    if plan["fetter_story"]:
        DBBuild.importFetterStories()
    if plan["chapter"]:
        DBBuild.importChapters()


def _process_voice_stage(plan, prune_missing):
    """
    处理voice阶段
    """
    if plan["voice"]:
        voiceItemImport.loadAvatars()
        voiceItemImport.importAllVoiceItems(reset=prune_missing)


def _process_readable_stage(plan, target_version):
    """
    处理readable阶段
    """
    readable_changed_files = sorted(plan["readable_changed"])
    readable_deleted_files = sorted(plan["readable_deleted"])
    readable_remap = bool(plan["readable_mapping_changed"])
    if readable_changed_files or readable_deleted_files or readable_remap:
        readableImport.importReadableByFiles(
            readable_changed_files,
            readable_deleted_files,
            current_version=target_version,
            refresh_mapping=readable_remap,
            write_versions=True,
        )


def _process_subtitle_stage(plan, target_version):
    """
    处理subtitle阶段
    """
    subtitle_changed_files = sorted(plan["subtitle_changed"])
    subtitle_deleted_files = sorted(plan["subtitle_deleted"])
    subtitle_remap = bool(plan["subtitle_mapping_changed"])
    if subtitle_changed_files or subtitle_deleted_files or subtitle_remap:
        subtitleImport.importSubtitlesByFiles(
            subtitle_changed_files,
            subtitle_deleted_files,
            current_version=target_version,
            refresh_mapping=subtitle_remap,
            write_versions=True,
        )


def _process_version_catalog_stage(plan, target_commit, base_commit):
    """
    处理version_catalog阶段
    """
    # Reuse history-backfill rules for textMap/quest/readable/subtitle version replay so
    # diff update and history replay share identical version decisions.
    history_backfill = None
    if plan["textmap_bases"]:
        if history_backfill is None:
            import history_backfill as history_backfill_module

            history_backfill = history_backfill_module
        history_backfill.backfill_textmap_versions_from_history(
            target_commit=target_commit,
            from_commit=base_commit,
            force=False,
        )
    unresolved_count = int(_meta_get("quest_version_unresolved_last_count", "0") or 0)
    unresolved_created_raw = _meta_get("quest_version_unresolved_created_last_count", "")
    if unresolved_created_raw and unresolved_created_raw.strip():
        unresolved_created_count = int(unresolved_created_raw or 0)
    else:
        unresolved_created_count = unresolved_count
    total_quest_count = int(_meta_get("quest_version_total_last_count", "0") or 0)
    unresolved_ratio = (
        (float(unresolved_created_count) / float(total_quest_count))
        if total_quest_count > 0
        else 0.0
    )
    if unresolved_created_count <= 0:
        _meta_set_many({"quest_version_last_replay_mode": "none"})
        print(
            "Quest history replay skipped: "
            f"created_null=0 (unresolved_total={unresolved_count}/{total_quest_count})"
        )
    elif plan["textmap_bases"] or plan["quest_related"] or plan["talk_changed"] or plan["talk_deleted"]:
        if history_backfill is None:
            import history_backfill as history_backfill_module

            history_backfill = history_backfill_module
        quest_replay_stats = history_backfill.backfill_quest_versions_from_history(
            target_commit=target_commit,
            from_commit=base_commit,
            force=False,
            unresolved_ratio_threshold=0.05,
        )
        replay_mode = str(quest_replay_stats.get("replay_mode", "unknown"))
        _meta_set_many({"quest_version_last_replay_mode": replay_mode})
        print(
            "Quest history replay done: "
            f"mode={replay_mode}, "
            f"unresolved={int(quest_replay_stats.get('unresolved_quests', 0))}/"
            f"{int(quest_replay_stats.get('total_quests', 0))}, "
            f"threshold=5%, pre_ratio(created_null)={unresolved_ratio * 100:.2f}%"
        )
    if plan["readable_changed"] or plan["readable_deleted"]:
        if history_backfill is None:
            import history_backfill as history_backfill_module

            history_backfill = history_backfill_module

        history_backfill.backfill_readable_versions_from_history(
            target_commit=target_commit,
            from_commit=base_commit,
            force=False,
        )
    if plan["subtitle_changed"] or plan["subtitle_deleted"]:
        if history_backfill is None:
            import history_backfill as history_backfill_module

            history_backfill = history_backfill_module

        history_backfill.backfill_subtitle_versions_from_history(
            target_commit=target_commit,
            from_commit=base_commit,
            force=False,
        )

    # 添加版本异常验证和修复
    if history_backfill is None:
        import history_backfill as history_backfill_module
        history_backfill = history_backfill_module

    print("\n=== 版本异常验证和修复 ===")

    # 验证和修复TextMap版本异常
    print("\n验证和修复TextMap版本异常...")
    check_cursor = conn.cursor()
    try:
        exception_data = analyze_textmap_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "TextMap")

        # 修复创建版本晚于更新版本的记录
        if exception_data['created_after_updated'] > 0:
            print(f"修复 TextMap 中创建版本晚于更新版本的 {exception_data['created_after_updated']} 条记录...")
            fixed = history_backfill.fix_created_after_updated_versions(check_cursor, "textMap", ["lang", "hash"])
            conn.commit()
            print(f"修复完成，共修复 {fixed} 条记录")
    finally:
        check_cursor.close()

    # 验证和修复Readable版本异常
    print("\n验证和修复Readable版本异常...")
    check_cursor = conn.cursor()
    try:
        exception_data = analyze_readable_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "Readable")

        # 修复创建版本晚于更新版本的记录
        if exception_data['created_after_updated'] > 0:
            print(f"修复 Readable 中创建版本晚于更新版本的 {exception_data['created_after_updated']} 条记录...")
            fixed = history_backfill.fix_created_after_updated_versions(check_cursor, "readable", ["fileName", "lang"])
            conn.commit()
            print(f"修复完成，共修复 {fixed} 条记录")
    finally:
        check_cursor.close()

    # 验证和修复Subtitle版本异常
    print("\n验证和修复Subtitle版本异常...")
    check_cursor = conn.cursor()
    try:
        exception_data = analyze_subtitle_version_exceptions(check_cursor)
        report_version_exceptions(exception_data, "Subtitle")

        # 修复创建版本晚于更新版本的记录
        if exception_data['created_after_updated'] > 0:
            print(f"修复 Subtitle 中创建版本晚于更新版本的 {exception_data['created_after_updated']} 条记录...")
            fixed = history_backfill.fix_created_after_updated_versions(check_cursor, "subtitle", ["subtitleKey"])
            conn.commit()
            print(f"修复完成，共修复 {fixed} 条记录")
    finally:
        check_cursor.close()

    # 验证和修复Quest版本异常
    print("\n验证和修复Quest版本异常...")
    history_backfill.validate_quest_versions(fix=True)

    # 修复创建版本或更新版本为空的条目
    print("\n=== 修复空版本条目 ===")

    # 定义通用的空版本修复函数
    def fix_null_version_entries(table_name, select_sql, build_file_path_fn, update_sql, desc):
        check_cursor = conn.cursor()
        try:
            # 查找创建版本或更新版本为空的记录
            check_cursor.execute(select_sql)
            null_version_records = check_cursor.fetchall()
            if null_version_records:
                print(f"发现 {len(null_version_records)} 条{table_name}空版本记录，尝试修复...")
                # 调用Git回溯函数修复
                history_backfill._backfill_git_versions(
                    check_cursor,
                    table_name,
                    select_sql,
                    build_file_path_fn,
                    update_sql,
                    desc
                )
            else:
                print(f"没有发现{table_name}空版本记录")
        finally:
            check_cursor.close()

    # 修复TextMap空版本条目
    print("\n修复TextMap空版本条目...")
    check_cursor = conn.cursor()
    try:
        # 查找创建版本或更新版本为空的TextMap记录
        check_cursor.execute("SELECT lang, hash FROM textMap WHERE created_version_id IS NULL OR updated_version_id IS NULL")
        null_version_records = check_cursor.fetchall()
        if null_version_records:
            print(f"发现 {len(null_version_records)} 条TextMap空版本记录，尝试修复...")
            # 导入TextMap语言映射
            textmap_lang_map = history_backfill._get_textmap_lang_id_map()
            # 调用Git回溯函数修复
            history_backfill._backfill_textmap_git_versions(check_cursor, textmap_lang_map, desc="TextMap null version fix")
        else:
            print("没有发现TextMap空版本记录")
    finally:
        check_cursor.close()

    # 修复Readable空版本条目
    print("\n修复Readable空版本条目...")
    # 定义构建文件路径的函数
    def build_readable_file_path(record):
        file_name, lang = record
        return f"Readable/{lang}/{file_name}"
    fix_null_version_entries(
        "Readable",
        "SELECT fileName, lang FROM readable WHERE created_version_id IS NULL OR updated_version_id IS NULL",
        build_readable_file_path,
        "UPDATE readable SET created_version_id = ?, updated_version_id = ? WHERE fileName = ? AND lang = ?",
        "Readable null version fix"
    )

    # 修复Subtitle空版本条目
    print("\n修复Subtitle空版本条目...")
    # 定义构建文件路径的函数
    def build_subtitle_file_path(record):
        subtitle_key, = record
        # 解析subtitleKey获取文件路径信息
        parts = subtitle_key.split('_')
        if len(parts) < 4:
            return None
        # 重建文件路径
        file_name = '_'.join(parts[:-3])
        lang_part = parts[-3]
        # 查找对应的语言目录
        lang_dir = None
        from lang_constants import LANG_CODE_MAP
        for lang_name, lang_id in LANG_CODE_MAP.items():
            if str(lang_id) == lang_part:
                lang_dir = lang_name
                break
        if not lang_dir:
            return None
        return f"Subtitle/{lang_dir}/{file_name}.srt"
    fix_null_version_entries(
        "Subtitle",
        "SELECT subtitleKey FROM subtitle WHERE created_version_id IS NULL OR updated_version_id IS NULL",
        build_subtitle_file_path,
        "UPDATE subtitle SET created_version_id = ?, updated_version_id = ? WHERE subtitleKey = ?",
        "Subtitle null version fix"
    )

    version_scopes: list[str] = []
    if plan["textmap_bases"]:
        version_scopes.append("textMap")
    if plan["textmap_bases"] or plan["quest_related"] or plan["talk_changed"] or plan["talk_deleted"]:
        version_scopes.append("quest")
    if plan["subtitle_changed"] or plan["subtitle_deleted"]:
        version_scopes.append("subtitle")
    if plan["readable_changed"] or plan["readable_deleted"]:
        version_scopes.append("readable")

    if version_scopes:
        catalog_stats = rebuild_version_catalog(version_scopes)
        stat_text = ", ".join(f"{k}={catalog_stats.get(k, 0)}" for k in version_scopes)
        print(f"Version catalog refreshed: {stat_text}")
    else:
        print("Version catalog refresh: no version-table changes detected, skip.")


def _process_finalize_stage(target_commit, normalized_remote_ref, target_version):
    """
    处理finalize阶段
    """
    set_current_version(target_commit, remote_ref=normalized_remote_ref, version_label=target_version)
    _meta_set_many(
        {
            "agd_last_checked_at": _utc_now_iso(),
        }
    )


def run_diff_update(
    *,
    remote_ref: str = "origin/master",
    from_commit: str | None = None,
    to_commit: str | None = None,
    fetch_remote: bool = True,
    prune_missing: bool = True,
):
    """
    执行diff更新
    """
    anomalies: list[str] = []
    _init_diff_update()

    repo_path = DATA_PATH
    normalized_remote_ref, remote_name, remote_branch = _prepare_git_operation(repo_path, remote_ref, fetch_remote)
    base_commit, target_commit, target_version = _resolve_commits(repo_path, from_commit, to_commit)

    if base_commit == target_commit:
        _handle_no_changes(target_commit, normalized_remote_ref, target_version)
        return

    diff_entries = _diff_name_status(repo_path, base_commit, target_commit)
    print(f"Diff range: {base_commit}..{target_commit}, changed files: {len(diff_entries)}")
    plan = _analyze_diff(diff_entries)

    resume_range = f"{base_commit}..{target_commit}|prune={1 if prune_missing else 0}"
    current_resume_range = _meta_get(DIFF_RESUME_RANGE_KEY, "")
    current_resume_stage = _meta_get(DIFF_RESUME_STAGE_KEY, "")
    stage_order = [
        "textmap",
        "talk",
        "quest",
        "quest_by_textmap",
        "core_tables",
        "voice",
        "readable",
        "subtitle",
        "source_file_version",
        "version_catalog",
        "finalize",
    ]
    stage_index = {name: idx for idx, name in enumerate(stage_order)}
    if current_resume_range != resume_range:
        current_resume_stage = ""
        _meta_set_many(
            {
                DIFF_RESUME_RANGE_KEY: resume_range,
                DIFF_RESUME_STAGE_KEY: "",
            }
        )
    elif current_resume_stage:
        print(f"Diff update resume: continue from stage after '{current_resume_stage}'.")

    state = {"stage": current_resume_stage or ""}

    def stage_done(stage_name: str) -> bool:
        stage = state["stage"]
        if not isinstance(stage, str):
            stage = ""
        return stage_index.get(stage, -1) >= stage_index[stage_name]

    def mark_stage(stage_name: str):
        payload = {
            DIFF_RESUME_RANGE_KEY: resume_range,
            DIFF_RESUME_STAGE_KEY: stage_name,
        }
        _meta_set_many(payload)
        state["stage"] = stage_name

    # Apply textMap changes first so downstream quest version checks can
    # use the latest textMap.updated_version marks for this target version.
    if not stage_done("textmap"):
        _process_textmap_stage(plan, prune_missing, target_version)
        mark_stage("textmap")

    if not stage_done("talk"):
        talk_anomalies = _process_talk_stage(plan, repo_path, base_commit)
        anomalies.extend(talk_anomalies)
        mark_stage("talk")

    if not stage_done("quest"):
        quest_anomalies = _process_quest_stage(plan, target_version, prune_missing)
        anomalies.extend(quest_anomalies)
        mark_stage("quest")

    if not stage_done("quest_by_textmap"):
        _process_quest_by_textmap_stage(plan, target_version)
        mark_stage("quest_by_textmap")

    if not stage_done("core_tables"):
        _process_core_tables_stage(plan)
        mark_stage("core_tables")

    if not stage_done("voice"):
        _process_voice_stage(plan, prune_missing)
        mark_stage("voice")

    if not stage_done("readable"):
        _process_readable_stage(plan, target_version)
        mark_stage("readable")

    if not stage_done("subtitle"):
        _process_subtitle_stage(plan, target_version)
        mark_stage("subtitle")

    if not stage_done("source_file_version"):
        _record_source_file_versions(diff_entries, target_version)
        mark_stage("source_file_version")

    if not stage_done("version_catalog"):
        _process_version_catalog_stage(plan, target_commit, base_commit)
        mark_stage("version_catalog")

    if not stage_done("finalize"):
        _process_finalize_stage(target_commit, normalized_remote_ref, target_version)
        mark_stage("finalize")

    _clear_diff_resume_state()
    _print_anomaly_summary(anomalies)
    print("Diff update finished.")

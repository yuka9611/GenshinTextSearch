import os
import re
import json
import sys
import subprocess
from datetime import datetime, timezone

from tqdm import tqdm

from DBConfig import conn, DATA_PATH, LANG_PATH
import DBBuild
import voiceItemImport
import readableImport
import subtitleImport
import textMapImport
from textmap_name_utils import parse_textmap_file_name, textmap_file_sort_key
from versioning import (
    ensure_version_schema,
    get_or_create_version_id,
    rebuild_version_catalog,
    resolve_version_label,
    set_current_version,
)


SOURCE_REPO_URL = "https://gitlab.com/Dimbreath/AnimeGameData.git"
DIFF_RESUME_RANGE_KEY = "db_diffupdate_resume_range"
DIFF_RESUME_STAGE_KEY = "db_diffupdate_resume_stage"


def _print_skip_summary(title: str, skipped_files: list[str], sample_size: int = 10):
    if not skipped_files:
        return
    samples = skipped_files[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(skipped_files) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SKIP] {title}: {len(skipped_files)} files skipped. samples: {sample_text}")


def _print_anomaly_summary(anomalies: list[str]):
    if not anomalies:
        print("[ANOMALY] no non-fatal anomalies detected.")
        return
    print("[ANOMALY] summary:")
    for idx, message in enumerate(anomalies, start=1):
        print(f"  {idx}. {message}")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_git(repo_path: str, args: list[str], check: bool = True) -> str:
    cmd = ["git", "-C", repo_path] + args
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or "git command failed")
    return (proc.stdout or "").strip()


def _resolve_commit(repo_path: str, rev: str) -> str:
    return _run_git(repo_path, ["rev-parse", rev], check=True).strip()


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
            pbar.update(mapped - last_progress)
            last_progress = mapped
        if status:
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

    with tqdm(
        total=100,
        desc=f"git pull {remote_name}",
        unit="%",
        leave=False,
        dynamic_ncols=True,
        file=sys.stdout,
    ) as pbar:
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

    for entry in diff_entries:
        action = entry["action"]
        path = entry["old_path"] if action == "D" else entry["new_path"]
        if not path:
            continue
        cur.execute(upsert_sql, (path, version_label, version_label, action, now_iso))

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
        return json.loads(text)
    except Exception:
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
    plan = {
        "talk_changed": set(),
        "talk_deleted": set(),
        "quest_changed": set(),
        "quest_deleted": set(),
        "quest_added": set(),
        "quest_related": False,
        "avatar": False,
        "npc": False,
        "manual": False,
        "fetters": False,
        "fetter_story": False,
        "chapter": False,
        "voice": False,
        "readable_changed": set(),
        "readable_deleted": set(),
        "readable_mapping_changed": False,
        "subtitle_changed": set(),
        "subtitle_deleted": set(),
        "subtitle_mapping_changed": False,
        "textmap_bases": set(),
    }

    def handle(action: str, rel: str, old_side: bool):
        if rel.startswith("BinOutput/Talk/") and rel.endswith(".json"):
            file_rel = rel[len("BinOutput/Talk/") :].replace("/", "\\")
            if action == "D" or (action.startswith("R") and old_side):
                plan["talk_deleted"].add(file_rel)
            elif not old_side:
                plan["talk_changed"].add(file_rel)
            return

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
        if rel.startswith("BinOutput/QuestBrief/"):
            plan["quest_related"] = True
            return

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

        if rel.startswith("BinOutput/Voice/Items/") or rel.startswith("BinOutput/Avatar/"):
            plan["voice"] = True
            return

        if rel.startswith("Readable/"):
            file_rel = rel[len("Readable/") :].replace("/", "\\")
            if action == "D" or (action.startswith("R") and old_side):
                plan["readable_deleted"].add(file_rel)
            elif not old_side:
                plan["readable_changed"].add(file_rel)
            return

        if rel == "ExcelBinOutput/DocumentExcelConfigData.json":
            plan["readable_mapping_changed"] = True
            return

        if rel.startswith("Subtitle/"):
            file_rel = rel[len("Subtitle/") :].replace("/", "\\")
            if action == "D" or (action.startswith("R") and old_side):
                plan["subtitle_deleted"].add(file_rel)
            elif not old_side:
                plan["subtitle_changed"].add(file_rel)
            return

        if rel == "ExcelBinOutput/LocalizationExcelConfigData.json":
            plan["subtitle_mapping_changed"] = True
            plan["readable_mapping_changed"] = True
            return

        if rel.startswith("TextMap/") and rel.endswith(".json"):
            file_name = rel[len("TextMap/") :]
            parsed = parse_textmap_file_name(file_name)
            if parsed is not None:
                plan["textmap_bases"].add(parsed[0])

    for entry in diff_entries:
        action = entry["action"]
        old_path = _normalize_path(entry.get("old_path"))
        new_path = _normalize_path(entry.get("new_path"))
        if old_path:
            handle(action, old_path, True)
        if new_path:
            handle(action, new_path, False)

    return plan


def run_diff_update(
    *,
    remote_ref: str = "origin/master",
    from_commit: str | None = None,
    to_commit: str | None = None,
    fetch_remote: bool = True,
    prune_missing: bool = True,
):
    anomalies: list[str] = []
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

    repo_path = DATA_PATH

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

    target_commit = _resolve_commit(repo_path, to_commit or "HEAD")
    target_version = resolve_version_label(target_commit, repo_path=repo_path)
    base_input = from_commit or _meta_get("db_current_commit") or _meta_get("agd_last_commit")
    if not base_input:
        raise RuntimeError(
            "Missing baseline commit. Use --from-commit or run a full build once to initialize app_meta.db_current_commit."
        )
    base_commit = _resolve_commit(repo_path, base_input)

    if base_commit == target_commit:
        print(f"No upstream changes. commit={target_commit}")
        set_current_version(target_commit, remote_ref=normalized_remote_ref, version_label=target_version)
        _meta_set_many(
            {
                "agd_last_checked_at": _utc_now_iso(),
            }
        )
        _clear_diff_resume_state()
        _print_anomaly_summary([])
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
        if plan["textmap_bases"]:
            groups = _collect_textmap_groups()
            for base_name in sorted(plan["textmap_bases"]):
                files = groups.get(base_name, [])
                if files:
                    textMapImport.importTextMap(
                        base_name,
                        files,
                        force_reimport=True,
                        prune_missing=prune_missing,
                        current_version=target_version,
                        write_versions=True,
                    )
        mark_stage("textmap")

    talk_skipped_files: list[str] = []
    touched_talk_ids: set[int] = set()
    if not stage_done("talk"):
        for talk_file in sorted(plan["talk_deleted"]):
            deleted_talk_id = _delete_talk_file_from_old_commit(repo_path, base_commit, talk_file)
            if deleted_talk_id is not None:
                touched_talk_ids.add(int(deleted_talk_id))

        for talk_file in sorted(plan["talk_changed"]):
            changed_talk_id, skipped = _replace_talk_file_from_local(talk_file)
            if changed_talk_id is not None:
                touched_talk_ids.add(int(changed_talk_id))
            if skipped:
                talk_skipped_files.append(talk_file)
        if touched_talk_ids:
            DBBuild.refreshQuestHashMapByTalkIds(touched_talk_ids, commit=True)
        mark_stage("talk")
        _print_skip_summary("diffupdate talk", talk_skipped_files)
        if talk_skipped_files:
            anomalies.append(
                "Talk files were skipped during diff import; talk schema mapping may need update."
            )

    if not stage_done("quest"):
        quest_file_total = len(plan["quest_changed"]) + len(plan["quest_deleted"])
        if plan["quest_related"]:
            print(
                "Quest files: "
                f"{quest_file_total} (changed={len(plan['quest_changed'])}, deleted={len(plan['quest_deleted'])})"
            )
        else:
            print("Quest files: 0")

        if plan["quest_related"]:
            quest_stats = DBBuild.importAllQuests(
                current_version=target_version,
                sync_delete=prune_missing,
                write_versions=True,
            ) or {}
            DBBuild.importQuestBriefs()
            quest_added_count = len(plan["quest_added"])
            new_quest_count = int(quest_stats.get("new_quest_count", 0) or 0)
            skipped_quest_count = int(quest_stats.get("skipped_file_count", 0) or 0)
            missing_title_count = int(quest_stats.get("missing_title_count", 0) or 0)
            no_talk_count = int(quest_stats.get("no_talk_count", 0) or 0)
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
            # Heuristic anomaly detection for possible mapping mismatch.
            if quest_added_count >= 3 and new_quest_count == 0:
                anomalies.append(
                    f"Quest files added in diff={quest_added_count}, but new quest rows=0; please verify quest mapping."
                )
            elif quest_added_count >= 10 and new_quest_count < max(1, int(quest_added_count * 0.2)):
                anomalies.append(
                    f"Quest new rows look unusually low: added_files={quest_added_count}, new_rows={new_quest_count}; possible mapping/schema change."
                )
        mark_stage("quest")

    if not stage_done("quest_by_textmap"):
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
                total_row = q_cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
                unresolved_row = q_cursor.execute(
                    "SELECT COUNT(*) FROM quest WHERE created_version_id IS NULL OR updated_version_id IS NULL"
                ).fetchone()
                unresolved_created_row = q_cursor.execute(
                    "SELECT COUNT(*) FROM quest WHERE created_version_id IS NULL"
                ).fetchone()
                total_quest = int(total_row[0] or 0) if total_row else 0
                unresolved_quest = int(unresolved_row[0] or 0) if unresolved_row else 0
                unresolved_created_quest = (
                    int(unresolved_created_row[0] or 0) if unresolved_created_row else 0
                )
                q_cursor.close()
                conn.commit()
                _meta_set_many(
                    {
                        "quest_version_total_last_count": str(total_quest),
                        "quest_version_unresolved_last_count": str(unresolved_quest),
                        "quest_version_unresolved_created_last_count": str(unresolved_created_quest),
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
                    f"created_null={unresolved_created_quest}"
                )
        else:
            q_cursor = conn.cursor()
            total_row = q_cursor.execute("SELECT COUNT(*) FROM quest").fetchone()
            unresolved_row = q_cursor.execute(
                "SELECT COUNT(*) FROM quest WHERE created_version_id IS NULL OR updated_version_id IS NULL"
            ).fetchone()
            unresolved_created_row = q_cursor.execute(
                "SELECT COUNT(*) FROM quest WHERE created_version_id IS NULL"
            ).fetchone()
            total_quest = int(total_row[0] or 0) if total_row else 0
            unresolved_quest = int(unresolved_row[0] or 0) if unresolved_row else 0
            unresolved_created_quest = (
                int(unresolved_created_row[0] or 0) if unresolved_created_row else 0
            )
            q_cursor.close()
            _meta_set_many(
                {
                    "quest_version_total_last_count": str(total_quest),
                    "quest_version_unresolved_last_count": str(unresolved_quest),
                    "quest_version_unresolved_created_last_count": str(unresolved_created_quest),
                    "quest_version_last_fastpath_at": _utc_now_iso(),
                    "quest_version_last_replay_mode": "none",
                }
            )
        mark_stage("quest_by_textmap")

    if not stage_done("core_tables"):
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
        mark_stage("core_tables")

    if not stage_done("voice"):
        if plan["voice"]:
            voiceItemImport.loadAvatars()
            voiceItemImport.importAllVoiceItems(reset=prune_missing)
        mark_stage("voice")

    if not stage_done("readable"):
        readable_changed_files = sorted(plan["readable_changed"])
        readable_deleted_files = sorted(plan["readable_deleted"])
        readable_remap = bool(plan["readable_mapping_changed"])
        if readable_changed_files or readable_deleted_files or readable_remap:
            readableImport.importReadableByFiles(
                readable_changed_files,
                readable_deleted_files,
                current_version=target_version,
                refresh_mapping=readable_remap,
                write_versions=False,
            )
        mark_stage("readable")

    if not stage_done("subtitle"):
        subtitle_changed_files = sorted(plan["subtitle_changed"])
        subtitle_deleted_files = sorted(plan["subtitle_deleted"])
        subtitle_remap = bool(plan["subtitle_mapping_changed"])
        if subtitle_changed_files or subtitle_deleted_files or subtitle_remap:
            subtitleImport.importSubtitlesByFiles(
                subtitle_changed_files,
                subtitle_deleted_files,
                current_version=target_version,
                refresh_mapping=subtitle_remap,
                write_versions=False,
            )
        mark_stage("subtitle")

    if not stage_done("source_file_version"):
        _record_source_file_versions(diff_entries, target_version)
        mark_stage("source_file_version")

    if not stage_done("version_catalog"):
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
                prune_missing=False,
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
                prune_missing=False,
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
                prune_missing=False,
            )
        if plan["subtitle_changed"] or plan["subtitle_deleted"]:
            if history_backfill is None:
                import history_backfill as history_backfill_module

                history_backfill = history_backfill_module

            history_backfill.backfill_subtitle_versions_from_history(
                target_commit=target_commit,
                from_commit=base_commit,
                force=False,
                prune_missing=False,
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
        mark_stage("version_catalog")

    if not stage_done("finalize"):
        set_current_version(target_commit, remote_ref=normalized_remote_ref, version_label=target_version)
        _meta_set_many(
            {
                "agd_last_checked_at": _utc_now_iso(),
            }
        )
        mark_stage("finalize")

    _clear_diff_resume_state()
    _print_anomaly_summary(anomalies)
    print("Diff update finished.")

import json
import argparse
import cProfile
import sys
import subprocess
import pstats
import time
from contextlib import contextmanager
from DBConfig import conn, DATA_PATH, DB_PATH
import voiceItemImport
import readableImport
import subtitleImport
import textMapImport
import questImport
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched, fast_import_pragmas
from version_control import (
    ensure_version_schema,
    rebuild_version_catalog,
    set_current_version,
)


class StageTimer:
    def __init__(self, enabled: bool = False):
        self.enabled = enabled
        self._records: list[tuple[str, float]] = []

    @contextmanager
    def track(self, stage_name: str):
        if not self.enabled:
            yield
            return
        start = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - start
            self._records.append((stage_name, elapsed))
            print(f"[PROFILE] stage={stage_name} elapsed={elapsed:.3f}s")

    def print_summary(self):
        if not self.enabled or not self._records:
            return
        total = sum(elapsed for _, elapsed in self._records)
        print("[PROFILE] Stage Summary:")
        for name, elapsed in sorted(self._records, key=lambda item: item[1], reverse=True):
            share = (elapsed / total * 100.0) if total > 0 else 0.0
            print(f"  - {name}: {elapsed:.3f}s ({share:.1f}%)")
        print(f"[PROFILE] total={total:.3f}s")


def _run_stage(stage_timer: StageTimer | None, stage_name: str, fn, *args, skip_asking=False, **kwargs):
    # 检查断点记录
    status = get_breakpoint_status(stage_name)

    # 如果已经选择不跳过某个阶段，则不询问
    if not skip_asking:
        if status == 'completed':
            # 如果阶段已完成，询问是否重新执行
            ans = input(f"{stage_name} already completed. Re-execute? (y/n): ")
            if ans != 'y':
                print(f"Skipping {stage_name} (already completed)...")
                return True  # 返回 True 表示跳过了该阶段
        elif status == 'in_progress':
            # 如果阶段正在进行中，询问是否继续执行
            ans = input(f"{stage_name} is in progress. Continue? (y/n): ")
            if ans != 'y':
                print(f"Skipping {stage_name} (in progress)...")
                return True  # 返回 True 表示跳过了该阶段
        else:
            # 如果没有断点记录，询问是否跳过
            ans = input(f"Skip {stage_name}? (y/n): ")
            if ans == 'y':
                print(f"Skipping {stage_name}...")
                return True  # 返回 True 表示跳过了该阶段

    print(f"Importing {stage_name}...")
    # 记录开始时间和状态
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    update_breakpoint_status(stage_name, 'in_progress', start_time)

    try:
        if stage_timer is None:
            fn(*args, **kwargs)
        else:
            with stage_timer.track(stage_name):
                fn(*args, **kwargs)
        # 记录完成时间和状态
        end_time = time.strftime('%Y-%m-%d %H:%M:%S')
        update_breakpoint_status(stage_name, 'completed', start_time, end_time)
        return False  # 返回 False 表示没有跳过该阶段
    except Exception as e:
        print(f"Error in {stage_name}: {e}")
        raise


def _dump_profile_stats(
    profiler: cProfile.Profile,
    *,
    sort_key: str = "cumulative",
    top_n: int = 40,
    stats_file: str = "",
):
    if stats_file:
        profiler.dump_stats(stats_file)
        print(f"[PROFILE] stats saved: {stats_file}")
    print(f"[PROFILE] cProfile top {max(1, top_n)} (sort={sort_key})")
    stats = pstats.Stats(profiler).strip_dirs().sort_stats(sort_key)
    stats.print_stats(max(1, top_n))


def _load_json_file(path: str):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _is_non_dialog_talk_obj(obj: dict) -> bool:
    # Keep compatibility for callers (e.g. diffUpdate) that use DBBuild as facade.
    return questImport._is_non_dialog_talk_obj(obj)


def importTalk(
    fileName: str,
    *,
    cursor=None,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    refresh_hash_map: bool = True,
    touched_talk_collector: set[int] | None = None,
) -> int:
    return questImport.importTalk(
        fileName,
        cursor=cursor,
        commit=commit,
        batch_size=batch_size,
        skip_collector=skip_collector,
        log_skip=log_skip,
        refresh_hash_map=refresh_hash_map,
        touched_talk_collector=touched_talk_collector,
    )


def importAllTalkItems(
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    return questImport.importAllTalkItems(
        commit=commit,
        batch_size=batch_size,
    )


def importAvatars(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    avatars = _load_json_file(DATA_PATH + "\\ExcelBinOutput\\AvatarExcelConfigData.json")

    sql1 = (
        "INSERT INTO avatar(avatarId, nameTextMapHash) VALUES (?,?) "
        "ON CONFLICT(avatarId) DO UPDATE SET "
        "nameTextMapHash=excluded.nameTextMapHash "
        "WHERE NOT (avatar.nameTextMapHash IS excluded.nameTextMapHash)"
    )

    executemany_batched(
        cursor,
        sql1,
        ((avatar['id'], avatar['nameTextMapHash']) for avatar in avatars),
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()


def importFetters(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    fetters = _load_json_file(DATA_PATH + "\\ExcelBinOutput\\FettersExcelConfigData.json")
    sql1 = (
        "INSERT INTO fetters(fetterId, avatarId, voiceTitleTextMapHash, voiceFileTextTextMapHash, voiceFile) "
        "VALUES (?,?,?,?,?) "
        "ON CONFLICT(fetterId) DO UPDATE SET "
        "avatarId=excluded.avatarId, "
        "voiceTitleTextMapHash=excluded.voiceTitleTextMapHash, "
        "voiceFileTextTextMapHash=excluded.voiceFileTextTextMapHash, "
        "voiceFile=excluded.voiceFile "
        "WHERE "
        "NOT (fetters.avatarId IS excluded.avatarId) "
        "OR NOT (fetters.voiceTitleTextMapHash IS excluded.voiceTitleTextMapHash) "
        "OR NOT (fetters.voiceFileTextTextMapHash IS excluded.voiceFileTextTextMapHash) "
        "OR NOT (fetters.voiceFile IS excluded.voiceFile)"
    )

    executemany_batched(
        cursor,
        sql1,
        (
            (
                fetter['fetterId'],
                fetter['avatarId'],
                fetter['voiceTitleTextMapHash'],
                fetter['voiceFileTextTextMapHash'],
                fetter['voiceFile'],
            )
            for fetter in fetters
        ),
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()


def importFetterStories(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    stories = _load_json_file(DATA_PATH + "\\ExcelBinOutput\\FetterStoryExcelConfigData.json")
    sql1 = ("INSERT INTO fetterStory("
            "fetterId, avatarId, storyTitleTextMapHash, storyTitle2TextMapHash, "
            "storyTitleLockedTextMapHash, storyContextTextMapHash, storyContext2TextMapHash"
            ") VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(fetterId) DO UPDATE SET "
            "avatarId=excluded.avatarId, "
            "storyTitleTextMapHash=excluded.storyTitleTextMapHash, "
            "storyTitle2TextMapHash=excluded.storyTitle2TextMapHash, "
            "storyTitleLockedTextMapHash=excluded.storyTitleLockedTextMapHash, "
            "storyContextTextMapHash=excluded.storyContextTextMapHash, "
            "storyContext2TextMapHash=excluded.storyContext2TextMapHash "
            "WHERE "
            "NOT (fetterStory.avatarId IS excluded.avatarId) "
            "OR NOT (fetterStory.storyTitleTextMapHash IS excluded.storyTitleTextMapHash) "
            "OR NOT (fetterStory.storyTitle2TextMapHash IS excluded.storyTitle2TextMapHash) "
            "OR NOT (fetterStory.storyTitleLockedTextMapHash IS excluded.storyTitleLockedTextMapHash) "
            "OR NOT (fetterStory.storyContextTextMapHash IS excluded.storyContextTextMapHash) "
            "OR NOT (fetterStory.storyContext2TextMapHash IS excluded.storyContext2TextMapHash)")

    executemany_batched(
        cursor,
        sql1,
        (
            (
                story['fetterId'],
                story['avatarId'],
                story['storyTitleTextMapHash'],
                story['storyTitle2TextMapHash'],
                story['storyTitleLockedTextMapHash'],
                story['storyContextTextMapHash'],
                story['storyContext2TextMapHash'],
            )
            for story in stories
        ),
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()


def importQuest(
    fileName: str,
    *,
    cursor=None,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    return questImport.importQuest(
        fileName,
        cursor=cursor,
        skip_collector=skip_collector,
        log_skip=log_skip,
        missing_title_collector=missing_title_collector,
        no_talk_collector=no_talk_collector,
    )


def importAllQuests(
    sync_delete: bool = False,
):
    return questImport.importAllQuests(
        sync_delete=sync_delete,
    )


def importAllQuestsForDiff(
    current_version: str,
    sync_delete: bool = False,
):
    return questImport.importAllQuestsForDiff(
        current_version=current_version,
        sync_delete=sync_delete,
    )


def importQuestBriefs(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    return questImport.importQuestBriefs(
        commit=commit,
        batch_size=batch_size,
    )


def refreshQuestHashMapByTalkIds(
    talk_ids,
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    return questImport.refreshQuestHashMapByTalkIds(
        talk_ids,
        commit=commit,
        batch_size=batch_size,
    )


def refreshQuestHashMapByQuestIds(
    quest_ids,
    *,
    commit: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    return questImport.refreshQuestHashMapByQuestIds(
        quest_ids,
        commit=commit,
        batch_size=batch_size,
    )


def importChapters(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    chapters = _load_json_file(DATA_PATH + "\\ExcelBinOutput\\ChapterExcelConfigData.json")
    sql1 = (
        "INSERT INTO chapter(chapterId, chapterTitleTextMapHash, chapterNumTextMapHash) VALUES (?,?,?) "
        "ON CONFLICT(chapterId) DO UPDATE SET "
        "chapterTitleTextMapHash=excluded.chapterTitleTextMapHash, "
        "chapterNumTextMapHash=excluded.chapterNumTextMapHash "
        "WHERE "
        "NOT (chapter.chapterTitleTextMapHash IS excluded.chapterTitleTextMapHash) "
        "OR NOT (chapter.chapterNumTextMapHash IS excluded.chapterNumTextMapHash)"
    )

    executemany_batched(
        cursor,
        sql1,
        ((chapter['id'], chapter['chapterTitleTextMapHash'], chapter['chapterNumTextMapHash']) for chapter in chapters),
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()


def importNPCs(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    NPCs = _load_json_file(DATA_PATH + "\\ExcelBinOutput\\NpcExcelConfigData.json")

    sql1 = (
        "INSERT INTO npc(npcId, textHash) VALUES (?,?) "
        "ON CONFLICT(npcId) DO UPDATE SET "
        "textHash=excluded.textHash "
        "WHERE NOT (npc.textHash IS excluded.textHash)"
    )

    executemany_batched(
        cursor,
        sql1,
        ((npc['id'], npc['nameTextMapHash']) for npc in NPCs),
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()


def importManualTextMap(*, commit: bool = True, batch_size: int = DEFAULT_BATCH_SIZE):
    cursor = conn.cursor()
    placeholders = _load_json_file(DATA_PATH + "\\ExcelBinOutput\\ManualTextMapConfigData.json")
    sql1 = (
        "INSERT INTO manualTextMap(textMapId, textHash) VALUES (?,?) "
        "ON CONFLICT(textMapId) DO UPDATE SET "
        "textHash=excluded.textHash "
        "WHERE NOT (manualTextMap.textHash IS excluded.textHash)"
    )

    executemany_batched(
        cursor,
        sql1,
        ((placeholder['textMapId'], placeholder['textMapContentTextMapHash']) for placeholder in placeholders),
        batch_size=batch_size,
    )

    cursor.close()
    if commit:
        conn.commit()

def main(
    *,
    prune_missing: bool = True,
    enable_stage_profile: bool = False,
    use_fast_pragmas: bool = True,
    clean_breakpoint: bool = False,
):
    stage_timer = StageTimer(enabled=enable_stage_profile)
    ensure_version_schema()
    # 确保断点表结构存在
    ensure_breakpoint_schema()

    # 如果需要清理断点，则清理
    if clean_breakpoint:
        print("Clearing breakpoints...")
        clear_breakpoints()

    # 定义所有导入阶段
    import_stages = [
        "talks",
        "avatars",
        "npcs",
        "manual_textmap",
        "fetters",
        "fetter_stories",
        "quests",
        "quest_briefs",
        "chapters",
        "load_voice_avatars",
        "voices",
        "readable",
        "subtitles",
        "textmap",
        "version_catalog"
    ]

    # 一次性询问所有阶段是否跳过
    skip_decisions = ask_all_stages_skip(import_stages)

    with fast_import_pragmas(conn, enabled=use_fast_pragmas):
        # 执行各个阶段
        for stage in import_stages:
            if skip_decisions[stage]:
                print(f"Skipping {stage}...")
                continue

            if stage == "talks":
                _run_stage(stage_timer, stage, importAllTalkItems, skip_asking=True)
            elif stage == "avatars":
                _run_stage(stage_timer, stage, importAvatars, skip_asking=True)
            elif stage == "npcs":
                _run_stage(stage_timer, stage, importNPCs, skip_asking=True)
            elif stage == "manual_textmap":
                _run_stage(stage_timer, stage, importManualTextMap, skip_asking=True)
            elif stage == "fetters":
                _run_stage(stage_timer, stage, importFetters, skip_asking=True)
            elif stage == "fetter_stories":
                _run_stage(stage_timer, stage, importFetterStories, skip_asking=True)
            elif stage == "quests":
                _run_stage(
                    stage_timer,
                    stage,
                    importAllQuests,
                    sync_delete=prune_missing,
                    skip_asking=True,
                )
            elif stage == "quest_briefs":
                _run_stage(stage_timer, stage, importQuestBriefs, skip_asking=True)
            elif stage == "chapters":
                _run_stage(stage_timer, stage, importChapters, skip_asking=True)
            elif stage == "load_voice_avatars":
                _run_stage(stage_timer, stage, voiceItemImport.loadAvatars, skip_asking=True)
            elif stage == "voices":
                _run_stage(stage_timer, stage, voiceItemImport.importAllVoiceItems, reset=prune_missing, skip_asking=True)
            elif stage == "readable":
                _run_stage(
                    stage_timer,
                    stage,
                    readableImport.importReadable,
                    prune_missing=prune_missing,
                    skip_asking=True,
                )
            elif stage == "subtitles":
                _run_stage(
                    stage_timer,
                    stage,
                    subtitleImport.importSubtitles,
                    prune_missing=prune_missing,
                    skip_asking=True,
                )
            elif stage == "textmap":
                _run_stage(
                    stage_timer,
                    stage,
                    textMapImport.importAllTextMap,
                    prune_missing=prune_missing,
                    skip_asking=True,
                )
            elif stage == "version_catalog":
                _run_stage(stage_timer, stage, rebuild_version_catalog, skip_asking=True)
    stage_timer.print_summary()
    print("Done!")


def set_db_version(conn, version: str):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
    cur.execute("INSERT OR REPLACE INTO app_meta(k, v) VALUES (?, ?)", ("db_version", version))
    conn.commit()


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


def clear_breakpoints():
    """清理所有断点信息"""
    cur = conn.cursor()
    cur.execute("DELETE FROM breakpoint")
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


def get_next_pending_stage(stages):
    """获取下一个待执行的阶段"""
    for stage in stages:
        status = get_breakpoint_status(stage)
        if status != 'completed':
            return stage
    return None


def ask_all_stages_skip(stages):
    """一次性询问所有阶段是否跳过"""
    skip_decisions = {}
    print("\n=== 导入和历史回填阶段配置 ===")
    print("请选择是否跳过以下阶段：")
    print("(输入 y 跳过，n 执行，默认 n)")
    print()

    for stage in stages:
        status = get_breakpoint_status(stage)
        if status == 'completed':
            prompt = f"{stage} (已完成，是否重新执行？) [n]: "
        elif status == 'in_progress':
            prompt = f"{stage} (执行中，是否继续？) [n]: "
        else:
            prompt = f"{stage} (未执行，是否跳过？) [n]: "

        ans = input(prompt).strip().lower()
        if status == 'completed':
            # 已完成的阶段，输入y表示跳过（不重新执行）
            skip_decisions[stage] = (ans == 'y')
        else:
            # 其他状态，输入y表示跳过
            skip_decisions[stage] = (ans == 'y')

    print()
    print("=== 执行计划 ===")
    for stage, skip in skip_decisions.items():
        status = get_breakpoint_status(stage)
        if status == 'completed':
            action = "跳过（不重新执行）" if skip else "重新执行"
        else:
            action = "跳过" if skip else "执行"
        print(f"{stage}: {action}")
    print()

    return skip_decisions


def _get_head_commit(repo_path: str):
    try:
        proc = subprocess.run(
            ["git", "-C", repo_path, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if proc.returncode != 0:
            return None
        return (proc.stdout or "").strip()
    except Exception:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dbver",
        "--db-version",
        type=str,
        default="",
        help="set db_version metadata only and exit, e.g. 2026-01-21.1",
    )
    parser.add_argument("--diff-update", "--diff", action="store_true", help="incremental update by git diff")
    parser.add_argument(
        "--quest-only",
        action="store_true",
        help="only import quest/questTalk data (defaults to no version writes)",
    )
    parser.add_argument(
        "--quest-only-skip-quests",
        action="store_true",
        help="when used with --quest-only, skip importing Quest/QuestBrief",
    )
    parser.add_argument(
        "--quest-only-skip-talk",
        action="store_true",
        help="when used with --quest-only, skip importing Talk",
    )

    parser.add_argument("--remote-ref", "--remote", type=str, default="origin/master", help="target remote ref for diff")
    parser.add_argument(
        "--from-commit",
        "--from",
        type=str,
        default="",
        help="base commit for diff-update, or start commit for history-backfill modes",
    )
    parser.add_argument("--to-commit", "--to", type=str, default="", help="target commit, default --remote-ref")
    parser.add_argument("--no-fetch", action="store_true", help="skip git pull in diff update")
    parser.add_argument(
        "--no-prune-missing",
        "--no-prune",
        action="store_true",
        help="keep rows not found in current source files (default: delete unseen rows)",
    )
    parser.add_argument("--profile", action="store_true", help="enable cProfile and stage timing output")
    parser.add_argument(
        "--profile-sort",
        type=str,
        default="cumulative",
        choices=["cumulative", "tottime", "time", "calls", "ncalls"],
        help="cProfile sort key",
    )
    parser.add_argument("--profile-top", type=int, default=40, help="number of top cProfile entries to print")
    parser.add_argument("--profile-stats-file", type=str, default="", help="optional .prof output path")
    parser.add_argument(
        "--no-fast-import-pragmas",
        "--no-fast-pragmas",
        action="store_true",
        help="disable temporary sqlite bulk-write tuning during full import",
    )
    parser.add_argument("--skip-history-backfill", "--skip-history", action="store_true", help="skip history version backfill")
    parser.add_argument("--force", action="store_true", help="force replay all commits for version backfill")
    parser.add_argument(
        "--history-reset-only",
        "--history-reset",
        type=str,
        default="",
        choices=["all", "textmap", "readable", "subtitle", "quest"],
        help=(
            "reset history version marks (created/updated version ids) for selected scope and exit; "
            "separate from --force"
        ),
    )
    parser.add_argument("--clean-breakpoint", action="store_true", help="clean breakpoint information and start from beginning")
    parser.add_argument("--verbose", "-v", action="store_true", help="enable verbose logging")
    args = parser.parse_args()
    prune_missing = not args.no_prune_missing
    print(f"[INFO] DB path: {DB_PATH}")
    if args.dbver:
        try:
            set_db_version(conn, args.dbver)
            print(f"[INFO] db_version set to: {args.dbver}")
            sys.exit(0)
        except Exception as e:
            print(f"[ERROR] failed to set db_version: {e}", file=sys.stderr)
            sys.exit(3)

    history_mode_count = int(bool(args.history_reset_only))
    if history_mode_count > 1:
        print(
            "[ERROR] only one history-backfill mode can be selected at a time.",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.diff_update and history_mode_count > 0:
        print(
            "[ERROR] --diff-update and --history-backfill-* flags cannot be used together.",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.quest_only and history_mode_count > 0:
        print(
            "[ERROR] --quest-only and --history-backfill-* flags cannot be used together.",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.quest_only and args.diff_update:
        print(
            "[ERROR] --quest-only and --diff-update cannot be used together.",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.quest_only_skip_quests and not args.quest_only:
        print(
            "[ERROR] --quest-only-skip-quests can only be used with --quest-only.",
            file=sys.stderr,
        )
        sys.exit(2)
    if args.quest_only_skip_talk and not args.quest_only:
        print(
            "[ERROR] --quest-only-skip-talk can only be used with --quest-only.",
            file=sys.stderr,
        )
        sys.exit(2)
    profiler = cProfile.Profile() if args.profile else None
    if profiler is not None:
        profiler.enable()

    # 1) Run selected mode: diff update, or full import.
    try:
        if args.history_reset_only:
            try:
                import history_backfill

                # 直接执行历史重置，不需要询问
                print(f"执行历史版本重置，范围: {args.history_reset_only}")
                history_backfill.reset_history_version_marks(scope=args.history_reset_only)
            except Exception as e:
                print(f"[ERROR] history version reset failed: {e}", file=sys.stderr)
                sys.exit(4)
        elif args.diff_update:
            try:
                import diffUpdate

                diffUpdate.run_diff_update(
                    remote_ref=args.remote_ref,
                    from_commit=args.from_commit or None,
                    to_commit=args.to_commit or None,
                    fetch_remote=not args.no_fetch,
                    prune_missing=prune_missing,
                )
            except Exception as e:
                print(f"[ERROR] diff update failed: {e}", file=sys.stderr)
                sys.exit(2)
        elif args.quest_only:
            try:
                include_quests = not bool(args.quest_only_skip_quests)
                include_talks = not bool(args.quest_only_skip_talk)
                print(
                    "Quest-only import mode: "
                    f"prune_missing={'yes' if prune_missing else 'no'}, "
                    f"run_quests={'yes' if include_quests else 'no'}, "
                    f"run_talks={'yes' if include_talks else 'no'}"
                )
                quest_stats = questImport.runQuestOnly(
                    prune_missing=prune_missing,
                    include_quests=include_quests,
                    include_talks=include_talks,
                )
                print(
                    "Quest-only import done: "
                    f"talk_rows={quest_stats.get('talk_rows_imported', 0)}, "
                    f"imported={quest_stats.get('imported_quest_count', 0)}, "
                    f"new={quest_stats.get('new_quest_count', 0)}, "
                    f"skipped={quest_stats.get('skipped_file_count', 0)}"
                )
            except Exception as e:
                print(f"[ERROR] quest-only import failed: {e}", file=sys.stderr)
                sys.exit(2)
        else:
            # 询问是否跳过导入
            skip_import = False
            ans = input("是否跳过导入，直接开始历史回填？(y/n): ").strip().lower()
            if ans == 'y':
                skip_import = True
                print("跳过导入，直接开始历史回填...")
            else:
                # 询问是否使用diffupdate
                use_diffupdate = False
                ans = input("是否使用diffupdate更新至最新commit？(y/n): ").strip().lower()
                if ans == 'y':
                    use_diffupdate = True
                    print("使用diffupdate更新至最新commit...")
                    try:
                        import diffUpdate
                        diffUpdate.run_diff_update(
                            remote_ref=args.remote_ref,
                            from_commit=args.from_commit or None,
                            to_commit=args.to_commit or None,
                            fetch_remote=not args.no_fetch,
                            prune_missing=prune_missing,
                        )
                    except Exception as e:
                        print(f"[ERROR] diff update failed: {e}", file=sys.stderr)
                        sys.exit(2)
                else:
                    # 执行完整导入
                    head_commit = _get_head_commit(DATA_PATH)
                    if head_commit:
                        set_current_version(head_commit)
                    main(
                        prune_missing=prune_missing,
                        enable_stage_profile=args.profile,
                        use_fast_pragmas=not args.no_fast_import_pragmas,
                        clean_breakpoint=args.clean_breakpoint,
                    )

            # 无论是否跳过导入，都询问是否执行历史回填
            if not args.skip_history_backfill:
                try:
                    import history_backfill

                    # 询问是否执行历史回填各阶段
                    history_stages = [
                        "history_backfill_textmap",
                        "history_backfill_readable",
                        "history_backfill_subtitle",
                        "history_backfill_quest"
                    ]
                    history_skip_decisions = ask_all_stages_skip(history_stages)

                    # 执行选中的历史回填阶段
                    head_commit = _get_head_commit(DATA_PATH)
                    if not head_commit:
                        print("[ERROR] 无法获取HEAD提交，无法执行历史回填。", file=sys.stderr)
                    else:
                        # 创建一个临时的StageTimer对象，用于历史回填阶段
                        history_stage_timer = StageTimer(enabled=args.profile)

                        if not history_skip_decisions["history_backfill_textmap"]:
                            _run_stage(
                                history_stage_timer,
                                "history_backfill_textmap",
                                history_backfill.backfill_textmap_versions_from_history,
                                target_commit=head_commit,
                                force=args.force,
                                verbose=args.verbose,
                                skip_asking=True
                            )

                        if not history_skip_decisions["history_backfill_readable"]:
                            _run_stage(
                                history_stage_timer,
                                "history_backfill_readable",
                                history_backfill.backfill_readable_versions_from_history,
                                target_commit=head_commit,
                                force=args.force,
                                verbose=args.verbose,
                                skip_asking=True
                            )

                        if not history_skip_decisions["history_backfill_subtitle"]:
                            _run_stage(
                                history_stage_timer,
                                "history_backfill_subtitle",
                                history_backfill.backfill_subtitle_versions_from_history,
                                target_commit=head_commit,
                                force=args.force,
                                verbose=args.verbose,
                                skip_asking=True
                            )

                        if not history_skip_decisions["history_backfill_quest"]:
                            _run_stage(
                                history_stage_timer,
                                "history_backfill_quest",
                                history_backfill.backfill_quest_versions_from_history,
                                target_commit=head_commit,
                                force=args.force,
                                verbose=args.verbose,
                                skip_asking=True
                            )

                        # 打印历史回填阶段的性能统计
                        history_stage_timer.print_summary()
                except Exception as e:
                    print(f"[ERROR] history backfill failed: {e}", file=sys.stderr)
                    sys.exit(4)
    finally:
        if profiler is not None:
            profiler.disable()
            _dump_profile_stats(
                profiler,
                sort_key=args.profile_sort,
                top_n=args.profile_top,
                stats_file=args.profile_stats_file,
            )

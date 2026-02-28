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
from versioning import (
    ensure_version_schema,
    get_current_version,
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
            result = fn(*args, **kwargs)
        else:
            with stage_timer.track(stage_name):
                result = fn(*args, **kwargs)
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
    current_version: str | None = None,
    *,
    cursor=None,
    write_versions: bool = True,
    skip_collector: list[str] | None = None,
    log_skip: bool = True,
    missing_title_collector: list[str] | None = None,
    no_talk_collector: list[str] | None = None,
) -> tuple[int | None, bool]:
    return questImport.importQuest(
        fileName,
        current_version=current_version,
        cursor=cursor,
        write_versions=write_versions,
        skip_collector=skip_collector,
        log_skip=log_skip,
        missing_title_collector=missing_title_collector,
        no_talk_collector=no_talk_collector,
    )


def importAllQuests(
    current_version: str | None = None,
    sync_delete: bool = False,
    *,
    write_versions: bool = True,
):
    return questImport.importAllQuests(
        current_version=current_version,
        sync_delete=sync_delete,
        write_versions=write_versions,
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
    current_version: str | None = None,
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

    version = current_version or get_current_version()
    with fast_import_pragmas(conn, enabled=use_fast_pragmas):
        # 跟踪是否已经选择不跳过某个阶段
        # 如果有任何一个阶段选择了不跳过，则后续阶段不需要询问
        no_skip_any = False

        # 执行各个阶段
        skipped = _run_stage(stage_timer, "talks", importAllTalkItems, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "avatars", importAvatars, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "npcs", importNPCs, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "manual_textmap", importManualTextMap, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "fetters", importFetters, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "fetter_stories", importFetterStories, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(
            stage_timer,
            "quests",
            importAllQuests,
            current_version=version,
            write_versions=False,
            sync_delete=prune_missing,
            skip_asking=no_skip_any,
        )
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "quest_briefs", importQuestBriefs, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "chapters", importChapters, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "load_voice_avatars", voiceItemImport.loadAvatars, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(stage_timer, "voices", voiceItemImport.importAllVoiceItems, reset=prune_missing, skip_asking=no_skip_any)
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(
            stage_timer,
            "readable",
            readableImport.importReadable,
            current_version=version,
            write_versions=False,
            prune_missing=prune_missing,
            skip_asking=no_skip_any,
        )
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(
            stage_timer,
            "subtitles",
            subtitleImport.importSubtitles,
            current_version=version,
            prune_missing=prune_missing,
            skip_asking=no_skip_any,
        )
        if not skipped:
            no_skip_any = True

        skipped = _run_stage(
            stage_timer,
            "textmap",
            textMapImport.importAllTextMap,
            current_version=version,
            write_versions=False,
            prune_missing=prune_missing,
            skip_asking=no_skip_any,
        )
        if not skipped:
            no_skip_any = True

        _run_stage(stage_timer, "version_catalog", rebuild_version_catalog, skip_asking=no_skip_any)
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
        "--quest-write-versions",
        action="store_true",
        help="when used with --quest-only, also write quest created/updated version ids",
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
    parser.add_argument("--history-backfill-only", "--history", action="store_true", help="only run history version backfill")
    parser.add_argument(
        "--history-backfill-textmap-only",
        "--history-textmap",
        action="store_true",
        help="only run textMap history version backfill",
    )
    parser.add_argument(
        "--history-backfill-quest-only",
        "--history-quest",
        action="store_true",
        help="only run quest history version backfill",
    )
    parser.add_argument(
        "--history-backfill-readable-only",
        "--history-readable",
        action="store_true",
        help="only run readable history version backfill",
    )
    parser.add_argument(
        "--history-backfill-subtitle-only",
        "--history-subtitle",
        action="store_true",
        help="only run subtitle history version backfill",
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
        help="disable temporary sqlite PRAGMA speedups during full import",
    )
    parser.add_argument("--skip-history-backfill", "--skip-history", action="store_true", help="skip first-build history version backfill")
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

    history_mode_count = (
        int(bool(args.history_backfill_only))
        + int(bool(args.history_backfill_textmap_only))
        + int(bool(args.history_backfill_quest_only))
        + int(bool(args.history_backfill_readable_only))
        + int(bool(args.history_backfill_subtitle_only))
        + int(bool(args.history_reset_only))
    )
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
    if args.quest_write_versions and not args.quest_only:
        print(
            "[ERROR] --quest-write-versions can only be used with --quest-only.",
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

    # 1) Run selected mode: history-only, diff update, or full import.
    try:
        if args.history_backfill_quest_only:
            try:
                import history_backfill

                target = args.to_commit or "HEAD"
                history_backfill.backfill_quest_versions_from_history(
                    target_commit=target,
                    from_commit=args.from_commit or None,
                    force=args.force,
                    prune_missing=prune_missing,
                )
            except Exception as e:
                print(f"[ERROR] quest history backfill failed: {e}", file=sys.stderr)
                sys.exit(4)
        elif args.history_backfill_readable_only:
            try:
                import history_backfill

                target = args.to_commit or "HEAD"
                history_backfill.backfill_readable_versions_from_history(
                    target_commit=target,
                    from_commit=args.from_commit or None,
                    force=args.force,
                    prune_missing=prune_missing,
                )
            except Exception as e:
                print(f"[ERROR] readable history backfill failed: {e}", file=sys.stderr)
                sys.exit(4)
        elif args.history_backfill_subtitle_only:
            try:
                import history_backfill

                target = args.to_commit or "HEAD"
                history_backfill.backfill_subtitle_versions_from_history(
                    target_commit=target,
                    from_commit=args.from_commit or None,
                    force=args.force,
                    prune_missing=prune_missing,
                )
            except Exception as e:
                print(f"[ERROR] subtitle history backfill failed: {e}", file=sys.stderr)
                sys.exit(4)
        elif args.history_reset_only:
            try:
                import history_backfill

                history_backfill.reset_history_version_marks(scope=args.history_reset_only)
            except Exception as e:
                print(f"[ERROR] history version reset failed: {e}", file=sys.stderr)
                sys.exit(4)
        elif args.history_backfill_textmap_only:
            try:
                import history_backfill

                target = args.to_commit or "HEAD"
                history_backfill.backfill_textmap_versions_from_history(
                    target_commit=target,
                    from_commit=args.from_commit or None,
                    force=args.force,
                    prune_missing=prune_missing,
                )
            except Exception as e:
                print(f"[ERROR] textMap history backfill failed: {e}", file=sys.stderr)
                sys.exit(4)
        elif args.history_backfill_only:
            try:
                import history_backfill

                target = args.to_commit or "HEAD"
                history_backfill.backfill_versions_from_history(
                    target_commit=target,
                    from_commit=args.from_commit or None,
                    force=args.force,
                    prune_missing=prune_missing,
                )
            except Exception as e:
                print(f"[ERROR] history backfill failed: {e}", file=sys.stderr)
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
                write_versions = bool(args.quest_write_versions)
                include_quests = not bool(args.quest_only_skip_quests)
                include_talks = not bool(args.quest_only_skip_talk)
                if write_versions and not include_quests:
                    print(
                        "[WARN] --quest-write-versions ignored in --quest-only mode "
                        "when quests are skipped."
                    )
                current_version = get_current_version() if write_versions else None
                print(
                    "Quest-only import mode: "
                    f"write_versions={'yes' if write_versions else 'no'}, "
                    f"prune_missing={'yes' if prune_missing else 'no'}, "
                    f"run_quests={'yes' if include_quests else 'no'}, "
                    f"run_talks={'yes' if include_talks else 'no'}"
                )
                quest_stats = questImport.runQuestOnly(
                    current_version=current_version,
                    write_versions=write_versions and include_quests,
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
            head_commit = _get_head_commit(DATA_PATH)
            if head_commit:
                set_current_version(head_commit)
            main(
                current_version=get_current_version(),
                prune_missing=prune_missing,
                enable_stage_profile=args.profile,
                use_fast_pragmas=not args.no_fast_import_pragmas,
                clean_breakpoint=args.clean_breakpoint,
            )
            if head_commit and not args.skip_history_backfill:
                try:
                    import history_backfill

                    history_backfill.backfill_versions_from_history(
                        target_commit=head_commit,
                        force=args.force,
                        prune_missing=prune_missing,
                        verbose=args.verbose,
                    )
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

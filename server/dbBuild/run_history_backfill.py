import history_backfill
import subprocess
import sys

# 获取HEAD提交
def get_head_commit(repo_path):
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
    from DBConfig import DATA_PATH

    head_commit = get_head_commit(DATA_PATH)
    if not head_commit:
        print("[ERROR] 无法获取HEAD提交，无法执行历史回填。")
        sys.exit(1)

    print(f"执行历史回填，目标提交: {head_commit}")

    # 执行TextMap历史回填
    print("执行TextMap历史回填...")
    history_backfill.backfill_textmap_versions_from_history(
        target_commit=head_commit,
        force=False,
        verbose=True,
    )

    # 执行Readable历史回填
    print("执行Readable历史回填...")
    history_backfill.backfill_readable_versions_from_history(
        target_commit=head_commit,
        force=False,
        verbose=True,
    )

    # 执行Subtitle历史回填
    print("执行Subtitle历史回填...")
    history_backfill.backfill_subtitle_versions_from_history(
        target_commit=head_commit,
        force=False,
        verbose=True,
    )

    # 执行Quest历史回填
    print("执行Quest历史回填...")
    history_backfill.backfill_quest_versions_from_history(
        target_commit=head_commit,
        force=False,
        verbose=True,
    )

    print("历史回填完成!")

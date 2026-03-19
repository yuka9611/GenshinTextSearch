import subprocess
import sys


def run_git(
    repo_path: str,
    args: list[str],
    check: bool = True,
    *,
    cache: dict | None = None,
    logger=None,
    low_priority: bool = False,
) -> str:
    cmd = ["git", "-C", repo_path] + args
    cache_key = (repo_path, tuple(args))
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    try:
        if logger is not None:
            logger.debug(f"Running git command: {' '.join(cmd)}")
        creationflags = 0
        if low_priority and sys.platform == "win32":
            creationflags = getattr(subprocess, "CREATE_LOW_PRIORITY_CLASS", 0)
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=creationflags,
        )
        if check and proc.returncode != 0:
            error_msg = proc.stderr.strip() or "git command failed"
            if logger is not None:
                logger.error(f"Git command failed: {error_msg}")
            raise RuntimeError(f"Git command failed: {' '.join(cmd)}\nError: {error_msg}")
        result = (proc.stdout or "").strip()
        if cache is not None:
            cache[cache_key] = result
        return result
    except Exception as exc:
        if isinstance(exc, RuntimeError):
            raise
        if logger is not None:
            logger.error(f"Error executing git command {' '.join(cmd)}: {exc}", exc_info=True)
        raise RuntimeError(f"Error executing git command {' '.join(cmd)}: {exc}")


def resolve_commit(
    repo_path: str,
    rev: str,
    *,
    cache: dict | None = None,
    logger=None,
    low_priority: bool = False,
) -> str:
    return run_git(
        repo_path,
        ["rev-parse", rev],
        check=True,
        cache=cache,
        logger=logger,
        low_priority=low_priority,
    ).strip()

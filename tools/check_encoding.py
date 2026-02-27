#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


TEXT_SUFFIXES = {
    ".py",
    ".js",
    ".ts",
    ".vue",
    ".json",
    ".yml",
    ".yaml",
    ".toml",
    ".ini",
    ".md",
    ".sql",
    ".csv",
    ".txt",
}

TEXT_EXACT_NAMES = {
    ".editorconfig",
    ".gitattributes",
    ".pre-commit-config.yaml",
}

SKIP_DIRS = {".git", ".venv", "build", "dist", "release"}

MOJIBAKE_REGEX = re.compile(
    r"(?:\u00c3.|"
    r"\u00c2.|"
    r"\u00e2\u20ac|"
    r"\u00ef\u00bb\u00bf|"
    r"\u00ef\u00bc|"
    r"\u00ef\u00bd)"
)


def _is_text_path(path: Path) -> bool:
    name = path.name.lower()
    if name in TEXT_EXACT_NAMES:
        return True
    return path.suffix.lower() in TEXT_SUFFIXES


def _iter_repo_files() -> list[Path]:
    try:
        proc = subprocess.run(
            ["git", "ls-files", "-z"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        entries = [entry for entry in proc.stdout.decode("utf-8", errors="strict").split("\x00") if entry]
        return [Path(entry) for entry in entries]
    except Exception:
        files: list[Path] = []
        for root, dirs, names in os.walk("."):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            for name in names:
                files.append(Path(root) / name)
        return files


def _contains_halfwidth_katakana(text: str) -> tuple[bool, str]:
    for idx, ch in enumerate(text):
        codepoint = ord(ch)
        if 0xFF61 <= codepoint <= 0xFF9F:
            return True, f"U+{codepoint:04X} at char index {idx}"
    return False, ""


def _find_mojibake_issue(text: str) -> str | None:
    if "\ufffd" in text:
        return "contains replacement character U+FFFD"

    has_halfwidth, detail = _contains_halfwidth_katakana(text)
    if has_halfwidth:
        return f"contains halfwidth katakana ({detail}), likely mojibake"

    match = MOJIBAKE_REGEX.search(text)
    if match:
        snippet = match.group(0).encode("unicode_escape").decode("ascii")
        return f"contains suspicious mojibake token: {snippet}"

    return None


def _normalize_input_paths(paths: list[str]) -> list[Path]:
    normalized: list[Path] = []
    seen: set[str] = set()
    for raw in paths:
        path = Path(raw)
        key = str(path).replace("\\", "/")
        if key in seen:
            continue
        seen.add(key)
        normalized.append(path)
    return normalized


def main() -> int:
    parser = argparse.ArgumentParser(description="Check UTF-8 encoding and detect common mojibake patterns.")
    parser.add_argument("paths", nargs="*", help="Files to check. If empty, checks all tracked files.")
    parser.add_argument(
        "--no-mojibake-check",
        action="store_true",
        help="Only check UTF-8 decoding and skip mojibake pattern checks.",
    )
    args = parser.parse_args()

    candidate_paths = _normalize_input_paths(args.paths) if args.paths else _iter_repo_files()

    checked_files = 0
    errors: list[str] = []

    for path in candidate_paths:
        if not path.exists() or path.is_dir():
            continue
        if not _is_text_path(path):
            continue

        checked_files += 1
        raw = path.read_bytes()
        try:
            text = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            errors.append(
                f"{path}: invalid UTF-8 at byte {exc.start} (reason: {exc.reason})"
            )
            continue

        if not args.no_mojibake_check:
            issue = _find_mojibake_issue(text)
            if issue:
                errors.append(f"{path}: {issue}")

    if errors:
        print("Encoding check failed:")
        for item in errors:
            print(f"  - {item}")
        return 1

    print(f"Encoding check passed ({checked_files} files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


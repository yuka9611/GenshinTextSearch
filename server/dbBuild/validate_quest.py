#!/usr/bin/env python3
"""Run quest version validation and print a summary."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from history_backfill import validate_quest_versions


def main():
    print("Running quest version validation...")
    result = validate_quest_versions(fix=True)

    print("\nValidation summary:")
    print(f"- Total anomalies: {result['total_abnormal']}")
    print(f"- Missing created version: {result['no_created_version']}")
    print(f"- Missing Git version: {result['no_git_version']}")
    print(f"- Invalid version values: {result['invalid_version']}")
    print(f"- quest_version rows without quest: {result['quest_version_no_quest']}")
    print(f"- quest_version rows without updated_version_id: {result['quest_version_no_updated']}")
    print(f"- quest_version rows with invalid updated_version_id: {result['quest_version_invalid']}")
    print(f"- quest rows older than quest_version minimum update: {result['quest_version_older']}")


if __name__ == "__main__":
    main()

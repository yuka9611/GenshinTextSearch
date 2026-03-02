#!/usr/bin/env python3
"""
单独执行任务版本异常验证
"""

import sys
import os

# 添加当前目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from history_backfill import validate_quest_versions

def main():
    print("开始执行任务版本异常修复...")
    result = validate_quest_versions(fix=True)
    print("\n验证完成，结果如下:")
    print(f"- 总异常任务数: {result['total_abnormal']}")
    print(f"- 没有创建版本的任务: {result['no_created_version']}")
    print(f"- 没有Git版本的任务: {result['no_git_version']}")
    print(f"- 版本号无效的任务: {result['invalid_version']}")
    print(f"- 版本号差异过大的任务: {result['large_version_diff']}")
    print(f"- quest_version表中没有对应quest的记录: {result['quest_version_no_quest']}")
    print(f"- quest_version表中没有更新版本的记录: {result['quest_version_no_updated']}")
    print(f"- quest_version表中版本号无效的记录: {result['quest_version_invalid']}")
    print(f"- quest_version表中更新版本早于创建版本的记录: {result['quest_version_older']}")

if __name__ == "__main__":
    main()

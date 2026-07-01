from __future__ import annotations

import importlib
import sqlite3
from pathlib import Path

try:
    openpyxl = importlib.import_module("openpyxl")
    Workbook = openpyxl.Workbook
    load_workbook = openpyxl.load_workbook
except ImportError as exc:
    raise ImportError(
        "Missing required dependency 'openpyxl'. Install it with "
        "'pip install openpyxl' and rerun."
    ) from exc


REPO_ROOT = Path("/Users/yuka9/Downloads/GenshinTextSearch")
DB_PATH = REPO_ROOT / "server" / "data.db"
OUTPUT_PATH = REPO_ROOT / "outputs" / "quest_export" / "genshin_all_quests.xlsx"
SHEET_NAME = "任务列表"

SOURCE_TYPE_LABELS = {
    "AQ": "魔神任务",
    "LQ": "传说任务",
    "WQ": "世界任务",
    "EQ": "活动任务",
    "IQ": "委托任务",
    "HANGOUT": "邀约事件",
    "ANECDOTE": "游逸旅闻",
    "UNKNOWN": "未分类",
}

QUERY = """
SELECT
  q.questId AS quest_id,
  COALESCE(tm_title.content, '') AS title,
  COALESCE(tm_chapter.content, '') AS chapter,
  COALESCE(q.source_type, 'UNKNOWN') AS source_type,
  COALESCE(vd_created.version_tag, vd_git.version_tag, vd_updated.version_tag, '') AS version,
  COALESCE(tm_desc.content, '') AS description
FROM quest q
LEFT JOIN textMap tm_title
  ON tm_title.hash = q.titleTextMapHash
 AND tm_title.lang = 1
LEFT JOIN chapter ch
  ON ch.chapterId = q.chapterId
LEFT JOIN textMap tm_chapter
  ON tm_chapter.hash = ch.chapterTitleTextMapHash
 AND tm_chapter.lang = 1
LEFT JOIN textMap tm_desc
  ON tm_desc.hash = q.descTextMapHash
 AND tm_desc.lang = 1
LEFT JOIN version_dim vd_created
  ON vd_created.id = q.created_version_id
LEFT JOIN version_dim vd_git
  ON vd_git.id = q.git_created_version_id
LEFT JOIN quest_version qv
  ON qv.questId = q.questId
 AND qv.lang = 1
LEFT JOIN version_dim vd_updated
  ON vd_updated.id = qv.updated_version_id
ORDER BY q.questId;
"""


def normalize(value) -> str:
    if value is None:
        return ""
    return str(value)


def load_rows() -> list[list[str]]:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(QUERY).fetchall()
    values = [["任务id", "任务名", "章节名", "任务类型", "版本", "任务描述"]]
    for row in rows:
        source_type = normalize(row["source_type"]).upper() or "UNKNOWN"
        values.append(
            [
                normalize(row["quest_id"]),
                normalize(row["title"]),
                normalize(row["chapter"]),
                SOURCE_TYPE_LABELS.get(source_type, SOURCE_TYPE_LABELS["UNKNOWN"]),
                normalize(row["version"]),
                normalize(row["description"]),
            ]
        )
    return values


def open_workbook():
    if OUTPUT_PATH.exists():
        workbook = load_workbook(OUTPUT_PATH)
        if SHEET_NAME in workbook.sheetnames:
            return workbook, workbook[SHEET_NAME], False
        return workbook, workbook.create_sheet(SHEET_NAME), False
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = SHEET_NAME
    return workbook, sheet, True


def remove_selection_column(sheet) -> None:
    if sheet.cell(1, 2).value == "选择":
        sheet.delete_cols(2)


def update_sheet(sheet, values: list[list[str]], *, new_workbook: bool) -> dict[str, int]:
    changed = 0
    blank_filled = 0
    unchanged = 0
    skipped_version = 0
    version_col = 5

    remove_selection_column(sheet)

    for row_index, row_values in enumerate(values, start=1):
        for col_index, target in enumerate(row_values, start=1):
            if col_index == version_col and not new_workbook:
                skipped_version += 1
                continue
            cell = sheet.cell(row_index, col_index)
            current = normalize(cell.value)
            target_text = normalize(target)
            if current == target_text:
                unchanged += 1
                continue
            if current == "":
                blank_filled += 1
            cell.value = target_text
            changed += 1

    return {
        "changedCells": changed,
        "blankCellsFilled": blank_filled,
        "unchangedCells": unchanged,
        "skippedVersionCells": skipped_version,
    }


def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    values = load_rows()
    workbook, sheet, new_workbook = open_workbook()
    stats = update_sheet(sheet, values, new_workbook=new_workbook)
    workbook.save(OUTPUT_PATH)
    print(
        {
            "outputPath": str(OUTPUT_PATH),
            "rowCount": len(values) - 1,
            "rows": sheet.max_row,
            "cols": sheet.max_column,
            **stats,
        }
    )


if __name__ == "__main__":
    main()

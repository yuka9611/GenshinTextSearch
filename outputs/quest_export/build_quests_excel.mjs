import fs from "node:fs/promises";
import path from "node:path";
import { execFileSync } from "node:child_process";
import { FileBlob, SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const repoRoot = "/Users/yuka9/Downloads/GenshinTextSearch";
const dbPath = path.join(repoRoot, "server", "data.db");
const outputDir = path.join(repoRoot, "outputs", "quest_export");
const outputPath = path.join(outputDir, "genshin_all_quests.xlsx");
const sheetName = "任务列表";

const sourceTypeLabels = {
  AQ: "魔神任务",
  LQ: "传说任务",
  WQ: "世界任务",
  EQ: "活动任务",
  IQ: "委托任务",
  HANGOUT: "邀约事件",
  ANECDOTE: "游逸旅闻",
  UNKNOWN: "未分类",
};

const query = `
SELECT
  q.questId AS "任务id",
  COALESCE(tm_title.content, '') AS "任务名",
  COALESCE(tm_chapter.content, '') AS "章节名",
  COALESCE(q.source_type, 'UNKNOWN') AS source_type,
  COALESCE(vd_created.version_tag, vd_git.version_tag, vd_updated.version_tag, '') AS "版本",
  COALESCE(tm_desc.content, '') AS "任务描述"
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
`;

const raw = execFileSync("sqlite3", ["-json", dbPath, query], { encoding: "utf8", maxBuffer: 32 * 1024 * 1024 });
const rows = JSON.parse(raw);
const headers = ["任务id", "任务名", "章节名", "任务类型", "版本", "任务描述"];
const versionColumnIndex = headers.indexOf("版本");
const values = [headers];

for (const row of rows) {
  const sourceType = String(row.source_type || "UNKNOWN").toUpperCase();
  values.push([
    String(row["任务id"] ?? ""),
    String(row["任务名"] ?? ""),
    String(row["章节名"] ?? ""),
    sourceTypeLabels[sourceType] || sourceTypeLabels.UNKNOWN,
    String(row["版本"] ?? ""),
    String(row["任务描述"] ?? ""),
  ]);
}

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

function columnName(index) {
  let value = index + 1;
  let name = "";
  while (value > 0) {
    const remainder = (value - 1) % 26;
    name = String.fromCharCode(65 + remainder) + name;
    value = Math.floor((value - 1) / 26);
  }
  return name;
}

function cellAddress(rowIndex, colIndex) {
  return `${columnName(colIndex)}${rowIndex + 1}`;
}

function normalizeCellValue(value) {
  return value == null ? "" : String(value);
}

await fs.mkdir(outputDir, { recursive: true });

let workbook;
let sheet;
let createdWorkbook = false;
if (await fileExists(outputPath)) {
  const input = await FileBlob.load(outputPath);
  workbook = await SpreadsheetFile.importXlsx(input);
  try {
    sheet = workbook.worksheets.getItem(sheetName);
  } catch {
    sheet = workbook.worksheets.add(sheetName);
  }
} else {
  workbook = Workbook.create();
  sheet = workbook.worksheets.add(sheetName);
  createdWorkbook = true;
}

let changedCells = 0;
let blankCellsFilled = 0;
let unchangedCells = 0;
if (createdWorkbook) {
  sheet.getRange(`A1:F${values.length}`).values = values;
  changedCells = values.length * headers.length;
  blankCellsFilled = changedCells;
} else {
  const existingValues = sheet.getRange(`A1:${columnName(headers.length - 1)}${values.length}`).values;
  for (let rowIndex = 0; rowIndex < values.length; rowIndex += 1) {
    const targetRow = values[rowIndex];
    const existingRow = existingValues[rowIndex] || [];
    for (let colIndex = 0; colIndex < targetRow.length; colIndex += 1) {
      if (colIndex === versionColumnIndex) {
        unchangedCells += 1;
        continue;
      }
      const targetValue = normalizeCellValue(targetRow[colIndex]);
      const existingValue = normalizeCellValue(existingRow[colIndex]);
      if (existingValue === targetValue) {
        unchangedCells += 1;
        continue;
      }
      if (existingValue === "") {
        blankCellsFilled += 1;
      }
      changedCells += 1;
      sheet.getRange(cellAddress(rowIndex, colIndex)).values = [[targetValue]];
    }
  }
}

const preview = await workbook.inspect({
  kind: "table",
  range: `${sheetName}!A1:F8`,
  include: "values",
  tableMaxRows: 8,
  tableMaxCols: 6,
});
console.log(preview.ndjson);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 20 },
  summary: "final formula error scan",
});
console.log(errors.ndjson);

const targetTitles = await workbook.inspect({
  kind: "match",
  searchTerm: "主宰地下世界的黑道大佬|来自秘闻馆的委托|枪弹与丝线的合围",
  options: { useRegex: true, maxResults: 20 },
  summary: "target 6.5 quest titles",
});
console.log(targetTitles.ndjson);

await workbook.render({ sheetName: "任务列表", range: "A1:F25", scale: 2 });

const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);

console.log(JSON.stringify({
  outputPath,
  rowCount: rows.length,
  dataRows: values.length - 1,
  changedCells,
  blankCellsFilled,
  unchangedCells,
}));

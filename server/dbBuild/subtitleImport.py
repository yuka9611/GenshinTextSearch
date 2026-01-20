import os
import json
import re
from DBConfig import conn, DATA_PATH
from tqdm import tqdm

# 语言文件夹名到数据库 langCode ID 的映射
LANG_MAP = {
    'CHS': 1, 'CHT': 2, 'DE': 3, 'EN': 4, 'ES': 5,
    'FR': 6, 'ID': 7, 'IT': 8, 'JP': 9, 'KR': 10,
    'PT': 11, 'RU': 12, 'TH': 13, 'TR': 14, 'VI': 15
}

def load_localization_config():
    """
    Loads LocalizationExcelConfigData.json and creates a mapping from filename to info (subtitleId).
    """
    loc_path = os.path.join(DATA_PATH, "ExcelBinOutput", "LocalizationExcelConfigData.json")
    if not os.path.exists(loc_path):
        print(f"Localization config not found: {loc_path}")
        return {}

    try:
        with open(loc_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading LocalizationExcelConfigData.json: {e}")
        return {}

    # Map filename to info
    filename_to_info = {}
    
    # Keys that might contain paths
    path_keys = [
        "dePath", "enPath", "esPath", "frPath", "idPath", "itPath", 
        "jpPath", "krPath", "ptPath", "ruPath", "tcPath", "thPath", 
        "trPath", "viPath", "EDPAFDDJJNM", "FNIFOPDJMMG"
    ]

    for entry in data:
        # Only care about LOC_SUBTITLE
        if entry.get('assetType') != 'LOC_SUBTITLE':
            continue

        subtitle_id = entry.get('id')
        
        for key in path_keys:
            if key in entry:
                path = entry[key]
                if isinstance(path, str):
                    # Extract filename from path
                    # Path example: "CHS/Ambor_Readings_CHS.mihoyobin"
                    # We need to match this with the actual .srt filename
                    # Actual .srt filename: Ambor_Readings_CHS.srt
                    # So we extract the basename without extension
                    filename_no_ext = os.path.splitext(os.path.basename(path))[0]
                    
                    filename_to_info[filename_no_ext] = {
                        'subtitleId': subtitle_id
                    }

    return filename_to_info

def parse_srt_time(time_str):
    """将 SRT 时间字符串 (00:00:01,500) 转换为秒 (1.5)"""
    try:
        time_str = time_str.replace(',', '.')
        parts = time_str.split(':')
        h = float(parts[0])
        m = float(parts[1])
        s = float(parts[2])
        return h * 3600 + m * 60 + s
    except:
        return 0.0

def importSubtitles():
    print("Loading localization configs for subtitles...")
    filename_to_info = load_localization_config()
    print(f"Loaded {len(filename_to_info)} subtitle file mappings.")

    print("Importing Subtitles (srt)...")
    cursor = conn.cursor()
    subtitle_root = os.path.join(DATA_PATH, "Subtitle")

    if not os.path.exists(subtitle_root):
        print(f"Subtitle path not found: {subtitle_root}")
        return

    sql_insert = "insert into subtitle(fileName, lang, startTime, endTime, content, subtitleId) values (?,?,?,?,?,?)"

    for lang_name, lang_id in LANG_MAP.items():
        lang_path = os.path.join(subtitle_root, lang_name)
        if not os.path.exists(lang_path):
            continue

        subtitle_files = []
        for root, _, files in os.walk(lang_path):
            for file_name in files:
                if file_name.endswith(".srt"):
                    subtitle_files.append(os.path.join(root, file_name))

        print(f"  Processing {lang_name} ({len(subtitle_files)} files)...")

        for full_path in tqdm(subtitle_files):
            file_name = os.path.basename(full_path)
            name_without_ext = os.path.splitext(file_name)[0]

            # 文件名作为标识符 (去除后缀)，保留子目录以避免重名冲突
            rel_path = os.path.relpath(full_path, lang_path)
            cleanFileName = os.path.splitext(rel_path)[0].replace(os.sep, "/")
            
            # Look up info
            info = filename_to_info.get(name_without_ext)
            subtitle_id = None
            if info:
                subtitle_id = info['subtitleId']

            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                # 简单的 SRT 解析: 按空行分割块
                blocks = re.split(r'\r?\n\s*\r?\n', content.strip())
                for block in blocks:
                    lines = [line.strip() for line in block.splitlines() if line.strip()]
                    if len(lines) < 2:
                        continue

                    # 查找包含 '-->' 的时间行
                    time_line_idx = -1
                    for idx, line in enumerate(lines):
                        if '-->' in line:
                            time_line_idx = idx
                            break

                    if time_line_idx == -1:
                        continue

                    # 解析时间
                    time_parts = lines[time_line_idx].split('-->')
                    if len(time_parts) != 2:
                        continue

                    start_time = parse_srt_time(time_parts[0].strip())
                    end_time = parse_srt_time(time_parts[1].strip())

                    # 获取文本内容 (时间行之后的所有行)
                    text_lines = lines[time_line_idx + 1:]
                    text_content = "\n".join(text_lines)

                    if text_content:
                        cursor.execute(
                            sql_insert,
                            (cleanFileName, lang_id, start_time, end_time, text_content, subtitle_id)
                        )

            except Exception as e:
                print(f"Error processing {file_name} in {lang_name}: {e}")

    cursor.close()
    conn.commit()
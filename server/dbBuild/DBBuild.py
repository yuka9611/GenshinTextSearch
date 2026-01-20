import os
import re
import json
from DBConfig import conn, DATA_PATH
import voiceItemImport
from tqdm import tqdm

# 语言文件夹名到数据库 langCode ID 的映射
# 基于 databaseDDL.sql 中的 insert 语句
LANG_MAP = {
    'CHS': 1, 'CHT': 2, 'DE': 3, 'EN': 4, 'ES': 5,
    'FR': 6, 'ID': 7, 'IT': 8, 'JP': 9, 'KR': 10,
    'PT': 11, 'RU': 12, 'TH': 13, 'TR': 14, 'VI': 15
}

# --- 原有的导入函数保持不变 (importTalk, importAvatars 等) ---

def importTalk(fileName: str):
    cursor = conn.cursor()
    obj = json.load(open(DATA_PATH + "\\BinOutput\\Talk\\" + fileName, encoding='utf-8'))

    if 'talkId' in obj:
        talkIdKey = 'talkId'
        dialogueListKey = 'dialogList'
        dialogueIdKey = 'id'
        talkRoleKey = 'talkRole'
        talkRoleTypeKey = 'type'
        talkRoleIdKey = '_id'
        talkContentTextMapHashKey = 'talkContentTextMapHash'
    # elif 'FEOACBMDCKJ' in obj:
    elif 'ADHLLDAPKCM' in obj:
        # talkIdKey = 'FEOACBMDCKJ'
        talkIdKey = 'ADHLLDAPKCM'
        # dialogueListKey = 'AAOAAFLLOJI'
        dialogueListKey = 'MOEOFGCKILF'
        # dialogueIdKey = 'CCFPGAKINNB'
        dialogueIdKey = 'ILHDNJDDEOP'
        # talkRoleKey = 'HJLEMJIGNFE'
        talkRoleKey = 'LCECPDILLEE'
        talkRoleTypeKey = '_type'
        talkRoleIdKey = '_id'
        # talkContentTextMapHashKey = 'BDOKCLNNDGN'
        talkContentTextMapHashKey = 'GABLFFECBDO'
    else:
        print("Skipping " + fileName)
        return

    talkId = obj[talkIdKey]
    if dialogueListKey not in obj or len(obj[dialogueListKey]) == 0:
        return

    sql1 = "insert or ignore into dialogue(dialogueId, talkerId, talkerType, talkId, textHash, coopQuestId) values (?,?,?,?,?,?)"

    coopMatch = re.match(r"^Coop[\\,/]([0-9]+)_[0-9]+.json$", fileName)
    if coopMatch:
        coopQuestId = coopMatch.group(1)
    else:
        coopQuestId = None

    for dialogue in obj[dialogueListKey]:
        dialogueId = dialogue[dialogueIdKey]
        if talkRoleKey in dialogue and \
                talkRoleIdKey in dialogue[talkRoleKey] and \
                talkRoleTypeKey in dialogue[talkRoleKey]:
            talkRoleId = dialogue[talkRoleKey][talkRoleIdKey]
            talkRoleType = dialogue[talkRoleKey][talkRoleTypeKey]
        else:
            talkRoleId = -1
            talkRoleType = None

        if talkContentTextMapHashKey not in dialogue:
            continue
        textHash = dialogue[talkContentTextMapHashKey]
        cursor.execute(sql1, (dialogueId, talkRoleId, talkRoleType, talkId, textHash, coopQuestId))

    cursor.close()
    conn.commit()


def importAllTalkItems():
    folders = os.listdir(DATA_PATH + "\\BinOutput\\Talk\\")
    for folder in folders:
        if not os.path.isdir(DATA_PATH + "\\BinOutput\\Talk\\" + folder):
            continue

        files = os.listdir(DATA_PATH + "\\BinOutput\\Talk\\" + folder)
        print("importing talk " + folder)
        for val, fileName in tqdm(enumerate(files), total=len(files)):
            importTalk(folder + "\\" + fileName)


def importAvatars():
    cursor = conn.cursor()
    avatars = json.load(open(DATA_PATH + "\\ExcelBinOutput\\AvatarExcelConfigData.json", encoding='utf-8'))

    sql1 = "insert into avatar(avatarId, nameTextMapHash) values (?,?)"

    for avatar in avatars:
        cursor.execute(sql1, (avatar['id'], avatar['nameTextMapHash']))

    cursor.close()
    conn.commit()


def importFetters():
    cursor = conn.cursor()
    fetters = json.load(open(DATA_PATH + "\\ExcelBinOutput\\FettersExcelConfigData.json", encoding='utf-8'))
    sql1 = "insert into fetters(fetterId, avatarId, voiceTitleTextMapHash, voiceFileTextTextMapHash, voiceFile) values (?,?,?,?,?)"

    for fetter in fetters:
        cursor.execute(sql1,(fetter['fetterId'], fetter['avatarId'], fetter['voiceTitleTextMapHash'], fetter['voiceFileTextTextMapHash'], fetter['voiceFile']))

    cursor.close()
    conn.commit()


def importChapters():
    cursor = conn.cursor()
    chapters = json.load(open(DATA_PATH + "\\ExcelBinOutput\\ChapterExcelConfigData.json", encoding='utf-8'))
    sql1 = "insert into chapter(chapterId, chapterTitleTextMapHash, chapterNumTextMapHash) values (?,?,?)"

    for chapter in chapters:
        cursor.execute(sql1,(chapter['id'], chapter['chapterTitleTextMapHash'], chapter['chapterNumTextMapHash']))

    cursor.close()
    conn.commit()
    

def importQuest(fileName: str):
    cursor = conn.cursor()
    obj = json.load(open(DATA_PATH + "\\BinOutput\\Quest\\" + fileName, encoding='utf-8'))

    sql1 = 'insert into quest(questId, titleTextMapHash, chapterId) VALUES (?,?,?)'
    sql2 = 'insert into questTalk(questId, talkId) values (?,?)'

    if 'id' in obj:
        keyQuestId = 'id'
        keyTitleTextMapHash = 'titleTextMapHash'
        keyChapterId = 'chapterId'
        keyTalks = 'talks'
        keyTalkId = 'id'
    elif 'ILHDNJDDEOP' in obj:
        # keyQuestId = 'CCFPGAKINNB'
        keyQuestId = 'ILHDNJDDEOP'
        # keyTitleTextMapHash = 'HLAINHJACPJ'
        keyTitleTextMapHash = 'MMOEEOFGHHG'
        # keyChapterId = 'FLCLAPBOOHF'
        keyChapterId = 'IBNCKLKHAKG'
        # keyTalks = 'PCNNNPLAEAI'
        keyTalks = 'IBEGAHMEABP'
        # keyTalkId = 'CCFPGAKINNB'
        keyTalkId = 'ILHDNJDDEOP'
    else:
        print("Skipping " + fileName)
        return

    questId = obj[keyQuestId]

    if keyTitleTextMapHash in obj:
        titleTextMapHash = obj[keyTitleTextMapHash]
    else:
        titleTextMapHash = None
        print("questId {} don't have TitleTextMapHash!".format(questId))

    if keyChapterId in obj:
        chapterId = obj[keyChapterId]
    else:
        chapterId = None

    cursor.execute(sql1, (questId, titleTextMapHash, chapterId))

    if keyTalks not in obj:
        print("questId {} don't have talk!".format(questId))
    else:

        for talk in obj[keyTalks]:
            talkId = talk[keyTalkId]
            cursor.execute(sql2, (questId, talkId))
            pass

    cursor.close()


def importAllQuests():
    files = os.listdir(DATA_PATH + "\\BinOutput\\Quest\\")
    n = len(files)
    for val, fileName in tqdm(enumerate(files), total=len(files)):
        # print("Now: {} {}/{}".format(fileName, val, n))
        importQuest(fileName)
    conn.commit()


def importNPCs():
    cursor = conn.cursor()
    NPCs = json.load(open(DATA_PATH + "\\ExcelBinOutput\\NpcExcelConfigData.json", encoding='utf-8'))
    sql1 = "insert into npc(npcId, textHash) values (?,?)"

    for npc in NPCs:
        cursor.execute(sql1, (npc['id'], npc['nameTextMapHash']))

    cursor.close()
    conn.commit()


def importManualTextMap():
    cursor = conn.cursor()
    placeholders = json.load(open(DATA_PATH + "\\ExcelBinOutput\\ManualTextMapConfigData.json", encoding='utf-8'))
    sql1 = "insert into manualTextMap(textMapId, textHash) values (?,?)"

    for placeholder in placeholders:
        cursor.execute(sql1, (placeholder['textMapId'], placeholder['textMapContentTextMapHash']))

    cursor.close()
    conn.commit()

# --- 新增的 Readable 和 Subtitle 导入逻辑 ---
def load_readable_mappings():
    """
    构建两个映射:
    1. filename_map: "Relic10008_4" -> 280084 (readableId)
    2. title_map: 280084 -> 1234567 (titleTextMapHash)
    """
    print("Loading Readable mappings...")

    filename_to_id = {}
    id_to_title = {}

    def normalize_id(value):
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return value

    def normalize_readable_name(name: str) -> str:
        clean_name = name.replace("\\", "/")
        stem = os.path.splitext(clean_name)[0]
        for lang_key in LANG_MAP.keys():
            suffix = f"_{lang_key}"
            if stem.endswith(suffix):
                stem = stem[:-len(suffix)]
                break
        return stem

    def register_filename(readable_id: int, raw_name: str):
        readable_id = normalize_id(readable_id)
        if not readable_id or not raw_name:
            return
        normalized = normalize_readable_name(raw_name)
        filename_to_id.setdefault(normalized, readable_id)
        base_name = os.path.basename(raw_name)
        if base_name:
            filename_to_id.setdefault(normalize_readable_name(base_name), readable_id)

    def walk_values(value):
        if isinstance(value, dict):
            for child in value.values():
                yield from walk_values(child)
        elif isinstance(value, list):
            for child in value:
                yield from walk_values(child)
        elif isinstance(value, str):
            yield value

    def register_paths_from_item(item_id: int, item: dict):
        for val in walk_values(item):
            val_lower = val.lower()
            if "readable" not in val_lower and not val_lower.endswith(".txt"):
                continue
            register_filename(item_id, val)

    # 1. 加载 Localization 建立 文件名 -> ID 映射
    # 路径: ExcelBinOutput/LocalizationExcelConfigData.json
    loc_path = os.path.join(DATA_PATH, "ExcelBinOutput", "LocalizationExcelConfigData.json")

    if os.path.exists(loc_path):
        try:
            loc_data = json.load(open(loc_path, encoding='utf-8'))
            for item in loc_data:
                item_id = normalize_id(item.get('id') or item.get('documentId'))
                if not item_id:
                    continue
                register_paths_from_item(item_id, item)
        except Exception as e:
            print(f"Error loading Localization config: {e}")
    else:
        print("LocalizationExcelConfigData.json not found!")

    # 2. 加载 Document 建立 ID -> TitleHash 映射 + 备用的文件名映射
    # 路径: ExcelBinOutput/DocumentExcelConfigData.json
    doc_path = os.path.join(DATA_PATH, "ExcelBinOutput", "DocumentExcelConfigData.json")

    if os.path.exists(doc_path):
        try:
            doc_data = json.load(open(doc_path, encoding='utf-8'))
            for item in doc_data:
                doc_id = normalize_id(item.get('id') or item.get('documentId'))
                title_hash = item.get('titleTextMapHash')
                if doc_id and title_hash:
                    id_to_title[doc_id] = title_hash
                if doc_id:
                    register_paths_from_item(doc_id, item)
                quest_ids = item.get('questIDList') or []
                if title_hash and isinstance(quest_ids, list):
                    for quest_id in quest_ids:
                        quest_id = normalize_id(quest_id)
                        if quest_id:
                            id_to_title.setdefault(quest_id, title_hash)
        except Exception as e:
            print(f"Error loading Document config: {e}")
    else:
        print("DocumentExcelConfigData.json not found!")

    return filename_to_id, id_to_title

def importReadables():
    cursor = conn.cursor()
    readable_root = os.path.join(DATA_PATH, "Readable")
    
    if not os.path.exists(readable_root):
        print("Readable folder not found.")
        return

    # 获取映射
    filename_to_id, id_to_title = load_readable_mappings()
    
    print(f"Mapped {len(filename_to_id)} filenames and {len(id_to_title)} titles.")
    
    # 缓存已插入的 readableId，避免重复插入主表
    inserted_readable_ids = set()

    def normalize_readable_name(name: str) -> str:
        clean_name = name.replace("\\", "/")
        stem = os.path.splitext(clean_name)[0]
        for lang_key in LANG_MAP.keys():
            suffix = f"_{lang_key}"
            if stem.endswith(suffix):
                stem = stem[:-len(suffix)]
                break
        return stem

    for lang_name, lang_id in LANG_MAP.items():
        lang_path = os.path.join(readable_root, lang_name)
        if not os.path.exists(lang_path):
            continue

        readable_files = []
        for root, _, files in os.walk(lang_path):
            for file_name in files:
                if file_name.endswith(".txt"):
                    readable_files.append(os.path.join(root, file_name))

        print(f"Importing Readable {lang_name} ({len(readable_files)} files)...")

        for full_path in tqdm(readable_files):
            file_name = os.path.basename(full_path)
            rel_path = os.path.relpath(full_path, lang_path).replace(os.sep, "/")
            name_stem = normalize_readable_name(file_name)
            rel_stem = normalize_readable_name(rel_path)
            readable_id = filename_to_id.get(rel_stem) or filename_to_id.get(name_stem)

            if not readable_id:
                digits = re.findall(r'\d+', name_stem)
                if digits:
                    candidate_id = int(''.join(digits))
                    if candidate_id in id_to_title:
                        readable_id = candidate_id
            if not readable_id:
                continue

            # 1. 插入/更新 Readable 主表 (如果还没处理过这个ID)
            title_hash = id_to_title.get(readable_id)
            if readable_id not in inserted_readable_ids:
                cursor.execute(
                    "INSERT OR REPLACE INTO readable(readableId, titleTextMapHash) VALUES (?, ?)",
                    (readable_id, title_hash)
                )
                inserted_readable_ids.add(readable_id)
            elif title_hash:
                cursor.execute(
                    "UPDATE readable SET titleTextMapHash = COALESCE(titleTextMapHash, ?) WHERE readableId = ?",
                    (title_hash, readable_id)
                )

            # 2. 插入内容表
            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                cursor.execute(
                    "INSERT INTO readableContent(readableId, lang, content) VALUES (?,?,?)",
                    (readable_id, lang_id, content)
                )
            except Exception as e:
                print(f"Error reading {file_name}: {e}")

    cursor.close()
    conn.commit()

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
    print("Importing Subtitles (srt)...")
    cursor = conn.cursor()
    subtitle_root = os.path.join(DATA_PATH, "Subtitle")

    if not os.path.exists(subtitle_root):
        print(f"Subtitle path not found: {subtitle_root}")
        return

    sql_insert = "insert into subtitle(fileName, lang, startTime, endTime, content) values (?,?,?,?,?)"

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

            # 文件名作为标识符 (去除后缀)，保留子目录以避免重名冲突
            rel_path = os.path.relpath(full_path, lang_path)
            cleanFileName = os.path.splitext(rel_path)[0].replace(os.sep, "/")

            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                # 简单的 SRT 解析: 按空行分割块
                # 每个块格式:
                # 1
                # 00:00:00,490 --> 00:00:02,630
                # 文本内容...
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
                            (cleanFileName, lang_id, start_time, end_time, text_content)
                        )

            except Exception as e:
                print(f"Error processing {file_name} in {lang_name}: {e}")

    cursor.close()
    conn.commit()


def main():
    print("Importing talks...")
    importAllTalkItems()
    print("Importing avatars...")
    importAvatars()
    print("Importing NPCs...")
    importNPCs()
    print("Importing ManualTextMap...")
    importManualTextMap()
    print("Importing fetters...")
    importFetters()
    print("Importing quests...")
    importAllQuests()
    print("Importing chapters...")
    importChapters()
    print("Importing books...")
    importReadables()
    print("Importing subtitles...")
    importSubtitles()
    
    print("Importing voices...")
    voiceItemImport.loadAvatars()
    voiceItemImport.importAllVoiceItems()
    print("Done!")


if __name__ == "__main__":
    main()
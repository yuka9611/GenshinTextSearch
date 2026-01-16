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
    elif 'FEOACBMDCKJ' in obj:
        talkIdKey = 'FEOACBMDCKJ'
        dialogueListKey = 'AAOAAFLLOJI'
        dialogueIdKey = 'CCFPGAKINNB'
        talkRoleKey = 'HJLEMJIGNFE'
        talkRoleTypeKey = '_type'
        talkRoleIdKey = '_id'
        talkContentTextMapHashKey = 'BDOKCLNNDGN'
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
    print("Importing Chapters from ExcelBinOutput...")
    cursor = conn.cursor()
    
    # 路径指向 ExcelBinOutput/ChapterExcelConfigData.json
    path = os.path.join(DATA_PATH, "ExcelBinOutput", "ChapterExcelConfigData.json")
    
    if not os.path.exists(path):
        print(f"Chapter config not found at: {path}")
        return

    try:
        chapters = json.load(open(path, encoding='utf-8'))
        
        # 插入语句
        sql = "insert or replace into chapter(chapterId, chapterTitleTextMapHash, chapterNumTextMapHash) values (?,?,?)"

        for chapter in chapters:
            # 兼容不同版本的 Key (通常是 id)
            c_id = chapter.get('id')
            # 尝试获取标题哈希
            c_title = chapter.get('chapterTitleTextMapHash')
            # 尝试获取章节编号哈希
            c_num = chapter.get('chapterNumTextMapHash')
            
            if c_id is not None:
                cursor.execute(sql, (c_id, c_title, c_num))
                
        cursor.close()
        conn.commit()
        print(f"Imported {len(chapters)} chapters.")
        
    except Exception as e:
        print(f"Error importing chapters: {e}")


def importQuestMeta():
    """
    第一步：从 ExcelBinOutput 导入任务的元数据 (标题, 章节ID)
    """
    cursor = conn.cursor()
    # 使用 MainQuestExcelConfigData 获取最全的任务信息
    path = os.path.join(DATA_PATH, "ExcelBinOutput", "MainQuestExcelConfigData.json")
    
    if not os.path.exists(path):
        # 备选：如果 MainQuest 不存在，尝试 QuestExcelConfigData (通常用于子任务，但有时混用)
        path = os.path.join(DATA_PATH, "ExcelBinOutput", "QuestExcelConfigData.json")
    
    if not os.path.exists(path):
        print("Quest Excel config not found.")
        return

    print(f"Importing Quest Metadata from {os.path.basename(path)}...")
    
    try:
        quests = json.load(open(path, encoding='utf-8'))
        sql = "insert or replace into quest(questId, titleTextMapHash, chapterId) values (?,?,?)"
        
        count = 0
        for q in quests:
            q_id = q.get('id') or q.get('MainId')
            # 任务标题
            q_title = q.get('titleTextMapHash')
            # 章节ID
            q_chapter = q.get('chapterId')
            
            if q_id is not None:
                cursor.execute(sql, (q_id, q_title, q_chapter))
                count += 1
                
        cursor.close()
        conn.commit()
        print(f"Imported metadata for {count} quests.")
        
    except Exception as e:
        print(f"Error importing quest meta: {e}")


def importQuestTalks():
    """
    第二步：从 BinOutput/Quest 递归导入任务与对话的关联 (Quest -> Talk)
    """
    cursor = conn.cursor()
    quest_root = os.path.join(DATA_PATH, "BinOutput", "Quest")
    
    if not os.path.exists(quest_root):
        print(f"BinOutput/Quest folder not found at: {quest_root}")
        return

    print("Scanning BinOutput/Quest for dialogues...")
    
    sql = "insert or ignore into questTalk(questId, talkId) values (?,?)"
    
    # 使用 os.walk 递归查找所有 .json 文件，解决文件夹结构变化问题
    files_to_process = []
    for root, dirs, files in os.walk(quest_root):
        for f in files:
            if f.endswith(".json"):
                files_to_process.append(os.path.join(root, f))
    
    print(f"Found {len(files_to_process)} Quest flow files. Processing...")
    
    for file_path in tqdm(files_to_process):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                obj = json.load(f)
            
            # 获取 Quest ID (通常在根节点的 id 字段)
            quest_id = obj.get('id') or obj.get('mainId') or obj.get('MainId')
            
            if not quest_id:
                # 尝试从文件名获取 (例如 1001.json -> 1001)
                base_name = os.path.basename(file_path)
                name_match = re.match(r'^(\d+)\.json$', base_name)
                if name_match:
                    quest_id = int(name_match.group(1))
            
            if not quest_id:
                continue

            # 获取 talks 列表
            # 结构通常是 "talks": [ {"id": 123, ...}, ... ]
            talks = obj.get('talks')
            if talks and isinstance(talks, list):
                for talk in talks:
                    talk_id = talk.get('id')
                    if talk_id:
                        cursor.execute(sql, (quest_id, talk_id))
                        
        except Exception as e:
            # 忽略个别文件读取错误，避免中断整个流程
            # print(f"Skipping {file_path}: {e}")
            pass
            
    cursor.close()
    conn.commit()


def importAllQuests():
    # 替代原本的循环逻辑，改为分步导入
    importQuestMeta()   # 先填 quest 表
    importQuestTalks()  # 再填 questTalk 表
    print("Quest import finished.")


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
    
    # 1. 加载 Localization 建立 文件名 -> ID 映射
    # 路径: ExcelBinOutput/LocalizationExcelConfigData.json
    loc_path = os.path.join(DATA_PATH, "ExcelBinOutput", "LocalizationExcelConfigData.json")
    filename_to_id = {}
    
    if os.path.exists(loc_path):
        try:
            loc_data = json.load(open(loc_path, encoding='utf-8'))
            for item in loc_data:
                item_id = item.get('id')
                if not item_id:
                    continue
                
                # 遍历所有可能的路径键，提取文件名
                # 示例: "enPath": "ART/UI/Readable/EN/Relic10008_4_EN"
                # 注意：有些文件名带有语言后缀 (Relic10008_4_EN)，有些没有 (Relic10008_4)。
                # 既然我们遍历的是 txt 文件，我们需要匹配去除了路径和语言后缀的核心名称，或者建立多对一映射。
                
                # 策略：收集该 ID 对应的所有可能的文件名标识
                # 通常 Localization 里的路径最后一部分是文件名
                
                for key, val in item.items():
                    if key.endswith('Path') and isinstance(val, str) and 'Readable' in val:
                        # val example: "ART/UI/Readable/EN/Relic10008_4_EN"
                        fname = os.path.basename(val) # "Relic10008_4_EN"
                        filename_to_id[fname] = item_id
                        
                        # 为了稳健，如果文件名包含语言后缀（如 _EN, _CHS），也可以尝试存储去除后缀的版本
                        # 但实际上读取 txt 时，我们可以拿 txt 的文件名去匹配这里记录的 fname
                        
        except Exception as e:
            print(f"Error loading Localization config: {e}")
    else:
        print("LocalizationExcelConfigData.json not found!")

    # 2. 加载 Document 建立 ID -> TitleHash 映射
    # 路径: ExcelBinOutput/DocumentExcelConfigData.json
    doc_path = os.path.join(DATA_PATH, "ExcelBinOutput", "DocumentExcelConfigData.json")
    id_to_title = {}
    
    if os.path.exists(doc_path):
        try:
            doc_data = json.load(open(doc_path, encoding='utf-8'))
            for item in doc_data:
                doc_id = item.get('id')
                title_hash = item.get('titleTextMapHash')
                if doc_id and title_hash:
                    id_to_title[doc_id] = title_hash
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

    for lang_name, lang_id in LANG_MAP.items():
        lang_path = os.path.join(readable_root, lang_name)
        if not os.path.exists(lang_path):
            continue
            
        files = os.listdir(lang_path)
        print(f"Importing Readable {lang_name}...")
        
        for file_name in tqdm(files):
            if not file_name.endswith(".txt"):
                continue
            
            # 匹配文件名
            # 文件名: "Relic10008_4.txt" -> 核心名 "Relic10008_4"
            # 或者是 "Relic10008_4_CHS.txt" -> "Relic10008_4_CHS"
            # 我们需要拿这个名字去 filename_to_id 查找
            
            name_stem = os.path.splitext(file_name)[0]
            readable_id = filename_to_id.get(name_stem)
            
            # 如果直接匹配不到，尝试去除可能的语言后缀匹配 (例如文件名是 Relic_CHS，但字典里只存了 Relic)
            # 或者字典里存了 Relic_CHS，但文件名是 Relic
            # 这里根据您提供的 JSON 例子，Map 里存的是带路径的完整字符串，我们之前 basename 处理过了。
            # 假设 Localization 里是 "Relic10008_4_CHS"，而文件也是 "Relic10008_4_CHS.txt"，则能直接匹配。
            
            if not readable_id:
                # 尝试 fuzzy match logic if needed, or simply skip
                continue
            
            # 1. 插入/更新 Readable 主表 (如果还没处理过这个ID)
            if readable_id not in inserted_readable_ids:
                title_hash = id_to_title.get(readable_id)
                # 使用 INSERT OR REPLACE 确保更新 titleHash
                cursor.execute("INSERT OR REPLACE INTO readable(readableId, titleTextMapHash) VALUES (?, ?)", 
                               (readable_id, title_hash))
                inserted_readable_ids.add(readable_id)
            
            # 2. 插入内容表
            try:
                full_path = os.path.join(lang_path, file_name)
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
            
        files = os.listdir(lang_path)
        print(f"  Processing {lang_name} ({len(files)} files)...")

        for fileName in tqdm(files):
            if not fileName.endswith(".srt"):
                continue

            # 文件名作为标识符 (去除后缀)
            cleanFileName = os.path.splitext(fileName)[0]
            full_path = os.path.join(lang_path, fileName)

            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                # 简单的 SRT 解析: 按空行分割块
                # 每个块格式:
                # 1
                # 00:00:00,490 --> 00:00:02,630
                # 文本内容...
                
                blocks = re.split(r'\n\s*\n', content.strip())
                for block in blocks:
                    lines = [l.strip() for l in block.split('\n') if l.strip()]
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
                    text_lines = lines[time_line_idx+1:]
                    text_content = "\n".join(text_lines)

                    if text_content:
                        cursor.execute(sql_insert, (cleanFileName, lang_id, start_time, end_time, text_content))

            except Exception as e:
                print(f"Error processing {fileName} in {lang_name}: {e}")

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
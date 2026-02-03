import os
import re
import json
import argparse
import shutil
from pathlib import Path
import sys
from DBConfig import conn, DATA_PATH
import voiceItemImport
import readableImport
import subtitleImport
import textMapImport
from tqdm import tqdm

# 语言文件夹名到数据库 langCode ID 的映射
LANG_MAP = {
    'CHS': 1, 'CHT': 2, 'DE': 3, 'EN': 4, 'ES': 5,
    'FR': 6, 'ID': 7, 'IT': 8, 'JP': 9, 'KR': 10,
    'PT': 11, 'RU': 12, 'TH': 13, 'TR': 14, 'VI': 15
}


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
            # print("Now: {} {}/{}".format(fileName, val, n))
            importTalk(folder + "\\" + fileName)


def importAvatars():
    cursor = conn.cursor()
    avatars = json.load(open(DATA_PATH + "\\ExcelBinOutput\\AvatarExcelConfigData.json", encoding='utf-8'))

    sql1 = "INSERT OR REPLACE INTO avatar(avatarId, nameTextMapHash) VALUES (?,?)"

    for avatar in avatars:
        cursor.execute(sql1, (avatar['id'], avatar['nameTextMapHash']))

    cursor.close()
    conn.commit()


def importFetters():
    cursor = conn.cursor()
    fetters = json.load(open(DATA_PATH + "\\ExcelBinOutput\\FettersExcelConfigData.json", encoding='utf-8'))
    sql1 = "INSERT OR REPLACE INTO fetters(fetterId, avatarId, voiceTitleTextMapHash, voiceFileTextTextMapHash, voiceFile) values (?,?,?,?,?)"

    for fetter in fetters:
        cursor.execute(sql1,(fetter['fetterId'], fetter['avatarId'], fetter['voiceTitleTextMapHash'], fetter['voiceFileTextTextMapHash'], fetter['voiceFile']))

    cursor.close()
    conn.commit()


def importFetterStories():
    cursor = conn.cursor()
    stories = json.load(open(DATA_PATH + "\\ExcelBinOutput\\FetterStoryExcelConfigData.json", encoding='utf-8'))
    sql1 = ("INSERT OR REPLACE INTO fetterStory("
            "fetterId, avatarId, storyTitleTextMapHash, storyTitle2TextMapHash, "
            "storyTitleLockedTextMapHash, storyContextTextMapHash, storyContext2TextMapHash"
            ") values (?,?,?,?,?,?,?)")

    for story in stories:
        cursor.execute(
            sql1,
            (
                story['fetterId'],
                story['avatarId'],
                story['storyTitleTextMapHash'],
                story['storyTitle2TextMapHash'],
                story['storyTitleLockedTextMapHash'],
                story['storyContextTextMapHash'],
                story['storyContext2TextMapHash'],
            ),
        )

    cursor.close()
    conn.commit()


def importQuest(fileName: str):
    cursor = conn.cursor()
    obj = json.load(open(DATA_PATH + "\\BinOutput\\Quest\\" + fileName, encoding='utf-8'))

    sql1 = 'INSERT OR REPLACE INTO quest(questId, titleTextMapHash, chapterId) VALUES (?,?,?)'
    sql2 = 'INSERT OR IGNORE INTO questTalk(questId, talkId) VALUES (?,?)'

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
    for val, fileName in tqdm(enumerate(files), total=len(files)):
        # print("Now: {} {}/{}".format(fileName, val, n))
        importQuest(fileName)
    conn.commit()


def importQuestBriefs():
    cursor = conn.cursor()
    folder = os.path.join(DATA_PATH, "BinOutput", "QuestBrief")
    if not os.path.isdir(folder):
        print("QuestBrief folder not found, skipping.")
        return

    sql = "INSERT OR IGNORE INTO questTalk(questId, talkId) VALUES (?,?)"
    files = os.listdir(folder)
    for _val, fileName in tqdm(enumerate(files), total=len(files)):
        if not fileName.endswith(".json"):
            continue
        try:
            obj = json.load(open(os.path.join(folder, fileName), encoding="utf-8"))
        except Exception:
            continue

        questId = None
        if "id" in obj:
            questId = obj["id"]
        elif "ILHDNJDDEOP" in obj:
            questId = obj["ILHDNJDDEOP"]

        subquests = None
        if "subQuests" in obj:
            subquests = obj["subQuests"]
        elif "GFLHMKOOHHA" in obj:
            subquests = obj["GFLHMKOOHHA"]

        if not isinstance(subquests, list):
            continue

        for subquest in subquests:
            mainQuestId = questId
            if mainQuestId is None:
                if "mainQuestId" in subquest:
                    mainQuestId = subquest["mainQuestId"]
                elif "GNGFBMPFBOK" in subquest:
                    mainQuestId = subquest["GNGFBMPFBOK"]

            contents = None
            if "finishCond" in subquest:
                contents = subquest["finishCond"]
            elif "KBFJAAFDHKJ" in subquest:
                contents = subquest["KBFJAAFDHKJ"]

            if not isinstance(contents, list):
                continue

            for cond in contents:
                cond_type = None
                if "type" in cond:
                    cond_type = cond["type"]
                elif "PAINLIBBLDK" in cond:
                    cond_type = cond["PAINLIBBLDK"]

                if cond_type != "QUEST_CONTENT_COMPLETE_TALK":
                    continue

                params = None
                if "param" in cond:
                    params = cond["param"]
                elif "paramList" in cond:
                    params = cond["paramList"]
                elif "LNHLPKELCAL" in cond:
                    params = cond["LNHLPKELCAL"]

                if not isinstance(params, list) or len(params) == 0:
                    continue
                talkId = params[0]
                if isinstance(talkId, int) and talkId > 0 and mainQuestId:
                    cursor.execute(sql, (mainQuestId, talkId))

    cursor.close()
    conn.commit()


def importChapters():
    cursor = conn.cursor()
    chapters = json.load(open(DATA_PATH + "\\ExcelBinOutput\\ChapterExcelConfigData.json", encoding='utf-8'))
    sql1 = "INSERT OR REPLACE INTO chapter(chapterId, chapterTitleTextMapHash, chapterNumTextMapHash) VALUES (?,?,?)"

    for chapter in chapters:
        cursor.execute(sql1,(chapter['id'], chapter['chapterTitleTextMapHash'], chapter['chapterNumTextMapHash']))

    cursor.close()
    conn.commit()


def importNPCs():
    cursor = conn.cursor()
    NPCs = json.load(open(DATA_PATH + "\\ExcelBinOutput\\NpcExcelConfigData.json", encoding='utf-8'))

    # 有就覆盖更新，没有就插入（兼容老 SQLite）
    sql1 = "INSERT OR REPLACE INTO npc(npcId, textHash) VALUES (?,?)"

    for npc in NPCs:
        cursor.execute(sql1, (npc['id'], npc['nameTextMapHash']))

    cursor.close()
    conn.commit()


def importManualTextMap():
    cursor = conn.cursor()
    placeholders = json.load(open(DATA_PATH + "\\ExcelBinOutput\\ManualTextMapConfigData.json", encoding='utf-8'))
    sql1 = "INSERT OR REPLACE INTO manualTextMap(textMapId, textHash) VALUES (?,?)"

    for placeholder in placeholders:
        cursor.execute(sql1, (placeholder['textMapId'], placeholder['textMapContentTextMapHash']))

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
    print("Importing fetter stories...")
    importFetterStories()
    print("Importing quests...")
    importAllQuests()
    print("Importing quest briefs...")
    importQuestBriefs()
    print("Importing chapters...")
    importChapters()
    print("Importing voices...")
    voiceItemImport.loadAvatars()
    voiceItemImport.importAllVoiceItems()
    print("Importing readable...")
    readableImport.importReadable()
    print("Importing subtitles...")
    subtitleImport.importSubtitles()
    print("Importing textMap...")
    textMapImport.importAllTextMap()
    print("Done!")


def set_db_version(conn, version: str):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS app_meta (k TEXT PRIMARY KEY, v TEXT)")
    cur.execute("INSERT OR REPLACE INTO app_meta(k, v) VALUES (?, ?)", ("db_version", version))
    conn.commit()

def _atomic_copy(src: Path, dst: Path):
    """
    安全复制数据库：
    - 先复制到临时文件
    - 再原子替换目标文件（Windows/Linux 都安全）
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, default="", help="将生成的数据库复制到指定路径")
    parser.add_argument("--dbver", type=str, default="", help="写入数据库版本号，例如 2026-01-21.1 或 v0.1.0-db3")
    args = parser.parse_args()

    # 1) 执行原 DBBuild
    main()

    # 2) 写入版本号（如果提供了）
    if args.dbver:
        # 你脚本里使用的 conn 是全局变量（你现在就是这样），所以这里能直接用
        try:
            set_db_version(conn, args.dbver)
            print(f"[INFO] db_version set to: {args.dbver}")
        except Exception as e:
            print(f"[ERROR] failed to set db_version: {e}", file=sys.stderr)
            sys.exit(3)

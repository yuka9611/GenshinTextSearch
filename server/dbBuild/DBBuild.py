import os
import re
import json
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


def importChapters():
    cursor = conn.cursor()
    chapters = json.load(open(DATA_PATH + "\\ExcelBinOutput\\ChapterExcelConfigData.json", encoding='utf-8'))
    sql1 = "insert into chapter(chapterId, chapterTitleTextMapHash, chapterNumTextMapHash) values (?,?,?)"

    for chapter in chapters:
        cursor.execute(sql1,(chapter['id'], chapter['chapterTitleTextMapHash'], chapter['chapterNumTextMapHash']))

    cursor.close()
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


if __name__ == "__main__":
    main()
    pass
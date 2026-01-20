import os
import sqlite3
import json
from DBConfig import conn, DATA_PATH
from tqdm import tqdm


# 去掉switch的角色名->角色ID
avatarMappings = {}


def loadAvatars():
    global avatarMappings
    avatarsJson = json.load(open(DATA_PATH + "\\ExcelBinOutput\\AvatarExcelConfigData.json", encoding='utf-8'))

    for avatar in avatarsJson:
        # 从icon里面拿名字
        avatarId = avatar['id']
        if avatarId >= 11000000:
            continue

        avatarName = avatar['iconName'][14:]

        # 从角色对应的ConfigAvatar里拿实际在角色语音使用的名字字段
        configAvatarFileName = f"ConfigAvatar_{avatarName}.json"
        configAvatarPath = os.path.join(DATA_PATH, "BinOutput", "Avatar", configAvatarFileName)

        if os.path.exists(configAvatarPath):
            avatarConfig = json.load(open(configAvatarPath, encoding='utf-8'))

            internalName = avatarConfig['PLJBIDOIHEA']['KDKDKOMMCOB']['GLMJHDNIGID'].lower()

            avatarMappings[internalName] = avatarId
        else:
            print(f"Error loading {configAvatarFileName} for {avatarName}")



def getAvatarIdFromVoiceItemAvatarName(avatarNameFromVoiceItem: str):
    rawName = avatarNameFromVoiceItem.lower()
    # GCG的怪物语音就不要了~
    if rawName.startswith("switch_gcg") or rawName.startswith("gcg"):
        return 0

    rawNameTranslate = {
        "switch_tartaglia_melee": 'switch_tartaglia'
    }
    if rawName in rawNameTranslate:
        rawName = rawNameTranslate[rawName]

    # rawNameTranslate = {
    #     'hero': 'playerboy',
    #     'heroine': 'playergirl',
    #     "tartaglia_melee": 'tartaglia',
    #     "alhaitham": "alhatham",
    #     'baizhu': 'baizhuer',
    #     'heizou': 'heizo',
    #     'kirara': 'momoka',
    #     'kujousara': 'sara',
    #     'lynette': 'linette',
    #     'lyney': 'liney',
    #     'raidenshogun': 'shougun',
    #     'thoma': 'tohma',
    #     'yaemiko': 'yae',
    #     'yanfei': 'feiyan',
    #     'xianyun': 'liuyun',
    #     'emelie': 'emilie'
    # }
    # if rawName in rawNameTranslate:
    #     rawName = rawNameTranslate[rawName]

    if rawName in avatarMappings:
        return avatarMappings[rawName]

    # raise Exception("AVATAR NOT FOUND! {}".format(rawName))
    print(f"Warning: Avatar not found in mapping: {rawName}")
    return 0


def importVoiceItem(fileName: str):
    cursor = conn.cursor()

    sql1 = "insert or ignore into voice(dialogueId, voicePath,gameTrigger, avatarId) values (?,?,?,?)"
    textMap = json.load(open(DATA_PATH + "\\BinOutput\\Voice\\Items\\" + fileName, encoding='utf-8'))
    for _, content in textMap.items():
        p1 = None
        p2 = None
        p3 = None
        p4 = None
        guidKeyName = None

        if 'guid' in content:
            p1 = 'gameTriggerArgs'
            p2 = 'sourceNames'
            p3 = 'sourceFileName'
            p4 = 'gameTrigger'
            guidKeyName = 'guid'
        
        elif 'Guid' in content:
            p1 = 'gameTriggerArgs'
            p2 = 'SourceNames'
            p3 = 'sourceFileName'
            p4 = 'GameTrigger'
            guidKeyName = 'Guid'

        # elif 'JFNDAOJCHPO' in content:
        elif 'ABAEBGLPCIK' in content:
            # p1 = 'FMHLBONJKPJ'
            p1 = 'MEDGFBMLDDK'
            # p2 = 'OFEEIPOMNKD'
            p2 = 'JKDJFGBGOEB'
            # p3 = 'CBGLAJNLFCB'
            p3 = 'DCIHFJLBLAP'
            # p4 = 'BFKCDJLLGNJ'
            p4 = 'BHOKINENJBN'
            guidKeyName = 'ABAEBGLPCIK'

        # elif 'HLGGFCENLPA' in content:
        elif 'NDLOFEPMEMO' in content:
            # p1 = 'FFDHLEAFBLM'
            p1 = 'HEKJMGHIJBM'
            # p2 = 'EIKJKDICKMJ'
            p2 = 'JKHGLBHOKIC'
            # p3 = 'HLGOMILNFNK'
            p3 = 'BJDAJEKPCFP'
            # p4 = 'BEHKGKMMAPD'
            p4 = 'HPIPCKOOMLL'
            guidKeyName = 'NDLOFEPMEMO'

        if p2 not in content or p1 not in content:
            continue

        dialogueId = content[p1]
        gameTrigger = content[p4]
        for voice in content[p2]:
            voicePath = voice[p3]
            if dialogueId is None or voicePath is None or gameTrigger is None:
                raise Exception("ERROR!")

            avatarId = 0
            # 没啥好办法，通过avatarName获得角色名称，再转换为角色id
            current_avatar_name = voice.get('avatarName') or voice.get('GDIJGLOHHFM', '')
            if guidKeyName in content and current_avatar_name != '':
                avatarId = getAvatarIdFromVoiceItemAvatarName(current_avatar_name)
                # print(voice['avatarName'], avatarId)

            cursor.execute(sql1, (dialogueId, voicePath, gameTrigger, avatarId))

    cursor.close()
    conn.commit()


def importAllVoiceItems():
    files = os.listdir(DATA_PATH + "\\BinOutput\\Voice\\Items\\")
    n = len(files)
    for val, fileName in tqdm(enumerate(files), total=len(files)):
        # print("Now: {} {}/{}".format(fileName, val, n))
        try:
            importVoiceItem(fileName)
        except Exception as e:
            print(f"Error processing file {fileName}: {e}")
            continue


if __name__ == "__main__":
    loadAvatars()
    importAllVoiceItems()
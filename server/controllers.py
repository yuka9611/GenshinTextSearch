import io
import zlib

import databaseHelper
import languagePackReader
import config
import placeholderHandler


def pickAssetDirViaDialog() -> str | None:
    """
    弹出选择文件夹对话框（Windows/macOS/Linux）
    - 返回选中的目录路径
    - 取消返回 None

    注意：这适合桌面发行版。如果你把服务部署到远端服务器，这个对话框不会在用户电脑上出现。
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None

    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        picked = filedialog.askdirectory(title="请选择原神资源目录（包含 StreamingAssets 或 Persistent）")
        root.destroy()
        if not picked:
            return None
        return picked
    except Exception:
        return None
    

def selectVoicePathFromTextHash(textHash: int):
    voicePath: str | None = databaseHelper.selectVoicePathFromTextHashInDialogue(textHash)
    if voicePath is not None:
        return voicePath

    voicePath = databaseHelper.selectVoicePathFromTextHashInFetter(textHash)
    if voicePath is not None:
        return voicePath

    return None


def selectVoiceOriginFromTextHash(textHash: int, langCode: int) -> tuple[str, bool]:
    origin = databaseHelper.getSourceFromDialogue(textHash, langCode)
    if origin is not None:
        return origin, True

    origin = databaseHelper.getSourceFromFetter(textHash, langCode)
    if origin is not None:
        return origin, False

    # TODO 支持更多类型的语音
    return "其他文本", False


def queryTextHashInfo(textHash: int, langs: 'list[int]', sourceLangCode: int, queryOrigin=True):
    obj = {'translates': {}, 'voicePaths': [], 'hash': textHash}
    translates = databaseHelper.selectTextMapFromTextHash(textHash, langs)
    for translate in translates:
        # #开头的要进行占位符替换
        if translate[0].startswith("#"):
            obj['translates'][translate[1]] = (placeholderHandler.replace(translate[0], config.getIsMale(), translate[1]))[1:]
        else:
            obj['translates'][translate[1]] = translate[0]

    if queryOrigin:
        origin, isTalk = selectVoiceOriginFromTextHash(textHash, sourceLangCode)
        obj['isTalk'] = isTalk
        obj['origin'] = origin

    voicePath = selectVoicePathFromTextHash(textHash)
    if voicePath is not None:
        voiceExist = False
        for lang in langs:
            if lang in languagePackReader.langPackages and languagePackReader.checkAudioBin(voicePath, lang):
                voiceExist = True
                break

        if voiceExist:
            obj['voicePaths'].append(voicePath)

    return obj


def getTranslateObj(keyword: str, langCode: int):
    # 找出目标语言的textMap包含keyword的文本
    ans = []

    contents = databaseHelper.selectTextMapFromKeyword(keyword, langCode)

    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    for content in contents:
        obj = queryTextHashInfo(content[0], langs, sourceLangCode)
        ans.append(obj)

    # Search readable
    langMap = databaseHelper.getLangCodeMap()
    if langCode in langMap:
        langStr = langMap[langCode]
        readableContents = databaseHelper.selectReadableFromKeyword(keyword, langStr)
        
        targetLangStrs = []
        for l in langs:
            if l in langMap:
                targetLangStrs.append(langMap[l])
        
        # Reverse map for result construction
        strToLangId = {v: k for k, v in langMap.items()}

        prefix_labels = {
            "Book": "书籍",
            "Costume": "衣装",
            "Relic": "圣遗物",
            "Weapon": "武器",
            "Wings": "风之翼",
        }

        for fileName, content, titleTextMapHash, readableId in readableContents:
            # Generate a stable hash for the filename
            fileHash = zlib.crc32(fileName.encode('utf-8'))
            
            origin_label = "阅读物"
            for prefix, label in prefix_labels.items():
                if fileName.startswith(prefix):
                    origin_label = label
                    break

            origin = f"{origin_label}: {fileName}"
            if titleTextMapHash:
                title = databaseHelper.getTextMapContent(titleTextMapHash, sourceLangCode)
                if title:
                    origin = f"{origin_label}: {fileName} ({title})"

            obj = {
                'translates': {},
                'voicePaths': [],
                'hash': fileHash,
                'isTalk': False,
                'origin': origin
            }
            
            # Get translations
            translations = []
            if readableId:
                # Use the readableId (Localization ID) to find translations directly
                translations = databaseHelper.selectReadableFromReadableId(readableId, targetLangStrs)
            
            # Fallback if readableId is missing or didn't return results (e.g. old DB or manual file)
            if not translations:
                translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)

            for transContent, transLangStr in translations:
                if transLangStr in strToLangId:
                    obj['translates'][strToLangId[transLangStr]] = transContent
            
            ans.append(obj)

    # Search subtitles
    subtitleContents = databaseHelper.selectSubtitleFromKeyword(keyword, langCode)
    for fileName, content, startTime, endTime, subtitleId in subtitleContents:
        # Generate a stable hash for the subtitle line
        # Use fileName + startTime to be unique
        fileHash = zlib.crc32(f"{fileName}_{startTime}".encode('utf-8'))
        
        origin = f"字幕: {fileName}"
        
        obj = {
            'translates': {},
            'voicePaths': [],
            'hash': fileHash,
            'isTalk': False,
            'origin': origin,
            'isSubtitle': True,
            'fileName': fileName,
            'subtitleId': subtitleId
        }
        
        # Get translations
        translations = []
        if subtitleId:
            translations = databaseHelper.selectSubtitleTranslationsBySubtitleId(subtitleId, startTime, langs)
        
        if not translations:
            translations = databaseHelper.selectSubtitleTranslations(fileName, startTime, langs)
        
        for transContent, transLangCode in translations:
            obj['translates'][transLangCode] = transContent
            
        ans.append(obj)

    return ans


def searchNameEntries(keyword: str, langCode: int):
    quests = []
    readables = []

    questMatches = databaseHelper.selectQuestByTitleKeyword(keyword, langCode)
    for questId, questTitle in questMatches:
        chapterName = databaseHelper.getQuestChapterName(questId, langCode)
        quests.append({
            "questId": questId,
            "title": questTitle,
            "chapterName": chapterName
        })

    langMap = databaseHelper.getLangCodeMap()
    if langCode in langMap:
        langStr = langMap[langCode]
        readableMatches = databaseHelper.selectReadableByTitleKeyword(keyword, langCode, langStr)
        for fileName, readableId, titleTextMapHash, title in readableMatches:
            readables.append({
                "fileName": fileName,
                "readableId": readableId,
                "title": title,
                "titleTextMapHash": titleTextMapHash
            })

    return {
        "quests": quests,
        "readables": readables
    }


def getQuestDialogues(questId: int, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    questCompleteName = databaseHelper.getQuestName(questId, sourceLangCode)
    talkIds = databaseHelper.selectQuestTalkIds(questId)
    dialogues = []

    for talkId in talkIds:
        rawDialogues = databaseHelper.getTalkContent(talkId, None)
        if rawDialogues is None:
            continue
        for rawDialogue in rawDialogues:
            textHash, talkerType, talkerId, dialogueId = rawDialogue
            obj = queryTextHashInfo(textHash, langs, sourceLangCode, False)
            obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
            obj['dialogueId'] = dialogueId
            obj['talkId'] = talkId
            dialogues.append(obj)

    dialogues.sort(key=lambda item: (item.get('talkId', 0), item.get('dialogueId', 0)))

    return {
        "talkQuestName": questCompleteName,
        "talkId": 0,
        "dialogues": dialogues
    }


# 根据hash值查询整个对话的内容
def getTalkFromHash(textHash: int, searchLang: int | None = None):
    # 先查到文本所属的talk，然后查询对话所属的任务的标题，然后查询对话所有的内容，对于每一句话，查询多语言翻译、说话者
    talkInfo = databaseHelper.getTalkInfo(textHash)
    if talkInfo is None:
        raise Exception("内容不属于任何对话！")

    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    talkId, talkerType, talkerId, coopQuestId = talkInfo
    if coopQuestId is None:
        questCompleteName = databaseHelper.getTalkQuestName(talkId, sourceLangCode)
    else:
        questCompleteName = databaseHelper.getCoopTalkQuestName(coopQuestId, sourceLangCode)

    rawDialogues = databaseHelper.getTalkContent(talkId, coopQuestId)
    dialogues = []

    if rawDialogues is None:
        rawDialogues = []

    for rawDialogue in rawDialogues:
        textHash, talkerType, talkerId, dialogueId = rawDialogue
        obj = queryTextHashInfo(textHash, langs, sourceLangCode, False)
        obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
        obj['dialogueId'] = dialogueId
        dialogues.append(obj)

    ans = {
        "talkQuestName": questCompleteName,
        "talkId": talkId,
        "dialogues": dialogues
    }

    return ans


def getReadableContent(readableId: int | None, fileName: str | None, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    langMap = databaseHelper.getLangCodeMap()
    targetLangStrs = []
    for l in langs:
        if l in langMap:
            targetLangStrs.append(langMap[l])

    readableInfo = databaseHelper.getReadableInfo(readableId, fileName)
    readableTitle = None
    if readableInfo:
        fileName, titleTextMapHash, readableId = readableInfo
        if titleTextMapHash:
            readableTitle = databaseHelper.getTextMapContent(titleTextMapHash, sourceLangCode)

    translations = []
    if readableId:
        translations = databaseHelper.selectReadableFromReadableId(readableId, targetLangStrs)
    if not translations and fileName:
        translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)

    strToLangId = {v: k for k, v in langMap.items()}
    translateMap = {}
    for transContent, transLangStr in translations:
        if transLangStr in strToLangId:
            translateMap[strToLangId[transLangStr]] = transContent

    return {
        "fileName": fileName,
        "readableId": readableId,
        "readableTitle": readableTitle,
        "translates": translateMap
    }

def getSubtitleContext(fileName: str, subtitleId: int | None = None, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    
    # Fetch all lines
    if subtitleId:
        lines = databaseHelper.selectSubtitleContextBySubtitleId(subtitleId, langs)
    else:
        lines = databaseHelper.selectSubtitleContext(fileName, langs)
        
    # Group by startTime (cluster within <threshold> seconds)
    clusters = []
    
    # Sort lines by startTime
    lines.sort(key=lambda x: x[2])

    threshold = 0.5
    
    for content, lang, startTime, endTime in lines:
        # Check last cluster
        if clusters and abs(clusters[-1]['time'] - startTime) < threshold and lang not in clusters[-1]['translates']:
            clusters[-1]['translates'][lang] = content
        else:
            clusters.append({
                'time': startTime,
                'translates': {lang: content},
                'voicePaths': [],
                'talker': '', # Subtitles usually don't have talker info in the text
                'dialogueId': int(startTime * 1000) # Use ms as ID
            })
            
    # Format for frontend
    dialogues = []
    for cluster in clusters:
        dialogues.append({
            'talker': '',
            'translates': cluster['translates'],
            'voicePaths': [],
            'dialogueId': cluster['dialogueId']
        })
        
    return {
        "talkQuestName": f"字幕: {fileName}",
        "talkId": 0,
        "dialogues": dialogues
    }


def getVoiceBinStream(voicePath, langCode):
    wemBin = languagePackReader.getAudioBin(voicePath, langCode)
    if wemBin is None:
        return None
    return io.BytesIO(wemBin)


def getLoadedVoicePacks():
    ans = {}
    for packId in languagePackReader.langPackages:
        ans[packId] = languagePackReader.langCodes[packId]

    return ans


def getImportedTextMapLangs():
    langs = databaseHelper.getImportedTextMapLangs()
    ans = {}
    for langItem in langs:
        ans[langItem[0]] = langItem[1]

    return ans


def getConfig():
    # 返回 config + 额外状态字段（前端可以直接显示“目录是否有效”）
    cfg = dict(config.config)
    cfg["assetDirValid"] = config.isAssetDirValid()
    return cfg


def setDefaultSearchLanguage(newLanguage: int):
    config.setDefaultSearchLanguage(newLanguage)


def setResultLanguages(newLanguages: list[int]):
    config.setResultLanguages(newLanguages)


def saveConfig():
    config.saveConfig()


def setSourceLanguage(newSourceLanguage):
    config.setSourceLanguage(newSourceLanguage)


def setIsMale(isMale):
    config.setIsMale(isMale)
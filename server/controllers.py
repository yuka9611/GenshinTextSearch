import io
import zlib

import databaseHelper
import languagePackReader
import config
import placeholderHandler


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

        for fileName, content, titleTextMapHash, readableId in readableContents:
            # Generate a stable hash for the filename
            fileHash = zlib.crc32(fileName.encode('utf-8'))
            
            origin = f"阅读物: {fileName}"
            if titleTextMapHash:
                title = databaseHelper.getTextMapContent(titleTextMapHash, sourceLangCode)
                if title:
                    origin = f"阅读物: {fileName} ({title})"

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
    return config.config


def setDefaultSearchLanguage(newLanguage: int):
    config.setDefaultSearchLanguage(newLanguage)


def setResultLanguages(newLanguages: list[int]):
    config.setResultLanguages(newLanguages)


def saveConfig():
    config.saveConfig()


def setSourceLanguage(newSourceLanguage):
    config.setSourceLanguage(newSourceLanguage)


def setIsMale(isMale: bool):
    config.setIsMale(isMale)
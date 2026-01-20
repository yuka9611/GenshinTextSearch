import io

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
    # 找出包含 keyword 的结果
    raw_results = databaseHelper.selectTextMapFromKeyword(keyword, langCode)

    ans = []
    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    for item in raw_results:
        if isinstance(item, dict):
            # --- 修复核心：将书籍/字幕字典封装成前端兼容的标准对象 ---
            obj = {
                # 前端依靠 translates 字段遍历显示。
                # key 是语言代码(int)，value 是内容(str)。
                'translates': { langCode: item['content'] }, 
                'voicePaths': [],
                'hash': item['id'], # 使用 id 作为 hash，确保唯一性
                'isTalk': False,
                'origin': item['origin'] # 之前在 databaseHelper 里设置好的标题/文件名
            }
            ans.append(obj)
        else:
            # 普通文本（元组），走原有逻辑
            textHash = item[0]
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            if len(item) > 1:
                 obj['content'] = item[1]
            ans.append(obj)
            
    return ans


# 根据hash值查询整个对话的内容
def getTalkFromHash(textHash: int):
    # 先查到文本所属的talk，然后查询对话所属的任务的标题，然后查询对话所有的内容，对于每一句话，查询多语言翻译、说话者
    talkInfo = databaseHelper.getTalkInfo(textHash)
    if talkInfo is None:
        raise ValueError("内容不属于任何对话！")

    langs = config.getResultLanguages()
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

import io
import re
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


def _normalize_text_map_content(content: str | None, lang_code: int):
    if content is None:
        return None
    if content.startswith("#"):
        return placeholderHandler.replace(content, config.getIsMale(), lang_code)[1:]
    return content


_INT_PATTERN = re.compile(r"^[+-]?\d+$")
_HEX_PATTERN = re.compile(r"^[+-]?0x[0-9a-fA-F]+$")


def _parse_int_keyword(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    if _HEX_PATTERN.match(text):
        return int(text, 16)
    if _INT_PATTERN.match(text):
        return int(text, 10)
    return None


def _build_text_map_translates(text_hash: int | None, langs: 'list[int]'):
    if not text_hash:
        return None
    translates = databaseHelper.selectTextMapFromTextHash(text_hash, langs)
    if not translates:
        return None
    result = {}
    for content, lang_code in translates:
        result[lang_code] = _normalize_text_map_content(content, lang_code)
    return result if result else None


def getTranslateObj(
    keyword: str,
    langCode: int,
    speaker: str | None = None,
    page: int = 1,
    page_size: int = 50,
    voice_filter: str = "all",
):
    speaker_keyword = (speaker or "").strip()
    keyword_trim = keyword.strip()

    def normalize_speaker(value: str) -> str:
        normalized = value.strip().lower()
        if langCode in databaseHelper.CHINESE_LANG_CODES:
            normalized = "".join(normalized.split())
        return normalized

    safe_page = page if page and page > 0 else 1
    safe_size = page_size if page_size and page_size > 0 else 50
    page_end = safe_page * safe_size
    candidate_limit = page_end if voice_filter == "all" else page_end * 3

    def apply_voice_filter(entries: list[dict]) -> list[dict]:
        if voice_filter == "with":
            return [entry for entry in entries if len(entry.get('voicePaths', [])) > 0]
        if voice_filter == "without":
            return [entry for entry in entries if len(entry.get('voicePaths', [])) == 0]
        return entries

    def paginate(entries: list[dict], total: int | None = None) -> tuple[list[dict], int]:
        total_count = total if total is not None else len(entries)
        safe_page = page if page and page > 0 else 1
        safe_size = page_size if page_size and page_size > 0 else 50
        start = (safe_page - 1) * safe_size
        end = start + safe_size
        return entries[start:end], total_count

    if keyword_trim == "" and speaker_keyword:
        ans = []
        langs = config.getResultLanguages().copy()
        if langCode not in langs:
            langs.append(langCode)
        sourceLangCode = config.getSourceLanguage()

        seen_hashes = set()
        speaker_norm = normalize_speaker(speaker_keyword)

        dialogue_rows = databaseHelper.selectDialogueByTalkerKeyword(
            speaker_keyword, langCode, candidate_limit
        )
        for textHash, talkerType, talkerId, _dialogueId in dialogue_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
            ans.append(obj)

        total = databaseHelper.countDialogueByTalkerKeyword(speaker_keyword, langCode)

        for talkerType in ("TALK_ROLE_PLAYER", "TALK_ROLE_MATE_AVATAR"):
            talkerName = databaseHelper.getTalkerName(talkerType, 0, langCode)
            if not talkerName:
                continue
            talker_norm = normalize_speaker(talkerName)
            if speaker_norm not in talker_norm:
                continue
            talker_rows = databaseHelper.selectDialogueByTalkerType(talkerType, candidate_limit)
            for textHash, talkerType, talkerId, _dialogueId in talker_rows:
                if textHash in seen_hashes:
                    continue
                seen_hashes.add(textHash)
                obj = queryTextHashInfo(textHash, langs, sourceLangCode)
                obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
                ans.append(obj)
            total += databaseHelper.countDialogueByTalkerType(talkerType)

        ans = apply_voice_filter(ans)
        return paginate(ans, total)
    if keyword_trim != "" and speaker_keyword:
        ans = []
        langs = config.getResultLanguages().copy()
        if langCode not in langs:
            langs.append(langCode)
        sourceLangCode = config.getSourceLanguage()

        seen_hashes = set()

        dialogue_rows = databaseHelper.selectDialogueByTalkerAndKeyword(
            speaker_keyword, keyword_trim, langCode, candidate_limit
        )
        for textHash, talkerType, talkerId, _dialogueId in dialogue_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, langCode)
            ans.append(obj)

        total = databaseHelper.countDialogueByTalkerAndKeyword(speaker_keyword, keyword_trim, langCode)

        speaker_norm = normalize_speaker(speaker_keyword)
        for talkerType in ("TALK_ROLE_PLAYER", "TALK_ROLE_MATE_AVATAR"):
            talkerName = databaseHelper.getTalkerName(talkerType, 0, langCode)
            if not talkerName:
                continue
            talker_norm = normalize_speaker(talkerName)
            if speaker_norm not in talker_norm:
                continue
            talker_rows = databaseHelper.selectDialogueByTalkerTypeAndKeyword(
                talkerType, keyword_trim, langCode, candidate_limit
            )
            for textHash, talkerType, talkerId, _dialogueId in talker_rows:
                if textHash in seen_hashes:
                    continue
                seen_hashes.add(textHash)
                obj = queryTextHashInfo(textHash, langs, sourceLangCode)
                obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, langCode)
                ans.append(obj)
            total += databaseHelper.countDialogueByTalkerTypeAndKeyword(talkerType, keyword_trim, langCode)

        fetter_rows = databaseHelper.selectFetterBySpeakerAndKeyword(
            speaker_keyword, keyword_trim, langCode, candidate_limit
        )
        for textHash, avatarId in fetter_rows:
            if textHash in seen_hashes:
                continue
            seen_hashes.add(textHash)
            obj = queryTextHashInfo(textHash, langs, sourceLangCode)
            obj['talker'] = databaseHelper.getCharterName(avatarId, langCode)
            ans.append(obj)
        total += databaseHelper.countFetterBySpeakerAndKeyword(speaker_keyword, keyword_trim, langCode)

        ans = apply_voice_filter(ans)
        return paginate(ans, total)
    # 找出目标语言的textMap包含keyword的文本
    ans = []

    langs = config.getResultLanguages().copy()
    if langCode not in langs:
        langs.append(langCode)
    sourceLangCode = config.getSourceLanguage()

    hash_value = _parse_int_keyword(keyword_trim)
    is_hash_query = hash_value is not None
    text_hashes_seen = set()
    hash_obj = None
    hash_extra = False

    if hash_value is not None:
        hash_obj = queryTextHashInfo(hash_value, langs, sourceLangCode)
        if hash_obj.get('translates'):
            hash_obj['hashMatch'] = True
            hash_extra = not databaseHelper.isTextMapHashInKeyword(
                hash_value, keyword_trim, langCode
            )

    langMap = databaseHelper.getLangCodeMap()
    langStr = langMap.get(langCode)
    targetLangStrs = []
    for l in langs:
        if l in langMap:
            targetLangStrs.append(langMap[l])
    strToLangId = {v: k for k, v in langMap.items()}
    prefix_labels = {
        "Book": "书籍",
        "Costume": "衣装",
        "Relic": "圣遗物",
        "Weapon": "武器",
        "Wings": "风之翼",
    }

    if voice_filter == "all":
        total_textmap = databaseHelper.countTextMapFromKeyword(keyword, langCode)
        total_readable = 0
        if langStr:
            total_readable = databaseHelper.countReadableFromKeyword(keyword, langCode, langStr)
        total_subtitle = databaseHelper.countSubtitleFromKeyword(keyword, langCode)
        total = total_textmap + total_readable + total_subtitle + (1 if hash_extra else 0)

        offset = (safe_page - 1) * safe_size
        remaining = safe_size

        if hash_extra and offset == 0 and hash_obj is not None:
            ans.append(hash_obj)
            text_hashes_seen.add(hash_value)
            remaining -= 1

        offset_after_hash = offset - (1 if hash_extra else 0)
        if offset_after_hash < 0:
            offset_after_hash = 0

        if remaining > 0 and offset_after_hash < total_textmap:
            rows = databaseHelper.selectTextMapFromKeywordPaged(
                keyword,
                langCode,
                remaining,
                offset_after_hash,
                hash_value if is_hash_query else None,
            )
            for text_hash, _content in rows:
                if text_hash in text_hashes_seen:
                    continue
                text_hashes_seen.add(text_hash)
                obj = queryTextHashInfo(text_hash, langs, sourceLangCode)
                ans.append(obj)
            remaining = safe_size - len(ans)
            offset_after_hash = 0
        else:
            offset_after_hash -= total_textmap

        if remaining > 0 and langStr and offset_after_hash < total_readable:
            readableContents = databaseHelper.selectReadableFromKeyword(
                keyword, langCode, langStr, remaining, offset_after_hash
            )
            for fileName, content, titleTextMapHash, readableId in readableContents:
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

                translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)
                for transContent, transLangStr in translations:
                    if transLangStr in strToLangId:
                        obj['translates'][strToLangId[transLangStr]] = transContent

                ans.append(obj)

            remaining = safe_size - len(ans)
            offset_after_hash = 0
        else:
            offset_after_hash -= total_readable

        if remaining > 0 and offset_after_hash < total_subtitle:
            subtitleContents = databaseHelper.selectSubtitleFromKeyword(
                keyword, langCode, remaining, offset_after_hash
            )
            for fileName, content, startTime, endTime, subtitleId in subtitleContents:
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
                translations = []
                if subtitleId:
                    translations = databaseHelper.selectSubtitleTranslationsBySubtitleId(subtitleId, startTime, langs)
                if not translations:
                    translations = databaseHelper.selectSubtitleTranslations(fileName, startTime, langs)
                for transContent, transLangCode in translations:
                    obj['translates'][transLangCode] = transContent
                ans.append(obj)

        return ans, total

    if voice_filter != "all":
        hash_extra_filtered = False
        if hash_extra and hash_obj is not None:
            hash_has_voice = databaseHelper.hasVoiceForTextHashDb(hash_value)
            if (voice_filter == "with" and hash_has_voice) or (
                voice_filter == "without" and not hash_has_voice
            ):
                hash_extra_filtered = True

        total_textmap = databaseHelper.countTextMapFromKeywordVoice(keyword, langCode, voice_filter)
        total_readable = 0
        total_subtitle = 0
        if voice_filter == "without":
            if langStr:
                total_readable = databaseHelper.countReadableFromKeyword(keyword, langCode, langStr)
            total_subtitle = databaseHelper.countSubtitleFromKeyword(keyword, langCode)

        total = total_textmap + total_readable + total_subtitle + (1 if hash_extra_filtered else 0)

        offset = (safe_page - 1) * safe_size
        remaining = safe_size

        if hash_extra_filtered and offset == 0 and hash_obj is not None:
            ans.append(hash_obj)
            remaining -= 1

        offset_after_hash = offset - (1 if hash_extra_filtered else 0)
        if offset_after_hash < 0:
            offset_after_hash = 0

        if remaining > 0 and offset_after_hash < total_textmap:
            rows = databaseHelper.selectTextMapFromKeywordPaged(
                keyword,
                langCode,
                remaining,
                offset_after_hash,
                hash_value if is_hash_query else None,
                voice_filter,
            )
            for text_hash, _content in rows:
                if text_hash in text_hashes_seen:
                    continue
                text_hashes_seen.add(text_hash)
                obj = queryTextHashInfo(text_hash, langs, sourceLangCode)
                ans.append(obj)
            remaining = safe_size - len(ans)
            offset_after_hash = 0
        else:
            offset_after_hash -= total_textmap

        if voice_filter == "without":
            if remaining > 0 and langStr and offset_after_hash < total_readable:
                readableContents = databaseHelper.selectReadableFromKeyword(
                    keyword, langCode, langStr, remaining, offset_after_hash
                )
                for fileName, content, titleTextMapHash, readableId in readableContents:
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

                    translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)
                    for transContent, transLangStr in translations:
                        if transLangStr in strToLangId:
                            obj['translates'][strToLangId[transLangStr]] = transContent

                    ans.append(obj)

                remaining = safe_size - len(ans)
                offset_after_hash = 0
            else:
                offset_after_hash -= total_readable

            if remaining > 0 and offset_after_hash < total_subtitle:
                subtitleContents = databaseHelper.selectSubtitleFromKeyword(
                    keyword, langCode, remaining, offset_after_hash
                )
                for fileName, content, startTime, endTime, subtitleId in subtitleContents:
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
                    translations = []
                    if subtitleId:
                        translations = databaseHelper.selectSubtitleTranslationsBySubtitleId(subtitleId, startTime, langs)
                    if not translations:
                        translations = databaseHelper.selectSubtitleTranslations(fileName, startTime, langs)
                    for transContent, transLangCode in translations:
                        obj['translates'][transLangCode] = transContent
                    ans.append(obj)

        return ans, total

    if hash_obj and hash_obj.get('translates'):
        ans.append(hash_obj)
        text_hashes_seen.add(hash_value)

    contents = databaseHelper.selectTextMapFromKeywordPaged(
        keyword, langCode, candidate_limit, 0, hash_value if is_hash_query else None
    )

    for content in contents:
        text_hash = content[0]
        if text_hash in text_hashes_seen:
            continue
        text_hashes_seen.add(text_hash)
        obj = queryTextHashInfo(text_hash, langs, sourceLangCode)
        ans.append(obj)

    if langStr:
        readableContents = databaseHelper.selectReadableFromKeyword(
            keyword, langCode, langStr, candidate_limit
        )
        for fileName, content, titleTextMapHash, readableId in readableContents:
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

            translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)
            for transContent, transLangStr in translations:
                if transLangStr in strToLangId:
                    obj['translates'][strToLangId[transLangStr]] = transContent
            ans.append(obj)

    subtitleContents = databaseHelper.selectSubtitleFromKeyword(keyword, langCode, candidate_limit)
    for fileName, content, startTime, endTime, subtitleId in subtitleContents:
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
        translations = []
        if subtitleId:
            translations = databaseHelper.selectSubtitleTranslationsBySubtitleId(subtitleId, startTime, langs)
        if not translations:
            translations = databaseHelper.selectSubtitleTranslations(fileName, startTime, langs)
        for transContent, transLangCode in translations:
            obj['translates'][transLangCode] = transContent
        ans.append(obj)

    keyword_lower = keyword.strip().lower()

    def is_exact_match(entry: dict) -> bool:
        if not keyword_lower:
            return False
        target_text = entry.get('translates', {}).get(langCode)
        if not isinstance(target_text, str):
            return False
        return keyword_lower in target_text.lower()

    def has_voice(entry: dict) -> bool:
        return len(entry.get('voicePaths', [])) > 0

    def sort_key(entry: dict) -> tuple[int, int, int, int]:
        hash_rank = 0 if (is_hash_query and entry.get('hash') == hash_value) else 1
        exact = is_exact_match(entry)
        voice = has_voice(entry)
        target_len = len(entry.get('translates', {}).get(langCode, "") or "")
        return (
            hash_rank,
            0 if exact else 1,
            0 if (not exact and voice) else 1,
            target_len,
        )

    ans.sort(key=sort_key)
    ans = apply_voice_filter(ans)

    total = databaseHelper.countTextMapFromKeyword(keyword, langCode)
    if langStr:
        total += databaseHelper.countReadableFromKeyword(keyword, langCode, langStr)
    total += databaseHelper.countSubtitleFromKeyword(keyword, langCode)
    if hash_extra:
        total += 1

    return paginate(ans, total)


def searchNameEntries(keyword: str, langCode: int):
    quests = []
    readables = []

    def format_chapter_name(chapter_num: str | None, chapter_title: str | None):
        if chapter_num and chapter_title:
            return '{} · {}'.format(chapter_num, chapter_title)
        if chapter_title:
            return chapter_title
        if chapter_num:
            return chapter_num
        return None
    quest_map = {}
    questMatches = databaseHelper.selectQuestByTitleKeyword(keyword, langCode)
    for questId, questTitle in questMatches:
        chapterName = databaseHelper.getQuestChapterName(questId, langCode)
        quest_map[questId] = {
            "questId": questId,
            "title": questTitle,
            "chapterName": chapterName
        }

    chapterMatches = databaseHelper.selectQuestByChapterKeyword(keyword, langCode)
    for questId, questTitle, chapterTitle, chapterNum in chapterMatches:
        if questId in quest_map:
            continue
        chapterName = format_chapter_name(chapterNum, chapterTitle)
        quest_map[questId] = {
            "questId": questId,
            "title": questTitle,
            "chapterName": chapterName
        }
    questIdMatches = databaseHelper.selectQuestByIdContains(keyword, langCode)
    for questId, questTitle in questIdMatches:
        if questId in quest_map:
            continue
        chapterName = databaseHelper.getQuestChapterName(questId, langCode)
        quest_map[questId] = {
            "questId": questId,
            "title": questTitle,
            "chapterName": chapterName
        }

    quests.extend(quest_map.values())

    langMap = databaseHelper.getLangCodeMap()
    if langCode in langMap:
        langStr = langMap[langCode]
        readableMatches = databaseHelper.selectReadableByTitleKeyword(keyword, langCode, langStr)
        readable_seen = set()
        for fileName, readableId, titleTextMapHash, title in readableMatches:
            readables.append({
                "fileName": fileName,
                "readableId": readableId,
                "title": title,
                "titleTextMapHash": titleTextMapHash
            })
            readable_seen.add((readableId, fileName))
        readableFileMatches = databaseHelper.selectReadableByFileNameContains(keyword, langCode, langStr)
        for fileName, readableId, titleTextMapHash, title in readableFileMatches:
            key = (readableId, fileName)
            if key in readable_seen:
                continue
            readable_seen.add(key)
            readables.append({
                "fileName": fileName,
                "readableId": readableId,
                "title": title or fileName,
                "titleTextMapHash": titleTextMapHash
            })

    return {
        "quests": quests,
        "readables": readables
    }


def searchAvatarEntries(keyword: str, langCode: int):
    avatars = []
    matches = databaseHelper.selectAvatarByNameKeyword(keyword, langCode)
    for avatarId, avatarName in matches:
        avatars.append({
            "avatarId": avatarId,
            "name": avatarName
        })
    return {
        "avatars": avatars
    }


def getAvatarVoices(avatarId: int, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    avatarName = databaseHelper.getCharterName(avatarId, sourceLangCode)
    voice_rows = databaseHelper.selectAvatarVoiceItems(avatarId)

    voices = []
    seen_hashes = set()

    for titleHash, textHash, voicePath in voice_rows:
        if textHash in seen_hashes:
            continue
        seen_hashes.add(textHash)

        obj = queryTextHashInfo(textHash, langs, sourceLangCode)
        obj['isTalk'] = False
        if avatarName:
            if titleHash:
                title = databaseHelper.getTextMapContent(titleHash, sourceLangCode)
                if title:
                    obj['origin'] = f"{avatarName} · {title}"
                    obj['voiceTitle'] = title
                else:
                    obj['origin'] = avatarName
                    obj['voiceTitle'] = ""
            else:
                obj['origin'] = avatarName
                obj['voiceTitle'] = ""
        else:
            obj['voiceTitle'] = ""

        if voicePath and voicePath not in obj.get('voicePaths', []):
            voiceExist = False
            for lang in langs:
                if lang in languagePackReader.langPackages and languagePackReader.checkAudioBin(voicePath, lang):
                    voiceExist = True
                    break
            if voiceExist:
                obj['voicePaths'].append(voicePath)

        voices.append(obj)

    return {
        "avatarId": avatarId,
        "avatarName": avatarName,
        "voices": voices
    }


def getAvatarStories(avatarId: int, searchLang: int | None = None):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    avatarName = databaseHelper.getCharterName(avatarId, sourceLangCode)
    story_rows = databaseHelper.selectAvatarStories(avatarId)

    stories = []

    for (fetterId,
         storyTitleHash,
         storyTitle2Hash,
         storyTitleLockedHash,
         storyContextHash,
         storyContext2Hash) in story_rows:

        per_story_seen = set()
        for context_hash, title_hash in (
            (storyContextHash, storyTitleHash),
            (storyContext2Hash, storyTitle2Hash),
        ):
            translates = _build_text_map_translates(context_hash, langs)
            if not translates:
                continue

            source_text = (translates.get(sourceLangCode) or "").strip()
            if source_text:
                if source_text in per_story_seen:
                    continue
                per_story_seen.add(source_text)

            title = None
            if title_hash:
                title = _normalize_text_map_content(
                    databaseHelper.getTextMapContent(title_hash, sourceLangCode),
                    sourceLangCode
                )
            if not title and storyTitleLockedHash:
                title = _normalize_text_map_content(
                    databaseHelper.getTextMapContent(storyTitleLockedHash, sourceLangCode),
                    sourceLangCode
                )

            if avatarName and title:
                origin = f"{avatarName} ﾂｷ {title}"
            elif avatarName:
                origin = avatarName
            elif title:
                origin = title
            else:
                origin = "角色故事"

            stories.append({
                "translates": translates,
                "voicePaths": [],
                "hash": context_hash,
                "isTalk": False,
                "origin": origin,
                "storyTitle": title or "",
                "fetterId": fetterId,
                "avatarName": avatarName or ""
            })

    return {
        "avatarId": avatarId,
        "avatarName": avatarName,
        "stories": stories
    }


def getQuestDialogues(
    questId: int,
    searchLang: int | None = None,
    page: int = 1,
    page_size: int = 200,
):
    langs = config.getResultLanguages().copy()
    if searchLang and searchLang not in langs:
        langs.append(searchLang)
    sourceLangCode = config.getSourceLanguage()

    questCompleteName = databaseHelper.getQuestName(questId, sourceLangCode)

    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 200

    total = databaseHelper.countQuestDialogues(questId)
    offset = (page - 1) * page_size
    rows = databaseHelper.selectQuestDialoguesPaged(questId, page_size, offset)
    dialogues = []

    for textHash, talkerType, talkerId, dialogueId, talkId in rows:
        obj = queryTextHashInfo(textHash, langs, sourceLangCode, False)
        obj['talker'] = databaseHelper.getTalkerName(talkerType, talkerId, sourceLangCode)
        obj['dialogueId'] = dialogueId
        obj['talkId'] = talkId
        dialogues.append(obj)

    return {
        "talkQuestName": questCompleteName,
        "talkId": 0,
        "dialogues": dialogues,
    }, total


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

    if fileName:
        readableInfo = databaseHelper.getReadableInfo(None, fileName)
    else:
        readableInfo = databaseHelper.getReadableInfo(readableId, None)
    readableTitle = None
    if readableInfo:
        fileName, titleTextMapHash, readableId = readableInfo
        if titleTextMapHash:
            readableTitle = databaseHelper.getTextMapContent(titleTextMapHash, sourceLangCode)

    translations = []
    if fileName:
        translations = databaseHelper.selectReadableFromFileName(fileName, targetLangStrs)
    elif readableId:
        translations = databaseHelper.selectReadableFromReadableId(readableId, targetLangStrs)

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

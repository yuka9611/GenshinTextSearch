import sqlite3
from contextlib import closing
import re
from pathlib import Path

import config


def get_connection() -> sqlite3.Connection:
    """
    连接用户目录下的 DB（可写、稳定）
    如果不存在就从打包资源复制出来
    """
    config.ensure_db_exists("data.db")
    db_path: Path = config.get_db_path()
    if not db_path.exists():
        # 给一个更直观的错误
        raise FileNotFoundError(f"Database file not found: {db_path}")

    return sqlite3.connect(str(db_path), check_same_thread=False)


# 单例连接（你原本就是单连接设计）
conn = get_connection()


def _escape_like(value: str) -> str:
    return (value
            .replace("\\", "\\\\")
            .replace("%", r"\%")
            .replace("_", r"\_"))


CHINESE_LANG_CODES = {1, 2}


def _build_like_patterns(keyword: str, lang_code: int) -> tuple[str, str]:
    escaped = _escape_like(keyword)
    exact = f"%{escaped}%"

    if lang_code in CHINESE_LANG_CODES:
        fuzzy_source = "".join(keyword.split())
        fuzzy_escaped_chars = [_escape_like(ch) for ch in fuzzy_source]
        fuzzy = "%" + "%".join(fuzzy_escaped_chars) + "%" if fuzzy_escaped_chars else exact
    else:
        fuzzy = exact
    return exact, fuzzy


def _voice_exists_expr(text_hash_field: str) -> str:
    return (
        "exists ("
        "select 1 from dialogue d "
        "join voice v on v.dialogueId = d.dialogueId "
        f"where d.textHash = {text_hash_field} limit 1"
        ") or exists ("
        "select 1 from fetters f "
        "join voice v on v.dialogueId = f.voiceFile "
        "and (v.avatarId = f.avatarId or v.avatarId = 0) "
        f"where f.voiceFileTextTextMapHash = {text_hash_field} limit 1"
        ")"
    )


def selectTextMapFromKeyword(keyWord: str, langCode: int, limit: int | None = None):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        sql1 = (
            "select hash, content from textMap "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\') "
            "order by case when content like ? escape '\\' then 0 else 1 end, length(content)"
        )
        params = [langCode, exact, fuzzy, exact]
        if limit is not None:
            sql1 += " limit ?"
            params.append(limit)
        cursor.execute(sql1, params)
        return cursor.fetchall()


def selectTextMapFromKeywordPaged(
    keyWord: str,
    langCode: int,
    limit: int,
    offset: int,
    hash_value: int | None = None,
    voice_filter: str | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        hash_value = hash_value if hash_value is not None else -1
        voice_expr = _voice_exists_expr("textMap.hash")
        sql = (
            "select hash, content from textMap "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\') "
        )
        if voice_filter == "with":
            sql += f"and ({voice_expr}) "
        elif voice_filter == "without":
            sql += f"and not ({voice_expr}) "
        sql += (
            "order by "
            "case when hash = ? then 0 else 1 end, "
            "case when content like ? escape '\\' then 0 else 1 end, "
            "case when content like ? escape '\\' then 1 "
            f"when {voice_expr} then 0 else 1 end, "
            "length(content) "
            "limit ? offset ?"
        )
        cursor.execute(
            sql,
            (langCode, exact, fuzzy, hash_value, exact, exact, limit, offset),
        )
        return cursor.fetchall()


def countTextMapFromKeyword(keyWord: str, langCode: int) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        sql = (
            "select count(*) from textMap "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\')"
        )
        cursor.execute(sql, (langCode, exact, fuzzy))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countTextMapFromKeywordVoice(keyWord: str, langCode: int, voice_filter: str | None) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyWord, langCode)
        voice_expr = _voice_exists_expr("textMap.hash")
        sql = (
            "select count(*) from textMap "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\') "
        )
        if voice_filter == "with":
            sql += f"and ({voice_expr})"
        elif voice_filter == "without":
            sql += f"and not ({voice_expr})"
        cursor.execute(sql, (langCode, exact, fuzzy))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def hasVoiceForTextHashDb(textHash: int) -> bool:
    with closing(conn.cursor()) as cursor:
        sql = "select 1 where " + _voice_exists_expr(str(textHash)) + " limit 1"
        cursor.execute(sql)
        return cursor.fetchone() is not None


def isTextMapHashInKeyword(textHash: int, keyword: str, langCode: int) -> bool:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select 1 from textMap "
            "where hash=? and lang=? and (content like ? escape '\\' or content like ? escape '\\') "
            "limit 1"
        )
        cursor.execute(sql, (textHash, langCode, exact, fuzzy))
        return cursor.fetchone() is not None


def selectTextMapFromTextHash(textHash: int, langs: list[int] | None = None):
    with closing(conn.cursor()) as cursor:
        if langs is not None and len(langs) > 0:
            langStr = ','.join([str(i) for i in langs])
            sql1 = f"select content, lang from textMap where hash=? and lang in ({langStr})"
        else:
            sql1 = "select content, lang from textMap where hash=?"
        cursor.execute(sql1, (textHash,))
        return cursor.fetchall()


def selectVoicePathFromTextHashInFetter(textHash: int):
    with closing(conn.cursor()) as cursor:
        sql1 = ("select voicePath,voice.avatarId from fetters, voice "
                "where voiceFileTextTextMapHash=? and fetters.voiceFile=voice.dialogueId "
                "and (fetters.avatarId=voice.avatarId or voice.avatarId=0)")
        cursor.execute(sql1, (textHash,))
        matches = cursor.fetchall()
        if len(matches) >= 1:
            return matches[0][0]
        return None


def selectVoicePathFromTextHashInDialogue(textHash: int):
    with closing(conn.cursor()) as cursor:
        sql1 = "select voicePath from dialogue join voice on voice.dialogueId= dialogue.dialogueId where textHash=?"
        cursor.execute(sql1, (textHash,))
        matches = cursor.fetchall()
        if len(matches) > 0:
            return matches[0][0]
        return None


def getImportedTextMapLangs():
    with closing(conn.cursor()) as cursor:
        sql1 = "select id,displayName from langCode where imported=1"
        cursor.execute(sql1)
        return cursor.fetchall()


def getSourceFromFetter(textHash: int, langCode: int = 1):
    with closing(conn.cursor()) as cursor:
        sql1 = ('select avatarId, content from fetters, textMap '
                'where voiceFileTextTextMapHash=? and voiceTitleTextMapHash = hash and lang=?')
        cursor.execute(sql1, (textHash, langCode))
        ans = cursor.fetchall()
        if len(ans) == 0:
            return None
        avatarId, voiceTitle = ans[0]

        sql2 = 'select content from avatar, textMap where avatarId=? and avatar.nameTextMapHash=textMap.hash and lang=?'
        cursor.execute(sql2, (avatarId, langCode))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        avatarName = ans2[0][0]

        return "{} · {}".format(avatarName, voiceTitle)


wanderNames = {}
travellerNames = {}


def getCharterName(avatarId: int, langCode: int = 1):
    with closing(conn.cursor()) as cursor:
        sql2 = 'select content from avatar, textMap where avatarId=? and avatar.nameTextMapHash=textMap.hash and lang=?'
        cursor.execute(sql2, (avatarId, langCode,))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        return ans2[0][0]


def getWanderName(langCode: int = 1):
    if langCode not in wanderNames:
        wanderNames[langCode] = getCharterName(10000075, langCode)
    return wanderNames[langCode]


def getTravellerName(langCode: int = 1):
    if langCode not in travellerNames:
        travellerNames[langCode] = getCharterName(10000005, langCode)
    return travellerNames[langCode]


def getTalkInfo(textHash: int):
    with closing(conn.cursor()) as cursor:
        sql1 = 'select talkerType, talkerId, talkId, coopQuestId from dialogue where textHash=?'
        cursor.execute(sql1, (textHash,))
        ans = cursor.fetchall()
        if len(ans) == 0:
            return None
        talkerType, talkerId, talkId, coopQuestId = ans[0]
        return talkId, talkerType, talkerId, coopQuestId


def getTalkerName(talkerType: str, talkerId: int, langCode: int = 1):
    with closing(conn.cursor()) as cursor:
        talkerName = None
        if talkerType == "TALK_ROLE_NPC":
            sqlGetNpcName = 'select content from npc, textMap indexed by textMap_hash_index where npcId = ? and textHash = hash and lang = ?'
            cursor.execute(sqlGetNpcName, (talkerId, langCode))
            ansNpcName = cursor.fetchall()
            if len(ansNpcName) > 0:
                talkerName = ansNpcName[0][0]
        elif talkerType == "TALK_ROLE_PLAYER":
            talkerName = "主角"
        elif talkerType == "TALK_ROLE_MATE_AVATAR":
            talkerName = "反主"

        if talkerName == '#{REALNAME[ID(1)|HOSTONLY(true)]}':
            talkerName = getWanderName(langCode)
        return talkerName


def getTalkerNameFromTextHash(textHash: int, langCode: int = 1):
    talkInfo = getTalkInfo(textHash)
    if talkInfo is not None:
        _, talkerType, talkerId, _ = talkInfo
        talkerName = getTalkerName(talkerType, talkerId, langCode)
        if talkerName:
            return talkerName

    with closing(conn.cursor()) as cursor:
        sql = "select avatarId from fetters where voiceFileTextTextMapHash=? limit 1"
        cursor.execute(sql, (textHash,))
        ans = cursor.fetchall()
        if len(ans) > 0:
            avatarId = ans[0][0]
            return getCharterName(avatarId, langCode)

    return None


def getTalkQuestId(talkId: int):
    with closing(conn.cursor()) as cursor:
        sql2 = ('select quest.questId from questTalk, quest '
                'where talkId=? and quest.questId=questTalk.questId')
        cursor.execute(sql2, (talkId,))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        return ans2[0][0]


def getQuestName(questId, langCode):
    with closing(conn.cursor()) as cursor:
        sql2 = ('select content from quest, textMap '
                'where quest.questId=? and titleTextMapHash=hash and lang=?')
        cursor.execute(sql2, (questId, langCode))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return "对话文本"

        questTitle = ans2[0][0]

        sql3 = 'select chapterTitleTextMapHash,chapterNumTextMapHash from chapter, quest where questId=? and quest.chapterId=chapter.chapterId'
        cursor.execute(sql3, (questId,))
        ans3 = cursor.fetchall()
        if len(ans3) == 0:
            return questTitle
        chapterTitleTextMapHash, chapterNumTextMapHash = ans3[0]

        sql4 = 'select content from textMap where hash=? and lang=?'
        cursor.execute(sql4, (chapterTitleTextMapHash, langCode))
        ans4 = cursor.fetchall()
        if len(ans4) == 0:
            return questTitle

        chapterTitleText = ans4[0][0]

        cursor.execute(sql4, (chapterNumTextMapHash, langCode))
        ans5 = cursor.fetchall()

        if len(ans5) > 0:
            chapterNumText = ans5[0][0]
            questCompleteName = '{} · {} · {}'.format(chapterNumText, chapterTitleText, questTitle)
        else:
            questCompleteName = '{} · {}'.format(chapterTitleText, questTitle)

        return questCompleteName


def getTalkQuestName(talkId: int, langCode: int = 1) -> str:
    questId = getTalkQuestId(talkId)
    if questId is None:
        return "对话文本"
    return getQuestName(questId, langCode)


def getCoopTalkQuestName(coopQuestId, langCode):
    return getQuestName(coopQuestId // 100, langCode)


def getSourceFromDialogue(textHash: int, langCode: int = 1):
    talkInfo = getTalkInfo(textHash)
    if talkInfo is None:
        return None

    talkId, talkerType, talkerId, coopQuestId = talkInfo
    talkerName = getTalkerName(talkerType, talkerId, langCode)

    if coopQuestId is None:
        questCompleteName = getTalkQuestName(talkId, langCode)
    else:
        questCompleteName = getCoopTalkQuestName(coopQuestId, langCode)

    if talkerName is None:
        return questCompleteName
    else:
        return f"{talkerName}, {questCompleteName}"


def getManualTextMap(placeHolderName, lang):
    with closing(conn.cursor()) as cursor:
        sql1 = 'select content from manualTextMap, textMap where textMapId=? and textHash = hash and lang=?'
        cursor.execute(sql1, (placeHolderName, lang))
        ans = cursor.fetchall()
        if len(ans) > 0:
            return ans[0][0]
        return None


def getTalkContent(talkId: int, coopQuestId: int | None):
    with closing(conn.cursor()) as cursor:
        if coopQuestId is None:
            sql1 = ('select textHash, talkerType, talkerId, dialogueId '
                    'from dialogue where talkId = ? and coopQuestId is null order by dialogueId')
            cursor.execute(sql1, (talkId,))
        else:
            sql1 = ('select textHash, talkerType, talkerId, dialogueId '
                    'from dialogue where talkId = ? and coopQuestId = ? order by dialogueId')
            cursor.execute(sql1, (talkId, coopQuestId))
        ans = cursor.fetchall()
        return ans if len(ans) > 0 else None


def getLangCodeMap():
    with closing(conn.cursor()) as cursor:
        sql = "select id, codeName from langCode"
        cursor.execute(sql)
        rows = cursor.fetchall()
        mapping = {}
        for row in rows:
            match = re.match(r'TextMap(.+)\.json', row[1])
            if match:
                mapping[row[0]] = match.group(1)
        return mapping


def selectReadableFromKeyword(
    keyword: str,
    langCode: int,
    langStr: str,
    limit: int | None = None,
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select fileName, content, titleTextMapHash, readableId from readable "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\') "
            "order by case when content like ? escape '\\' then 0 else 1 end, length(content)"
        )
        params = [langStr, exact, fuzzy, exact]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        if offset is not None and offset > 0:
            sql += " offset ?"
            params.append(offset)
        cursor.execute(sql, params)
        return cursor.fetchall()


def countReadableFromKeyword(keyword: str, langCode: int, langStr: str) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) from readable "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\')"
        )
        cursor.execute(sql, (langStr, exact, fuzzy))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def selectReadableFromFileName(fileName: str, langs: list[str]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        placeholders = ','.join(['?'] * len(langs))
        sql = f"select content, lang from readable where fileName=? and lang in ({placeholders})"
        params = [fileName] + langs
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableFromReadableId(readableId: int, langs: list[str]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        placeholders = ','.join(['?'] * len(langs))
        sql = f"select content, lang from readable where readableId=? and lang in ({placeholders})"
        params = [readableId] + langs
        cursor.execute(sql, params)
        return cursor.fetchall()


def getReadableInfo(readableId: int | None = None, fileName: str | None = None):
    with closing(conn.cursor()) as cursor:
        if readableId is not None:
            sql = "select fileName, titleTextMapHash, readableId from readable where readableId=? limit 1"
            cursor.execute(sql, (readableId,))
        elif fileName is not None:
            sql = "select fileName, titleTextMapHash, readableId from readable where fileName=? limit 1"
            cursor.execute(sql, (fileName,))
        else:
            return None
        ans = cursor.fetchall()
        if len(ans) > 0:
            return ans[0]
        return None


def getTextMapContent(textHash: int, langCode: int):
    with closing(conn.cursor()) as cursor:
        sql = "select content from textMap where hash=? and lang=?"
        cursor.execute(sql, (textHash, langCode))
        ans = cursor.fetchall()
        if len(ans) > 0:
            return ans[0][0]
        return None


def selectQuestByTitleKeyword(keyword: str, langCode: int):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = ("select quest.questId, textMap.content from quest "
               "join textMap on quest.titleTextMapHash=textMap.hash "
               "where textMap.lang=? and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') "
               "order by case when textMap.content like ? escape '\\' then 0 else 1 end, length(textMap.content) "
               "limit 200")
        cursor.execute(sql, (langCode, exact, fuzzy, exact))
        return cursor.fetchall()


def selectQuestByIdContains(keyword: str, langCode: int):
    with closing(conn.cursor()) as cursor:
        escaped = _escape_like(keyword)
        pattern = f"%{escaped}%"
        sql = (
            "select quest.questId, textMap.content "
            "from quest "
            "left join textMap on quest.titleTextMapHash=textMap.hash and textMap.lang=? "
            "where cast(quest.questId as text) like ? escape '\\' "
            "order by length(cast(quest.questId as text)) "
            "limit 200"
        )
        cursor.execute(sql, (langCode, pattern))
        return cursor.fetchall()


def selectAvatarByNameKeyword(keyword: str, langCode: int):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select avatar.avatarId, textMap.content "
            "from avatar join textMap on avatar.nameTextMapHash=textMap.hash "
            "where textMap.lang=? and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') "
            "order by case when textMap.content like ? escape '\\' then 0 else 1 end, length(textMap.content) "
            "limit 200"
        )
        cursor.execute(sql, (langCode, exact, fuzzy, exact))
        return cursor.fetchall()


def selectAvatarVoiceItems(avatarId: int, limit: int = 400):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select fetters.voiceTitleTextMapHash, fetters.voiceFileTextTextMapHash, voice.voicePath "
            "from fetters "
            "left join voice on voice.dialogueId = fetters.voiceFile "
            "and (voice.avatarId = fetters.avatarId or voice.avatarId = 0) "
            "where fetters.avatarId=? "
            "order by fetters.fetterId "
            "limit ?"
        )
        cursor.execute(sql, (avatarId, limit))
        return cursor.fetchall()


def selectAvatarStories(avatarId: int, limit: int = 800):
    with closing(conn.cursor()) as cursor:
        try:
            sql = (
                "select fetterId, storyTitleTextMapHash, storyTitle2TextMapHash, "
                "storyTitleLockedTextMapHash, storyContextTextMapHash, storyContext2TextMapHash "
                "from fetterStory "
                "where avatarId=? "
                "order by fetterId "
                "limit ?"
            )
            cursor.execute(sql, (avatarId, limit))
            return cursor.fetchall()
        except sqlite3.OperationalError:
            return []


def selectQuestByChapterKeyword(keyword: str, langCode: int):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select quest.questId, questTitle.content, chapterTitle.content, chapterNum.content "
            "from quest "
            "join textMap as questTitle on quest.titleTextMapHash=questTitle.hash "
            "join chapter on quest.chapterId=chapter.chapterId "
            "left join textMap as chapterTitle on chapter.chapterTitleTextMapHash=chapterTitle.hash "
            "and chapterTitle.lang=? "
            "left join textMap as chapterNum on chapter.chapterNumTextMapHash=chapterNum.hash "
            "and chapterNum.lang=? "
            "where questTitle.lang=? and ("
            "chapterTitle.content like ? escape '\\' or chapterNum.content like ? escape '\\' "
            "or chapterTitle.content like ? escape '\\' or chapterNum.content like ? escape '\\'"
            ") "
            "order by case when (chapterTitle.content like ? escape '\\' or chapterNum.content like ? escape '\\') then 0 else 1 end, "
            "length(coalesce(chapterTitle.content, chapterNum.content)) "
            "limit 200"
        )
        cursor.execute(
            sql,
            (
                langCode,
                langCode,
                langCode,
                exact,
                exact,
                fuzzy,
                fuzzy,
                exact,
                exact,
            ),
        )
        return cursor.fetchall()
    
    
def getQuestChapterName(questId: int, langCode: int):
    with closing(conn.cursor()) as cursor:
        sql = "select chapterId from quest where questId=?"
        cursor.execute(sql, (questId,))
        ans = cursor.fetchall()
        if len(ans) == 0 or ans[0][0] is None:
            return None
        chapterId = ans[0][0]

        sql2 = ("select chapterTitleTextMapHash, chapterNumTextMapHash "
                "from chapter where chapterId=?")
        cursor.execute(sql2, (chapterId,))
        chapter_row = cursor.fetchall()
        if len(chapter_row) == 0:
            return None
        chapterTitleTextMapHash, chapterNumTextMapHash = chapter_row[0]

        sql3 = 'select content from textMap where hash=? and lang=?'
        cursor.execute(sql3, (chapterTitleTextMapHash, langCode))
        ans2 = cursor.fetchall()
        if len(ans2) == 0:
            return None
        chapterTitleText = ans2[0][0]

        cursor.execute(sql3, (chapterNumTextMapHash, langCode))
        ans3 = cursor.fetchall()
        if len(ans3) > 0:
            chapterNumText = ans3[0][0]
            return '{} · {}'.format(chapterNumText, chapterTitleText)
        return chapterTitleText


def selectQuestTalkIds(questId: int):
    with closing(conn.cursor()) as cursor:
        sql = "select talkId from questTalk where questId=? order by talkId"
        cursor.execute(sql, (questId,))
        return [row[0] for row in cursor.fetchall()]


def countQuestDialogues(questId: int) -> int:
    with closing(conn.cursor()) as cursor:
        sql = (
            "select count(*) from dialogue d "
            "join (select distinct talkId from questTalk where questId=?) qt "
            "on qt.talkId = d.talkId "
            "where d.coopQuestId is null"
        )
        cursor.execute(sql, (questId,))
        row = cursor.fetchone()
        return row[0] if row else 0


def selectQuestDialoguesPaged(
    questId: int,
    limit: int | None = None,
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select d.textHash, d.talkerType, d.talkerId, d.dialogueId, d.talkId "
            "from dialogue d "
            "join (select distinct talkId from questTalk where questId=?) qt "
            "on qt.talkId = d.talkId "
            "where d.coopQuestId is null "
            "order by d.talkId, d.dialogueId"
        )
        params = [questId]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        if offset is not None and offset > 0:
            if limit is None:
                sql += " limit -1"
            sql += " offset ?"
            params.append(offset)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectReadableByTitleKeyword(keyword: str, langCode: int, langStr: str):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = ("select readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content "
               "from readable join textMap on readable.titleTextMapHash=textMap.hash "
               "where readable.lang=? and textMap.lang=? "
               "and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') "
               "group by readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content "
               "order by case when textMap.content like ? escape '\\' then 0 else 1 end, length(textMap.content) "
               "limit 200")
        cursor.execute(sql, (langStr, langCode, exact, fuzzy, exact))
        return cursor.fetchall()


def selectReadableByFileNameContains(keyword: str, langCode: int, langStr: str):
    with closing(conn.cursor()) as cursor:
        escaped = _escape_like(keyword)
        pattern = f"%{escaped}%"
        sql = (
            "select readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content "
            "from readable "
            "left join textMap on readable.titleTextMapHash=textMap.hash and textMap.lang=? "
            "where readable.lang=? and readable.fileName like ? escape '\\' "
            "group by readable.fileName, readable.readableId, readable.titleTextMapHash, textMap.content "
            "order by length(readable.fileName) "
            "limit 200"
        )
        cursor.execute(sql, (langCode, langStr, pattern))
        return cursor.fetchall()


def selectSubtitleFromKeyword(
    keyword: str,
    langCode: int,
    limit: int | None = None,
    offset: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select fileName, content, startTime, endTime, subtitleId from subtitle "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\') "
            "order by case when content like ? escape '\\' then 0 else 1 end, length(content)"
        )
        params = [langCode, exact, fuzzy, exact]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        if offset is not None and offset > 0:
            sql += " offset ?"
            params.append(offset)
        cursor.execute(sql, params)
        return cursor.fetchall()


def countSubtitleFromKeyword(keyword: str, langCode: int) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) from subtitle "
            "where lang=? and (content like ? escape '\\' or content like ? escape '\\')"
        )
        cursor.execute(sql, (langCode, exact, fuzzy))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def selectDialogueByTalkerKeyword(keyword: str, langCode: int, limit: int | None = None):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap on npc.textHash = textMap.hash "
            "where textMap.lang=? and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\') "
            "order by case when textMap.content like ? escape '\\' then 0 else 1 end, length(textMap.content), dialogue.dialogueId"
        )
        params = [langCode, exact, fuzzy, exact]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def countDialogueByTalkerKeyword(keyword: str, langCode: int) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from dialogue "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap on npc.textHash = textMap.hash "
            "where textMap.lang=? and (textMap.content like ? escape '\\' or textMap.content like ? escape '\\')"
        )
        cursor.execute(sql, (langCode, exact, fuzzy))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def selectDialogueByTalkerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap as npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            "where (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
            "and (npcName.content like ? escape '\\' or npcName.content like ? escape '\\') "
            "order by case when dialogueText.content like ? escape '\\' then 0 else 1 end, "
            "length(dialogueText.content), dialogue.dialogueId"
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
            keyword_exact,
        ]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectDialogueByTalkerTypeAndKeyword(
    talkerType: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select dialogue.textHash, dialogue.talkerType, dialogue.talkerId, dialogue.dialogueId "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "where dialogue.talkerType = ? "
            "and (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
            "order by case when dialogueText.content like ? escape '\\' then 0 else 1 end, "
            "length(dialogueText.content), dialogue.dialogueId"
        )
        params = [langCode, talkerType, exact, fuzzy, exact]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectFetterBySpeakerAndKeyword(
    speaker_keyword: str,
    keyword: str,
    langCode: int,
    limit: int | None = None,
):
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select fetters.voiceFileTextTextMapHash, fetters.avatarId "
            "from fetters "
            "join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "join avatar on fetters.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (voiceText.content like ? escape '\\' or voiceText.content like ? escape '\\') "
            "and (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\') "
            "order by case when voiceText.content like ? escape '\\' then 0 else 1 end, "
            "length(voiceText.content), fetters.fetterId"
        )
        params = [
            langCode,
            langCode,
            keyword_exact,
            keyword_fuzzy,
            speaker_exact,
            speaker_fuzzy,
            keyword_exact,
        ]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectDialogueByTalkerType(talkerType: str, limit: int | None = None):
    with closing(conn.cursor()) as cursor:
        sql = (
            "select textHash, talkerType, talkerId, dialogueId "
            "from dialogue where talkerType=? "
            "order by dialogueId"
        )
        params = [talkerType]
        if limit is not None:
            sql += " limit ?"
            params.append(limit)
        cursor.execute(sql, params)
        return cursor.fetchall()


def countDialogueByTalkerAndKeyword(speaker_keyword: str, keyword: str, langCode: int) -> int:
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "join npc on dialogue.talkerType = 'TALK_ROLE_NPC' and dialogue.talkerId = npc.npcId "
            "join textMap as npcName on npc.textHash = npcName.hash and npcName.lang = ? "
            "where (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\') "
            "and (npcName.content like ? escape '\\' or npcName.content like ? escape '\\')"
        )
        cursor.execute(
            sql,
            (
                langCode,
                langCode,
                keyword_exact,
                keyword_fuzzy,
                speaker_exact,
                speaker_fuzzy,
            ),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countDialogueByTalkerTypeAndKeyword(talkerType: str, keyword: str, langCode: int) -> int:
    with closing(conn.cursor()) as cursor:
        exact, fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from dialogue "
            "join textMap as dialogueText on dialogue.textHash = dialogueText.hash "
            "and dialogueText.lang = ? "
            "where dialogue.talkerType = ? "
            "and (dialogueText.content like ? escape '\\' or dialogueText.content like ? escape '\\')"
        )
        cursor.execute(sql, (langCode, talkerType, exact, fuzzy))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countFetterBySpeakerAndKeyword(speaker_keyword: str, keyword: str, langCode: int) -> int:
    with closing(conn.cursor()) as cursor:
        speaker_exact, speaker_fuzzy = _build_like_patterns(speaker_keyword, langCode)
        keyword_exact, keyword_fuzzy = _build_like_patterns(keyword, langCode)
        sql = (
            "select count(*) "
            "from fetters "
            "join textMap as voiceText on fetters.voiceFileTextTextMapHash = voiceText.hash "
            "and voiceText.lang = ? "
            "join avatar on fetters.avatarId = avatar.avatarId "
            "join textMap as avatarName on avatar.nameTextMapHash = avatarName.hash "
            "and avatarName.lang = ? "
            "where (voiceText.content like ? escape '\\' or voiceText.content like ? escape '\\') "
            "and (avatarName.content like ? escape '\\' or avatarName.content like ? escape '\\')"
        )
        cursor.execute(
            sql,
            (
                langCode,
                langCode,
                keyword_exact,
                keyword_fuzzy,
                speaker_exact,
                speaker_fuzzy,
            ),
        )
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def countDialogueByTalkerType(talkerType: str) -> int:
    with closing(conn.cursor()) as cursor:
        sql = "select count(*) from dialogue where talkerType=?"
        cursor.execute(sql, (talkerType,))
        row = cursor.fetchone()
        return int(row[0]) if row else 0


def selectSubtitleTranslations(fileName: str, startTime: float, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        placeholders = ','.join(['?'] * len(langs))
        sql = f"""
            select content, lang 
            from subtitle 
            where fileName=? 
            and lang in ({placeholders})
            and abs(startTime - ?) < 0.5
        """
        params = [fileName] + langs + [startTime]
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectSubtitleTranslationsBySubtitleId(subtitleId: int, startTime: float, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        placeholders = ','.join(['?'] * len(langs))
        sql = f"""
            select content, lang 
            from subtitle 
            where subtitleId=? 
            and lang in ({placeholders})
            and abs(startTime - ?) < 0.5
        """
        params = [subtitleId] + langs + [startTime]
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectSubtitleContext(fileName: str, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        placeholders = ','.join(['?'] * len(langs))
        sql = f"select content, lang, startTime, endTime from subtitle where fileName=? and lang in ({placeholders}) order by startTime"
        params = [fileName] + langs
        cursor.execute(sql, params)
        return cursor.fetchall()


def selectSubtitleContextBySubtitleId(subtitleId: int, langs: list[int]):
    with closing(conn.cursor()) as cursor:
        if not langs:
            return []
        placeholders = ','.join(['?'] * len(langs))
        sql = f"select content, lang, startTime, endTime from subtitle where subtitleId=? and lang in ({placeholders}) order by startTime"
        params = [subtitleId] + langs
        cursor.execute(sql, params)
        return cursor.fetchall()

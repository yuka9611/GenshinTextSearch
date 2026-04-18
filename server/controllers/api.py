import os
import sqlite3
from flask import Blueprint, current_app, request, jsonify

import controllers as controllers_module
from databaseHelper import selectTextMapFromKeywordPaged, selectVoiceFromKeywordPaged, getVoicePath, getTextMapByHash, getVersionData, getLangCodeMap
from utils.browser_session import (
    disconnect_browser_client,
    is_browser_auto_shutdown_enabled,
    touch_browser_client,
)
from utils.helpers import getLangFromRequest, normalizeSearchTerm, getLanguageName
from utils.cache import search_cache

# 构建语言代码到语言ID的映射
lang_code_map = None
def get_lang_id(lang_code: str) -> int:
    """
    将语言代码字符串转换为语言ID整数
    """
    global lang_code_map
    if lang_code_map is None:
        # 初始化语言代码映射
        code_map = getLangCodeMap()
        # 构建反向映射
        lang_code_map = {v.lower(): k for k, v in code_map.items()}

    # 默认语言为中文
    return lang_code_map.get(lang_code.lower(), 1)

api_bp = Blueprint('api', __name__)


def _get_browser_client_id() -> str:
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        client_id = payload.get("clientId", "")
        if isinstance(client_id, str):
            client_id = client_id.strip()
            if client_id:
                return client_id

    client_id = request.form.get("clientId", "").strip()
    if client_id:
        return client_id

    return request.args.get("clientId", "", type=str).strip()


def _is_database_corruption_error(error: sqlite3.DatabaseError) -> bool:
    message = str(error).lower()
    return (
        "database disk image is malformed" in message
        or "malformed" in message
        or "file is not a database" in message
    )


@api_bp.errorhandler(sqlite3.DatabaseError)
def handle_sqlite_database_error(error: sqlite3.DatabaseError):
    current_app.logger.exception("SQLite database error during API request")
    if _is_database_corruption_error(error):
        message = (
            "server/data.db is corrupted. Rebuild or replace the database file, "
            "then restart the server."
        )
    else:
        message = "Database query failed. Check server/data.db and retry."
    return jsonify({
        "data": None,
        "code": 500,
        "msg": message,
    })

@api_bp.route('/api/search', methods=['GET'])
def search_text():
    """
    搜索文本
    """
    keyword = request.args.get('keyword', '', type=str)
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 10, type=int)
    lang = getLangFromRequest()

    # 生成缓存键
    cache_key = f"search:{keyword}:{page}:{size}:{lang}"

    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)

    # 标准化搜索词
    normalized_keyword = normalizeSearchTerm(keyword)

    # 转换语言代码为整数
    lang_id = get_lang_id(lang)

    # 计算偏移量
    offset = (page - 1) * size
    # 执行搜索
    rows = selectTextMapFromKeywordPaged(normalized_keyword, lang_id, size, offset)

    # 将元组列表转换为字典列表
    results = []
    for text_hash, content, created_version, updated_version in rows:
        results.append({
            'hash': text_hash,
            'content': content,
            'createdVersion': created_version,
            'updatedVersion': updated_version
        })

    # 构建响应
    response = {
        'keyword': keyword,
        'lang': lang,
        'page': page,
        'size': size,
        'total': len(results),
        'results': results
    }

    # 缓存结果
    search_cache.set(cache_key, response)

    return jsonify(response)

@api_bp.route('/api/voice', methods=['GET'])
def search_voice():
    """
    搜索语音
    """
    keyword = request.args.get('keyword', '', type=str)
    page = request.args.get('page', 1, type=int)
    size = request.args.get('size', 10, type=int)
    lang = getLangFromRequest()

    # 生成缓存键
    cache_key = f"voice:{keyword}:{page}:{size}:{lang}"

    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)

    # 标准化搜索词
    normalized_keyword = normalizeSearchTerm(keyword)

    # 转换语言代码为整数
    lang_id = get_lang_id(lang)

    # 执行搜索
    results = selectVoiceFromKeywordPaged(normalized_keyword, page, size, lang_id)

    # 构建响应
    response = {
        'keyword': keyword,
        'lang': lang,
        'page': page,
        'size': size,
        'total': len(results),
        'results': results
    }

    # 缓存结果
    search_cache.set(cache_key, response)

    return jsonify(response)

@api_bp.route('/api/voice/path', methods=['GET'])
def get_voice_path_api():
    """
    获取语音路径
    """
    voice_hash = request.args.get('hash', '', type=str)
    lang = getLangFromRequest()

    # 生成缓存键
    cache_key = f"voice_path:{voice_hash}:{lang}"

    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)

    # 转换语言代码为整数
    lang_id = get_lang_id(lang)

    # 获取语音路径
    path = getVoicePath(voice_hash, lang_id)

    # 构建响应
    response = {
        'hash': voice_hash,
        'lang': lang,
        'path': path
    }

    # 缓存结果
    search_cache.set(cache_key, response)

    return jsonify(response)

@api_bp.route('/api/textmap', methods=['GET'])
def get_text_map_api():
    """
    获取文本映射
    """
    hash_val = request.args.get('hash', '', type=str)
    lang = getLangFromRequest()

    # 生成缓存键
    cache_key = f"textmap:{hash_val}:{lang}"

    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)

    # 转换语言代码为整数
    lang_id = get_lang_id(lang)

    # 获取文本映射
    text = getTextMapByHash(hash_val, lang_id)

    # 构建响应
    response = {
        'hash': hash_val,
        'lang': lang,
        'text': text
    }

    # 缓存结果
    search_cache.set(cache_key, response)

    return jsonify(response)

@api_bp.route('/api/version', methods=['GET'])
def get_version_api():
    """
    获取版本数据
    """
    # 获取当前语言
    lang = getLangFromRequest()

    # 生成缓存键
    cache_key = f"version_data:{lang}"

    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)

    # 转换语言代码为整数
    lang_id = get_lang_id(lang)

    # 获取版本数据
    version_data = getVersionData(lang_id, include_current=True)

    # 缓存结果
    search_cache.set(cache_key, version_data)

    return jsonify(version_data)

@api_bp.route('/api/languages', methods=['GET'])
def get_languages_api():
    """
    获取语言列表
    """
    # 生成缓存键
    cache_key = "languages"

    # 尝试从缓存获取结果
    cached_result = search_cache.get(cache_key)
    if cached_result:
        return jsonify(cached_result)

    # 构建语言列表
    languages = [
        {"code": "zh-cn", "name": getLanguageName("zh-cn")},
        {"code": "en-us", "name": getLanguageName("en-us")},
        {"code": "ja-jp", "name": getLanguageName("ja-jp")},
        {"code": "ko-kr", "name": getLanguageName("ko-kr")}
    ]

    # 缓存结果
    search_cache.set(cache_key, languages)

    return jsonify(languages)


# ----------------------------
# Startup / Settings APIs
# ----------------------------
@api_bp.route("/api/startupStatus")
def startupStatus():
    import config
    return jsonify({
        "data": {
            "assetDirValid": config.isAssetDirValid(),
            "assetDir": config.getAssetDir(),
            "browserAutoShutdownEnabled": is_browser_auto_shutdown_enabled(),
        },
        "code": 200,
        "msg": "ok"
    })


@api_bp.route("/api/browser-session/heartbeat", methods=["POST"])
def browserSessionHeartbeat():
    client_id = _get_browser_client_id()
    if not client_id:
        return jsonify({"data": None, "code": 400, "msg": "Missing clientId"})

    active_clients = touch_browser_client(client_id)
    return jsonify({
        "data": {
            "activeClients": active_clients,
            "autoShutdownEnabled": is_browser_auto_shutdown_enabled(),
        },
        "code": 200,
        "msg": "ok",
    })


@api_bp.route("/api/browser-session/disconnect", methods=["POST"])
def browserSessionDisconnect():
    client_id = _get_browser_client_id()
    if not client_id:
        return jsonify({"data": None, "code": 400, "msg": "Missing clientId"})

    active_clients = disconnect_browser_client(client_id)
    return jsonify({
        "data": {
            "activeClients": active_clients,
            "autoShutdownEnabled": is_browser_auto_shutdown_enabled(),
        },
        "code": 200,
        "msg": "ok",
    })

@api_bp.route("/api/setAssetDir", methods=["POST"])
def setAssetDir():
    import config
    import languagePackReader

    assetDir = request.json.get("assetDir")
    if not assetDir or not os.path.isdir(assetDir):
        return jsonify({"data": None, "code": 400, "msg": "Invalid directory"})

    config.setAssetDir(assetDir)
    config.saveConfig()

    # 重新加载语音包
    languagePackReader.reloadLangPackages()

    return jsonify({
        "data": {
            "assetDir": config.getAssetDir(),
            "assetDirValid": config.isAssetDirValid()
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/pickAssetDir", methods=["POST"])
def pickAssetDir():
    import config
    import languagePackReader

    dialog_error = getattr(controllers_module, "AssetDirDialogUnavailableError", RuntimeError)
    try:
        picked = controllers_module.pickAssetDirViaDialog() # type: ignore
    except dialog_error as exc:
        current_app.logger.warning("Asset directory dialog is unavailable: %s", exc)
        return jsonify({
            "data": {
                "cancel": False,
                "dialogUnavailable": True,
                "assetDir": config.getAssetDir(),
                "assetDirValid": config.isAssetDirValid()
            },
            "code": 501,
            "msg": "Asset directory dialog is unavailable"
        })

    if not picked:
        return jsonify({
            "data": {
                "cancel": True,
                "dialogUnavailable": False,
                "assetDir": config.getAssetDir(),
                "assetDirValid": config.isAssetDirValid()
            },
            "code": 200,
            "msg": "ok"
        })

    if not os.path.isdir(picked):
        return jsonify({"data": None, "code": 400, "msg": "Invalid directory"})

    config.setAssetDir(picked)
    config.saveConfig()
    languagePackReader.reloadLangPackages()

    return jsonify({
        "data": {
            "cancel": False,
            "dialogUnavailable": False,
            "assetDir": config.getAssetDir(),
            "assetDirValid": config.isAssetDirValid()
        },
        "code": 200,
        "msg": "ok"
    })

# ----------------------------
# Existing APIs
# ----------------------------
@api_bp.route("/api/getImportedTextLanguages")
def getImportedTextLanguages():
    return jsonify({
        "data": controllers_module.getImportedTextMapLangs(), # type: ignore
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getImportedVoiceLanguages")
def getImportedVoiceLanguages():
    return jsonify({
        "data": controllers_module.getLoadedVoicePacks(), # type: ignore
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getAvailableVersions")
def getAvailableVersions():
    return jsonify({
        "data": controllers_module.getAvailableVersions(), # type: ignore
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/keywordQuery", methods=["POST"])
def keywordQuery():
    import time

    langCode = int(request.json["langCode"])
    keyword: str = request.json["keyword"]
    speaker = request.json.get("speaker")
    createdVersion = request.json.get("createdVersion")
    updatedVersion = request.json.get("updatedVersion")
    sourceType = request.json.get("sourceType")
    page = request.json.get("page", 1)
    pageSize = request.json.get("pageSize", 50)
    voiceFilter = request.json.get("voiceFilter", "all")

    page = max(1, int(page) if page else 1)
    pageSize = max(1, int(pageSize) if pageSize else 50)

    if voiceFilter not in ("all", "with", "without"):
        voiceFilter = "all"

    has_keyword = keyword.strip() != ""
    has_speaker = speaker and speaker.strip() != ""
    has_created = createdVersion and str(createdVersion).strip() != ""
    has_updated = updatedVersion and str(updatedVersion).strip() != ""
    has_source_type = sourceType and str(sourceType).strip() != ""
    has_voice_filter = voiceFilter in ("with", "without")
    if not has_keyword and not has_speaker and not has_created and not has_updated and not has_source_type and not has_voice_filter:
        return jsonify({
            "data": {
                "contents": [],
                "total": 0,
                "page": page,
                "pageSize": pageSize,
                "time": 0
            },
            "code": 200,
            "msg": "ok"
        })

    start = time.time()
    contents, total = controllers_module.getTranslateObj( # type: ignore
        keyword,
        langCode,
        speaker,
        page=page,
        page_size=pageSize,
        voice_filter=voiceFilter,
        created_version=createdVersion,
        updated_version=updatedVersion,
        source_type=sourceType,
    )
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "total": total,
            "page": page,
            "pageSize": pageSize,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getVoiceOver", methods=["POST"])
def getVoiceOver():
    from flask import send_file, make_response

    try:
        langCode = int(request.json["langCode"])
    except Exception:
        return jsonify({"data": None, "code": 400, "msg": "Invalid langCode"})

    voicePath = request.json["voicePath"]
    wemStream = controllers_module.getVoiceBinStream(voicePath, langCode) # type: ignore

    if wemStream is None:
        resp = make_response("Audio File Not Found")
        resp.headers["Access-Control-Expose-Headers"] = "Error"
        resp.headers["Error"] = "True"
        return resp

    return send_file(
        wemStream,
        download_name=os.path.basename(voicePath),
        mimetype="application/octet-stream"
    )

@api_bp.route("/api/getTalkFromHash", methods=["POST"])
def getTalkFromHash():
    import time

    textHash: int = request.json["textHash"]
    searchLang = request.json.get("searchLang")
    page = request.json.get("page")
    pageSize = request.json.get("pageSize", 200)
    if searchLang:
        searchLang = int(searchLang)
    if page is not None and str(page).strip() != "":
        try:
            page = int(page)
        except Exception:
            page = None
    else:
        page = None
    try:
        pageSize = int(pageSize)
    except Exception:
        pageSize = 200
    if pageSize < 1:
        pageSize = 200

    try:
        start = time.time()
        contents = controllers_module.getTalkFromHash( # type: ignore
            textHash,
            searchLang,
            page=page,
            page_size=pageSize,
        )
        end = time.time()
    except Exception as e:
        return jsonify({"data": None, "code": 114, "msg": str(e)})

    return jsonify({
        "data": {
            "contents": contents,
            "total": contents.get("total", len(contents.get("dialogues", []))),
            "page": contents.get("page", 1),
            "pageSize": contents.get("pageSize", pageSize),
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getDialogueGroup", methods=["POST"])
def getDialogueGroup():
    import time

    talkId = request.json.get("talkId", 0)
    coopQuestId = request.json.get("coopQuestId")
    dialogueIdFallback = request.json.get("dialogueIdFallback")
    searchLang = request.json.get("searchLang")
    page = request.json.get("page", 1)
    pageSize = request.json.get("pageSize", 200)

    try:
        talkId = int(talkId or 0)
    except Exception:
        talkId = 0
    if coopQuestId is not None and str(coopQuestId).strip() != "":
        try:
            coopQuestId = int(coopQuestId)
        except Exception:
            coopQuestId = None
    else:
        coopQuestId = None
    if dialogueIdFallback is not None and str(dialogueIdFallback).strip() != "":
        try:
            dialogueIdFallback = int(dialogueIdFallback)
        except Exception:
            dialogueIdFallback = None
    else:
        dialogueIdFallback = None
    if searchLang:
        searchLang = int(searchLang)
    try:
        page = int(page)
    except Exception:
        page = 1
    try:
        pageSize = int(pageSize)
    except Exception:
        pageSize = 200
    if pageSize < 1:
        pageSize = 200

    try:
        start = time.time()
        contents = controllers_module.getDialogueGroup(  # type: ignore
            talkId,
            coopQuestId=coopQuestId,
            dialogueIdFallback=dialogueIdFallback,
            searchLang=searchLang,
            page=page,
            page_size=pageSize,
        )
        end = time.time()
    except Exception as e:
        return jsonify({"data": None, "code": 114, "msg": str(e)})

    return jsonify({
        "data": {
            "contents": contents,
            "total": contents.get("total", len(contents.get("dialogues", []))),
            "page": contents.get("page", 1),
            "pageSize": contents.get("pageSize", pageSize),
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getSubtitleContext", methods=["POST"])
def getSubtitleContext():
    import time

    fileName = request.json.get("fileName")
    subtitleId = request.json.get("subtitleId")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)
    if subtitleId is not None and str(subtitleId).strip() != "":
        subtitleId = int(subtitleId)
    else:
        subtitleId = None

    start = time.time()
    contents = controllers_module.getSubtitleContext(fileName, subtitleId, searchLang) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/nameSearch", methods=["POST"])
def nameSearch():
    import time

    langCode = int(request.json["langCode"])
    keyword: str = request.json.get("keyword", "")
    createdVersion = request.json.get("createdVersion")
    updatedVersion = request.json.get("updatedVersion")
    questSourceType = request.json.get("questSourceType")
    speakerKeyword = request.json.get("speakerKeyword", "")
    readableCategory = request.json.get("readableCategory")

    has_keyword = keyword.strip() != ""
    has_created = createdVersion and str(createdVersion).strip() != ""
    has_updated = updatedVersion and str(updatedVersion).strip() != ""
    has_source_type = questSourceType and str(questSourceType).strip() != ""
    has_speaker = speakerKeyword and str(speakerKeyword).strip() != ""
    has_readable_category = readableCategory and str(readableCategory).strip() != ""
    if not has_keyword and not has_created and not has_updated and not has_source_type and not has_speaker and not has_readable_category:
        return jsonify({
            "data": {
                "contents": {
                    "quests": [],
                    "readables": []
                },
                "time": 0
            },
            "code": 200,
            "msg": "ok"
        })

    start = time.time()
    contents = controllers_module.searchNameEntries( # type: ignore
        keyword,
        langCode,
        created_version=createdVersion,
        updated_version=updatedVersion,
        quest_source_type=questSourceType,
        speaker_keyword=speakerKeyword,
        readable_category=readableCategory,
    )
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/npcDialogueSearch", methods=["POST"])
def npcDialogueSearch():
    import time

    langCode = int(request.json["langCode"])
    keyword: str = request.json.get("keyword", "")
    npcCreatedVersion = request.json.get("npcCreatedVersion")
    npcUpdatedVersion = request.json.get("npcUpdatedVersion")

    has_keyword = keyword.strip() != ""
    has_created = npcCreatedVersion and str(npcCreatedVersion).strip() != ""
    has_updated = npcUpdatedVersion and str(npcUpdatedVersion).strip() != ""
    if not has_keyword and not has_created and not has_updated:
        return jsonify({
            "data": {
                "contents": {
                    "npcs": []
                },
                "time": 0
            },
            "code": 200,
            "msg": "ok"
        })

    start = time.time()
    contents = controllers_module.searchNpcDialogueEntries(  # type: ignore
        keyword,
        langCode,
        npc_created_version=npcCreatedVersion,
        npc_updated_version=npcUpdatedVersion,
    )
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/npcDialogues", methods=["POST"])
def npcDialogues():
    import time

    npc_ids_input = request.json.get("npcIds")
    npcIds: list[int] = []
    if isinstance(npc_ids_input, list):
        for item in npc_ids_input:
            try:
                npc_value = int(item)
            except Exception:
                continue
            if npc_value > 0 and npc_value not in npcIds:
                npcIds.append(npc_value)

    npcId_raw = request.json.get("npcId")
    try:
        npcId = int(npcId_raw) if npcId_raw is not None else None
    except Exception:
        npcId = None
    searchLang = request.json.get("searchLang")
    dialogueCreatedVersion = request.json.get("dialogueCreatedVersion")
    dialogueUpdatedVersion = request.json.get("dialogueUpdatedVersion")
    page = request.json.get("page", 1)
    pageSize = request.json.get("pageSize", 20)
    if searchLang:
        searchLang = int(searchLang)
    try:
        page = int(page)
    except Exception:
        page = 1
    try:
        pageSize = int(pageSize)
    except Exception:
        pageSize = 20

    start = time.time()
    contents = controllers_module.getNpcDialogues(  # type: ignore
        npcId,
        npcIds=npcIds,
        searchLang=searchLang,
        dialogue_created_version=dialogueCreatedVersion,
        dialogue_updated_version=dialogueUpdatedVersion,
        page=page,
        page_size=pageSize,
    )
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/avatarSearch", methods=["POST"])
def avatarSearch():
    import time

    langCode = int(request.json["langCode"])
    keyword: str = request.json["keyword"]

    if keyword.strip() == "":
        return jsonify({
            "data": {
                "avatars": []
            },
            "code": 200,
            "msg": "ok"
        })

    start = time.time()
    contents = controllers_module.searchAvatarEntries(keyword, langCode) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/avatarVoice", methods=["POST"])
def avatarVoice():
    import time

    avatarId = request.json.get("avatarId")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)

    if avatarId is None:
        return jsonify({"data": None, "code": 400, "msg": "avatarId is required"})

    avatarId = int(avatarId)

    start = time.time()
    contents = controllers_module.getAvatarVoices(avatarId, searchLang) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/avatarVoiceSearch", methods=["POST"])
def avatarVoiceSearch():
    import time

    titleKeyword = request.json.get("titleKeyword", "")
    createdVersion = request.json.get("createdVersion")
    updatedVersion = request.json.get("updatedVersion")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)

    has_title = str(titleKeyword).strip() != ""
    has_created = createdVersion and str(createdVersion).strip() != ""
    has_updated = updatedVersion and str(updatedVersion).strip() != ""
    if not has_title and not has_created and not has_updated:
        return jsonify({
            "data": {
                "contents": {
                    "voices": []
                },
                "time": 0
            },
            "code": 200,
            "msg": "ok"
        })

    start = time.time()
    contents = controllers_module.searchAvatarVoicesByFilters( # type: ignore
        title_keyword=titleKeyword,
        searchLang=searchLang,
        created_version=createdVersion,
        updated_version=updatedVersion,
    )
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/avatarStory", methods=["POST"])
def avatarStory():
    import time

    avatarId = request.json.get("avatarId")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)

    if avatarId is None:
        return jsonify({"data": None, "code": 400, "msg": "avatarId is required"})

    avatarId = int(avatarId)

    start = time.time()
    contents = controllers_module.getAvatarStories(avatarId, searchLang) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/avatarStorySearch", methods=["POST"])
def avatarStorySearch():
    import time

    titleKeyword = request.json.get("titleKeyword", "")
    createdVersion = request.json.get("createdVersion")
    updatedVersion = request.json.get("updatedVersion")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)

    has_title = str(titleKeyword).strip() != ""
    has_created = createdVersion and str(createdVersion).strip() != ""
    has_updated = updatedVersion and str(updatedVersion).strip() != ""
    if not has_title and not has_created and not has_updated:
        return jsonify({
            "data": {
                "contents": {
                    "stories": []
                },
                "time": 0
            },
            "code": 200,
            "msg": "ok"
        })

    start = time.time()
    contents = controllers_module.searchAvatarStoriesByFilters( # type: ignore
        title_keyword=titleKeyword,
        searchLang=searchLang,
        created_version=createdVersion,
        updated_version=updatedVersion,
    )
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getReadableContent", methods=["POST"])
def getReadableContent():
    import time

    readableId = request.json.get("readableId")
    fileName = request.json.get("fileName")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)
    if readableId is not None:
        readableId = int(readableId)

    start = time.time()
    contents = controllers_module.getReadableContent(readableId, fileName, searchLang) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getQuestDialogues", methods=["POST"])
def getQuestDialogues():
    import time

    questId = request.json.get("questId")
    searchLang = request.json.get("searchLang")
    page = request.json.get("page", 1)
    pageSize = request.json.get("pageSize", 200)
    if searchLang:
        searchLang = int(searchLang)

    if questId is None:
        return jsonify({"data": None, "code": 400, "msg": "questId is required"})

    questId = int(questId)
    try:
        page = int(page)
    except Exception:
        page = 1
    try:
        pageSize = int(pageSize)
    except Exception:
        pageSize = 200
    if page < 1:
        page = 1
    if pageSize < 1:
        pageSize = 200

    start = time.time()
    contents, total = controllers_module.getQuestDialogues(questId, searchLang, page, pageSize) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "total": total,
            "page": page,
            "pageSize": pageSize,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })


@api_bp.route("/api/getEntityTexts", methods=["POST"])
def getEntityTexts():
    import time

    sourceTypeCode = request.json.get("sourceTypeCode")
    entityId = request.json.get("entityId")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)

    if sourceTypeCode is None or entityId is None:
        return jsonify({"data": None, "code": 400, "msg": "sourceTypeCode and entityId are required"})

    start = time.time()
    contents = controllers_module.getEntityTexts(int(sourceTypeCode), int(entityId), searchLang) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })


@api_bp.route("/api/getTextEntitySources", methods=["POST"])
def getTextEntitySources():
    import time

    textHash = request.json.get("textHash")
    searchLang = request.json.get("searchLang")
    if searchLang:
        searchLang = int(searchLang)

    if textHash is None:
        return jsonify({"data": None, "code": 400, "msg": "textHash is required"})

    start = time.time()
    contents = controllers_module.getTextEntitySources(int(textHash), searchLang) # type: ignore
    end = time.time()

    return jsonify({
        "data": {
            "contents": contents,
            "time": (end - start) * 1000
        },
        "code": 200,
        "msg": "ok"
    })


@api_bp.route("/api/catalogSearch", methods=["POST"])
def catalogSearch():
    keyword = request.json.get("keyword", "").strip()
    langCode = request.json.get("langCode")
    sourceTypeCode = request.json.get("sourceTypeCode")
    subCategory = request.json.get("subCategory")
    createdVersion = request.json.get("createdVersion", "")
    updatedVersion = request.json.get("updatedVersion", "")
    page = int(request.json.get("page", 1))
    pageSize = int(request.json.get("pageSize", 50))

    if langCode is None:
        return jsonify({"data": None, "code": 400, "msg": "langCode is required"})

    stc = int(sourceTypeCode) if sourceTypeCode is not None and sourceTypeCode != "" else None
    sub = int(subCategory) if subCategory is not None and subCategory != "" else None
    has_created = bool(str(createdVersion or "").strip())
    has_updated = bool(str(updatedVersion or "").strip())

    if not keyword and stc is None and sub is None and not has_created and not has_updated:
        return jsonify({"data": None, "code": 400, "msg": "keyword or a filter is required"})

    result = controllers_module.searchCatalog(
        keyword, int(langCode), stc, sub, page, pageSize,
        createdVersion=createdVersion or None,
        updatedVersion=updatedVersion or None,
    )
    return jsonify({"data": result, "code": 200, "msg": "ok"})


@api_bp.route("/api/catalogMeta", methods=["GET"])
def catalogMeta():
    return jsonify({
        "data": {
            "mainCategories": controllers_module.getCatalogMainCategories(),  # type: ignore
            "subCategories": controllers_module.getCatalogSubCategories(),  # type: ignore
            "subCategoryGroups": controllers_module.getCatalogSubCategoryGroups(),  # type: ignore
            "uncategorizedSubCategory": controllers_module.getCatalogUncategorizedSubCategory(),  # type: ignore
        },
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/saveSettings", methods=["POST"])
def saveSettings():

    newConfig = request.json["config"]
    if "defaultSearchLanguage" in newConfig:
        controllers_module.setDefaultSearchLanguage(newConfig["defaultSearchLanguage"]) # type: ignore

    if "resultLanguages" in newConfig:
        controllers_module.setResultLanguages(newConfig["resultLanguages"]) # type: ignore

    if "sourceLanguage" in newConfig:
        controllers_module.setSourceLanguage(newConfig["sourceLanguage"]) # type: ignore

    if "isMale" in newConfig:
        controllers_module.setIsMale(newConfig["isMale"]) # type: ignore

    controllers_module.saveConfig() # type: ignore
    return jsonify({
        "data": controllers_module.getConfig(), # type: ignore
        "code": 200,
        "msg": "ok"
    })

@api_bp.route("/api/getSettings")
def getConfigApi():
    return jsonify({
        "data": controllers_module.getConfig(), # type: ignore
        "code": 200,
        "msg": "ok"
    })

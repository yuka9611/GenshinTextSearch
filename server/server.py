import os
import sys
import time
import threading
import webbrowser

from flask import Flask, jsonify, request, send_file, make_response, send_from_directory


def resource_path(rel_path: str) -> str:
    """
    兼容 PyInstaller 和源码运行的资源路径
    - 打包后资源在 sys._MEIPASS
    - 源码运行时以项目根目录为基准（server/..）
    """
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS  # type: ignore
    else:
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(base_path, rel_path)


def buildResponse(data=None, code=200, msg="ok"):
    return jsonify({"data": data, "code": code, "msg": msg})


def create_app() -> Flask:
    """
    工厂模式：把重 import/初始化从模块顶层挪走，提升 PyInstaller 启动速度
    """
    app = Flask(__name__)

    # 延迟导入 CORS（减少顶层 import）
    from flask_cors import CORS
    CORS(app)

    # ----------------------------
    # Startup / Settings APIs
    # ----------------------------
    @app.route("/api/startupStatus")
    def startupStatus():
        import config
        return buildResponse({
            "assetDirValid": config.isAssetDirValid(),
            "assetDir": config.getAssetDir()
        })

    @app.route("/api/setAssetDir", methods=["POST"])
    def setAssetDir():
        import config
        import languagePackReader

        assetDir = request.json.get("assetDir")
        if not assetDir or not os.path.isdir(assetDir):
            return buildResponse(code=400, msg="Invalid directory")

        config.setAssetDir(assetDir)
        config.saveConfig()

        # 重新加载语音包
        languagePackReader.reloadLangPackages()

        return buildResponse({
            "assetDir": assetDir,
            "assetDirValid": config.isAssetDirValid()
        })

    @app.route("/api/pickAssetDir", methods=["POST"])
    def pickAssetDir():
        import controllers
        import config
        import languagePackReader

        picked = controllers.pickAssetDirViaDialog()
        if not picked:
            return buildResponse({
                "cancel": True,
                "assetDir": config.getAssetDir(),
                "assetDirValid": config.isAssetDirValid()
            })

        if not os.path.isdir(picked):
            return buildResponse(code=400, msg="Invalid directory")

        config.setAssetDir(picked)
        config.saveConfig()
        languagePackReader.reloadLangPackages()

        return buildResponse({
            "cancel": False,
            "assetDir": picked,
            "assetDirValid": config.isAssetDirValid()
        })

    # ----------------------------
    # Existing APIs
    # ----------------------------
    @app.route("/api/getImportedTextLanguages")
    def getImportedTextLanguages():
        import controllers
        return buildResponse(controllers.getImportedTextMapLangs())

    @app.route("/api/getImportedVoiceLanguages")
    def getImportedVoiceLanguages():
        import controllers
        return buildResponse(controllers.getLoadedVoicePacks())

    @app.route("/api/keywordQuery", methods=["POST"])
    def keywordQuery():
        import controllers

        langCode = int(request.json["langCode"])
        keyword: str = request.json["keyword"]
        speaker = request.json.get("speaker")
        page = request.json.get("page", 1)
        pageSize = request.json.get("pageSize", 50)
        voiceFilter = request.json.get("voiceFilter", "all")

        try:
            page = int(page)
        except Exception:
            page = 1

        try:
            pageSize = int(pageSize)
        except Exception:
            pageSize = 50

        if page < 1:
            page = 1
        if pageSize < 1:
            pageSize = 50

        if voiceFilter not in ("all", "with", "without"):
            voiceFilter = "all"

        if keyword.strip() == "" and (speaker is None or str(speaker).strip() == ""):
            return buildResponse({
                "contents": [],
                "total": 0,
                "page": page,
                "pageSize": pageSize,
                "time": 0
            })

        start = time.time()
        contents, total = controllers.getTranslateObj(
            keyword,
            langCode,
            speaker,
            page=page,
            page_size=pageSize,
            voice_filter=voiceFilter,
        )
        end = time.time()

        return buildResponse({
            "contents": contents,
            "total": total,
            "page": page,
            "pageSize": pageSize,
            "time": (end - start) * 1000
        })

    @app.route("/api/getVoiceOver", methods=["POST"])
    def getVoiceOver():
        import controllers

        try:
            langCode = int(request.json["langCode"])
        except Exception:
            return buildResponse(code=400, msg="Invalid langCode")

        voicePath = request.json["voicePath"]
        wemStream = controllers.getVoiceBinStream(voicePath, langCode)

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

    @app.route("/api/getTalkFromHash", methods=["POST"])
    def getTalkFromHash():
        import controllers

        textHash: int = request.json["textHash"]
        searchLang = request.json.get("searchLang")
        if searchLang:
            searchLang = int(searchLang)

        try:
            start = time.time()
            contents = controllers.getTalkFromHash(textHash, searchLang)
            end = time.time()
        except Exception as e:
            return buildResponse(code=114, msg=str(e))

        return buildResponse({
            "contents": contents,
            "time": (end - start) * 1000
        })

    @app.route("/api/getSubtitleContext", methods=["POST"])
    def getSubtitleContext():
        import controllers

        fileName = request.json.get("fileName")
        subtitleId = request.json.get("subtitleId")
        searchLang = request.json.get("searchLang")
        if searchLang:
            searchLang = int(searchLang)

        start = time.time()
        contents = controllers.getSubtitleContext(fileName, subtitleId, searchLang)
        end = time.time()

        return buildResponse({
            "contents": contents,
            "time": (end - start) * 1000
        })

    @app.route("/api/nameSearch", methods=["POST"])
    def nameSearch():
        import controllers

        langCode = int(request.json["langCode"])
        keyword: str = request.json["keyword"]

        if keyword.strip() == "":
            return buildResponse({
                "quests": [],
                "readables": []
            })

        start = time.time()
        contents = controllers.searchNameEntries(keyword, langCode)
        end = time.time()

        return buildResponse({
            "contents": contents,
            "time": (end - start) * 1000
        })

    @app.route("/api/avatarSearch", methods=["POST"])
    def avatarSearch():
        import controllers

        langCode = int(request.json["langCode"])
        keyword: str = request.json["keyword"]

        if keyword.strip() == "":
            return buildResponse({
                "avatars": []
            })

        start = time.time()
        contents = controllers.searchAvatarEntries(keyword, langCode)
        end = time.time()

        return buildResponse({
            "contents": contents,
            "time": (end - start) * 1000
        })

    @app.route("/api/avatarVoice", methods=["POST"])
    def avatarVoice():
        import controllers

        avatarId = request.json.get("avatarId")
        searchLang = request.json.get("searchLang")
        if searchLang:
            searchLang = int(searchLang)

        if avatarId is None:
            return buildResponse(code=400, msg="avatarId is required")

        avatarId = int(avatarId)

        start = time.time()
        contents = controllers.getAvatarVoices(avatarId, searchLang)
        end = time.time()

        return buildResponse({
            "contents": contents,
            "time": (end - start) * 1000
        })

    @app.route("/api/avatarStory", methods=["POST"])
    def avatarStory():
        import controllers

        avatarId = request.json.get("avatarId")
        searchLang = request.json.get("searchLang")
        if searchLang:
            searchLang = int(searchLang)

        if avatarId is None:
            return buildResponse(code=400, msg="avatarId is required")

        avatarId = int(avatarId)

        start = time.time()
        contents = controllers.getAvatarStories(avatarId, searchLang)
        end = time.time()

        return buildResponse({
            "contents": contents,
            "time": (end - start) * 1000
        })

    @app.route("/api/getReadableContent", methods=["POST"])
    def getReadableContent():
        import controllers

        readableId = request.json.get("readableId")
        fileName = request.json.get("fileName")
        searchLang = request.json.get("searchLang")
        if searchLang:
            searchLang = int(searchLang)
        if readableId is not None:
            readableId = int(readableId)

        start = time.time()
        contents = controllers.getReadableContent(readableId, fileName, searchLang)
        end = time.time()

        return buildResponse({
            "contents": contents,
            "time": (end - start) * 1000
        })

    @app.route("/api/getQuestDialogues", methods=["POST"])
    def getQuestDialogues():
        import controllers

        questId = request.json.get("questId")
        searchLang = request.json.get("searchLang")
        page = request.json.get("page", 1)
        pageSize = request.json.get("pageSize", 200)
        if searchLang:
            searchLang = int(searchLang)

        if questId is None:
            return buildResponse(code=400, msg="questId is required")

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
        contents, total = controllers.getQuestDialogues(questId, searchLang, page, pageSize)
        end = time.time()

        return buildResponse({
            "contents": contents,
            "total": total,
            "page": page,
            "pageSize": pageSize,
            "time": (end - start) * 1000
        })

    @app.route("/api/saveSettings", methods=["POST"])
    def saveSettings():
        import controllers

        newConfig = request.json["config"]
        if "defaultSearchLanguage" in newConfig:
            controllers.setDefaultSearchLanguage(newConfig["defaultSearchLanguage"])

        if "resultLanguages" in newConfig:
            controllers.setResultLanguages(newConfig["resultLanguages"])

        if "sourceLanguage" in newConfig:
            controllers.setSourceLanguage(newConfig["sourceLanguage"])

        if "isMale" in newConfig:
            controllers.setIsMale(newConfig["isMale"])

        controllers.saveConfig()
        return buildResponse(controllers.getConfig())

    @app.route("/api/getSettings")
    def getConfigApi():
        import controllers
        return buildResponse(controllers.getConfig())

    # ----------------------------
    # Static frontend (webui/dist)
    # ----------------------------
    staticDir = resource_path("webui/dist")

    @app.route("/")
    def serveRoot():
        return send_from_directory(staticDir, "index.html")

    @app.route("/<path:path>")
    def serveStatic(path):
        filePath = os.path.join(staticDir, path)
        if os.path.exists(filePath):
            return send_from_directory(staticDir, path)
        return send_from_directory(staticDir, "index.html")

    return app


def maybe_open_browser(url: str):
    """
    可控地打开浏览器：
    - 默认打开（方便发行版）
    - 若设置环境变量 GTS_NO_BROWSER=1 则不打开
    """
    if os.environ.get("GTS_NO_BROWSER", "").strip() == "1":
        return

    def _open():
        # 给 Flask 多一点时间起来（onefile 解压 + 初始化）
        time.sleep(1.5)
        try:
            webbrowser.open(url, new=1)
        except Exception:
            pass

    threading.Thread(target=_open, daemon=True).start()


if __name__ == "__main__":
    app = create_app()
    maybe_open_browser("http://127.0.0.1:5000/")
    # 桌面发行版建议只监听本机
    app.run(debug=False, host="127.0.0.1", port=5000)

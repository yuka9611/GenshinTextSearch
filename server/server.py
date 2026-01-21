import os
import sys
import time

from flask import Flask, jsonify, request, send_file, make_response, send_from_directory
from flask_cors import CORS

import controllers
import config
import languagePackReader
import threading
import webbrowser


def resource_path(rel_path: str) -> str:
    """
    兼容 PyInstaller 和源码运行的资源路径
    - 打包后资源在 sys._MEIPASS
    - 源码运行时以项目根目录为基准（server/..）
    """
    if hasattr(sys, "_MEIPASS"):
        base_path = sys._MEIPASS # type: ignore
    else:
        base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    return os.path.join(base_path, rel_path)


app = Flask(__name__)
CORS(app)


def buildResponse(data=None, code=200, msg="ok"):
    return jsonify({"data": data, "code": code, "msg": msg})


# ----------------------------
# Startup / Settings APIs
# ----------------------------

@app.route("/api/startupStatus")
def startupStatus():
    """
    前端启动时调用：判断 assetDir 是否有效，决定是否弹窗引导用户选择目录
    """
    return buildResponse({
        "assetDirValid": config.isAssetDirValid(),
        "assetDir": config.getAssetDir()
    })


@app.route("/api/setAssetDir", methods=["POST"])
def setAssetDir():
    """
    前端选择目录后调用：保存 assetDir，并尝试重新加载语音包
    """
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
    """
    由后端弹出“选择文件夹”对话框（更适合发行版/本地桌面使用）
    - 选中：保存并 reload
    - 取消：返回 cancel=True
    """
    picked = controllers.pickAssetDirViaDialog()
    if not picked:
        return buildResponse({"cancel": True, "assetDir": config.getAssetDir(), "assetDirValid": config.isAssetDirValid()})

    if not os.path.isdir(picked):
        return buildResponse(code=400, msg="Invalid directory")

    config.setAssetDir(picked)
    config.saveConfig()
    languagePackReader.reloadLangPackages()

    return buildResponse({"cancel": False, "assetDir": picked, "assetDirValid": config.isAssetDirValid()})


# ----------------------------
# Existing APIs
# ----------------------------

@app.route("/api/getImportedTextLanguages")
def getImportedTextLanguages():
    return buildResponse(controllers.getImportedTextMapLangs())


@app.route("/api/getImportedVoiceLanguages")
def getImportedVoiceLanguages():
    return buildResponse(controllers.getLoadedVoicePacks())


@app.route("/api/keywordQuery", methods=['POST'])
def keywordQuery():
    langCode = int(request.json['langCode'])
    keyword: str = request.json['keyword']

    if keyword.strip() == "":
        return buildResponse([])

    start = time.time()
    contents = controllers.getTranslateObj(keyword, langCode)
    end = time.time()

    return buildResponse({
        'contents': contents,
        'time': (end - start) * 1000
    })


@app.route("/api/getVoiceOver", methods=['POST'])
def getVoiceOver():
    try:
        langCode = int(request.json['langCode'])   # ✅ 关键：转 int
    except Exception:
        return buildResponse(code=400, msg="Invalid langCode")

    voicePath = request.json['voicePath']

    wemStream = controllers.getVoiceBinStream(voicePath, langCode)
    if wemStream is None:
        resp = make_response("Audio File Not Found")
        resp.headers['Access-Control-Expose-Headers'] = 'Error'
        resp.headers['Error'] = 'True'
        return resp

    return send_file(
        wemStream,
        download_name=os.path.basename(voicePath),   # ✅ 不要写死 'voicePath'
        mimetype='application/octet-stream'          # ✅ 不要用 image/png
    )


@app.route("/api/getTalkFromHash", methods=['POST'])
def getTalkFromHash():
    textHash: int = request.json['textHash']
    searchLang = request.json.get('searchLang')
    if searchLang:
        searchLang = int(searchLang)
    try:
        start = time.time()
        contents = controllers.getTalkFromHash(textHash, searchLang)
        end = time.time()
    except Exception as e:
        return buildResponse(code=114, msg=str(e))

    return buildResponse({
        'contents': contents,
        'time': (end - start) * 1000
    })


@app.route("/api/getSubtitleContext", methods=['POST'])
def getSubtitleContext():
    fileName = request.json.get('fileName')
    subtitleId = request.json.get('subtitleId')
    searchLang = request.json.get('searchLang')
    if searchLang:
        searchLang = int(searchLang)

    start = time.time()
    contents = controllers.getSubtitleContext(fileName, subtitleId, searchLang)
    end = time.time()

    return buildResponse({
        'contents': contents,
        'time': (end - start) * 1000
    })


@app.route("/api/nameSearch", methods=['POST'])
def nameSearch():
    langCode = int(request.json['langCode'])
    keyword: str = request.json['keyword']

    if keyword.strip() == "":
        return buildResponse({
            "quests": [],
            "readables": []
        })

    start = time.time()
    contents = controllers.searchNameEntries(keyword, langCode)
    end = time.time()

    return buildResponse({
        'contents': contents,
        'time': (end - start) * 1000
    })


@app.route("/api/getReadableContent", methods=['POST'])
def getReadableContent():
    readableId = request.json.get('readableId')
    fileName = request.json.get('fileName')
    searchLang = request.json.get('searchLang')
    if searchLang:
        searchLang = int(searchLang)
    if readableId is not None:
        readableId = int(readableId)

    start = time.time()
    contents = controllers.getReadableContent(readableId, fileName, searchLang)
    end = time.time()

    return buildResponse({
        'contents': contents,
        'time': (end - start) * 1000
    })


@app.route("/api/getQuestDialogues", methods=['POST'])
def getQuestDialogues():
    questId = request.json.get('questId')
    searchLang = request.json.get('searchLang')
    if searchLang:
        searchLang = int(searchLang)
    if questId is None:
        return buildResponse(code=400, msg="questId is required")
    questId = int(questId)

    start = time.time()
    contents = controllers.getQuestDialogues(questId, searchLang)
    end = time.time()

    return buildResponse({
        'contents': contents,
        'time': (end - start) * 1000
    })


@app.route("/api/saveSettings", methods=['POST'])
def saveSettings():
    newConfig = request.json['config']
    if 'defaultSearchLanguage' in newConfig:
        controllers.setDefaultSearchLanguage(newConfig['defaultSearchLanguage'])

    if 'resultLanguages' in newConfig:
        controllers.setResultLanguages(newConfig['resultLanguages'])

    if 'sourceLanguage' in newConfig:
        controllers.setSourceLanguage(newConfig['sourceLanguage'])

    if 'isMale' in newConfig:
        controllers.setIsMale(newConfig['isMale'])

    controllers.saveConfig()

    return buildResponse(controllers.getConfig())


@app.route("/api/getSettings")
def getConfigApi():
    return buildResponse(controllers.getConfig())


# ----------------------------
# Static frontend (webui/dist)
# ----------------------------

# 关键：发行版里用 resource_path 才能稳定找到 dist
staticDir = resource_path("webui/dist")


@app.route('/')
def serveRoot():
    return send_from_directory(staticDir, 'index.html')


@app.route("/<path:path>")
def serveStatic(path):
    filePath = os.path.join(staticDir, path)
    if os.path.exists(filePath):
        return send_from_directory(staticDir, path)
    else:
        return send_from_directory(staticDir, 'index.html')


if __name__ == "__main__":
    def open_browser():
        # 延迟一下，等 Flask 起来
        time.sleep(0.8)
        webbrowser.open("http://127.0.0.1:5000/", new=1)

    threading.Thread(target=open_browser, daemon=True).start()
    app.run(debug=False, host="127.0.0.1", port=5000)
    # 桌面发行版建议只监听本机
    app.run(debug=False, host='127.0.0.1', port=5000)

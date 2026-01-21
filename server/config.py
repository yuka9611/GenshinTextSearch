import json
import os
import sys
from pathlib import Path


# ----------------------------
# App dirs (user writable)
# ----------------------------

def get_app_dir() -> Path:
    """
    用户目录：跨平台、可写、不会被 PyInstaller 临时目录吞掉
    """
    base = Path.home() / ".genshin_text_search"
    base.mkdir(parents=True, exist_ok=True)
    return base


CONFIG_FILE = get_app_dir() / "config.json"
DB_FILE = get_app_dir() / "data.db"


def is_packaged() -> bool:
    return hasattr(sys, "_MEIPASS")


def project_root() -> Path:
    # config.py 在项目根目录时：root = config.py 所在目录
    # 如果你把 config.py 放在 server/ 里，请相应调整 parent
    return Path(__file__).resolve().parent


def bundled_resource_path(rel: str) -> Path:
    """
    打包后：资源在 sys._MEIPASS
    源码：资源在项目根目录
    """
    if is_packaged():
        return Path(sys._MEIPASS) / rel  # type: ignore
    return project_root() / rel


# ----------------------------
# Config model
# ----------------------------

config = {
    "resultLanguages": [1, 4, 9],
    "defaultSearchLanguage": 1,
    # 默认空，让前端引导用户设置（比硬编码路径更“发行级”）
    "assetDir": "",
    "sourceLanguage": 1,
    # 兼容你现在的用法：bool / "both"
    "isMale": "both"
}


def loadConfig():
    if not CONFIG_FILE.exists():
        return

    try:
        with open(CONFIG_FILE, encoding='utf-8') as fp:
            fileJson = json.load(fp)
    except Exception:
        return

    if "assetDir" in fileJson:
        config["assetDir"] = fileJson['assetDir']

    if "resultLanguages" in fileJson and isinstance(fileJson["resultLanguages"], list):
        config["resultLanguages"] = fileJson["resultLanguages"]

    if "defaultSearchLanguage" in fileJson and isinstance(fileJson["defaultSearchLanguage"], int):
        config["defaultSearchLanguage"] = fileJson["defaultSearchLanguage"]

    if "sourceLanguage" in fileJson and isinstance(fileJson['sourceLanguage'], int):
        config['sourceLanguage'] = fileJson['sourceLanguage']

    # 兼容旧逻辑：bool 或 "both"
    if "isMale" in fileJson and isinstance(fileJson['isMale'], bool):
        config['isMale'] = fileJson['isMale']
    elif "isMale" in fileJson and fileJson['isMale'] == "both":
        config['isMale'] = "both"


def saveConfig():
    with open(CONFIG_FILE, encoding='utf-8', mode="w") as fp:
        json.dump(config, fp, ensure_ascii=False, indent=2)


def setDefaultSearchLanguage(newLanguage: int):
    config['defaultSearchLanguage'] = newLanguage


def setResultLanguages(newLanguages: list[int]):
    config["resultLanguages"] = newLanguages


def setSourceLanguage(newSourceLanguage: int):
    config['sourceLanguage'] = newSourceLanguage


def setIsMale(isMale):
    config['isMale'] = isMale


def setAssetDir(assetDir: str):
    assetDir = os.path.normpath(assetDir)
    if os.path.basename(assetDir).lower() == "streamingassets":
        assetDir = os.path.dirname(assetDir)
    config['assetDir'] = assetDir


def getDefaultSearchLanguage():
    return config['defaultSearchLanguage']


def getResultLanguages():
    return config["resultLanguages"]


def getSourceLanguage():
    return config['sourceLanguage']


def getAssetDir():
    return config['assetDir']


def getIsMale():
    return config['isMale']


def isAssetDirValid() -> bool:
    assetDir = getAssetDir()
    if not assetDir or not os.path.isdir(assetDir):
        return False

    streaming_audio = os.path.join(assetDir, "StreamingAssets", "AudioAssets")
    persistent_audio = os.path.join(assetDir, "Persistent", "AudioAssets")

    return os.path.isdir(streaming_audio) or os.path.isdir(persistent_audio)


def get_db_path() -> Path:
    """
    实际运行使用的 DB 路径（用户目录，可写）
    """
    return DB_FILE


def ensure_db_exists(bundled_rel_path: str = "data.db"):
    """
    发行版：把打包进 exe 的 data.db 复制到用户目录，避免:
    - 相对路径找不到
    - _MEIPASS 临时目录不可写/会丢失
    """
    db_path = get_db_path()
    if db_path.exists() and db_path.stat().st_size > 0:
        return

    src = bundled_resource_path(bundled_rel_path)
    if not src.exists():
        # 如果你开发时 db 还没放到项目根目录，也不强制报错
        # 但此时 databaseHelper 连接会失败，建议你确保打包时带上 data.db
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_bytes(src.read_bytes())


loadConfig()

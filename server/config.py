import json
import os
import sys
from pathlib import Path


# ----------------------------
# Local app files (server folder)
# ----------------------------


def is_packaged() -> bool:
    # onedir 场景通常只有 sys.frozen，没有 _MEIPASS
    return bool(getattr(sys, "frozen", False) or hasattr(sys, "_MEIPASS"))

def _resolve_server_dir() -> Path:
    current = Path(__file__).resolve().parent
    candidates = []
    if current.name != "server":
        candidates.append(current / "server")
    if hasattr(sys, "_MEIPASS"):
        candidates.append(Path(sys._MEIPASS) / "server")  # type: ignore[attr-defined]
    if is_packaged():
        exec_dir = Path(sys.executable).resolve().parent
        candidates.extend([exec_dir / "server", exec_dir / "_internal" / "server"])
    candidates.append(current)
    candidates.append(current.parent / "server")
    for candidate in candidates:
        if candidate.is_dir():
            return candidate
    return current

def _fallback_user_dir() -> Path:
    base = Path.home() / ".genshin_text_search"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _dir_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        test_file = path / ".write_test"
        with open(test_file, "w", encoding="utf-8") as fp:
            fp.write("ok")
        test_file.unlink(missing_ok=True)
        return True
    except Exception:
        return False


SERVER_DIR = _resolve_server_dir()
RUNTIME_DIR = SERVER_DIR if _dir_writable(SERVER_DIR) else _fallback_user_dir()
CONFIG_FILE = RUNTIME_DIR / "config.json"
DB_FILE = RUNTIME_DIR / "data.db"


def project_root() -> Path:
    # config.py 在项目根目录时：root = config.py 所在目录
    # 如果你把 config.py 放在 server/ 里，请相应调整 parent
     return Path(__file__).resolve().parents[1]


def executable_dir() -> Path:
    """
    打包后：exe 所在目录
    源码：项目根目录
    """
    if is_packaged():
        return Path(sys.executable).resolve().parent
    return project_root()


def bundled_resource_path(rel: str) -> Path:
    """
    打包后：资源在 sys._MEIPASS
    源码：资源在项目根目录
    """
    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS) / rel  # type: ignore[attr-defined]
    if is_packaged():
        return executable_dir() / rel
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

    exec_dir = executable_dir()
    repo_root = project_root()
    cwd = Path.cwd()

    def _with_server_dir(base: Path) -> list[Path]:
        return [
            base / bundled_rel_path,
            base / "server" / bundled_rel_path,
            base / "_internal" / "server" / bundled_rel_path,
        ]

    candidates: list[Path] = []
    candidates.extend(_with_server_dir(bundled_resource_path("")))
    candidates.extend(_with_server_dir(exec_dir))
    candidates.extend(_with_server_dir(exec_dir.parent))
    candidates.extend(_with_server_dir(cwd))
    candidates.extend(_with_server_dir(repo_root))

    src = next((path for path in candidates if path.exists()), None)
    if src is None:
        # 如果你开发时 db 还没放到项目根目录，也不强制报错
        # 但此时 databaseHelper 连接会失败，建议你确保打包时带上 data.db
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_bytes(src.read_bytes())


loadConfig()

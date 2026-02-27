import json
import os
import sys
from pathlib import Path


# ----------------------------
# Local app files (server folder)
# ----------------------------


def is_packaged() -> bool:
    # onedir usually has sys.frozen; onefile may also expose _MEIPASS.
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
# DB path is fixed to server/data.db.
DB_FILE = SERVER_DIR / "data.db"


def project_root() -> Path:
    # Project root. If config.py is moved, adjust this parent depth accordingly.
    return Path(__file__).resolve().parents[1]


def executable_dir() -> Path:
    """
    Runtime executable directory in packaged mode.
    Falls back to project root in source mode.
    """
    if is_packaged():
        return Path(sys.executable).resolve().parent
    return project_root()



# ----------------------------
# Config model
# ----------------------------

config = {
    "resultLanguages": [1, 4, 9],
    "defaultSearchLanguage": 1,
    # Asset root directory, selected by user at first run.
    "assetDir": "",
    "sourceLanguage": 1,
    # Keep compatibility with legacy values: bool or "both".
    "isMale": "both",
    # FTS settings.
    "enableTextMapFts": True,
    # Only build textMap FTS for these languages (e.g. CHS/EN/JP).
    "ftsLangAllowList": [1, 4, 9],
    "ftsTokenizer": "trigram",
    # Extra tokenizer args string, extension-specific.
    "ftsTokenizerArgs": "",
    # Chinese query/index tokenization mode for non-trigram tokenizers.
    # Supported: auto / jieba / char_bigram / none
    "ftsChineseSegmenter": "auto",
    # Optional jieba user dictionary path.
    "ftsJiebaUserDict": "",
    "ftsExtensionPath": "",
    "ftsExtensionEntry": "",
    # Query-time token filtering for non-trigram tokenizers.
    "ftsStopwords": [],
    "ftsMinTokenLength": 1,
    "ftsMaxTokenLength": 32,
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
    # Backward compatibility: bool in old config, or "both".
    if "isMale" in fileJson and isinstance(fileJson['isMale'], bool):
        config['isMale'] = fileJson['isMale']
    elif "isMale" in fileJson and fileJson['isMale'] == "both":
        config['isMale'] = "both"

    if "ftsTokenizer" in fileJson and isinstance(fileJson["ftsTokenizer"], str):
        config["ftsTokenizer"] = fileJson["ftsTokenizer"].strip() or "trigram"

    if "enableTextMapFts" in fileJson:
        config["enableTextMapFts"] = bool(fileJson["enableTextMapFts"])

    if "ftsLangAllowList" in fileJson and isinstance(fileJson["ftsLangAllowList"], list):
        normalized_langs = []
        for raw in fileJson["ftsLangAllowList"]:
            try:
                normalized_langs.append(int(raw))
            except Exception:
                continue
        if normalized_langs:
            config["ftsLangAllowList"] = normalized_langs

    if "ftsExtensionPath" in fileJson and isinstance(fileJson["ftsExtensionPath"], str):
        config["ftsExtensionPath"] = fileJson["ftsExtensionPath"].strip()

    if "ftsExtensionEntry" in fileJson and isinstance(fileJson["ftsExtensionEntry"], str):
        config["ftsExtensionEntry"] = fileJson["ftsExtensionEntry"].strip()

    if "ftsTokenizerArgs" in fileJson and isinstance(fileJson["ftsTokenizerArgs"], str):
        config["ftsTokenizerArgs"] = fileJson["ftsTokenizerArgs"].strip()

    if "ftsChineseSegmenter" in fileJson and isinstance(fileJson["ftsChineseSegmenter"], str):
        mode = fileJson["ftsChineseSegmenter"].strip().lower()
        if mode in ("auto", "jieba", "char_bigram", "none"):
            config["ftsChineseSegmenter"] = mode

    if "ftsJiebaUserDict" in fileJson and isinstance(fileJson["ftsJiebaUserDict"], str):
        config["ftsJiebaUserDict"] = fileJson["ftsJiebaUserDict"].strip()

    if "ftsStopwords" in fileJson and isinstance(fileJson["ftsStopwords"], list):
        words = []
        for raw in fileJson["ftsStopwords"]:
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                words.append(text)
        config["ftsStopwords"] = words

    if "ftsMinTokenLength" in fileJson:
        try:
            config["ftsMinTokenLength"] = max(1, int(fileJson["ftsMinTokenLength"]))
        except Exception:
            pass

    if "ftsMaxTokenLength" in fileJson:
        try:
            config["ftsMaxTokenLength"] = max(1, int(fileJson["ftsMaxTokenLength"]))
        except Exception:
            pass


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


def getFtsTokenizer():
    return str(config.get("ftsTokenizer") or "trigram")


def getEnableTextMapFts():
    return bool(config.get("enableTextMapFts", True))


def getFtsLangAllowList():
    raw = config.get("ftsLangAllowList") or []
    result = []
    for item in raw:
        try:
            result.append(int(item))
        except Exception:
            continue
    return result


def getFtsExtensionPath():
    return str(config.get("ftsExtensionPath") or "")


def getFtsExtensionEntry():
    return str(config.get("ftsExtensionEntry") or "")


def getFtsTokenizerArgs():
    return str(config.get("ftsTokenizerArgs") or "")


def getFtsChineseSegmenter():
    mode = str(config.get("ftsChineseSegmenter") or "auto").strip().lower()
    if mode in ("auto", "jieba", "char_bigram", "none"):
        return mode
    return "auto"


def getFtsJiebaUserDict():
    return str(config.get("ftsJiebaUserDict") or "").strip()


def getFtsStopwords():
    raw = config.get("ftsStopwords") or []
    words = []
    for item in raw:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            words.append(text)
    return words


def getFtsMinTokenLength():
    try:
        return max(1, int(config.get("ftsMinTokenLength", 1)))
    except Exception:
        return 1


def getFtsMaxTokenLength():
    try:
        return max(1, int(config.get("ftsMaxTokenLength", 32)))
    except Exception:
        return 32


def isAssetDirValid() -> bool:
    assetDir = getAssetDir()
    if not assetDir or not os.path.isdir(assetDir):
        return False

    streaming_audio = os.path.join(assetDir, "StreamingAssets", "AudioAssets")
    persistent_audio = os.path.join(assetDir, "Persistent", "AudioAssets")

    return os.path.isdir(streaming_audio) or os.path.isdir(persistent_audio)


def get_db_path() -> Path:
    """
    Runtime DB path (fixed to server/data.db).
    """
    return DB_FILE


def ensure_db_exists(bundled_rel_path: str = "data.db"):
    """
    If runtime DB file is missing, try copying from external locations.
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
    candidates.extend(_with_server_dir(exec_dir))
    candidates.extend(_with_server_dir(exec_dir.parent))
    candidates.extend(_with_server_dir(cwd))
    candidates.extend(_with_server_dir(repo_root))

    src = next((path for path in candidates if path.exists()), None)
    if src is None:
        # No source DB found. Caller will surface a clear FileNotFoundError.
        return

    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_bytes(src.read_bytes())


loadConfig()


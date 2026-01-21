import os
import config

langCodes = {
    1: "Chinese",
    4: "English(US)",
    9: "Japanese",
    10: "Korean"
}

langPackages: dict[int, str] = {}  # langCode -> 目录路径


def reloadLangPackages():
    langPackages.clear()
    loadLangPackages()


def loadLangPackages():
    assetDir = config.getAssetDir()
    if not config.isAssetDirValid():
        return

    paths = [
        os.path.join(assetDir, "StreamingAssets", "AudioAssets"),
        os.path.join(assetDir, "Persistent", "AudioAssets")
    ]

    for base in paths:
        if not os.path.isdir(base):
            continue

        for code, name in langCodes.items():
            if code in langPackages:
                continue
            langDir = os.path.join(base, name)
            if os.path.isdir(langDir):
                files = os.listdir(langDir)
                if len(files) > 10:  # 基本判定：不是空目录
                    langPackages[code] = langDir


def checkAudioBin(voicePath: str, langCode: int) -> bool:
    if langCode not in langPackages:
        return False
    return os.path.isfile(os.path.join(langPackages[langCode], voicePath))


def getAudioBin(voicePath: str, langCode: int) -> bytes | None:
    if langCode not in langPackages:
        return None
    filePath = os.path.join(langPackages[langCode], voicePath)
    if not os.path.isfile(filePath):
        return None
    with open(filePath, "rb") as f:
        return f.read()


# 启动时尝试加载
loadLangPackages()

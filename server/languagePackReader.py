import os

from AudioReader.FilePackager import Package, fnv_hash_64

import config

langCodes = {
    1: "Chinese",
    4: "English(US)",
    9: "Japanese",
    10: "Korean",
}

langPackages: dict[int, Package] = {}


def reloadLangPackages():
    langPackages.clear()
    loadLangPackages()


def loadLangPackages():
    assetDir = config.getAssetDir()
    if not config.isAssetDirValid():
        return

    paths = [
        os.path.join(assetDir, "Persistent", "AudioAssets"),
        os.path.join(assetDir, "StreamingAssets", "AudioAssets"),
    ]

    for pathDir in paths:
        if not os.path.exists(pathDir):
            continue

        for langCode, langName in langCodes.items():
            if langCode in langPackages:
                continue
            langPackPath = os.path.join(pathDir, langName)
            if not os.path.exists(langPackPath):
                continue
            files = os.listdir(langPackPath)
            if len(files) < 10:
                continue

            voicePack = Package()
            for fileName in files:
                fobj = open(os.path.join(langPackPath, fileName), "rb")
                voicePack.addfile(fobj)
            langPackages[langCode] = voicePack


def getAudioBin(path: str, langCode: int) -> bytes | None:
    if langCode not in langPackages:
        return None
    langStr = langCodes[langCode]
    hashVal = fnv_hash_64((langStr + "\\" + path).lower())
    try:
        voicePack = langPackages[langCode]
        wemFiles = voicePack.get_file_data_by_hash(hashVal, langid=0, mode=2)
        wemBin, pckPath = wemFiles[0]
        return wemBin
    except (FileNotFoundError, KeyError):
        return None


def checkAudioBin(path: str, langCode: int) -> bool:
    if langCode not in langPackages:
        return False
    langStr = langCodes[langCode]
    hashVal = fnv_hash_64((langStr + "\\" + path).lower())
    voicePack = langPackages[langCode]

    return voicePack.check_file_by_hash(hashVal, langid=0, mode=2)


loadLangPackages()
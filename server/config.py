import json
import os.path

config = {
    "resultLanguages": [
        1, 4, 9
    ],

    "defaultSearchLanguage": 1,
    "assetDir": "D:\\Program Files\\Genshin Impact\\Genshin Impact game\\GenshinImpact_Data",
    "sourceLanguage": 1,
    "isMale": "both"
}


def loadConfig():
    if not os.path.isfile("config.json"):
        return
    fp = open("config.json", encoding='utf-8')
    fileJson = json.load(fp)
    fp.close()

    if "assetDir" in fileJson:
        config["assetDir"] = fileJson['assetDir']

    if "resultLanguages" in fileJson and isinstance(fileJson["resultLanguages"], list):
        config["resultLanguages"] = fileJson["resultLanguages"]

    if "defaultSearchLanguage" in fileJson and isinstance(fileJson["defaultSearchLanguage"], int):
        config["defaultSearchLanguage"] = fileJson["defaultSearchLanguage"]

    if "sourceLanguage" in fileJson and isinstance(fileJson['sourceLanguage'], int):
        config['sourceLanguage'] = fileJson['sourceLanguage']

    if "isMale" in fileJson and isinstance(fileJson['isMale'], bool):
        config['isMale'] = fileJson['isMale']
    elif "isMale" in fileJson and fileJson['isMale'] == "both":
        config['isMale'] = "both"


def saveConfig():
    fp = open("config.json", encoding='utf-8', mode="w")
    json.dump(config, fp)
    fp.close()


def setDefaultSearchLanguage(newLanguage: int):
    config['defaultSearchLanguage'] = newLanguage


def setResultLanguages(newLanguages: list[int]):
    config["resultLanguages"] = newLanguages


def setSourceLanguage(newSourceLanguage: int):
    config['sourceLanguage'] = newSourceLanguage


def setIsMale(isMale):
    config['isMale'] = isMale


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


loadConfig()
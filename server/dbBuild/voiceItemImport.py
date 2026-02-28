import os
import json
import sys
from lightweight_progress import LightweightProgress

from DBConfig import conn, DATA_PATH
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched
from versioning import ensure_version_schema


avatarMappings = {}
_UNMAPPED_AVATAR_NAMES: set[str] = set()
GENERIC_SWITCH_NAMES = {
    "switch_boy",
    "switch_girl",
    "switch_male",
    "switch_female",
    "switch_loli",
    "switch_lady",
    "switch_other",
}


def _print_summary(title: str, items: list[str] | set[str], sample_size: int = 10):
    values = list(items)
    if not values:
        return
    samples = values[: max(1, sample_size)]
    sample_text = ", ".join(samples)
    remaining = len(values) - len(samples)
    if remaining > 0:
        sample_text += f", ...(+{remaining})"
    print(f"[SUMMARY] {title}: {len(values)}. samples: {sample_text}")


def _collect_switch_names(config_node, output: set):
    if isinstance(config_node, dict):
        for value in config_node.values():
            _collect_switch_names(value, output)
    elif isinstance(config_node, list):
        for item in config_node:
            _collect_switch_names(item, output)
    elif isinstance(config_node, str):
        lowered = config_node.lower()
        if lowered.startswith("switch_"):
            output.add(lowered)


def _extract_internal_names(avatar_config: dict) -> set:
    switch_names = set()
    _collect_switch_names(avatar_config, switch_names)
    filtered_names = {name for name in switch_names if name not in GENERIC_SWITCH_NAMES}
    if filtered_names:
        return filtered_names

    legacy_name = (
        avatar_config.get("PLJBIDOIHEA", {})
        .get("KDKDKOMMCOB", {})
        .get("GLMJHDNIGID")
    )
    if isinstance(legacy_name, str) and legacy_name:
        return {legacy_name.lower()}

    return set()


def loadAvatars():
    global avatarMappings, _UNMAPPED_AVATAR_NAMES
    avatarMappings = {}
    _UNMAPPED_AVATAR_NAMES.clear()
    missing_configs: list[str] = []
    unresolved_switch_names: list[str] = []

    avatar_excel = os.path.join(DATA_PATH, "ExcelBinOutput", "AvatarExcelConfigData.json")
    with open(avatar_excel, encoding="utf-8") as f:
        avatarsJson = json.load(f)

    for avatar in avatarsJson:
        avatarId = avatar["id"]
        if avatarId >= 11000000:
            continue

        avatarName = avatar["iconName"][14:]
        configAvatarFileName = f"ConfigAvatar_{avatarName}.json"
        configAvatarPath = os.path.join(DATA_PATH, "BinOutput", "Avatar", configAvatarFileName)

        if not os.path.exists(configAvatarPath):
            missing_configs.append(configAvatarFileName)
            continue

        with open(configAvatarPath, encoding="utf-8") as f:
            avatarConfig = json.load(f)

        internalNames = _extract_internal_names(avatarConfig)
        if not internalNames:
            unresolved_switch_names.append(configAvatarFileName)
            continue

        for internalName in internalNames:
            if internalName in avatarMappings and avatarMappings[internalName] != avatarId:
                continue
            avatarMappings[internalName] = avatarId

    _print_summary("voice avatar config missing", missing_configs)
    _print_summary("voice avatar switch unresolved", unresolved_switch_names)


def getAvatarIdFromVoiceItemAvatarName(avatarNameFromVoiceItem: str):
    rawName = avatarNameFromVoiceItem.lower()
    if rawName.startswith("switch_gcg") or rawName.startswith("gcg"):
        return 0

    rawNameTranslate = {
        "switch_tartaglia_melee": "switch_tartaglia",
    }
    if rawName in rawNameTranslate:
        rawName = rawNameTranslate[rawName]

    if rawName in avatarMappings:
        return avatarMappings[rawName]

    _UNMAPPED_AVATAR_NAMES.add(rawName)
    return 0


def _resolve_voice_schema(content: dict):
    if "guid" in content:
        return "gameTriggerArgs", "sourceNames", "sourceFileName", "gameTrigger", "guid"
    if "Guid" in content:
        return "gameTriggerArgs", "SourceNames", "sourceFileName", "GameTrigger", "Guid"
    if "ABAEBGLPCIK" in content:
        return "MEDGFBMLDDK", "JKDJFGBGOEB", "DCIHFJLBLAP", "BHOKINENJBN", "ABAEBGLPCIK"
    if "NDLOFEPMEMO" in content:
        return "HEKJMGHIJBM", "JKHGLBHOKIC", "BJDAJEKPCFP", "HPIPCKOOMLL", "NDLOFEPMEMO"
    return None


def importVoiceItem(fileName: str, cursor, *, batch_size: int = DEFAULT_BATCH_SIZE):
    sql_insert = (
        "INSERT INTO voice(dialogueId, voicePath, gameTrigger, avatarId) VALUES (?,?,?,?) "
        "ON CONFLICT(dialogueId, voicePath) DO UPDATE SET "
        "gameTrigger=excluded.gameTrigger, "
        "avatarId=excluded.avatarId "
        "WHERE "
        "NOT (voice.gameTrigger IS excluded.gameTrigger) "
        "OR NOT (voice.avatarId IS excluded.avatarId)"
    )
    file_path = os.path.join(DATA_PATH, "BinOutput", "Voice", "Items", fileName)

    with open(file_path, encoding="utf-8") as f:
        textMap = json.load(f)

    rows = []
    for content in textMap.values():
        schema = _resolve_voice_schema(content)
        if schema is None:
            continue

        p1, p2, p3, p4, guidKeyName = schema
        if p2 not in content or p1 not in content or p4 not in content:
            continue

        dialogueId = content.get(p1)
        gameTrigger = content.get(p4)
        source_names = content.get(p2)

        if dialogueId is None or gameTrigger is None or not isinstance(source_names, list):
            continue

        for voice in source_names:
            if not isinstance(voice, dict):
                continue
            voicePath = voice.get(p3)
            if voicePath is None:
                continue

            avatarId = 0
            current_avatar_name = voice.get("avatarName") or voice.get("GDIJGLOHHFM", "")
            if guidKeyName in content and current_avatar_name:
                avatarId = getAvatarIdFromVoiceItemAvatarName(current_avatar_name)

            rows.append((dialogueId, voicePath, gameTrigger, avatarId))

    executemany_batched(cursor, sql_insert, rows, batch_size=batch_size)


def importAllVoiceItems(
    *,
    commit: bool = True,
    reset: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    ensure_version_schema()
    files = os.listdir(os.path.join(DATA_PATH, "BinOutput", "Voice", "Items"))
    cursor = conn.cursor()
    failed_files: list[str] = []

    if reset:
        cursor.execute("DELETE FROM voice")

    with LightweightProgress(len(files), desc="Voice files", unit="files") as pbar:
        for fileName in files:
            try:
                importVoiceItem(fileName, cursor, batch_size=batch_size)
            except Exception as e:
                failed_files.append(f"{fileName} ({e})")
                continue
            finally:
                pbar.update()

    cursor.close()
    if commit:
        conn.commit()
    _print_summary("voice item import failed files", failed_files)
    _print_summary("voice avatar unmapped names", sorted(_UNMAPPED_AVATAR_NAMES))


if __name__ == "__main__":
    loadAvatars()
    importAllVoiceItems(reset=False)

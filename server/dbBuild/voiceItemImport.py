import os
import json
import sys
from lightweight_progress import LightweightProgress

from DBConfig import conn, DATA_PATH
from import_utils import DEFAULT_BATCH_SIZE, executemany_batched
from version_control import ensure_version_schema


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

SOURCE_PATH_KEYS = (
    "sourceFileName",
    "MDOCAGOFPAP",
    "DCIHFJLBLAP",
    "BJDAJEKPCFP",
)

AVATAR_NAME_KEYS = (
    "avatarName",
    "GDIJGLOHHFM",
    "IEPAMKPOOII",
    "switchName",
)


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
    if rawName == "switch_hero":
        return 10000005
    if rawName == "switch_heroine":
        return 10000007

    rawNameTranslate = {
        "switch_tartaglia_melee": "switch_tartaglia",
    }
    if rawName in rawNameTranslate:
        rawName = rawNameTranslate[rawName]

    if rawName in avatarMappings:
        return avatarMappings[rawName]

    _UNMAPPED_AVATAR_NAMES.add(rawName)
    return 0


def _pick_first_value(node: dict, keys: tuple[str, ...]):
    for key in keys:
        value = node.get(key)
        if value is not None:
            return value
    return None


def _resolve_voice_schema(content: dict):
    if "guid" in content:
        return {
            "entry_id_key": "gameTriggerArgs",
            "source_list_key": "sourceNames",
            "trigger_key": "gameTrigger",
            "guid_key": "guid",
            "voice_path_keys": SOURCE_PATH_KEYS,
            "avatar_name_keys": AVATAR_NAME_KEYS,
        }
    if "Guid" in content:
        return {
            "entry_id_key": "gameTriggerArgs",
            "source_list_key": "SourceNames",
            "trigger_key": "GameTrigger",
            "guid_key": "Guid",
            "voice_path_keys": SOURCE_PATH_KEYS,
            "avatar_name_keys": AVATAR_NAME_KEYS,
        }
    if "ABAEBGLPCIK" in content:
        return {
            "entry_id_key": "MEDGFBMLDDK",
            "source_list_key": "JKDJFGBGOEB",
            "trigger_key": "BHOKINENJBN",
            "guid_key": "ABAEBGLPCIK",
            "voice_path_keys": ("DCIHFJLBLAP",) + SOURCE_PATH_KEYS,
            "avatar_name_keys": AVATAR_NAME_KEYS,
        }
    if "NDLOFEPMEMO" in content:
        return {
            "entry_id_key": "HEKJMGHIJBM",
            "source_list_key": "JKHGLBHOKIC",
            "trigger_key": "HPIPCKOOMLL",
            "guid_key": "NDLOFEPMEMO",
            "voice_path_keys": ("BJDAJEKPCFP",) + SOURCE_PATH_KEYS,
            "avatar_name_keys": AVATAR_NAME_KEYS,
        }
    if "IACPGADBANJ" in content:
        return {
            "entry_id_key": "MGOMDNKKLCP",
            "source_list_key": "OPGDOEDEJOJ",
            "trigger_key": "IACPGADBANJ",
            "guid_key": "ABNMMNPAKKP",
            "voice_path_keys": ("MDOCAGOFPAP",) + SOURCE_PATH_KEYS,
            "avatar_name_keys": AVATAR_NAME_KEYS,
        }
    return None


def _normalize_voice_item_content(content: dict):
    if not isinstance(content, dict):
        return None

    schema = _resolve_voice_schema(content)
    if schema is None:
        return None

    entry_id = content.get(schema["entry_id_key"])
    source_list = content.get(schema["source_list_key"])
    trigger = content.get(schema["trigger_key"])
    if entry_id is None or trigger is None or not isinstance(source_list, list):
        return None

    entry_type = str(trigger or "").strip().lower()
    if not entry_type:
        return None

    return {
        "entry_type": entry_type,
        "entry_id": entry_id,
        "source_list": source_list,
        "trigger": trigger,
        "guid": content.get(schema["guid_key"]),
        "voice_path_keys": schema["voice_path_keys"],
        "avatar_name_keys": schema["avatar_name_keys"],
    }


def _extract_voice_path(source: dict, voice_path_keys: tuple[str, ...]) -> str:
    value = _pick_first_value(source, voice_path_keys)
    return str(value or "").strip()


def _extract_avatar_name(source: dict, avatar_name_keys: tuple[str, ...]) -> str:
    value = _pick_first_value(source, avatar_name_keys)
    return str(value or "").strip()


def _ensure_fetter_voice_schema(cursor):
    row = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='fetterVoice'"
    ).fetchone()
    if row:
        columns = {
            column_row[1]
            for column_row in cursor.execute("PRAGMA table_info(fetterVoice)").fetchall()
        }
        if "avatarId" not in columns:
            cursor.execute("DROP TABLE IF EXISTS fetterVoice")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fetterVoice
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            avatarId INTEGER NOT NULL,
            voiceFile INTEGER NOT NULL,
            voicePath TEXT NOT NULL
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS fetterVoice_avatarId_voiceFile_index ON fetterVoice(avatarId, voiceFile)"
    )
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS fetterVoice_avatarId_voiceFile_voicePath_uindex "
        "ON fetterVoice(avatarId, voiceFile, voicePath)"
    )


def _extract_fetter_voice_rows(content: dict):
    normalized = _normalize_voice_item_content(content)
    if normalized is None or normalized["entry_type"] != "fetter":
        return []

    raw_voice_file = normalized["entry_id"]
    try:
        voice_file = int(raw_voice_file)
    except (TypeError, ValueError):
        return []

    rows = []
    seen_rows = set()
    for source in normalized["source_list"]:
        if not isinstance(source, dict):
            continue
        avatar_name = _extract_avatar_name(source, normalized["avatar_name_keys"])
        avatar_id = getAvatarIdFromVoiceItemAvatarName(str(avatar_name or ""))
        if avatar_id <= 0:
            continue
        voice_path_text = _extract_voice_path(source, normalized["voice_path_keys"])
        row_key = (avatar_id, voice_path_text)
        if not voice_path_text or row_key in seen_rows:
            continue
        seen_rows.add(row_key)
        rows.append((avatar_id, voice_file, voice_path_text))
    return rows


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
        normalized = _normalize_voice_item_content(content)
        if normalized is None or normalized["entry_type"] != "dialog":
            continue

        dialogueId = normalized["entry_id"]
        gameTrigger = normalized["trigger"]
        try:
            dialogueId = int(dialogueId)
        except (TypeError, ValueError):
            continue

        for voice in normalized["source_list"]:
            if not isinstance(voice, dict):
                continue
            voicePath = _extract_voice_path(voice, normalized["voice_path_keys"])
            if not voicePath:
                continue

            avatarId = 0
            current_avatar_name = _extract_avatar_name(voice, normalized["avatar_name_keys"])
            if normalized["guid"] is not None and current_avatar_name:
                avatarId = getAvatarIdFromVoiceItemAvatarName(current_avatar_name)

            rows.append((dialogueId, voicePath, gameTrigger, avatarId))

    executemany_batched(cursor, sql_insert, rows, batch_size=batch_size)


def importFetterVoiceItem(fileName: str, cursor, *, batch_size: int = DEFAULT_BATCH_SIZE):
    _ensure_fetter_voice_schema(cursor)
    sql_insert = (
        "INSERT INTO fetterVoice(avatarId, voiceFile, voicePath) VALUES (?, ?, ?) "
        "ON CONFLICT(avatarId, voiceFile, voicePath) DO NOTHING"
    )
    file_path = os.path.join(DATA_PATH, "BinOutput", "Voice", "Items", fileName)

    with open(file_path, encoding="utf-8") as f:
        textMap = json.load(f)

    rows = []
    for content in textMap.values():
        if not isinstance(content, dict):
            continue
        rows.extend(_extract_fetter_voice_rows(content))

    if rows:
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
    _ensure_fetter_voice_schema(cursor)

    if reset:
        cursor.execute("DELETE FROM voice")
        cursor.execute("DELETE FROM fetterVoice")

    with LightweightProgress(len(files), desc="Voice files", unit="files") as pbar:
        for fileName in files:
            try:
                importVoiceItem(fileName, cursor, batch_size=batch_size)
                importFetterVoiceItem(fileName, cursor, batch_size=batch_size)
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

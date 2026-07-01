import os
import re
from import_utils import load_json_file


_SUBTITLE_PATH_KEYS = [
    "AENCKCKHDFK",
    "dePath",
    "enPath",
    "esPath",
    "frPath",
    "HJBAJOBPLGE",
    "idPath",
    "INAGBNHLPIE",
    "itPath",
    "jpPath",
    "krPath",
    "NAGBHOLCGCH",
    "ptPath",
    "ruPath",
    "tcPath",
    "thPath",
    "trPath",
    "viPath",
    "EDPAFDDJJNM",
    "FNIFOPDJMMG",
]

_LANG_SUFFIX_RE = re.compile(r"_(CHS|CHT|DE|EN|ES|FR|ID|IT|JP|KR|PT|RU|TH|TR|VI)$", re.IGNORECASE)


def _looks_like_subtitle_path(path: str) -> bool:
    normalized = str(path or "").replace("\\", "/")
    parts = [part for part in normalized.split("/") if part]
    if any(part.lower() == "subtitle" for part in parts):
        return True
    base_name = os.path.basename(normalized)
    root, ext = os.path.splitext(base_name)
    return root.startswith("Cs_") and ext.lower() in {".mihoyobin", ".srt"}


def load_document_loc_title_hash(data_path: str) -> dict:
    """Map localization ID -> titleTextMapHash from DocumentExcelConfigData."""
    doc_path = os.path.join(data_path, "ExcelBinOutput", "DocumentExcelConfigData.json")
    data = load_json_file(
        doc_path,
        missing_msg="Document config not found",
        error_msg="Error loading DocumentExcelConfigData.json",
        default=None,
    )
    if not isinstance(data, list):
        return {}

    loc_id_to_title_hash = {}
    for entry in data:
        title_hash = entry.get("titleTextMapHash")
        quest_id_list = entry.get("questIDList", [])
        for loc_id in quest_id_list:
            loc_id_to_title_hash[loc_id] = title_hash
    return loc_id_to_title_hash


def load_localization_entries(data_path: str) -> list[dict]:
    """Load LocalizationExcelConfigData entries."""
    loc_path = os.path.join(data_path, "ExcelBinOutput", "LocalizationExcelConfigData.json")
    data = load_json_file(
        loc_path,
        missing_msg="Localization config not found",
        error_msg="Error loading LocalizationExcelConfigData.json",
        default=None,
    )
    if isinstance(data, list):
        return data
    return []


def build_readable_filename_map(localization_entries: list[dict], loc_id_to_title_hash: dict) -> dict:
    """Map readable file name variants to readable metadata."""
    filename_to_info = {}

    def add_mapping(file_name: str, title_hash, loc_id):
        if not file_name:
            return
        base_name = os.path.basename(file_name)
        if not base_name:
            return
        root, ext = os.path.splitext(base_name)
        normalized = _LANG_SUFFIX_RE.sub("", root) + ext
        info = {"titleHash": title_hash, "readableId": loc_id}
        filename_to_info[base_name] = info
        filename_to_info[root] = info
        filename_to_info[normalized] = info
        filename_to_info[os.path.splitext(normalized)[0]] = info

    for entry in localization_entries:
        loc_id = entry.get("id")
        if loc_id not in loc_id_to_title_hash:
            continue
        title_hash = loc_id_to_title_hash[loc_id]
        for value in entry.values():
            if isinstance(value, str) and "Readable" in value:
                add_mapping(value, title_hash, loc_id)
    return filename_to_info


def build_subtitle_filename_map(localization_entries: list[dict]) -> dict:
    """Map subtitle file stem -> subtitleId from LocalizationExcelConfigData."""
    filename_to_info = {}
    for entry in localization_entries:
        subtitle_paths = [
            entry.get(key)
            for key in _SUBTITLE_PATH_KEYS
            if isinstance(entry.get(key), str)
        ]
        asset_type = entry.get("assetType")
        if asset_type is not None and asset_type != "LOC_SUBTITLE":
            continue
        if asset_type is None and not any(_looks_like_subtitle_path(path) for path in subtitle_paths):
            continue
        subtitle_id = entry.get("id")
        for path in subtitle_paths:
            filename_no_ext = os.path.splitext(os.path.basename(path))[0]
            filename_to_info[filename_no_ext] = {"subtitleId": subtitle_id}
    return filename_to_info

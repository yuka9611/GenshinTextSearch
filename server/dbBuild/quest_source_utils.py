from __future__ import annotations

import glob
import os
import re

from DBConfig import DATA_PATH
from import_utils import load_json_file, normalize_unique_ints
from quest_utils import extract_quest_id


SOURCE_TYPE_AQ = "AQ"
SOURCE_TYPE_LQ = "LQ"
SOURCE_TYPE_WQ = "WQ"
SOURCE_TYPE_EQ = "EQ"
SOURCE_TYPE_IQ = "IQ"
SOURCE_TYPE_HANGOUT = "HANGOUT"
SOURCE_TYPE_ANECDOTE = "ANECDOTE"
SOURCE_TYPE_UNKNOWN = "UNKNOWN"
ANECDOTE_SOURCE_STATUS_IMPORTABLE = "importable"
ANECDOTE_SOURCE_STATUS_MAPPING_MISS = "mapping_miss"
BASE_QUEST_SOURCE_TYPES = {
    SOURCE_TYPE_AQ,
    SOURCE_TYPE_LQ,
    SOURCE_TYPE_WQ,
    SOURCE_TYPE_EQ,
    SOURCE_TYPE_IQ,
}
_STEP_TALK_CONDITION_TYPES = {
    "QUEST_CONTENT_COMPLETE_TALK",
    "QUEST_CONTENT_FINISH_PLOT",
}

_QUEST_SOURCE_RAW_BY_ID: dict[int, str] | None = None
_HANGOUT_QUEST_IDS: set[int] | None = None
_MAIN_COOP_IDS_BY_QUEST_ID: dict[int, list[int]] | None = None
_STORYBOARD_TALK_EXCEL_BY_QUEST_ID: dict[int, list[int]] | None = None
_STORYBOARD_FILE_BY_TALK_ID: dict[int, str] | None = None


def _extract_first_positive_int(row: dict, *keys: str) -> int | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def extract_anecdote_core_fields(row: dict) -> dict | None:
    if not isinstance(row, dict):
        return None

    anecdote_id = _extract_first_positive_int(row, "GBDGFHNLDFF", "DBGCFNMLHAJ")
    if anecdote_id is None:
        return None

    title_text_map_hash = _extract_first_positive_int(row, "PPANCKHJOGI", "EJMLGHMLPLD")
    desc_text_map_hash = _extract_first_positive_int(row, "AJKAHOPOBJB", "JKNBFACAMCF")
    long_desc_text_map_hash = _extract_first_positive_int(row, "OBLBGMIHBHL")

    group_ids = normalize_unique_ints(row.get("BBOMCGBIOFM"), positive_only=True)
    if not group_ids:
        group_ids = normalize_unique_ints(row.get("LIIPHELCPKJ"), positive_only=True)

    return {
        "quest_id": anecdote_id,
        "title_text_map_hash": title_text_map_hash,
        "desc_text_map_hash": desc_text_map_hash,
        "long_desc_text_map_hash": long_desc_text_map_hash,
        "group_ids": group_ids,
    }


def normalize_source_code_raw(value) -> str:
    if not isinstance(value, str):
        return SOURCE_TYPE_UNKNOWN
    normalized = value.strip().upper()
    if normalized in BASE_QUEST_SOURCE_TYPES:
        return normalized
    return SOURCE_TYPE_UNKNOWN


def reset_quest_source_caches():
    global _QUEST_SOURCE_RAW_BY_ID, _HANGOUT_QUEST_IDS, _MAIN_COOP_IDS_BY_QUEST_ID
    global _STORYBOARD_TALK_EXCEL_BY_QUEST_ID, _STORYBOARD_FILE_BY_TALK_ID
    _QUEST_SOURCE_RAW_BY_ID = None
    _HANGOUT_QUEST_IDS = None
    _MAIN_COOP_IDS_BY_QUEST_ID = None
    _STORYBOARD_TALK_EXCEL_BY_QUEST_ID = None
    _STORYBOARD_FILE_BY_TALK_ID = None


def load_quest_source_raw_by_id() -> dict[int, str]:
    global _QUEST_SOURCE_RAW_BY_ID
    if _QUEST_SOURCE_RAW_BY_ID is not None:
        return _QUEST_SOURCE_RAW_BY_ID

    mapping: dict[int, str] = {}
    folder = os.path.join(DATA_PATH, "BinOutput", "QuestBrief")
    if os.path.isdir(folder):
        for file_name in sorted(os.listdir(folder)):
            if not file_name.endswith(".json"):
                continue
            path = os.path.join(folder, file_name)
            try:
                obj = load_json_file(path)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            quest_id = extract_quest_id(obj)
            if not isinstance(quest_id, int) or quest_id <= 0:
                continue
            mapping[quest_id] = normalize_source_code_raw(
                obj.get("HAHEIAHBPEJ") or obj.get("DLPKMDPABFM")
            )

    _QUEST_SOURCE_RAW_BY_ID = mapping
    return mapping


def extract_main_coop_ids(rows, quest_id: int | None = None) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    if not isinstance(rows, list):
        return result
    for row in rows:
        if not isinstance(row, dict):
            continue
        raw_id = row.get("id")
        if not isinstance(raw_id, int) or raw_id <= 0:
            raw_id = row.get("JLJFKNHFLJP")
        if not isinstance(raw_id, int) or raw_id <= 0:
            continue
        if quest_id is not None and raw_id // 100 != quest_id:
            continue
        if raw_id in seen:
            continue
        seen.add(raw_id)
        result.append(raw_id)
    return result


def load_main_coop_ids_by_quest_id() -> dict[int, list[int]]:
    global _MAIN_COOP_IDS_BY_QUEST_ID
    if _MAIN_COOP_IDS_BY_QUEST_ID is not None:
        return _MAIN_COOP_IDS_BY_QUEST_ID

    mapping: dict[int, list[int]] = {}
    main_coop_path = os.path.join(DATA_PATH, "ExcelBinOutput", "MainCoopExcelConfigData.json")
    if os.path.isfile(main_coop_path):
        rows = load_json_file(main_coop_path, error_msg="Error loading MainCoopExcelConfigData.json", default=[])
        for raw_id in extract_main_coop_ids(rows):
            quest_id = raw_id // 100
            mapping.setdefault(quest_id, []).append(raw_id)

    coop_folder = os.path.join(DATA_PATH, "BinOutput", "Coop")
    if os.path.isdir(coop_folder):
        for file_name in os.listdir(coop_folder):
            match = re.match(r"^Coop(\d+)\.json$", file_name)
            if not match:
                continue
            raw_id = int(match.group(1))
            quest_id = raw_id // 100
            bucket = mapping.setdefault(quest_id, [])
            if raw_id not in bucket:
                bucket.append(raw_id)

    _MAIN_COOP_IDS_BY_QUEST_ID = mapping
    return mapping


def load_hangout_quest_ids() -> set[int]:
    global _HANGOUT_QUEST_IDS
    if _HANGOUT_QUEST_IDS is not None:
        return _HANGOUT_QUEST_IDS

    quest_ids: set[int] = set(load_main_coop_ids_by_quest_id().keys())
    coop_folder = os.path.join(DATA_PATH, "BinOutput", "Coop")
    if os.path.isdir(coop_folder):
        for file_name in os.listdir(coop_folder):
            match = re.match(r"^Coop(\d+)\.json$", file_name)
            if not match:
                continue
            try:
                quest_ids.add(int(match.group(1)) // 100)
            except Exception:
                continue

    _HANGOUT_QUEST_IDS = quest_ids
    return quest_ids


def resolve_quest_source_fields(quest_id: int | None, *, is_anecdote: bool = False) -> tuple[str, str]:
    if is_anecdote:
        return SOURCE_TYPE_ANECDOTE, SOURCE_TYPE_ANECDOTE
    if not isinstance(quest_id, int) or quest_id <= 0:
        return SOURCE_TYPE_UNKNOWN, SOURCE_TYPE_UNKNOWN

    source_code_raw = load_quest_source_raw_by_id().get(quest_id, SOURCE_TYPE_UNKNOWN)
    if quest_id in load_hangout_quest_ids():
        return SOURCE_TYPE_HANGOUT, source_code_raw
    if source_code_raw in BASE_QUEST_SOURCE_TYPES:
        return source_code_raw, source_code_raw
    return SOURCE_TYPE_UNKNOWN, source_code_raw


def get_quest_subquests(obj: dict) -> list[dict]:
    subquests = obj.get("MEGJPCLADOG")
    if not isinstance(subquests, list):
        subquests = obj.get("NLCNGJKMAEN")
    if not isinstance(subquests, list):
        subquests = obj.get("subQuests")
    if not isinstance(subquests, list):
        subquests = obj.get("GFLHMKOOHHA")
    if not isinstance(subquests, list):
        return []
    return [item for item in subquests if isinstance(item, dict)]


def _get_subquest_id(step_obj: dict) -> int | None:
    for key in ("MPKBGPAKIOA", "subId", "KKMJBEPGLGD"):
        value = step_obj.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def get_step_desc_text_map_hash(step_obj: dict) -> int | None:
    for key in (
        "AJGGCMPLKHK",
        "stepDescTextMapHash",
        "OCMKKHHNKJO",
        "BMBANCMPPOM",
        "NAEMBIJFJCA",
        "HMLBMECMBGA",
    ):
        value = step_obj.get(key)
        if isinstance(value, int) and value != 0:
            return value
    return None


def _get_quest_talk_rows(obj: dict) -> list[dict]:
    for key in ("NFFIGDHFAJG", "talks", "IBEGAHMEABP", "DGJMIPFDEOF", "DCHHEHNNEOO"):
        value = obj.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _get_quest_talk_id(talk_obj: dict) -> int | None:
    for key in ("NFIEHACCECI", "id", "ILHDNJDDEOP", "BLKKAMEMBBJ", "BPMABFNPCMI"):
        value = talk_obj.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def _get_talk_start_condition_subquest_ids(talk_obj: dict) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    conditions = talk_obj.get("MPFAEHLBPJE")
    if not isinstance(conditions, list):
        conditions = talk_obj.get("beginCond")
    if not isinstance(conditions, list):
        return result

    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        cond_type = (
            condition.get("_type")
            or condition.get("type")
            or condition.get("HAHEIAHBPEJ")
            or condition.get("DLPKMDPABFM")
        )
        if cond_type != "QUEST_COND_STATE_EQUAL":
            continue
        params = (
            condition.get("_param")
            or condition.get("param")
            or condition.get("paramList")
            or condition.get("AAHAKNIPEDM")
        )
        if not isinstance(params, list) or not params:
            continue
        try:
            sub_id = int(str(params[0]))
        except (TypeError, ValueError):
            continue
        if sub_id > 0 and sub_id not in seen:
            seen.add(sub_id)
            result.append(sub_id)
    return result


def get_step_talk_ids(step_obj: dict) -> list[int]:
    talk_ids: list[int] = []
    seen: set[int] = set()

    conditions = step_obj.get("POPHAFEBKIH")
    if not isinstance(conditions, list):
        conditions = step_obj.get("AACKELGGJGC")
    if not isinstance(conditions, list):
        conditions = step_obj.get("finishCond")
    if not isinstance(conditions, list):
        conditions = step_obj.get("KBFJAAFDHKJ")
    if not isinstance(conditions, list):
        return talk_ids

    for condition in conditions:
        if not isinstance(condition, dict):
            continue
        cond_type = (
            condition.get("HAHEIAHBPEJ")
            or condition.get("DLPKMDPABFM")
            or condition.get("type")
            or condition.get("PAINLIBBLDK")
        )
        if cond_type not in _STEP_TALK_CONDITION_TYPES:
            continue
        params = (
            condition.get("AAHAKNIPEDM")
            or condition.get("IEKGEJMAOCN")
            or condition.get("param")
            or condition.get("paramList")
            or condition.get("LNHLPKELCAL")
        )
        if not isinstance(params, list) or not params:
            continue
        talk_id = params[0]
        if isinstance(talk_id, int) and talk_id > 0 and talk_id not in seen:
            seen.add(talk_id)
            talk_ids.append(talk_id)
    return talk_ids


def build_step_title_hash_by_talk_id(obj: dict) -> dict[int, int]:
    mapping: dict[int, int] = {}
    title_hash_by_subquest_id: dict[int, int] = {}
    for subquest in get_quest_subquests(obj):
        step_hash = get_step_desc_text_map_hash(subquest)
        if not isinstance(step_hash, int) or step_hash == 0:
            continue
        subquest_id = _get_subquest_id(subquest)
        if isinstance(subquest_id, int):
            title_hash_by_subquest_id.setdefault(subquest_id, step_hash)
        for talk_id in get_step_talk_ids(subquest):
            mapping.setdefault(talk_id, step_hash)

    for talk_obj in _get_quest_talk_rows(obj):
        talk_id = _get_quest_talk_id(talk_obj)
        if not isinstance(talk_id, int):
            continue
        if talk_id in title_hash_by_subquest_id:
            mapping.setdefault(talk_id, title_hash_by_subquest_id[talk_id])
        for subquest_id in _get_talk_start_condition_subquest_ids(talk_obj):
            step_hash = title_hash_by_subquest_id.get(subquest_id)
            if isinstance(step_hash, int):
                mapping.setdefault(talk_id, step_hash)
    return mapping


def resolve_main_quest_id_for_subquest(subquest: dict, fallback_quest_id: int | None = None) -> int | None:
    if isinstance(fallback_quest_id, int) and fallback_quest_id > 0:
        return fallback_quest_id
    for key in ("JPBOKMKMHCJ", "mainQuestId", "GNGFBMPFBOK", "JKHGFFKOFFN"):
        value = subquest.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def iter_subquest_talk_rows(obj: dict, fallback_quest_id: int | None = None, *, normal_coop_id: int = 0):
    for subquest in get_quest_subquests(obj):
        main_quest_id = resolve_main_quest_id_for_subquest(subquest, fallback_quest_id)
        if not isinstance(main_quest_id, int) or main_quest_id <= 0:
            continue
        step_hash = get_step_desc_text_map_hash(subquest)
        for talk_id in get_step_talk_ids(subquest):
            yield (main_quest_id, talk_id, step_hash, normal_coop_id)


def iter_talk_excel_config_paths() -> list[str]:
    pattern = os.path.join(DATA_PATH, "ExcelBinOutput", "TalkExcelConfigData*.json")
    return sorted(glob.glob(pattern))


def load_storyboard_talk_excel_by_quest_id() -> dict[int, list[int]]:
    global _STORYBOARD_TALK_EXCEL_BY_QUEST_ID
    if _STORYBOARD_TALK_EXCEL_BY_QUEST_ID is not None:
        return _STORYBOARD_TALK_EXCEL_BY_QUEST_ID

    mapping: dict[int, list[int]] = {}
    seen_pairs: set[tuple[int, int]] = set()
    for path in iter_talk_excel_config_paths():
        rows = load_json_file(path, error_msg=f"Error loading {os.path.basename(path)}", default=[])
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            if row.get("loadType") != "TALK_STORYBOARD":
                continue
            quest_id = row.get("questId")
            talk_id = row.get("id")
            if not isinstance(quest_id, int) or quest_id <= 0:
                continue
            if not isinstance(talk_id, int) or talk_id <= 0:
                continue
            pair = (quest_id, talk_id)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            mapping.setdefault(quest_id, []).append(talk_id)

    _STORYBOARD_TALK_EXCEL_BY_QUEST_ID = mapping
    return mapping


def iter_storyboard_talk_paths() -> list[str]:
    pattern = os.path.join(DATA_PATH, "BinOutput", "Talk", "Storyboard", "*.json")
    return sorted(glob.glob(pattern))


def load_storyboard_file_by_talk_id() -> dict[int, str]:
    global _STORYBOARD_FILE_BY_TALK_ID
    if _STORYBOARD_FILE_BY_TALK_ID is not None:
        return _STORYBOARD_FILE_BY_TALK_ID

    mapping: dict[int, str] = {}
    for path in iter_storyboard_talk_paths():
        try:
            obj = load_json_file(path)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        talk_id = obj.get("AADKDKPMGNO")
        if not isinstance(talk_id, int) or talk_id <= 0:
            talk_id = obj.get("LBPGKDMGFBN")
        if not isinstance(talk_id, int) or talk_id <= 0:
            continue
        rel_path = os.path.relpath(path, DATA_PATH).replace(os.sep, "/")
        mapping.setdefault(talk_id, rel_path)

    _STORYBOARD_FILE_BY_TALK_ID = mapping
    return mapping


def extract_storyboard_group_talk_ids(obj: dict) -> list[int]:
    if not isinstance(obj, dict):
        return []
    items = obj.get("NFFIGDHFAJG")
    if not isinstance(items, list):
        items = obj.get("DGJMIPFDEOF")
    if not isinstance(items, list):
        items = obj.get("talks")
    if not isinstance(items, list):
        return []

    talk_ids: list[int] = []
    seen: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        talk_id = item.get("NFIEHACCECI")
        if not isinstance(talk_id, int) or talk_id <= 0 or talk_id in seen:
            talk_id = item.get("BLKKAMEMBBJ")
        if not isinstance(talk_id, int) or talk_id <= 0 or talk_id in seen:
            continue
        seen.add(talk_id)
        talk_ids.append(talk_id)
    return talk_ids


def extract_anecdote_payload(
    row: dict,
    *,
    talk_excel_map: dict[int, list[int]] | None = None,
    storyboard_file_by_talk_id: dict[int, str] | None = None,
    normal_coop_id: int = 0,
) -> dict | None:
    core_fields = extract_anecdote_core_fields(row)
    if core_fields is None:
        return None

    anecdote_id = core_fields["quest_id"]
    title_text_map_hash = core_fields["title_text_map_hash"]
    desc_text_map_hash = core_fields["desc_text_map_hash"]
    long_desc_text_map_hash = core_fields["long_desc_text_map_hash"]
    group_ids = core_fields["group_ids"]
    if talk_excel_map is None:
        talk_excel_map = load_storyboard_talk_excel_by_quest_id()
    if storyboard_file_by_talk_id is None:
        storyboard_file_by_talk_id = load_storyboard_file_by_talk_id()

    talk_ids: list[int] = []
    seen_talk_ids: set[int] = set()
    mapping_miss_refs: list[str] = []
    group_statuses: list[str] = []
    storyboard_group_root = os.path.join(DATA_PATH, "BinOutput", "Talk", "StoryboardGroup")

    for group_id in group_ids:
        latest_talk_ids = talk_excel_map.get(group_id) or []
        if latest_talk_ids:
            importable_talk_ids = [
                talk_id
                for talk_id in latest_talk_ids
                if isinstance(talk_id, int) and talk_id > 0 and talk_id in storyboard_file_by_talk_id
            ]
            if importable_talk_ids:
                group_statuses.append(ANECDOTE_SOURCE_STATUS_IMPORTABLE)
                for talk_id in importable_talk_ids:
                    if talk_id in seen_talk_ids:
                        continue
                    seen_talk_ids.add(talk_id)
                    talk_ids.append(talk_id)
            else:
                group_statuses.append(ANECDOTE_SOURCE_STATUS_MAPPING_MISS)
                mapping_miss_refs.append(f"{anecdote_id}:{group_id}")
            continue

        group_path = os.path.join(storyboard_group_root, f"{group_id}.json")
        if not os.path.isfile(group_path):
            group_statuses.append(ANECDOTE_SOURCE_STATUS_MAPPING_MISS)
            mapping_miss_refs.append(f"{anecdote_id}:{group_id}")
            continue
        try:
            group_obj = load_json_file(group_path)
        except Exception:
            group_statuses.append(ANECDOTE_SOURCE_STATUS_MAPPING_MISS)
            mapping_miss_refs.append(f"{anecdote_id}:{group_id}")
            continue
        if not isinstance(group_obj, dict):
            group_statuses.append(ANECDOTE_SOURCE_STATUS_MAPPING_MISS)
            mapping_miss_refs.append(f"{anecdote_id}:{group_id}")
            continue
        group_talk_ids = extract_storyboard_group_talk_ids(group_obj)
        if not group_talk_ids:
            group_statuses.append(ANECDOTE_SOURCE_STATUS_MAPPING_MISS)
            mapping_miss_refs.append(f"{anecdote_id}:{group_id}")
            continue
        group_statuses.append(ANECDOTE_SOURCE_STATUS_IMPORTABLE)
        for talk_id in group_talk_ids:
            if talk_id in seen_talk_ids:
                continue
            seen_talk_ids.add(talk_id)
            talk_ids.append(talk_id)

    source_status = ANECDOTE_SOURCE_STATUS_MAPPING_MISS
    if ANECDOTE_SOURCE_STATUS_IMPORTABLE in group_statuses:
        source_status = ANECDOTE_SOURCE_STATUS_IMPORTABLE

    talk_rows = [(talk_id, None, normal_coop_id) for talk_id in sorted(talk_ids)]
    if source_status == ANECDOTE_SOURCE_STATUS_MAPPING_MISS:
        talk_rows = []

    return {
        "quest_id": anecdote_id,
        "title_text_map_hash": title_text_map_hash,
        "desc_text_map_hash": desc_text_map_hash,
        "long_desc_text_map_hash": long_desc_text_map_hash,
        "talk_rows": talk_rows,
        "source_status": source_status,
        "mapping_miss_refs": mapping_miss_refs,
        "source_type": SOURCE_TYPE_ANECDOTE,
        "source_code_raw": SOURCE_TYPE_ANECDOTE,
    }


def extract_nested_text_map_hash(node) -> int | None:
    if isinstance(node, int) and node != 0:
        return node
    if not isinstance(node, dict):
        return None
    for key in ("BKGOAJFIHCO", "AEMBEELBLML", "textMapHash", "hash"):
        value = node.get(key)
        if isinstance(value, int) and value != 0:
            return value
    return None


def load_hangout_codex_hashes(quest_id: int) -> tuple[int | None, int | None]:
    for candidate_id in (quest_id, quest_id + 10000):
        path = os.path.join(DATA_PATH, "BinOutput", "CodexQuest", f"{candidate_id}.json")
        if not os.path.isfile(path):
            continue
        try:
            obj = load_json_file(path)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        title_hash = None
        for key in ("HOBBPLPNMDP", "HCGANIMKKLM"):
            title_hash = extract_nested_text_map_hash(obj.get(key))
            if title_hash is not None:
                break
        desc_hash = None
        for key in ("PIAAIKBBCGB", "NCBJBOHPGNA"):
            desc_hash = extract_nested_text_map_hash(obj.get(key))
            if desc_hash is not None:
                break
        if title_hash is not None or desc_hash is not None:
            return title_hash, desc_hash
    return None, None


def extract_hangout_coop_quest_ids(coop_obj: dict) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    root = coop_obj.get("NPOJHKBJIDO")
    if isinstance(root, dict):
        for raw_key, value in root.items():
            candidates = [raw_key]
            if isinstance(value, dict):
                candidates.append(value.get("BLKKAMEMBBJ"))
            for candidate in candidates:
                try:
                    coop_quest_id = int(candidate)
                except Exception:
                    continue
                if coop_quest_id <= 0 or coop_quest_id in seen:
                    continue
                seen.add(coop_quest_id)
                result.append(coop_quest_id)
    return result


def collect_hangout_talk_rows(
    main_coop_id: int,
    *,
    missing_coop_collector: list[str] | None = None,
) -> list[tuple[int, int | None, int]]:
    coop_path = os.path.join(DATA_PATH, "BinOutput", "Coop", f"Coop{main_coop_id}.json")
    if not os.path.isfile(coop_path):
        if missing_coop_collector is not None:
            missing_coop_collector.append(str(main_coop_id))
        return []
    try:
        coop_obj = load_json_file(coop_path)
    except Exception:
        if missing_coop_collector is not None:
            missing_coop_collector.append(str(main_coop_id))
        return []
    if not isinstance(coop_obj, dict):
        if missing_coop_collector is not None:
            missing_coop_collector.append(str(main_coop_id))
        return []

    talk_rows: list[tuple[int, int | None, int]] = []
    seen_pairs: set[tuple[int, int]] = set()
    talk_root = os.path.join(DATA_PATH, "BinOutput", "Talk", "Coop")
    for coop_quest_id in extract_hangout_coop_quest_ids(coop_obj):
        pattern = os.path.join(talk_root, f"{coop_quest_id}_*.json")
        for talk_path in sorted(glob.glob(pattern)):
            try:
                talk_obj = load_json_file(talk_path)
            except Exception:
                continue
            if not isinstance(talk_obj, dict):
                continue
            talk_id = talk_obj.get("AADKDKPMGNO")
            if not isinstance(talk_id, int) or talk_id <= 0:
                talk_id = talk_obj.get("LBPGKDMGFBN")
            if not isinstance(talk_id, int) or talk_id <= 0:
                continue
            key = (talk_id, coop_quest_id)
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            talk_rows.append((talk_id, None, coop_quest_id))
    return talk_rows


def is_existing_real_hangout_quest(existing_row) -> bool:
    if not existing_row:
        return False
    chapter_id = existing_row[3]
    source_code_raw = existing_row[5]
    return chapter_id is not None or source_code_raw in BASE_QUEST_SOURCE_TYPES


def build_hangout_payload(
    quest_id: int,
    *,
    existing_quest_row,
    missing_coop_collector: list[str] | None = None,
) -> dict | None:
    main_coop_ids = load_main_coop_ids_by_quest_id().get(quest_id)
    if not main_coop_ids:
        return None

    is_real_existing_quest = is_existing_real_hangout_quest(existing_quest_row)
    codex_title_hash, codex_desc_hash = load_hangout_codex_hashes(quest_id)

    talk_rows: list[tuple[int, int | None, int]] = []
    for main_coop_id in main_coop_ids:
        talk_rows.extend(
            collect_hangout_talk_rows(
                main_coop_id,
                missing_coop_collector=missing_coop_collector,
            )
        )

    if is_real_existing_quest:
        title_text_map_hash = existing_quest_row[0] if existing_quest_row[0] not in (0,) else codex_title_hash
        desc_text_map_hash = existing_quest_row[1] if existing_quest_row[1] not in (0,) else codex_desc_hash
        chapter_id = existing_quest_row[3]
        raw_source = existing_quest_row[5]
        source_code_raw = raw_source if raw_source in BASE_QUEST_SOURCE_TYPES else SOURCE_TYPE_HANGOUT
    else:
        title_text_map_hash = codex_title_hash
        desc_text_map_hash = codex_desc_hash
        chapter_id = None
        source_code_raw = SOURCE_TYPE_HANGOUT

    return {
        "quest_id": quest_id,
        "title_text_map_hash": title_text_map_hash,
        "desc_text_map_hash": desc_text_map_hash,
        "long_desc_text_map_hash": None,
        "chapter_id": chapter_id,
        "source_type": SOURCE_TYPE_HANGOUT,
        "source_code_raw": source_code_raw,
        "talk_rows": talk_rows,
        "is_real_existing_quest": is_real_existing_quest,
        "existing_quest_row": existing_quest_row,
    }

"""Quest-source, anecdote, and hangout resolution over injected game data."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Optional

from .access import GameDataAccess
from .compat import CompatibilityProfile, GTS_COMPAT
from .quest import QuestParser
from .talk import TalkRef, extract_first_positive_int, extract_talk_id


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
    SOURCE_TYPE_AQ, SOURCE_TYPE_LQ, SOURCE_TYPE_WQ, SOURCE_TYPE_EQ, SOURCE_TYPE_IQ,
}
LEGACY_HANGOUT_QUEST_ID_MIN = 19001
LEGACY_HANGOUT_QUEST_ID_MAX = 19187


@dataclass(frozen=True)
class AnecdotePayload:
    value: dict[str, Any]


@dataclass(frozen=True)
class HangoutPayload:
    value: dict[str, Any]


def _normalize_positive_ints(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []
    result: list[int] = []
    seen: set[int] = set()
    for item in value:
        try:
            normalized = int(item)
        except (TypeError, ValueError):
            continue
        if normalized <= 0 or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def normalize_source_code_raw(value: Any) -> str:
    if not isinstance(value, str):
        return SOURCE_TYPE_UNKNOWN
    normalized = value.strip().upper()
    return normalized if normalized in BASE_QUEST_SOURCE_TYPES else SOURCE_TYPE_UNKNOWN


def extract_main_coop_ids(rows: Any, quest_id: Optional[int] = None) -> list[int]:
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
        if raw_id not in seen:
            seen.add(raw_id)
            result.append(raw_id)
    return result


def extract_storyboard_group_talk_ids(obj: Any) -> list[int]:
    if not isinstance(obj, dict):
        return []
    items = None
    for key in ("NFFIGDHFAJG", "DGJMIPFDEOF", "talks", "GDDPNNHLGBL"):
        value = obj.get(key)
        if isinstance(value, list):
            items = value
            break
    if items is None:
        return []
    result: list[int] = []
    seen: set[int] = set()
    for item in items:
        talk_id = extract_first_positive_int(item, "NFIEHACCECI", "BLKKAMEMBBJ", "ANKFNLMKOII")
        if talk_id is not None and talk_id not in seen:
            seen.add(talk_id)
            result.append(talk_id)
    return result


def extract_anecdote_core_fields(
    row: Any,
    profile: CompatibilityProfile = GTS_COMPAT,
) -> Optional[dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    anecdote_id = extract_first_positive_int(row, "IDEHFGDCPDL", "GIJOCHMAJCI", "GBDGFHNLDFF", "DBGCFNMLHAJ")
    if anecdote_id is None:
        return None
    title_keys = ["IBGEKMBPNNO", "EHGEFIODFHD", "titleTextMapHash"]
    desc_keys = ["EBDFJDKDDFJ", "OBJANDCNDMA", "descTextMapHash"]
    if profile.anecdote_extended_aliases:
        title_keys[2:2] = ["PPANCKHJOGI", "EJMLGHMLPLD"]
        desc_keys[2:2] = ["AJKAHOPOBJB", "JKNBFACAMCF"]
    story_ids: list[int] = []
    for key in ("MCGGPAGBGKO", "AEEMNELFAIO", "BBOMCGBIOFM", "LIIPHELCPKJ"):
        story_ids = _normalize_positive_ints(row.get(key))
        if story_ids:
            break
    legacy_ids: list[int] = []
    for key in ("BHAGNOEMPHL", "HCFJCJFMPDC"):
        legacy_ids = _normalize_positive_ints(row.get(key))
        if legacy_ids:
            break
    if not legacy_ids and profile.anecdote_story_ids_as_legacy_fallback:
        legacy_ids = list(story_ids)
    result = {
        "quest_id": anecdote_id,
        "title_text_map_hash": extract_first_positive_int(row, *title_keys),
        "desc_text_map_hash": extract_first_positive_int(row, *desc_keys),
        "long_desc_text_map_hash": extract_first_positive_int(row, "OBLBGMIHBHL") if profile.anecdote_extended_aliases else None,
        "story_quest_ids": story_ids,
        "legacy_group_ids": legacy_ids,
    }
    if profile.name == "bwiki":
        result["group_ids"] = story_ids
    return result


def extract_nested_text_map_hash(node: Any) -> Optional[int]:
    if isinstance(node, int) and node != 0:
        return node
    return extract_first_positive_int(node, "BKGOAJFIHCO", "AEMBEELBLML", "textMapHash", "hash")


def extract_hangout_coop_quest_ids(coop_obj: Any) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    if not isinstance(coop_obj, dict):
        return result
    root = coop_obj.get("NPOJHKBJIDO")
    if not isinstance(root, dict):
        return result
    for raw_key, value in root.items():
        candidates = [raw_key]
        if isinstance(value, dict):
            candidates.append(value.get("BLKKAMEMBBJ"))
        for candidate in candidates:
            try:
                coop_id = int(candidate)
            except (TypeError, ValueError):
                continue
            if coop_id > 0 and coop_id not in seen:
                seen.add(coop_id)
                result.append(coop_id)
    return result


class QuestSourceResolver:
    def __init__(
        self,
        access: GameDataAccess,
        profile: CompatibilityProfile = GTS_COMPAT,
        parser: Optional[QuestParser] = None,
    ):
        self.access = access
        self.profile = profile
        self.parser = parser or QuestParser(profile)
        self.clear()

    def clear(self) -> None:
        self._quest_source_raw_by_id: Optional[dict[int, str]] = None
        self._main_coop_ids_by_quest_id: Optional[dict[int, list[int]]] = None
        self._hangout_quest_ids: Optional[set[int]] = None
        self._storyboard_talk_excel_by_quest_id: Optional[dict[int, list[int]]] = None
        self._storyboard_file_by_talk_id: Optional[dict[int, str]] = None

    def load_quest_source_raw_by_id(self) -> dict[int, str]:
        if self._quest_source_raw_by_id is not None:
            return self._quest_source_raw_by_id
        result: dict[int, str] = {}
        for path in self.access.glob_paths("BinOutput/QuestBrief/*.json"):
            obj = self._read_absolute(path)
            quest_id = self.parser.extract_quest_id(obj)
            if not isinstance(quest_id, int) or quest_id <= 0 or not isinstance(obj, dict):
                continue
            result[quest_id] = normalize_source_code_raw(
                obj.get("HAHEIAHBPEJ") or obj.get("DLPKMDPABFM") or obj.get("MEGMIMEDODJ") or obj.get("BPEHONLLNNK")
            )
        self._quest_source_raw_by_id = result
        return result

    def _read_absolute(self, path: str) -> Any:
        roots = getattr(self.access, "roots", ())
        for root in roots:
            try:
                relative = os.path.relpath(path, root).replace(os.sep, "/")
            except ValueError:
                continue
            if not relative.startswith("../"):
                return self.access.read_json(relative)
        try:
            import json
            with open(path, encoding="utf-8-sig") as handle:
                return json.load(handle)
        except Exception:
            return None

    def load_main_coop_ids_by_quest_id(self) -> dict[int, list[int]]:
        if self._main_coop_ids_by_quest_id is not None:
            return self._main_coop_ids_by_quest_id
        result: dict[int, list[int]] = {}
        for raw_id in extract_main_coop_ids(self.access.get_main_coop_excel_config_data() or []):
            result.setdefault(raw_id // 100, []).append(raw_id)
        folder = self.access.get_existing_dir("BinOutput/Coop")
        if folder:
            for file_name in os.listdir(folder):
                match = re.match(r"^Coop(\d+)\.json$", file_name)
                if not match:
                    continue
                raw_id = int(match.group(1))
                bucket = result.setdefault(raw_id // 100, [])
                if raw_id not in bucket:
                    bucket.append(raw_id)
        self._main_coop_ids_by_quest_id = result
        return result

    def load_hangout_quest_ids(self) -> set[int]:
        if self._hangout_quest_ids is None:
            result = set(self.load_main_coop_ids_by_quest_id())
            if self.profile.include_legacy_hangout_range:
                result.update(range(LEGACY_HANGOUT_QUEST_ID_MIN, LEGACY_HANGOUT_QUEST_ID_MAX + 1))
            self._hangout_quest_ids = result
        return self._hangout_quest_ids

    def resolve_quest_source_fields(self, quest_id: Optional[int], *, is_anecdote: bool = False) -> tuple[str, str]:
        if is_anecdote:
            return SOURCE_TYPE_ANECDOTE, SOURCE_TYPE_ANECDOTE
        if not isinstance(quest_id, int) or quest_id <= 0:
            return SOURCE_TYPE_UNKNOWN, SOURCE_TYPE_UNKNOWN
        raw = self.load_quest_source_raw_by_id().get(quest_id, SOURCE_TYPE_UNKNOWN)
        if quest_id in self.load_hangout_quest_ids():
            return SOURCE_TYPE_HANGOUT, raw
        return (raw, raw) if raw in BASE_QUEST_SOURCE_TYPES else (SOURCE_TYPE_UNKNOWN, raw)

    def load_storyboard_talk_excel_by_quest_id(self) -> dict[int, list[int]]:
        if self._storyboard_talk_excel_by_quest_id is not None:
            return self._storyboard_talk_excel_by_quest_id
        result: dict[int, list[int]] = {}
        seen: set[tuple[int, int]] = set()
        for rows in self.access.get_talk_excel_config_data_parts():
            for row in rows:
                if not isinstance(row, dict) or row.get("loadType") != "TALK_STORYBOARD":
                    continue
                quest_id = extract_first_positive_int(row, "questId")
                talk_id = extract_first_positive_int(row, "id")
                if quest_id is None or talk_id is None or (quest_id, talk_id) in seen:
                    continue
                seen.add((quest_id, talk_id))
                result.setdefault(quest_id, []).append(talk_id)
        self._storyboard_talk_excel_by_quest_id = result
        return result

    def load_storyboard_file_by_talk_id(self) -> dict[int, str]:
        if self._storyboard_file_by_talk_id is not None:
            return self._storyboard_file_by_talk_id
        result: dict[int, str] = {}
        for path in self.access.glob_paths("BinOutput/Talk/Storyboard/*.json"):
            obj = self._read_absolute(path)
            talk_id = extract_first_positive_int(obj, "AADKDKPMGNO", "LDLMECNIJFC", "LBPGKDMGFBN")
            if talk_id is None:
                stem = os.path.splitext(os.path.basename(path))[0]
                talk_id = int(stem) if stem.isdigit() else None
            if isinstance(talk_id, int):
                relative = path
                for root in getattr(self.access, "roots", ()):
                    candidate = os.path.relpath(path, root).replace(os.sep, "/")
                    if not candidate.startswith("../"):
                        relative = candidate
                        break
                result.setdefault(talk_id, relative)
        self._storyboard_file_by_talk_id = result
        return result

    def extract_anecdote_payload(
        self,
        row: Any,
        *,
        talk_excel_map: Optional[dict[int, list[int]]] = None,
        storyboard_file_by_talk_id: Optional[dict[int, str]] = None,
        normal_coop_id: int = 0,
    ) -> Optional[dict[str, Any]]:
        core = extract_anecdote_core_fields(row, self.profile)
        if core is None:
            return None
        talk_excel_map = talk_excel_map if talk_excel_map is not None else self.load_storyboard_talk_excel_by_quest_id()
        storyboard_file_by_talk_id = storyboard_file_by_talk_id if storyboard_file_by_talk_id is not None else self.load_storyboard_file_by_talk_id()
        talk_ids: list[int] = []
        seen: set[int] = set()
        misses: list[str] = []
        statuses: list[str] = []

        def collect_group(group_id: int) -> None:
            mapped = talk_excel_map.get(group_id) or []
            if mapped:
                found = [talk_id for talk_id in mapped if talk_id in storyboard_file_by_talk_id or self.access.get_talk_storyboard_candidate(talk_id) is not None]
                if found:
                    statuses.append(ANECDOTE_SOURCE_STATUS_IMPORTABLE)
                    for talk_id in found:
                        if talk_id not in seen:
                            seen.add(talk_id)
                            talk_ids.append(talk_id)
                else:
                    statuses.append(ANECDOTE_SOURCE_STATUS_MAPPING_MISS)
                    misses.append(f"{core['quest_id']}:{group_id}")
                return
            group_obj = self.access.get_talk_storyboard_group_output(group_id)
            found = extract_storyboard_group_talk_ids(group_obj)
            if found:
                statuses.append(ANECDOTE_SOURCE_STATUS_IMPORTABLE)
                for talk_id in found:
                    if talk_id not in seen:
                        seen.add(talk_id)
                        talk_ids.append(talk_id)
            else:
                statuses.append(ANECDOTE_SOURCE_STATUS_MAPPING_MISS)
                misses.append(f"{core['quest_id']}:{group_id}")

        primary = core["story_quest_ids"] or core.get("group_ids", [])
        for group_id in primary:
            collect_group(group_id)
        if ANECDOTE_SOURCE_STATUS_IMPORTABLE not in statuses:
            for group_id in core["legacy_group_ids"]:
                if group_id not in primary:
                    collect_group(group_id)
        status = ANECDOTE_SOURCE_STATUS_IMPORTABLE if ANECDOTE_SOURCE_STATUS_IMPORTABLE in statuses else ANECDOTE_SOURCE_STATUS_MAPPING_MISS
        return {
            "quest_id": core["quest_id"],
            "title_text_map_hash": core["title_text_map_hash"],
            "desc_text_map_hash": core["desc_text_map_hash"],
            "long_desc_text_map_hash": core["long_desc_text_map_hash"],
            "talk_ids": talk_ids,
            "talk_rows": [(talk_id, None, normal_coop_id) for talk_id in talk_ids] if status == ANECDOTE_SOURCE_STATUS_IMPORTABLE else [],
            "source_status": status,
            "mapping_miss_refs": misses,
            "source_type": SOURCE_TYPE_ANECDOTE,
            "source_code_raw": SOURCE_TYPE_ANECDOTE,
        }

    def load_hangout_codex_hashes(self, quest_id: int) -> tuple[Optional[int], Optional[int]]:
        for candidate_id in (quest_id, quest_id + 10000):
            obj = self.access.get_codex_quest_output(candidate_id)
            if not isinstance(obj, dict):
                continue
            title = extract_nested_text_map_hash(obj.get("HCGANIMKKLM"))
            desc = extract_nested_text_map_hash(obj.get("NCBJBOHPGNA"))
            if title is not None or desc is not None:
                return title, desc
        return None, None

    def collect_hangout_talk_refs(self, main_coop_id: int, *, missing_coop_collector: Optional[list[str]] = None) -> list[TalkRef]:
        coop_obj = self.access.get_coop_output(main_coop_id)
        if not isinstance(coop_obj, dict):
            if missing_coop_collector is not None:
                missing_coop_collector.append(str(main_coop_id))
            return []
        result: list[TalkRef] = []
        seen: set[tuple[int, int]] = set()
        for coop_quest_id in extract_hangout_coop_quest_ids(coop_obj):
            for path in self.access.glob_paths(f"BinOutput/Talk/Coop/{coop_quest_id}_*.json"):
                file_stem = os.path.splitext(os.path.basename(path))[0]
                talk_obj = self.access.get_talk_coop_output(file_stem)
                talk_id = extract_talk_id(talk_obj)
                if talk_id is None or (talk_id, coop_quest_id) in seen:
                    continue
                seen.add((talk_id, coop_quest_id))
                result.append(TalkRef("coop", talk_id, coop_quest_id, file_stem=file_stem))
        return result

    def build_hangout_payload(
        self,
        quest_id: int,
        *,
        existing_quest_row: Any = None,
        missing_coop_collector: Optional[list[str]] = None,
        title_text_map_hash: Optional[int] = None,
        desc_text_map_hash: Optional[int] = None,
        chapter_id: Optional[int] = None,
        source_code_raw: str = SOURCE_TYPE_UNKNOWN,
    ) -> Optional[dict[str, Any]]:
        main_coop_ids = self.load_main_coop_ids_by_quest_id().get(quest_id)
        if not main_coop_ids:
            return None
        codex_title, codex_desc = self.load_hangout_codex_hashes(quest_id)
        refs: list[TalkRef] = []
        for main_coop_id in main_coop_ids:
            refs.extend(self.collect_hangout_talk_refs(main_coop_id, missing_coop_collector=missing_coop_collector))
        is_real_existing = bool(existing_quest_row and (existing_quest_row[3] is not None or existing_quest_row[5] in BASE_QUEST_SOURCE_TYPES))
        if is_real_existing:
            title_text_map_hash = existing_quest_row[0] if existing_quest_row[0] not in (0, None) else codex_title
            desc_text_map_hash = existing_quest_row[1] if existing_quest_row[1] not in (0, None) else codex_desc
            chapter_id = existing_quest_row[3]
            raw = existing_quest_row[5]
            source_code_raw = raw if raw in BASE_QUEST_SOURCE_TYPES else SOURCE_TYPE_HANGOUT
        else:
            title_text_map_hash = title_text_map_hash or codex_title
            desc_text_map_hash = desc_text_map_hash or codex_desc
            source_code_raw = source_code_raw if source_code_raw in BASE_QUEST_SOURCE_TYPES else SOURCE_TYPE_HANGOUT
        return {
            "quest_id": quest_id,
            "title_text_map_hash": title_text_map_hash,
            "desc_text_map_hash": desc_text_map_hash,
            "long_desc_text_map_hash": None,
            "chapter_id": chapter_id,
            "source_type": SOURCE_TYPE_HANGOUT,
            "source_code_raw": source_code_raw,
            "talk_refs": [ref.__dict__ for ref in refs],
            "talk_rows": [(ref.talk_id, None, int(ref.coop_quest_id or 0)) for ref in refs],
            "is_real_existing_quest": is_real_existing,
            "existing_quest_row": existing_quest_row,
        }


def iter_subquest_talk_rows(
    obj: Any,
    fallback_quest_id: Optional[int] = None,
    *,
    normal_coop_id: int = 0,
    parser: Optional[QuestParser] = None,
):
    parser = parser or QuestParser(GTS_COMPAT)
    for subquest in parser.get_quest_subquests(obj):
        quest_id = extract_first_positive_int(subquest, "JPBOKMKMHCJ", "mainQuestId", "CBOGAFHNHNI", "PHPKOAIPNFO") or fallback_quest_id
        step_hash = parser.get_step_desc_text_map_hash(subquest)
        for talk_id in parser.get_step_talk_ids(subquest):
            yield quest_id, talk_id, step_hash, normal_coop_id

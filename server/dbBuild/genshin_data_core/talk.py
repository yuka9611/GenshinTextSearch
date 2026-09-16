"""Talk-file models and schema-independent object recognition."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Tuple

from .compat import CompatibilityProfile, GTS_COMPAT


@dataclass(frozen=True)
class TalkCandidate:
    scope: str
    talk_id: int
    coop_quest_id: Optional[int] = None
    file_stem: Optional[str] = None
    relative_path: Optional[str] = None

    def cache_key(self) -> Tuple[str, int, int, str, str]:
        return (
            self.scope,
            int(self.talk_id),
            int(self.coop_quest_id or 0),
            self.file_stem or "",
            self.relative_path or "",
        )


@dataclass(frozen=True)
class TalkRef:
    scope: str
    talk_id: int
    coop_quest_id: Optional[int] = None
    file_stem: Optional[str] = None
    relative_path: Optional[str] = None


def extract_first_positive_int(obj: Any, *keys: str) -> Optional[int]:
    if not isinstance(obj, dict):
        return None
    for key in keys:
        value = obj.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def extract_talk_id(obj: Any) -> Optional[int]:
    if not isinstance(obj, dict):
        return None
    key_groups = (
        ("talkId", "dialogList"),
        ("ADHLLDAPKCM", "MOEOFGCKILF"),
        ("FEOACBMDCKJ", "AAOAAFLLOJI"),
        ("LBPGKDMGFBN", "LOJEOMAPIIM"),
        ("AADKDKPMGNO", "GALIDJOEHOC"),
        ("KFCNJPJOJLA", "IOEDPLCPFFB"),
        ("LDLMECNIJFC", "GDDPNNHLGBL"),
        ("IOKNFDJFGDH", "PFALHAKIILD"),
    )
    for talk_id_key, dialogue_list_key in key_groups:
        if talk_id_key in obj and dialogue_list_key in obj:
            value = obj.get(talk_id_key)
            if isinstance(value, int) and value > 0:
                return value
    return None


def parse_coop_file_stem(file_stem: str) -> Optional[int]:
    match = re.fullmatch(r"(\d+)_\d+", file_stem)
    return int(match.group(1)) if match else None


def is_non_dialog_talk_obj(
    obj: Any,
    profile: CompatibilityProfile = GTS_COMPAT,
) -> bool:
    if not isinstance(obj, dict):
        return False
    keys = set(obj.keys())
    if keys == {"activityId", "talks"}:
        return True
    if "talks" in obj and isinstance(obj.get("talks"), list):
        return True
    # 7.0 ActivityGroup files contain embedded Talk rows under OJACLOOEAMG;
    # they are containers, not dialogue payloads.  The same field in a Quest
    # file is handled by QuestParser as embedded Talk references.
    if "OJACLOOEAMG" in obj and isinstance(obj.get("OJACLOOEAMG"), list):
        return True
    # A schema-shaped object with no positive Talk id or no dialogue rows is
    # a generated placeholder, not an importable Talk payload.
    if (
        "IOKNFDJFGDH" in obj
        and "PFALHAKIILD" in obj
        and (
            not isinstance(obj.get("IOKNFDJFGDH"), int)
            or obj.get("IOKNFDJFGDH", 0) <= 0
            or not isinstance(obj.get("PFALHAKIILD"), list)
            or not obj.get("PFALHAKIILD")
        )
    ):
        return True
    # Other Talk directories contain containers whose children are embedded
    # Talk rows; their shape is distinct from the dialogue-list schemas above.
    if "NFFIGDHFAJG" in obj and isinstance(obj.get("NFFIGDHFAJG"), list):
        return True
    if "PBAEPDPNKEJ" in obj and "KJNKFMPAGAA" in obj and isinstance(obj.get("KJNKFMPAGAA"), list):
        return True
    if "JDOFKFPHIDC" in obj and "PCNNNPLAEAI" in obj and isinstance(obj.get("PCNNNPLAEAI"), list):
        return True
    if "DMIMNILOLKP" in obj and isinstance(obj.get("DMIMNILOLKP"), list):
        return True
    if "ANKFNLMKOII" in obj and "GIIPBNJFFAK" in obj and isinstance(obj.get("FMEEPGFAKOL"), list):
        return True
    if set(obj) == {"talkId", "type"}:
        return True
    if "FEOACBMDCKJ" in obj and "JNMCHAGDLOL" in obj:
        return True
    if "damageRatio" in obj and "talkId" in obj:
        return True
    if "defaultVocalBoneName" in obj:
        return True
    if "DGJMIPFDEOF" in obj and isinstance(obj.get("DGJMIPFDEOF"), list):
        if any(key in obj for key in (
            "CAKFHGJGEEK", "BLPHCANGKPL", "EOFLGOBJBCG",
            "configId", "groupId", "npcId",
        )):
            return True
    if (
        profile.detect_legacy_storyboard_container
        and keys == {"ANCLPHMACIF", "CIAOBJHFJJM"}
        and isinstance(obj.get("CIAOBJHFJJM"), list)
    ):
        return True
    if "DLPKMDPABFM" in obj and "LBPGKDMGFBN" in obj:
        if not isinstance(obj.get("LOJEOMAPIIM"), list):
            return True
    if "AFKIEPNELHE" in obj and "IKCBIFLCCOH" in obj and "PDFCHAAMEHA" in obj:
        return True
    if "AFNAKLCPGNF" in obj and "speed" in obj and "maxSpeed" in obj:
        return True
    if "FDAAMLIPKAK" in obj and "reApplyModifierOnStateChange" in obj:
        return True
    return False


# (talk id, dialogue list, dialogue id, talk role, role type, role id, text hash)
# Keep this schema table shared by imports, coverage audits, and BwikiHelper's
# compatibility loader so a new upstream Talk shape cannot silently diverge.
TALK_DIALOGUE_SCHEMAS = (
    ("talkId", "dialogList", "id", "talkRole", "type", "_id", "talkContentTextMapHash"),
    ("ADHLLDAPKCM", "MOEOFGCKILF", "ILHDNJDDEOP", "LCECPDILLEE", "_type", "_id", "GABLFFECBDO"),
    ("FEOACBMDCKJ", "AAOAAFLLOJI", "CCFPGAKINNB", "HJLEMJIGNFE", "type", "id", "BDOKCLNNDGN"),
    ("LBPGKDMGFBN", "LOJEOMAPIIM", "BLKKAMEMBBJ", "HJIPOJOECIF", "_type", "_id", "CMKPOJOEHHA"),
    ("AADKDKPMGNO", "GALIDJOEHOC", "NFIEHACCECI", "PIBKEGJOJHN", "_type", "_id", "AIGJBMCHCJG"),
    ("KFCNJPJOJLA", "IOEDPLCPFFB", "GMOMCKNPBGE", "DGGDDIMMIDO", "_type", "_id", "HJJLLECCCPI"),
    ("LDLMECNIJFC", "GDDPNNHLGBL", "ANKFNLMKOII", "EENIFNIGHCH", "_type", "_id", "DMIFDJDEFAL"),
    ("IOKNFDJFGDH", "PFALHAKIILD", "OIFGMOHKPOI", "LFGCLNLPAPB", "_type", "_id", "OACNIBLFFDI"),
)

# The compatibility table is ordered by discovery history, not source
# freshness.  Keep freshness explicit so the 7.0 IOKN/PFAL schema wins over
# the older hashed AADK/GALI schema when both describe one dialogue id.
TALK_DIALOGUE_SCHEMA_PRIORITY = {
    0: 0,  # readable talkId/dialogList shape
    6: 1,  # current 7.0 IOKNFDJFGDH/PFALHAKIILD shape
    5: 2,
    4: 3,
    3: 4,
    2: 5,
    1: 6,
}


def extract_talk_dialogue_payload_with_schema(obj: Any):
    """Return ``(schema_rank, talk_id, rows)`` for a dialogue payload.

    The rank follows :data:`TALK_DIALOGUE_SCHEMA_PRIORITY`; lower ranks are
    newer and therefore win when old and new source files describe the same scoped
    dialogue id.  This metadata is intentionally kept out of
    :func:`extract_talk_dialogue_payload` so its public return contract remains
    compatible with existing importers and callers.
    The returned rows are ``(dialogue_id, text_hash, talker_id, talker_type)``.
    A recognized but empty/non-text file returns an empty row list; an
    unrecognized or intentional container returns ``None``.
    """
    if not isinstance(obj, dict) or is_non_dialog_talk_obj(obj):
        return None
    for schema_rank, (
        talk_id_key,
        dialogue_list_key,
        dialogue_id_key,
        talk_role_key,
        talk_role_type_key,
        talk_role_id_key,
        text_hash_key,
    ) in enumerate(TALK_DIALOGUE_SCHEMAS):
        if talk_id_key not in obj or dialogue_list_key not in obj:
            continue
        try:
            talk_id = int(obj.get(talk_id_key))
        except (TypeError, ValueError):
            return None
        raw_dialogues = obj.get(dialogue_list_key)
        if not isinstance(raw_dialogues, list):
            return talk_id, []
        rows = []
        for dialogue in raw_dialogues:
            if not isinstance(dialogue, dict):
                continue
            dialogue_id = dialogue.get(dialogue_id_key)
            if dialogue_id is None or text_hash_key not in dialogue:
                continue
            talk_role = dialogue.get(talk_role_key)
            if (
                isinstance(talk_role, dict)
                and talk_role_id_key in talk_role
                and talk_role_type_key in talk_role
            ):
                talker_id = talk_role[talk_role_id_key]
                talker_type = talk_role[talk_role_type_key]
            else:
                talker_id = -1
                talker_type = None
            rows.append(
                (dialogue_id, dialogue[text_hash_key], talker_id, talker_type)
            )
        return TALK_DIALOGUE_SCHEMA_PRIORITY.get(schema_rank, schema_rank), talk_id, rows
    return None


def extract_talk_dialogue_payload(obj: Any):
    """Extract a Talk id and rows while preserving the historical API."""
    parsed = extract_talk_dialogue_payload_with_schema(obj)
    if parsed is None:
        return None
    _schema_rank, talk_id, rows = parsed
    return talk_id, rows


def normalize_talk_dialogue_rows(
    candidates: Iterable[tuple[int, Iterable[tuple[Any, Any, Any, Any]]]],
    *,
    valid_text_hashes: set[int] | None = None,
) -> list[tuple[Any, int, Any, Any]]:
    """Normalize dialogue rows collected from one ``(talk, coop)`` scope.

    Candidates are ``(schema_rank, rows)`` pairs.  For each dialogue id, only
    the best (newest) schema rank is retained, but every distinct valid hash
    from that rank is preserved.  This is important for source directories
    which intentionally carry different payloads under the same dialogue id.

    When ``valid_text_hashes`` is supplied, ``NULL``, zero, non-integer hashes,
    and hashes absent from the current TextMap are discarded.  Import code
    uses the same routine without the optional set before TextMap is loaded;
    the database cleanup pass applies the authoritative SQL-side filter later.
    """
    grouped: dict[int, list[tuple[int, int, Any, Any]]] = {}
    for schema_rank, rows in candidates:
        try:
            rank = int(schema_rank)
        except (TypeError, ValueError):
            continue
        for row in rows:
            if not isinstance(row, (tuple, list)) or len(row) != 4:
                continue
            dialogue_id, text_hash, talker_id, talker_type = row
            try:
                normalized_dialogue_id = int(dialogue_id)
                normalized_hash = int(text_hash)
            except (TypeError, ValueError):
                continue
            if normalized_dialogue_id <= 0 or normalized_hash == 0:
                continue
            if valid_text_hashes is not None and normalized_hash not in valid_text_hashes:
                continue
            try:
                normalized_talker_id = int(talker_id)
            except (TypeError, ValueError):
                normalized_talker_id = talker_id
            grouped.setdefault(normalized_dialogue_id, []).append(
                (rank, normalized_hash, normalized_talker_id, talker_type)
            )

    normalized: list[tuple[Any, int, Any, Any]] = []
    for dialogue_id, rows in grouped.items():
        best_rank = min(row[0] for row in rows)
        seen: set[tuple[int, str, str]] = set()
        for _rank, text_hash, talker_id, talker_type in rows:
            if _rank != best_rank:
                continue
            marker = (text_hash, repr(talker_id), repr(talker_type))
            if marker in seen:
                continue
            seen.add(marker)
            normalized.append((dialogue_id, text_hash, talker_id, talker_type))
    normalized.sort(key=lambda row: (int(row[0]), int(row[1]), repr(row[2]), repr(row[3])))
    return normalized

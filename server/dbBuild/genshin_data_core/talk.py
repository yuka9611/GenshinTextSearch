"""Talk-file models and schema-independent object recognition."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Tuple

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


def extract_talk_dialogue_payload(obj: Any):
    """Extract a Talk id and text-bearing dialogue rows without database I/O.

    The returned rows are ``(dialogue_id, text_hash, talker_id, talker_type)``.
    A recognized but empty/non-text file returns an empty row list; an
    unrecognized or intentional container returns ``None``.
    """
    if not isinstance(obj, dict) or is_non_dialog_talk_obj(obj):
        return None
    for (
        talk_id_key,
        dialogue_list_key,
        dialogue_id_key,
        talk_role_key,
        talk_role_type_key,
        talk_role_id_key,
        text_hash_key,
    ) in TALK_DIALOGUE_SCHEMAS:
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
        return talk_id, rows
    return None

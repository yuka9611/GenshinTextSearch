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

"""Quest schema parsing shared by importers and wiki generators."""

from __future__ import annotations

from typing import Any, NamedTuple, Optional

from .compat import BWIKI_COMPAT, GTS_COMPAT, CompatibilityProfile
from .talk import extract_first_positive_int


class QuestFields(NamedTuple):
    quest_id: int
    title_text_map_hash: Optional[int]
    chapter_id: Optional[int]


_QUEST_SCHEMA_VARIANTS: tuple[dict[str, Any], ...] = (
    {"quest_id": "NFIEHACCECI", "title_hash": "BPNEONFJEEO", "chapter_keys": ("BALAIBAGIEL",), "talks_key": "NFFIGDHFAJG", "talk_id": "NFIEHACCECI"},
    {"quest_id": "id", "title_hash": "titleTextMapHash", "chapter_keys": ("chapterId",), "talks_key": "talks", "talk_id": "id"},
    {"quest_id": "ILHDNJDDEOP", "title_hash": "MMOEEOFGHHG", "chapter_keys": ("IBNCKLKHAKG",), "talks_key": "IBEGAHMEABP", "talk_id": "ILHDNJDDEOP"},
    {"quest_id": "BLKKAMEMBBJ", "title_hash": "DMLOMLNJCNA", "chapter_keys": ("KDKGIPFDENG",), "talks_key": "DGJMIPFDEOF", "talk_id": "BLKKAMEMBBJ"},
    {"quest_id": "FJIMHCGKKPJ", "title_hash": "HMPOGBDMBOK", "chapter_keys": ("NKEKKINIKEB", "ODOCBCAGDJA"), "talks_key": "DCHHEHNNEOO", "talk_id": "BPMABFNPCMI"},
    {"quest_id": "GMOMCKNPBGE", "title_hash": "ALLMCLJBBDM", "chapter_keys": None, "talks_key": "EOHJIHHMBAN", "talk_id": None},
    {"quest_id": "ANKFNLMKOII", "title_hash": "OCCBMCOGDOO", "chapter_keys": ("JBDLGLCIOHM", "HONEAMECBEN"), "talks_key": "GDDPNNHLGBL", "talk_id": "ANKFNLMKOII"},
    {"quest_id": "OIFGMOHKPOI", "title_hash": "LKAHEACOLML", "chapter_keys": ("CKMCPKCGFKC", "BEADANLODNC"), "talks_key": "PFALHAKIILD", "talk_id": "OIFGMOHKPOI"},
)

_STEP_TALK_CONDITION_TYPES = {
    "QUEST_CONTENT_COMPLETE_TALK",
    "QUEST_CONTENT_FINISH_PLOT",
}


def _nonzero_int(value: Any) -> Optional[int]:
    return value if isinstance(value, int) and value != 0 else None


class QuestParser:
    def __init__(self, profile: CompatibilityProfile = GTS_COMPAT):
        self.profile = profile

    def _schema(self, obj: Any) -> Optional[dict[str, Any]]:
        if not isinstance(obj, dict):
            return None
        for schema in _QUEST_SCHEMA_VARIANTS:
            if schema["quest_id"] in obj:
                return schema
        return None

    def extract_quest_row(self, obj: Any) -> Optional[QuestFields]:
        if not isinstance(obj, dict):
            return None
        schema = self._schema(obj)
        if schema is None:
            if self.profile.allow_subquest_id_fallback:
                for subquest in self.get_quest_subquests(obj):
                    quest_id = extract_first_positive_int(subquest, "PHPKOAIPNFO")
                    if quest_id is not None:
                        return QuestFields(quest_id, None, None)
            return None
        quest_id = obj.get(schema["quest_id"])
        if not isinstance(quest_id, int):
            return None
        if self.profile.name == "bwiki" and quest_id <= 0:
            return None
        title_hash = _nonzero_int(obj.get(schema["title_hash"]))
        chapter_keys = schema["chapter_keys"]
        if chapter_keys is None:
            chapter_keys = self.profile.quest_brief_66_chapter_keys
        chapter_id = None
        for key in chapter_keys:
            chapter_id = _nonzero_int(obj.get(key))
            if chapter_id is not None:
                break
        return QuestFields(quest_id, title_hash, chapter_id)

    def extract_quest_id(self, obj: Any) -> Optional[int]:
        row = self.extract_quest_row(obj)
        return row.quest_id if row is not None else None

    def extract_quest_talk_ids(self, obj: Any) -> list[int]:
        if not isinstance(obj, dict):
            return []
        result: list[int] = []
        seen: set[int] = set()

        def collect_talk_rows(
            talks: Any,
            talk_id_keys: tuple[str, ...],
            *,
            allow_int: bool = False,
            dedupe: bool = True,
        ) -> None:
            if not isinstance(talks, list):
                return
            for talk in talks:
                if allow_int and isinstance(talk, int):
                    talk_id = talk if talk > 0 else None
                elif isinstance(talk, dict):
                    talk_id = extract_first_positive_int(talk, *talk_id_keys)
                else:
                    talk_id = None
                if talk_id is None or (dedupe and talk_id in seen):
                    continue
                if dedupe:
                    seen.add(talk_id)
                result.append(talk_id)

        schema = self._schema(obj)
        talks = obj.get(schema["talks_key"]) if schema is not None else None
        if not isinstance(talks, list) and self.profile.allow_subquest_id_fallback:
            talks = obj.get("GDDPNNHLGBL")
            if not isinstance(talks, list):
                talks = obj.get("PFALHAKIILD")
        if isinstance(talks, list):
            talk_id_key = schema["talk_id"] if schema is not None else "ANKFNLMKOII"
            collect_talk_rows(
                talks,
                tuple(
                    key
                    for key in (
                        talk_id_key,
                        "NFIEHACCECI",
                        "BLKKAMEMBBJ",
                        "ILHDNJDDEOP",
                        "BPMABFNPCMI",
                        "ANKFNLMKOII",
                        "OIFGMOHKPOI",
                        "id",
                    )
                    if isinstance(key, str)
                ),
                allow_int=schema is not None and schema["talk_id"] is None,
                dedupe=self.profile.dedupe_top_level_talk_ids,
            )

        # Quest files also carry embedded Talk rows.  They are separate from
        # the top-level dialogue list and changed from OBPMJEILMMK to
        # OJACLOOEAMG in 7.0.
        for embedded_key in ("OBPMJEILMMK", "OJACLOOEAMG"):
            collect_talk_rows(obj.get(embedded_key), ("ANKFNLMKOII", "OIFGMOHKPOI"))

        if not result:
            return []
        return result

    @staticmethod
    def get_quest_subquests(obj: Any) -> list[dict[str, Any]]:
        if not isinstance(obj, dict):
            return []
        for key in ("MEGJPCLADOG", "NLCNGJKMAEN", "subQuests", "GFLHMKOOHHA", "IKECHKLEFFK", "HLCINEMBGEF", "EBNBLBEIFFJ"):
            value = obj.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    @staticmethod
    def get_step_desc_text_map_hash(step_obj: Any) -> Optional[int]:
        return extract_first_positive_int(
            step_obj, "AJGGCMPLKHK", "stepDescTextMapHash", "OCMKKHHNKJO",
            "BMBANCMPPOM", "NAEMBIJFJCA", "HMLBMECMBGA", "JDFENJAFCPF", "BMEACBBPBGK", "OANENPOPCFO",
        )

    @staticmethod
    def get_step_sub_id(step_obj: Any) -> Optional[int]:
        return extract_first_positive_int(step_obj, "MPKBGPAKIOA", "subId", "KKMJBEPGLGD", "LAFBPKMMBHD", "NDOFAOCKPGE", "KCGAKLCHDCC")

    @staticmethod
    def get_step_order(step_obj: Any) -> Optional[int]:
        return extract_first_positive_int(step_obj, "EDICBFEMNNF", "DGINIFCGMGL", "EDPMKKJIKCJ", "order")

    @staticmethod
    def get_step_talk_ids(step_obj: Any) -> list[int]:
        if not isinstance(step_obj, dict):
            return []
        conditions = None
        for key in ("POPHAFEBKIH", "AACKELGGJGC", "finishCond", "KBFJAAFDHKJ", "PGELADPAKLA", "FCBEKGAHMPD", "ANBEKNMDKCH"):
            value = step_obj.get(key)
            if isinstance(value, list):
                conditions = value
                break
        if conditions is None:
            return []
        result: list[int] = []
        seen: set[int] = set()
        for condition in conditions:
            if not isinstance(condition, dict):
                continue
            cond_type = next((condition.get(key) for key in (
                "HAHEIAHBPEJ", "DLPKMDPABFM", "type", "PAINLIBBLDK", "MEGMIMEDODJ", "BPEHONLLNNK", "ALBFHGKNMLK",
            ) if condition.get(key)), None)
            if cond_type not in _STEP_TALK_CONDITION_TYPES:
                continue
            params = next((condition.get(key) for key in (
                "AAHAKNIPEDM", "IEKGEJMAOCN", "param", "paramList", "LNHLPKELCAL", "KFDJJBPNIHG", "PALPAGCBFDI", "OPDGHDAADJC",
            ) if isinstance(condition.get(key), list)), None)
            if not params:
                continue
            talk_id = params[0]
            if isinstance(talk_id, int) and talk_id > 0 and talk_id not in seen:
                seen.add(talk_id)
                result.append(talk_id)
        return result

    def _talk_rows(self, obj: Any) -> list[dict[str, Any]]:
        if not isinstance(obj, dict):
            return []
        schema = self._schema(obj)
        initial = obj.get(schema["talks_key"]) if schema is not None else None
        if isinstance(initial, list):
            return [item for item in initial if isinstance(item, dict)]
        for key in ("NFFIGDHFAJG", "talks", "IBEGAHMEABP", "DGJMIPFDEOF", "DCHHEHNNEOO", "GDDPNNHLGBL", "PFALHAKIILD"):
            value = obj.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    def _talk_id(self, obj: dict[str, Any], schema: Optional[dict[str, Any]]) -> Optional[int]:
        keys: list[str] = []
        if schema is not None and isinstance(schema.get("talk_id"), str):
            keys.append(schema["talk_id"])
        keys.extend(("NFIEHACCECI", "id", "ILHDNJDDEOP", "BLKKAMEMBBJ", "BPMABFNPCMI", "ANKFNLMKOII", "OIFGMOHKPOI"))
        return extract_first_positive_int(obj, *keys)

    @staticmethod
    def _talk_start_subquest_ids(talk_obj: dict[str, Any]) -> list[int]:
        conditions = None
        for key in ("MPFAEHLBPJE", "beginCond", "BLCEJLFCFPH"):
            value = talk_obj.get(key)
            if isinstance(value, list):
                conditions = value
                break
        if conditions is None:
            return []
        result: list[int] = []
        seen: set[int] = set()
        for condition in conditions:
            if not isinstance(condition, dict):
                continue
            cond_type = next((condition.get(key) for key in (
                "_type", "type", "HAHEIAHBPEJ", "DLPKMDPABFM", "BPEHONLLNNK", "ALBFHGKNMLK",
            ) if condition.get(key)), None)
            if cond_type != "QUEST_COND_STATE_EQUAL":
                continue
            params = next((condition.get(key) for key in (
                "_param", "param", "paramList", "AAHAKNIPEDM", "PALPAGCBFDI", "OPDGHDAADJC",
            ) if isinstance(condition.get(key), list)), None)
            if not params:
                continue
            try:
                subquest_id = int(str(params[0]))
            except (TypeError, ValueError):
                continue
            if subquest_id > 0 and subquest_id not in seen:
                seen.add(subquest_id)
                result.append(subquest_id)
        return result

    def build_step_title_hash_by_talk_id(self, obj: Any) -> dict[int, int]:
        mapping: dict[int, int] = {}
        title_by_subquest: dict[int, int] = {}
        for subquest in self.get_quest_subquests(obj):
            step_hash = self.get_step_desc_text_map_hash(subquest)
            if step_hash is None:
                continue
            subquest_id = self.get_step_sub_id(subquest)
            if subquest_id is not None:
                title_by_subquest.setdefault(subquest_id, step_hash)
            for talk_id in self.get_step_talk_ids(subquest):
                mapping.setdefault(talk_id, step_hash)
        schema = self._schema(obj)
        for talk_obj in self._talk_rows(obj):
            talk_id = self._talk_id(talk_obj, schema)
            if talk_id is None:
                continue
            if talk_id in title_by_subquest:
                mapping.setdefault(talk_id, title_by_subquest[talk_id])
            for subquest_id in self._talk_start_subquest_ids(talk_obj):
                step_hash = title_by_subquest.get(subquest_id)
                if step_hash is not None:
                    mapping.setdefault(talk_id, step_hash)
        return mapping

    def resolve_chapter_id(self, main_obj: Any, quest_obj: Any) -> Optional[int]:
        main_row = self.extract_quest_row(main_obj)
        main_chapter_id = main_row.chapter_id if main_row is not None else None
        quest_row = self.extract_quest_row(quest_obj)
        quest_chapter_id = quest_row.chapter_id if quest_row is not None else None
        return self.resolve_chapter_values(main_chapter_id, quest_chapter_id)

    @staticmethod
    def resolve_chapter_values(main_chapter_id: Any, quest_chapter_id: Any) -> Optional[int]:
        if isinstance(main_chapter_id, int) and main_chapter_id > 0:
            return main_chapter_id
        if isinstance(quest_chapter_id, int) and quest_chapter_id > 0:
            return quest_chapter_id
        return None


GTS_QUEST_PARSER = QuestParser(GTS_COMPAT)
BWIKI_QUEST_PARSER = QuestParser(BWIKI_COMPAT)

extract_quest_row = GTS_QUEST_PARSER.extract_quest_row
extract_quest_id = GTS_QUEST_PARSER.extract_quest_id
extract_quest_talk_ids = GTS_QUEST_PARSER.extract_quest_talk_ids
get_quest_subquests = GTS_QUEST_PARSER.get_quest_subquests
get_step_desc_text_map_hash = GTS_QUEST_PARSER.get_step_desc_text_map_hash
get_step_sub_id = GTS_QUEST_PARSER.get_step_sub_id
get_step_order = GTS_QUEST_PARSER.get_step_order
get_step_talk_ids = GTS_QUEST_PARSER.get_step_talk_ids
build_step_title_hash_by_talk_id = GTS_QUEST_PARSER.build_step_title_hash_by_talk_id

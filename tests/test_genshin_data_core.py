import os
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from genshin_data_core.access import FilesystemGameDataAccess
from genshin_data_core.compat import BWIKI_COMPAT, GTS_COMPAT
from genshin_data_core.hall import (
    get_hall_desc_text_hash,
    get_hall_facility_style_id,
    get_hall_name_text_hash,
    get_hall_style_id,
    is_public_hall,
)
from genshin_data_core.quest import BWIKI_QUEST_PARSER, GTS_QUEST_PARSER
from genshin_data_core.sources import QuestSourceResolver, extract_anecdote_core_fields
from genshin_data_core.talk import extract_talk_dialogue_payload, is_non_dialog_talk_obj


def test_66_chapter_precedence_is_consumer_compatible():
    obj = {
        "GMOMCKNPBGE": 6034,
        "ALLMCLJBBDM": 1000,
        "DMKHKJJFOAA": 1611,
        "JILHIMLENJK": 6034,
        "EOHJIHHMBAN": [],
    }
    assert GTS_QUEST_PARSER.extract_quest_row(obj).chapter_id == 6034
    assert BWIKI_QUEST_PARSER.extract_quest_row(obj).chapter_id == 1611


def test_legacy_hangout_range_is_gts_only(tmp_path):
    access = FilesystemGameDataAccess(str(tmp_path))
    assert 19001 in QuestSourceResolver(access, GTS_COMPAT).load_hangout_quest_ids()
    assert 19001 not in QuestSourceResolver(access, BWIKI_COMPAT).load_hangout_quest_ids()


def test_7_1_quest_brief_source_type_uses_type_field(tmp_path):
    import json

    folder = tmp_path / "BinOutput" / "QuestBrief"
    folder.mkdir(parents=True)
    (folder / "72721.json").write_text(
        json.dumps({"id": 72721, "type": "WQ", "DOOCLIPFECE": 3256870607, "JIJKODHIEED": []}),
        encoding="utf-8",
    )
    (folder / "7017.json").write_text(
        json.dumps({"id": 7017, "DOOCLIPFECE": 3256870607, "JIJKODHIEED": []}),
        encoding="utf-8",
    )
    resolver = QuestSourceResolver(FilesystemGameDataAccess(str(tmp_path)), GTS_COMPAT)
    assert resolver.resolve_quest_source_fields(72721) == ("WQ", "WQ")
    assert resolver.resolve_quest_source_fields(7017) == ("AQ", "AQ")


def test_extended_anecdote_aliases_are_bwiki_compatible():
    row = {
        "GBDGFHNLDFF": 107501,
        "PPANCKHJOGI": 325970011,
        "AJKAHOPOBJB": 1907257608,
        "OBLBGMIHBHL": 1917204629,
        "BBOMCGBIOFM": [510750101],
    }
    gts = extract_anecdote_core_fields(row, GTS_COMPAT)
    bwiki = extract_anecdote_core_fields(row, BWIKI_COMPAT)
    assert gts["title_text_map_hash"] is None
    assert bwiki["title_text_map_hash"] == 325970011
    assert bwiki["long_desc_text_map_hash"] == 1917204629
    assert bwiki["legacy_group_ids"] == [510750101]


def test_legacy_storyboard_container_detection_is_profiled():
    obj = {"ANCLPHMACIF": 1, "CIAOBJHFJJM": []}
    assert is_non_dialog_talk_obj(obj, GTS_COMPAT)
    assert not is_non_dialog_talk_obj(obj, BWIKI_COMPAT)


def test_hall_schema_aliases_are_shared():
    row = {
        "AOGCNHLHJMJ": 1001,
        "BNKLMBACEDF": "BEYOND_HALL_PUBLIC",
        "CKIGKAIIFFI": 11,
        "PPOAOFDNLDJ": 2001,
    }
    assert get_hall_style_id(row) == 11
    assert get_hall_name_text_hash(row) == 1001
    assert get_hall_desc_text_hash(row) == 2001
    assert is_public_hall(row)


def test_hall_schema_aliases_support_7_0_keys():
    row = {
        "MDCHMLFLPID": 1001,
        "AAJDEFOOIKH": "BEYOND_HALL_PUBLIC",
        "NMEDGNKECLH": 11,
        "JMPHKBBFCDB": 2001,
    }
    assert get_hall_style_id(row) == 11
    assert get_hall_name_text_hash(row) == 1001
    assert get_hall_desc_text_hash(row) == 2001
    assert is_public_hall(row)


def test_hall_facility_schema_aliases_support_7_0_and_legacy_keys():
    assert get_hall_facility_style_id({"ELPPMBJMBIO": 18}) == 18
    assert get_hall_facility_style_id({"ELPPMBJMBIO": 0, "OJGEAGGJALA": 11}) == 11
    assert get_hall_facility_style_id({"ELPPMBJMBIO": "18"}) is None


def test_hall_schema_aliases_support_7_1_keys():
    row = {
        "OANKFNDIFGE": 11,
        "NOBNAHLBFEG": 1001,
        "HOCAJGPPDII": 2001,
        "DFIKEPIFLPG": "BEYOND_HALL_PUBLIC",
    }
    assert get_hall_style_id(row) == 11
    assert get_hall_name_text_hash(row) == 1001
    assert get_hall_desc_text_hash(row) == 2001
    assert is_public_hall(row)
    assert get_hall_facility_style_id({"GCABMKJNPCK": 18}) == 18
    assert get_hall_facility_style_id({"GCABMKJNPCK": 0}) is None


def test_7_1_quest_and_talk_shapes_keep_title_steps_and_dialogue():
    quest = {
        "id": 1000,
        "DOOCLIPFECE": 492422065,
        "chapterId": 1101,
        "DLLABGGCEBM": [
            {"id": 100001, "JEDNDGCOMGC": [{"type": "QUEST_COND_STATE_EQUAL", "param": ["100000", "2"]}]}
        ],
        "JIJKODHIEED": [
            {"NFGFDHPPBIF": 100000, "NBOJMAHCCGM": 66312846, "finishCond": [{"type": "QUEST_CONTENT_FINISH_PLOT", "param": [100001]}]}
        ],
    }
    assert GTS_QUEST_PARSER.extract_quest_row(quest) == (1000, 492422065, 1101)
    assert GTS_QUEST_PARSER.extract_quest_talk_ids(quest) == [100001]
    assert GTS_QUEST_PARSER.build_step_title_hash_by_talk_id(quest)[100001] == 66312846
    assert GTS_QUEST_PARSER.extract_quest_row({"DLLABGGCEBM": []}) is None
    quest_without_embedded_talks = {
        "id": 19071,
        "DOOCLIPFECE": 2395540167,
        "JIJKODHIEED": [],
        "PCIAMAFDDAA": [{"id": 190710393}],
    }
    assert GTS_QUEST_PARSER.extract_quest_row(quest_without_embedded_talks).title_text_map_hash == 2395540167
    assert GTS_QUEST_PARSER.extract_quest_talk_ids(quest_without_embedded_talks) == [190710393]

    talk = {
        "talkId": 100001,
        "PCIAMAFDDAA": [
            {"id": 10000101, "LKECPJIFFEE": 1076121409, "KBPOBGFGLKN": {"id": "1005", "type": "TALK_ROLE_NPC"}}
        ],
    }
    assert extract_talk_dialogue_payload(talk) == (
        100001, [(10000101, 1076121409, "1005", "TALK_ROLE_NPC")]
    )
    assert extract_talk_dialogue_payload({"talkId": 100001, "PCIAMAFDDAA": None}) == (100001, [])

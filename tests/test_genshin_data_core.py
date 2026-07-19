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
    get_hall_name_text_hash,
    get_hall_style_id,
    is_public_hall,
)
from genshin_data_core.quest import BWIKI_QUEST_PARSER, GTS_QUEST_PARSER
from genshin_data_core.sources import QuestSourceResolver, extract_anecdote_core_fields
from genshin_data_core.talk import is_non_dialog_talk_obj


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

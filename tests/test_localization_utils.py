import os
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import localization_utils


def test_build_subtitle_filename_map_supports_obfuscated_path_keys():
    mapping = localization_utils.build_subtitle_filename_map(
        [
            {"id": 101, "assetType": "LOC_SUBTITLE", "EDPAFDDJJNM": "Subtitle/EN/Cutscene_01.srt"},
            {"id": 102, "assetType": "LOC_SUBTITLE", "FNIFOPDJMMG": "Subtitle/JP/Cutscene_02.srt"},
            {"id": 103, "AENCKCKHDFK": "Subtitle/CHS/Cutscene_03.srt"},
            {"id": 104, "HJBAJOBPLGE": "ART/UI/Readable/CHS/NotSubtitle"},
            {"id": 105, "HJBAJOBPLGE": "CHS/Cs_MDAQ019_DragonInCity_CHS.mihoyobin"},
            {"id": 106, "DEFNEHAFMMA": "Subtitle/CHS/Cutscene_06.srt"},
            {"id": 107, "JDNBKKPEFAI": "CHS/Cs_MDAQ020_Trial_CHS.mihoyobin"},
        ]
    )

    assert mapping["Cutscene_01"] == {"subtitleId": 101}
    assert mapping["Cutscene_02"] == {"subtitleId": 102}
    assert mapping["Cutscene_03"] == {"subtitleId": 103}
    assert mapping["Cutscene_06"] == {"subtitleId": 106}
    assert mapping["Cs_MDAQ020_Trial_CHS"] == {"subtitleId": 107}
    assert mapping["Cs_MDAQ019_DragonInCity_CHS"] == {"subtitleId": 105}
    assert "NotSubtitle" not in mapping


def test_build_readable_filename_map_matches_multiple_filename_variants():
    mapping = localization_utils.build_readable_filename_map(
        [{"id": 7, "englishPath": "Readable/Books/Archive_EN.txt"}],
        {7: 9001},
    )

    expected = {"titleHash": 9001, "readableId": 7}
    assert mapping["Archive_EN.txt"] == expected
    assert mapping["Archive_EN"] == expected
    assert mapping["Archive.txt"] == expected
    assert mapping["Archive"] == expected


def test_build_readable_filename_map_keeps_path_only_readable_ids_without_fake_title():
    mapping = localization_utils.build_readable_filename_map(
        [
            {
                "id": 201224,
                "DEFNEHAFMMA": "ART/UI/Readable/CHS/Book1224",
                "enPath": "ART/UI/Readable/EN/Book1224_EN",
            }
        ],
        {},
    )

    assert mapping["Book1224"]["readableId"] == 201224
    assert mapping["Book1224"]["titleHash"] is None
    assert mapping["Book1224_EN"]["readableId"] == 201224

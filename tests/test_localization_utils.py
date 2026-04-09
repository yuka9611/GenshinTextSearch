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
        ]
    )

    assert mapping["Cutscene_01"] == {"subtitleId": 101}
    assert mapping["Cutscene_02"] == {"subtitleId": 102}


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

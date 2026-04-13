"""Regression tests for voice item schema parsing."""
import os
import sys


TESTS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.normpath(os.path.join(TESTS_DIR, os.pardir))
DBBUILD_DIR = os.path.join(REPO_ROOT, "server", "dbBuild")
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import databaseHelper
import voiceItemImport


def _latest_fetter_content():
    return {
        "NEJBKIJIBHJ": [
            {
                "NKPOOPONEHG": "VO_friendship\\VO_zibai\\vo_zibai_redeem_01.wem",
                "FBCCEBGJEDB": "Switch_zibai",
            }
        ],
        "LNBFOPNLLDB": "0de82020-7ad0-4330-95de-f56af640ec72",
        "JOMDCMBGNMB": "Fetter",
        "BKGHEBIGJNC": 710001,
    }


def test_dbbuild_extract_fetter_voice_rows_supports_latest_schema(monkeypatch):
    monkeypatch.setattr(
        voiceItemImport,
        "getAvatarIdFromVoiceItemAvatarName",
        lambda raw_name: 10000126 if str(raw_name).lower() == "switch_zibai" else 0,
    )

    rows = voiceItemImport._extract_fetter_voice_rows(_latest_fetter_content())

    assert rows == [
        (10000126, 710001, "VO_friendship\\VO_zibai\\vo_zibai_redeem_01.wem")
    ]


def test_runtime_extract_fetter_voice_rows_supports_latest_schema(monkeypatch):
    monkeypatch.setattr(
        databaseHelper,
        "_load_fetter_avatar_mappings",
        lambda: {"switch_zibai": 10000126},
    )

    rows = databaseHelper._extract_fetter_voice_rows(_latest_fetter_content())

    assert rows == [
        (10000126, 710001, "VO_friendship\\VO_zibai\\vo_zibai_redeem_01.wem")
    ]

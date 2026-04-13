"""Tests for entity source import helpers."""
import os
import sys


TESTS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.normpath(os.path.join(TESTS_DIR, os.pardir))
DBBUILD_DIR = os.path.join(REPO_ROOT, "server", "dbBuild")
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

import entitySourceImport


def test_gender_code_marks_both_body_types_as_unisex():
    assert entitySourceImport._gender_code(["BODY_GIRL", "BODY_BOY"]) == 3


def test_iter_costume_mappings_supports_obfuscated_body_type_field():
    rows = [
        {
            "costumeId": 260001,
            "nameTextMapHash": 2098177162,
            "descriptionTextMapHash": 33590471,
            "IAHOEKGIPPJ": ["BODY_GIRL", "BODY_BOY"],
        }
    ]

    result = list(entitySourceImport._iter_costume_mappings(rows))

    assert result == [
        (
            33590471,
            entitySourceImport.SOURCE_TYPE_COSTUME,
            260001,
            2098177162,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC, 3),
            entitySourceImport.SUB_QIANXING_PARADOX,
        )
    ]

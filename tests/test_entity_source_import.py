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


def test_iter_emoji_mappings_emits_qianxing_emoji_desc():
    rows = [
        {
            "id": 1001,
            "nameTextMapHash": 741611961,
            "descriptionTextMapHash": 2762799464,
        }
    ]

    result = list(entitySourceImport._iter_emoji_mappings(rows))

    assert result == [
        (
            2762799464,
            entitySourceImport.SOURCE_TYPE_QIANXING_EMOJI,
            1001,
            741611961,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_EMOJI,
        )
    ]


def test_iter_pose_mappings_marks_gender_from_body_type():
    rows = [
        {
            "id": 611000,
            "nameTextMapHash": 1491722704,
            "descriptionTextMapHash": 4282661314,
            "bodyType": "BODY_GIRL",
        }
    ]

    result = list(entitySourceImport._iter_pose_mappings(rows))

    assert result == [
        (
            4282661314,
            entitySourceImport.SOURCE_TYPE_QIANXING_POSE,
            611000,
            1491722704,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC, 2),
            entitySourceImport.SUB_QIANXING_POSE,
        )
    ]


def test_iter_effect_mappings_emits_qianxing_effect_desc():
    rows = [
        {
            "id": 2001,
            "nameTextMapHash": 3244865603,
            "descriptionTextMapHash": 3531184282,
        }
    ]

    result = list(entitySourceImport._iter_effect_mappings(rows))

    assert result == [
        (
            3531184282,
            entitySourceImport.SOURCE_TYPE_QIANXING_EFFECT,
            2001,
            3244865603,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_EFFECT,
        )
    ]


def test_iter_hall_template_mappings_skips_public_hall_and_supports_fallback_keys():
    rows = [
        {
            "DCOBMNILGJL": 15,
            "KMMKMJLOFGC": 1209931833,
            "DKBHBHOOGAP": 3795564820,
            "BMIILBDKBIO": "BEYOND_HALL_PRIVATE",
        },
        {
            "COGKFPLDLLL": 1015,
            "LDCAAIEKMOE": 1823553705,
            "BPKNEMEJEPF": 4197610804,
            "PEMNJBEBBOG": "BEYOND_HALL_PUBLIC",
        },
    ]

    result = list(entitySourceImport._iter_hall_template_mappings(rows))

    assert result == [
        (
            3795564820,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            15,
            1209931833,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]


def test_iter_hall_facility_mappings_accepts_bwiki_style_id_fallback_keys():
    rows = [
        {
            "id": 351101,
            "nameTextMapHash": 2097032544,
            "descTextMapHash": 3061297604,
            "DGBOKBNOJKE": 1,
        }
    ]

    result = list(entitySourceImport._iter_hall_facility_mappings(rows))

    assert result == [
        (
            3061297604,
            entitySourceImport.SOURCE_TYPE_QIANXING_HALL,
            351101,
            2097032544,
            entitySourceImport._pack_extra(entitySourceImport.FIELD_DESC),
            entitySourceImport.SUB_QIANXING_HALL,
        )
    ]

import os
import sys


DBBUILD_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), os.pardir, "server", "dbBuild")
)
if DBBUILD_DIR not in sys.path:
    sys.path.insert(0, DBBUILD_DIR)

from textmap_name_utils import parse_textmap_file_name, textmap_file_sort_key


def test_parse_textmap_file_name_supports_textmap_medium_with_underscore():
    assert parse_textmap_file_name("TextMap_MediumEN.json") == ("TextMapEN.json", None)
    assert parse_textmap_file_name("TextMap_MediumEN_1.json") == ("TextMapEN.json", 1)


def test_parse_textmap_file_name_supports_plain_text_files():
    assert parse_textmap_file_name("TextJA.json") == ("TextMapJP.json", None)
    assert parse_textmap_file_name("TextKO_2.json") == ("TextMapKR.json", 2)


def test_textmap_file_sort_key_keeps_medium_variants_after_normal_files():
    normal_key = textmap_file_sort_key("TextMapEN.json")
    medium_key = textmap_file_sort_key("TextMap_MediumEN.json")
    split_key = textmap_file_sort_key("TextMap_MediumEN_1.json")

    assert normal_key < medium_key < split_key

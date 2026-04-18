"""Tests for server/placeholderHandler.py — DB calls are mocked."""
from unittest.mock import patch

import placeholderHandler


# ---------------------------------------------------------------------------
# _replace_with_names  (internal helper, core logic)
# ---------------------------------------------------------------------------

class TestReplaceWithNames:
    """Test gender placeholder replacement without involving the DB layer."""

    def test_male_gender_selection(self):
        text = "{M#他}{F#她}去了商店"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="",
        )
        assert result == "他去了商店"

    def test_female_gender_selection(self):
        text = "{M#他}{F#她}去了商店"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=False, lang=1, wander_name="", traveller_name="",
        )
        assert result == "她去了商店"

    def test_both_gender_shows_both(self):
        text = "{M#他}{F#她}去了商店"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale="both", lang=1, wander_name="", traveller_name="",
        )
        assert result == "{他/她}去了商店"

    def test_reversed_gender_tags(self):
        text = "{F#她}{M#他}去了商店"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="",
        )
        assert result == "他去了商店"

    def test_nickname_replacement(self):
        text = "{NICKNAME}你好"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="荧",
        )
        assert result == "荧你好"

    def test_wanderer_realname_replacement(self):
        text = "{REALNAME[ID(1)|HOSTONLY(true)]}来了"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="流浪者", traveller_name="",
        )
        assert result == "流浪者来了"

    def test_little_one_showhost_replacement(self):
        text = "{REALNAME[ID(2)|SHOWHOST(true)]}来了"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="",
        )
        assert result == "小家伙来了"

    def test_little_one_showhost_replacement_with_leading_hash(self):
        text = "#{REALNAME[ID(2)|SHOWHOST(true)]}来了"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="",
        )
        assert result == "小家伙来了"

    def test_leading_hash_stripped(self):
        text = "#开头的文本"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="",
        )
        assert result == "开头的文本"

    def test_no_placeholders_unchanged(self):
        text = "普通文本，没有任何占位符"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="",
        )
        assert result == text

    @patch("placeholderHandler.databaseHelper")
    def test_sexpro_male(self, mock_db):
        mock_db.getManualTextMap.side_effect = lambda pid, lang: {
            "INFO_MALE_PRONOUN_HE": "他",
            "INFO_FEMALE_PRONOUN_SHE": "她",
        }.get(pid, "")

        text = "{PLAYERAVATAR#SEXPRO[INFO_MALE_PRONOUN_HE|INFO_FEMALE_PRONOUN_SHE]}很强"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale=True, lang=1, wander_name="", traveller_name="",
        )
        assert result == "他很强"

    @patch("placeholderHandler.databaseHelper")
    def test_sexpro_both(self, mock_db):
        mock_db.getManualTextMap.side_effect = lambda pid, lang: {
            "INFO_MALE_PRONOUN_HE": "他",
            "INFO_FEMALE_PRONOUN_SHE": "她",
        }.get(pid, "")

        text = "{PLAYERAVATAR#SEXPRO[INFO_MALE_PRONOUN_HE|INFO_FEMALE_PRONOUN_SHE]}很强"
        result = placeholderHandler._replace_with_names(
            text, playerIsMale="both", lang=1, wander_name="", traveller_name="",
        )
        assert result == "{他/她}很强"


# ---------------------------------------------------------------------------
# _normalize_special_name
# ---------------------------------------------------------------------------

class TestNormalizeSpecialName:
    def test_none_returns_empty(self):
        assert placeholderHandler._normalize_special_name(None, True, 1) == ""

    def test_empty_returns_empty(self):
        assert placeholderHandler._normalize_special_name("", True, 1) == ""

    def test_plain_text_unchanged(self):
        result = placeholderHandler._normalize_special_name("普通名字", True, 1)
        assert result == "普通名字"


# ---------------------------------------------------------------------------
# replace  (top-level function — mocks DB)
# ---------------------------------------------------------------------------

class TestReplace:
    def test_none_returns_none(self):
        assert placeholderHandler.replace(None, True, 1) is None

    def test_empty_returns_empty(self):
        assert placeholderHandler.replace("", True, 1) == ""

    @patch("placeholderHandler.databaseHelper")
    def test_replace_calls_db_for_character_names(self, mock_db):
        mock_db.getCharacterNameRaw.return_value = None
        mock_db.getManualTextMap.return_value = None

        result = placeholderHandler.replace("普通文本", True, 1)
        assert result == "普通文本"
        # Should call getCharacterNameRaw for wanderer and traveller
        assert mock_db.getCharacterNameRaw.call_count == 2

    @patch("placeholderHandler.databaseHelper")
    def test_replace_with_nickname(self, mock_db):
        mock_db.getCharacterNameRaw.return_value = None
        mock_db.getManualTextMap.return_value = None

        result = placeholderHandler.replace("{NICKNAME}，早上好", True, 1)
        # With no character name found, nickname is replaced with empty-ish
        assert result is not None
        assert "{NICKNAME}" not in result

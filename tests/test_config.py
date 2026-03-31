"""Tests for server/config.py — uses tmp_config fixture to isolate file I/O."""
import json


# ---------------------------------------------------------------------------
# loadConfig / saveConfig round-trip
# ---------------------------------------------------------------------------

class TestLoadSaveConfig:
    def test_load_missing_file_does_not_crash(self, tmp_config):
        # CONFIG_FILE doesn't exist yet — loadConfig should be a no-op
        tmp_config.loadConfig()

    def test_save_then_load_round_trip(self, tmp_config):
        tmp_config.config["assetDir"] = "/some/path"
        tmp_config.config["defaultSearchLanguage"] = 9
        tmp_config.saveConfig()

        # Reset in-memory and reload
        tmp_config.config["assetDir"] = ""
        tmp_config.config["defaultSearchLanguage"] = 1
        tmp_config.loadConfig()

        assert tmp_config.config["assetDir"] == "/some/path"
        assert tmp_config.config["defaultSearchLanguage"] == 9

    def test_load_partial_json_keeps_defaults(self, tmp_config):
        # Write a JSON with only one key
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"assetDir": "/partial"}, fp)

        tmp_config.loadConfig()
        assert tmp_config.config["assetDir"] == "/partial"
        # Other fields keep their defaults
        assert isinstance(tmp_config.config["resultLanguages"], list)

    def test_load_invalid_json_does_not_crash(self, tmp_config):
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            fp.write("{invalid json{{")
        tmp_config.loadConfig()  # should not raise

    def test_load_isMale_bool(self, tmp_config):
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"isMale": True}, fp)
        tmp_config.loadConfig()
        assert tmp_config.config["isMale"] is True

    def test_load_isMale_both(self, tmp_config):
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"isMale": "both"}, fp)
        tmp_config.loadConfig()
        assert tmp_config.config["isMale"] == "both"

    def test_load_ftsTokenizer_empty_defaults_to_trigram(self, tmp_config):
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"ftsTokenizer": ""}, fp)
        tmp_config.loadConfig()
        assert tmp_config.config["ftsTokenizer"] == "trigram"

    def test_load_ftsLangAllowList_filters_non_int(self, tmp_config):
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"ftsLangAllowList": [1, "abc", 9]}, fp)
        tmp_config.loadConfig()
        assert tmp_config.config["ftsLangAllowList"] == [1, 9]

    def test_load_ftsChineseSegmenter_valid(self, tmp_config):
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"ftsChineseSegmenter": "jieba"}, fp)
        tmp_config.loadConfig()
        assert tmp_config.config["ftsChineseSegmenter"] == "jieba"

    def test_load_ftsChineseSegmenter_invalid_ignored(self, tmp_config):
        previous = tmp_config.config["ftsChineseSegmenter"]
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"ftsChineseSegmenter": "invalid_mode"}, fp)
        tmp_config.loadConfig()
        # Should remain unchanged when invalid input is provided.
        assert tmp_config.config["ftsChineseSegmenter"] == previous

    def test_load_ftsStopwords_filters_none_and_empty(self, tmp_config):
        with open(tmp_config.CONFIG_FILE, "w", encoding="utf-8") as fp:
            json.dump({"ftsStopwords": ["the", None, "", "of"]}, fp)
        tmp_config.loadConfig()
        assert tmp_config.config["ftsStopwords"] == ["the", "of"]


# ---------------------------------------------------------------------------
# Getters / Setters
# ---------------------------------------------------------------------------

class TestGettersSetters:
    def test_setAssetDir_normalizes_path(self, tmp_config):
        tmp_config.setAssetDir("/foo/bar/../baz")
        assert ".." not in tmp_config.getAssetDir()

    def test_setAssetDir_strips_streaming_assets(self, tmp_config):
        tmp_config.setAssetDir("/game/GenshinImpact_Data/StreamingAssets")
        assert tmp_config.getAssetDir().endswith("GenshinImpact_Data")

    def test_setIsMale_bool(self, tmp_config):
        tmp_config.setIsMale(False)
        assert tmp_config.getIsMale() is False

    def test_setIsMale_both(self, tmp_config):
        tmp_config.setIsMale("both")
        assert tmp_config.getIsMale() == "both"

    def test_setDefaultSearchLanguage(self, tmp_config):
        tmp_config.setDefaultSearchLanguage(9)
        assert tmp_config.getDefaultSearchLanguage() == 9

    def test_setResultLanguages(self, tmp_config):
        tmp_config.setResultLanguages([1, 4])
        assert tmp_config.getResultLanguages() == [1, 4]

    def test_setSourceLanguage(self, tmp_config):
        tmp_config.setSourceLanguage(4)
        assert tmp_config.getSourceLanguage() == 4


# ---------------------------------------------------------------------------
# FTS-related getters
# ---------------------------------------------------------------------------

class TestFtsGetters:
    def test_getFtsTokenizer_default(self, tmp_config):
        assert tmp_config.getFtsTokenizer() == str(tmp_config.config.get("ftsTokenizer") or "trigram")

    def test_getEnableTextMapFts_default(self, tmp_config):
        assert tmp_config.getEnableTextMapFts() is True

    def test_getFtsChineseSegmenter_default(self, tmp_config):
        mode = str(tmp_config.config.get("ftsChineseSegmenter") or "auto").strip().lower()
        expected = mode if mode in ("auto", "jieba", "char_bigram", "none") else "auto"
        assert tmp_config.getFtsChineseSegmenter() == expected

    def test_getFtsChineseSegmenter_invalid_returns_auto(self, tmp_config):
        tmp_config.config["ftsChineseSegmenter"] = "bad_value"
        assert tmp_config.getFtsChineseSegmenter() == "auto"

    def test_getFtsLangAllowList_default(self, tmp_config):
        result = tmp_config.getFtsLangAllowList()
        assert isinstance(result, list)
        assert all(isinstance(x, int) for x in result)

    def test_getFtsStopwords_default(self, tmp_config):
        expected = []
        for item in tmp_config.config.get("ftsStopwords") or []:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                expected.append(text)
        assert tmp_config.getFtsStopwords() == expected

    def test_getFtsJiebaUserDict_default(self, tmp_config):
        assert tmp_config.getFtsJiebaUserDict() == ""


# ---------------------------------------------------------------------------
# is_packaged
# ---------------------------------------------------------------------------

class TestIsPackaged:
    def test_not_packaged_in_test_env(self, tmp_config):
        assert tmp_config.is_packaged() is False

"""Tests for server/controllers/api.py using Flask request contexts."""
import io
import json
import sqlite3
import sys
from urllib.parse import urlencode

from flask import Flask

import controllers.api as api


def _app() -> Flask:
    app = Flask(__name__)
    app.config["TESTING"] = True
    return app


def _request_context(app: Flask, path: str, method: str = "GET", query_string=None, json_body=None):
    query = urlencode(query_string or {}, doseq=True)
    body = b""
    content_type = None
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        content_type = "application/json"

    environ = {
        "REQUEST_METHOD": method,
        "SCRIPT_NAME": "",
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "SERVER_NAME": "localhost",
        "SERVER_PORT": "5000",
        "REMOTE_ADDR": "127.0.0.1",
        "CONTENT_TYPE": content_type or "",
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.version": (1, 0),
        "wsgi.url_scheme": "http",
        "wsgi.input": io.BytesIO(body),
        "wsgi.errors": sys.stderr,
        "wsgi.multithread": False,
        "wsgi.multiprocess": False,
        "wsgi.run_once": False,
    }
    return app.request_context(environ)


class TestSearchEndpoint:
    def test_search_empty_keyword(self, monkeypatch):
        monkeypatch.setattr(api.search_cache, "get", lambda key: None)
        monkeypatch.setattr(api.search_cache, "set", lambda key, value: None)
        monkeypatch.setattr(api, "get_lang_id", lambda lang: 1)
        monkeypatch.setattr(api, "selectTextMapFromKeywordPaged", lambda keyword, lang, size, offset: [])

        app = _app()
        with _request_context(app, "/api/search", query_string={"keyword": "", "lang": "zh-cn"}):
            resp = api.search_text()

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["results"] == []
        assert data["total"] == 0

    def test_search_returns_results(self, monkeypatch):
        monkeypatch.setattr(api.search_cache, "get", lambda key: None)
        monkeypatch.setattr(api.search_cache, "set", lambda key, value: None)
        monkeypatch.setattr(api, "get_lang_id", lambda lang: 1)
        monkeypatch.setattr(
            api,
            "selectTextMapFromKeywordPaged",
            lambda keyword, lang, size, offset: [(12345, "测试内容", "1.0", "2.0")],
        )

        app = _app()
        with _request_context(app, "/api/search", query_string={"keyword": "测试", "lang": "zh-cn"}):
            resp = api.search_text()

        data = resp.get_json()
        assert resp.status_code == 200
        assert len(data["results"]) == 1
        assert data["results"][0]["hash"] == 12345

    def test_search_returns_cached(self, monkeypatch):
        cached = {"keyword": "test", "results": [{"hash": 1}], "total": 1}
        monkeypatch.setattr(api.search_cache, "get", lambda key: cached)

        app = _app()
        with _request_context(app, "/api/search", query_string={"keyword": "test"}):
            resp = api.search_text()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["results"] == [{"hash": 1}]


class TestVoicePathEndpoint:
    def test_voice_path(self, monkeypatch):
        monkeypatch.setattr(api.search_cache, "get", lambda key: None)
        monkeypatch.setattr(api.search_cache, "set", lambda key, value: None)
        monkeypatch.setattr(api, "get_lang_id", lambda lang: 1)
        monkeypatch.setattr(api, "getVoicePath", lambda voice_hash, lang_id: "/audio/vo_123.wem")

        app = _app()
        with _request_context(app, "/api/voice/path", query_string={"hash": "12345", "lang": "zh-cn"}):
            resp = api.get_voice_path_api()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["path"] == "/audio/vo_123.wem"


class TestTextMapEndpoint:
    def test_textmap_by_hash(self, monkeypatch):
        monkeypatch.setattr(api.search_cache, "get", lambda key: None)
        monkeypatch.setattr(api.search_cache, "set", lambda key, value: None)
        monkeypatch.setattr(api, "get_lang_id", lambda lang: 1)
        monkeypatch.setattr(api, "getTextMapByHash", lambda hash_val, lang_id: "你好世界")

        app = _app()
        with _request_context(app, "/api/textmap", query_string={"hash": "99999", "lang": "zh-cn"}):
            resp = api.get_text_map_api()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["text"] == "你好世界"


class TestVersionEndpoint:
    def test_version(self, monkeypatch):
        monkeypatch.setattr(api.search_cache, "get", lambda key: None)
        monkeypatch.setattr(api.search_cache, "set", lambda key, value: None)
        monkeypatch.setattr(api, "get_lang_id", lambda lang: 1)
        monkeypatch.setattr(api, "getVersionData", lambda lang_id, include_current=True: [{"version": "4.0"}])

        app = _app()
        with _request_context(app, "/api/version", query_string={"lang": "zh-cn"}):
            resp = api.get_version_api()

        data = resp.get_json()
        assert resp.status_code == 200
        assert isinstance(data, list)


class TestLanguagesEndpoint:
    def test_languages(self, monkeypatch):
        monkeypatch.setattr(api.search_cache, "get", lambda key: None)
        monkeypatch.setattr(api.search_cache, "set", lambda key, value: None)

        app = _app()
        with _request_context(app, "/api/languages"):
            resp = api.get_languages_api()

        data = resp.get_json()
        assert resp.status_code == 200
        assert isinstance(data, list)
        codes = [item["code"] for item in data]
        assert "zh-cn" in codes
        assert "en-us" in codes


class TestKeywordQueryEndpoint:
    def test_keyword_query_empty_returns_empty(self):
        app = _app()
        payload = {"langCode": 1, "keyword": ""}
        with _request_context(app, "/api/keywordQuery", method="POST", json_body=payload):
            resp = api.keywordQuery()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["data"]["contents"] == []
        assert data["data"]["total"] == 0

    def test_keyword_query_with_keyword(self, monkeypatch):
        monkeypatch.setattr(
            api.controllers_module,
            "getTranslateObj",
            lambda keyword, lang_code, speaker, page, page_size, voice_filter, created_version, updated_version, source_type: (
                [{"textHash": 1, "translates": []}],
                1,
            ),
        )

        app = _app()
        payload = {"langCode": 1, "keyword": "测试", "page": 1, "pageSize": 10}
        with _request_context(app, "/api/keywordQuery", method="POST", json_body=payload):
            resp = api.keywordQuery()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert data["data"]["total"] == 1


class TestCatalogSearchEndpoint:
    def test_catalog_search_rejects_empty_payload(self):
        app = _app()
        payload = {"langCode": 1}
        with _request_context(app, "/api/catalogSearch", method="POST", json_body=payload):
            resp = api.catalogSearch()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 400
        assert data["msg"] == "keyword or a filter is required"

    def test_catalog_search_accepts_created_version_only(self, monkeypatch):
        calls = {}

        def fake_search_catalog(*args, **kwargs):
            calls["args"] = args
            calls["kwargs"] = kwargs
            return {"contents": [], "total": 0, "page": 1, "pageSize": 50, "time": 1.23}

        monkeypatch.setattr(api.controllers_module, "searchCatalog", fake_search_catalog)

        app = _app()
        payload = {"langCode": 1, "createdVersion": "5.0"}
        with _request_context(app, "/api/catalogSearch", method="POST", json_body=payload):
            resp = api.catalogSearch()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert calls["args"] == ("", 1, None, None, 1, 50)
        assert calls["kwargs"] == {"createdVersion": "5.0", "updatedVersion": None}

    def test_catalog_search_accepts_updated_version_only(self, monkeypatch):
        calls = {}

        def fake_search_catalog(*args, **kwargs):
            calls["args"] = args
            calls["kwargs"] = kwargs
            return {"contents": [], "total": 0, "page": 1, "pageSize": 50, "time": 1.23}

        monkeypatch.setattr(api.controllers_module, "searchCatalog", fake_search_catalog)

        app = _app()
        payload = {"langCode": 1, "updatedVersion": "5.1"}
        with _request_context(app, "/api/catalogSearch", method="POST", json_body=payload):
            resp = api.catalogSearch()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert calls["args"] == ("", 1, None, None, 1, 50)
        assert calls["kwargs"] == {"createdVersion": None, "updatedVersion": "5.1"}

    def test_catalog_search_accepts_keyword_search(self, monkeypatch):
        calls = {}

        def fake_search_catalog(*args, **kwargs):
            calls["args"] = args
            calls["kwargs"] = kwargs
            return {"contents": [{"entityId": 1}], "total": 1, "page": 1, "pageSize": 50, "time": 1.23}

        monkeypatch.setattr(api.controllers_module, "searchCatalog", fake_search_catalog)

        app = _app()
        payload = {"langCode": 1, "keyword": "风神"}
        with _request_context(app, "/api/catalogSearch", method="POST", json_body=payload):
            resp = api.catalogSearch()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert calls["args"] == ("风神", 1, None, None, 1, 50)
        assert calls["kwargs"] == {"createdVersion": None, "updatedVersion": None}


class TestAssetDirEndpoints:
    def test_set_asset_dir_rejects_invalid_directory(self, monkeypatch):
        monkeypatch.setattr(api.os.path, "isdir", lambda path: False)

        app = _app()
        payload = {"assetDir": "/invalid/path"}
        with _request_context(app, "/api/setAssetDir", method="POST", json_body=payload):
            resp = api.setAssetDir()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 400
        assert data["msg"] == "Invalid directory"

    def test_set_asset_dir_returns_normalized_path(self, monkeypatch):
        import config
        import languagePackReader

        state = {"assetDir": "", "saved": False, "reloaded": False}

        monkeypatch.setattr(api.os.path, "isdir", lambda path: True)
        monkeypatch.setattr(config, "setAssetDir", lambda path: state.__setitem__("assetDir", "/game/GenshinImpact_Data"))
        monkeypatch.setattr(config, "getAssetDir", lambda: state["assetDir"])
        monkeypatch.setattr(config, "isAssetDirValid", lambda: True)
        monkeypatch.setattr(config, "saveConfig", lambda: state.__setitem__("saved", True))
        monkeypatch.setattr(languagePackReader, "reloadLangPackages", lambda: state.__setitem__("reloaded", True))

        app = _app()
        payload = {"assetDir": "/game/GenshinImpact_Data/StreamingAssets"}
        with _request_context(app, "/api/setAssetDir", method="POST", json_body=payload):
            resp = api.setAssetDir()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert data["data"]["assetDir"] == "/game/GenshinImpact_Data"
        assert data["data"]["assetDirValid"] is True
        assert state["saved"] is True
        assert state["reloaded"] is True

    def test_pick_asset_dir_cancel_returns_cancel_flag(self, monkeypatch):
        import config

        monkeypatch.setattr(api.controllers_module, "pickAssetDirViaDialog", lambda: None)
        monkeypatch.setattr(config, "getAssetDir", lambda: "/existing/path")
        monkeypatch.setattr(config, "isAssetDirValid", lambda: True)

        app = _app()
        with _request_context(app, "/api/pickAssetDir", method="POST", json_body={}):
            resp = api.pickAssetDir()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert data["data"]["cancel"] is True
        assert data["data"]["dialogUnavailable"] is False
        assert data["data"]["assetDir"] == "/existing/path"

    def test_pick_asset_dir_reports_dialog_unavailable(self, monkeypatch):
        import config

        class FakeDialogError(RuntimeError):
            pass

        def _raise():
            raise FakeDialogError("no gui")

        monkeypatch.setattr(api.controllers_module, "AssetDirDialogUnavailableError", FakeDialogError, raising=False)
        monkeypatch.setattr(api.controllers_module, "pickAssetDirViaDialog", _raise)
        monkeypatch.setattr(config, "getAssetDir", lambda: "/existing/path")
        monkeypatch.setattr(config, "isAssetDirValid", lambda: False)

        app = _app()
        with _request_context(app, "/api/pickAssetDir", method="POST", json_body={}):
            resp = api.pickAssetDir()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 501
        assert data["data"]["cancel"] is False
        assert data["data"]["dialogUnavailable"] is True
        assert data["data"]["assetDir"] == "/existing/path"

    def test_pick_asset_dir_returns_normalized_path(self, monkeypatch):
        import config
        import languagePackReader

        state = {"assetDir": "", "saved": False, "reloaded": False}

        monkeypatch.setattr(api.os.path, "isdir", lambda path: True)
        monkeypatch.setattr(api.controllers_module, "pickAssetDirViaDialog", lambda: "/game/GenshinImpact_Data/StreamingAssets")
        monkeypatch.setattr(config, "setAssetDir", lambda path: state.__setitem__("assetDir", "/game/GenshinImpact_Data"))
        monkeypatch.setattr(config, "getAssetDir", lambda: state["assetDir"])
        monkeypatch.setattr(config, "isAssetDirValid", lambda: True)
        monkeypatch.setattr(config, "saveConfig", lambda: state.__setitem__("saved", True))
        monkeypatch.setattr(languagePackReader, "reloadLangPackages", lambda: state.__setitem__("reloaded", True))

        app = _app()
        with _request_context(app, "/api/pickAssetDir", method="POST", json_body={}):
            resp = api.pickAssetDir()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert data["data"]["cancel"] is False
        assert data["data"]["dialogUnavailable"] is False
        assert data["data"]["assetDir"] == "/game/GenshinImpact_Data"
        assert state["saved"] is True
        assert state["reloaded"] is True


class TestImportedEndpoints:
    def test_get_quest_dialogues_preserves_version_fields(self, monkeypatch):
        monkeypatch.setattr(
            api.controllers_module,
            "getQuestDialogues",
            lambda quest_id, search_lang, page, page_size: (
                {
                    "talkQuestName": "风起鹤归",
                    "questId": quest_id,
                    "dialogues": [],
                    "createdVersion": "4.4",
                    "updatedVersion": "4.7",
                    "createdVersionRaw": "Version 4.4",
                    "updatedVersionRaw": "Version 4.7",
                },
                0,
            ),
        )

        app = _app()
        payload = {"questId": 42, "searchLang": 1}
        with _request_context(app, "/api/getQuestDialogues", method="POST", json_body=payload):
            resp = api.getQuestDialogues()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["code"] == 200
        assert data["data"]["contents"]["createdVersion"] == "4.4"
        assert data["data"]["contents"]["updatedVersion"] == "4.7"

    def test_available_versions(self, monkeypatch):
        monkeypatch.setattr(api.controllers_module, "getAvailableVersions", lambda: ["4.0", "4.1", "4.2"])

        app = _app()
        with _request_context(app, "/api/getAvailableVersions"):
            resp = api.getAvailableVersions()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["data"] == ["4.0", "4.1", "4.2"]

    def test_imported_text_languages(self, monkeypatch):
        monkeypatch.setattr(api.controllers_module, "getImportedTextMapLangs", lambda: [1, 4, 9])

        app = _app()
        with _request_context(app, "/api/getImportedTextLanguages"):
            resp = api.getImportedTextLanguages()

        data = resp.get_json()
        assert resp.status_code == 200
        assert data["data"] == [1, 4, 9]


class TestDatabaseErrorHelper:
    def test_malformed_detected(self):
        err = sqlite3.DatabaseError("database disk image is malformed")
        assert api._is_database_corruption_error(err) is True

    def test_normal_error_not_corruption(self):
        err = sqlite3.DatabaseError("table not found")
        assert api._is_database_corruption_error(err) is False

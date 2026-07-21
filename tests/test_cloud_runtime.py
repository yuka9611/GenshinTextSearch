"""Cloud deployment policy and public API regressions."""

import pytest

import cloud_runtime
import server


@pytest.fixture(autouse=True)
def _cloud_environment(monkeypatch):
    monkeypatch.setenv("GTS_CLOUD_MODE", "1")
    monkeypatch.setenv("GTS_ENABLE_LOCAL_FEATURES", "0")
    monkeypatch.setenv("GTS_ENABLE_VOICE_PLAYBACK", "0")
    monkeypatch.setenv("GTS_ALLOW_SETTINGS_WRITE", "0")
    monkeypatch.delenv("GTS_TRUST_PROXY", raising=False)
    cloud_runtime._reset_rate_limit_for_tests()
    yield
    cloud_runtime._reset_rate_limit_for_tests()


def test_cloud_status_hides_local_paths_and_capabilities():
    app = server.create_app()
    client = app.test_client()

    response = client.get("/api/startupStatus")

    assert response.status_code == 200
    payload = response.get_json()["data"]
    assert payload["cloudMode"] is True
    assert payload["assetDir"] == ""
    assert payload["assetDirValid"] is False
    assert payload["localFeaturesEnabled"] is False
    assert payload["settingsWritable"] is False
    assert payload["voicePlaybackEnabled"] is False
    assert payload["browserAutoShutdownEnabled"] is False


@pytest.mark.parametrize("path", [
    "/api/setAssetDir",
    "/api/pickAssetDir",
    "/api/saveSettings",
])
def test_cloud_mutation_endpoints_are_forbidden(path):
    app = server.create_app()
    client = app.test_client()

    response = client.post(path, json={"assetDir": "/tmp", "config": {}})

    assert response.status_code == 403
    assert response.get_json()["code"] == 403


def test_cloud_voice_endpoints_do_not_load_local_assets():
    app = server.create_app()
    client = app.test_client()

    languages_response = client.get("/api/getImportedVoiceLanguages")
    voice_response = client.post("/api/getVoiceOver", json={"voicePath": "x", "langCode": 1})

    assert languages_response.status_code == 200
    assert languages_response.get_json()["data"] == {}
    assert voice_response.status_code == 403


def test_cloud_cors_allows_only_configured_frontend(monkeypatch):
    monkeypatch.setenv("GTS_CORS_ORIGINS", "https://search.example.com")
    app = server.create_app()
    client = app.test_client()

    allowed = client.get("/api/startupStatus", headers={"Origin": "https://search.example.com"})
    denied = client.get("/api/startupStatus", headers={"Origin": "https://other.example.com"})

    assert allowed.headers.get("Access-Control-Allow-Origin") == "https://search.example.com"
    assert denied.headers.get("Access-Control-Allow-Origin") is None


def test_cloud_rate_limit_returns_429(monkeypatch):
    monkeypatch.setenv("GTS_RATE_LIMIT_REQUESTS", "1")
    monkeypatch.setenv("GTS_RATE_LIMIT_WINDOW_SECONDS", "60")
    app = server.create_app()
    client = app.test_client()

    first = client.get("/api/startupStatus")
    second = client.get("/api/startupStatus")

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.get_json()["code"] == 429
    assert int(second.headers["Retry-After"]) >= 1


def test_cloud_root_and_health_do_not_require_frontend_bundle():
    app = server.create_app()
    client = app.test_client()

    assert client.get("/").get_json()["status"] == "ok"
    assert client.get("/healthz").get_json() == {"cloudMode": True, "status": "ok"}

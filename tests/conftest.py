"""Shared pytest fixtures for GenshinTextSearch tests."""
import os
import sys

import pytest

# Ensure server/ is on sys.path so that imports like `import config` work.
SERVER_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "server")
SERVER_DIR = os.path.normpath(SERVER_DIR)
if SERVER_DIR not in sys.path:
    sys.path.insert(0, SERVER_DIR)


@pytest.fixture()
def tmp_config(tmp_path, monkeypatch):
    """Provide a temporary config environment that won't touch real files."""
    import config

    # Back up original values
    original = dict(config.config)
    config_file = tmp_path / "config.json"

    # Ensure each test starts from the same baseline.
    config.config.clear()
    config.config.update(original)

    monkeypatch.setattr(config, "CONFIG_FILE", config_file)
    monkeypatch.setattr(config, "RUNTIME_DIR", tmp_path)

    yield config

    # Restore original config dict
    config.config.clear()
    config.config.update(original)

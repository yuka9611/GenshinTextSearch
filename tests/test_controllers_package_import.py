"""Regression tests for the controllers package migration."""

import inspect
import subprocess
import sys


def test_controllers_api_uses_package_facade():
    import controllers.api as api

    assert callable(api._get_controllers)
    source = inspect.getsource(api)
    assert "import controllers as controllers_module" not in source
    assert "from databaseHelper import" not in source


def test_importing_controllers_api_does_not_load_database_helper():
    script = (
        "import sys; "
        "sys.path.insert(0, 'server'); "
        "import controllers.api; "
        "assert 'databaseHelper' not in sys.modules; "
        "assert 'controllers.common' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


def test_server_module_no_longer_uses_dynamic_controller_loader():
    import server

    source = inspect.getsource(server)
    assert "spec_from_file_location('controllers_module'" not in source
    assert "import controllers as controllers_module" not in source

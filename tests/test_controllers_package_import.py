"""Regression tests for the controllers package migration."""

import inspect


def test_controllers_api_uses_package_facade():
    import controllers
    import controllers.api as api

    assert controllers.__file__.endswith("server/controllers/__init__.py")
    assert api.controllers_module is controllers


def test_server_module_no_longer_uses_dynamic_controller_loader():
    import server

    source = inspect.getsource(server)
    assert "spec_from_file_location('controllers_module'" not in source
    assert "import controllers as controllers_module" in source

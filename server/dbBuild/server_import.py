"""Helpers for importing modules from the sibling ``server`` package."""

from __future__ import annotations

import importlib
import os
import sys

SERVER_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), os.pardir))


def import_server_module(module_name: str):
    """Import a module that lives under ``server/``.

    Most dbBuild scripts run with ``server/dbBuild`` on ``sys.path`` only. This
    helper keeps the import fallback in one place instead of duplicating the
    same ``try/except ImportError`` pattern across modules.
    """

    try:
        return importlib.import_module(module_name)
    except ImportError:
        if SERVER_DIR not in sys.path:
            sys.path.insert(0, SERVER_DIR)
        return importlib.import_module(module_name)

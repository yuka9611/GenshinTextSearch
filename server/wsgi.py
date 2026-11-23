"""WSGI entry point for the cloud API service."""

import logging
import os
import threading

import controllers.common as common_controller
import databaseHelper
from server import create_app


app = create_app()

_LOGGER = logging.getLogger(__name__)


def _warmup_runtime() -> None:
    try:
        databaseHelper.warmup_runtime_caches()
    except Exception:
        _LOGGER.warning("Background database cache warmup failed", exc_info=True)
    try:
        common_controller.getAvailableVersionFilters()
    except Exception:
        _LOGGER.warning("Background version filter warmup failed", exc_info=True)


def _start_warmup_thread() -> None:
    if os.environ.get("GTS_DISABLE_WARMUP") == "1":
        return
    worker = threading.Thread(target=_warmup_runtime, name="gts-warmup", daemon=True)
    worker.start()


_start_warmup_thread()

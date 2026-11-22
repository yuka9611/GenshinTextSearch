"""Reuse a SQLite connection within a thread, never across request threads."""

import os
import threading


class ThreadLocalConnection:
    def __init__(self, factory):
        self._factory = factory
        self._local = threading.local()
        # Connection setup may create runtime indexes. Serialize setup only;
        # independent readers must be able to run at the same time.
        self._setup_lock = threading.Lock()

    def _get_connection(self):
        pid = os.getpid()
        connection = getattr(self._local, "connection", None)
        if connection is not None and self._local.pid != pid:
            connection.close()
            connection = None
            del self._local.connection
        if connection is None:
            with self._setup_lock:
                connection = self._factory()
            self._local.connection = connection
            self._local.pid = pid
        return connection

    def close(self):
        connection = getattr(self._local, "connection", None)
        if connection is not None:
            connection.close()
            del self._local.connection

    def __getattr__(self, name):
        return getattr(self._get_connection(), name)

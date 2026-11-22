import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from utils.database_connection import ThreadLocalConnection


def test_parallel_readers_have_independent_connections_and_temporary_tables(tmp_path):
    database = tmp_path / "threaded.db"
    with sqlite3.connect(database) as seed:
        seed.execute("CREATE TABLE texts (content TEXT)")
        seed.execute("INSERT INTO texts VALUES ('shared data')")
    connections = ThreadLocalConnection(lambda: sqlite3.connect(database))
    barrier = threading.Barrier(2)

    def read(value):
        connection = connections._get_connection()
        try:
            connections.execute("CREATE TEMP TABLE request_context (value TEXT)")
            connections.execute("INSERT INTO request_context VALUES (?)", (value,))
            barrier.wait(timeout=3)
            assert connections._get_connection() is connection
            return connections.execute(
                "SELECT value, content FROM request_context, texts"
            ).fetchone()
        finally:
            connections.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert list(executor.map(read, ["Japanese", "English"])) == [
            ("Japanese", "shared data"), ("English", "shared data"),
        ]


def test_close_releases_current_connection_and_next_call_reopens():
    connections = ThreadLocalConnection(lambda: sqlite3.connect(":memory:"))
    original = connections._get_connection()
    connections.close()
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        original.execute("SELECT 1")
    assert connections._get_connection() is not original
    assert connections.execute("SELECT 1").fetchone() == (1,)
    connections.close()


def test_connection_setup_failure_can_be_retried():
    attempts = 0

    def connect():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise sqlite3.OperationalError("unavailable")
        return sqlite3.connect(":memory:")

    connections = ThreadLocalConnection(connect)
    with pytest.raises(sqlite3.OperationalError, match="unavailable"):
        connections.execute("SELECT 1")
    assert connections.execute("SELECT 1").fetchone() == (1,)
    connections.close()

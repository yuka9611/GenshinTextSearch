import os
from contextlib import contextmanager
from itertools import islice


DEFAULT_BATCH_SIZE = max(100, int(os.environ.get("GTS_DB_BATCH_SIZE", "10000")))


def iter_batches(iterable, batch_size: int):
    """Yield list batches from any iterable."""
    iterator = iter(iterable)
    while True:
        batch = list(islice(iterator, batch_size))
        if not batch:
            break
        yield batch


def executemany_batched(cursor, sql: str, rows, batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Execute an INSERT/UPDATE statement in chunks to reduce sqlite overhead."""
    inserted = 0
    for batch in iter_batches(rows, max(1, batch_size)):
        cursor.executemany(sql, batch)
        inserted += len(batch)
    return inserted


def reset_temp_table(cursor, create_sql: str, table_name: str):
    """Create temp table if missing, then clear rows."""
    cursor.execute(create_sql)
    cursor.execute(f"DELETE FROM {table_name}")


def drop_temp_table(cursor, table_name: str):
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


def to_hash_value(raw_hash):
    """Normalize JSON hash keys to int when possible while keeping original fallback."""
    try:
        return int(raw_hash)
    except Exception:
        return raw_hash


# 版本控制相关功能已移至 version_control.py
from version_control import build_versioned_upsert_sql


class BufferedExecutemany:
    """Buffer rows for one or two executemany statements and flush by size."""

    def __init__(
        self,
        cursor,
        primary_sql: str,
        *,
        flush_size: int,
        secondary_sql: str | None = None,
    ):
        self.cursor = cursor
        self.primary_sql = primary_sql
        self.secondary_sql = secondary_sql
        self.flush_size = max(1, int(flush_size))
        self._primary_rows = []
        self._secondary_rows = []

    def add(self, primary_row, secondary_row=None):
        self._primary_rows.append(primary_row)
        if self.secondary_sql is not None and secondary_row is not None:
            self._secondary_rows.append(secondary_row)
        if len(self._primary_rows) >= self.flush_size:
            self.flush()

    def flush(self):
        if self._primary_rows:
            self.cursor.executemany(self.primary_sql, self._primary_rows)
            self._primary_rows = []
        if self.secondary_sql is not None and self._secondary_rows:
            self.cursor.executemany(self.secondary_sql, self._secondary_rows)
            self._secondary_rows = []


@contextmanager
def fast_import_pragmas(conn, enabled: bool = True):
    """
    Temporarily relax sqlite durability settings for faster bulk imports.
    Values are restored at the end of the context.
    """
    if not enabled:
        yield
        return

    cursor = conn.cursor()
    old_settings = {}
    pragma_names = ("synchronous", "temp_store", "cache_size", "journal_mode", "foreign_keys")

    try:
        for name in pragma_names:
            row = cursor.execute(f"PRAGMA {name}").fetchone()
            old_settings[name] = row[0] if row else None

        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA cache_size = -500000")  # 增加缓存大小
        cursor.execute("PRAGMA journal_mode = OFF")  # 禁用日志以提高速度
        cursor.execute("PRAGMA foreign_keys = OFF")  # 禁用外键检查以提高速度
        yield
    finally:
        for name in pragma_names:
            old_value = old_settings.get(name)
            if old_value is not None:
                cursor.execute(f"PRAGMA {name} = {old_value}")
        cursor.close()

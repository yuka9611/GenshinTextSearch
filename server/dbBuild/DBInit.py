import os
import re

from DBConfig import conn


_DBBUILD_DIR = os.path.abspath(os.path.dirname(__file__))
_DDL_PATH = os.path.join(_DBBUILD_DIR, "databaseDDL.sql")
_BASE_SCHEMA_TABLES = (
    "avatar",
    "chapter",
    "dialogue",
    "fetters",
    "fetterStory",
    "langCode",
    "manualTextMap",
    "npc",
    "quest",
    "questTalk",
    "quest_version",
    "readable",
    "readable_meta",
    "subtitle",
    "textMap",
    "voice",
    "fetterVoice",
)


def _read_database_ddl() -> str:
    with open(_DDL_PATH, "r", encoding="utf8") as sql_file:
        return sql_file.read()


def _make_database_ddl_idempotent(sql: str) -> str:
    statements = str(sql)
    statements = re.sub(
        r"(?im)^create table(?!\s+if not exists)\s+",
        "CREATE TABLE IF NOT EXISTS ",
        statements,
    )
    statements = re.sub(
        r"(?im)^create unique index(?!\s+if not exists)\s+",
        "CREATE UNIQUE INDEX IF NOT EXISTS ",
        statements,
    )
    statements = re.sub(
        r"(?im)^create index(?!\s+if not exists)\s+",
        "CREATE INDEX IF NOT EXISTS ",
        statements,
    )
    statements = re.sub(
        r"(?im)^insert into\s+",
        "INSERT OR IGNORE INTO ",
        statements,
    )
    return statements


def ensure_base_schema(required_tables=None) -> bool:
    table_names = tuple(required_tables or _BASE_SCHEMA_TABLES)
    if not table_names:
        return False

    placeholders = ",".join("?" for _ in table_names)
    cursor = conn.cursor()
    try:
        existing_tables = {
            row[0]
            for row in cursor.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})",
                table_names,
            ).fetchall()
        }
    finally:
        cursor.close()

    missing_tables = [table_name for table_name in table_names if table_name not in existing_tables]
    if not missing_tables:
        return False

    ddl = _make_database_ddl_idempotent(_read_database_ddl())
    cursor = conn.cursor()
    try:
        cursor.executescript(ddl)
        conn.commit()
    finally:
        cursor.close()
    return True


def build():
    ensure_base_schema()
    print("Done. Go running DBBuild.py to import more data.")


if __name__ == "__main__":
    build()

import os
import sqlite3

_DBBUILD_DIR = os.path.abspath(os.path.dirname(__file__))
_SERVER_DIR = os.path.abspath(os.path.join(_DBBUILD_DIR, ".."))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SERVER_DIR, ".."))

# Local AnimeGameData checkout path. Defaults to a sibling folder next to this repo.
_default_data_path = os.path.join(_PROJECT_ROOT, "..", "AnimeGameData")
DATA_PATH = os.path.abspath(os.environ.get("GTS_DATA_PATH", _default_data_path))

# TextMap language files path.
LANG_PATH = os.path.join(DATA_PATH, "TextMap")

# Default DB should be server/data.db (same DB used by runtime server).
_default_db_path = os.path.join(_SERVER_DIR, "data.db")
DB_PATH = _default_db_path

# Allow shared use across threads in importer/runtime flows.
conn = sqlite3.connect(DB_PATH, check_same_thread=False)

# Readable files path.
READABLE_PATH = os.path.join(DATA_PATH, "Readable")

# Subtitle files path.
SUBTITLE_PATH = os.path.join(DATA_PATH, "Subtitle")

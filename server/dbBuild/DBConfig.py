import os
import sqlite3

# Local AnimeGameData checkout path.
DATA_PATH = r"C:\Users\yuka9\Downloads\AnimeGameData"

# TextMap language files path.
LANG_PATH = os.path.join(DATA_PATH, "TextMap")

# Default DB should be server/data.db (same DB used by runtime server).
_default_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data.db"))
DB_PATH = _default_db_path
conn = sqlite3.connect(DB_PATH)

# Readable files path.
READABLE_PATH = os.path.join(DATA_PATH, "Readable")

# Subtitle files path.
SUBTITLE_PATH = os.path.join(DATA_PATH, "Subtitle")

import sqlite3
import os

# Dim的解包数据仓库所在的文件夹，如果只是导入语言包不用管
DATA_PATH = r''

# Dim的解包数据的TextMap文件夹位置，请在其中放置需要导入的语言json，并保持其TextMapXX.json的文件名不变
LANG_PATH = os.path.join(DATA_PATH, 'TextMap')

# 导入/生成的数据库的位置，默认为../data.db，如果要新建从头建立数据库建议选一个其他位置
DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")
conn = sqlite3.connect(DB_PATH)

# Dim的解包数据的Readable文件夹位置
READABLE_PATH = os.path.join(DATA_PATH, 'Readable')

# Dim的解包数据的Subtitle文件夹位置
SUBTITLE_PATH = os.path.join(DATA_PATH, 'Subtitle')

# 所有数据库相关的操作请在dbBuild目录下运行脚本！
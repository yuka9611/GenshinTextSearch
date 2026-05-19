"""Startup-path regressions for server.py."""

import subprocess
import sys


def test_startup_asset_prompt_does_not_import_database_helpers():
    script = r'''
import sys
import types

sys.path.insert(0, "server")

tkinter = types.ModuleType("tkinter")
messagebox = types.ModuleType("tkinter.messagebox")
filedialog = types.ModuleType("tkinter.filedialog")

class FakeRoot:
    def withdraw(self): pass
    def attributes(self, *_args): pass
    def destroy(self): pass

tkinter.Tk = FakeRoot
messagebox.askyesno = lambda *_args, **_kwargs: True
messagebox.showerror = lambda *_args, **_kwargs: None
filedialog.askdirectory = lambda *_args, **_kwargs: "/game/GenshinImpact_Data"
tkinter.messagebox = messagebox
tkinter.filedialog = filedialog
sys.modules["tkinter"] = tkinter
sys.modules["tkinter.messagebox"] = messagebox
sys.modules["tkinter.filedialog"] = filedialog

import server

class FakeConfig:
    saved = False
    asset_dir = ""

    @classmethod
    def getAssetDir(cls):
        return cls.asset_dir

    @classmethod
    def isAssetDirValid(cls):
        return bool(cls.asset_dir)

    @classmethod
    def setAssetDir(cls, value):
        cls.asset_dir = value

    @classmethod
    def saveConfig(cls):
        cls.saved = True

server._prompt_for_asset_dir_if_needed(FakeConfig)
assert FakeConfig.asset_dir == "/game/GenshinImpact_Data"
assert FakeConfig.saved is True
assert "controllers.common" not in sys.modules
assert "databaseHelper" not in sys.modules
'''
    subprocess.run([sys.executable, "-c", script], check=True)

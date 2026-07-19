"""Filesystem access abstraction used by shared game-data resolvers."""

from __future__ import annotations

import glob
import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional, Protocol, Sequence

from .talk import TalkCandidate, extract_talk_id


class GameDataAccess(Protocol):
    def read_json(self, relative_path: str) -> Any: ...
    def read_json_glob(self, relative_pattern: str) -> list[Any]: ...
    def glob_paths(self, relative_pattern: str) -> list[str]: ...
    def get_existing_dir(self, relative_path: str) -> Optional[str]: ...
    def get_main_coop_excel_config_data(self) -> Any: ...
    def get_talk_excel_config_data_parts(self) -> list[list[dict[str, Any]]]: ...
    def get_talk_storyboard_candidate(self, talk_id: int) -> Optional[TalkCandidate]: ...
    def get_talk_storyboard_group_output(self, group_id: int) -> Any: ...
    def get_codex_quest_output(self, quest_id: int) -> Any: ...
    def get_coop_output(self, main_coop_id: int) -> Any: ...
    def get_talk_coop_output(self, file_stem: str) -> Any: ...


class FilesystemGameDataAccess:
    def __init__(self, roots: str | os.PathLike[str] | Sequence[str | os.PathLike[str]]):
        if isinstance(roots, (str, os.PathLike)):
            roots = [roots]
        self.roots = tuple(str(Path(root)) for root in roots)
        self._json_cache: dict[str, Any] = {}
        self._glob_cache: dict[str, list[Any]] = {}
        self._storyboard_index: Optional[dict[int, str]] = None

    def clear(self) -> None:
        self._json_cache.clear()
        self._glob_cache.clear()
        self._storyboard_index = None

    def read_json(self, relative_path: str) -> Any:
        if relative_path in self._json_cache:
            return self._json_cache[relative_path]
        for root in self.roots:
            path = os.path.join(root, relative_path)
            if not os.path.isfile(path):
                continue
            try:
                with open(path, encoding="utf-8-sig") as handle:
                    value = json.load(handle)
            except Exception:
                continue
            self._json_cache[relative_path] = value
            return value
        return None

    def read_json_glob(self, relative_pattern: str) -> list[Any]:
        if relative_pattern in self._glob_cache:
            return self._glob_cache[relative_pattern]
        values: list[Any] = []
        seen: set[str] = set()
        for path in self.glob_paths(relative_pattern):
            if path in seen:
                continue
            seen.add(path)
            try:
                with open(path, encoding="utf-8-sig") as handle:
                    values.append(json.load(handle))
            except Exception:
                continue
        self._glob_cache[relative_pattern] = values
        return values

    def glob_paths(self, relative_pattern: str) -> list[str]:
        paths: list[str] = []
        seen: set[str] = set()
        for root in self.roots:
            for path in sorted(glob.glob(os.path.join(root, relative_pattern))):
                if path not in seen:
                    seen.add(path)
                    paths.append(path)
        return paths

    def get_existing_dir(self, relative_path: str) -> Optional[str]:
        for root in self.roots:
            path = os.path.join(root, relative_path)
            if os.path.isdir(path):
                return path
        return None

    def get_main_coop_excel_config_data(self) -> Any:
        return self.read_json("ExcelBinOutput/MainCoopExcelConfigData.json")

    def get_talk_excel_config_data_parts(self) -> list[list[dict[str, Any]]]:
        return [value for value in self.read_json_glob("ExcelBinOutput/TalkExcelConfigData*.json") if isinstance(value, list)]

    def _load_storyboard_index(self) -> dict[int, str]:
        if self._storyboard_index is not None:
            return self._storyboard_index
        result: dict[int, str] = {}
        for path in self.glob_paths("BinOutput/Talk/Storyboard/*.json"):
            relative = self._relative_path(path)
            obj = self.read_json(relative)
            talk_id = extract_talk_id(obj)
            if isinstance(talk_id, int):
                result.setdefault(talk_id, relative)
        self._storyboard_index = result
        return result

    def _relative_path(self, path: str) -> str:
        for root in self.roots:
            try:
                return str(Path(path).relative_to(root)).replace(os.sep, "/")
            except ValueError:
                continue
        return path.replace(os.sep, "/")

    def get_talk_storyboard_candidate(self, talk_id: int) -> Optional[TalkCandidate]:
        relative = f"BinOutput/Talk/Storyboard/{talk_id}.json"
        if not isinstance(self.read_json(relative), dict):
            relative = self._load_storyboard_index().get(talk_id, "")
        if not relative:
            return None
        prefix = "BinOutput/Talk/"
        return TalkCandidate(
            scope="storyboard",
            talk_id=talk_id,
            relative_path=relative[len(prefix):] if relative.startswith(prefix) else relative,
        )

    def get_talk_storyboard_group_output(self, group_id: int) -> Any:
        return self.read_json(f"BinOutput/Talk/StoryboardGroup/{group_id}.json")

    def get_codex_quest_output(self, quest_id: int) -> Any:
        return self.read_json(f"BinOutput/CodexQuest/{quest_id}.json")

    def get_coop_output(self, main_coop_id: int) -> Any:
        return self.read_json(f"BinOutput/Coop/Coop{main_coop_id}.json")

    def get_talk_coop_output(self, file_stem: str) -> Any:
        return self.read_json(f"BinOutput/Talk/Coop/{file_stem}.json")

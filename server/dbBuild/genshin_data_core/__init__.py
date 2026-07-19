"""Shared Genshin game-data parsing used by GTS and BwikiHelper."""

from .compat import BWIKI_COMPAT, GTS_COMPAT, CompatibilityProfile
from .quest import BWIKI_QUEST_PARSER, GTS_QUEST_PARSER, QuestFields, QuestParser

__all__ = [
    "BWIKI_COMPAT",
    "GTS_COMPAT",
    "CompatibilityProfile",
    "BWIKI_QUEST_PARSER",
    "GTS_QUEST_PARSER",
    "QuestFields",
    "QuestParser",
]

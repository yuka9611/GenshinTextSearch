"""Compatibility profiles for consumers of the shared game-data parser."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CompatibilityProfile:
    name: str
    quest_brief_66_chapter_keys: tuple[str, ...]
    allow_subquest_id_fallback: bool
    dedupe_top_level_talk_ids: bool
    include_legacy_hangout_range: bool
    anecdote_extended_aliases: bool
    anecdote_story_ids_as_legacy_fallback: bool
    detect_legacy_storyboard_container: bool


GTS_COMPAT = CompatibilityProfile(
    name="gts",
    quest_brief_66_chapter_keys=("JILHIMLENJK",),
    allow_subquest_id_fallback=False,
    dedupe_top_level_talk_ids=False,
    include_legacy_hangout_range=True,
    anecdote_extended_aliases=False,
    anecdote_story_ids_as_legacy_fallback=False,
    detect_legacy_storyboard_container=True,
)

BWIKI_COMPAT = CompatibilityProfile(
    name="bwiki",
    quest_brief_66_chapter_keys=("DMKHKJJFOAA", "JILHIMLENJK"),
    allow_subquest_id_fallback=True,
    dedupe_top_level_talk_ids=True,
    include_legacy_hangout_range=False,
    anecdote_extended_aliases=True,
    anecdote_story_ids_as_legacy_fallback=True,
    detect_legacy_storyboard_container=False,
)

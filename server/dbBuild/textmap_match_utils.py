from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from difflib import SequenceMatcher

from import_utils import to_hash_value
from server_import import import_server_module

is_short_generic_text = import_server_module("quest_text_filters").is_short_generic_text


_SCALAR_TYPE_TAGS = {
    str: "str",
    int: "int",
    float: "float",
    bool: "bool",
    type(None): "none",
}
_TEXTMAP_MISSING = object()
TEXTMAP_MATCH_KIND_SAME_CONTENT = "same_content"
TEXTMAP_MATCH_KIND_SAME_HASH_CHANGED = "same_hash_changed"
TEXTMAP_MATCH_KIND_CROSS_HASH_SIMILAR = "cross_hash_similar"
TEXTMAP_MATCH_KIND_NEW = "new"
DEFAULT_TEXTMAP_SIMILARITY_THRESHOLD = 0.60
DEFAULT_TEXTMAP_SIMILARITY_MARGIN = 0.08
DEFAULT_TEXTMAP_MAX_SIMILARITY_PAIRS = 50_000
DEFAULT_TEXTMAP_MAX_SIMILARITY_CANDIDATES_PER_CURRENT = 1_024
_TEXTMAP_SIMILARITY_ANCHOR_SIZE = 4


def normalize_textmap_value_for_compare(value: object) -> object:
    if isinstance(value, str):
        return value.replace("\r\n", "\n").replace("\r", "\n")
    return value


def textmap_values_match(left: object, right: object) -> bool:
    return normalize_textmap_value_for_compare(left) == normalize_textmap_value_for_compare(right)


def textmap_content_key(value: object) -> tuple[str, object]:
    normalized = normalize_textmap_value_for_compare(value)
    value_type = type(normalized)
    type_tag = _SCALAR_TYPE_TAGS.get(value_type)
    if type_tag is not None:
        return (type_tag, normalized)

    try:
        serialized = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except Exception:
        serialized = repr(normalized)
    return ("json", serialized)


def stringify_textmap_value_for_similarity(value: object) -> str:
    normalized = normalize_textmap_value_for_compare(value)
    if isinstance(normalized, str):
        return normalized
    value_type = type(normalized)
    type_tag = _SCALAR_TYPE_TAGS.get(value_type)
    if type_tag is not None:
        return f"{type_tag}:{normalized}"
    try:
        return json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except Exception:
        return repr(normalized)


def textmap_similarity_score(left: object, right: object) -> float:
    if textmap_values_match(left, right):
        return 1.0
    left_text = stringify_textmap_value_for_similarity(left)
    right_text = stringify_textmap_value_for_similarity(right)
    if not left_text or not right_text:
        return 0.0
    matcher = SequenceMatcher(a=left_text, b=right_text)
    if matcher.real_quick_ratio() <= 0.0:
        return 0.0
    return matcher.ratio()


@dataclass(frozen=True)
class TextMapContentIndex:
    hash_to_content: dict[int, object]
    hashes_by_content: dict[tuple[str, object], tuple[int, ...]]


@dataclass(frozen=True)
class TextMapLineageState:
    snapshot_hash: int
    content: object


@dataclass(frozen=True)
class TextMapLineageMatch:
    current_hash: int
    predecessor_hash: int | None
    predecessor_content: object | None
    match_kind: str
    score: float | None = None

    @property
    def starts_current_content_here(self) -> bool:
        return self.match_kind != TEXTMAP_MATCH_KIND_SAME_CONTENT


@dataclass(frozen=True)
class TextMapSimilarityCandidateIndex:
    text_by_hash: dict[int, str]
    length_by_hash: dict[int, int]
    hashes_by_anchor: dict[tuple[str, str], tuple[int, ...]]
    hashes_by_length: dict[int, tuple[int, ...]]


def build_textmap_content_index(
    textmap_obj: Mapping[object, object] | None,
    *,
    allowed_hashes: Collection[int] | None = None,
) -> TextMapContentIndex:
    hash_to_content: dict[int, object] = {}
    hashes_by_content: dict[tuple[str, object], list[int]] = defaultdict(list)
    allowed = set(allowed_hashes) if allowed_hashes is not None else None

    if not textmap_obj:
        return TextMapContentIndex(hash_to_content={}, hashes_by_content={})

    for raw_hash, content in textmap_obj.items():
        try:
            hash_value = int(to_hash_value(raw_hash))
        except Exception:
            continue
        if allowed is not None and hash_value not in allowed:
            continue
        hash_to_content[hash_value] = content
        hashes_by_content[textmap_content_key(content)].append(hash_value)

    frozen_hashes_by_content = {
        content_key: tuple(sorted(hash_values))
        for content_key, hash_values in hashes_by_content.items()
    }
    return TextMapContentIndex(
        hash_to_content=hash_to_content,
        hashes_by_content=frozen_hashes_by_content,
    )


def build_textmap_lineage_states(
    textmap_obj: Mapping[object, object] | None,
    *,
    allowed_hashes: Collection[int] | None = None,
) -> dict[int, TextMapLineageState]:
    allowed = set(allowed_hashes) if allowed_hashes is not None else None
    states: dict[int, TextMapLineageState] = {}
    if not textmap_obj:
        return states

    for raw_hash, content in textmap_obj.items():
        try:
            hash_value = int(to_hash_value(raw_hash))
        except Exception:
            continue
        if allowed is not None and hash_value not in allowed:
            continue
        states[hash_value] = TextMapLineageState(snapshot_hash=hash_value, content=content)
    return states


def _similarity_anchor_keys(text: str) -> tuple[tuple[str, str], ...]:
    if not text:
        return tuple()

    window = min(_TEXTMAP_SIMILARITY_ANCHOR_SIZE, len(text))
    anchors: list[tuple[str, str]] = [("prefix", text[:window]), ("suffix", text[-window:])]
    middle_start = max(0, (len(text) - window) // 2)
    anchors.append(("middle", text[middle_start : middle_start + window]))

    seen: set[tuple[str, str]] = set()
    deduped: list[tuple[str, str]] = []
    for anchor in anchors:
        if anchor in seen:
            continue
        seen.add(anchor)
        deduped.append(anchor)
    return tuple(deduped)


def _build_textmap_similarity_candidate_index(
    previous_rows: Collection[tuple[int, object]],
) -> TextMapSimilarityCandidateIndex:
    text_by_hash: dict[int, str] = {}
    length_by_hash: dict[int, int] = {}
    hashes_by_anchor: dict[tuple[str, str], list[int]] = defaultdict(list)
    hashes_by_length: dict[int, list[int]] = defaultdict(list)

    for previous_hash, previous_content in previous_rows:
        text = stringify_textmap_value_for_similarity(previous_content)
        text_by_hash[previous_hash] = text
        text_length = len(text)
        length_by_hash[previous_hash] = text_length
        hashes_by_length[text_length].append(previous_hash)
        for anchor_key in _similarity_anchor_keys(text):
            hashes_by_anchor[anchor_key].append(previous_hash)

    return TextMapSimilarityCandidateIndex(
        text_by_hash=text_by_hash,
        length_by_hash=length_by_hash,
        hashes_by_anchor={
            anchor_key: tuple(hash_values)
            for anchor_key, hash_values in hashes_by_anchor.items()
        },
        hashes_by_length={
            text_length: tuple(hash_values)
            for text_length, hash_values in hashes_by_length.items()
        },
    )


def _select_textmap_similarity_candidate_hashes(
    current_text: str,
    candidate_index: TextMapSimilarityCandidateIndex,
    *,
    max_candidates: int,
) -> tuple[int, ...]:
    if not current_text or max_candidates <= 0:
        return tuple()

    current_length = len(current_text)
    length_delta = max(2, min(64, current_length // 8))
    anchor_hits: dict[int, int] = {}

    for anchor_key in _similarity_anchor_keys(current_text):
        for previous_hash in candidate_index.hashes_by_anchor.get(anchor_key, ()):
            previous_length = candidate_index.length_by_hash.get(previous_hash, 0)
            if abs(previous_length - current_length) > length_delta:
                continue
            anchor_hits[previous_hash] = anchor_hits.get(previous_hash, 0) + 1

    if anchor_hits:
        ranked_hashes = sorted(
            anchor_hits.keys(),
            key=lambda previous_hash: (
                -anchor_hits[previous_hash],
                abs(candidate_index.length_by_hash.get(previous_hash, 0) - current_length),
                previous_hash,
            ),
        )
        return tuple(ranked_hashes[:max_candidates])

    near_length_hashes: list[int] = []
    for text_length in range(
        max(0, current_length - length_delta),
        current_length + length_delta + 1,
    ):
        for previous_hash in candidate_index.hashes_by_length.get(text_length, ()):
            near_length_hashes.append(previous_hash)
            if len(near_length_hashes) >= max_candidates:
                return tuple(near_length_hashes)
    return tuple(near_length_hashes)


def allocate_textmap_current_matches(
    current_index: TextMapContentIndex,
    history_index: TextMapContentIndex | None,
) -> dict[int, int]:
    if history_index is None:
        return {}

    matched: dict[int, int] = {}
    consumed_history_hashes: set[int] = set()

    for hash_value in sorted(current_index.hash_to_content.keys()):
        history_content = history_index.hash_to_content.get(hash_value, _TEXTMAP_MISSING)
        if history_content is _TEXTMAP_MISSING:
            continue
        current_content = current_index.hash_to_content[hash_value]
        if not textmap_values_match(current_content, history_content):
            continue
        matched[hash_value] = hash_value
        consumed_history_hashes.add(hash_value)

    consumed_current_hashes = set(matched.keys())
    for content_key in sorted(current_index.hashes_by_content.keys()):
        current_hashes = current_index.hashes_by_content[content_key]
        history_hashes = history_index.hashes_by_content.get(content_key)
        if not history_hashes:
            continue
        remaining_current_hashes = [
            hash_value for hash_value in current_hashes if hash_value not in consumed_current_hashes
        ]
        if not remaining_current_hashes:
            continue
        remaining_history_hashes = [
            hash_value for hash_value in history_hashes if hash_value not in consumed_history_hashes
        ]
        if not remaining_history_hashes:
            continue
        current_content = current_index.hash_to_content.get(remaining_current_hashes[0])
        if is_short_generic_text(current_content):
            continue

        for current_hash, history_hash in zip(remaining_current_hashes, remaining_history_hashes):
            matched[current_hash] = history_hash
            consumed_current_hashes.add(current_hash)
            consumed_history_hashes.add(history_hash)

    return matched


def match_textmap_lineage_to_previous(
    current_states: Mapping[int, TextMapLineageState],
    previous_obj: Mapping[object, object] | None,
    *,
    previous_index: TextMapContentIndex | None = None,
    similarity_threshold: float = DEFAULT_TEXTMAP_SIMILARITY_THRESHOLD,
    similarity_margin: float = DEFAULT_TEXTMAP_SIMILARITY_MARGIN,
    max_similarity_pairs: int = DEFAULT_TEXTMAP_MAX_SIMILARITY_PAIRS,
) -> dict[int, TextMapLineageMatch]:
    if not current_states:
        return {}

    previous_index = previous_index or build_textmap_content_index(previous_obj)
    previous_hash_to_content = previous_index.hash_to_content
    if not previous_hash_to_content:
        return {
            current_hash: TextMapLineageMatch(
                current_hash=current_hash,
                predecessor_hash=None,
                predecessor_content=None,
                match_kind=TEXTMAP_MATCH_KIND_NEW,
            )
            for current_hash in sorted(current_states.keys())
        }
    matched: dict[int, TextMapLineageMatch] = {}
    consumed_previous_hashes: set[int] = set()
    remaining_current_hashes: list[int] = []
    previous_get = previous_hash_to_content.get
    values_match = textmap_values_match

    for current_hash, state in current_states.items():
        previous_content = previous_get(state.snapshot_hash, _TEXTMAP_MISSING)
        if previous_content is _TEXTMAP_MISSING:
            remaining_current_hashes.append(current_hash)
            continue
        if not values_match(state.content, previous_content):
            remaining_current_hashes.append(current_hash)
            continue
        matched[current_hash] = TextMapLineageMatch(
            current_hash=current_hash,
            predecessor_hash=state.snapshot_hash,
            predecessor_content=previous_content,
            match_kind=TEXTMAP_MATCH_KIND_SAME_CONTENT,
        )
        consumed_previous_hashes.add(state.snapshot_hash)

    unmatched_by_content: dict[tuple[str, object], list[int]] = defaultdict(list)
    for current_hash in remaining_current_hashes:
        state = current_states[current_hash]
        unmatched_by_content[textmap_content_key(state.content)].append(current_hash)

    for content_key in sorted(unmatched_by_content.keys()):
        previous_hashes = previous_index.hashes_by_content.get(content_key)
        if not previous_hashes:
            continue
        remaining_previous_hashes = [
            previous_hash
            for previous_hash in previous_hashes
            if previous_hash not in consumed_previous_hashes
        ]
        if not remaining_previous_hashes:
            continue
        current_content = current_states[unmatched_by_content[content_key][0]].content
        if is_short_generic_text(current_content):
            continue
        for current_hash, previous_hash in zip(sorted(unmatched_by_content[content_key]), remaining_previous_hashes):
            previous_content = previous_get(previous_hash)
            matched[current_hash] = TextMapLineageMatch(
                current_hash=current_hash,
                predecessor_hash=previous_hash,
                predecessor_content=previous_content,
                match_kind=TEXTMAP_MATCH_KIND_SAME_CONTENT,
            )
            consumed_previous_hashes.add(previous_hash)

    remaining_current_hashes = [
        current_hash for current_hash in remaining_current_hashes if current_hash not in matched
    ]
    for current_hash in remaining_current_hashes:
        state = current_states[current_hash]
        if state.snapshot_hash in consumed_previous_hashes:
            continue
        previous_content = previous_get(state.snapshot_hash, _TEXTMAP_MISSING)
        if previous_content is _TEXTMAP_MISSING:
            continue
        matched[current_hash] = TextMapLineageMatch(
            current_hash=current_hash,
            predecessor_hash=state.snapshot_hash,
            predecessor_content=previous_content,
            match_kind=TEXTMAP_MATCH_KIND_SAME_HASH_CHANGED,
        )
        consumed_previous_hashes.add(state.snapshot_hash)

    remaining_current_hashes = [
        current_hash for current_hash in remaining_current_hashes if current_hash not in matched
    ]
    if remaining_current_hashes:
        if len(remaining_current_hashes) * len(previous_hash_to_content) <= max_similarity_pairs:
            previous_items_iter = sorted(previous_hash_to_content.items())
        else:
            previous_items_iter = previous_hash_to_content.items()
        remaining_previous_rows = [
            (previous_hash, previous_content)
            for previous_hash, previous_content in previous_items_iter
            if previous_hash not in consumed_previous_hashes
        ]
    else:
        remaining_previous_rows = []
    if remaining_current_hashes:
        candidate_limit_per_current = min(
            DEFAULT_TEXTMAP_MAX_SIMILARITY_CANDIDATES_PER_CURRENT,
            max(
                1,
                max_similarity_pairs // max(1, len(remaining_current_hashes)),
            ),
        )
    else:
        candidate_limit_per_current = 0

    if remaining_current_hashes and remaining_previous_rows:
        current_text_cache = {
            current_hash: stringify_textmap_value_for_similarity(current_states[current_hash].content)
            for current_hash in remaining_current_hashes
        }
        current_is_short_generic = {
            current_hash: is_short_generic_text(current_states[current_hash].content)
            for current_hash in remaining_current_hashes
        }
        previous_text_cache: dict[int, str]
        candidate_rows_by_current: dict[int, tuple[tuple[int, object], ...]]
        filtered_previous_rows = [
            (previous_hash, previous_content)
            for previous_hash, previous_content in remaining_previous_rows
            if not is_short_generic_text(previous_content)
        ]
        filtered_pair_count = len(remaining_current_hashes) * len(filtered_previous_rows)
        if filtered_pair_count <= max_similarity_pairs:
            previous_text_cache = {
                previous_hash: stringify_textmap_value_for_similarity(previous_content)
                for previous_hash, previous_content in filtered_previous_rows
            }
            candidate_rows = tuple(filtered_previous_rows)
            candidate_rows_by_current = {
                current_hash: tuple() if current_is_short_generic[current_hash] else candidate_rows
                for current_hash in remaining_current_hashes
            }
        else:
            candidate_index = _build_textmap_similarity_candidate_index(filtered_previous_rows)
            previous_text_cache = dict(candidate_index.text_by_hash)
            previous_row_map = {
                previous_hash: previous_content
                for previous_hash, previous_content in filtered_previous_rows
            }
            candidate_rows_by_current = {}
            for current_hash in remaining_current_hashes:
                if current_is_short_generic[current_hash]:
                    continue
                candidate_hashes = _select_textmap_similarity_candidate_hashes(
                    current_text_cache[current_hash],
                    candidate_index,
                    max_candidates=candidate_limit_per_current,
                )
                if not candidate_hashes:
                    continue
                candidate_rows_by_current[current_hash] = tuple(
                    (previous_hash, previous_row_map[previous_hash])
                    for previous_hash in candidate_hashes
                    if previous_hash in previous_row_map
                )
        best_previous_for_current: dict[int, tuple[float, int, float]] = {}
        best_current_for_previous: dict[int, tuple[float, int]] = {}

        for current_hash in remaining_current_hashes:
            current_text = current_text_cache[current_hash]
            candidate_rows = candidate_rows_by_current.get(current_hash, ())
            if not candidate_rows:
                continue
            best_score = 0.0
            best_previous_hash: int | None = None
            second_best_score = 0.0
            for previous_hash, _previous_content in candidate_rows:
                previous_text = previous_text_cache[previous_hash]
                if not current_text or not previous_text:
                    score = 0.0
                else:
                    matcher = SequenceMatcher(a=current_text, b=previous_text)
                    if matcher.real_quick_ratio() < similarity_threshold:
                        continue
                    score = matcher.ratio()
                if score < similarity_threshold:
                    continue
                if score > best_score:
                    second_best_score = best_score
                    best_score = score
                    best_previous_hash = previous_hash
                elif score > second_best_score:
                    second_best_score = score
                previous_best = best_current_for_previous.get(previous_hash)
                if (
                    previous_best is None
                    or score > previous_best[0]
                    or (score == previous_best[0] and current_hash < previous_best[1])
                ):
                    best_current_for_previous[previous_hash] = (score, current_hash)
            if best_previous_hash is not None:
                best_previous_for_current[current_hash] = (
                    best_score,
                    best_previous_hash,
                    second_best_score,
                )

        accepted_matches: list[tuple[float, int, int]] = []
        for current_hash, (best_score, previous_hash, second_best_score) in best_previous_for_current.items():
            previous_best = best_current_for_previous.get(previous_hash)
            if previous_best is None or previous_best[1] != current_hash:
                continue
            if best_score - second_best_score < similarity_margin:
                continue
            accepted_matches.append((best_score, current_hash, previous_hash))

        for score, current_hash, previous_hash in sorted(
            accepted_matches,
            key=lambda item: (-item[0], item[1], item[2]),
        ):
            if current_hash in matched or previous_hash in consumed_previous_hashes:
                continue
            previous_content = previous_get(previous_hash)
            matched[current_hash] = TextMapLineageMatch(
                current_hash=current_hash,
                predecessor_hash=previous_hash,
                predecessor_content=previous_content,
                match_kind=TEXTMAP_MATCH_KIND_CROSS_HASH_SIMILAR,
                score=score,
            )
            consumed_previous_hashes.add(previous_hash)

    for current_hash in remaining_current_hashes:
        if current_hash in matched:
            continue
        matched[current_hash] = TextMapLineageMatch(
            current_hash=current_hash,
            predecessor_hash=None,
            predecessor_content=None,
            match_kind=TEXTMAP_MATCH_KIND_NEW,
        )

    return matched

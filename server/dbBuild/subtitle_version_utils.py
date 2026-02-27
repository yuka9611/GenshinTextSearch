from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence


_MISSING = object()


def _analyze_same_key_changes(
    old_content_by_key: Mapping[str, str | None],
    new_key_content_pairs: Sequence[tuple[str, str | None]],
) -> tuple[set[str], list[str | None]]:
    exact_unchanged_keys: set[str] = set()
    changed_same_key_old_contents: list[str | None] = []
    for key, new_content in new_key_content_pairs:
        old_content = old_content_by_key.get(key, _MISSING)
        if old_content is _MISSING:
            continue
        if old_content == new_content:
            exact_unchanged_keys.add(key)
        else:
            changed_same_key_old_contents.append(old_content)  # type: ignore
    return exact_unchanged_keys, changed_same_key_old_contents


def _consume_counter(counter: Counter, value):
    count = counter.get(value, 0)
    if count <= 0:
        return False
    if count == 1:
        del counter[value]
    else:
        counter[value] = count - 1
    return True


def subtitle_text_changed_keys(old_rows: Mapping[str, str | None], new_rows: Mapping[str, str | None]) -> list[str]:
    """
    Return subtitle keys whose TEXT changed.
    Key-only movement caused by start/end timestamp changes is treated as unchanged.
    """
    old_content_by_key = dict(old_rows)
    new_pairs = list(new_rows.items())
    exact_unchanged_keys, changed_same_key_old_contents = _analyze_same_key_changes(
        old_content_by_key,
        new_pairs,
    )

    reusable_old_content = Counter()
    for key, old_content in old_content_by_key.items():
        if key in exact_unchanged_keys:
            continue
        reusable_old_content[old_content] += 1
    for old_content in changed_same_key_old_contents:
        _consume_counter(reusable_old_content, old_content)

    changed_keys: list[str] = []
    for key, new_content in new_pairs:
        old_content = old_content_by_key.get(key, _MISSING)
        if old_content is not _MISSING:
            if old_content != new_content:
                changed_keys.append(key)
            continue

        if _consume_counter(reusable_old_content, new_content):
            # Same content moved to another subtitleKey (timestamp-only movement).
            continue
        changed_keys.append(key)

    return changed_keys


def assign_subtitle_versions_by_text(
    existing_rows: list[tuple[str, str | None, int | None, int | None]],
    new_rows: list[tuple[str, float, float, str]],
    fallback_version_id: int | None,
) -> list[tuple[str, float, float, str, int | None, int | None]]:
    """
    Assign (created_version_id, updated_version_id) for new subtitle rows.
    Rows judged as timestamp-only movement keep old version ids.
    """
    old_by_key: dict[str, tuple[str | None, int | None, int | None]] = {}
    old_content_by_key: dict[str, str | None] = {}
    for key, old_content, created_id, updated_id in existing_rows:
        old_by_key[key] = (old_content, created_id, updated_id)
        old_content_by_key[key] = old_content

    new_pairs = [(key, text_content) for key, _s, _e, text_content in new_rows]
    new_rows_dict = {key: text for key, text in new_pairs}
    text_changed_keys = set(subtitle_text_changed_keys(old_content_by_key, new_rows_dict))
    exact_unchanged_keys, changed_same_key_old_contents = _analyze_same_key_changes(
        old_content_by_key,
        new_pairs,
    )

    reusable_versions: dict[str | None, list[tuple[int | None, int | None]]] = defaultdict(list)
    for key, old_content, created_id, updated_id in existing_rows:
        if key in exact_unchanged_keys:
            continue
        reusable_versions[old_content].append((created_id, updated_id))
    for old_content in changed_same_key_old_contents:
        candidates = reusable_versions.get(old_content)
        if candidates:
            candidates.pop()
            if not candidates:
                del reusable_versions[old_content]

    assigned_rows: list[tuple[str, float, float, str, int | None, int | None]] = []
    for key, start_time, end_time, text_content in new_rows:
        old_entry = old_by_key.get(key)
        if old_entry is not None:
            old_content, created_id, updated_id = old_entry
            if key not in text_changed_keys and old_content == text_content:
                assigned_rows.append((key, start_time, end_time, text_content, created_id, updated_id))
                continue
            assigned_rows.append(
                (
                    key,
                    start_time,
                    end_time,
                    text_content,
                    fallback_version_id,
                    fallback_version_id,
                )
            )
            continue

        if key not in text_changed_keys:
            candidates = reusable_versions.get(text_content)
            if candidates:
                created_id, updated_id = candidates.pop()
                if not candidates:
                    del reusable_versions[text_content]
                assigned_rows.append((key, start_time, end_time, text_content, created_id, updated_id))
                continue

        assigned_rows.append(
            (
                key,
                start_time,
                end_time,
                text_content,
                fallback_version_id,
                fallback_version_id,
            )
        )

    return assigned_rows

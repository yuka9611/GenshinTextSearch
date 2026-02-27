def readable_text_changed(old_text: str | None, new_text: str | None) -> bool:
    return (old_text or "") != (new_text or "")


def assign_readable_versions_by_text(
    existing_row: tuple[str | None, int | None, int | None] | None,
    new_content: str,
    fallback_version_id: int | None,
) -> tuple[int | None, int | None]:
    """
    Assign (created_version_id, updated_version_id) by text comparison.
    If content is unchanged, keep existing versions; otherwise use fallback version.
    """
    if existing_row is None:
        return fallback_version_id, fallback_version_id

    old_content, old_created, old_updated = existing_row
    if not readable_text_changed(old_content, new_content):
        return old_created, old_updated

    return fallback_version_id, fallback_version_id

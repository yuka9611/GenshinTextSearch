import re


_TEXTMAP_BASE_STEM_RE = re.compile(r"^(?:TextMap|Text)([A-Za-z0-9_]+)$", re.IGNORECASE)
_TEXTMAP_SPLIT_SUFFIX_RE = re.compile(r"^(?P<stem>.+?)_(?P<part>\d+)$")
_LANG_ALIASES = {
    # Historical naming in some commits.
    "JA": "JP",
    "KO": "KR",
}


def parse_textmap_file_name(file_name: str) -> tuple[str, int | None] | None:
    """
    Parse TextMap/Text json file names and normalize to canonical base map name.

    Supported patterns:
    - TextMap<lang>.json
    - TextMap<lang>_<number>.json
    - Text<lang>.json
    - Text<lang>_<number>.json

    Returns:
    - (canonical_base_name, split_part_index_or_none) on success
    - None when file name is unsupported
    """
    if not isinstance(file_name, str):
        return None

    text = file_name.strip()
    if not text or not text.lower().endswith(".json"):
        return None

    stem = text[:-5]
    split_part: int | None = None
    split_match = _TEXTMAP_SPLIT_SUFFIX_RE.match(stem)
    if split_match:
        stem = split_match.group("stem")
        split_part = int(split_match.group("part"))

    base_match = _TEXTMAP_BASE_STEM_RE.match(stem)
    if not base_match:
        return None

    lang = base_match.group(1).upper()
    lang = _LANG_ALIASES.get(lang, lang)
    canonical_base = f"TextMap{lang}.json"
    return canonical_base, split_part


def textmap_file_sort_key(file_name: str) -> tuple[int, int, str]:
    """
    Sort unsuffixed base file first, then split parts by numeric order.
    Unknown names are sorted to the end.
    """
    parsed = parse_textmap_file_name(file_name)
    if parsed is None:
        return (2, 0, file_name.lower())

    _, split_part = parsed
    if split_part is None:
        return (0, 0, file_name.lower())
    return (1, split_part, file_name.lower())

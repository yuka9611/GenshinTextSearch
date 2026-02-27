_QUEST_SCHEMA_VARIANTS = [
    {
        "quest_id": "id",
        "title_hash": "titleTextMapHash",
        "chapter_id": "chapterId",
        "chapter_id_fallback": None,
        "talks_key": "talks",
        "talk_id": "id",
    },
    {
        "quest_id": "ILHDNJDDEOP",
        "title_hash": "MMOEEOFGHHG",
        "chapter_id": "IBNCKLKHAKG",
        "chapter_id_fallback": None,
        "talks_key": "IBEGAHMEABP",
        "talk_id": "ILHDNJDDEOP",
    },
    {
        "quest_id": "BLKKAMEMBBJ",
        "title_hash": "DMLOMLNJCNA",
        "chapter_id": "KDKGIPFDENG",
        "chapter_id_fallback": None,
        "talks_key": "DGJMIPFDEOF",
        "talk_id": "BLKKAMEMBBJ",
    },
    {
        "quest_id": "FJIMHCGKKPJ",
        "title_hash": "HMPOGBDMBOK",
        "chapter_id": "NKEKKINIKEB",
        "chapter_id_fallback": "ODOCBCAGDJA",
        "talks_key": "DCHHEHNNEOO",
        "talk_id": "BPMABFNPCMI",
    },
]


def _get_quest_schema(obj: dict):
    for schema in _QUEST_SCHEMA_VARIANTS:
        if schema["quest_id"] in obj:
            return schema
    return None


def extract_quest_row(obj: dict):
    """
    Return (quest_id, title_hash, chapter_id) from one quest object.
    Returns None when schema is unsupported.
    """
    if not isinstance(obj, dict):
        return None
    schema = _get_quest_schema(obj)
    if schema is None:
        return None
    quest_id = obj[schema["quest_id"]]
    title_hash = obj.get(schema["title_hash"])
    chapter_id = obj.get(schema["chapter_id"])
    if chapter_id is None and schema["chapter_id_fallback"]:
        chapter_id = obj.get(schema["chapter_id_fallback"])
    return quest_id, title_hash, chapter_id


def extract_quest_id(obj: dict):
    row = extract_quest_row(obj)
    if row is None:
        return None
    return row[0]


def extract_quest_talk_ids(obj: dict) -> list:
    """Return talk IDs from quest object across supported schemas."""
    if not isinstance(obj, dict):
        return []
    schema = _get_quest_schema(obj)
    if schema is None:
        return []
    talks = obj.get(schema["talks_key"])
    if not isinstance(talks, list):
        return []
    talk_id_key = schema["talk_id"]
    result = []
    for talk in talks:
        if isinstance(talk, dict) and talk_id_key in talk:
            result.append(talk[talk_id_key])
    return result

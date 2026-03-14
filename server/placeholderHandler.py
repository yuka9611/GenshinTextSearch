import re

import databaseHelper


_WANDERER_AVATAR_ID = 10000075
_TRAVELLER_AVATAR_ID = 10000005
_LITTLE_ONE_NAME = "\u5c0f\u5bb6\u4f19"


def _replace_with_names(text: str, playerIsMale, lang: int, wander_name: str, traveller_name: str) -> str:
    def replace_twin_text(match: "re.Match") -> str:
        male_text = match.group(1)
        female_text = match.group(2)
        if playerIsMale == "both":
            return f"{{{male_text}/{female_text}}}"
        return male_text if playerIsMale else female_text

    def replace_twin_text_reverse(match: "re.Match") -> str:
        female_text = match.group(1)
        male_text = match.group(2)
        if playerIsMale == "both":
            return f"{{{male_text}/{female_text}}}"
        return male_text if playerIsMale else female_text

    def replace_sex_pro(match: "re.Match") -> str:
        if playerIsMale == "both":
            male_text = databaseHelper.getManualTextMap(match.group(2), lang) or ""
            female_text = databaseHelper.getManualTextMap(match.group(3), lang) or ""
            return f"{{{male_text}/{female_text}}}"

        is_mate = match.group(1) == "MATE"
        placeholder_id = match.group(3) if is_mate == playerIsMale else match.group(2)
        result = databaseHelper.getManualTextMap(placeholder_id, lang)
        return result if result is not None else ""

    normalized = re.sub(r"\{M#(.*?)}\{F#(.*?)}", replace_twin_text, text)
    normalized = re.sub(r"\{F#(.*?)}\{M#(.*?)}", replace_twin_text_reverse, normalized)
    normalized = re.sub(r"\{(.*?)AVATAR#SEXPRO\[(.*?)\|(.*?)]}", replace_sex_pro, normalized)
    normalized = re.sub(r"\{REALNAME\[ID\(1\)\|HOSTONLY\(true\)]}", wander_name, normalized)
    normalized = re.sub(r"#\{REALNAME\[ID\(2\)\|SHOWHOST\(true\)]}", _LITTLE_ONE_NAME, normalized)
    normalized = re.sub(r"\{REALNAME\[ID\(2\)\|HOSTONLY\(true\)]}", _LITTLE_ONE_NAME, normalized)
    normalized = re.sub(r"\{NICKNAME}", traveller_name, normalized)
    if normalized.startswith("#"):
        return normalized[1:]
    return normalized


def _normalize_special_name(raw_text: str | None, playerIsMale, lang: int) -> str:
    if not raw_text:
        return ""
    return _replace_with_names(raw_text, playerIsMale, lang, "", "")


def replace(textMap: str | None, playerIsMale, lang: int):
    if textMap is None:
        return None
    if textMap == "":
        return ""

    wander_name = _normalize_special_name(
        databaseHelper.getCharacterNameRaw(_WANDERER_AVATAR_ID, lang),
        playerIsMale,
        lang,
    )
    traveller_name = _normalize_special_name(
        databaseHelper.getCharacterNameRaw(_TRAVELLER_AVATAR_ID, lang),
        playerIsMale,
        lang,
    )
    return _replace_with_names(textMap, playerIsMale, lang, wander_name, traveller_name)


if __name__ == "__main__":
    print(
        replace(
            "#\u55ef\uff0c{NICKNAME}{M#\u4ed6\u4eec}{F#\u5979\u4eec}\u4eca\u5929\u4e5f\u4f1a\u6765\u53c2\u52a0\u5e86\u795d\u6d3b\u52a8\u3002\n"
            "{PLAYERAVATAR#SEXPRO[INFO_MALE_PRONOUN_HE|INFO_FEMALE_PRONOUN_SHE]}\u662f\u7279\u522b\u7684\uff0c"
            "\u4e0d\u9700\u8981\u795e\u4e4b\u773c\u4e5f\u53ef\u4ee5\u4f7f\u7528\u5143\u7d20\u529b\u3002"
            "{REALNAME[ID(1)|HOSTONLY(true)]}",
            True,
            1,
        )
    )

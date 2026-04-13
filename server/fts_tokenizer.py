import re

CHINESE_LANG_CODES = {1, 2}
_SUPPORTED_SEGMENTERS = {"auto", "jieba", "char_bigram", "none"}
_QUOTE_PAIRS = (
    ('"', '"'),
    ("'", "'"),
    ("“", "”"),
    ("‘", "’"),
    ("「", "」"),
    ("『", "』"),
    ("《", "》"),
    ("〈", "〉"),
    ("«", "»"),
    ("‹", "›"),
)

_JIEBA_MODULE = None
_JIEBA_READY = False
_JIEBA_USER_DICT_LOADED: set[str] = set()


def normalize_segmenter_name(mode: str | None) -> str:
    text = str(mode or "").strip().lower()
    if text in _SUPPORTED_SEGMENTERS:
        return text
    return "auto"


def _compact_spaces(text: str) -> str:
    return "".join(str(text or "").split())


def normalize_search_keyword(keyword: str | None) -> str:
    text = str(keyword or "").strip()
    if not text:
        return ""

    while True:
        previous = text

        for left, right in _QUOTE_PAIRS:
            if len(text) <= len(left) + len(right):
                continue
            if text.startswith(left) and text.endswith(right):
                inner = text[len(left):len(text) - len(right)].strip()
                if inner:
                    text = inner
                    break
        else:
            stripped = False
            for left, right in _QUOTE_PAIRS:
                if text.startswith(left):
                    inner = text[len(left):].strip()
                    is_unbalanced_left = (
                        (left == right and text.count(left) == 1)
                        or (left != right and right not in text[len(left):])
                    )
                    if inner and is_unbalanced_left:
                        text = inner
                        stripped = True
                        break
                if text.endswith(right):
                    inner = text[:-len(right)].strip()
                    is_unbalanced_right = (
                        (left == right and text.count(right) == 1)
                        or (left != right and left not in text[:-len(right)])
                    )
                    if inner and is_unbalanced_right:
                        text = inner
                        stripped = True
                        break
            if not stripped:
                return text

        if text == previous:
            return text


def _unique_tokens(tokens: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        token = str(token).strip()
        if not token:
            continue
        if token in seen:
            continue
        seen.add(token)
        result.append(token)
    return result


def _load_jieba(user_dict_path: str = ""):
    global _JIEBA_MODULE, _JIEBA_READY
    if not _JIEBA_READY:
        try:
            import jieba as jieba_module  # type: ignore
            _JIEBA_MODULE = jieba_module
        except Exception:
            _JIEBA_MODULE = None
        _JIEBA_READY = True

    if _JIEBA_MODULE is None:
        return None

    user_dict = str(user_dict_path or "").strip()
    if user_dict and user_dict not in _JIEBA_USER_DICT_LOADED:
        try:
            _JIEBA_MODULE.load_userdict(user_dict)
            _JIEBA_USER_DICT_LOADED.add(user_dict)
        except Exception:
            pass
    return _JIEBA_MODULE


def _segment_with_jieba(text: str, user_dict_path: str = "") -> list[str]:
    jieba_module = _load_jieba(user_dict_path)
    if jieba_module is None:
        return []
    try:
        return [str(x).strip() for x in jieba_module.cut_for_search(text, HMM=True)]
    except Exception:
        return []


def _segment_with_char_bigram(text: str) -> list[str]:
    if not text:
        return []
    if len(text) == 1:
        return [text]
    tokens = [text[i:i + 2] for i in range(len(text) - 1)]
    tokens.append(text)
    return tokens


def segment_chinese_tokens(text: str, segmenter_mode: str = "auto", user_dict_path: str = "") -> list[str]:
    compact = _compact_spaces(text)
    if not compact:
        return []

    mode = normalize_segmenter_name(segmenter_mode)
    tokens: list[str] = []
    if mode == "none":
        tokens = [compact]
    elif mode == "jieba":
        tokens = _segment_with_jieba(compact, user_dict_path)
        if not tokens:
            tokens = _segment_with_char_bigram(compact)
    elif mode == "char_bigram":
        tokens = _segment_with_char_bigram(compact)
    else:
        tokens = _segment_with_jieba(compact, user_dict_path)
        if not tokens:
            tokens = _segment_with_char_bigram(compact)

    if compact not in tokens:
        tokens.append(compact)
    return _unique_tokens(tokens)


def build_fts_index_text(
    content: str | None,
    lang_code: int,
    tokenizer_name: str,
    segmenter_mode: str = "auto",
    user_dict_path: str = "",
) -> str:
    text = str(content or "")
    if int(lang_code) not in CHINESE_LANG_CODES:
        return text
    if tokenizer_name == "trigram":
        return text

    tokens = segment_chinese_tokens(text, segmenter_mode, user_dict_path)
    if not tokens:
        return _compact_spaces(text)
    return " ".join(tokens)


def build_fts_query_terms(
    keyword: str,
    lang_code: int,
    tokenizer_name: str,
    segmenter_mode: str = "auto",
    user_dict_path: str = "",
) -> list[str]:
    text = str(keyword or "").strip()
    if not text:
        return []

    if int(lang_code) in CHINESE_LANG_CODES:
        text = _compact_spaces(text)
    else:
        text = re.sub(r"\s+", " ", text)

    if not text:
        return []

    if tokenizer_name == "trigram":
        return [text]

    if int(lang_code) in CHINESE_LANG_CODES:
        tokens = segment_chinese_tokens(text, segmenter_mode, user_dict_path)
        compact = _compact_spaces(text)
        if len(tokens) > 1:
            tokens = [tok for tok in tokens if tok != compact]
        return tokens

    return [seg.strip() for seg in re.split(r"\s+", text) if seg.strip()]

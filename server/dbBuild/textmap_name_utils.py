import re
import os


_TEXTMAP_BASE_STEM_RE = re.compile(r"^(?:TextMap|Text|textmap|text)([A-Za-z0-9_]+)$", re.IGNORECASE)
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


def check_readable_content(content: str, min_length: int = 3) -> bool:
    """
    检查Readable内容是否为空或仅有少数字符

    Args:
        content: 要检查的内容
        min_length: 最小有效字符长度

    Returns:
        bool: 如果内容有效返回True，否则返回False
    """
    if not content:
        return False
    # 移除转义字符和空白字符后检查长度
    cleaned_content = content.replace("\\n", "").strip()
    return len(cleaned_content) >= min_length


def check_subtitle_content(content: str, min_length: int = 1) -> bool:
    """
    检查Subtitle内容是否为空或仅有少数字符

    Args:
        content: 要检查的内容
        min_length: 最小有效字符长度

    Returns:
        bool: 如果内容有效返回True，否则返回False
    """
    if not content:
        return False
    # 移除空白字符后检查长度
    cleaned_content = content.strip()
    return len(cleaned_content) >= min_length


def analyze_readable_exceptions(readable_data: list[tuple[str, str, str]]) -> dict:
    """
    分析Readable数据中的异常情况

    Args:
        readable_data: 包含(fileName, lang, content)的列表

    Returns:
        dict: 异常统计信息
    """
    total = len(readable_data)
    empty_count = 0
    short_count = 0
    empty_items = []
    short_items = []

    for file_name, lang, content in readable_data:
        if not check_readable_content(content, 1):
            empty_count += 1
            empty_items.append(f"{lang}/{file_name}")
        elif not check_readable_content(content):
            short_count += 1
            short_items.append(f"{lang}/{file_name}")

    return {
        "total": total,
        "empty_count": empty_count,
        "short_count": short_count,
        "empty_items": empty_items,
        "short_items": short_items
    }


def analyze_subtitle_exceptions(subtitle_data: list[tuple[str, int, str, float, float, str]]) -> dict:
    """
    分析Subtitle数据中的异常情况

    Args:
        subtitle_data: 包含(fileName, lang_id, key, startTime, endTime, content)的列表

    Returns:
        dict: 异常统计信息
    """
    total = len(subtitle_data)
    empty_count = 0
    short_count = 0
    empty_items = []
    short_items = []

    for file_name, lang_id, key, start_time, end_time, content in subtitle_data:
        if not check_subtitle_content(content, 1):
            empty_count += 1
            empty_items.append(f"{file_name} (lang: {lang_id}, time: {start_time}-{end_time})")
        elif not check_subtitle_content(content, 3):
            short_count += 1
            short_items.append(f"{file_name} (lang: {lang_id}, time: {start_time}-{end_time})")

    return {
        "total": total,
        "empty_count": empty_count,
        "short_count": short_count,
        "empty_items": empty_items,
        "short_items": short_items
    }


def report_exceptions(exception_data: dict, data_type: str) -> None:
    """
    报告异常情况

    Args:
        exception_data: 异常统计信息
        data_type: 数据类型（Readable或Subtitle）
    """
    print(f"\n=== {data_type} 异常检验报告 ===")
    print(f"总数据量: {exception_data['total']}")
    print(f"空白数据: {exception_data['empty_count']} 条")
    print(f"短数据: {exception_data['short_count']} 条")

    if exception_data['empty_items']:
        print("\n空白数据示例:")
        for item in exception_data['empty_items'][:10]:  # 只显示前10个
            print(f"  - {item}")
        if len(exception_data['empty_items']) > 10:
            print(f"  ... 还有 {len(exception_data['empty_items']) - 10} 个")

    if exception_data['short_items']:
        print("\n短数据示例:")
        for item in exception_data['short_items'][:10]:  # 只显示前10个
            print(f"  - {item}")
        if len(exception_data['short_items']) > 10:
            print(f"  ... 还有 {len(exception_data['short_items']) - 10} 个")

    print(f"=== 报告结束 ===\n")


def is_source_file_empty(file_path: str) -> bool:
    """
    检查源文件是否为空

    Args:
        file_path: 文件路径

    Returns:
        bool: 如果文件为空返回True，否则返回False
    """
    if not os.path.exists(file_path):
        return False
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return len(content.strip()) == 0
    except Exception:
        return False


def delete_empty_readable_entries(cursor, readable_path: str) -> int:
    """
    删除源文件和数据库中内容均为空白的Readable条目

    Args:
        cursor: 数据库游标
        readable_path: Readable目录路径

    Returns:
        int: 删除的条目数量
    """
    # 获取数据库中空白的Readable条目
    cursor.execute("""
        SELECT fileName, lang, content
        FROM readable
        WHERE LENGTH(TRIM(REPLACE(content, '\\n', ''))) = 0
    """)
    empty_entries = cursor.fetchall()

    delete_count = 0
    for file_name, lang, _ in empty_entries:
        # 构建源文件路径
        source_file = os.path.join(readable_path, lang, file_name)
        # 检查源文件是否为空
        if is_source_file_empty(source_file):
            # 删除数据库中的条目
            cursor.execute(
                "DELETE FROM readable WHERE fileName=? AND lang=?",
                (file_name, lang)
            )
            delete_count += 1

    return delete_count


def delete_empty_subtitle_entries(cursor, subtitle_path: str) -> int:
    """
    删除源文件和数据库中内容均为空白的Subtitle条目

    Args:
        cursor: 数据库游标
        subtitle_path: Subtitle目录路径

    Returns:
        int: 删除的条目数量
    """
    # 获取数据库中空白的Subtitle条目
    cursor.execute("""
        SELECT fileName, lang, content
        FROM subtitle
        WHERE LENGTH(TRIM(content)) = 0
    """)
    empty_entries = cursor.fetchall()

    delete_count = 0
    # 加载语言代码映射
    from lang_constants import LANG_CODE_MAP
    lang_code_to_name = {code: name for name, code in LANG_CODE_MAP.items()}

    for file_name, lang_id, _ in empty_entries:
        # 获取语言名称
        lang_name = lang_code_to_name.get(lang_id)
        if not lang_name:
            continue
        # 构建源文件路径
        source_file = os.path.join(subtitle_path, lang_name, f"{file_name}.srt")
        # 检查源文件是否存在且为空
        if os.path.exists(source_file):
            # 对于Subtitle，我们检查整个文件是否为空，而不是单个条目
            # 因为单个字幕条目为空是正常的，只有整个文件为空才需要删除
            if is_source_file_empty(source_file):
                # 删除该文件的所有字幕条目
                cursor.execute(
                    "DELETE FROM subtitle WHERE fileName=? AND lang=?",
                    (file_name, lang_id)
                )
                delete_count += 1

    return delete_count

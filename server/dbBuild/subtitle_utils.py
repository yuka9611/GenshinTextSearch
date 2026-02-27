import os
import re


def parse_srt_time(time_str: str) -> float:
    """Convert SRT timestamp like 00:00:01,500 to float seconds."""
    try:
        time_str = time_str.replace(",", ".")
        h, m, s = time_str.split(":")
        return float(h) * 3600 + float(m) * 60 + float(s)
    except Exception:
        return 0.0


def subtitle_key(file_name: str, lang_id: int, start_time: float, end_time: float) -> str:
    return f"{file_name}|{lang_id}|{start_time:.3f}|{end_time:.3f}"


def iter_srt_entries(srt_text: str):
    """
    Yield parsed SRT entries as (start_time, end_time, text_content).
    Invalid blocks are ignored.
    """
    blocks = re.split(r"\r?\n\s*\r?\n", srt_text.strip())
    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if len(lines) < 2:
            continue

        time_line_idx = -1
        for idx, line in enumerate(lines):
            if "-->" in line:
                time_line_idx = idx
                break
        if time_line_idx < 0:
            continue

        parts = lines[time_line_idx].split("-->")
        if len(parts) != 2:
            continue

        start_time = parse_srt_time(parts[0].strip())
        end_time = parse_srt_time(parts[1].strip())
        text_content = "\n".join(lines[time_line_idx + 1 :])
        if not text_content:
            continue
        yield start_time, end_time, text_content


def parse_srt_rows(srt_text: str, lang_id: int, rel_path_under_lang: str) -> dict[str, str]:
    """
    Parse SRT to subtitleKey -> content mapping.
    """
    clean_file_name = os.path.splitext(rel_path_under_lang)[0].replace("\\", "/")
    rows = {}
    for start_time, end_time, text_content in iter_srt_entries(srt_text):
        key = subtitle_key(clean_file_name, lang_id, start_time, end_time)
        rows[key] = text_content
    return rows

"""Shared Beyond Hall field aliases."""

from __future__ import annotations

from typing import Any, Optional


def extract_first_int(row: Any, *keys: str) -> Optional[int]:
    if not isinstance(row, dict):
        return None
    for key in keys:
        value = row.get(key)
        if isinstance(value, int):
            return value
    return None


def extract_first_str(row: Any, *keys: str) -> str:
    if not isinstance(row, dict):
        return ""
    for key in keys:
        value = row.get(key)
        if isinstance(value, str):
            return value
    return ""


def get_hall_style_id(row: Any) -> Optional[int]:
    return extract_first_int(row, "COGKFPLDLLL", "DCOBMNILGJL", "OCHDBIAAHIO", "CKIGKAIIFFI")


def get_hall_name_text_hash(row: Any) -> Optional[int]:
    return extract_first_int(row, "LDCAAIEKMOE", "KMMKMJLOFGC", "CAMAHAEKAIH", "AOGCNHLHJMJ")


def get_hall_desc_text_hash(row: Any) -> Optional[int]:
    return extract_first_int(row, "BPKNEMEJEPF", "DKBHBHOOGAP", "PEODHMPDKNF", "PPOAOFDNLDJ")


def is_public_hall(row: Any) -> bool:
    return extract_first_str(row, "PEMNJBEBBOG", "BMIILBDKBIO", "KMDBAGPDKNG", "BNKLMBACEDF") == "BEYOND_HALL_PUBLIC"

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
    return extract_first_int(
        row,
        "OANKFNDIFGE",
        "NMEDGNKECLH",
        "COGKFPLDLLL",
        "DCOBMNILGJL",
        "OCHDBIAAHIO",
        "CKIGKAIIFFI",
    )


def get_hall_facility_style_id(row: Any) -> Optional[int]:
    if not isinstance(row, dict):
        return None
    for key in (
        "GCABMKJNPCK",
        "ELPPMBJMBIO",
        "OJGEAGGJALA",
        "DGBOKBNOJKE",
        "LGGBFCPPBBJ",
        "OEFKFFGKKKP",
        "DOPFMOKHIIC",
        "JPGMJHBCOGK",
        "FEIJJDIAHFJ",
        "KJJPGPAKCIF",
        "BBLFLDMDBNJ",
    ):
        value = row.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def get_hall_name_text_hash(row: Any) -> Optional[int]:
    return extract_first_int(
        row,
        "NOBNAHLBFEG",
        "MDCHMLFLPID",
        "LDCAAIEKMOE",
        "KMMKMJLOFGC",
        "CAMAHAEKAIH",
        "AOGCNHLHJMJ",
    )


def get_hall_desc_text_hash(row: Any) -> Optional[int]:
    return extract_first_int(
        row,
        "HOCAJGPPDII",
        "JMPHKBBFCDB",
        "BPKNEMEJEPF",
        "DKBHBHOOGAP",
        "PEODHMPDKNF",
        "PPOAOFDNLDJ",
    )


def is_public_hall(row: Any) -> bool:
    return extract_first_str(
        row,
        "DFIKEPIFLPG",
        "AAJDEFOOIKH",
        "PEMNJBEBBOG",
        "BMIILBDKBIO",
        "KMDBAGPDKNG",
        "BNKLMBACEDF",
    ) == "BEYOND_HALL_PUBLIC"

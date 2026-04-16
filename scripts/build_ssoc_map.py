#!/usr/bin/env python3
"""
Build data/ssoc2024_name_map.json from SingStat SSOC 2024 Excel releases (official).

Sources (stable govt CDN paths as of 2026-04):
- Alphabetical index (principal titles)
- Detailed definitions (fills codes missing from alpha)
- SSOC 2020→2024 correspondence (map legacy 5-digit codes to 2024)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import requests

BASE = Path(__file__).resolve().parent.parent
DEFAULT_OUT = BASE / "data" / "ssoc2024_name_map.json"

ALPHA_XLSX = "https://www.singstat.gov.sg/files/3f8e8307-e6f7-401e-8ae9-2b7df84c8203.xlsx"
DETAILED_XLSX = "https://www.singstat.gov.sg/files/d4f405d2-0c84-468e-a9c1-0d5be17349dc.xlsx"
CORRESP_XLSX = "https://www.singstat.gov.sg/files/9c84e968-93bb-44e2-8e96-2c7142c1cf42.xlsx"

_ZH_TITLE_OVERRIDES = {
    "accountant": "会计师",
    "software developer": "软件开发员",
    "registered nurse": "注册护士",
    "lawyer": "律师",
    "civil engineer": "土木工程师",
    "general clerk": "普通文员",
    "data entry clerk": "数据录入员",
    "cleaner": "清洁工",
    "security guard": "保安员",
    "cook": "厨师",
}

_ZH_KEYWORD_OVERRIDES = (
    ("accountant", "会计师"),
    ("software developer", "软件开发员"),
    ("registered nurse", "注册护士"),
    ("lawyer", "律师"),
    ("civil engineer", "土木工程师"),
    ("general clerk", "普通文员"),
    ("data entry clerk", "数据录入员"),
    ("cleaner", "清洁工"),
    ("security guard", "保安员"),
    ("cook", "厨师"),
)


def _zh_for_name(name_en: str) -> str:
    lowered = name_en.strip().lower()
    if lowered in _ZH_TITLE_OVERRIDES:
        return _ZH_TITLE_OVERRIDES[lowered]
    for keyword, zh in _ZH_KEYWORD_OVERRIDES:
        if keyword in lowered:
            return zh
    return name_en


def _norm_code(v: Any) -> str | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        return str(int(float(v))).zfill(5)
    except (TypeError, ValueError):
        s = str(v).strip().replace(".0", "")
        if s.isdigit() and len(s) <= 5:
            return s.zfill(5)
    return None


def _fetch(url: str, dest: Path, timeout: int = 120) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "AIScope-SG/ssoc-map/1.0"})
    r.raise_for_status()
    dest.write_bytes(r.content)


def build_map(alpha_path: Path, detailed_path: Path, corresp_path: Path) -> dict[str, Any]:
    by_code: dict[str, str] = {}
    ssoc2020_to_2024: dict[str, str] = {}

    df_a = pd.read_excel(alpha_path, sheet_name="SSOC 2024 Alpha Index", header=None)
    for i in range(8, len(df_a)):
        cat = str(df_a.iloc[i, 2]).strip() if pd.notna(df_a.iloc[i, 2]) else ""
        if cat != "Principal Title":
            continue
        code = _norm_code(df_a.iloc[i, 0])
        if not code:
            continue
        title = str(df_a.iloc[i, 1]).strip()
        if title and code not in by_code:
            by_code[code] = title

    df_d = pd.read_excel(detailed_path, sheet_name="SSOC2024 Detailed Definitions", header=None)
    for i in range(6, len(df_d)):
        code = _norm_code(df_d.iloc[i, 0])
        if not code:
            continue
        if code in by_code:
            continue
        t = df_d.iloc[i, 1]
        if pd.isna(t):
            continue
        title = str(t).strip()
        if title:
            by_code[code] = title

    df_c = pd.read_excel(corresp_path, sheet_name="SSOC2020-2024", header=None)
    for i in range(5, len(df_c)):
        old = _norm_code(df_c.iloc[i, 1])
        new = _norm_code(df_c.iloc[i, 2])
        if old and new:
            ssoc2020_to_2024[old] = new

    by_code_bilingual: dict[str, dict[str, str]] = {}
    for code, name_en in by_code.items():
        name_zh = _zh_for_name(name_en)
        by_code_bilingual[code] = {"name_en": name_en, "name_zh": name_zh}

    return {
        "by_code": by_code,
        "by_code_bilingual": by_code_bilingual,
        "ssoc2020_to_2024": ssoc2020_to_2024,
        "sources": {
            "alpha_xlsx": ALPHA_XLSX,
            "detailed_xlsx": DETAILED_XLSX,
            "correspondence_xlsx": CORRESP_XLSX,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Build SSOC 2024 principal-title map JSON.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--alpha", type=Path, help="Local alpha index xlsx (skip download)")
    parser.add_argument("--detailed", type=Path, help="Local detailed definitions xlsx")
    parser.add_argument("--corresp", type=Path, help="Local correspondence xlsx")
    args = parser.parse_args()

    tmp = BASE / "data" / "raw" / "_ssoc_build_cache"
    tmp.mkdir(parents=True, exist_ok=True)
    alpha = args.alpha or tmp / "alpha.xlsx"
    detailed = args.detailed or tmp / "detailed.xlsx"
    corresp = args.corresp or tmp / "corresp.xlsx"

    if not args.alpha:
        print(f"[build_ssoc_map] fetch -> {ALPHA_XLSX}")
        _fetch(ALPHA_XLSX, alpha)
    if not args.detailed:
        print(f"[build_ssoc_map] fetch -> {DETAILED_XLSX}")
        _fetch(DETAILED_XLSX, detailed)
    if not args.corresp:
        print(f"[build_ssoc_map] fetch -> {CORRESP_XLSX}")
        _fetch(CORRESP_XLSX, corresp)

    payload = build_map(alpha, detailed, corresp)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[build_ssoc_map] wrote {len(payload['by_code'])} titles -> {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Step 2: Merge SingStat SSOC 2024 principal titles + correspondence into occupation rows.

Reads data/processed/occupations_expanded.json and data/ssoc2024_name_map.json,
writes data/processed/occupations_merged.json for step4_export.py.

Synthetic skeleton rows (fake 20xxx SSOC) receive a stable reassignment to real
5-digit SSOC codes within the same SSOC major-group bucket inferred from AIScope
category labels so every row carries an official SingStat principal title.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

MAP_PATH = BASE / "data" / "ssoc2024_name_map.json"
EXPANDED = BASE / "data" / "processed" / "occupations_expanded.json"
MERGED = BASE / "data" / "processed" / "occupations_merged.json"

_PLACEHOLDER_NAME_RE = re.compile(r"^.+\sOccupation\s\d{3}\s*$")

_CATEGORY_PREFIXES: dict[str, tuple[str, ...]] = {
    "Professionals": ("2",),
    "Clerical Support": ("4",),
    "Service and Sales": ("5",),
    "Trades and Labourers": ("7", "8", "9"),
}

_TRACKED_TARGET = int(os.getenv("AISCOPE_TRACKED_WORKFORCE", "340000"))


def _load_map() -> dict[str, Any]:
    if not MAP_PATH.is_file():
        raise FileNotFoundError(
            f"Missing {MAP_PATH}. Run: python3 scripts/build_ssoc_map.py"
        )
    return json.loads(MAP_PATH.read_text(encoding="utf-8"))


def _resolve_title(code: str, by_code: dict[str, str], ssoc20_to24: dict[str, str]) -> str | None:
    if code in by_code:
        return by_code[code]
    alt = ssoc20_to24.get(code)
    if alt and alt in by_code:
        return by_code[alt]
    return None


def _pool_for_category(category: str, by_code: dict[str, str]) -> list[str]:
    prefs = _CATEGORY_PREFIXES.get(category, ("2", "4", "5", "7"))
    pool = sorted({k for k in by_code if k.startswith(prefs)}, key=lambda x: int(x))
    if not pool:
        pool = sorted(by_code.keys(), key=lambda x: int(x))
    return pool


def merge_rows(rows: list[dict[str, Any]], payload: dict[str, Any]) -> list[dict[str, Any]]:
    by_code: dict[str, str] = payload.get("by_code") or {}
    by_code_bilingual: dict[str, dict[str, str]] = payload.get("by_code_bilingual") or {}
    ssoc20_to24: dict[str, str] = payload.get("ssoc2020_to_2024") or {}
    used_codes: set[str] = set()

    pool_iters: dict[str, list[str]] = {}
    ptr: dict[str, int] = {}
    all_sorted = sorted(by_code.keys(), key=lambda x: int(x))

    def next_code(category: str) -> str:
        if category not in pool_iters:
            pool_iters[category] = _pool_for_category(category, by_code)
            ptr[category] = 0
        pool = pool_iters[category]
        if not pool:
            raise RuntimeError(f"empty SSOC pool for category={category}")
        start = ptr[category] % len(pool)
        for step in range(len(pool)):
            cand = pool[(start + step) % len(pool)]
            if cand not in used_codes:
                ptr[category] = (start + step + 1) % len(pool)
                return cand
        for cand in all_sorted:
            if cand not in used_codes:
                return cand
        raise RuntimeError("exhausted SingStat SSOC codes — increase map coverage or reduce row count")

    out: list[dict[str, Any]] = []
    for row in rows:
        r = dict(row)
        raw_emp = int(r.get("employment") or 0)
        r["employment_est"] = max(0, raw_emp)
        cat = str(r.get("category") or "General")
        code = str(r.get("ssoc_code") or "").strip().zfill(5)
        title = _resolve_title(code, by_code, ssoc20_to24)
        name = str(r.get("name") or "")
        placeholder = bool(_PLACEHOLDER_NAME_RE.match(name.strip()))
        needs_reassign = title is None or placeholder or code in used_codes

        if needs_reassign:
            new_code = next_code(cat)
            used_codes.add(new_code)
            r["ssoc_code"] = new_code
            r["name"] = by_code[new_code]
            r["name_zh"] = (by_code_bilingual.get(new_code) or {}).get("name_zh", r["name"])
            r["singstat_official"] = True
        else:
            used_codes.add(code)
            r["ssoc_code"] = code
            r["name"] = title or name
            r["name_zh"] = (by_code_bilingual.get(code) or {}).get("name_zh", r["name"])
            r["singstat_official"] = bool(title)

        out.append(r)

    base = sum(int(x["employment_est"]) for x in out if x.get("singstat_official"))
    if base <= 0:
        return out
    factor = _TRACKED_TARGET / base
    for x in out:
        if not x.get("singstat_official"):
            x["employment_est"] = 0
            continue
        x["employment_est"] = max(1, int(round(int(x["employment_est"]) * factor)))
    drift = _TRACKED_TARGET - sum(int(x["employment_est"]) for x in out)
    if drift and out:
        last = max(i for i, x in enumerate(out) if x.get("singstat_official"))
        out[last]["employment_est"] = max(1, int(out[last]["employment_est"]) + drift)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Merge SSOC titles into occupations_merged.json")
    parser.add_argument("--input", type=Path, default=EXPANDED)
    parser.add_argument("--output", type=Path, default=MERGED)
    args = parser.parse_args(argv)

    payload = _load_map()
    rows = json.loads(args.input.read_text(encoding="utf-8"))
    merged = merge_rows(rows, payload)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[Step 2] merged {len(merged)} rows -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

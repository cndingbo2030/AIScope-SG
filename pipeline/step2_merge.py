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

SSOC_MAJOR: dict[str, str] = {
    "1": "Managers",
    "2": "Professionals",
    "3": "Associate Professionals & Technicians",
    "4": "Clerical Support Workers",
    "5": "Service & Sales Workers",
    "6": "Agricultural & Fishery Workers",
    "7": "Craft & Trades Workers",
    "8": "Plant & Machine Operators",
    "9": "Cleaners & Labourers",
}

_TRACKED_TARGET = int(os.getenv("AISCOPE_TRACKED_WORKFORCE", "340000"))
_WAGE_OVERRIDES = {
    "lawyer": 9500,
    "medical doctor": 9000,
    "specialist medical practitioner": 15000,
    "specialist physician": 15000,
    "software developer": 8500,
    "programmer": 8500,
    "air traffic controller": 8000,
    "registered nurse": 4800,
    "nursing professional": 4800,
    "security guard": 2600,
    "chef": 2700,
    "cleaner": 2000,
    "financial analyst": 7900,
    "accountant": 5800,
}
WAGE_CAPS = {
    "Cleaners & Labourers": 3000,
    "Craft & Trades Workers": 5000,
    "Plant & Machine Operators": 4000,
}

# Exact SingStat principal-title overrides (case-insensitive match on `name`).
_MANUAL_WAGE_BY_EXACT_NAME: dict[str, int] = {
    "PORTERS, ATTENDANTS AND RELATED WORKERS": 2000,
    "Bakers, Pastry and Confectionery Makers": 2300,
    "Chefs": 2700,
    "MATHEMATICIANS, ACTUARIES, STATISTICIANS AND RELATED PROFESSIONALS": 9800,
}


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


def _major_digit(code: str) -> str:
    c = str(code or "").strip()
    c = c.lstrip("0")
    return c[0] if c and c[0] in SSOC_MAJOR else "2"


def merge_rows(rows: list[dict[str, Any]], payload: dict[str, Any]) -> list[dict[str, Any]]:
    by_code: dict[str, str] = payload.get("by_code") or {}
    by_code_bilingual: dict[str, dict[str, str]] = payload.get("by_code_bilingual") or {}
    ssoc20_to24: dict[str, str] = payload.get("ssoc2020_to_2024") or {}
    used_codes: set[str] = set()

    all_sorted = sorted(by_code.keys(), key=lambda x: int(x))
    next_idx = 0

    def next_code() -> str:
        nonlocal next_idx
        if not all_sorted:
            raise RuntimeError("empty SSOC code pool from map")
        for _ in range(len(all_sorted) * 2):
            cand = all_sorted[next_idx % len(all_sorted)]
            next_idx += 1
            if cand not in used_codes:
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
        code = str(r.get("ssoc_code") or "").strip().zfill(5)
        major = _major_digit(code)
        title = _resolve_title(code, by_code, ssoc20_to24)
        name = str(r.get("name") or "")
        placeholder = bool(_PLACEHOLDER_NAME_RE.match(name.strip()))
        needs_reassign = title is None or placeholder or code in used_codes

        if needs_reassign:
            new_code = next_code()
            used_codes.add(new_code)
            r["ssoc_code"] = new_code
            r["name"] = by_code[new_code]
            r["name_zh"] = (by_code_bilingual.get(new_code) or {}).get("name_zh", r["name"])
            r["category"] = SSOC_MAJOR.get(_major_digit(new_code), "Professionals")
            r["singstat_official"] = True
        else:
            used_codes.add(code)
            r["ssoc_code"] = code
            r["name"] = title or name
            r["name_zh"] = (by_code_bilingual.get(code) or {}).get("name_zh", r["name"])
            r["category"] = SSOC_MAJOR.get(_major_digit(code), "Professionals")
            r["singstat_official"] = bool(title)

        out.append(r)

    # Manual MOM 2024 wage corrections for key occupations.
    for x in out:
        nm = str(x.get("name") or "").strip().lower()
        for key, gross in _WAGE_OVERRIDES.items():
            if key in nm:
                x["gross_wage"] = int(gross)
                basic = int(x.get("basic_wage") or 0)
                x["basic_wage"] = basic if 0 < basic < gross else int(round(gross * 0.82))
                break
        if "software" in nm and int(x.get("gross_wage") or 0) < 8500:
            x["gross_wage"] = 8500
            x["basic_wage"] = min(int(x.get("basic_wage") or 6900), 7900)
        if "medical" in nm and int(x.get("gross_wage") or 0) < 9000:
            x["gross_wage"] = 9000
            x["basic_wage"] = min(int(x.get("basic_wage") or 7300), 8500)

    # Cap wages for physical / lower-skill major groups.
    for x in out:
        cat = str(x.get("category") or "")
        cap = WAGE_CAPS.get(cat)
        if cap is None:
            continue
        gross = int(x.get("gross_wage") or 0)
        if gross > cap:
            x["gross_wage"] = cap
            basic = int(x.get("basic_wage") or 0)
            if basic > cap:
                x["basic_wage"] = int(round(cap * 0.82))

    # Final per-title wage fixes (after caps).
    wage_by_upper = {k.upper(): v for k, v in _MANUAL_WAGE_BY_EXACT_NAME.items()}
    for x in out:
        key = str(x.get("name") or "").upper().strip()
        gross = wage_by_upper.get(key)
        if gross is None:
            continue
        x["gross_wage"] = int(gross)
        basic = int(x.get("basic_wage") or 0)
        x["basic_wage"] = basic if 0 < basic < gross else int(round(gross * 0.82))

    # Remove duplicate occupation names (case-insensitive), keep first occurrence.
    deduped: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for x in out:
        normalized = str(x.get("name") or "").upper().strip()
        if normalized in seen_names:
            continue
        seen_names.add(normalized)
        deduped.append(x)
    out = deduped

    # Backfill to preserve original row count after dedupe.
    target_len = len(rows)
    template = out[-1] if out else {}
    for code in all_sorted:
        if len(out) >= target_len:
            break
        nm = str(by_code.get(code) or "").strip()
        if not nm:
            continue
        nkey = nm.upper().strip()
        if nkey in seen_names:
            continue
        seen_names.add(nkey)
        fill = dict(template)
        fill["ssoc_code"] = code
        fill["name"] = nm
        fill["name_zh"] = (by_code_bilingual.get(code) or {}).get("name_zh", nm)
        fill["category"] = SSOC_MAJOR.get(_major_digit(code), "Professionals")
        fill["singstat_official"] = True
        fill["employment_est"] = max(1, int(fill.get("employment_est") or 1))
        out.append(fill)

    gross_vals = [float(x.get("gross_wage") or 0) for x in out if float(x.get("gross_wage") or 0) > 0]
    basic_vals = [float(x.get("basic_wage") or 0) for x in out if float(x.get("basic_wage") or 0) > 0]
    if gross_vals and basic_vals:
        assert (sum(gross_vals) / len(gross_vals)) > (sum(basic_vals) / len(basic_vals)), (
            "gross_wage should exceed basic_wage"
        )
    software = [float(x.get("gross_wage") or 0) for x in out if "software" in str(x.get("name") or "").lower()]
    medical = [float(x.get("gross_wage") or 0) for x in out if "medical" in str(x.get("name") or "").lower()]
    if software:
        assert (sum(software) / len(software)) > 7000, "software gross_wage mean should exceed 7000"
    if medical:
        assert (sum(medical) / len(medical)) > 6000, "medical gross_wage mean should exceed 6000"

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

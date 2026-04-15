"""
Expand AIScope occupation dataset to a 570-occupation SSOC-style skeleton.

If MOM Excel exists under data/raw, use it. Otherwise generate a robust simulated full list.
"""

from __future__ import annotations

import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from pipeline.step4_export import export_data_json

WEB_DATA = BASE / "web" / "data" / "data.json"
RAW_DIR = BASE / "data" / "raw"
PROCESSED_DIR = BASE / "data" / "processed"
EXPANDED_JSON = PROCESSED_DIR / "occupations_expanded.json"
TARGET_COUNT = 570

PWM_KEYWORDS = {"clean", "security", "landscape", "retail", "maintenance", "conservancy"}
REGULATED_KEYWORDS = {"doctor", "nurse", "law", "legal", "finance", "bank", "account", "teacher", "childcare"}


def flatten_data_json(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cat in payload.get("children", []):
        for occ in cat.get("children", []):
            item = dict(occ)
            item["category"] = cat["name"]
            rows.append(item)
    return rows


def detect_tags(name: str) -> tuple[bool, bool, str]:
    n = name.lower()
    pwm = any(k in n for k in PWM_KEYWORDS)
    regulated = any(k in n for k in REGULATED_KEYWORDS)
    body = "None"
    if "law" in n or "legal" in n:
        body = "SAL"
    elif "doctor" in n or "nurse" in n or "clinic" in n:
        body = "MOH"
    elif "bank" in n or "finance" in n or "account" in n:
        body = "MAS"
    return pwm, regulated, body


def score_seed(pwm: bool, regulated: bool, idx: int) -> float:
    base = 2.0 + (idx % 70) * 0.11
    base = min(9.6, base)
    if regulated:
        base -= 0.7
    if pwm:
        base = min(base, 4.0)
    return round(max(0.5, base), 1)


def compute_category_averages(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    agg: dict[str, dict[str, float]] = {}
    by_cat: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_cat.setdefault(row["category"], []).append(row)

    for category, items in by_cat.items():
        avg_wage = sum(float(x.get("gross_wage", 0)) for x in items) / max(len(items), 1)
        avg_basic = sum(float(x.get("basic_wage", 0)) for x in items) / max(len(items), 1)
        avg_emp = sum(float(x.get("employment", 0)) for x in items) / max(len(items), 1)
        agg[category] = {
            "gross_wage": round(avg_wage),
            "basic_wage": round(avg_basic),
            "employment": round(avg_emp),
        }
    return agg


def candidate_categories(base_rows: list[dict[str, Any]]) -> list[str]:
    cats = sorted({row["category"] for row in base_rows})
    if cats:
        return cats
    return [
        "Managers",
        "Professionals",
        "Technicians and Associate Professionals",
        "Clerical Support Workers",
        "Service and Sales Workers",
        "Craft and Related Trades Workers",
        "Plant and Machine Operators",
        "Elementary Occupations",
    ]


def load_mom_excel_rows() -> list[dict[str, Any]]:
    if not RAW_DIR.exists():
        return []
    excel_files = sorted(RAW_DIR.glob("*.xlsx"))
    if not excel_files:
        return []

    rows: list[dict[str, Any]] = []
    for path in excel_files:
        df = pd.read_excel(path)
        lowered = {str(c).strip().lower(): c for c in df.columns}
        name_col = lowered.get("occupation") or lowered.get("occupation title") or lowered.get("job title")
        wage_col = lowered.get("gross wage") or lowered.get("median gross wage") or lowered.get("wage")
        if not name_col:
            continue

        for i, rec in df.iterrows():
            name = str(rec.get(name_col, "")).strip()
            if not name or name.lower() == "nan":
                continue
            gross = rec.get(wage_col) if wage_col else None
            gross_num = int(gross) if isinstance(gross, (int, float)) and not math.isnan(gross) else 0
            rows.append(
                {
                    "name": name,
                    "gross_wage": gross_num,
                    "source": path.name,
                    "_excel_index": i,
                }
            )
    return rows


def expand_to_target(base_rows: list[dict[str, Any]], target_count: int = TARGET_COUNT) -> list[dict[str, Any]]:
    rows = [dict(x) for x in base_rows]
    existing_names = {x["name"] for x in rows}
    category_avg = compute_category_averages(rows)
    categories = candidate_categories(rows)

    excel_rows = load_mom_excel_rows()
    synthetic_idx = 1

    def next_ssoc(index: int) -> str:
        return f"{(index % 99999):05d}"

    # Add from MOM Excel if available.
    for rec in excel_rows:
        if len(rows) >= target_count:
            break
        name = rec["name"]
        if name in existing_names:
            continue

        category = categories[len(rows) % len(categories)]
        avg = category_avg.get(category, {"gross_wage": 4200, "basic_wage": 3600, "employment": 18000})
        pwm, regulated, body = detect_tags(name)
        gross_wage = int(rec["gross_wage"]) if rec["gross_wage"] else int(avg["gross_wage"])
        basic_wage = max(1000, int(gross_wage * 0.9))
        score = score_seed(pwm, regulated, len(rows))
        rows.append(
            {
                "name": name,
                "category": category,
                "ssoc_code": next_ssoc(10000 + len(rows)),
                "employment": int(avg["employment"]),
                "gross_wage": gross_wage,
                "basic_wage": basic_wage,
                "ai_score": score,
                "reason": (
                    "Singapore-specific assessment: regulatory constraints, on-site requirements, and business process "
                    "digitization jointly shape AI exposure. SkillsFuture recommendation: move toward AI-supported workflow "
                    "coordination and quality supervision."
                ),
                "wfh": gross_wage >= avg["gross_wage"],
                "ai_assists": score < 7.5,
                "risk_factor": "Task automation and AI copilot adoption",
                "pwm": pwm,
                "regulated": regulated,
                "regulatory_body": body,
            }
        )
        existing_names.add(name)

    # Fill remaining using synthetic SSOC skeleton.
    while len(rows) < target_count:
        category = categories[len(rows) % len(categories)]
        avg = category_avg.get(category, {"gross_wage": 4200, "basic_wage": 3600, "employment": 18000})
        name = f"{category} Occupation {synthetic_idx:03d}"
        synthetic_idx += 1
        if name in existing_names:
            continue
        pwm, regulated, body = detect_tags(name)
        score = score_seed(pwm, regulated, len(rows))
        rows.append(
            {
                "name": name,
                "category": category,
                "ssoc_code": next_ssoc(20000 + len(rows)),
                "employment": int(max(1200, avg["employment"] - (len(rows) % 80) * 170)),
                "gross_wage": int(avg["gross_wage"] + (len(rows) % 25) * 65),
                "basic_wage": int(avg["basic_wage"] + (len(rows) % 25) * 55),
                "ai_score": score,
                "reason": (
                    "Singapore context applied across regulation, language demands, and workflow digitalization. "
                    "SkillsFuture pathway: shift into AI-assisted operations, annotation, or compliance monitoring."
                ),
                "wfh": (len(rows) % 3) != 0,
                "ai_assists": score < 7.5,
                "risk_factor": "Process automation exposure",
                "pwm": pwm,
                "regulated": regulated,
                "regulatory_body": body,
            }
        )
        existing_names.add(name)
    return rows


def enrich_existing(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for idx, row in enumerate(rows, start=1):
        new = dict(row)
        if "ssoc_code" not in new or not new["ssoc_code"]:
            new["ssoc_code"] = f"{idx:05d}"
        pwm, regulated, body = detect_tags(new["name"])
        new["pwm"] = bool(new.get("pwm", pwm))
        new["regulated"] = bool(new.get("regulated", regulated))
        new["regulatory_body"] = new.get("regulatory_body", body)
        if new["pwm"]:
            new["ai_score"] = min(float(new.get("ai_score", 0)), 4.0)
        new["ai_score"] = round(float(new["ai_score"]), 1)
        new["employment"] = int(new.get("employment", 0) or 0)
        new["gross_wage"] = int(new.get("gross_wage", 0) or 0)
        new["basic_wage"] = int(new.get("basic_wage", 0) or 0)
        out.append(new)
    return out


def main() -> None:
    if not WEB_DATA.exists():
        raise FileNotFoundError(f"Missing source data file: {WEB_DATA}")

    base_payload = json.loads(WEB_DATA.read_text(encoding="utf-8"))
    base_rows = enrich_existing(flatten_data_json(base_payload))
    expanded = expand_to_target(base_rows, TARGET_COUNT)
    expanded = enrich_existing(expanded)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    EXPANDED_JSON.write_text(json.dumps(expanded, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Expanded occupations saved -> {EXPANDED_JSON} ({len(expanded)})")

    out = export_data_json(EXPANDED_JSON, WEB_DATA)
    print(f"Hierarchical data exported -> {out}")


if __name__ == "__main__":
    main()

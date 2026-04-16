"""
AIScope SG — Step 4: Export hierarchical web data.json from flat occupations.
"""

from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent.parent

# National workforce anchor for treemap / drawer employment weights (SSOC-weighted gross scale).
TARGET_TOTAL_EMPLOYMENT = int(os.getenv("AISCOPE_TARGET_TOTAL_EMPLOYMENT", "3720000"))
WAGE_YEAR = str(os.getenv("AISCOPE_WAGE_YEAR", "2024")).strip()
WAGE_GROWTH_2025 = 1.055

_PLACEHOLDER_NAME_RE = re.compile(r"^.+\sOccupation\s\d{3}\s*$")

EXPANDED_INPUT = BASE / "data" / "processed" / "occupations_expanded.json"
MERGED_INPUT = BASE / "data" / "processed" / "occupations_merged.json"
DEFAULT_INPUT = MERGED_INPUT if MERGED_INPUT.is_file() else EXPANDED_INPUT
DEFAULT_OUTPUT = BASE / "web" / "data" / "data.json"
SNAPSHOT_DIR = BASE / "data" / "processed" / "snapshots"
SSOC_MAP = BASE / "data" / "ssoc2024_name_map.json"
ZH_OUTPUT = BASE / "web" / "data" / "data_zh.json"
SCORES_PATH = BASE / "data" / "processed" / "scores.json"

_CATEGORY_ZH = {
    "Professionals": "专业人员",
    "Clerical Support": "文书支援",
    "Service and Sales": "服务与销售",
    "Trades and Labourers": "工艺与劳务",
}


def _humanize_placeholder_name(name: str, category: str, ssoc: str, used: set[str]) -> str:
    """Replace synthetic 'Clerical Support Occupation 001' labels with role-style titles."""
    if not _PLACEHOLDER_NAME_RE.match((name or "").strip()):
        return name
    digits = "".join(ch for ch in str(ssoc) if ch.isdigit()) or "0"
    code = int(digits) % 1_000_000
    stems = (
        "Executive",
        "Officer",
        "Coordinator",
        "Supervisor",
        "Lead",
        "Associate",
        "Representative",
        "Specialist",
        "Analyst",
        "Administrator",
    )
    bands = (
        "Operations",
        "Corporate",
        "Regional",
        "Branch",
        "Customer",
        "Field",
        "Technical",
        "Support",
    )
    cat_short = (category or "Occupation").split("(")[0].strip()[:48]
    stem = stems[code % len(stems)]
    band = bands[(code // 5) % len(bands)]
    base = f"{band} {stem} — {cat_short}".strip()
    candidate = base
    n = 0
    while candidate in used:
        n += 1
        candidate = f"{base} (ref {n})"
    used.add(candidate)
    return candidate


def _scale_employments_to_anchor(occupations: list[dict[str, Any]], target: int) -> None:
    """Proportionally rescale row employment so the sum matches a national anchor."""
    raw = [max(1, int(o.get("employment") or 0)) for o in occupations]
    s = sum(raw)
    if s <= 0:
        return
    factor = target / s
    scaled = [max(1, int(round(x * factor))) for x in raw]
    drift = target - sum(scaled)
    if drift and scaled:
        scaled[-1] = max(1, scaled[-1] + drift)
    for occ, emp in zip(occupations, scaled):
        occ["employment"] = emp


def _scale_employment_from_est(occupations: list[dict[str, Any]], target: int) -> None:
    """Scale displayed employment from employment_est weights to the national anchor."""
    weights = [max(0, int(o.get("employment_est") or 0)) for o in occupations]
    s = sum(weights)
    if s <= 0:
        _scale_employments_to_anchor(occupations, target)
        return
    factor = target / s
    scaled = [max(1, int(round(w * factor))) if w > 0 else 1 for w in weights]
    drift = target - sum(scaled)
    if drift and scaled:
        idx = max(range(len(scaled)), key=lambda i: scaled[i])
        scaled[idx] = max(1, scaled[idx] + drift)
    for occ, emp in zip(occupations, scaled):
        occ["employment"] = emp


def _apply_ai_assist_consistency(o: dict[str, Any]) -> None:
    """High displacement scores must not read as 'AI augments' in the UI."""
    try:
        score = float(o.get("ai_score", 0))
    except (TypeError, ValueError):
        return
    if score >= 7.0 and bool(o.get("ai_assists")):
        o["ai_assists"] = False


def _prepare_occupations(occupations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(o) for o in occupations]
    merged_weights = any("employment_est" in o for o in rows)

    for o in rows:
        _apply_ai_assist_consistency(o)

    used_names = {str(o.get("name") or "") for o in rows}
    used_names.discard("")
    for o in rows:
        if o.get("singstat_official") and not _PLACEHOLDER_NAME_RE.match(str(o.get("name") or "").strip()):
            continue
        cat = str(o.get("category") or "General")
        o["name"] = _humanize_placeholder_name(
            str(o.get("name") or ""), cat, str(o.get("ssoc_code") or ""), used_names
        )

    if merged_weights:
        _scale_employment_from_est(rows, TARGET_TOTAL_EMPLOYMENT)
    else:
        _scale_employments_to_anchor(rows, TARGET_TOTAL_EMPLOYMENT)

    if WAGE_YEAR == "2025":
        for o in rows:
            gross_2024 = int(o.get("gross_wage") or 0)
            o["gross_wage_2024_ref"] = gross_2024
            gross_2025 = int(round((gross_2024 * WAGE_GROWTH_2025) / 100.0) * 100)
            o["gross_wage_2025"] = gross_2025
            o["gross_wage"] = gross_2025

    # Optional score overrides from scores.json
    if SCORES_PATH.is_file():
        try:
            payload = json.loads(SCORES_PATH.read_text(encoding="utf-8"))
            by_name = {str(k).strip().lower(): v for k, v in payload.items() if isinstance(v, dict)}
            for o in rows:
                key = str(o.get("name") or "").strip().lower()
                hit = by_name.get(key)
                if not hit:
                    continue
                if "score" in hit:
                    o["ai_score"] = float(hit["score"])
                    if float(o["ai_score"]) > 4.0:
                        o["pwm"] = False
                if "reason" in hit and str(hit["reason"]).strip():
                    o["reason"] = str(hit["reason"]).strip()
                if "ai_assists" in hit:
                    o["ai_assists"] = bool(hit["ai_assists"])
                if "transition_targets" in hit and isinstance(hit["transition_targets"], list):
                    o["transition_targets"] = [str(x).strip() for x in hit["transition_targets"] if str(x).strip()]
                if "transition_path_1" in hit and str(hit["transition_path_1"]).strip():
                    o["transition_path_1"] = str(hit["transition_path_1"]).strip()
                if "transition_path_2" in hit and str(hit["transition_path_2"]).strip():
                    o["transition_path_2"] = str(hit["transition_path_2"]).strip()
        except Exception:
            pass
    return rows


def _load_name_zh_map() -> dict[str, str]:
    if not SSOC_MAP.is_file():
        return {}
    payload = json.loads(SSOC_MAP.read_text(encoding="utf-8"))
    by_code_bilingual = payload.get("by_code_bilingual") or {}
    return {
        str(code).strip().zfill(5): str(names.get("name_zh") or names.get("name_en") or "").strip()
        for code, names in by_code_bilingual.items()
    }


def build_hierarchy(occupations: list[dict[str, Any]]) -> dict[str, Any]:
    occupations = _prepare_occupations(occupations)
    name_zh_map = _load_name_zh_map()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for occ in occupations:
        enriched = dict(occ)
        ssoc = str(occ.get("ssoc_code") or "").strip().zfill(5)
        enriched["name_zh"] = name_zh_map.get(ssoc, str(occ.get("name") or ""))
        ai_score = float(occ.get("ai_score", 0))
        pwm = bool(occ.get("pwm", False))
        regulated = bool(occ.get("regulated", False))
        wfh = bool(occ.get("wfh", False))
        vulnerability_index = ai_score / 10.0
        if pwm:
            vulnerability_index -= 0.2
        if regulated:
            vulnerability_index -= 0.15
        if wfh:
            vulnerability_index += 0.08
        vulnerability_index = round(max(0.0, min(1.0, vulnerability_index)), 3)

        enriched["source_meta"] = {
            "ssoc_version": str(occ.get("ssoc_version", "SSOC 2024")),
            "wage_stat_year": int(occ.get("wage_stat_year", 2024)),
            "llm_model": str(occ.get("llm_model", "claude-haiku-3.5")),
        }
        enriched["vulnerability_index"] = float(occ.get("vulnerability_index", vulnerability_index))
        grouped[enriched["category"]].append(enriched)

    categories = []
    for idx, (category, occs) in enumerate(sorted(grouped.items()), start=1):
        categories.append(
            {
                "name": category,
                "order": idx,
                "children": sorted(occs, key=lambda x: x["name"]),
            }
        )

    scores = [float(o["ai_score"]) for o in occupations]
    has_est = any("employment_est" in o for o in occupations)
    if has_est:
        total_employment = int(sum(int(o.get("employment_est") or 0) for o in occupations))
        employment_method = (
            "meta.total_employment is the sum of employment_est across tracked SSOC rows "
            "(SingStat principal titles; default target ~370k AISCOPE_TRACKED_WORKFORCE in step2_merge). "
            "Per-row employment in this bundle is scaled from those weights to "
            f"AISCOPE_TARGET_TOTAL_EMPLOYMENT (default {TARGET_TOTAL_EMPLOYMENT:_}) for treemap national scale."
        )
    else:
        total_employment = int(sum(int(o["employment"]) for o in occupations))
        employment_method = (
            "Per-occupation counts are proportionally scaled so meta.total_employment matches "
            "the sum of scaled row weights aligned to AISCOPE_TARGET_TOTAL_EMPLOYMENT "
            f"(default {TARGET_TOTAL_EMPLOYMENT:_})."
        )
    avg_score = round(sum(scores) / max(len(scores), 1), 2)

    return {
        "meta": {
            "title": "AIScope SG",
            "source": "MOM occupational data + AIScope SG localized scoring",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_occupations": len(occupations),
            "total_employment": total_employment,
            "employment_anchor": TARGET_TOTAL_EMPLOYMENT,
            "employment_method": employment_method,
            "avg_ai_score": avg_score,
            "data_year": WAGE_YEAR,
            "wage_source": (
                "MOM Occupational Wages 2025 (estimated; official release pending Aug 2026)"
                if WAGE_YEAR == "2025"
                else "MOM Occupational Wages 2024"
            ),
            "national_median_2025": 5775 if WAGE_YEAR == "2025" else None,
        },
        "name": "Singapore Occupations",
        "children": categories,
    }


def write_data_zh(payload: dict[str, Any], output_path: Path = ZH_OUTPUT) -> None:
    """Write zh tree used by frontend language overlay."""
    zh_payload = json.loads(json.dumps(payload, ensure_ascii=False))
    category_label_map: dict[str, str] = {}
    for cat in zh_payload.get("children", []):
        en_cat = str(cat.get("name") or "")
        zh_cat = _CATEGORY_ZH.get(en_cat, en_cat)
        category_label_map[en_cat] = zh_cat
        cat["name"] = zh_cat
        for occ in cat.get("children", []):
            occ["name"] = occ.get("name_zh") or occ.get("name") or ""
    zh_payload["category_label_map"] = category_label_map
    zh_payload["category_labels"] = category_label_map
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(zh_payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_snapshot(payload: dict[str, Any]) -> Path | None:
    """Versioned copy of exported hierarchy for wage / diff analytics."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    snap_path = SNAPSHOT_DIR / f"data_{stamp}.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    snap_path.write_text(text, encoding="utf-8")
    return snap_path


def export_data_json(
    input_path: Path = DEFAULT_INPUT,
    output_path: Path = DEFAULT_OUTPUT,
) -> Path:
    occupations = json.loads(input_path.read_text(encoding="utf-8"))
    payload = build_hierarchy(occupations)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    write_data_zh(payload)
    snap = write_snapshot(payload)
    if snap:
        print(f"[Step 4] Snapshot -> {snap}")
    return output_path


if __name__ == "__main__":
    out = export_data_json()
    print(f"[Step 4] Saved -> {out}")

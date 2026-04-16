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

# National workforce anchor (policy-credible order of magnitude).
# See: World Bank / MOM labour-force statistics (~3.7M employed persons, 2024 ballpark).
TARGET_TOTAL_EMPLOYMENT = int(os.getenv("AISCOPE_TARGET_TOTAL_EMPLOYMENT", "3720000"))

_PLACEHOLDER_NAME_RE = re.compile(r"^.+\sOccupation\s\d{3}\s*$")


def _humanize_placeholder_name(name: str, category: str, ssoc: str, used: set[str]) -> str:
    """Replace synthetic 'Clerical Support Occupation 001' labels with role-style titles (still synthetic, not SingStat)."""
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
    """Proportionally rescale row employment so the sum matches a national anchor (fixes stacked synthetic counts)."""
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


def _prepare_occupations(occupations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [dict(o) for o in occupations]
    used_names = {str(o.get("name") or "") for o in rows}
    used_names.discard("")
    for o in rows:
        cat = str(o.get("category") or "General")
        o["name"] = _humanize_placeholder_name(str(o.get("name") or ""), cat, str(o.get("ssoc_code") or ""), used_names)
    _scale_employments_to_anchor(rows, TARGET_TOTAL_EMPLOYMENT)
    return rows
DEFAULT_INPUT = BASE / "data" / "processed" / "occupations_expanded.json"
DEFAULT_OUTPUT = BASE / "web" / "data" / "data.json"
SNAPSHOT_DIR = BASE / "data" / "processed" / "snapshots"


def build_hierarchy(occupations: list[dict[str, Any]]) -> dict[str, Any]:
    occupations = _prepare_occupations(occupations)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for occ in occupations:
        enriched = dict(occ)
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
    total_employment = int(sum(int(o["employment"]) for o in occupations))
    avg_score = round(sum(scores) / max(len(scores), 1), 2)

    return {
        "meta": {
            "title": "AIScope SG",
            "source": "MOM occupational data + AIScope SG localized scoring",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_occupations": len(occupations),
            "total_employment": total_employment,
            "employment_anchor": TARGET_TOTAL_EMPLOYMENT,
            "employment_method": (
                "Per-occupation counts are proportionally scaled so meta.total_employment matches "
                "AISCOPE_TARGET_TOTAL_EMPLOYMENT (default 3_720_000) while preserving relative weights "
                "across SSOC rows (corrects synthetic pipeline double-counting)."
            ),
            "avg_ai_score": avg_score,
        },
        "name": "Singapore Occupations",
        "children": categories,
    }


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
    snap = write_snapshot(payload)
    if snap:
        print(f"[Step 4] Snapshot -> {snap}")
    return output_path


if __name__ == "__main__":
    out = export_data_json()
    print(f"[Step 4] Saved -> {out}")

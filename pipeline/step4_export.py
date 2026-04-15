"""
AIScope SG — Step 4: Export hierarchical web data.json from flat occupations.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = BASE / "data" / "processed" / "occupations_expanded.json"
DEFAULT_OUTPUT = BASE / "web" / "data" / "data.json"


def build_hierarchy(occupations: list[dict[str, Any]]) -> dict[str, Any]:
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
            "avg_ai_score": avg_score,
        },
        "name": "Singapore Occupations",
        "children": categories,
    }


def export_data_json(
    input_path: Path = DEFAULT_INPUT,
    output_path: Path = DEFAULT_OUTPUT,
) -> Path:
    occupations = json.loads(input_path.read_text(encoding="utf-8"))
    payload = build_hierarchy(occupations)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


if __name__ == "__main__":
    out = export_data_json()
    print(f"[Step 4] Saved -> {out}")

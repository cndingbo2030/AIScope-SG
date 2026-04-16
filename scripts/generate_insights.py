#!/usr/bin/env python3
"""
Build web/data/insights.json — movers vs baseline, policy crossover copy, optional LLM overview.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent.parent
DEFAULT_CURRENT = BASE / "web" / "data" / "data.json"
DEFAULT_BASELINE = BASE / "web" / "data" / "data_baseline.json"
DEFAULT_OUT = BASE / "web" / "data" / "insights.json"
SNAP_DIR = BASE / "data" / "processed" / "snapshots"


def mean_gross_wage_proxy(payload: dict[str, Any]) -> float:
    """Simple occupation-mean gross wage for snapshot drift detection."""
    total = 0.0
    n = 0
    for cat in payload.get("children", []):
        for occ in cat.get("children", []):
            total += float(occ.get("gross_wage", 0) or 0)
            n += 1
    return total / max(n, 1)


def select_prior_snapshot_path() -> Path | None:
    """Prefer second-latest dated snapshot vs current export (step4 overwrites same day)."""
    if not SNAP_DIR.is_dir():
        return None
    files = sorted(SNAP_DIR.glob("data_*.json"))
    if len(files) >= 2:
        return files[-2]
    if len(files) == 1:
        return files[-1]
    return None


def wage_volatility_block(current_payload: dict[str, Any]) -> dict[str, Any]:
    prior_path = select_prior_snapshot_path()
    if not prior_path:
        return {
            "flag": False,
            "pct_change": None,
            "prior_snapshot": None,
            "note_en": "",
            "note_zh": "",
        }
    try:
        prior_payload = json.loads(prior_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "flag": False,
            "pct_change": None,
            "prior_snapshot": prior_path.name,
            "note_en": "",
            "note_zh": "",
        }

    cur_m = mean_gross_wage_proxy(current_payload)
    old_m = mean_gross_wage_proxy(prior_payload)
    if old_m <= 0:
        return {
            "flag": False,
            "pct_change": None,
            "prior_snapshot": prior_path.name,
            "note_en": "",
            "note_zh": "",
        }

    pct = (cur_m - old_m) / old_m * 100.0
    if abs(pct) <= 5.0:
        return {
            "flag": False,
            "pct_change": round(pct, 2),
            "prior_snapshot": prior_path.name,
            "note_en": "",
            "note_zh": "",
        }

    return {
        "flag": True,
        "pct_change": round(pct, 2),
        "prior_snapshot": prior_path.name,
        "note_en": (
            f"Occupation-mean gross wage proxy moved ~{pct:+.1f}% vs `{prior_path.name}`. "
            "Treat 2026 Market Outlook sector splits as provisional until MOM/SSOC release notes are reviewed."
        ),
        "note_zh": (
            f"职位平均税前月薪代理指标相对 `{prior_path.name}` 波动约 {pct:+.1f}%，在核对 MOM/SSOC 说明前请谨慎解读行业分布。"
        ),
    }


def flatten_scores(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for cat in payload.get("children", []):
        for occ in cat.get("children", []):
            code = str(occ.get("ssoc_code", "")).strip()
            if not code:
                continue
            out[code] = {
                "name": occ.get("name", ""),
                "ai_score": float(occ.get("ai_score", 0)),
                "gross_wage": float(occ.get("gross_wage", 0)),
                "category": cat.get("name", ""),
            }
    return out


def policy_crossover(current: dict[str, dict[str, Any]]) -> str:
    rows = list(current.values())
    n = max(len(rows), 1)
    high_wage = sum(1 for r in rows if r["gross_wage"] >= 5000 and r["ai_score"] >= 7)
    low_wage = sum(1 for r in rows if r["gross_wage"] < 5000 and r["ai_score"] >= 7)
    hw_pct = round(100 * high_wage / n, 1)
    lw_pct = round(100 * low_wage / n, 1)
    return (
        f"MOM median-wage trends intersect with AIScope exposure: {hw_pct}% of occupations sit in the "
        f"high-wage/high-exposure quadrant while {lw_pct}% combine lower wages with high exposure — "
        "a structural tension for SkillsFuture and sector councils as 2026 reasoning-heavy automation "
        "compresses premium desk workflows faster than frontline PWM floors adjust."
    )


def top_movers(
    current: dict[str, dict[str, Any]],
    baseline: dict[str, dict[str, Any]],
    limit: int = 10,
) -> list[dict[str, Any]]:
    deltas: list[tuple[float, str, float, float]] = []
    for code, cur in current.items():
        if code not in baseline:
            continue
        old = float(baseline[code]["ai_score"])
        new = float(cur["ai_score"])
        d = new - old
        if d > 0.05:
            deltas.append((d, code, old, new))
    deltas.sort(reverse=True)
    out: list[dict[str, Any]] = []
    for d, code, old, new in deltas[:limit]:
        row = current[code]
        out.append(
            {
                "ssoc_code": code,
                "name": row["name"],
                "category": row["category"],
                "previous_ai_score": round(old, 2),
                "current_ai_score": round(new, 2),
                "delta": round(d, 2),
            }
        )
    return out


def fallback_overview(n: int, avg: float) -> str:
    return (
        f"Singapore snapshot ({n} SSOC-style rows): mean AI exposure sits near {avg:.2f}/10 with a fat "
        "right tail in clerical and professional services as 2026 reasoning models absorb cross-document "
        "work. PWM-covered frontline roles remain capped but face gradual hardware substitution in pilots; "
        "policy focus should pair wage statistics with reskilling velocity, not headline model releases alone."
    )


def llm_overview_market(n: int, avg: float, api_key: str) -> str:
    try:
        from anthropic import Anthropic
    except ImportError:
        return fallback_overview(n, avg)

    client = Anthropic(api_key=api_key)
    model = os.getenv("ANTHROPIC_INSIGHTS_MODEL", "claude-3-5-haiku-20241022").strip()
    prompt = (
        f"Write ~200 Chinese characters (±30) summarising Singapore labour AI exposure for {n} "
        f"occupations with mean score {avg:.2f}/10. Mention policy + SkillsFuture once. No lists. Plain prose."
    )
    msg = client.messages.create(
        model=model,
        max_tokens=400,
        temperature=0.35,
        messages=[{"role": "user", "content": prompt}],
    )
    parts: list[str] = []
    for block in msg.content:
        if getattr(block, "type", None) == "text":
            parts.append(block.text)
    text = "\n".join(parts).strip()
    return text or fallback_overview(n, avg)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate web/data/insights.json")
    parser.add_argument("--current", type=Path, default=DEFAULT_CURRENT)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Skip Anthropic call; use deterministic market_overview text.",
    )
    args = parser.parse_args()

    if not args.current.exists():
        raise FileNotFoundError(f"Missing current data: {args.current}")

    current_payload = json.loads(args.current.read_text(encoding="utf-8"))
    current = flatten_scores(current_payload)
    scores = [x["ai_score"] for x in current.values()]
    avg = sum(scores) / max(len(scores), 1)

    baseline_map: dict[str, dict[str, Any]] = {}
    if args.baseline.exists():
        baseline_map = flatten_scores(json.loads(args.baseline.read_text(encoding="utf-8")))
    else:
        print(
            f"[insights] No baseline at {args.baseline}; top_movers will be empty. "
            "Copy current data.json to data_baseline.json before the next release to enable diffs.",
            file=sys.stderr,
        )

    movers = top_movers(current, baseline_map, limit=10)
    policy = policy_crossover(current)
    wage_vol = wage_volatility_block(current_payload)

    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if args.no_llm or not api_key:
        overview = fallback_overview(len(current), avg)
    else:
        overview = llm_overview_market(len(current), avg, api_key)

    last_updated = datetime.now(timezone.utc).strftime("%Y-%m")
    payload = {
        "last_updated": last_updated,
        "display_stamp": "2026-04",
        "top_movers": movers,
        "policy_crossover": policy,
        "market_overview": overview,
        "wage_volatility": wage_vol,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[insights] Wrote {args.out} (movers={len(movers)})")


if __name__ == "__main__":
    main()

"""
AIScope SG — Step 3: Localized AI exposure scoring
Score occupations with strict Singapore-specific constraints (2026 calibration).
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from anthropic import Anthropic

BASE = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = BASE / "data" / "processed" / "occupations_expanded.json"
DEFAULT_OUTPUT = BASE / "data" / "processed" / "scores.json"

DEFAULT_SCORING_MODEL = "claude-3-5-sonnet-20241022"

SYSTEM_PROMPT = """
You are a Singapore labor-market AI risk assessor for AIScope SG (2026 calibration).

Return ONLY strict JSON with this structure:
{
  "score": number,
  "reason": string,
  "wfh": boolean,
  "ai_assists": boolean,
  "risk_factor": string
}

Scoring requirements (2026 technology boundary):
1) Score range is 0.0-10.0 with one decimal place.
2) Chain-of-thought / reasoning models (e.g. long-horizon document reasoning, cross-clause
   consistency, spreadsheet-to-memo synthesis) materially raise exposure for roles whose
   core work is structured analysis: accounting, audit, paralegal-style legal drafting,
   compliance checking, and similar desk cognition. Where that applies, scores should reflect
   materially higher automation pressure than a 2024-era baseline (often +1.0 to +2.0 vs
   legacy intuition), unless PWM or licensing moats dominate.
3) PWM hard cap rule:
   - If occupation has pwm=true, final score MUST NOT exceed 4.0.
4) Physical-interaction "dynamic decay" (2025-2026):
   - Humanoid and mobile service robots are in early commercial pilots in Singapore retail,
     cleaning, security patrol, and F&B support. Physical isolation is NO LONGER a permanent
     moat: treat frontline cleaning/security/landscape-style PWM roles as having a slowly
     eroding physical barrier — allow scores toward the top of the PWM band when robot
     hardware, vendor SLAs, or standardized facility layouts could plausibly absorb tasks,
     while still respecting the 4.0 PWM ceiling.
5) Regulatory moat:
   - For legal (SAL), medical (MOH), and financial (MAS) contexts, evaluate licensing
     enforceability. Strong mandatory licensing should materially lower replacement risk.
   - Physically present, safety-critical, CAAS/regulatory-certified roles MUST score <= 4.0
     regardless of how cognitive the task may sound.
6) SkillsFuture transition advice:
   - reason must include one practical transition suggestion using a "SkillsFuture" framing.
7) Multilingual moat:
   - Account for Singapore multi-language + Singlish frontline communication as a possible
     defense in customer-facing roles (weaker for pure back-office cognition).
8) Distinguish augment vs replace:
   - ai_assists=true if AI mostly assists.
   - ai_assists=false if AI can directly replace a major chunk of work.

Reason style:
- 2 to 4 concise sentences.
- Mention one Singapore-specific factor and one transition pathway.

MANDATORY OVERRIDE RULES:
- If occupation involves physical presence AND government licensing: score <= 4.0
- 'Accountant','Bookkeeper','Auditor': score must be 7.0-8.5
- 'Air Traffic Controller','Pilot','Security Officer': score must be 2.0-3.5

MANDATORY SCORE CONSTRAINTS:
1. Accountants / Auditors / Bookkeepers: MUST score 7.0–8.5
2. Air Traffic Controllers / Pilots: MUST score 2.0–3.5
3. Nurses (all nursing roles): MUST score 2.5–4.0
4. Doctors / Medical Practitioners: MUST score 4.0–5.5
5. Any occupation where 'physical presence' OR 'government licensed' appears in reason:
   AUTO-CAP at 5.0 unless cognitive-only evidence
"""


PHYSICAL_DECAY_HINTS = (
    "clean",
    "janitor",
    "security",
    "guard",
    "porter",
    "housekeep",
    "landscape",
    "sanitary",
    "conservancy",
    "waiter",
    "waitress",
    "kitchen",
    "dish",
    "cook",
    "f&b",
    "retail",
    "cashier",
    "concierge",
)


def score_delta_driver(occ: dict[str, Any]) -> str:
    blob = f"{occ.get('name', '')} {occ.get('category', '')}".lower()
    if any(h in blob for h in PHYSICAL_DECAY_HINTS):
        return "Robot Hardware"
    return "Reasoning Capability"


@dataclass
class ScoreResult:
    score: float
    reason: str
    wfh: bool
    ai_assists: bool
    risk_factor: str

    def to_dict(self, scoring_model: str) -> dict[str, Any]:
        return {
            "score": self.score,
            "reason": self.reason,
            "wfh": self.wfh,
            "ai_assists": self.ai_assists,
            "risk_factor": self.risk_factor,
            "scoring_model": scoring_model,
        }


def load_occupations(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Input occupations file not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_scores(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def build_user_prompt(occ: dict[str, Any]) -> str:
    payload = {
        "occupation_name": occ.get("name"),
        "category": occ.get("category"),
        "ssoc_code": occ.get("ssoc_code"),
        "employment": occ.get("employment"),
        "gross_wage": occ.get("gross_wage"),
        "basic_wage": occ.get("basic_wage"),
        "pwm": bool(occ.get("pwm", False)),
        "regulated": bool(occ.get("regulated", False)),
        "notes": occ.get("notes", ""),
    }
    return (
        "Score this occupation for Singapore AI exposure using the 2026 calibration "
        "(reasoning-model exposure + dynamic physical moat decay under PWM caps).\n"
        f"Occupation payload:\n{json.dumps(payload, ensure_ascii=False)}\n\n"
        "Return strict JSON only."
    )


def extract_text_content(message: Any) -> str:
    chunks: list[str] = []
    for block in message.content:
        if getattr(block, "type", None) == "text":
            chunks.append(block.text)
    return "\n".join(chunks).strip()


def coerce_json(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json", "", 1).strip()
    left = text.find("{")
    right = text.rfind("}")
    if left == -1 or right == -1 or right <= left:
        raise ValueError("No JSON object found in response")
    return json.loads(text[left : right + 1])


def validate_result(payload: dict[str, Any], is_pwm: bool, occupation_name: str) -> ScoreResult:
    required = {"score", "reason", "wfh", "ai_assists", "risk_factor"}
    missing = required - payload.keys()
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")

    score = round(float(payload["score"]), 1)
    score = max(0.0, min(10.0, score))
    role = str(occupation_name or "").lower()
    reason_txt = str(payload.get("reason", "")).lower()

    if any(k in role for k in ("accountant", "bookkeeper", "auditor")):
        score = min(8.5, max(7.0, score))
    if any(k in role for k in ("air traffic controller", "pilot")):
        score = min(3.5, max(2.0, score))
    if "nurs" in role:
        score = min(4.0, max(2.5, score))
    if any(k in role for k in ("doctor", "medical practitioner", "physician")):
        score = min(5.5, max(4.0, score))
    if "physical presence" in reason_txt or "government licensed" in reason_txt:
        score = min(score, 5.0)
    if is_pwm:
        score = min(score, 4.0)

    result = ScoreResult(
        score=score,
        reason=str(payload["reason"]).strip(),
        wfh=bool(payload["wfh"]),
        ai_assists=bool(payload["ai_assists"]),
        risk_factor=str(payload["risk_factor"]).strip() or "General AI automation pressure",
    )
    return result


def score_with_retry(
    client: Anthropic,
    model: str,
    occupation: dict[str, Any],
    retries: int = 4,
    base_delay: float = 1.2,
) -> ScoreResult:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=900,
                temperature=0.1,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": build_user_prompt(occupation)}],
            )
            raw = extract_text_content(response)
            parsed = coerce_json(raw)
            return validate_result(
                parsed,
                is_pwm=bool(occupation.get("pwm", False)),
                occupation_name=str(occupation.get("name", "")),
            )
        except Exception as err:  # noqa: BLE001
            last_err = err
            if attempt == retries:
                break
            wait = base_delay * attempt
            print(f"  Retry {attempt}/{retries - 1} for {occupation.get('name')} after error: {err}")
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def maybe_append_refresh_note(
    occ: dict[str, Any],
    result: ScoreResult,
    previous_scores: dict[str, float],
    refresh_scores: bool,
) -> ScoreResult:
    if not refresh_scores:
        return result
    name = str(occ.get("name", ""))
    if name not in previous_scores:
        return result
    old = float(previous_scores[name])
    if abs(result.score - old) <= 1.0:
        return result
    driver = score_delta_driver(occ)
    suffix = f" Updated in 2026 due to {driver}."
    new_reason = result.reason.rstrip()
    if "Updated in 2026 due to" in new_reason:
        return result
    return ScoreResult(
        score=result.score,
        reason=new_reason + suffix,
        wfh=result.wfh,
        ai_assists=result.ai_assists,
        risk_factor=result.risk_factor,
    )


def run_step3(
    input_path: Path = DEFAULT_INPUT,
    output_path: Path = DEFAULT_OUTPUT,
    *,
    refresh_scores: bool = False,
    rerun_categories: list[str] | None = None,
) -> None:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY is required for step3 scoring.")

    model = os.getenv("ANTHROPIC_SCORING_MODEL", DEFAULT_SCORING_MODEL).strip() or DEFAULT_SCORING_MODEL

    occupations = load_occupations(input_path)
    scored = load_scores(output_path)
    previous_scores: dict[str, float] = {}
    if refresh_scores and scored:
        for key, val in scored.items():
            if isinstance(val, dict) and "score" in val:
                previous_scores[key] = float(val["score"])

    client = Anthropic(api_key=api_key)

    print(f"[Step 3] Model: {model}")
    print(f"[Step 3] Loaded occupations: {len(occupations)}")
    print(f"[Step 3] Existing scored entries: {len(scored)}")
    print(f"[Step 3] refresh_scores={refresh_scores}")
    if rerun_categories:
        print(f"[Step 3] rerun_categories={rerun_categories}")

    for idx, occ in enumerate(occupations, start=1):
        name = occ.get("name", f"occupation_{idx}")
        if rerun_categories:
            lname = str(name).lower()
            lcat = str(occ.get("category", "")).lower()
            if not any(p.lower() in lname or p.lower() in lcat for p in rerun_categories):
                continue
        if not refresh_scores and name in scored:
            continue

        print(f"[Step 3] Scoring {idx}/{len(occupations)}: {name}")
        try:
            result = score_with_retry(client, model, occ)
            result = maybe_append_refresh_note(occ, result, previous_scores, refresh_scores)
        except Exception as err:  # noqa: BLE001
            print(f"  Failed to score {name}: {err}")
            continue

        scored[name] = result.to_dict(model)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(scored, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        time.sleep(0.45)

    print(f"[Step 3] Completed. Saved -> {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="AIScope SG — localized LLM scoring (step 3).")
    parser.add_argument(
        "--refresh-scores",
        action="store_true",
        help="Re-score every occupation; if |Δscore|>1 vs prior scores.json, append 2026 refresh note.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Occupations JSON (list or compatible).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="scores.json output path.",
    )
    parser.add_argument(
        "--rerun-categories",
        nargs="+",
        default=None,
        help="Only re-score occupations whose name/category contains these substrings.",
    )
    args = parser.parse_args()
    run_step3(
        args.input,
        args.output,
        refresh_scores=args.refresh_scores,
        rerun_categories=args.rerun_categories,
    )


if __name__ == "__main__":
    main()

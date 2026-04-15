"""
AIScope SG — Step 3: Localized AI exposure scoring
Score occupations with strict Singapore-specific constraints.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from anthropic import Anthropic

BASE = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = BASE / "data" / "processed" / "occupations.json"
DEFAULT_OUTPUT = BASE / "data" / "processed" / "scores.json"

SYSTEM_PROMPT = """
You are a Singapore labor-market AI risk assessor for AIScope SG.

Return ONLY strict JSON with this structure:
{
  "score": number,
  "reason": string,
  "wfh": boolean,
  "ai_assists": boolean,
  "risk_factor": string
}

Scoring requirements:
1) Score range is 0.0-10.0 with one decimal place.
2) PWM hard cap rule:
   - If occupation has pwm=true, final score MUST NOT exceed 4.0.
   - This reflects physical isolation and policy wage-floor protections.
3) Regulatory moat:
   - For legal (SAL), medical (MOH), and financial (MAS) contexts, evaluate licensing enforceability.
   - Strong mandatory licensing should materially lower replacement risk.
4) SkillsFuture transition advice:
   - reason must include one practical transition suggestion using a "SkillsFuture" framing.
   - Example: data-entry role -> AI data labeling / workflow QA.
5) Multilingual moat:
   - Account for Singapore multi-language + Singlish frontline communication as a possible defense in customer-facing roles.
6) Distinguish augment vs replace:
   - ai_assists=true if AI mostly assists.
   - ai_assists=false if AI can directly replace a major chunk of work.

Reason style:
- 2 to 4 concise sentences.
- Mention one Singapore-specific factor and one transition pathway.
"""


@dataclass
class ScoreResult:
    score: float
    reason: str
    wfh: bool
    ai_assists: bool
    risk_factor: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "reason": self.reason,
            "wfh": self.wfh,
            "ai_assists": self.ai_assists,
            "risk_factor": self.risk_factor,
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
        "Score this occupation for Singapore AI exposure.\n"
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


def validate_result(payload: dict[str, Any], is_pwm: bool) -> ScoreResult:
    required = {"score", "reason", "wfh", "ai_assists", "risk_factor"}
    missing = required - payload.keys()
    if missing:
        raise ValueError(f"Missing fields: {sorted(missing)}")

    score = round(float(payload["score"]), 1)
    score = max(0.0, min(10.0, score))
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
    occupation: dict[str, Any],
    retries: int = 4,
    base_delay: float = 1.2,
) -> ScoreResult:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=420,
                temperature=0.1,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": build_user_prompt(occupation)}],
            )
            raw = extract_text_content(response)
            parsed = coerce_json(raw)
            return validate_result(parsed, is_pwm=bool(occupation.get("pwm", False)))
        except Exception as err:  # noqa: BLE001
            last_err = err
            if attempt == retries:
                break
            wait = base_delay * attempt
            print(f"  Retry {attempt}/{retries - 1} for {occupation.get('name')} after error: {err}")
            time.sleep(wait)
    assert last_err is not None
    raise last_err


def run_step3(input_path: Path = DEFAULT_INPUT, output_path: Path = DEFAULT_OUTPUT) -> None:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY is required for step3 scoring.")

    occupations = load_occupations(input_path)
    scored = load_scores(output_path)
    client = Anthropic(api_key=api_key)

    print(f"[Step 3] Loaded occupations: {len(occupations)}")
    print(f"[Step 3] Existing scored entries: {len(scored)}")

    for idx, occ in enumerate(occupations, start=1):
        name = occ.get("name", f"occupation_{idx}")
        if name in scored:
            continue

        print(f"[Step 3] Scoring {idx}/{len(occupations)}: {name}")
        try:
            result = score_with_retry(client, occ)
        except Exception as err:  # noqa: BLE001
            print(f"  Failed to score {name}: {err}")
            continue

        scored[name] = result.to_dict()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(scored, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        time.sleep(0.45)

    print(f"[Step 3] Completed. Saved -> {output_path}")


if __name__ == "__main__":
    run_step3()

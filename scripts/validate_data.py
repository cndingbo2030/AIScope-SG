"""
Validate web/data/data.json and generate audit reports.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

BASE = Path(__file__).resolve().parent.parent
DATA_JSON = BASE / "web" / "data" / "data.json"
ZH_JSON = BASE / "web" / "data" / "occupations_zh.json"
SCHEMA_JSON = BASE / "docs" / "data.schema.json"
DOCS_DIR = BASE / "docs"
AUDIT_JSON = DOCS_DIR / "audit_report.json"
AUDIT_MD = DOCS_DIR / "audit_summary.md"

REQUIRED_OCC_FIELDS = {
    "name",
    "ssoc_code",
    "employment",
    "gross_wage",
    "basic_wage",
    "ai_score",
    "reason",
    "wfh",
    "ai_assists",
    "risk_factor",
    "pwm",
    "regulated",
    "source_meta",
    "vulnerability_index",
}


def validate_occupations_zh(rows: list[dict[str, Any]]) -> list[str]:
    errs: list[str] = []
    if not ZH_JSON.exists():
        errs.append(f"missing {ZH_JSON.relative_to(BASE)} (run: python3 scripts/translate_data.py --seed)")
        return errs
    try:
        zh_payload = json.loads(ZH_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"occupations_zh.json invalid JSON: {exc}"]
    by_ssoc = zh_payload.get("by_ssoc") or {}
    codes = {str(r["ssoc_code"]).strip() for r in rows if str(r.get("ssoc_code", "")).strip()}
    if len(by_ssoc) != len(codes):
        errs.append(
            f"occupations_zh.json: by_ssoc count {len(by_ssoc)} != occupations {len(codes)}"
        )
    missing = codes - set(by_ssoc.keys())
    if missing:
        sample = sorted(missing)[:8]
        errs.append(f"occupations_zh.json missing {len(missing)} ssoc keys (e.g. {sample})")
    extra = set(by_ssoc.keys()) - codes
    if extra:
        errs.append(f"occupations_zh.json has {len(extra)} unknown ssoc keys vs data.json")
    return errs


def flatten_occupations(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for category in data.get("children", []):
        for occ in category.get("children", []):
            rows.append({**occ, "_category": category.get("name", "unknown")})
    return rows


def write_reports(report: dict[str, Any]) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# AIScope SG Audit Summary",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Occupation count: `{report['occupation_count']}`",
        f"- Validation pass rate: `{report['pass_rate_percent']:.2f}%`",
        f"- Error count: `{len(report['errors'])}`",
        f"- Warning count: `{len(report['warnings'])}`",
        "",
        "## Source Meta Statistics",
        f"- SSOC versions: `{report['source_meta_stats']['ssoc_versions']}`",
        f"- Wage years: `{report['source_meta_stats']['wage_years']}`",
        f"- LLM models: `{report['source_meta_stats']['llm_models']}`",
        "",
        "## Notable Anomalies",
    ]
    anomalies = report["anomalies"][:30]
    if not anomalies:
        lines.append("- None detected")
    else:
        for item in anomalies:
            lines.append(f"- {item}")
    AUDIT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    if not DATA_JSON.exists():
        print(f"ERROR: missing file {DATA_JSON}")
        return 1

    data = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA_JSON.read_text(encoding="utf-8"))
    errors: list[str] = []
    warnings: list[str] = []
    anomalies: list[str] = []

    children = data.get("children", [])
    if not isinstance(children, list) or not children:
        errors.append("children must be a non-empty list")
    all_occupations = flatten_occupations(data)

    errors.extend(validate_occupations_zh(all_occupations))

    validator = Draft202012Validator(schema)
    schema_errors = sorted(validator.iter_errors(data), key=lambda e: e.path)
    for err in schema_errors:
        path = "/".join(str(x) for x in err.path)
        errors.append(f"schema:{path}: {err.message}")

    for category in children:
        name = category.get("name")
        if not name:
            errors.append("category missing name")
            continue
        occs = category.get("children", [])
        if not isinstance(occs, list):
            errors.append(f"{name}: children must be list")
            continue
        for occ in occs:
            missing = REQUIRED_OCC_FIELDS - occ.keys()
            if missing:
                errors.append(f"{name}/{occ.get('name', '<unnamed>')}: missing {sorted(missing)}")
                continue

            score = occ.get("ai_score")
            try:
                score = float(score)
            except (TypeError, ValueError):
                errors.append(f"{name}/{occ.get('name')}: ai_score is not numeric")
                continue

            if score < 0 or score > 10:
                errors.append(f"{name}/{occ.get('name')}: ai_score out of range {score}")
            if score >= 9.8:
                warnings.append(f"{name}/{occ.get('name')}: extremely high score {score}")
                anomalies.append(f"{name}/{occ.get('name')}: extremely high AI score {score}")
            if score <= 0.1:
                warnings.append(f"{name}/{occ.get('name')}: near-zero score {score}")
                anomalies.append(f"{name}/{occ.get('name')}: near-zero AI score {score}")

            if occ.get("pwm") and score > 4.0:
                errors.append(f"{name}/{occ.get('name')}: pwm=true but score={score} exceeds 4.0 cap")

            emp = occ.get("employment")
            if not isinstance(emp, (int, float)) or emp <= 0:
                errors.append(f"{name}/{occ.get('name')}: employment must be > 0")
            elif emp > 300000:
                anomalies.append(f"{name}/{occ.get('name')}: unusually high employment {emp}")

            if score == 0:
                warnings.append(f"{name}/{occ.get('name')}: zero score detected")

            gross = occ.get("gross_wage")
            if isinstance(gross, (int, float)) and gross > 20000:
                anomalies.append(f"{name}/{occ.get('name')}: unusually high wage {gross}")

    print(f"Validated occupations: {len(all_occupations)}")
    if len(all_occupations) < 570:
        warnings.append(f"occupation count below target 570: current={len(all_occupations)}")
    if warnings:
        print("Warnings:")
        for item in warnings:
            print(f"  - {item}")

    total_checks = max(1, len(all_occupations))
    pass_count = max(0, total_checks - len(errors))
    pass_rate = (pass_count / total_checks) * 100

    source_meta_versions = Counter(str(x.get("source_meta", {}).get("ssoc_version", "unknown")) for x in all_occupations)
    source_meta_years = Counter(str(x.get("source_meta", {}).get("wage_stat_year", "unknown")) for x in all_occupations)
    source_meta_models = Counter(str(x.get("source_meta", {}).get("llm_model", "unknown")) for x in all_occupations)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "occupation_count": len(all_occupations),
        "pass_rate_percent": pass_rate,
        "errors": errors,
        "warnings": warnings,
        "anomalies": anomalies,
        "source_meta_stats": {
            "ssoc_versions": dict(source_meta_versions),
            "wage_years": dict(source_meta_years),
            "llm_models": dict(source_meta_models),
        },
    }
    write_reports(report)

    if errors:
        print("Errors:")
        for item in errors:
            print(f"  - {item}")
        print(f"Audit report -> {AUDIT_JSON}")
        print(f"Audit summary -> {AUDIT_MD}")
        return 1

    print("Validation passed.")
    print(f"Audit report -> {AUDIT_JSON}")
    print(f"Audit summary -> {AUDIT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

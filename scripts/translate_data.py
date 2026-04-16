#!/usr/bin/env python3
"""
Optional: LLM-translate occupation titles to zh-CN, or --seed rule-based placeholders.

Writes web/data/occupations_zh.json: { meta, by_ssoc, category_labels }.

With --data-zh: batch-translate names, reasons, and categories into Singapore Chinese using
Claude 3.5 Sonnet (or ANTHROPIC_TRANSLATE_MODEL), writes web/data/data_zh.json (same tree shape
as data.json plus category_label_map), refreshes occupations_zh.json for CI parity, and validates
structure vs data.json.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parent.parent
DATA_JSON = BASE / "web" / "data" / "data.json"
OUT_JSON = BASE / "web" / "data" / "occupations_zh.json"
DATA_ZH_JSON = BASE / "web" / "data" / "data_zh.json"

# Heuristic category labels (expand as needed).
CATEGORY_ZH: dict[str, str] = {
    "Clerical Support": "文书支援",
    "Managers": "经理人员",
    "Professionals": "专业人员",
    "Technicians and Associate Professionals": "技师与助理专业人员",
    "Service and Sales Workers": "服务与销售人员",
    "Craft and Related Trades Workers": "工艺及相关行业人员",
    "Plant and Machine Operators": "工厂与机械操作员",
    "Elementary Occupations": "基础职业",
    "Cleaners and Helpers": "清洁工与帮工",
    "Security Guards": "保安人员",
}


def flatten(data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cat in data.get("children", []):
        cname = str(cat.get("name", ""))
        for occ in cat.get("children", []):
            rows.append({**occ, "_category": cname})
    return rows


def seed_zh_name(occ: dict[str, Any]) -> str:
    cat = str(occ.get("_category", occ.get("category", "")))
    base = CATEGORY_ZH.get(cat, cat[:8] if cat else "职业")
    name = str(occ.get("name", ""))
    m = re.search(r"(\d{3,5})\s*$", name)
    tail = m.group(1) if m else str(occ.get("ssoc_code", ""))[-3:]
    return f"{base}岗位 {tail}"


def seed_category_labels(rows: list[dict[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for occ in rows:
        c = str(occ.get("_category", ""))
        if c and c not in out:
            out[c] = CATEGORY_ZH.get(c, c)
    return out


def run_seed() -> None:
    data = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    rows = flatten(data)
    by_ssoc: dict[str, str] = {}
    for occ in rows:
        code = str(occ.get("ssoc_code", "")).strip()
        if not code:
            continue
        by_ssoc[code] = seed_zh_name(occ)
    payload = {
        "meta": {
            "mode": "seed",
            "note": "Rule-based labels for CI; run translate_data.py with ANTHROPIC_API_KEY for LLM polish.",
        },
        "by_ssoc": by_ssoc,
        "category_labels": seed_category_labels(rows),
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[translate] seed wrote {len(by_ssoc)} titles -> {OUT_JSON}")


def _extract_json_object(text: str) -> dict[str, Any]:
    left = text.find("{")
    right = text.rfind("}")
    if left == -1 or right == -1:
        raise RuntimeError("No JSON object in model response")
    return json.loads(text[left : right + 1])


def run_llm(batch_size: int = 40) -> None:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY required for LLM mode (or use --seed).")

    from anthropic import Anthropic

    model = os.getenv("ANTHROPIC_TRANSLATE_MODEL", "claude-3-5-sonnet-20241022").strip()
    data = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    rows = flatten(data)
    existing: dict[str, str] = {}
    if OUT_JSON.exists():
        prev = json.loads(OUT_JSON.read_text(encoding="utf-8"))
        existing = dict(prev.get("by_ssoc") or {})

    client = Anthropic(api_key=api_key)
    by_ssoc = dict(existing)
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        lines = [
            f"{str(r.get('ssoc_code','')).strip()}\t{r.get('name','')}\t{r.get('_category','')}"
            for r in chunk
        ]
        prompt = (
            "Translate the JOB TITLE column to concise zh-CN for a Singapore workforce dashboard. "
            "Keep SSOC codes unchanged. Output STRICT JSON object mapping ssoc_code string -> zh title only, "
            "no markdown.\n\nTSV (ssoc, name_en, category):\n"
            + "\n".join(lines)
        )
        msg = client.messages.create(
            model=model,
            max_tokens=1200,
            temperature=0.2,
            messages=[{"role": "user", "content": prompt}],
        )
        text = "".join(b.text for b in msg.content if getattr(b, "type", None) == "text")
        part = _extract_json_object(text)
        for k, v in part.items():
            if isinstance(v, str) and v.strip():
                by_ssoc[str(k).strip()] = v.strip()
        print(f"[translate] batch {i // batch_size + 1} merged, total keys={len(by_ssoc)}")
        time.sleep(0.35)

    payload = {
        "meta": {"mode": "llm", "model": model},
        "by_ssoc": by_ssoc,
        "category_labels": seed_category_labels(rows),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[translate] wrote -> {OUT_JSON}")


def _same_structure(en: Any, zh: Any, path: str) -> list[str]:
    """Recursively compare JSON shape (keys, list lengths, types); leaf values may differ (e.g. EN vs zh)."""
    errs: list[str] = []
    if type(en) is not type(zh):
        errs.append(f"type mismatch at {path}: {type(en).__name__} vs {type(zh).__name__}")
        return errs
    if isinstance(en, dict):
        ek, zk = set(en), set(zh)
        if ek != zk:
            missing = sorted(ek - zk)
            extra = sorted(zk - ek)
            errs.append(f"dict key mismatch at {path}: missing={missing[:8]} extra={extra[:8]}")
        for k in sorted(ek & zk):
            errs.extend(_same_structure(en[k], zh[k], f"{path}.{k}"))
    elif isinstance(en, list):
        if len(en) != len(zh):
            errs.append(f"list length at {path}: {len(en)} vs {len(zh)}")
        else:
            for i, (a, b) in enumerate(zip(en, zh, strict=True)):
                errs.extend(_same_structure(a, b, f"{path}[{i}]"))
    return errs


def validate_data_zh(en_data: dict[str, Any], zh_data: dict[str, Any]) -> list[str]:
    """Ensure zh tree mirrors en (same keys and nesting); leaf string values may differ."""
    zh_trim = {k: v for k, v in zh_data.items() if k != "category_label_map"}
    return _same_structure(en_data, zh_trim, "root")


def _translate_categories_llm(
    client: Any, model: str, names: list[str]
) -> dict[str, str]:
    prompt = (
        "Map each English SSOC category label to natural Singapore Chinese for a workforce dashboard. "
        "Use local wording (e.g. 巴士车长 style where it fits job families). "
        "Output STRICT JSON only: object mapping each English string exactly as given -> Chinese label.\n\n"
        "Categories (JSON array):\n"
        + json.dumps(names, ensure_ascii=False)
    )
    msg = client.messages.create(
        model=model,
        max_tokens=1500,
        temperature=0.2,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(b.text for b in msg.content if getattr(b, "type", None) == "text")
    part = _extract_json_object(text)
    out: dict[str, str] = {}
    for en in names:
        v = part.get(en)
        if isinstance(v, str) and v.strip():
            out[en] = v.strip()
    return out


def _translate_occ_batch_llm(
    client: Any, model: str, chunk: list[dict[str, Any]]
) -> dict[str, dict[str, str]]:
    lines = []
    for r in chunk:
        code = str(r.get("ssoc_code", "")).strip()
        name = str(r.get("name", "")).replace("\t", " ")
        reason = str(r.get("reason", "")).replace("\t", " ").replace("\n", " ")[:1200]
        cat = str(r.get("_category", ""))
        lines.append(f"{code}\t{name}\t{cat}\t{reason}")
    prompt = (
        "You translate for AIScope SG (Singapore AI job exposure index). "
        "Translate each occupation display name and the public scoring rationale into natural Singapore Chinese "
        "(zh-Hans, Singapore usage: 初级学院教师, 巴士车长, 小贩助手, etc. where appropriate). "
        "Preserve numbers, SSOC codes, and policy facts; keep tone formal and concise.\n"
        "Output STRICT JSON only: object mapping ssoc_code string -> "
        '{"name": "中文职位名", "reason": "中文理由段落"}.\n\n'
        "TSV rows: ssoc_code, name_en, category_en, reason_en\n"
        + "\n".join(lines)
    )
    msg = client.messages.create(
        model=model,
        max_tokens=4096,
        temperature=0.2,
        messages=[{"role": "user", "content": prompt}],
    )
    text = "".join(b.text for b in msg.content if getattr(b, "type", None) == "text")
    part = _extract_json_object(text)
    out: dict[str, dict[str, str]] = {}
    for k, v in part.items():
        code = str(k).strip()
        if not code or not isinstance(v, dict):
            continue
        nm = str(v.get("name", "")).strip()
        rs = str(v.get("reason", "")).strip()
        if nm:
            out[code] = {"name": nm, "reason": rs}
    return out


def _write_occupations_sidecar(
    zh_tree: dict[str, Any], category_label_map: dict[str, str], model: str
) -> None:
    by_ssoc: dict[str, str] = {}
    for cat in zh_tree.get("children", []):
        for occ in cat.get("children", []):
            code = str(occ.get("ssoc_code", "")).strip()
            if code:
                by_ssoc[code] = str(occ.get("name", ""))
    payload = {
        "meta": {"mode": "data-zh-derived", "model": model},
        "by_ssoc": by_ssoc,
        "category_labels": dict(category_label_map),
    }
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_data_zh(batch_size: int = 22) -> None:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("ANTHROPIC_API_KEY required for --data-zh (or use --seed).")

    from anthropic import Anthropic

    model = os.getenv("ANTHROPIC_TRANSLATE_MODEL", "claude-3-5-sonnet-20241022").strip()
    en_data = json.loads(DATA_JSON.read_text(encoding="utf-8"))
    rows = flatten(en_data)
    cat_names = [str(c.get("name", "")) for c in en_data.get("children", []) if c.get("name")]

    client = Anthropic(api_key=api_key)
    print("[translate] translating category labels…")
    category_label_map = _translate_categories_llm(client, model, cat_names)
    for n in cat_names:
        category_label_map.setdefault(n, CATEGORY_ZH.get(n, n))

    by_ssoc_tr: dict[str, dict[str, str]] = {}
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        part = _translate_occ_batch_llm(client, model, chunk)
        by_ssoc_tr.update(part)
        print(f"[translate] data-zh batch {i // batch_size + 1}, total ssoc keys={len(by_ssoc_tr)}")
        time.sleep(0.4)

    zh_tree: dict[str, Any] = copy.deepcopy(en_data)
    for cat in zh_tree.get("children", []):
        en_cat = str(cat.get("name", ""))
        cat["name"] = category_label_map.get(en_cat, en_cat)
        for occ in cat.get("children", []):
            code = str(occ.get("ssoc_code", "")).strip()
            tr = by_ssoc_tr.get(code)
            if tr:
                occ["name"] = tr.get("name", occ.get("name"))
                if tr.get("reason"):
                    occ["reason"] = tr["reason"]

    zh_tree["category_label_map"] = category_label_map

    errs = validate_data_zh(en_data, zh_tree)
    if errs:
        print("VALIDATION FAILED (structure):", file=sys.stderr)
        for e in errs[:40]:
            print(f"  {e}", file=sys.stderr)
        raise SystemExit(1)

    codes_en = {str(r.get("ssoc_code", "")).strip() for r in rows if str(r.get("ssoc_code", "")).strip()}
    missing = sorted(codes_en - set(by_ssoc_tr))
    if missing:
        print(
            f"[translate] warning: {len(missing)} occupations missing LLM name/reason "
            f"(e.g. {missing[:5]}); English left for those fields.",
            file=sys.stderr,
        )

    DATA_ZH_JSON.parent.mkdir(parents=True, exist_ok=True)
    DATA_ZH_JSON.write_text(json.dumps(zh_tree, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[translate] wrote -> {DATA_ZH_JSON}")

    _write_occupations_sidecar(zh_tree, category_label_map, model)
    print(f"[translate] refreshed -> {OUT_JSON}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build occupations_zh.json or full data_zh.json")
    parser.add_argument("--seed", action="store_true", help="Deterministic zh labels without API.")
    parser.add_argument(
        "--data-zh",
        action="store_true",
        help="Translate names, reasons, categories into data_zh.json (+ refresh occupations_zh.json).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="Occupations per LLM batch for --data-zh (default 22).",
    )
    args = parser.parse_args()
    if not DATA_JSON.exists():
        print(f"ERROR: missing {DATA_JSON}", file=sys.stderr)
        return 1
    if args.seed:
        run_seed()
        return 0
    if args.data_zh:
        bs = args.batch_size if args.batch_size > 0 else 22
        run_data_zh(batch_size=bs)
        return 0
    run_llm()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

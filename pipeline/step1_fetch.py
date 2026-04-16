"""
AIScope SG — Step 1: Fetch wage / occupation tabular data from data.gov.sg (CKAN datastore).

Uses Production API key from env DATA_GOV_SG_API_KEY in header ``x-api-key`` (official spec).
If the key is missing or the API returns an error, falls back to ``data/raw/wages_fallback.json``
and exits successfully so CI/CD does not break.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent.parent
ENV_PATH = BASE / ".env"
DEFAULT_FALLBACK = BASE / "data" / "raw" / "wages_fallback.json"
DEFAULT_OUTPUT = BASE / "data" / "raw" / "wages_fetched.json"

# Default resource_id = dataset_id from data.gov.sg dataset URL (tabular resource).
DEFAULT_RESOURCE_ID = "d_ec5d0e4ebdd2baee2a5aa1322a3156a5"
DATASTORE_SEARCH = "https://data.gov.sg/api/action/datastore_search"


def _headers(api_key: str) -> dict[str, str]:
    h: dict[str, str] = {"Accept": "application/json", "User-Agent": "AIScope-SG-pipeline/1.0"}
    if api_key:
        # Official: https://guide.data.gov.sg/developer-guide/api-overview/how-to-use-your-api-key
        h["x-api-key"] = api_key
    return h


def fetch_datastore(
    resource_id: str,
    api_key: str,
    *,
    limit: int = 100,
    timeout: int = 60,
) -> dict[str, Any]:
    params: dict[str, Any] = {"resource_id": resource_id, "limit": limit}
    resp = requests.get(
        DATASTORE_SEARCH,
        params=params,
        headers=_headers(api_key),
        timeout=timeout,
    )
    resp.raise_for_status()
    body = resp.json()
    if not body.get("success"):
        raise RuntimeError(f"datastore_search success=false: {repr(body)[:400]}")
    result = body.get("result") or {}
    return {
        "source": "data.gov.sg_api",
        "resource_id": resource_id,
        "endpoint": DATASTORE_SEARCH,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "fields": result.get("fields"),
        "records": result.get("records") or [],
        "_total": result.get("total"),
    }


def load_fallback(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Fallback file missing: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    # Accept either full envelope or bare list of records.
    if isinstance(raw, list):
        records = raw
    elif isinstance(raw, dict) and "records" in raw:
        records = raw["records"]
    else:
        raise ValueError("Fallback JSON must be a list of records or {\"records\": [...]}")
    return {
        "source": "fallback_file",
        "resource_id": None,
        "endpoint": None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "fields": None,
        "records": records,
        "_total": len(records),
    }


def run_fetch(
    *,
    resource_id: str | None = None,
    api_key: str | None = None,
    fallback_path: Path = DEFAULT_FALLBACK,
    output_path: Path = DEFAULT_OUTPUT,
    limit: int = 100,
) -> Path:
    load_dotenv(ENV_PATH, override=False)

    rid = (resource_id or os.getenv("DATA_GOV_SG_RESOURCE_ID", DEFAULT_RESOURCE_ID)).strip()
    key = (api_key if api_key is not None else os.getenv("DATA_GOV_SG_API_KEY", "")).strip()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not key:
        print("[Step 1] WARN: DATA_GOV_SG_API_KEY not set — using wages fallback.", file=sys.stderr)
        payload = load_fallback(fallback_path)
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[Step 1] Fallback saved -> {output_path} ({len(payload['records'])} records)")
        return output_path

    try:
        payload = fetch_datastore(rid, key, limit=limit)
    except Exception as err:  # noqa: BLE001
        print(f"[Step 1] WARN: API fetch failed ({err}); using wages fallback.", file=sys.stderr)
        try:
            payload = load_fallback(fallback_path)
        except Exception as fb_err:  # noqa: BLE001
            print(f"[Step 1] ERROR: fallback also failed: {fb_err}", file=sys.stderr)
            raise
        output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[Step 1] Fallback saved -> {output_path} ({len(payload['records'])} records)")
        return output_path

    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[Step 1] API OK -> {output_path} ({len(payload['records'])} records, total={payload.get('_total')})")
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch data.gov.sg datastore into data/raw/wages_fetched.json")
    parser.add_argument("--resource-id", default=None, help="Override DATA_GOV_SG_RESOURCE_ID / default dataset id")
    parser.add_argument("--limit", type=int, default=100, help="Max rows per request (pagination not implemented)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--fallback", type=Path, default=DEFAULT_FALLBACK)
    args = parser.parse_args()
    try:
        run_fetch(
            resource_id=args.resource_id,
            api_key=None,
            fallback_path=args.fallback,
            output_path=args.output,
            limit=args.limit,
        )
        return 0
    except Exception as err:  # noqa: BLE001
        print(f"[Step 1] FATAL: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

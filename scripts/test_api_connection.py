#!/usr/bin/env python3
"""
Smoke-test data.gov.sg Production API key against a known dataset resource.

Usage:
  export DATA_GOV_SG_API_KEY=...   # or load from .env via dotenv
  python3 scripts/test_api_connection.py

Default resource_id matches pipeline/step1_fetch.py (dataset tabular id).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from dotenv import load_dotenv

load_dotenv(BASE / ".env", override=False)

from pipeline.step1_fetch import (  # noqa: E402
    DEFAULT_RESOURCE_ID,
    DATASTORE_SEARCH,
    _headers,
)


def main() -> int:
    key = os.getenv("DATA_GOV_SG_API_KEY", "").strip()
    if not key:
        print("ERROR: set DATA_GOV_SG_API_KEY (or add it to .env)", file=sys.stderr)
        return 1

    import requests

    rid = os.getenv("DATA_GOV_SG_RESOURCE_ID", DEFAULT_RESOURCE_ID).strip()
    url = DATASTORE_SEARCH
    params = {"resource_id": rid, "limit": 5}
    resp = requests.get(url, params=params, headers=_headers(key), timeout=60)
    print(f"HTTP {resp.status_code} GET {url}")
    print(f"resource_id={rid}")
    try:
        body = resp.json()
    except json.JSONDecodeError:
        print(resp.text[:500])
        return 1

    if not body.get("success"):
        print("ERROR: success=false", json.dumps(body, indent=2)[:1200])
        return 1

    result = body.get("result") or {}
    records = result.get("records") or []
    if not records:
        print("WARN: zero records (check resource_id / dataset access). Raw result keys:", list(result.keys()))
        return 2

    first = records[0]
    print("First occupation row (keys):", list(first.keys()))
    print("First occupation row (JSON):")
    print(json.dumps(first, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

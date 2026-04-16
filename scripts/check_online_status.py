"""
Smoke-test the public GitHub Pages deployment: HTTP 200 + index contains <base id="ais-base">.
"""

from __future__ import annotations

import os
import sys

import requests

DEFAULT_URL = "https://cndingbo2030.github.io/AIiScope-SG/"


def main() -> int:
    base = os.environ.get("GITHUB_PAGES_URL", DEFAULT_URL).strip()
    if not base.endswith("/"):
        base += "/"
    no_slash = base.rstrip("/")
    urls = [base, no_slash, base + "?job=20008"]
    session = requests.Session()
    session.headers.update({"User-Agent": "AIScope-SG-check_online_status/1.0"})

    for url in urls:
        print(f"[check] GET {url}")
        try:
            resp = session.get(url, timeout=25)
        except requests.RequestException as exc:
            print(f"  ERROR: request failed: {exc}", file=sys.stderr)
            return 1
        print(f"  status={resp.status_code} len={len(resp.text)}")
        if resp.status_code != 200:
            print("  ERROR: expected HTTP 200", file=sys.stderr)
            return 1
        body = resp.text
        if 'id="ais-base"' not in body and "id='ais-base'" not in body:
            print("  ERROR: response HTML missing <base id=\"ais-base\"> (deploy may be stale or wrong URL)", file=sys.stderr)
            return 1
        print("  OK: ais-base present")

    print("[check] All URLs passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

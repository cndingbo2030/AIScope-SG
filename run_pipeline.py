#!/usr/bin/env python3
"""
AIScope SG pipeline entrypoint.

Examples:
  python run_pipeline.py --fetch              # data.gov.sg fetch (fallback-safe)
  python run_pipeline.py --refresh-scores   # Re-run LLM scoring with 2026 delta notes
  python pipeline/step3_score.py             # Incremental scoring (default)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))


def main() -> None:
    parser = argparse.ArgumentParser(description="AIScope SG pipeline runner.")
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Run pipeline/step1_fetch.py (data.gov.sg + fallback).",
    )
    parser.add_argument(
        "--refresh-scores",
        action="store_true",
        help="Re-score all occupations (see pipeline/step3_score.py).",
    )
    parser.add_argument(
        "--wage-year",
        default=None,
        help="Set wage projection year for export metadata/scaling (e.g. 2025).",
    )
    parser.add_argument(
        "--rerun-categories",
        nargs="+",
        default=None,
        help="Re-score occupations by name/category keyword subsets.",
    )
    args = parser.parse_args()

    if args.wage_year:
        os.environ["AISCOPE_WAGE_YEAR"] = str(args.wage_year).strip()

    if args.fetch:
        from pipeline.step1_fetch import main as step1_main

        # step1_fetch must not parse parent's argv (e.g. --fetch is unknown there).
        raise SystemExit(step1_main([]))

    if args.refresh_scores:
        from pipeline.step3_score import run_step3

        run_step3(refresh_scores=True, rerun_categories=args.rerun_categories)
        return

    if args.rerun_categories:
        from pipeline.step3_score import run_step3

        run_step3(refresh_scores=True, rerun_categories=args.rerun_categories)
        return

    parser.print_help()
    print("\nTip: use `python run_pipeline.py --fetch` for open-data pull, "
          "`python pipeline/step3_score.py` for incremental scoring, or "
          "`python run_pipeline.py --refresh-scores` for a full refresh.")


if __name__ == "__main__":
    main()

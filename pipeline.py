"""
pipeline.py — Main Orchestrator
=================================
Entry point that runs the full ELT flow:

    INGEST (dlt + GitHub API)
        → STAGING (DuckDB SQL cleaning)
            → MARTS (Polars incremental aggregation)

Usage::

    python pipeline.py                         # defaults: org=apache, lookback=7d
    python pipeline.py --org python            # fetch Python org repos
    python pipeline.py --lookback-days 30      # wider incremental window
    python pipeline.py --skip-ingest           # re-run transforms only
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv


# ── Banner ───────────────────────────────────────────────────────────────────

BANNER = r"""
 ╔══════════════════════════════════════════════════════════════╗
 ║   Embedded Modern Data Stack — ELT Pipeline                 ║
 ║   dlt → Parquet → DuckDB → Polars  (Zero-Copy Arrow)       ║
 ╚══════════════════════════════════════════════════════════════╝
"""


# ── Helpers ──────────────────────────────────────────────────────────────────

def _phase(label: str) -> None:
    """Print a phase separator."""
    width = 60
    print(f"\n{'─' * width}")
    print(f"  {label}")
    print(f"{'─' * width}")


def _print_summary() -> None:
    """Print a summary of all Parquet files produced by the pipeline."""
    dirs = [Path("data/raw"), Path("data/staging"), Path("data/marts")]
    print(f"\n{'━' * 60}")
    print("  📊 Pipeline Summary")
    print(f"{'━' * 60}")

    for d in dirs:
        files = list(d.rglob("*.parquet")) if d.exists() else []
        total_bytes = sum(f.stat().st_size for f in files)
        total_mb = total_bytes / (1024 * 1024)
        print(f"  {d!s:<20s}  {len(files):>3} file(s)  {total_mb:>7.2f} MB")

    print(f"{'━' * 60}\n")


# ── Main ─────────────────────────────────────────────────────────────────────

def main(org: str, lookback_days: int, skip_ingest: bool) -> None:
    """Orchestrate the full ELT pipeline."""

    print(BANNER)
    t0 = time.perf_counter()

    # ── 1. INGEST ────────────────────────────────────────────────────────
    if skip_ingest:
        _phase("⏭  INGEST  (skipped)")
    else:
        _phase("📥 INGEST  — dlt → data/raw/")
        from ingest import run_pipeline as run_ingest

        run_ingest(org=org)

    # ── 2. STAGING ───────────────────────────────────────────────────────
    _phase("🔧 STAGING — DuckDB SQL → data/staging/")
    from transform.staging import run_staging

    run_staging()

    # ── 3. MARTS ─────────────────────────────────────────────────────────
    _phase("📈 MARTS   — Polars aggregation → data/marts/")
    from transform.marts import run_marts

    run_marts(lookback_days=lookback_days)

    # ── Summary ──────────────────────────────────────────────────────────
    elapsed = time.perf_counter() - t0
    _print_summary()
    print(f"  ✅ Pipeline completed in {elapsed:.2f}s\n")


# ── CLI ──────────────────────────────────────────────────────────────────────

def cli() -> None:
    """Parse CLI args and run the pipeline."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Embedded Modern Data Stack — ELT Pipeline",
    )
    parser.add_argument(
        "--org",
        default=os.getenv("GITHUB_ORG", "apache"),
        help="GitHub organisation to fetch repos from (default: $GITHUB_ORG or 'apache')",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Number of days for the incremental lookback window (default: 7)",
    )
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip the ingestion step (useful when re-running transforms only)",
    )

    args = parser.parse_args()
    main(org=args.org, lookback_days=args.lookback_days, skip_ingest=args.skip_ingest)


if __name__ == "__main__":
    cli()

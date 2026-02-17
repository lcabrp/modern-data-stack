"""
transform/marts.py — Marts Layer (Incremental Aggregation)
============================================================
Reads the staged Parquet file, applies a **lookback window** to only
process recently-updated data, and produces two aggregated mart tables
using Polars:

* ``repos_per_language`` — repo count and average stars per language.
* ``daily_activity``     — daily push count with a 7-day rolling average.

**Zero-Copy path**: PyArrow reads Parquet → ``pl.from_arrow()`` (no copy).
**Incremental logic**: Only rows with ``updated_at >= now - lookback_days``
are processed, following the *Lookback* pattern.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

# ── Constants ────────────────────────────────────────────────────────────────

DATA_STAGING_DIR = Path("data/staging")
DATA_MARTS_DIR = Path("data/marts")
STAGING_INPUT = DATA_STAGING_DIR / "repos.parquet"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _read_staged_arrow() -> pl.DataFrame:
    """Read the staging Parquet via PyArrow and hand off to Polars (zero-copy)."""
    arrow_table = pq.read_table(STAGING_INPUT)
    return pl.from_arrow(arrow_table)  # zero-copy conversion


def _apply_lookback(df: pl.DataFrame, lookback_days: int) -> pl.DataFrame:
    """Filter the DataFrame to only contain rows updated within the lookback window.

    Handles both timezone-aware and timezone-naive ``updated_at`` columns.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    updated_col = df.schema["updated_at"]
    if updated_col.time_zone is None:
        # Column is naive — compare against naive cutoff
        cutoff = cutoff.replace(tzinfo=None)

    return df.filter(pl.col("updated_at") >= cutoff)


# ── Mart: repos_per_language ─────────────────────────────────────────────────

def _build_repos_per_language(df: pl.DataFrame) -> pl.DataFrame:
    """Aggregate repo counts and average stars grouped by programming language.

    Returns a DataFrame sorted by repo count descending.
    """
    return (
        df.group_by("language")
        .agg(
            pl.col("repo_id").count().alias("repo_count"),
            pl.col("stars").mean().round(1).alias("avg_stars"),
            pl.col("forks").mean().round(1).alias("avg_forks"),
        )
        .sort("repo_count", descending=True)
    )


# ── Mart: daily_activity ─────────────────────────────────────────────────────

def _build_daily_activity(df: pl.DataFrame) -> pl.DataFrame:
    """Compute daily push counts with a 7-day rolling average.

    Groups by the *date* portion of ``pushed_at`` to count pushes per day,
    then applies a rolling mean over a 7-day window.
    """
    daily = (
        df.with_columns(pl.col("pushed_at").dt.date().alias("push_date"))
        .group_by("push_date")
        .agg(pl.col("repo_id").count().alias("push_count"))
        .sort("push_date")
    )

    # Rolling 7-day average of daily push counts
    daily = daily.with_columns(
        pl.col("push_count")
        .rolling_mean(window_size=7, min_periods=1)
        .round(2)
        .alias("rolling_7d_avg")
    )

    return daily


# ── Runner ───────────────────────────────────────────────────────────────────

def run_marts(lookback_days: int = 7) -> None:
    """Run the marts layer.

    1. Read staged data via Arrow (zero-copy into Polars).
    2. Apply the lookback filter for incremental processing.
    3. Build mart aggregations.
    4. Write results to ``data/marts/``.
    """
    DATA_MARTS_DIR.mkdir(parents=True, exist_ok=True)

    if not STAGING_INPUT.exists():
        print("  ⚠  No staging file found — skipping marts.")
        return

    # Zero-copy: Arrow → Polars
    df = _read_staged_arrow()
    print(f"  → Loaded {len(df):,} staged rows")

    # Incremental: apply lookback window
    df_recent = _apply_lookback(df, lookback_days)
    print(f"  → {len(df_recent):,} rows within {lookback_days}-day lookback window")

    if df_recent.is_empty():
        print("  ⚠  No recent data after lookback filter — skipping marts.")
        return

    # ── Build & write marts ──────────────────────────────────────────────

    # 1. Repos per language
    repos_lang = _build_repos_per_language(df_recent)
    repos_lang_path = DATA_MARTS_DIR / "repos_per_language.parquet"
    repos_lang.write_parquet(repos_lang_path)
    print(f"  ✓ repos_per_language  → {repos_lang_path}  ({len(repos_lang):,} rows)")

    # 2. Daily activity
    daily = _build_daily_activity(df_recent)
    daily_path = DATA_MARTS_DIR / "daily_activity.parquet"
    daily.write_parquet(daily_path)
    print(f"  ✓ daily_activity      → {daily_path}  ({len(daily):,} rows)")


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_marts()

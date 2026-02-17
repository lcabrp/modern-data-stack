"""
transform/staging.py — Staging Layer
======================================
Reads raw Parquet files produced by ingest.py, applies SQL-based cleaning
and type-casting using DuckDB, and writes the result to
``data/staging/repos.parquet``.

This mirrors the "staging" models in a dbt project: renaming, casting,
filtering nulls, and selecting the columns the downstream marts need.

**Zero-Copy path**: DuckDB → Arrow Table → Parquet (via PyArrow).
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq

# ── Constants ────────────────────────────────────────────────────────────────

DATA_RAW_DIR = Path("data/raw")
DATA_STAGING_DIR = Path("data/staging")
STAGING_OUTPUT = DATA_STAGING_DIR / "repos.parquet"

# ── SQL ──────────────────────────────────────────────────────────────────────

STAGING_SQL_TEMPLATE = """
SELECT
    CAST(id        AS BIGINT)       AS repo_id,
    CAST(name      AS VARCHAR)      AS repo_name,
    CAST(full_name AS VARCHAR)      AS repo_full_name,
    COALESCE(CAST(description AS VARCHAR), '') AS description,

    -- Metrics
    CAST(stargazers_count AS INTEGER) AS stars,
    CAST(forks_count      AS INTEGER) AS forks,

    -- Categorisation
    COALESCE(CAST(language AS VARCHAR), 'Unknown') AS language,

    -- Timestamps → proper TIMESTAMP type
    CAST(created_at AS TIMESTAMP)   AS created_at,
    CAST(updated_at AS TIMESTAMP)   AS updated_at,
    CAST(pushed_at  AS TIMESTAMP)   AS pushed_at

FROM read_parquet('{raw_glob}', union_by_name = true)
WHERE name IS NOT NULL
ORDER BY updated_at DESC
"""


# ── Runner ───────────────────────────────────────────────────────────────────

def run_staging() -> None:
    """Run the staging transformation.

    1. Glob all raw Parquet files.
    2. Execute the staging SQL in an in-memory DuckDB instance.
    3. Fetch the result as an Apache Arrow table (zero-copy).
    4. Write the Arrow table to ``data/staging/repos.parquet``.
    """
    DATA_STAGING_DIR.mkdir(parents=True, exist_ok=True)

    raw_glob = str(DATA_RAW_DIR / "**" / "*.parquet").replace("\\", "/")
    raw_files = list(DATA_RAW_DIR.rglob("*.parquet"))

    if not raw_files:
        print("  ⚠  No raw Parquet files found — skipping staging.")
        return

    print(f"  → Reading {len(raw_files)} raw file(s) …")

    sql = STAGING_SQL_TEMPLATE.format(raw_glob=raw_glob)
    con = duckdb.connect(database=":memory:")
    arrow_table = con.execute(sql).fetch_arrow_table()
    con.close()

    pq.write_table(arrow_table, STAGING_OUTPUT)

    print(f"  ✓ Staged {arrow_table.num_rows:,} rows → {STAGING_OUTPUT}")


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    run_staging()

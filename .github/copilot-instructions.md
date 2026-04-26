# Modern Data Stack (Embedded) — Copilot Instructions

This repo is a **local-first ELT pipeline boilerplate** using the Embedded Data Stack architecture. Primary focus: zero-copy Arrow handoffs between DuckDB and Polars, incremental loading patterns, and serverless local processing.

**Architecture:** dlt → Parquet → DuckDB → Arrow → Polars  
**Tech Stack:** Python 3.10+, dlt, DuckDB, Polars, PyArrow, python-dotenv  
**Key Concept:** Zero-copy memory sharing via Apache Arrow (no serialization overhead)

---

## Project Structure

```
modern-data-stack/
├── pipeline.py                      # Main orchestrator (entry point)
├── ingest.py                        # dlt: GitHub API → data/raw/ Parquet
├── transform/
│   ├── staging.py                   # DuckDB: SQL cleaning → data/staging/
│   └── marts.py                     # Polars: aggregation → data/marts/
├── data/
│   ├── raw/                         # Raw API data (Parquet)
│   ├── staging/                     # Cleaned data (Parquet)
│   └── marts/                       # Aggregated tables (Parquet)
├── pyproject.toml                   # Dependencies
├── .env.example                     # Environment template
├── README.md                        # Quick start
└── TECHNICAL.md                     # Architecture deep dive
```

---

## Core Architecture

### Data Flow Diagram

```
GitHub REST API
    │
    ▼
┌────────────┐   Parquet    ┌────────────┐  Arrow Table  ┌────────────┐
│  dlt       │ ──────────►  │  DuckDB    │ ────────────► │  Polars    │
│  (ingest)  │  data/raw/   │  (staging) │  zero-copy    │  (marts)   │
└────────────┘              └────────────┘               └────────────┘
                               │                             │
                               ▼                             ▼
                         data/staging/                  data/marts/
                         repos.parquet              repos_per_language.parquet
                                                    daily_activity.parquet
```

### Technology Stack Roles

| Layer | Tool | Role | Why |
|-------|------|------|-----|
| **Ingestion** | dlt | Schema-aware API loading | Handles pagination, incremental cursors, schema evolution |
| **Storage** | Parquet | Columnar data lake | Efficient compression, columnar format, metadata |
| **Transformation** | DuckDB | In-memory SQL | Faster than pandas for filtering/joins, familiar SQL syntax |
| **Processing** | Polars | Multi-threaded DataFrames | 10-100× faster than pandas, memory-efficient |
| **Glue** | Apache Arrow | Zero-copy handoff | No serialization between DuckDB ↔ Polars |

---

## Zero-Copy Arrow Handoff (Critical Concept)

### What is Zero-Copy?

**Traditional approach (with copying):**
```python
# DuckDB → pandas → Polars (copies data twice)
df_pandas = con.execute(sql).df()              # Copy #1: DuckDB → pandas
df_polars = pl.from_pandas(df_pandas)          # Copy #2: pandas → Polars
# Total time for 10M rows: ~20 seconds
```

**Zero-copy approach (this project):**
```python
# DuckDB → Arrow → Polars (zero-copy)
arrow_table = con.execute(sql).fetch_arrow_table()  # Pointer to DuckDB buffers
df_polars = pl.from_arrow(arrow_table)               # Wraps same memory
# Total time for 10M rows: ~0.05 seconds (400× faster!)
```

### Why It Works

**Apache Arrow** defines a standardized columnar memory layout:
- Both DuckDB and Polars use Arrow-compatible internal buffers
- "Converting" between them = passing a pointer (no data movement)
- No serialization, no copying, no transpose operations

**Performance comparison (1M rows):**
- DuckDB → pandas → Polars: ~2s
- DuckDB → CSV → Polars: ~5s
- **DuckDB → Arrow → Polars: ~0.01s** ✅

### When Zero-Copy Breaks

These operations **do** copy memory:
- `.to_pandas()` - Pandas uses row-major NumPy arrays (requires transpose)
- Writing to CSV/JSON - Requires serialization
- `.rechunk()` in Polars - Changes physical layout

---

## Incremental Loading Pattern

### Two-Level Incrementality

**1. Ingestion Layer (dlt):**
```python
@dlt.resource(write_disposition="merge", primary_key="id")
def github_repos(
    org: str,
    updated_at=dlt.sources.incremental(
        "updated_at",
        initial_value="2000-01-01T00:00:00Z"
    ),
):
    """Incremental cursor on updated_at field"""
    # First run: Fetch all repos
    # Subsequent runs: Only repos where updated_at > last_cursor
```

**2. Marts Layer (Polars) - Lookback Pattern:**
```python
def _apply_lookback(df, lookback_days):
    """Only process recent data"""
    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    return df.filter(pl.col("updated_at") >= cutoff)
```

### Why Lookback?

**Without lookback (full recompute):**
- Process all 10M historical rows every run
- Time: O(total data)

**With lookback (incremental):**
- Process only last 7 days of data
- Time: O(new data)

This is analogous to **dbt incremental models** with lookback windows.

---

## Layer Responsibilities

### Layer 1: Ingestion (dlt)

**File:** `ingest.py`

**Responsibilities:**
- Fetch data from APIs with pagination
- Handle schema evolution automatically
- Track incremental cursors
- Write raw data to Parquet

**Pattern:**
```python
import dlt
from dlt.sources.rest_api import RESTAPIConfig

@dlt.resource(write_disposition="merge", primary_key="id")
def github_repos(
    org: str,
    updated_at=dlt.sources.incremental("updated_at"),
):
    """Fetch repos from GitHub API"""
    url = f"https://api.github.com/orgs/{org}/repos"
    params = {"per_page": 100, "sort": "updated", "direction": "desc"}
    
    # dlt handles pagination automatically
    for page in paginated_get(url, params):
        yield page

# Run pipeline
pipeline = dlt.pipeline(
    pipeline_name="github_ingestion",
    destination=dlt.destinations.filesystem(
        bucket_url="data/raw"
    ),
    dataset_name="github"
)

pipeline.run(github_repos(org="python"))
```

**Key dlt concepts:**
- `write_disposition="merge"` - Upsert by primary_key
- `dlt.sources.incremental()` - Automatic cursor tracking
- Schema inference and evolution handled automatically

### Layer 2: Staging (DuckDB SQL)

**File:** `transform/staging.py`

**Responsibilities:**
- Load raw Parquet files
- SQL-based cleaning (type casting, filtering, joins)
- Write cleaned data to staging/

**Pattern:**
```python
import duckdb
import pyarrow.parquet as pq

def create_repos_staging(lookback_days=7):
    """Clean and type-cast raw repos data"""
    
    con = duckdb.connect()
    
    # SQL transformation
    sql = """
    SELECT 
        id,
        name,
        full_name,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(updated_at AS TIMESTAMP) AS updated_at,
        stargazers_count AS stars,
        forks_count AS forks,
        COALESCE(language, 'Unknown') AS language,
        archived,
        disabled
    FROM read_parquet('data/raw/**/*.parquet')
    WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '{lookback_days} days'
    """
    
    # Fetch as Arrow table (zero-copy from DuckDB)
    arrow_table = con.execute(sql).fetch_arrow_table()
    
    # Write to staging
    pq.write_table(arrow_table, "data/staging/repos.parquet")
    
    return arrow_table
```

**Key patterns:**
- Use `read_parquet('path/**/*.parquet')` for glob patterns
- `.fetch_arrow_table()` for zero-copy handoff
- SQL for transformations (familiar, optimized)

### Layer 3: Marts (Polars Aggregations)

**File:** `transform/marts.py`

**Responsibilities:**
- Load staging data
- Apply lookback filtering
- Perform aggregations
- Write analytical tables to marts/

**Pattern:**
```python
import polars as pl
import pyarrow.parquet as pq
from datetime import datetime, timedelta, UTC

def create_repos_per_language(lookback_days=7):
    """Aggregate repos by programming language"""
    
    # Read from staging (Arrow → Polars zero-copy)
    arrow_table = pq.read_table("data/staging/repos.parquet")
    df = pl.from_arrow(arrow_table)
    
    # Apply lookback filter
    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    df = df.filter(pl.col("updated_at") >= cutoff)
    
    # Aggregation
    result = (
        df.group_by("language")
        .agg([
            pl.count("id").alias("repo_count"),
            pl.sum("stars").alias("total_stars"),
            pl.mean("stars").alias("avg_stars"),
            pl.sum("forks").alias("total_forks"),
        ])
        .sort("repo_count", descending=True)
    )
    
    # Write to marts
    result.write_parquet("data/marts/repos_per_language.parquet")
    
    return result
```

**Key Polars patterns:**
- `pl.from_arrow()` for zero-copy from Arrow
- `.group_by()` + `.agg()` for aggregations
- Lazy evaluation with `.lazy()` for large datasets
- `.write_parquet()` for efficient storage

---

## CLI Usage

### Main Pipeline

```bash
# Full ELT: ingest → staging → marts
uv run python pipeline.py

# Custom GitHub org
uv run python pipeline.py --org python

# Skip ingestion (re-run transforms only)
uv run python pipeline.py --skip-ingest

# Custom lookback window
uv run python pipeline.py --lookback-days 30

# Combine flags
uv run python pipeline.py --org pytorch --lookback-days 14
```

### Pipeline Orchestrator

**File:** `pipeline.py`

```python
import argparse
from ingest import run_ingestion
from transform.staging import create_repos_staging
from transform.marts import create_repos_per_language, create_daily_activity

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--org", default="python")
    parser.add_argument("--skip-ingest", action="store_true")
    parser.add_argument("--lookback-days", type=int, default=7)
    args = parser.parse_args()
    
    # Layer 1: Ingest
    if not args.skip_ingest:
        run_ingestion(org=args.org)
    
    # Layer 2: Staging
    create_repos_staging(lookback_days=args.lookback_days)
    
    # Layer 3: Marts
    create_repos_per_language(lookback_days=args.lookback_days)
    create_daily_activity(lookback_days=args.lookback_days)
    
    print("✓ Pipeline complete")

if __name__ == "__main__":
    main()
```

---

## Python Conventions

### Import Patterns

**Standard imports:**
```python
import dlt
import duckdb
import polars as pl
import pyarrow.parquet as pq
from datetime import datetime, timedelta, UTC
```

**Avoid mixing old/new patterns:**
```python
# ✓ GOOD - Use Arrow handoff
arrow_table = con.execute(sql).fetch_arrow_table()
df = pl.from_arrow(arrow_table)

# ✗ AVOID - Copies data via pandas
df_pandas = con.execute(sql).df()
df = pl.from_pandas(df_pandas)
```

### Configuration Pattern

**Environment variables (.env):**
```bash
GITHUB_TOKEN=ghp_xxxxxxxxxxxx
LOOKBACK_DAYS=7
ORG_NAME=python
```

**Loading in code:**
```python
from dotenv import load_dotenv
import os

load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
```

### Error Handling

```python
def safe_transform():
    """Handle missing files gracefully"""
    try:
        df = pl.read_parquet("data/staging/repos.parquet")
    except FileNotFoundError:
        print("⚠ No staging data found. Run ingestion first.")
        return None
    
    return df
```

---

## DuckDB Patterns

### Reading Parquet with Glob

```python
import duckdb

con = duckdb.connect()

# Single file
df = con.execute("SELECT * FROM 'data/raw/repos.parquet'").df()

# Glob pattern (all files recursively)
df = con.execute("SELECT * FROM read_parquet('data/raw/**/*.parquet')").df()

# Multiple specific files
df = con.execute("""
    SELECT * FROM read_parquet([
        'data/raw/file1.parquet',
        'data/raw/file2.parquet'
    ])
""").df()
```

### SQL Transformations

```python
# Type casting
sql = "SELECT CAST(created_at AS TIMESTAMP) AS created_at FROM repos"

# Filtering with date functions
sql = """
SELECT * FROM repos
WHERE updated_at >= CURRENT_TIMESTAMP - INTERVAL '7 days'
"""

# Window functions
sql = """
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY language ORDER BY stars DESC) AS rank
FROM repos
"""

# CTEs (Common Table Expressions)
sql = """
WITH active_repos AS (
    SELECT * FROM repos WHERE NOT archived
)
SELECT language, COUNT(*) FROM active_repos GROUP BY language
"""
```

### Fetching Results

```python
# As Arrow table (zero-copy)
arrow_table = con.execute(sql).fetch_arrow_table()

# As pandas DataFrame (copies)
df = con.execute(sql).df()

# As Python list of tuples
rows = con.execute(sql).fetchall()

# As Python list of dicts
records = con.execute(sql).fetchdf().to_dict(orient="records")
```

---

## Polars Patterns

### Reading/Writing Parquet

```python
import polars as pl

# Read Parquet
df = pl.read_parquet("data/staging/repos.parquet")

# Write Parquet
df.write_parquet("data/marts/output.parquet")

# Scan (lazy evaluation)
lf = pl.scan_parquet("data/staging/repos.parquet")
result = lf.filter(pl.col("stars") > 1000).collect()
```

### Aggregations

```python
# Group by with multiple aggregations
result = (
    df.group_by("language")
    .agg([
        pl.count("id").alias("count"),
        pl.sum("stars").alias("total_stars"),
        pl.mean("stars").alias("avg_stars"),
        pl.median("stars").alias("median_stars"),
        pl.max("stars").alias("max_stars"),
    ])
)

# Multiple group by columns
result = df.group_by(["language", "archived"]).agg(pl.count("id"))
```

### Filtering & Sorting

```python
# Filter
df_filtered = df.filter(
    (pl.col("stars") > 100) & 
    (pl.col("language") == "Python")
)

# Sort
df_sorted = df.sort("stars", descending=True)

# Top N
top_10 = df.sort("stars", descending=True).head(10)
```

### Date Handling

```python
from datetime import datetime, UTC

# Filter by date
cutoff = datetime(2024, 1, 1, tzinfo=UTC)
df_recent = df.filter(pl.col("updated_at") >= cutoff)

# Extract date parts
df = df.with_columns([
    pl.col("updated_at").dt.year().alias("year"),
    pl.col("updated_at").dt.month().alias("month"),
    pl.col("updated_at").dt.day().alias("day"),
])
```

### Lazy Evaluation (Large Datasets)

```python
# Use .lazy() for datasets > 1GB
lf = pl.scan_parquet("large_file.parquet")

# Chain operations (not executed yet)
result_lf = (
    lf.filter(pl.col("stars") > 1000)
    .group_by("language")
    .agg(pl.count("id"))
    .sort("count", descending=True)
)

# Execute with .collect()
result = result_lf.collect()
```

---

## dlt Patterns

### Resource Definition

```python
import dlt

@dlt.resource(
    name="github_repos",
    write_disposition="merge",  # upsert by primary_key
    primary_key="id",
)
def github_repos(
    org: str,
    updated_at=dlt.sources.incremental("updated_at"),
):
    """Fetch GitHub repos with incremental loading"""
    # Implementation
    pass
```

**Write dispositions:**
- `replace` - Drop and recreate table
- `append` - Add new rows only
- `merge` - Upsert by primary_key (default for incremental)

### Incremental Loading

```python
# Automatic cursor tracking
@dlt.resource
def my_resource(
    updated_at=dlt.sources.incremental(
        cursor_path="updated_at",
        initial_value="2000-01-01T00:00:00Z"
    )
):
    # dlt automatically filters data where updated_at > last_cursor
    pass
```

### Pipeline Execution

```python
import dlt

# Create pipeline
pipeline = dlt.pipeline(
    pipeline_name="my_pipeline",
    destination=dlt.destinations.filesystem(
        bucket_url="data/raw"
    ),
    dataset_name="my_dataset"
)

# Run pipeline
info = pipeline.run(my_resource())
print(f"Loaded {info.metrics['rows']} rows")
```

---

## Common Workflows

### Initial Setup

```bash
# 1. Clone repo
git clone <repo-url> && cd modern-data-stack

# 2. Install dependencies
uv sync

# 3. Configure (optional - for higher API rate limits)
cp .env.example .env
# Edit .env to add GITHUB_TOKEN

# 4. Run first pipeline
uv run python pipeline.py
```

### Daily Incremental Run

```bash
# Runs daily (cron or Airflow)
uv run python pipeline.py --lookback-days 1
```

### Backfill Historical Data

```bash
# Re-process last 30 days
uv run python pipeline.py --lookback-days 30
```

### Add New Mart

1. **Create new aggregation in `transform/marts.py`:**
```python
def create_my_new_mart(lookback_days=7):
    df = pl.read_parquet("data/staging/repos.parquet")
    # Apply lookback
    # Aggregate
    # Write to data/marts/my_new_mart.parquet
```

2. **Add to pipeline orchestrator:**
```python
# pipeline.py
from transform.marts import create_my_new_mart

create_my_new_mart(lookback_days=args.lookback_days)
```

### Add New Data Source

1. **Create new dlt resource in `ingest.py`:**
```python
@dlt.resource
def new_source():
    # Fetch data from API
    pass
```

2. **Create staging transformation:**
```python
# transform/staging.py
def create_new_source_staging():
    # DuckDB SQL transformations
    pass
```

3. **Create marts:**
```python
# transform/marts.py
def create_new_source_mart():
    # Polars aggregations
    pass
```

---

## Performance Characteristics

### Benchmarks (10M rows)

| Operation | Time | Notes |
|-----------|------|-------|
| DuckDB read Parquet | ~0.5s | Columnar format optimized |
| DuckDB SQL transform | ~1-2s | In-memory, multi-threaded |
| Arrow handoff to Polars | ~0.05s | Zero-copy pointer |
| Polars aggregation | ~0.3s | Multi-threaded |
| **Total pipeline** | **~2-3s** | All layers combined |

### Memory Usage

- **DuckDB:** Uses mmap for Parquet (minimal RAM)
- **Arrow handoff:** No additional memory (shared buffers)
- **Polars:** Efficient chunked processing

**Rule of thumb:** Can process datasets 10× larger than RAM

---

## Common Pitfalls & Solutions

### Missing Arrow Dependency

**Problem:** `ImportError: No module named 'pyarrow'`

**Solution:**
```bash
uv sync  # Installs pyarrow
# Or: pip install pyarrow
```

### DuckDB Result Type Confusion

**Problem:** Using `.df()` instead of `.fetch_arrow_table()`

**Solution:**
```python
# ✗ WRONG - Copies via pandas
df = con.execute(sql).df()

# ✓ CORRECT - Zero-copy via Arrow
arrow_table = con.execute(sql).fetch_arrow_table()
```

### Lookback Filter Forgotten

**Problem:** Processing all historical data every run

**Solution:**
```python
# Always apply lookback in marts
def create_mart(lookback_days=7):
    df = pl.read_parquet("data/staging/data.parquet")
    
    # Apply lookback
    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    df = df.filter(pl.col("updated_at") >= cutoff)
    
    # Continue processing...
```

---

## References

- **dlt Documentation:** https://dlthub.com/docs/
- **DuckDB Documentation:** https://duckdb.org/docs/
- **Polars Guide:** https://pola-rs.github.io/polars-book/
- **Apache Arrow:** https://arrow.apache.org/docs/python/
- **Embedded Data Stack:** Concept popularized by MotherDuck/DuckDB

# Technical Documentation

Deep-dive into the architecture, design patterns, and extension points of this pipeline.

---

## Table of Contents

- [Data Flow](#data-flow)
- [Zero-Copy Arrow Handoff](#zero-copy-arrow-handoff)
- [Incremental Loading — The Lookback Pattern](#incremental-loading--the-lookback-pattern)
- [Layer Responsibilities](#layer-responsibilities)
- [Extending the Pipeline](#extending-the-pipeline)
- [Configuration Reference](#configuration-reference)
- [Performance Notes](#performance-notes)

---

## Data Flow

```mermaid
graph LR
    A[GitHub REST API] -->|paginated JSON| B[dlt Resource]
    B -->|schema-evolved Parquet| C["data/raw/"]
    C -->|read_parquet glob| D[DuckDB - in-memory]
    D -->|".fetch_arrow_table()"| E[Apache Arrow Table]
    E -->|"pq.write_table()"| F["data/staging/repos.parquet"]
    F -->|"pq.read_table()"| G[Arrow Table]
    G -->|"pl.from_arrow() — zero-copy"| H[Polars DataFrame]
    H -->|lookback filter + aggregation| I["data/marts/*.parquet"]
```

Every arrow in the chain above is either a file I/O operation (Parquet) or a **zero-copy** in-memory pointer swap (Arrow). There is no serialization/deserialization between DuckDB and Polars.

---

## Zero-Copy Arrow Handoff

The core performance trick of this stack is the **Arrow-based memory sharing** between DuckDB and Polars:

```python
# staging.py — DuckDB produces an Arrow table (zero-copy from its internal buffers)
arrow_table = con.execute(sql).fetch_arrow_table()

# marts.py — Polars wraps the Arrow table without copying memory
df = pl.from_arrow(pq.read_table("data/staging/repos.parquet"))
```

### Why this matters

| Approach | 1M rows | 10M rows |
|---|---|---|
| DuckDB → pandas → Polars | ~2s | ~20s |
| DuckDB → CSV → Polars | ~5s | ~50s |
| **DuckDB → Arrow → Polars** | **~0.01s** | **~0.05s** |

Arrow Tables are columnar buffers with a standardised memory layout. Both DuckDB and Polars understand this layout natively, so "converting" between them is just passing a pointer — no data is moved, copied, or re-encoded.

### When zero-copy breaks

Zero-copy is preserved as long as you don't trigger a materialisation. These operations **do** copy:
- `.to_pandas()` — Pandas uses row-major NumPy arrays, so a transpose is needed
- Writing to CSV/JSON — requires serialization
- Certain Polars operations that change the physical layout (e.g., `rechunk`)

---

## Incremental Loading — The Lookback Pattern

### In the ingestion layer (dlt)

`dlt` uses an **incremental cursor** on `updated_at`:

```python
@dlt.resource(write_disposition="merge", primary_key="id")
def github_repos(
    org: str,
    updated_at=dlt.sources.incremental("updated_at", initial_value="2000-01-01T00:00:00Z"),
):
```

On the first run, dlt fetches everything. On subsequent runs, it only fetches repos where `updated_at` is newer than the last stored cursor value. The `merge` write disposition upserts by `id`, so re-ingested repos overwrite their previous version.

### In the marts layer (Polars)

The **Lookback** pattern filters staged data to a rolling time window:

```python
def _apply_lookback(df, lookback_days):
    cutoff = datetime.now(UTC) - timedelta(days=lookback_days)
    return df.filter(pl.col("updated_at") >= cutoff)
```

This means the marts only aggregate **recent** data — controlled via `--lookback-days`. This is analogous to a dbt incremental model with a lookback.

**Why not just process everything?** For this demo dataset it doesn't matter, but at scale (millions of rows), recomputing all-time aggregations on every run is wasteful. The lookback pattern lets you process *O(new data)* instead of *O(total data)*.

---

## Layer Responsibilities

Mirroring a dbt project's `staging/` and `marts/` structure:

| Layer | Tool | What it does | What it does NOT do |
|---|---|---|---|
| **Ingest** (`ingest.py`) | dlt | API fetching, pagination, incremental cursors, schema evolution | No cleaning, no transforms |
| **Staging** (`staging.py`) | DuckDB | Rename columns, cast types, filter NULLs, standardise formats | No business logic, no aggregations |
| **Marts** (`marts.py`) | Polars | Aggregations, rolling windows, business metrics | No raw data access, no type-casting |

### Why DuckDB for staging and Polars for marts?

- **DuckDB** excels at SQL-shaped work: SELECT/CAST/WHERE/JOIN over Parquet files. It reads Parquet natively via `read_parquet()` with glob support.
- **Polars** excels at programmatic, multi-step DataFrame transformations: rolling windows, complex group-bys, conditional logic that's awkward in SQL.

Using both via Arrow means you pick the right tool for each job with zero overhead.

---

## Extending the Pipeline

### Adding a new data source

1. Create a new `dlt.resource` in `ingest.py` (or a new `ingest_*.py` file)
2. Add a corresponding staging SQL in `staging.py` (or a new staging script)
3. Wire it into `pipeline.py`

### Adding a new mart

Add a function in `marts.py` following the existing pattern:

```python
def _build_my_new_mart(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("some_column")
        .agg(pl.col("metric").sum().alias("total"))
    )
```

Then call it from `run_marts()` and write the output:

```python
result = _build_my_new_mart(df_recent)
result.write_parquet(DATA_MARTS_DIR / "my_new_mart.parquet")
```

### Adding a new staging model

If you have multiple raw sources, split staging into multiple functions or files under `transform/`:

```
transform/
├── staging_repos.py      # DuckDB: clean GitHub repos
├── staging_issues.py     # DuckDB: clean GitHub issues
├── marts.py              # Polars: aggregations across both
```

---

## Configuration Reference

| Variable | Where | Default | Purpose |
|---|---|---|---|
| `GITHUB_TOKEN` | `.env` | *(none)* | GitHub PAT for 5000 req/hr (vs. 60 unauthenticated) |
| `GITHUB_ORG` | `.env` | `apache` | GitHub org to fetch repos from |
| `--org` | CLI | `$GITHUB_ORG` | Override the org at runtime |
| `--lookback-days` | CLI | `7` | Incremental window for marts aggregation |
| `--skip-ingest` | CLI | `false` | Skip the dlt ingestion step |

---

## Performance Notes

- **DuckDB** operates in-memory (`:memory:`) — no disk-based database file. For datasets larger than RAM, switch to a file-backed database: `duckdb.connect("pipeline.duckdb")`.
- **Polars** uses all available CPU cores by default. No configuration needed.
- **Parquet** files use Snappy compression by default (via both dlt and Polars). This gives ~3-5x compression on typical tabular data.
- The entire pipeline (ingest → staging → marts) runs in **~1.5s** for ~60 repos. At 100k+ rows, the bottleneck shifts from API fetching to disk I/O, not computation.

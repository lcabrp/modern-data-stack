# Modern Data Stack

**The "Boring" Way to Build High-Performance Data Pipelines.**

A serverless, local-first ELT pipeline boilerplate using the **Embedded Data Stack** architecture. Replace complex cloud infrastructure with a lightweight stack that runs entirely on your laptop.

---

## 🛠️ The Stack

| Layer | Tool | Role |
|---|---|---|
| **Ingestion** | [dlt](https://dlthub.com/) | Schema-aware API data landing with incremental loading |
| **Storage** | Parquet (local) | Columnar data lake on the filesystem |
| **Transformation** | [DuckDB](https://duckdb.org/) | In-process SQL filtering, joins, and type-casting |
| **Processing** | [Polars](https://pola.rs/) | Multi-threaded DataFrame aggregations |
| **Glue** | [Apache Arrow](https://arrow.apache.org/) | Zero-copy memory sharing between DuckDB ↔ Polars |

---

## 📁 Project Structure

```
modern-data-stack/
├── pipeline.py              # Main orchestrator (entry point)
├── ingest.py                # dlt: GitHub API → data/raw/ Parquet
├── transform/
│   ├── staging.py           # DuckDB: SQL cleaning → data/staging/
│   └── marts.py             # Polars: incremental aggregation → data/marts/
├── data/
│   ├── raw/                 # Raw API data (Parquet)
│   ├── staging/             # Cleaned data (Parquet)
│   └── marts/               # Aggregated tables (Parquet)
├── pyproject.toml           # Dependencies
├── .env.example             # Environment variable template
└── README.md
```

---

## 🚀 Quickstart

### 1. Prerequisites

- Python ≥ 3.10
- [uv](https://docs.astral.sh/uv/) (recommended package manager)

### 2. Install

```bash
# Clone the repo
git clone <your-repo-url> && cd modern-data-stack

# Create a virtual environment & install deps
uv sync
```

### 3. Configure (optional)

```bash
cp .env.example .env
# Edit .env to set GITHUB_TOKEN for higher API rate limits
```

### 4. Run the pipeline

```bash
# Full ELT: ingest → staging → marts
uv run python pipeline.py

# Fetch a specific GitHub org
uv run python pipeline.py --org python

# Skip ingestion, re-run transforms only
uv run python pipeline.py --skip-ingest

# Wider lookback window for incremental processing
uv run python pipeline.py --lookback-days 30
```

---

## 🏗️ Architecture — Zero-Copy Arrow Flow

```
GitHub API
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

**Key principle**: DuckDB's `.arrow()` method returns an Apache Arrow table. Polars' `pl.from_arrow()` wraps it *without copying memory*. This means the DuckDB → Polars handoff is essentially free regardless of data size.

---

## 📖 What You'll Learn

1. How to land raw API data into a local data lake using **dlt** with incremental loading.
2. How to use **DuckDB** to clean and type-cast data using standard SQL.
3. How to perform **zero-copy** handoffs from DuckDB to **Polars** via Apache Arrow.
4. How to implement the **Lookback** pattern for incremental processing so your pipeline only touches new data.
5. How to structure a Python project like **dbt** (Staging vs. Marts) without needing dbt itself.

---

## 🔬 Technical Deep-Dive

See [TECHNICAL.md](TECHNICAL.md) for detailed documentation on:
- The zero-copy Arrow handoff mechanism
- The incremental lookback pattern
- Layer responsibilities (staging vs. marts)
- How to extend the pipeline with new sources and marts
- Performance characteristics

---

## License

[MIT](LICENSE)

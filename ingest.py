"""
ingest.py — Data Ingestion Layer
=================================
Uses dlt (Data Load Tool) to fetch repositories from the GitHub REST API
and land them as Parquet files in data/raw/.

The source uses dlt's built-in incremental loading so that re-runs only
fetch repos updated since the last successful run.
"""

from __future__ import annotations

import os
from pathlib import Path

import dlt
import requests

# ── Constants ────────────────────────────────────────────────────────────────

DATA_RAW_DIR = Path("data/raw")
GITHUB_API = "https://api.github.com"
PAGE_SIZE = 100  # max per GitHub API page


# ── dlt Resource ─────────────────────────────────────────────────────────────

@dlt.resource(
    name="github_repos",
    write_disposition="merge",
    primary_key="id",
)
def github_repos(
    org: str,
    updated_at: dlt.sources.incremental[str] = dlt.sources.incremental(
        cursor_path="updated_at",
        initial_value="2000-01-01T00:00:00Z",
    ),
) -> None:
    """Yield GitHub repos for *org*, page by page.

    Uses `dlt.sources.incremental` on `updated_at` so subsequent runs
    only process repos that changed since the last successful load.
    """
    headers: dict[str, str] = {"Accept": "application/vnd.github+json"}
    token = os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    page = 1
    while True:
        url = f"{GITHUB_API}/orgs/{org}/repos"
        resp = requests.get(
            url,
            headers=headers,
            params={
                "per_page": PAGE_SIZE,
                "page": page,
                "sort": "updated",
                "direction": "desc",
            },
            timeout=30,
        )
        resp.raise_for_status()
        repos = resp.json()

        if not repos:
            break

        yield from repos
        page += 1


# ── Pipeline runner ──────────────────────────────────────────────────────────

def run_pipeline(org: str = "apache") -> None:
    """Execute the ingestion pipeline.

    Fetches repos for the given GitHub *org* and writes Parquet files
    into ``data/raw/``.
    """
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)

    pipeline = dlt.pipeline(
        pipeline_name="github_repos_pipeline",
        destination=dlt.destinations.filesystem(
            bucket_url=str(DATA_RAW_DIR),
            layout="{table_name}/{load_id}.{file_id}.{ext}",
        ),
        dataset_name="github",
    )

    load_info = pipeline.run(github_repos(org=org), loader_file_format="parquet")
    print(load_info)


# ── CLI entry point ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    run_pipeline(org=os.getenv("GITHUB_ORG", "apache"))

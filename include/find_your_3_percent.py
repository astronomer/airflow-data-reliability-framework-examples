"""
Find your own 3%: rank DAGs by how many scheduled runs failed recently.

Uses the Airflow 3 REST API, so it works on Astro and anywhere else you can't
query the metadata database. Two calls per DAG, each asking only for a count,
so it stays fast on large fleets.

Make sure you set the following environment variables and install requests before running:

    export AIRFLOW_URL="https://<your-deployment-url>"   # no trailing /api/v2
    export AIRFLOW_TOKEN="<Astro API token or Airflow JWT>"
    python find_your_3_percent.py            # last 90 days, top 25
    python find_your_3_percent.py 30 50      # last 30 days, top 50

Sorts by failure count, not failure rate: a DAG that failed both of its 2 runs
matters less than one that failed 20% of 3,000.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

URL = os.environ["AIRFLOW_URL"].rstrip("/") + "/api/v2"
HEADERS = {"Authorization": f"Bearer {os.environ['AIRFLOW_TOKEN']}"}
DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 90
TOP = int(sys.argv[2]) if len(sys.argv) > 2 else 25
SINCE = (datetime.now(timezone.utc) - timedelta(days=DAYS)).isoformat()


def get(path: str, **params) -> dict:
    r = requests.get(f"{URL}{path}", headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def all_dag_ids() -> list[str]:
    ids, offset = [], 0
    while True:
        page = get("/dags", limit=100, offset=offset)
        ids += [d["dag_id"] for d in page["dags"]]
        offset += 100
        if offset >= page["total_entries"]:
            return ids


def run_count(dag_id: str, state: str | None = None) -> int:
    params = {"run_type": "scheduled", "run_after_gte": SINCE, "limit": 1}
    if state:
        params["state"] = state
    return get(f"/dags/{dag_id}/dagRuns", **params)["total_entries"]


rows = []
for dag_id in all_dag_ids():
    runs = run_count(dag_id)
    if runs:
        rows.append((dag_id, runs, run_count(dag_id, "failed")))

rows.sort(key=lambda r: r[2], reverse=True)
total_failures = sum(r[2] for r in rows) or 1

print(f"Scheduled runs since {SINCE[:10]} ({len(rows)} DAGs with runs)\n")
print(f"{'dag_id':<48}{'runs':>8}{'failed':>8}{'rate':>7}{'share':>8}")
for dag_id, runs, failed in rows[:TOP]:
    print(f"{dag_id:<48}{runs:>8}{failed:>8}{failed / runs:>7.0%}{failed / total_failures:>8.0%}")

top_share = sum(r[2] for r in rows[: max(1, round(len(rows) * 0.03))]) / total_failures
print(f"\nYour top 3% of DAGs account for {top_share:.0%} of scheduled-run failures.")

"""
Limit the number of cached classifications selected for retry.

    measure_retry_pool >> circuit_breaker >> classify_with_llm

The circuit breaker compares the retry pool with both a row limit and a ratio
limit before downstream model calls begin. This keeps automated recovery within
a defined processing boundary.

`classify_with_llm` is a placeholder for the classification implementation.
"""

import os
from datetime import datetime

from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import dag, task

CONN_ID = os.getenv("RELIABILITY_DEMO_CONN_ID", "warehouse")
TABLE_NAMESPACE = os.getenv("RELIABILITY_DEMO_TABLE_NAMESPACE", "").strip(".")
TABLE_PREFIX = os.getenv("RELIABILITY_DEMO_TABLE_PREFIX", "demo_")


def _table_name(base_name: str) -> str:
    """Build a table name using the optional database/schema namespace."""
    table_name = f"{TABLE_PREFIX}{base_name}"
    return f"{TABLE_NAMESPACE}.{table_name}" if TABLE_NAMESPACE else table_name


PERSONAS_CACHE_TABLE = _table_name("personas_cache")

MAX_RETRY_RATIO = 0.05
MIN_RETRY_ROWS = 1000  # Minimum retry volume before the ratio limit applies.


@dag(
    dag_id="layer5_circuit_breaker",
    schedule=None,  # production: the classifier's sub-hourly cadence
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reliability-demo", "layer-5"],
    doc_md=__doc__,
)
def layer5_circuit_breaker():
    measure_retry_pool = SQLExecuteQueryOperator(
        task_id="measure_retry_pool",
        conn_id=CONN_ID,
        sql=f"""
            SELECT SUM(CASE WHEN persona IS NULL THEN 1 ELSE 0 END) AS to_retry,
                   COUNT(*) AS cache_size
            FROM {PERSONAS_CACHE_TABLE}
        """,
    )

    @task(retries=0)
    def circuit_breaker(result) -> int:
        row = result[0]
        to_retry, cache_size = int(row[0] or 0), int(row[1] or 0)
        ratio = to_retry / cache_size if cache_size else 0.0
        print(f"Retry pool: {to_retry} of {cache_size} cached rows ({ratio:.1%})")

        if to_retry >= MIN_RETRY_ROWS and ratio > MAX_RETRY_RATIO:
            raise AirflowFailException(
                f"Circuit open: retry would re-select {to_retry} rows ({ratio:.1%} of "
                "the cache). Classification was skipped for review."
            )
        return to_retry

    @task
    def classify_with_llm(to_retry: int) -> None:
        # Replace this placeholder with a model call for the approved retry pool.
        print(f"Would send {to_retry} titles to the classifier.")

    classify_with_llm(circuit_breaker(measure_retry_pool.output))


layer5_circuit_breaker()

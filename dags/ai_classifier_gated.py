"""
Classify job titles with bounded model usage and validation gates.

    measure_retry_pool >> circuit_breaker >> fetch_batch >> classify >> stage >> audit >> publish

The circuit breaker limits the size of the retry pool, and each run processes
at most BATCH_SIZE titles. Structured output restricts classifications to the
supported taxonomy. The audit task validates the result before updating the
cache.

Set RELIABILITY_DEMO_REQUIRE_APPROVAL=1 to review model output in the Airflow UI
before staging it. The DAG uses the LLM connection configured by
RELIABILITY_DEMO_LLM_CONN_ID, which defaults to `llm`.

Run `reliability_demo_setup` before running this DAG.
"""

import os
from datetime import datetime, timedelta
from typing import Literal, Optional

from pydantic import BaseModel
from pydantic_ai.usage import UsageLimits

try:  # Airflow 3.3+
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:  # Airflow 3.0-3.2
    from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import BaseHook, dag, task

CONN_ID = os.getenv("RELIABILITY_DEMO_CONN_ID", "warehouse")
LLM_CONN_ID = os.getenv("RELIABILITY_DEMO_LLM_CONN_ID", "llm")
REQUIRE_APPROVAL = os.getenv("RELIABILITY_DEMO_REQUIRE_APPROVAL", "0") == "1"
TABLE_NAMESPACE = os.getenv("RELIABILITY_DEMO_TABLE_NAMESPACE", "").strip(".")
TABLE_PREFIX = os.getenv("RELIABILITY_DEMO_TABLE_PREFIX", "demo_")


def _table_name(base_name: str) -> str:
    """Build a table name using the optional database/schema namespace."""
    table_name = f"{TABLE_PREFIX}{base_name}"
    return f"{TABLE_NAMESPACE}.{table_name}" if TABLE_NAMESPACE else table_name


PERSONAS_CACHE_TABLE = _table_name("personas_cache")
STAGING_PERSONAS_TABLE = _table_name("personas_staging")
TAXONOMY_TABLE = _table_name("taxonomy")

BATCH_SIZE = 50
MAX_MODEL_REQUESTS = 5
MAX_RETRY_RATIO, MIN_RETRY_ROWS = 0.05, 1000
MAX_UNCLASSIFIED_RATIO, MIN_UNCLASSIFIED_ROWS = 0.10, 25

Persona = Literal[
    "Data Engineer",
    "Analytics Engineer",
    "Data Scientist",
    "ML Engineer",
    "Platform Engineer",
    "Executive",
    "Other Technical",
]


class Label(BaseModel):
    title: str
    persona: Optional[Persona]  # None means the model abstained


class Labels(BaseModel):
    labels: list[Label]


def _get(obj, key):
    return obj[key] if isinstance(obj, dict) else getattr(obj, key)


@dag(
    dag_id="ai_classifier_gated",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reliability-demo", "ai"],
    doc_md=__doc__,
)
def ai_classifier_gated():
    measure_retry_pool = SQLExecuteQueryOperator(
        task_id="measure_retry_pool",
        conn_id=CONN_ID,
        sql=f"""
            SELECT SUM(CASE WHEN persona IS NULL THEN 1 ELSE 0 END), COUNT(*)
            FROM {PERSONAS_CACHE_TABLE}
        """,
    )

    @task(retries=0)
    def circuit_breaker(result) -> int:
        to_retry, cache_size = (int(v or 0) for v in result[0])
        ratio = to_retry / cache_size if cache_size else 0.0
        print(f"Retry pool: {to_retry} of {cache_size} cached rows ({ratio:.1%})")
        if to_retry >= MIN_RETRY_ROWS and ratio > MAX_RETRY_RATIO:
            raise AirflowFailException(
                f"Circuit open: {to_retry} rows exceed the configured retry limits."
            )
        return to_retry

    fetch_batch = SQLExecuteQueryOperator(
        task_id="fetch_batch",
        conn_id=CONN_ID,
        sql=f"""SELECT title FROM {PERSONAS_CACHE_TABLE}
            WHERE persona IS NULL ORDER BY title LIMIT {BATCH_SIZE}""",
    )

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "Classify each job title into exactly one persona from the allowed list. "
            "If a title is not a real job title, set persona to null."
        ),
        output_type=Labels,
        serialize_output=True,  # Keep the XCom representation consistent.
        usage_limits=UsageLimits(request_limit=MAX_MODEL_REQUESTS),
        require_approval=REQUIRE_APPROVAL,
        approval_timeout=timedelta(hours=24),
        retries=0,
    )
    def classify(rows):
        titles = "\n".join(r[0] for r in rows)
        return f"Classify these job titles:\n{titles}"

    @task
    def stage(labels) -> int:
        hook = BaseHook.get_hook(conn_id=CONN_ID)
        rows = [(_get(l, "title"), _get(l, "persona")) for l in _get(labels, "labels")]
        hook.run(f"DELETE FROM {STAGING_PERSONAS_TABLE}")
        if rows:
            hook.insert_rows(
                STAGING_PERSONAS_TABLE, rows, target_fields=["title", "persona"]
            )
        return len(rows)

    measure_batch = SQLExecuteQueryOperator(
        task_id="measure_batch",
        conn_id=CONN_ID,
        sql=f"""
            SELECT SUM(CASE WHEN t.label IS NULL THEN 1 ELSE 0 END), COUNT(*)
            FROM {STAGING_PERSONAS_TABLE} s
            LEFT JOIN {TAXONOMY_TABLE} t ON s.persona = t.label
        """,
    )

    @task(retries=0)
    def audit(result) -> None:
        unclassified, total = (int(v or 0) for v in result[0])
        ratio = unclassified / total if total else 0.0
        print(f"{unclassified} of {total} labels unclassified ({ratio:.1%})")
        if unclassified > MIN_UNCLASSIFIED_ROWS and ratio > MAX_UNCLASSIFIED_RATIO:
            raise AirflowFailException(
                f"Quality gate failed: {unclassified} labels are unclassified."
            )

    publish = SQLExecuteQueryOperator(
        task_id="publish",
        conn_id=CONN_ID,
        sql=f"""
            UPDATE {PERSONAS_CACHE_TABLE}
            SET persona = (SELECT s.persona FROM {STAGING_PERSONAS_TABLE} s
                           WHERE s.title = {PERSONAS_CACHE_TABLE}.title)
            WHERE title IN (
                SELECT title FROM {STAGING_PERSONAS_TABLE}
                WHERE persona IS NOT NULL
            )
        """,
    )

    breaker = circuit_breaker(measure_retry_pool.output)
    breaker >> fetch_batch
    staged = stage(classify(fetch_batch.output))
    staged >> measure_batch
    audit(measure_batch.output) >> publish


ai_classifier_gated()

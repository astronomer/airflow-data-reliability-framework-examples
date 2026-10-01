"""
Audit staged classifications before publishing them.

    write_staging >> measure_batch >> audit >> publish

The audit rejects an empty batch. For non-empty batches, it blocks publication
when the unclassified count exceeds both the configured row threshold and
ratio threshold. Using both thresholds prevents small batches from failing
because of a small number of unclassified rows.
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


INCOMING_PERSONAS_TABLE = _table_name("personas_incoming")
STAGING_PERSONAS_TABLE = _table_name("personas_staging")
PERSONAS_TABLE = _table_name("personas")
TAXONOMY_TABLE = _table_name("taxonomy")

MAX_UNCLASSIFIED_RATIO = 0.10
MIN_UNCLASSIFIED_ROWS = 25  # the floor


@dag(
    dag_id="layer4_publish_gate",
    schedule=None,  # production: whatever cadence the classifier runs on
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reliability-demo", "layer-4"],
    doc_md=__doc__,
)
def layer4_publish_gate():
    write_staging = SQLExecuteQueryOperator(
        task_id="write_staging",
        conn_id=CONN_ID,
        sql=[
            f"DELETE FROM {STAGING_PERSONAS_TABLE}",
            f"""INSERT INTO {STAGING_PERSONAS_TABLE}
                SELECT title, persona FROM {INCOMING_PERSONAS_TABLE}""",
        ],
    )

    # Count NULL labels and labels that are absent from the taxonomy.
    measure_batch = SQLExecuteQueryOperator(
        task_id="measure_batch",
        conn_id=CONN_ID,
        sql=f"""
            SELECT SUM(CASE WHEN t.label IS NULL THEN 1 ELSE 0 END) AS unclassified,
                   COUNT(*) AS total
            FROM {STAGING_PERSONAS_TABLE} s
            LEFT JOIN {TAXONOMY_TABLE} t ON s.persona = t.label
        """,
    )

    @task(retries=0)
    def audit(result) -> None:
        row = result[0]
        unclassified, total = int(row[0] or 0), int(row[1] or 0)
        ratio = unclassified / total if total else 1.0
        print(f"{unclassified} of {total} rows unclassified ({ratio:.1%})")

        if total == 0:
            raise AirflowFailException("Empty batch; refusing to publish.")
        if unclassified > MIN_UNCLASSIFIED_ROWS and ratio > MAX_UNCLASSIFIED_RATIO:
            raise AirflowFailException(
                f"Quality gate failed: {unclassified} unclassified rows "
                f"({ratio:.1%}). Publication was skipped."
            )

    publish = SQLExecuteQueryOperator(
        task_id="publish",
        conn_id=CONN_ID,
        sql=[
            f"DELETE FROM {PERSONAS_TABLE}",
            f"""INSERT INTO {PERSONAS_TABLE}
                SELECT title, persona FROM {STAGING_PERSONAS_TABLE}""",
        ],
    )

    write_staging >> measure_batch
    audit(measure_batch.output) >> publish


layer4_publish_gate()

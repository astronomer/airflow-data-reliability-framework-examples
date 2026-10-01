"""
Validate staged classifications before publishing them.

    write_staging >> [labels_not_null, has_rows, labels_in_taxonomy] >> publish

The publish task runs only when the batch contains rows, every label is
populated, and every label exists in the taxonomy table. Keeping the taxonomy
in a table allows categories to change without modifying this DAG.

Validation tasks do not retry because they evaluate the contents of a fixed
batch. A new or corrected batch should be evaluated in a new DAG run.
"""

import os
from datetime import datetime

from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
from airflow.sdk import dag

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


@dag(
    dag_id="layer2_data_checks_gate",
    schedule=None,  # production: whatever cadence the classifier runs on
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reliability-demo", "layer-2"],
    doc_md=__doc__,
)
def layer2_data_checks_gate():
    write_staging = SQLExecuteQueryOperator(
        task_id="write_staging",
        conn_id=CONN_ID,
        sql=[
            f"DELETE FROM {STAGING_PERSONAS_TABLE}",
            f"""INSERT INTO {STAGING_PERSONAS_TABLE}
                SELECT title, persona FROM {INCOMING_PERSONAS_TABLE}""",
        ],
    )

    # Require every staged row to have a label.
    labels_not_null = SQLColumnCheckOperator(
        task_id="labels_not_null",
        conn_id=CONN_ID,
        table=STAGING_PERSONAS_TABLE,
        column_mapping={"persona": {"null_check": {"equal_to": 0}}},
        retries=0,
    )

    # Require at least one row in the staged batch.
    has_rows = SQLTableCheckOperator(
        task_id="has_rows",
        conn_id=CONN_ID,
        table=STAGING_PERSONAS_TABLE,
        checks={"row_count": {"check_statement": "COUNT(*) > 0"}},
        retries=0,
    )

    # Require every non-NULL label to exist in the taxonomy table.
    labels_in_taxonomy = SQLCheckOperator(
        task_id="labels_in_taxonomy",
        conn_id=CONN_ID,
        sql=f"""
            SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
            FROM {STAGING_PERSONAS_TABLE} s
            LEFT JOIN {TAXONOMY_TABLE} t ON s.persona = t.label
            WHERE s.persona IS NOT NULL AND t.label IS NULL
        """,
        retries=0,
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

    write_staging >> [labels_not_null, has_rows, labels_in_taxonomy] >> publish


layer2_data_checks_gate()

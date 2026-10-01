"""
Create and seed the tables used by the reliability-pattern examples.

Choose one of two scenarios when manually triggering the DAG:

* healthy - seeds data that passes the example checks
* broken  - seeds data that demonstrates each failure condition

Each run recreates the demo tables before loading the selected dataset.
"""

import os
from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, dag, task

CONN_ID = os.getenv("RELIABILITY_DEMO_CONN_ID", "warehouse")
TABLE_NAMESPACE = os.getenv("RELIABILITY_DEMO_TABLE_NAMESPACE", "").strip(".")
TABLE_PREFIX = os.getenv("RELIABILITY_DEMO_TABLE_PREFIX", "demo_")


def _table_name(base_name: str) -> str:
    """Build a table name using the optional database/schema namespace."""
    table_name = f"{TABLE_PREFIX}{base_name}"
    return f"{TABLE_NAMESPACE}.{table_name}" if TABLE_NAMESPACE else table_name


TAXONOMY = [
    "Data Engineer",
    "Analytics Engineer",
    "Data Scientist",
    "ML Engineer",
    "Platform Engineer",
    "Executive",
    "Other Technical",
]

TAXONOMY_TABLE = _table_name("taxonomy")
INCOMING_PERSONAS_TABLE = _table_name("personas_incoming")
STAGING_PERSONAS_TABLE = _table_name("personas_staging")
PERSONAS_TABLE = _table_name("personas")
PERSONAS_CACHE_TABLE = _table_name("personas_cache")
VENDOR_ORDERS_TABLE = _table_name("vendor_orders")

TABLES = {
    TAXONOMY_TABLE: "label VARCHAR(100)",
    INCOMING_PERSONAS_TABLE: "title VARCHAR(200), persona VARCHAR(100)",
    STAGING_PERSONAS_TABLE: "title VARCHAR(200), persona VARCHAR(100)",
    PERSONAS_TABLE: "title VARCHAR(200), persona VARCHAR(100)",
    PERSONAS_CACHE_TABLE: "title VARCHAR(200), persona VARCHAR(100)",
    VENDOR_ORDERS_TABLE: "order_id INTEGER, load_date DATE",
}

INCOMING_ROW_COUNT = 200
BROKEN_NULL_LABEL_COUNT = 60
BROKEN_UNKNOWN_LABEL_COUNT = 20
CACHE_ROW_COUNT = 3_000
HEALTHY_CACHE_NULL_INTERVAL = 333
BROKEN_CACHE_NULL_INTERVAL = 2
VENDOR_ORDER_COUNT = 50
CURRENT_DATE_SQL = "CURRENT_DATE"


def _sql_literal(value: str | int | None) -> str:
    """Convert a Python value to the SQL used by this demo."""
    if value is None:
        return "NULL"
    if isinstance(value, int) or value == CURRENT_DATE_SQL:
        return str(value)
    escaped_value = value.replace("'", "''")
    return f"'{escaped_value}'"


def _insert_statements(
    table: str,
    rows: list[tuple[str | int | None, ...]],
    batch_size: int = 500,
) -> list[str]:
    """Build portable multi-row inserts without exceeding warehouse limits."""
    statements = []

    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        sql_rows = []

        for row in batch:
            values = ", ".join(_sql_literal(value) for value in row)
            sql_rows.append(f"({values})")

        statements.append(f"INSERT INTO {table} VALUES {', '.join(sql_rows)}")

    return statements


def _incoming_persona_rows(is_broken: bool) -> list[tuple[str, str | None]]:
    """Create the current batch of job-title classifications."""
    rows = []

    for index in range(INCOMING_ROW_COUNT):
        persona = TAXONOMY[index % len(TAXONOMY)]

        if is_broken and index < BROKEN_NULL_LABEL_COUNT:
            persona = None
        elif is_broken and index < (
            BROKEN_NULL_LABEL_COUNT + BROKEN_UNKNOWN_LABEL_COUNT
        ):
            persona = "DE"

        rows.append((f"Job title {index}", persona))

    return rows


def _cached_persona_rows(is_broken: bool) -> list[tuple[str, str | None]]:
    """Create a cache with either a normal or unusually high NULL rate."""
    null_every = (
        BROKEN_CACHE_NULL_INTERVAL
        if is_broken
        else HEALTHY_CACHE_NULL_INTERVAL
    )
    rows = []

    for index in range(CACHE_ROW_COUNT):
        persona = None if index % null_every == 0 else TAXONOMY[index % len(TAXONOMY)]
        rows.append((f"Cached title {index}", persona))

    return rows


def _vendor_order_rows(is_broken: bool) -> list[tuple[int, str]]:
    """Create a current vendor delivery, or an old one for the broken scenario."""
    load_date = "2000-01-01" if is_broken else CURRENT_DATE_SQL
    return [(order_id, load_date) for order_id in range(VENDOR_ORDER_COUNT)]


def _seed_statements(scenario: str) -> list[str]:
    """Build all seed statements for one demo scenario."""
    is_broken = scenario == "broken"

    return [
        *_insert_statements(TAXONOMY_TABLE, [(label,) for label in TAXONOMY]),
        *_insert_statements(
            INCOMING_PERSONAS_TABLE, _incoming_persona_rows(is_broken)
        ),
        *_insert_statements(PERSONAS_CACHE_TABLE, _cached_persona_rows(is_broken)),
        *_insert_statements(VENDOR_ORDERS_TABLE, _vendor_order_rows(is_broken)),
    ]


@dag(
    dag_id="reliability_demo_setup",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reliability-demo"],
    params={
        "scenario": Param(
            "healthy",
            enum=["healthy", "broken"],
            description="Choose data that passes or demonstrates the checks.",
        )
    },
    doc_md=__doc__,
)
def reliability_demo_setup():
    reset_tables = SQLExecuteQueryOperator(
        task_id="reset_tables",
        conn_id=CONN_ID,
        sql=[f"DROP TABLE IF EXISTS {t}" for t in TABLES]
        + [f"CREATE TABLE {t} ({cols})" for t, cols in TABLES.items()],
    )

    @task.branch
    def choose_scenario(**context) -> str:
        return f"seed_{context['params']['scenario']}"

    seed_healthy = SQLExecuteQueryOperator(
        task_id="seed_healthy", conn_id=CONN_ID, sql=_seed_statements("healthy")
    )
    seed_broken = SQLExecuteQueryOperator(
        task_id="seed_broken", conn_id=CONN_ID, sql=_seed_statements("broken")
    )

    reset_tables >> choose_scenario() >> [seed_healthy, seed_broken]


reliability_demo_setup()

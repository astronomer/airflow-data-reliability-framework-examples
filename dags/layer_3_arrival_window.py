"""
Wait for the current vendor delivery before processing orders.

    vendor_file_landed >> load_orders

The sensor succeeds when rows dated with the current day are available. It
uses reschedule mode so the worker slot is released between checks.

Configure the waiting period with RELIABILITY_DEMO_ARRIVAL_WINDOW_MINUTES.
The default is 10 minutes for a short demonstration cycle.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sdk import dag

CONN_ID = os.getenv("RELIABILITY_DEMO_CONN_ID", "warehouse")
TABLE_NAMESPACE = os.getenv("RELIABILITY_DEMO_TABLE_NAMESPACE", "").strip(".")
TABLE_PREFIX = os.getenv("RELIABILITY_DEMO_TABLE_PREFIX", "demo_")


def _table_name(base_name: str) -> str:
    """Build a table name using the optional database/schema namespace."""
    table_name = f"{TABLE_PREFIX}{base_name}"
    return f"{TABLE_NAMESPACE}.{table_name}" if TABLE_NAMESPACE else table_name


VENDOR_ORDERS_TABLE = _table_name("vendor_orders")
WINDOW = timedelta(
    minutes=int(os.getenv("RELIABILITY_DEMO_ARRIVAL_WINDOW_MINUTES", "10"))
)


@dag(
    dag_id="layer3_arrival_window",
    schedule=None,  # production: e.g. "0 6 * * *", when the file is due
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reliability-demo", "layer-3"],
    doc_md=__doc__,
)
def layer3_arrival_window():
    # Wait until the vendor table contains at least one row for today.
    # Scheduled or backfilled workflows can use the Airflow data interval instead.
    vendor_file_landed = SqlSensor(
        task_id="vendor_file_landed",
        conn_id=CONN_ID,
        sql=f"""SELECT COUNT(*) FROM {VENDOR_ORDERS_TABLE}
            WHERE load_date = CURRENT_DATE""",
        mode="reschedule",
        poke_interval=60,
        timeout=WINDOW.total_seconds(),
        retries=0,  # The sensor timeout defines the complete arrival window.
    )

    load_orders = SQLExecuteQueryOperator(
        task_id="load_orders",
        conn_id=CONN_ID,
        sql=f"""SELECT COUNT(*) FROM {VENDOR_ORDERS_TABLE}
            WHERE load_date = CURRENT_DATE""",
    )

    vendor_file_landed >> load_orders


layer3_arrival_window()

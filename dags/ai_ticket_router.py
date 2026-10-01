"""
Route support tickets with structured model output.

    route_ticket (one mapped task per ticket) >> act_on_route

Each ticket is processed independently through dynamic task mapping. The model
must return one of the queue names defined by the Queue type. Usage limits bound
model requests, and the retry policy distinguishes transient errors from errors
that require a configuration or prompt change.

The DAG uses the LLM connection configured by RELIABILITY_DEMO_LLM_CONN_ID.
Set RELIABILITY_DEMO_MODEL to override the model configured on that connection.
Set RELIABILITY_DEMO_REQUIRE_APPROVAL=0 to skip review in the Airflow UI.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Literal

from pydantic_ai.usage import UsageLimits

from airflow.sdk import RetryAction, RetryRule, dag, task
from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy, RetryDecision

LLM_CONN_ID = os.getenv("RELIABILITY_DEMO_LLM_CONN_ID", "llm")
MODEL_ID = os.getenv("RELIABILITY_DEMO_MODEL") or None
REQUIRE_APPROVAL = os.getenv("RELIABILITY_DEMO_REQUIRE_APPROVAL", "1") == "1"

Queue = Literal["billing", "support", "spam"]

TICKETS = [
    "I was charged twice for September. Can you refund one of them?",
    "The scheduler stopped picking up new DAG runs after the upgrade.",
    "Congratulations, you have won a free cruise! Click here.",
    "My invoice shows the wrong company name, and the export button 500s.",
]


class ModelCallPolicy(ExceptionRetryPolicy):
    """Handle authentication failures before applying general HTTP rules."""

    def evaluate(self, exception, try_number, max_tries, context=None):
        if getattr(exception, "status_code", None) in (401, 403):
            return RetryDecision(
                action=RetryAction.FAIL, reason="Model authentication failed"
            )
        return super().evaluate(exception, try_number, max_tries, context)


model_call_policy = ModelCallPolicy(
    rules=[
        RetryRule(
            exception="pydantic_ai.exceptions.ModelHTTPError",
            action=RetryAction.RETRY,
            retry_delay=timedelta(seconds=60),
            reason="Transient model provider response",
        ),
        RetryRule(
            exception=[ConnectionError, TimeoutError],
            action=RetryAction.RETRY,
            retry_delay=timedelta(seconds=10),
            reason="Transient network error",
        ),
        RetryRule(
            exception="pydantic_ai.exceptions.UnexpectedModelBehavior",
            action=RetryAction.FAIL,
            reason="Model output did not match the requested structure",
        ),
        RetryRule(
            exception="pydantic_ai.exceptions.UsageLimitExceeded",
            action=RetryAction.FAIL,
            reason="Model request limit reached",
        ),
        RetryRule(
            exception=PermissionError,
            action=RetryAction.FAIL,
            reason="Model authentication failed",
        ),
    ],
)


@dag(
    dag_id="ai_ticket_router",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["reliability-demo", "ai"],
    doc_md=__doc__,
)
def ai_ticket_router():
    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        model_id=MODEL_ID,
        system_prompt=(
            "Route the support ticket. billing: invoices, charges, refunds. "
            "support: a product problem that needs an engineer. spam: not a real request."
        ),
        output_type=Queue,
        usage_limits=UsageLimits(request_limit=3),
        require_approval=REQUIRE_APPROVAL,
        approval_timeout=timedelta(hours=4),
        retries=3,
        retry_policy=model_call_policy,
    )
    def route_ticket(ticket: str):
        return ticket

    @task
    def act_on_route(routes: list[str]) -> None:
        for ticket, route in zip(TICKETS, routes):
            print(f"{route:>8}  {ticket}")

    act_on_route(route_ticket.expand(ticket=TICKETS))


ai_ticket_router()

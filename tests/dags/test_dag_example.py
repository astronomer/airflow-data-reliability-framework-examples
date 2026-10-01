"""Unit and validation tests for the reliability example DAGs."""

import importlib.util
import logging
import os
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.sdk import RetryAction
from airflow.sdk.exceptions import AirflowFailException
from pydantic import TypeAdapter, ValidationError


@contextmanager
def suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors():
    """
    Generate a tuple for import errors in the dag bag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

        def strip_path_prefix(path):
            return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

        # prepend "(None,None)" to ensure that a test object is always created even if it's a no op.
        return [(None, None)] + [
            (strip_path_prefix(k), v.strip()) for k, v in dag_bag.import_errors.items()
        ]


def get_dags():
    """
    Generate a tuple of dag_id, <DAG objects> in the DagBag
    """
    with suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))

    return [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


@pytest.mark.parametrize(
    "rel_path,rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(f"{rel_path} failed to import with message \n {rv}")


APPROVED_TAGS = {}


@pytest.mark.parametrize(
    "dag_id,dag,fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a Dag is tagged and if those TAGs are in the approved list
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert not set(dag.tags) - APPROVED_TAGS


EXPECTED_TASKS = {
    "ai_classifier_gated": {
        "measure_retry_pool": set(),
        "circuit_breaker": {"measure_retry_pool"},
        "fetch_batch": {"circuit_breaker"},
        "classify": {"fetch_batch"},
        "stage": {"classify"},
        "measure_batch": {"stage"},
        "audit": {"measure_batch"},
        "publish": {"audit"},
    },
    "ai_ticket_router": {
        "route_ticket": set(),
        "act_on_route": {"route_ticket"},
    },
    "layer2_data_checks_gate": {
        "write_staging": set(),
        "labels_not_null": {"write_staging"},
        "has_rows": {"write_staging"},
        "labels_in_taxonomy": {"write_staging"},
        "publish": {"labels_not_null", "has_rows", "labels_in_taxonomy"},
    },
    "layer3_arrival_window": {
        "vendor_file_landed": set(),
        "load_orders": {"vendor_file_landed"},
    },
    "layer4_publish_gate": {
        "write_staging": set(),
        "measure_batch": {"write_staging"},
        "audit": {"measure_batch"},
        "publish": {"audit"},
    },
    "layer5_circuit_breaker": {
        "measure_retry_pool": set(),
        "circuit_breaker": {"measure_retry_pool"},
        "classify_with_llm": {"circuit_breaker"},
    },
    "reliability_demo_setup": {
        "reset_tables": set(),
        "choose_scenario": {"reset_tables"},
        "seed_healthy": {"choose_scenario"},
        "seed_broken": {"choose_scenario"},
    },
}

TABLE_MODULES = [
    "ai_classifier_gated",
    "layer_2_data_gate_check",
    "layer_3_arrival_window",
    "layer_4_publish_gate",
    "layer_5_circuit_breaker",
    "reliability_demo_setup",
]


@lru_cache
def load_dag_module(module_name):
    """Load a DAG file as a normal Python module for unit testing helpers."""
    path = Path(os.environ["AIRFLOW_HOME"]) / "dags" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(f"tested_{module_name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(include_examples=False)


def test_expected_dag_ids(dag_bag):
    assert not dag_bag.import_errors
    assert set(dag_bag.dags) == set(EXPECTED_TASKS)


@pytest.mark.parametrize("dag_id", EXPECTED_TASKS)
def test_task_dependencies(dag_bag, dag_id):
    dag = dag_bag.dags[dag_id]
    expected = EXPECTED_TASKS[dag_id]

    assert set(dag.task_ids) == set(expected)
    for task_id, upstream_task_ids in expected.items():
        assert dag.get_task(task_id).upstream_task_ids == upstream_task_ids


@pytest.mark.parametrize("module_name", TABLE_MODULES)
def test_table_name_uses_namespace_and_prefix(monkeypatch, module_name):
    module = load_dag_module(module_name)
    monkeypatch.setattr(module, "TABLE_NAMESPACE", "analytics.reporting")
    monkeypatch.setattr(module, "TABLE_PREFIX", "sample_")

    assert module._table_name("personas") == "analytics.reporting.sample_personas"


@pytest.mark.parametrize("module_name", TABLE_MODULES)
def test_table_name_without_namespace(monkeypatch, module_name):
    module = load_dag_module(module_name)
    monkeypatch.setattr(module, "TABLE_NAMESPACE", "")
    monkeypatch.setattr(module, "TABLE_PREFIX", "demo_")

    assert module._table_name("personas") == "demo_personas"


def test_setup_healthy_and_broken_incoming_rows():
    setup = load_dag_module("reliability_demo_setup")
    healthy = setup._incoming_persona_rows(is_broken=False)
    broken = setup._incoming_persona_rows(is_broken=True)

    assert len(healthy) == setup.INCOMING_ROW_COUNT
    assert all(persona in setup.TAXONOMY for _, persona in healthy)
    assert sum(persona is None for _, persona in broken) == setup.BROKEN_NULL_LABEL_COUNT
    assert sum(persona == "DE" for _, persona in broken) == setup.BROKEN_UNKNOWN_LABEL_COUNT


def test_setup_healthy_and_broken_cache_rows():
    setup = load_dag_module("reliability_demo_setup")
    healthy = setup._cached_persona_rows(is_broken=False)
    broken = setup._cached_persona_rows(is_broken=True)

    assert len(healthy) == len(broken) == setup.CACHE_ROW_COUNT
    assert sum(persona is None for _, persona in healthy) == 10
    assert sum(persona is None for _, persona in broken) == 1_500


def test_setup_vendor_order_dates():
    setup = load_dag_module("reliability_demo_setup")

    assert {date for _, date in setup._vendor_order_rows(False)} == {
        setup.CURRENT_DATE_SQL
    }
    assert {date for _, date in setup._vendor_order_rows(True)} == {"2000-01-01"}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, "NULL"),
        (42, "42"),
        ("CURRENT_DATE", "CURRENT_DATE"),
        ("O'Brien", "'O''Brien'"),
    ],
)
def test_setup_sql_literals(value, expected):
    setup = load_dag_module("reliability_demo_setup")
    assert setup._sql_literal(value) == expected


def test_insert_statements_are_batched():
    setup = load_dag_module("reliability_demo_setup")
    rows = [(index,) for index in range(1_001)]

    statements = setup._insert_statements("demo_rows", rows, batch_size=500)

    assert len(statements) == 3
    assert all(statement.startswith("INSERT INTO demo_rows VALUES") for statement in statements)


def test_setup_branch_uses_selected_scenario(dag_bag):
    choose_scenario = dag_bag.dags["reliability_demo_setup"].get_task(
        "choose_scenario"
    )

    assert choose_scenario.python_callable(params={"scenario": "healthy"}) == "seed_healthy"
    assert choose_scenario.python_callable(params={"scenario": "broken"}) == "seed_broken"


def test_arrival_sensor_configuration(dag_bag):
    sensor = dag_bag.dags["layer3_arrival_window"].get_task("vendor_file_landed")
    layer3 = load_dag_module("layer_3_arrival_window")

    assert sensor.mode == "reschedule"
    assert sensor.poke_interval == 60
    assert sensor.timeout == layer3.WINDOW.total_seconds()
    assert sensor.retries == 0
    assert layer3.VENDOR_ORDERS_TABLE in sensor.sql
    assert "load_date = CURRENT_DATE" in sensor.sql


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ([(0, 0)], 0),
        ([(50, 2_000)], 50),
        ([(999, 2_000)], 999),
    ],
)
def test_layer5_circuit_breaker_allows_bounded_retries(dag_bag, result, expected):
    task = dag_bag.dags["layer5_circuit_breaker"].get_task("circuit_breaker")
    assert task.python_callable(result) == expected


def test_layer5_circuit_breaker_rejects_large_retry_pool(dag_bag):
    task = dag_bag.dags["layer5_circuit_breaker"].get_task("circuit_breaker")

    with pytest.raises(AirflowFailException, match="Circuit open"):
        task.python_callable([(1_500, 3_000)])


def test_layer4_audit_thresholds(dag_bag):
    audit = dag_bag.dags["layer4_publish_gate"].get_task("audit").python_callable

    audit([(25, 200)])
    with pytest.raises(AirflowFailException, match="Quality gate failed"):
        audit([(26, 200)])
    with pytest.raises(AirflowFailException, match="Empty batch"):
        audit([(0, 0)])


def test_ai_classifier_circuit_breaker_and_audit(dag_bag):
    dag = dag_bag.dags["ai_classifier_gated"]
    circuit_breaker = dag.get_task("circuit_breaker").python_callable
    audit = dag.get_task("audit").python_callable

    assert circuit_breaker([(50, 2_000)]) == 50
    with pytest.raises(AirflowFailException, match="Circuit open"):
        circuit_breaker([(1_500, 3_000)])

    audit([(25, 200)])
    with pytest.raises(AirflowFailException, match="Quality gate failed"):
        audit([(26, 200)])


def test_ai_classifier_structured_output_models():
    classifier = load_dag_module("ai_classifier_gated")

    label = classifier.Label(title="Data platform engineer", persona="Data Engineer")
    assert label.persona == "Data Engineer"
    assert classifier.Label(title="Unknown", persona=None).persona is None

    with pytest.raises(ValidationError):
        classifier.Label(title="Invalid", persona="Not a persona")


def test_ticket_router_queue_type():
    router = load_dag_module("ai_ticket_router")
    adapter = TypeAdapter(router.Queue)

    assert adapter.validate_python("billing") == "billing"
    with pytest.raises(ValidationError):
        adapter.validate_python("unknown")


def test_ticket_router_authentication_errors_fail_without_retry():
    router = load_dag_module("ai_ticket_router")

    class AuthenticationError(Exception):
        status_code = 401

    decision = router.model_call_policy.evaluate(
        AuthenticationError(), try_number=1, max_tries=3
    )

    assert decision.action == RetryAction.FAIL
    assert decision.reason == "Model authentication failed"


def test_ai_task_configuration(dag_bag):
    classifier_module = load_dag_module("ai_classifier_gated")
    router_module = load_dag_module("ai_ticket_router")
    classifier = dag_bag.dags["ai_classifier_gated"].get_task("classify")
    router = dag_bag.dags["ai_ticket_router"].get_task("route_ticket")

    assert classifier.llm_conn_id == classifier_module.LLM_CONN_ID
    assert classifier.serialize_output is True
    assert classifier.usage_limits.request_limit == 5
    assert classifier.approval_timeout.total_seconds() == 24 * 60 * 60

    assert router.partial_kwargs["llm_conn_id"] == router_module.LLM_CONN_ID
    assert router.partial_kwargs["model_id"] == router_module.MODEL_ID
    assert router.partial_kwargs["usage_limits"].request_limit == 3
    assert router.partial_kwargs["approval_timeout"].total_seconds() == 4 * 60 * 60

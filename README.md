# Airflow Data Reliability Framework Examples

This repo accompanies the Astronomer webinar [What 400 million pipeline runs reveal about pipeline failures](https://www.astronomer.io/events/webinars/what-400-million-pipeline-runs-reveal-about-pipeline-failures-video/), presented October 1, 2026. A [PDF of the slides](Webinar_-_What_400_Million_Pipeline_Runs_Reveal.pdf) is included in the repo. It contains Airflow DAGs that implement reliability gates for data, time, state, and recovery.

The talk focuses on failures caused by moving inputs: schema changes, late data, and state that becomes invalid without raising an error. The framework has five layers:

1. **Code:** review, tests, and CI.
2. **Data:** validate inputs and block publication when checks fail.
3. **Time:** enforce arrival windows and freshness requirements.
4. **State:** write to staging, audit the result, then publish.
5. **Recovery:** bound retries and stop automated recovery when its scope is too large.

A layer can be Blind, Detecting, Alerting, or Gating. These examples focus on Gating for layers 2–5: a failed check prevents downstream work from running.

The examples are based on a job-title classifier incident. A model response-format change produced `NULL` labels without raising an error. An automated recovery branch then selected the full 246,000-row table on every run for four days. The layer 4 gate would have blocked publication, the layer 5 circuit breaker would have stopped the retry batch, and `ai_classifier_gated` combines both checks around the model call.

## DAGs

| DAG | Layer | Pattern | Broken scenario |
|---|---|---|---|
| `reliability_demo_setup` | Demo setup | Recreates six demo tables and seeds the selected scenario | `reset_tables` and `choose_scenario` succeed; `seed_broken` succeeds and `seed_healthy` is `skipped` |
| `layer2_data_checks_gate` | 2 · Data | `write_staging` feeds three SQL checks that gate `publish` | `write_staging` and `has_rows` succeed; `labels_not_null` and `labels_in_taxonomy` fail; `publish` is `upstream_failed` |
| `layer3_arrival_window` | 3 · Time | A `SqlSensor` enforces the delivery window before `load_orders` | `vendor_file_landed` fails after its timeout because no row is dated today; `load_orders` is `upstream_failed` |
| `layer4_publish_gate` | 4 · State | `write_staging` → `measure_batch` → `audit` → `publish` | `write_staging` and `measure_batch` succeed; `audit` fails on 80 unclassified rows out of 200; `publish` is `upstream_failed` |
| `layer5_circuit_breaker` | 5 · Recovery | A count-and-ratio circuit breaker runs before classification | `measure_retry_pool` succeeds; `circuit_breaker` fails on 1,500 retries out of 3,000 rows; `classify_with_llm` is `upstream_failed` |
| `ai_classifier_gated` | 2, 4, and 5 | Bounded batch, typed LLM output, optional approval, audit, and gated cache update | `measure_retry_pool` succeeds; `circuit_breaker` fails before `fetch_batch`; every downstream task is `upstream_failed`, so no model call runs |
| `ai_ticket_router` | 2–5 for an AI task | Typed routing output, dynamic task mapping, usage limits, optional approval, and an exception-aware retry policy | No broken-data path; four mapped `route_ticket` tasks feed `act_on_route` |

## Run the demo

The setup DAG creates these tables using the configured namespace and prefix:

- `demo_taxonomy`
- `demo_personas_incoming`
- `demo_personas_staging`
- `demo_personas`
- `demo_personas_cache`
- `demo_vendor_orders`

Start the local environment:

```bash
cp .env.example .env
astro dev start
```

Configure the warehouse connection and table location in `.env` before starting Airflow. For Snowflake, a namespace can include both database and schema:

```bash
RELIABILITY_DEMO_CONN_ID=warehouse
RELIABILITY_DEMO_TABLE_NAMESPACE=MY_DATABASE.MY_SCHEMA
RELIABILITY_DEMO_TABLE_PREFIX=demo_
```

Trigger `reliability_demo_setup` manually and choose `healthy` or `broken`. The setup DAG drops and recreates every demo table on each run, so do not point it at table names that contain data you need to keep. After setup succeeds, trigger the layer DAGs individually.

The scenarios contain:

| Table | Healthy | Broken |
|---|---|---|
| Taxonomy | 7 valid persona labels | The same 7 labels |
| Incoming classifications | 200 rows with valid labels | 200 rows: 60 `NULL` labels, 20 unknown `DE` labels, and 120 valid labels |
| Classification cache | 3,000 rows with 10 transient `NULL` labels, about 0.3% | 3,000 rows with 1,500 `NULL` labels, or 50% |
| Vendor orders | 50 rows dated `CURRENT_DATE` | 50 rows dated `2000-01-01` |

With healthy data, the layer gates pass. With broken data, each DAG stops at the gate described in the DAG table.

## Find your highest-failure DAGs

`include/find_your_3_percent.py` ranks DAGs by the number of failed scheduled runs in a recent window. It reads through the Airflow 3 REST API and never queries the Airflow metadata database.

The script counts only runs where `run_type=scheduled`. Manual runs and backfills are excluded. It sorts by failure count, prints the failure rate and share for each DAG, and calculates how much of all scheduled-run failures came from the top 3% of DAGs with scheduled runs.

Install `requests`, then provide the Airflow base URL and a bearer token:

```bash
python -m pip install requests

export AIRFLOW_URL="https://<your-airflow-url>"  # Do not include /api/v2.
export AIRFLOW_TOKEN="<Astro API token or Airflow JWT>"

python include/find_your_3_percent.py
```

The optional positional arguments are the number of days and the number of rows to print:

```bash
# Last 30 days, print the top 50 DAGs.
python include/find_your_3_percent.py 30 50
```

Defaults are 90 days and 25 output rows. The script makes two count requests per DAG, uses a 30-second timeout per request, and fails immediately on an HTTP error.

## AI DAGs

The AI examples require:

- Airflow 3.3 or later for the retry policy used by `ai_ticket_router`.
- `apache-airflow-providers-common-ai>=0.9.0`.
- A `pydantic-ai-slim` extra for the selected model provider. This repo installs `pydantic-ai-slim[openai]`; use another extra such as `[anthropic]` or `[typesafe]` when the configured model requires it.
- An Airflow connection with connection type `pydanticai`. The default connection ID is `llm`.

For local development, define the `llm` connection in `.env`:

```bash
AIRFLOW_CONN_LLM='{"conn_type":"pydanticai","password":"<API_KEY>","extra":{"model":"openai:gpt-5-mini"}}'
```

`ai_classifier_gated` always uses the model in the connection's `extra.model` field. `ai_ticket_router` uses `RELIABILITY_DEMO_MODEL` when it is set; otherwise it also uses the connection model.

For an offline dry run, use PydanticAI's test model for both paths:

```bash
AIRFLOW_CONN_LLM='{"conn_type":"pydanticai","extra":{"model":"test"}}'
RELIABILITY_DEMO_MODEL=test
```

Approval is controlled with one variable:

```bash
# Require approval in the Airflow UI before downstream work continues.
RELIABILITY_DEMO_REQUIRE_APPROVAL=1

# Run without approval.
RELIABILITY_DEMO_REQUIRE_APPROVAL=0
```

When the variable is unset, `ai_classifier_gated` defaults to approval off and `ai_ticket_router` defaults to approval on. The supplied `.env.example` sets it to `0`, which disables approval for both DAGs.

## Add these examples to an Astro project

1. Copy the DAG files into the target project's `dags/` directory.
2. Optionally copy `include/find_your_3_percent.py` into its `include/` directory.
3. Merge this repo's Python dependencies into the target project's `requirements.txt`. Do not replace existing requirements.
4. Create the warehouse connection referenced by `RELIABILITY_DEMO_CONN_ID`.
5. For the AI DAGs, create the `pydanticai` connection referenced by `RELIABILITY_DEMO_LLM_CONN_ID`.
6. Copy the relevant values from `.env.example` into the target project's `.env`.
7. Start and validate the project.

```bash
astro dev start
astro dev parse
astro dev pytest
```

A Snowflake warehouse connection can be supplied locally as an Airflow connection environment variable:

```bash
AIRFLOW_CONN_WAREHOUSE='{"conn_type":"snowflake","login":"<USER>","password":"<PASSWORD>","schema":"<SCHEMA>","extra":{"account":"<ACCOUNT>","database":"<DATABASE>","warehouse":"<WAREHOUSE>","role":"<ROLE>"}}'
```

Use Airflow connections or a secrets backend for credentials. Do not put credentials in DAG source or commit them in `.env`.

## Tuning

- Pair ratio thresholds with row-count floors. The layer 4 audit checks both a 10% ratio and a 25-row floor. The layer 5 breaker checks both a 5% ratio and a 1,000-row floor.
- Gate tasks use `retries=0`. A retry evaluates the same data again and does not correct a failed validation.
- Keep the taxonomy in a table. Adding or removing a category should be a data change, not a DAG code change.
- Set the layer 3 arrival window to the upstream delivery agreement rather than leaving the 10-minute demo default.
- Set `BATCH_SIZE`, model request limits, and approval timeouts to match the expected workload and operating process.

## Environment variables

| Variable | Default | Used for |
|---|---|---|
| `RELIABILITY_DEMO_CONN_ID` | `warehouse` | Airflow connection used by the setup, SQL gate, and classifier DAGs |
| `RELIABILITY_DEMO_TABLE_NAMESPACE` | Empty | Optional table namespace, such as `MY_DATABASE.MY_SCHEMA` or a schema name |
| `RELIABILITY_DEMO_TABLE_PREFIX` | `demo_` | Prefix added to every demo table name |
| `RELIABILITY_DEMO_ARRIVAL_WINDOW_MINUTES` | `10` | Layer 3 sensor timeout in minutes |
| `RELIABILITY_DEMO_LLM_CONN_ID` | `llm` | PydanticAI connection used by both AI DAGs |
| `RELIABILITY_DEMO_MODEL` | Unset | Optional model override used only by `ai_ticket_router` |
| `RELIABILITY_DEMO_REQUIRE_APPROVAL` | Classifier: `0`; router: `1` | Enables approval only when the value is exactly `1`; `.env.example` sets `0` for both |
| `AIRFLOW_URL` | Required | Base URL used by `find_your_3_percent.py`; the script appends `/api/v2` |
| `AIRFLOW_TOKEN` | Required | Bearer token used by `find_your_3_percent.py` |
| `AIRFLOW_HOME` | Set by Airflow | Used by the test suite to locate DAG files |
| `AIRFLOW_CONN_LLM` | None | Optional environment-defined Airflow connection for the default `llm` connection ID |
| `AIRFLOW_CONN_WAREHOUSE` | None | Optional environment-defined Airflow connection when using the default `warehouse` connection ID |

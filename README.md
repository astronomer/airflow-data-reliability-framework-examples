Overview
========

Companion DAGs for the webinar *What 400 million pipeline runs reveal about pipeline failures*.
There is one DAG per layer of the reliability framework, plus a setup DAG that seeds demo data in a
**healthy** or **broken** scenario, so you can watch each gate pass or trip.

Configure the demo for your warehouse in `.env`:

```bash
RELIABILITY_DEMO_CONN_ID=warehouse
RELIABILITY_DEMO_TABLE_NAMESPACE=MY_DATABASE.MY_SCHEMA
RELIABILITY_DEMO_TABLE_PREFIX=demo_
```

`RELIABILITY_DEMO_TABLE_NAMESPACE` can be a database and schema, a schema only, or empty to use the
connection defaults. The prefix is optional and defaults to `demo_`.

| DAG | Layer | Pattern | When the scenario is broken |
|---|---|---|---|
| `layer2_data_checks_gate` | 2 · Data | SQL check operators that `publish` depends on | `labels_not_null` and `labels_in_taxonomy` fail, and `publish` never runs |
| `layer3_arrival_window` | 3 · Time | `SqlSensor` with a timeout, in reschedule mode | the sensor times out, and `load_orders` never runs |
| `layer4_publish_gate` | 4 · State | Write-audit-publish with a ratio and a floor | `audit` fails: 80 unclassified rows (40%), so nothing is published |
| `layer5_circuit_breaker` | 5 · Recovery | A preflight check on the retry pool size before billable calls | the breaker opens at 1,500 rows (50%), and no model calls go out |
| `ai_ticket_router` | 2–5 for an AI pipeline | Ticket routing with the Common AI provider: typed output, pinned model (Jev by default), human approval, and an Airflow 3.3 retry policy | n/a: this one shows the layers, not a failure |
| `ai_classifier_gated` | 2, 4, 5 around an LLM | The webinar's classifier rebuilt with the Common AI provider: breaker, bounded batch, `usage_limits`, typed output, optional human approval, audit before publish | the breaker opens before any model call; `classify` never runs |
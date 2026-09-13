# Pipeline as a Web Framework

Runnable companion demo for [Lior Baber's blog post *Data pipeline as a web framework*](https://liorbaber.com). It shows how Apache Airflow TaskFlow pipelines can borrow web-framework patterns: **settings at the edge**, a **service container**, **signature-based dependency injection**, a **documented XCom codec**, and **local Iceberg + DuckDB** for lakehouse-style loads.

## The metaphor

| Web framework | This demo |
|---|---|
| URL routes | DAG + task graph |
| Controller / handler | `@task` functions |
| `Settings` / env config | `pipeline_web/settings.py` (pydantic-settings) |
| App factory / lifespan | `build_container()` at the DAG edge |
| Request-scoped DI | `@inject_services` reads `ServiceContainer` from XCom |
| JSON/session serialization | `pipeline_web/xcom_serde/` custom codec + XCom backend |
| Database repository | `IcebergOrderWriter` + local filesystem warehouse |

## Quick start (Docker — recommended)

```bash
git clone <this-repo>
cd pipeline-as-web-framework

# Start Airflow (Postgres + webserver + scheduler)
make up

# Trigger the demo DAG
make trigger

# Tail scheduler logs — look for "Injected services into …"
make logs
```

Open **http://localhost:8080** (login: `admin` / `admin`), find DAG **`pipeline_as_web`**, and inspect task logs. Each task logs which services were injected, e.g.:

```
Injected services into extract_orders: ['order_service']
extract_orders: fetched 5 raw orders
transform_orders: 3 valid orders after filter
load_summary: count=3 revenue=161.24 USD
publish_iceberg: wrote 3 orders snapshot_id=… metadata=…/metadata/0000….metadata.json
```

## Quick start (local dev / unit tests)

```bash
python3 -m venv .venv && source .venv/bin/activate
make install
make test
make dag-check   # verifies the DAG file imports without a running cluster
```

No cloud warehouse credentials are required — orders come from a bundled JSON fixture.

## Iceberg with local DuckDB

This repo demonstrates **Iceberg snapshot versioning** fully on disk:

| Operation | Tool | Notes |
|---|---|---|
| Create table, append, rollback metadata pointer | **pyiceberg** + SqlCatalog (SQLite) | Writes to `./data/warehouse` by default |
| List snapshots, time-travel reads | **DuckDB** `iceberg` extension | Read-only; point at the **metadata JSON** path |

> **Honest caveat:** DuckDB's path-based `iceberg_scan('table/root/')` expects a `version-hint.text` layout that pyiceberg does not emit. After pyiceberg writes, use the latest **`…/metadata/0000N-….metadata.json`** path for DuckDB queries (the demo helpers and DAG logs print it).

> **SQLAlchemy note:** pyiceberg's SqlCatalog requires SQLAlchemy 2.x; Airflow 2.10 pins SQLAlchemy 1.4. The Docker stack therefore runs Iceberg writes in a **dedicated venv** (`PIPELINE_ICEBERG_ISOLATED_PYTHON`) while keeping DI in the TaskFlow task signature. Local `make test` / `make iceberg-demo` install pyiceberg directly (no Airflow).

### Prerequisites

- DuckDB **≥ 1.2.2** (pinned in `[iceberg]` optional deps) with the bundled iceberg extension:

```sql
INSTALL iceberg;
LOAD iceberg;
```

- pyiceberg with SQLite catalog: `pip install -e ".[dev,iceberg]"`

### Run the standalone versioning demo

Simulates **v1 good → v2 bad → list snapshots → DuckDB time travel → pyiceberg rollback**:

```bash
make install      # installs .[dev,iceberg] — no Airflow required for tests/demo
make iceberg-demo
```

Blog-quotable snippets the demo exercises:

```sql
-- List snapshots (metadata JSON path from pyiceberg)
SELECT sequence_number, snapshot_id
FROM iceberg_snapshots('/path/to/orders/metadata/00002-….metadata.json');

-- Time-travel read the last-good snapshot
SELECT order_id, total
FROM iceberg_scan(
  '/path/to/orders/metadata/00002-….metadata.json',
  snapshot_from_id = 5634854342715360982
);
```

Python rollback helper (after a bad append):

```python
writer.rollback_to_snapshot(good_snapshot_id)  # pyiceberg ManageSnapshots API
```

### Airflow integration

The DAG adds a fourth task, **`publish_iceberg`**, which appends the filtered orders to a local Iceberg table via injected `IcebergOrderWriter`. Run `make trigger` and inspect that task's logs for `snapshot_id` and `metadata_path`.

For the full bad-append + time-travel story without modifying the DAG, use `make iceberg-demo`.

## Project layout

```
src/pipeline_web/
  settings.py          # pydantic-settings (env / .env)
  container.py         # ServiceContainer + build_container() wiring
  di.py                # @inject_services — signature-based DI for tasks
  services/
    order_store.py     # InMemoryOrderStore (fixture-backed collaborator)
    order_service.py   # OrderService: extract / normalize / filter / summarize
  iceberg/
    writer.py          # pyiceberg SqlCatalog writes + rollback helper
    duckdb_reader.py   # DuckDB iceberg_snapshots / iceberg_scan time travel
    demo.py            # v1 → v2 bad → snapshots → time travel → rollback
    isolated.py        # subprocess bridge for Airflow + pyiceberg SQLAlchemy split
    schema.py          # Shared Arrow + Iceberg schemas
  fixtures/
    sample_orders.json # Local data — no external APIs
  xcom_serde/
    codec.py           # Documented JSON envelope (NOT pickle)
    backend.py         # Custom BaseXCom backend hooking the codec

dags/
  pipeline_as_web_dag.py   # 4 TaskFlow tasks: extract → transform → load → publish_iceberg

examples/
  iceberg_demo.py      # wrapper → pipeline_web.iceberg.demo

tests/                 # pytest (services, serde, DI, iceberg/duckdb)
docker-compose.yml     # Local Airflow stack
Makefile               # install / test / up / trigger / iceberg-demo
```

## How the demo works

### 1. Edge bootstrap

At DAG definition time, `build_container()` wires `InMemoryOrderStore` → `OrderService` and `IcebergOrderWriter` from `Settings`. This is the pipeline equivalent of a web app factory.

### 2. Custom XCom serde (documented codec)

Service instances are **not pickled**. The codec serializes a reconstruction recipe:

```json
{
  "__pipeline_service_container__": true,
  "version": 1,
  "settings": { "app_name": "...", "min_order_total": 10.0, "currency": "USD", ... }
}
```

`ServiceContainerXComBackend` (configured via `AIRFLOW__CORE__XCOM_BACKEND`) intercepts serialize/deserialize so `ServiceContainer` objects inside task payloads round-trip through XCom as this envelope. See `src/pipeline_web/xcom_serde/codec.py` for the full contract.

### 3. TaskFlow handlers with DI

| Task | Handler responsibility | Injected service(s) |
|---|---|---|
| `extract_orders` | Fetch raw orders | `OrderService` |
| `transform_orders` | Normalize + filter valid | `OrderService` |
| `load_summary` | Aggregate revenue summary | `OrderService` |
| `publish_iceberg` | Append valid orders to Iceberg | `IcebergOrderWriter` |

Each handler declares dependencies in its signature. The `@inject_services` decorator resolves live instances from the `ServiceContainer` embedded in the XCom payload.

### 4. XCom flow

```
edge_container ──► extract ──► transform ──► load_summary ──► publish_iceberg
                   (+container   (+container   (+summary/orders  (local Iceberg
                    via XCom)     via XCom)      +container)       snapshot)
```

## Configuration

Copy `.env.example` → `.env` or set env vars with the `PIPELINE_` prefix:

| Variable | Default | Description |
|---|---|---|
| `PIPELINE_MIN_ORDER_TOTAL` | `10.0` | Minimum order total to count as valid |
| `PIPELINE_CURRENCY` | `USD` | Currency code for summaries |
| `PIPELINE_STORE_FIXTURE` | `sample_orders.json` | Fixture filename under `fixtures/` |
| `PIPELINE_ICEBERG_WAREHOUSE` | `./data/warehouse` | Local filesystem Iceberg warehouse |
| `PIPELINE_ICEBERG_NAMESPACE` | `pipeline` | Iceberg namespace for the orders table |

## Future work (TBD)

- **OpenLineage** — lineage events emitted per task via Airflow listener / `openlineage-airflow`

## CI

GitHub Actions runs `pytest` and a DAG import check on every push/PR (`.github/workflows/ci.yml`).

## License

MIT (see blog post / author for context).

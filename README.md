# Pipeline as a Web Framework

Runnable companion demo for [Lior Baber's blog post *Data pipeline as a web framework*](https://liorbaber.com). It shows how Apache Airflow TaskFlow pipelines can borrow web-framework patterns: **settings at the edge**, a **service container**, **signature-based dependency injection**, and a **documented XCom codec** instead of ad-hoc pickle.

## The metaphor

| Web framework | This demo |
|---|---|
| URL routes | DAG + task graph |
| Controller / handler | `@task` functions |
| `Settings` / env config | `pipeline_web/settings.py` (pydantic-settings) |
| App factory / lifespan | `build_container()` at the DAG edge |
| Request-scoped DI | `@inject_services` reads `ServiceContainer` from XCom |
| JSON/session serialization | `pipeline_web/xcom_serde/` custom codec + XCom backend |

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
```

## Quick start (local dev / unit tests)

```bash
python -m venv .venv && source .venv/bin/activate
make install
make test
make dag-check   # verifies the DAG file imports without a running cluster
```

No cloud warehouse credentials are required — orders come from a bundled JSON fixture.

## Project layout

```
src/pipeline_web/
  settings.py          # pydantic-settings (env / .env)
  container.py         # ServiceContainer + build_container() wiring
  di.py                # @inject_services — signature-based DI for tasks
  services/
    order_store.py     # InMemoryOrderStore (fixture-backed collaborator)
    order_service.py   # OrderService: extract / normalize / filter / summarize
  fixtures/
    sample_orders.json # Local data — no external APIs
  xcom_serde/
    codec.py           # Documented JSON envelope (NOT pickle)
    backend.py         # Custom BaseXCom backend hooking the codec

dags/
  pipeline_as_web_dag.py   # 3 TaskFlow tasks: extract → transform → load

tests/                 # pytest (services, codec round-trip, DI)
docker-compose.yml     # Local Airflow stack
Makefile               # install / test / up / trigger / logs
```

## How the demo works

### 1. Edge bootstrap

At DAG definition time, `build_container()` wires `InMemoryOrderStore` → `OrderService` from `Settings`. This is the pipeline equivalent of a web app factory.

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

### 3. Three TaskFlow handlers with DI

| Task | Handler responsibility | Injected service |
|---|---|---|
| `extract_orders` | Fetch raw orders | `OrderService` |
| `transform_orders` | Normalize + filter valid | `OrderService` |
| `load_summary` | Aggregate revenue summary | `OrderService` |

Each handler declares `order_service: OrderService` in its signature. The `@inject_services` decorator resolves the live instance from the `ServiceContainer` embedded in the XCom payload — handlers stay short and pure-ish.

### 4. XCom flow

```
edge_container ──► extract_orders ──► transform_orders ──► load_summary
                   (returns orders    (returns valid       (returns summary)
                    + container)      orders + container)
```

The container is re-emitted in task outputs so every hop exercises the custom XCom backend.

## Configuration

Copy `.env.example` → `.env` or set env vars with the `PIPELINE_` prefix:

| Variable | Default | Description |
|---|---|---|
| `PIPELINE_MIN_ORDER_TOTAL` | `10.0` | Minimum order total to count as valid |
| `PIPELINE_CURRENCY` | `USD` | Currency code for summaries |
| `PIPELINE_STORE_FIXTURE` | `sample_orders.json` | Fixture filename under `fixtures/` |

## Future work (TBD — intentionally stubbed)

These are called out in the blog narrative but **not implemented** in this demo so the happy path stays lightweight:

- **Apache Iceberg** — table format + catalog integration for the load step (object storage, REST catalog, etc.)
- **OpenLineage** — lineage events emitted per task via Airflow listener / `openlineage-airflow`

Contributions welcome on both; see issues or extend `load_summary` with clearly marked `# TBD: Iceberg` / `# TBD: OpenLineage` hooks.

## CI

GitHub Actions runs `pytest` and a DAG import check on every push/PR (`.github/workflows/ci.yml`).

## License

MIT (see blog post / author for context).

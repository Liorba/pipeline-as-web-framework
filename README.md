# pipeline-as-web-framework

Runnable companion demo for [Data pipeline as a web framework](https://liorbaber.com) — Apache Airflow TaskFlow with **pydantic-settings**, a **composition root**, and **signature-based dependency injection** over a **documented XCom codec** (not pickle).

## What this demonstrates

| Blog concept | Repo location |
|---|---|
| Configuration / env | `src/pipeline_web/settings.py` |
| Domain services | `src/pipeline_web/services/` |
| Composition root (wire graph once) | `src/pipeline_web/container.py`, `bootstrap.py` |
| XCom serde (portable service payloads) | `src/pipeline_web/xcom_serde/codec.py` |
| TaskFlow DI by signature | `src/pipeline_web/di.py` |
| Orchestrator DAG | `dags/pipeline_as_web_dag.py` |

### Pipeline shape

```
bootstrap  →  extract  →  transform  →  load_summary
 (edge)         ↑            ↑              ↑
           inject OrderService + Settings from XCom bundle
```

- **bootstrap** — composition root: build `OrderService` + `InMemoryOrderStore` from `Settings`, encode via `pipeline-web/v1` JSON codec, push to XCom.
- **extract / transform / load_summary** — three pure `@task` functions; each **declares** dependencies in its signature (`order_service: OrderService = INJECT`, etc.). The `@inject_services` decorator decodes the bundle and injects only what each task asks for. The `INJECT` sentinel keeps TaskFlow wiring happy while preserving readable, blog-quotable signatures.

No cloud warehouse credentials are required — sample orders live in settings.

## Quickstart (local Airflow)

**Requirements:** Docker + Docker Compose

```bash
git clone <this-repo>
cd pipeline-as-web-framework
make up
```

1. Open **http://localhost:8080** — login `admin` / `admin`
2. Enable and trigger DAG **`pipeline_as_web_framework`**
3. Open task logs for `extract`, `transform`, or `load_summary`

**What to look for in logs:**

```
DI inject into extract: order_service (codec round-trip via XCom)
DI inject into transform: order_service (codec round-trip via XCom)
DI inject into load_summary: order_service, settings (codec round-trip via XCom)
```

The final `load_summary` XCom / log output resembles:

```json
{
  "app": "pipeline-as-web-framework",
  "order_count": 2,
  "total_amount": 67.5,
  "currency": "USD",
  "order_ids": ["o1", "o3"],
  "pipeline_min_amount": 10.0
}
```

Trigger from CLI:

```bash
make trigger
make logs
```

## Development (no cluster)

```bash
python -m pip install -e ".[dev]"
pytest -v
```

Tests cover pure services, the XCom codec round-trip, and signature injection — no running Airflow required.

## XCom codec (documented contract)

Service graphs cross task boundaries as JSON matching **`pipeline-web/v1`** (see `src/pipeline_web/xcom_serde/codec.py`):

- `settings` — pydantic-settings snapshot
- `services` — typed registry entries (`InMemoryOrderStore`, `OrderService`) with explicit `depends_on` edges

This is intentional, reviewable serialization — **not** `pickle`.

Optional Airflow serde registration is documented in `src/pipeline_web/xcom_serde/backend.py`.

## Project layout

```
src/pipeline_web/
  settings.py          # pydantic-settings
  container.py         # ServiceBundle + wiring
  bootstrap.py         # edge entrypoint
  di.py                # signature-based injection decorator
  services/            # OrderService + InMemoryOrderStore
  xcom_serde/          # codec + optional Airflow backend hook
dags/
  pipeline_as_web_dag.py
tests/
docker-compose.yml
Makefile
```

## Future work (TBD — not in this demo)

### Apache Iceberg

- Replace `InMemoryOrderStore` with an Iceberg table scan/commit path
- Wire object-store / catalog settings through `Settings`
- Add an `load` task that writes curated orders to an Iceberg table

### OpenLineage

- Emit dataset + job facets from each task (`extract`, `transform`, `load_summary`)
- Register lineage metadata for the encoded service bundle boundary
- Connect to Marquez or another OpenLineage-compatible backend

These are called out here so the core DI + XCom story stays small and blog-quotable.

## Configuration

Environment variables (prefix `PIPELINE_`):

| Variable | Default | Description |
|---|---|---|
| `PIPELINE_APP_NAME` | `pipeline-as-web-framework` | App label in summaries |
| `PIPELINE_MIN_ORDER_AMOUNT` | `10.0` | Filter threshold |
| `PIPELINE_CURRENCY` | `USD` | Currency code |
| `PIPELINE_SAMPLE_ORDERS_JSON` | (built-in sample) | Raw orders JSON array |

## License

MIT (demo code)

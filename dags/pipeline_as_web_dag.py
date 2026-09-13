"""TaskFlow DAG demonstrating web-framework-style DI in a data pipeline."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from pipeline_web.bootstrap import bootstrap_pipeline
from pipeline_web.di import INJECT, inject_services
from pipeline_web.services import OrderService
from pipeline_web.settings import Settings


@dag(
    dag_id="pipeline_as_web_framework",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "di", "pipeline-as-web"],
    doc_md="""
    ## Pipeline as a Web Framework

    1. **bootstrap** — composition root: build services once, encode for XCom
    2. **extract** — inject `OrderService`, normalize raw orders
    3. **transform** — inject `OrderService`, filter valid orders
    4. **load_summary** — inject `OrderService`, produce final summary

    Check task logs for `DI inject into ...` lines proving signature-based injection.
    """,
)
def pipeline_as_web_framework() -> None:
    @task
    def bootstrap() -> str:
        """Edge wiring: instantiate the service graph and serialize for XCom."""
        return bootstrap_pipeline()

    @task
    @inject_services
    def extract(service_bundle: str, order_service: OrderService = INJECT) -> list[dict]:
        raw = order_service.fetch_raw()
        return order_service.normalize(raw)

    @task
    @inject_services
    def transform(
        service_bundle: str,
        orders: list[dict],
        order_service: OrderService = INJECT,
    ) -> list[dict]:
        return order_service.filter_valid(orders)

    @task
    @inject_services
    def load_summary(
        service_bundle: str,
        orders: list[dict],
        order_service: OrderService = INJECT,
        settings: Settings = INJECT,
    ) -> dict:
        summary = order_service.summarize(orders)
        summary["pipeline_min_amount"] = settings.min_order_amount
        return summary

    bundle = bootstrap()
    normalized = extract(bundle)
    valid = transform(bundle, normalized)
    load_summary(bundle, valid)


pipeline_as_web_framework()

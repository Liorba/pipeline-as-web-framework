"""TaskFlow DAG: pipeline as a web framework.

Metaphor
--------
- DAG file          →  URL router / app module
- @task handlers    →  controller actions (pure-ish)
- ServiceContainer  →  app.state / DI graph built at startup
- XCom + codec      →  serialized context passed between handlers

Three tasks: extract → transform → load. Each handler declares its service
dependencies in its signature; the ``@inject_services`` decorator resolves them
from the ``ServiceContainer`` that round-trips through XCom via the custom backend.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

from pipeline_web.container import ServiceContainer, build_container
from pipeline_web.di import inject_services
from pipeline_web.services.order_service import OrderService
from pipeline_web.services.order_store import Order

logger = logging.getLogger(__name__)


@dag(
    dag_id="pipeline_as_web",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "pipeline-as-web"],
    doc_md=__doc__,
)
def pipeline_as_web():
    """Extract → transform → summarize with DI-injected services."""

    # Edge bootstrap — instantiate the service graph once (like app factory).
    edge_container: ServiceContainer = build_container()
    logger.info(
        "Edge bootstrap: built ServiceContainer(app=%s, min_total=%s)",
        edge_container.settings.app_name,
        edge_container.settings.min_order_total,
    )

    @task
    @inject_services
    def extract_orders(
        container: ServiceContainer,
        order_service: OrderService,
    ) -> dict:
        """Handler: fetch raw orders. Re-emits container for downstream XCom."""
        orders = order_service.extract_raw()
        logger.info("extract_orders: fetched %d raw orders", len(orders))
        return {
            "orders": [order.__dict__ for order in orders],
            "container": container,
        }

    @task
    @inject_services
    def transform_orders(
        extract_payload: dict,
        order_service: OrderService,
    ) -> dict:
        """Handler: normalize + filter. Container arrives via XCom serde."""
        orders = [Order(**item) for item in extract_payload["orders"]]
        normalized = order_service.normalize(orders)
        valid = order_service.filter_valid(normalized)
        logger.info("transform_orders: %d valid orders after filter", len(valid))
        return {
            "orders": [order.__dict__ for order in valid],
            "container": extract_payload["container"],
        }

    @task
    @inject_services
    def load_summary(
        transform_payload: dict,
        order_service: OrderService,
    ) -> dict:
        """Handler: summarize for load. Container again round-trips through XCom."""
        orders = [Order(**item) for item in transform_payload["orders"]]
        summary = order_service.summarize(orders)
        logger.info(
            "load_summary: count=%s revenue=%s %s",
            summary["order_count"],
            summary["total_revenue"],
            summary["currency"],
        )
        # TBD: OpenLineage — emit lineage event for this summary dataset
        # TBD: Iceberg — commit summary rows to an Iceberg table via catalog
        return summary

    extracted = extract_orders(edge_container)
    transformed = transform_orders(extracted)
    load_summary(transformed)


pipeline_as_web_dag = pipeline_as_web()

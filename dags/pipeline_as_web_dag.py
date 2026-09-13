"""TaskFlow DAG: pipeline as a web framework.

Metaphor
--------
- DAG file          →  URL router / app module
- @task handlers    →  controller actions (pure-ish)
- ServiceContainer  →  app.state / DI graph built at startup
- XCom + codec      →  serialized context passed between handlers

Four tasks: extract → transform → load → publish_iceberg. Handlers declare
service dependencies in their signature; ``@inject_services`` resolves them
from the ``ServiceContainer`` that round-trips through XCom via the custom backend.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING

from airflow.decorators import dag, task

if TYPE_CHECKING:
    from pipeline_web.iceberg.writer import IcebergOrderWriter

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
    """Extract → transform → summarize → publish to local Iceberg."""

    edge_container: ServiceContainer = build_container()
    logger.info(
        "Edge bootstrap: built ServiceContainer(app=%s, warehouse=%s)",
        edge_container.settings.app_name,
        edge_container.settings.iceberg_warehouse,
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
        return {
            "summary": summary,
            "orders": transform_payload["orders"],
            "container": transform_payload["container"],
        }

    @task
    @inject_services
    def publish_iceberg(
        load_payload: dict,
        iceberg_writer: "IcebergOrderWriter",
    ) -> dict:
        """Handler: commit valid orders to a local Iceberg table (pyiceberg append)."""
        from pipeline_web.iceberg.isolated import append_orders_isolated, should_use_isolated

        orders = [Order(**item) for item in load_payload["orders"]]
        if should_use_isolated():
            result = append_orders_isolated(iceberg_writer._settings, orders)
            logger.info(
                "publish_iceberg (isolated): wrote %s orders snapshot_id=%s",
                result["order_count"],
                result["snapshot_id"],
            )
            return result

        snapshot_id = iceberg_writer.append_orders(orders)
        metadata_path = str(iceberg_writer.metadata_path())
        logger.info(
            "publish_iceberg: wrote %d orders snapshot_id=%s metadata=%s",
            len(orders),
            snapshot_id,
            metadata_path,
        )
        return {
            "snapshot_id": snapshot_id,
            "metadata_path": metadata_path,
            "table_root": str(iceberg_writer.table_root()),
            "order_count": len(orders),
        }

    extracted = extract_orders(edge_container)
    transformed = transform_orders(extracted)
    loaded = load_summary(transformed)
    publish_iceberg(loaded)


pipeline_as_web_dag = pipeline_as_web()

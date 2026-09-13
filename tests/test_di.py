"""Tests for signature-based service injection (no Airflow cluster)."""

from pipeline_web.container import build_container
from pipeline_web.di import inject_services
from pipeline_web.services.order_service import OrderService


@inject_services
def _extract_handler(container: object, order_service: OrderService) -> int:
    return len(order_service.extract_raw())


@inject_services
def _transform_handler(extract_payload: dict, order_service: OrderService) -> int:
    orders = order_service.normalize(
        [order_service.extract_raw()[0].__class__(**item) for item in extract_payload["orders"]]
    )
    return len(order_service.filter_valid(orders))


def test_inject_services_from_container_object() -> None:
    container = build_container()
    count = _extract_handler(container)
    assert count == 5


def test_inject_services_from_payload_with_nested_container() -> None:
    container = build_container()
    payload = {"orders": [o.__dict__ for o in container.order_service.extract_raw()], "container": container}
    count = _transform_handler(payload)
    assert count >= 1

"""Tests for OrderService business logic (no Airflow required)."""

from pathlib import Path

import pytest

from pipeline_web.services.order_service import OrderService
from pipeline_web.services.order_store import InMemoryOrderStore, Order


@pytest.fixture
def order_service() -> OrderService:
    fixture = Path(__file__).resolve().parents[1] / "src/pipeline_web/fixtures/sample_orders.json"
    store = InMemoryOrderStore(fixture_path=fixture)
    return OrderService(store=store, min_order_total=10.0, currency="USD")


def test_extract_raw_returns_fixture_orders(order_service: OrderService) -> None:
    orders = order_service.extract_raw()
    assert len(orders) == 5
    assert orders[0].order_id == " o-100 "


def test_normalize_trims_and_title_cases(order_service: OrderService) -> None:
    raw = [Order(order_id=" x ", customer="alice smith", total=10.556, status="COMPLETED")]
    normalized = order_service.normalize(raw)
    assert normalized[0].order_id == "x"
    assert normalized[0].customer == "Alice Smith"
    assert normalized[0].total == 10.56
    assert normalized[0].status == "completed"


def test_filter_valid_applies_min_total_and_status(order_service: OrderService) -> None:
    orders = order_service.extract_raw()
    normalized = order_service.normalize(orders)
    valid = order_service.filter_valid(normalized)
    ids = {order.order_id for order in valid}
    assert ids == {"o-100", "o-103", "o-104"}


def test_summarize_aggregates_valid_orders(order_service: OrderService) -> None:
    orders = order_service.extract_raw()
    normalized = order_service.normalize(orders)
    valid = order_service.filter_valid(normalized)
    summary = order_service.summarize(valid)
    assert summary["order_count"] == 3
    assert summary["total_revenue"] == 161.24
    assert summary["currency"] == "USD"
    assert summary["customers"] == ["Alice Smith", "Dan Nguyen", "Erin Park"]

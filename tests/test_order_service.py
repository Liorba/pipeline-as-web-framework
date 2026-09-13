"""Unit tests for domain services — no Airflow cluster required."""

from pipeline_web.container import build_service_bundle
from pipeline_web.settings import Settings


def test_normalize_title_cases_customers() -> None:
    bundle = build_service_bundle(
        Settings(sample_orders_json='[{"id":"x","customer":" ada ","amount":1,"status":"NEW"}]')
    )
    service = bundle.order_service
    normalized = service.normalize(service.fetch_raw())
    assert normalized[0]["customer"] == "Ada"
    assert normalized[0]["status"] == "new"
    assert normalized[0]["currency"] == "USD"


def test_filter_valid_respects_min_amount() -> None:
    bundle = build_service_bundle(Settings(min_order_amount=10.0))
    service = bundle.order_service
    normalized = service.normalize(service.fetch_raw())
    valid = service.filter_valid(normalized)
    ids = {order["id"] for order in valid}
    assert ids == {"o1", "o3"}


def test_summarize_totals_valid_orders() -> None:
    bundle = build_service_bundle(Settings(min_order_amount=10.0))
    service = bundle.order_service
    normalized = service.normalize(service.fetch_raw())
    valid = service.filter_valid(normalized)
    summary = service.summarize(valid)
    assert summary["order_count"] == 2
    assert summary["total_amount"] == 67.5
    assert summary["currency"] == "USD"

"""Tests for signature-based service injection."""

from pipeline_web.bootstrap import bootstrap_pipeline
from pipeline_web.di import INJECT, inject_services
from pipeline_web.services import OrderService
from pipeline_web.settings import Settings


@inject_services
def _sample_task(
    service_bundle: str,
    order_service: OrderService = INJECT,
    settings: Settings = INJECT,
) -> dict:
    orders = order_service.normalize(order_service.fetch_raw())
    return {
        "injected_app": settings.app_name,
        "order_count": len(orders),
        "service_class": order_service.__class__.__name__,
    }


def test_inject_services_from_bundle_signature() -> None:
    encoded = bootstrap_pipeline()
    result = _sample_task(encoded)
    assert result["service_class"] == "OrderService"
    assert result["order_count"] == 3
    assert result["injected_app"] == "pipeline-as-web-framework"

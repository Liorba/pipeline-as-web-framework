"""Composition root — wire the service graph from Settings (web-framework style)."""

from __future__ import annotations

from dataclasses import dataclass

from pipeline_web.services import InMemoryOrderStore, OrderService
from pipeline_web.settings import Settings


@dataclass
class ServiceBundle:
    """Wired service instances created once at the pipeline edge."""

    settings: Settings
    order_store: InMemoryOrderStore
    order_service: OrderService


def build_service_bundle(settings: Settings | None = None) -> ServiceBundle:
    """Instantiate collaborators and domain services from configuration."""
    resolved_settings = settings or Settings()
    order_store = InMemoryOrderStore(resolved_settings)
    order_service = OrderService(order_store, resolved_settings)
    return ServiceBundle(
        settings=resolved_settings,
        order_store=order_store,
        order_service=order_service,
    )

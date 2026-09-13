"""Service graph bootstrap — build dependencies once at the pipeline edge."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pipeline_web.services import InMemoryOrderStore, OrderService
from pipeline_web.settings import Settings


@dataclass
class ServiceContainer:
    """Holds live service instances for the pipeline run."""

    settings: Settings
    order_service: OrderService
    _iceberg_writer: object = field(default=None, repr=False, compare=False)

    @property
    def iceberg_writer(self):
        """Lazy Iceberg writer — avoids SqlCatalog init during DAG parsing."""
        if self._iceberg_writer is None:
            from pipeline_web.iceberg.writer import IcebergOrderWriter

            self._iceberg_writer = IcebergOrderWriter(self.settings)
        return self._iceberg_writer


def _fixture_path(settings: Settings) -> Path:
    return Path(__file__).resolve().parent / "fixtures" / settings.store_fixture


def build_container(settings: Settings | None = None) -> ServiceContainer:
    """Wire the service graph from settings (like a web app startup hook)."""
    settings = settings or Settings()
    store = InMemoryOrderStore(fixture_path=_fixture_path(settings))
    order_service = OrderService(
        store=store,
        min_order_total=settings.min_order_total,
        currency=settings.currency,
    )
    return ServiceContainer(settings=settings, order_service=order_service)

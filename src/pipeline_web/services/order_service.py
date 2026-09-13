"""Domain service — pure pipeline logic injected into TaskFlow tasks."""

from __future__ import annotations

from typing import Any

from pipeline_web.services.store import InMemoryOrderStore, RawOrder
from pipeline_web.settings import Settings


class OrderService:
    """Normalize, validate, and summarize orders."""

    def __init__(self, store: InMemoryOrderStore, settings: Settings) -> None:
        self._store = store
        self._settings = settings

    @property
    def store(self) -> InMemoryOrderStore:
        return self._store

    @property
    def settings(self) -> Settings:
        return self._settings

    def fetch_raw(self) -> list[RawOrder]:
        return self._store.fetch_raw()

    def normalize(self, orders: list[RawOrder]) -> list[dict[str, Any]]:
        return [
            {
                "id": order.id,
                "customer": order.customer.strip().title(),
                "amount": round(order.amount, 2),
                "currency": self._settings.currency,
                "status": order.status.lower(),
            }
            for order in orders
        ]

    def filter_valid(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        minimum = self._settings.min_order_amount
        return [
            order
            for order in orders
            if order["amount"] >= minimum and order["status"] in {"new", "pending"}
        ]

    def summarize(self, orders: list[dict[str, Any]]) -> dict[str, Any]:
        total = round(sum(order["amount"] for order in orders), 2)
        return {
            "app": self._settings.app_name,
            "order_count": len(orders),
            "total_amount": total,
            "currency": self._settings.currency,
            "order_ids": [order["id"] for order in orders],
        }

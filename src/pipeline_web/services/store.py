"""Thin data-access collaborator — stands in for a DB/API client in a real pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pipeline_web.settings import Settings


@dataclass(frozen=True)
class RawOrder:
    id: str
    customer: str
    amount: float
    status: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RawOrder:
        return cls(
            id=str(data["id"]),
            customer=str(data["customer"]),
            amount=float(data["amount"]),
            status=str(data["status"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "customer": self.customer,
            "amount": self.amount,
            "status": self.status,
        }


class InMemoryOrderStore:
    """In-memory order source — no warehouse credentials required."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._orders = self._load_sample_orders()

    @property
    def settings(self) -> Settings:
        return self._settings

    def _load_sample_orders(self) -> list[RawOrder]:
        payload = json.loads(self._settings.sample_orders_json)
        return [RawOrder.from_dict(item) for item in payload]

    def fetch_raw(self) -> list[RawOrder]:
        return list(self._orders)

    def to_state(self) -> dict[str, Any]:
        return {"orders": [order.to_dict() for order in self._orders]}

    @classmethod
    def from_state(cls, settings: Settings, state: dict[str, Any]) -> InMemoryOrderStore:
        store = cls(settings)
        store._orders = [RawOrder.from_dict(item) for item in state.get("orders", [])]
        return store

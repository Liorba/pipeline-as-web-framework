"""Thin data-access collaborator — stands in for a DB/API client."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class Order:
    order_id: str
    customer: str
    total: float
    status: str


class OrderStore(Protocol):
    """Collaborator interface — like a repository or HTTP client."""

    def load_orders(self) -> list[Order]: ...


class InMemoryOrderStore:
    """Loads orders from a bundled JSON fixture (no cloud credentials)."""

    def __init__(self, fixture_path: Path) -> None:
        self._fixture_path = fixture_path

    def load_orders(self) -> list[Order]:
        raw = json.loads(self._fixture_path.read_text(encoding="utf-8"))
        return [
            Order(
                order_id=item["order_id"],
                customer=item["customer"],
                total=float(item["total"]),
                status=item["status"],
            )
            for item in raw
        ]

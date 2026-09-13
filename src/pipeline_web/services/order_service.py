"""Order domain service — pure-ish handlers delegate here for business logic."""

from __future__ import annotations

from pipeline_web.services.order_store import Order, OrderStore


class OrderService:
    """Normalize, validate, and summarize orders."""

    def __init__(self, store: OrderStore, min_order_total: float, currency: str) -> None:
        self._store = store
        self._min_order_total = min_order_total
        self._currency = currency

    @property
    def currency(self) -> str:
        return self._currency

    def extract_raw(self) -> list[Order]:
        """Fetch raw orders from the store."""
        return self._store.load_orders()

    def normalize(self, orders: list[Order]) -> list[Order]:
        """Normalize totals and trim customer names."""
        return [
            Order(
                order_id=order.order_id.strip(),
                customer=order.customer.strip().title(),
                total=round(order.total, 2),
                status=order.status.lower(),
            )
            for order in orders
        ]

    def filter_valid(self, orders: list[Order]) -> list[Order]:
        """Keep only completed orders above the configured minimum."""
        return [
            order
            for order in orders
            if order.status == "completed" and order.total >= self._min_order_total
        ]

    def summarize(self, orders: list[Order]) -> dict[str, object]:
        """Produce a load-ready summary payload."""
        total_revenue = round(sum(order.total for order in orders), 2)
        return {
            "order_count": len(orders),
            "total_revenue": total_revenue,
            "currency": self._currency,
            "customers": sorted({order.customer for order in orders}),
        }

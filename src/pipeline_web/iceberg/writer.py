"""Write Iceberg tables locally via pyiceberg + SqlCatalog."""

from __future__ import annotations

import logging
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table

from pipeline_web.iceberg.schema import ORDER_ARROW_SCHEMA, ORDER_ICEBERG_SCHEMA, TABLE_IDENTIFIER
from pipeline_web.services.order_store import Order
from pipeline_web.settings import Settings

logger = logging.getLogger(__name__)


class IcebergOrderWriter:
    """Manage a local filesystem Iceberg orders table (writes via pyiceberg)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._warehouse = Path(settings.iceberg_warehouse).resolve()
        self._warehouse.mkdir(parents=True, exist_ok=True)
        self._catalog_db = self._warehouse / "catalog.db"
        self._namespace = settings.iceberg_namespace
        self._table_name = TABLE_IDENTIFIER
        self._identifier = f"{self._namespace}.{self._table_name}"
        self._catalog = load_catalog(
            "pipeline_local",
            **{
                "type": "sql",
                "uri": f"sqlite:///{self._catalog_db}",
                "warehouse": f"file://{self._warehouse}",
            },
        )

    @property
    def warehouse_path(self) -> Path:
        return self._warehouse

    @property
    def table_identifier(self) -> str:
        return self._identifier

    def _load_table(self) -> Table:
        return self._catalog.load_table(self._identifier)

    def ensure_table(self) -> Table:
        """Create namespace + table if they do not exist."""
        self._catalog.create_namespace_if_not_exists(self._namespace)
        try:
            return self._catalog.load_table(self._identifier)
        except NoSuchTableError:
            logger.info("Creating Iceberg table %s under %s", self._identifier, self._warehouse)
            return self._catalog.create_table(self._identifier, schema=ORDER_ICEBERG_SCHEMA)

    @staticmethod
    def orders_to_arrow(orders: list[Order]) -> pa.Table:
        return pa.table(
            {
                "order_id": [order.order_id for order in orders],
                "customer": [order.customer for order in orders],
                "total": [order.total for order in orders],
                "status": [order.status for order in orders],
            },
            schema=ORDER_ARROW_SCHEMA,
        )

    def append_orders(self, orders: list[Order]) -> int:
        """Append a batch and return the new snapshot id."""
        table = self.ensure_table()
        table.append(self.orders_to_arrow(orders))
        snapshot_id = table.current_snapshot().snapshot_id
        logger.info(
            "Appended %d orders to %s (snapshot_id=%s)",
            len(orders),
            self._identifier,
            snapshot_id,
        )
        return snapshot_id

    def append_bad_orders(self) -> int:
        """Append a deliberately bad batch for rollback / time-travel demos."""
        bad = [
            Order(order_id="BAD-1", customer="Bad Actor", total=-999.0, status="bad"),
            Order(order_id="BAD-2", customer="Bad Actor", total=-1.0, status="bad"),
        ]
        return self.append_orders(bad)

    def list_snapshot_ids(self) -> list[int]:
        table = self._load_table()
        return [snapshot.snapshot_id for snapshot in table.snapshots()]

    def current_snapshot_id(self) -> int:
        return self._load_table().current_snapshot().snapshot_id

    def metadata_path(self) -> Path:
        """Latest metadata JSON — DuckDB path-based reads use this file."""
        location = self._load_table().metadata_location
        return Path(location.removeprefix("file://"))

    def table_root(self) -> Path:
        location = self._load_table().location()
        return Path(location.removeprefix("file://"))

    def rollback_to_snapshot(self, snapshot_id: int) -> int:
        """Point the table's current snapshot back to a known-good version."""
        table = self._load_table()
        table.manage_snapshots().rollback_to_snapshot(snapshot_id).commit()
        table.refresh()
        current = table.current_snapshot().snapshot_id
        logger.info("Rolled back %s to snapshot_id=%s", self._identifier, current)
        return current

    def scan_order_count(self) -> int:
        """Return row count at the table's current snapshot (pyiceberg scan)."""
        return len(self._load_table().scan().to_arrow())

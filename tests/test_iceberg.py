"""Integration tests: pyiceberg writes → DuckDB snapshots / time travel."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pipeline_web.iceberg.writer import IcebergOrderWriter
from pipeline_web.services.order_store import Order
from pipeline_web.settings import Settings

if TYPE_CHECKING:
    from pipeline_web.iceberg.duckdb_reader import DuckDBIcebergReader

pytestmark = pytest.mark.iceberg


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    return tmp_path / "warehouse"


@pytest.fixture
def writer(warehouse: Path) -> IcebergOrderWriter:
    settings = Settings(iceberg_warehouse=str(warehouse), iceberg_namespace="demo")
    return IcebergOrderWriter(settings)


@pytest.fixture
def reader() -> DuckDBIcebergReader:
    from pipeline_web.iceberg.duckdb_reader import DuckDBIcebergReader

    return DuckDBIcebergReader()


def test_pyiceberg_write_list_snapshots_duckdb_time_travel(
    writer: IcebergOrderWriter,
    reader: DuckDBIcebergReader,
) -> None:
    good = [
        Order(order_id="o-1", customer="Alice", total=42.0, status="completed"),
        Order(order_id="o-2", customer="Bob", total=18.0, status="completed"),
    ]
    v1_snapshot = writer.append_orders(good)
    v2_snapshot = writer.append_bad_orders()
    assert v2_snapshot != v1_snapshot

    metadata_path = writer.metadata_path()
    snapshots = reader.list_snapshots(metadata_path)
    assert len(snapshots) == 2
    assert snapshots[0].snapshot_id == v1_snapshot

    v1_rows = reader.scan_orders(metadata_path, snapshot_id=v1_snapshot)
    assert len(v1_rows) == 2
    assert all(row[2] >= 0 for row in v1_rows)

    bad_rows = reader.scan_orders(metadata_path)
    assert len(bad_rows) == 4
    assert any(row[0].startswith("BAD") for row in bad_rows)


def test_rollback_restores_good_snapshot(
    writer: IcebergOrderWriter,
    reader: DuckDBIcebergReader,
) -> None:
    good = [Order(order_id="o-1", customer="Alice", total=10.0, status="completed")]
    v1_snapshot = writer.append_orders(good)
    writer.append_bad_orders()

    writer.rollback_to_snapshot(v1_snapshot)
    assert writer.scan_order_count() == 1

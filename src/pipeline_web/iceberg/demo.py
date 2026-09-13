"""Local Iceberg versioning demo: v1 good → v2 bad → time travel → rollback."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline_web.iceberg.duckdb_reader import DuckDBIcebergReader
from pipeline_web.iceberg.writer import IcebergOrderWriter
from pipeline_web.services.order_store import Order
from pipeline_web.settings import Settings


def run_demo(warehouse: Path | None = None) -> None:
    settings = Settings(
        iceberg_warehouse=str(warehouse or Path("./data/warehouse")),
    )
    writer = IcebergOrderWriter(settings)
    reader = DuckDBIcebergReader()

    good_orders = [
        Order(order_id="o-100", customer="Alice Smith", total=42.5, status="completed"),
        Order(order_id="o-103", customer="Dan Nguyen", total=18.75, status="completed"),
        Order(order_id="o-104", customer="Erin Park", total=99.99, status="completed"),
    ]

    print(f"Warehouse: {writer.warehouse_path}")
    print("\n== Step 1: write v1 (good orders) via pyiceberg ==")
    v1_snapshot = writer.append_orders(good_orders)
    print(f"  snapshot_id={v1_snapshot}")

    print("\n== Step 2: write v2 (bad append) via pyiceberg ==")
    v2_snapshot = writer.append_bad_orders()
    print(f"  snapshot_id={v2_snapshot}")

    metadata_path = writer.metadata_path()
    print(f"  metadata={metadata_path}")

    print("\n== Step 3: list snapshots via DuckDB iceberg_snapshots ==")
    snapshots = reader.list_snapshots(metadata_path)
    for snap in snapshots:
        marker = " <- good" if snap.snapshot_id == v1_snapshot else ""
        marker = " <- bad (current)" if snap.snapshot_id == v2_snapshot else marker
        print(f"  #{snap.sequence_number} snapshot_id={snap.snapshot_id}{marker}")

    print("\n== Step 4: time-travel read v1 via DuckDB iceberg_scan ==")
    v1_rows = reader.scan_orders(metadata_path, snapshot_id=v1_snapshot)
    print(f"  rows at v1: {len(v1_rows)}")
    for row in v1_rows:
        print(f"    {row}")

    current_rows = reader.scan_orders(metadata_path)
    print(f"\n  rows at current metadata head (includes bad append): {len(current_rows)}")

    print("\n== Step 5: rollback metadata pointer via pyiceberg ==")
    writer.rollback_to_snapshot(v1_snapshot)
    print(f"  pyiceberg current row count after rollback: {writer.scan_order_count()}")
    print("  (Use DuckDB snapshot_from_id for historical reads; current metadata head may differ.)")

    print("\nDemo complete. DuckDB SQL used under the hood:")
    print("  INSTALL iceberg; LOAD iceberg;")
    print(f"  SELECT * FROM iceberg_snapshots('{metadata_path}');")
    print(
        f"  SELECT * FROM iceberg_scan('{metadata_path}', snapshot_from_id = {v1_snapshot});"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse",
        type=Path,
        default=None,
        help="Iceberg warehouse directory (default: ./data/warehouse)",
    )
    args = parser.parse_args()
    run_demo(args.warehouse)


if __name__ == "__main__":
    main()

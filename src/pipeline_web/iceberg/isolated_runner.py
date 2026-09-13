"""CLI entrypoint for isolated Iceberg operations (subprocess from Airflow)."""

from __future__ import annotations

import json
import sys

from pipeline_web.iceberg.writer import IcebergOrderWriter
from pipeline_web.services.order_store import Order
from pipeline_web.settings import Settings


def _writer(settings_dict: dict) -> IcebergOrderWriter:
    return IcebergOrderWriter(Settings.model_validate(settings_dict))


def main() -> None:
    command = sys.argv[1] if len(sys.argv) > 1 else "append"
    payload = json.loads(sys.stdin.read())
    settings_dict = payload["settings"]

    if command == "append":
        orders = [Order(**item) for item in payload["orders"]]
        writer = _writer(settings_dict)
        snapshot_id = writer.append_orders(orders)
        result = {
            "snapshot_id": snapshot_id,
            "metadata_path": str(writer.metadata_path()),
            "table_root": str(writer.table_root()),
            "order_count": len(orders),
            "row_count": writer.scan_order_count(),
        }
    elif command == "append-bad":
        writer = _writer(settings_dict)
        snapshot_id = writer.append_bad_orders()
        result = {
            "snapshot_id": snapshot_id,
            "metadata_path": str(writer.metadata_path()),
        }
    elif command == "rollback":
        writer = _writer(settings_dict)
        current = writer.rollback_to_snapshot(int(payload["snapshot_id"]))
        result = {
            "snapshot_id": current,
            "metadata_path": str(writer.metadata_path()),
            "row_count": writer.scan_order_count(),
        }
    else:
        raise SystemExit(f"Unknown command: {command}")

    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()

"""Read Iceberg tables with DuckDB's iceberg extension (read-only)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

# DuckDB 1.2+ exposes snapshot_from_id on iceberg_scan; pin in pyproject.toml.
DUCKDB_MIN_VERSION = "1.2.2"


@dataclass(frozen=True)
class IcebergSnapshot:
    sequence_number: int
    snapshot_id: int


class DuckDBIcebergReader:
    """Query local Iceberg metadata/data via DuckDB — no cloud credentials."""

    def __init__(self, connection: duckdb.DuckDBPyConnection | None = None) -> None:
        self._con = connection or duckdb.connect()
        self._ensure_extension()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def _ensure_extension(self) -> None:
        self._con.execute("INSTALL iceberg;")
        self._con.execute("LOAD iceberg;")

    @staticmethod
    def _metadata_path(metadata_path: Path | str) -> str:
        return str(Path(metadata_path).resolve())

    def list_snapshots(self, metadata_path: Path | str) -> list[IcebergSnapshot]:
        """List snapshots via ``iceberg_snapshots('<metadata.json>')``."""
        rows = self._con.execute(
            """
            SELECT sequence_number, snapshot_id
            FROM iceberg_snapshots(?)
            ORDER BY sequence_number
            """,
            [self._metadata_path(metadata_path)],
        ).fetchall()
        return [IcebergSnapshot(sequence_number=row[0], snapshot_id=row[1]) for row in rows]

    def scan_orders(
        self,
        metadata_path: Path | str,
        snapshot_id: int | None = None,
    ) -> list[tuple[str, str, float, str]]:
        """Read order rows, optionally at a historical snapshot (time travel)."""
        path = self._metadata_path(metadata_path)
        if snapshot_id is None:
            sql = "SELECT order_id, customer, total, status FROM iceberg_scan(?)"
            rows = self._con.execute(sql, [path]).fetchall()
        else:
            sql = """
                SELECT order_id, customer, total, status
                FROM iceberg_scan(?, snapshot_from_id = ?)
            """
            rows = self._con.execute(sql, [path, snapshot_id]).fetchall()
        return [(row[0], row[1], float(row[2]), row[3]) for row in rows]

    def count_orders(self, metadata_path: Path | str, snapshot_id: int | None = None) -> int:
        return len(self.scan_orders(metadata_path, snapshot_id=snapshot_id))

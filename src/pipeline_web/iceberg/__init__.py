"""Local Iceberg integration — pyiceberg writes, DuckDB reads."""

from pipeline_web.iceberg.duckdb_reader import DuckDBIcebergReader
from pipeline_web.iceberg.writer import IcebergOrderWriter

__all__ = ["DuckDBIcebergReader", "IcebergOrderWriter"]

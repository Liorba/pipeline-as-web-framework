"""Optional Airflow XCom backend hook for the pipeline-web codec.

For local docker-compose we rely on explicit encode/decode in task code.
This module documents how to register the codec with Airflow's serde layer
when you want framework-level recognition of service payloads.
"""

from __future__ import annotations

from typing import Any

from pipeline_web.container import ServiceBundle
from pipeline_web.xcom_serde.codec import decode_service_bundle, encode_service_bundle


def serialize_service_bundle(value: ServiceBundle) -> tuple[str, str, Any]:
    """Return (namespace, name, payload) tuple for Airflow serde registration."""
    return ("pipeline_web", "ServiceBundle", encode_service_bundle(value))


def deserialize_service_bundle(_version: int, data: str) -> ServiceBundle:
    return decode_service_bundle(data)


def try_register_airflow_serde() -> None:
    """Best-effort registration with Airflow's serde registry (Airflow 2.8+)."""
    try:
        from airflow.serialization.serde import register

        register(
            ServiceBundle,
            serializer=serialize_service_bundle,
            deserializer=deserialize_service_bundle,
        )
    except ImportError:
        # Airflow not installed — unit tests and pure service tests skip this path.
        pass

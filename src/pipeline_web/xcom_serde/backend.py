"""Custom XCom backend that recognizes service-container envelopes.

Why a backend (not only helper functions)?
------------------------------------------
TaskFlow tasks return Python objects that Airflow persists to XCom. By
subclassing ``BaseXCom``, we hook serialize/deserialize centrally: when a
task payload contains a ``ServiceContainer`` (top-level or nested in dicts /
lists), the codec runs instead of pickle. This works in local
``airflow standalone`` and docker-compose without extra plugins.

For all other values we fall back to Airflow's default JSON serialization.
"""

from __future__ import annotations

import json
from typing import Any

from airflow.models.xcom import BaseXCom

from pipeline_web.container import ServiceContainer
from pipeline_web.xcom_serde.codec import (
    ServiceContainerCodec,
    is_service_container_envelope,
)


class ServiceContainerXComBackend(BaseXCom):
    """XCom backend with explicit service-container codec support."""

    @staticmethod
    def serialize_value(value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, ServiceContainer):
            return ServiceContainerCodec.encode(value)
        if is_service_container_envelope(value):
            return value
        if isinstance(value, dict):
            return {key: ServiceContainerXComBackend.serialize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [ServiceContainerXComBackend.serialize_value(item) for item in value]
        if isinstance(value, tuple):
            return tuple(ServiceContainerXComBackend.serialize_value(item) for item in value)
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        if result is None or isinstance(result, (bool, int, float, str)):
            return result
        if isinstance(result, bytes):
            return result.decode("utf-8")
        if isinstance(result, ServiceContainer):
            return result
        if is_service_container_envelope(result):
            return ServiceContainerCodec.decode(result)
        if isinstance(result, str):
            try:
                parsed = json.loads(result)
            except json.JSONDecodeError:
                return result
            if is_service_container_envelope(parsed):
                return ServiceContainerCodec.decode(parsed)
            return result
        if isinstance(result, dict):
            if is_service_container_envelope(result):
                return ServiceContainerCodec.decode(result)
            return {key: ServiceContainerXComBackend.deserialize_value(item) for key, item in result.items()}
        if isinstance(result, list):
            return [ServiceContainerXComBackend.deserialize_value(item) for item in result]
        if isinstance(result, tuple):
            return tuple(ServiceContainerXComBackend.deserialize_value(item) for item in result)
        return BaseXCom.deserialize_value(result)

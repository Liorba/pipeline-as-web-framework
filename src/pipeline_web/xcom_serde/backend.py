"""Custom XCom backend that recognizes service-container envelopes."""

from __future__ import annotations

import json
from typing import Any

from pipeline_web.container import ServiceContainer
from pipeline_web.xcom_serde.codec import (
    ServiceContainerCodec,
    is_service_container_envelope,
)


class _ServiceContainerXComBackendMixin:
    """Implementation mixin — combined with BaseXCom lazily to avoid circular imports."""

    @staticmethod
    def _serialize_item(value: Any) -> Any:
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, ServiceContainer):
            return ServiceContainerCodec.encode(value)
        if is_service_container_envelope(value):
            return value
        if isinstance(value, dict):
            return {key: _ServiceContainerXComBackendMixin._serialize_item(item) for key, item in value.items()}
        if isinstance(value, list):
            return [_ServiceContainerXComBackendMixin._serialize_item(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_ServiceContainerXComBackendMixin._serialize_item(item) for item in value)
        return value

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int | None = None,
    ) -> Any:
        serialized = _ServiceContainerXComBackendMixin._serialize_item(value)
        try:
            return json.dumps(serialized).encode("UTF-8")
        except (TypeError, ValueError):
            from airflow.models.xcom import BaseXCom

            return BaseXCom.serialize_value(
                value,
                key=key,
                task_id=task_id,
                dag_id=dag_id,
                run_id=run_id,
                map_index=map_index,
            )

    @staticmethod
    def _deserialize_item(result: Any) -> Any:
        if result is None or isinstance(result, (bool, int, float, str)):
            return result
        if isinstance(result, bytes):
            result = json.loads(result.decode("utf-8"))
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
            return {key: _ServiceContainerXComBackendMixin._deserialize_item(item) for key, item in result.items()}
        if isinstance(result, list):
            return [_ServiceContainerXComBackendMixin._deserialize_item(item) for item in result]
        if isinstance(result, tuple):
            return tuple(_ServiceContainerXComBackendMixin._deserialize_item(item) for item in result)
        return result

    @staticmethod
    def deserialize_value(result: Any) -> Any:
        raw = result.value if hasattr(result, "value") else result
        if raw is None:
            return None
        if isinstance(raw, bytes):
            try:
                raw = json.loads(raw.decode("UTF-8"))
            except json.JSONDecodeError:
                from airflow.models.xcom import BaseXCom

                return BaseXCom.deserialize_value(result)
        return _ServiceContainerXComBackendMixin._deserialize_item(raw)


_ServiceContainerXComBackendClass: type | None = None


def _build_backend_class() -> type:
    from airflow.models.xcom import BaseXCom

    class ServiceContainerXComBackend(_ServiceContainerXComBackendMixin, BaseXCom):
        """XCom backend with explicit service-container codec support."""

    return ServiceContainerXComBackend


def __getattr__(name: str) -> type:
    global _ServiceContainerXComBackendClass
    if name == "ServiceContainerXComBackend":
        if _ServiceContainerXComBackendClass is None:
            _ServiceContainerXComBackendClass = _build_backend_class()
        return _ServiceContainerXComBackendClass
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

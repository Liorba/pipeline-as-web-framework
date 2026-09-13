"""Documented JSON codec for ServiceContainer payloads.

Design choice
-------------
Airflow XCom defaults to JSON (or pickle when enabled). Service instances
are not JSON-native, so we serialize a **reconstruction recipe** instead of
object state:

  settings dict  →  rebuild InMemoryOrderStore + OrderService on decode

This mirrors web-framework DI: the container token travels over the wire;
fresh instances are materialized at each task boundary from known config.

The companion ``ServiceContainerXComBackend`` (backend.py) delegates to this
codec when it sees the ``__pipeline_service_container__`` envelope key, so
TaskFlow return values round-trip without ad-hoc pickle.
"""

from __future__ import annotations

import json
from typing import Any

from pipeline_web.container import ServiceContainer, build_container
from pipeline_web.settings import Settings

SERVICE_CONTAINER_KEY = "__pipeline_service_container__"
CODEC_VERSION = 1


class ServiceContainerCodec:
    """Explicit encode/decode for service-container XCom payloads."""

    @staticmethod
    def encode(container: ServiceContainer) -> dict[str, Any]:
        return {
            SERVICE_CONTAINER_KEY: True,
            "version": CODEC_VERSION,
            "settings": container.settings.model_dump(),
        }

    @staticmethod
    def decode(payload: dict[str, Any]) -> ServiceContainer:
        if payload.get(SERVICE_CONTAINER_KEY) is not True:
            raise ValueError("Not a service-container envelope")
        if payload.get("version") != CODEC_VERSION:
            raise ValueError(f"Unsupported codec version: {payload.get('version')}")
        settings = Settings.model_validate(payload["settings"])
        return build_container(settings)


def encode_container(container: ServiceContainer) -> str:
    """Serialize a container to a JSON string for XCom transport."""
    return json.dumps(ServiceContainerCodec.encode(container))


def decode_container(payload: str | dict[str, Any]) -> ServiceContainer:
    """Deserialize a container from JSON string or dict envelope."""
    data = json.loads(payload) if isinstance(payload, str) else payload
    return ServiceContainerCodec.decode(data)


def is_service_container_envelope(value: Any) -> bool:
    """Return True when *value* is our documented service-container envelope."""
    return isinstance(value, dict) and value.get(SERVICE_CONTAINER_KEY) is True

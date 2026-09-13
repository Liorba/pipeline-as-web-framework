"""Tests for the documented service-container XCom codec."""

import json

import pytest

from pipeline_web.container import ServiceContainer, build_container
from pipeline_web.settings import Settings
from pipeline_web.xcom_serde import backend as backend_module
from pipeline_web.xcom_serde.codec import (
    SERVICE_CONTAINER_KEY,
    ServiceContainerCodec,
    decode_container,
    encode_container,
    is_service_container_envelope,
)

ServiceContainerXComBackend = backend_module.ServiceContainerXComBackend


def test_encode_produces_documented_envelope() -> None:
    container = build_container(Settings(min_order_total=15.0, currency="EUR"))
    envelope = ServiceContainerCodec.encode(container)
    assert envelope[SERVICE_CONTAINER_KEY] is True
    assert envelope["version"] == 1
    assert envelope["settings"]["min_order_total"] == 15.0
    assert envelope["settings"]["currency"] == "EUR"


def test_round_trip_preserves_behavior() -> None:
    original = build_container(Settings(min_order_total=10.0))
    encoded = encode_container(original)
    restored = decode_container(encoded)
    assert isinstance(restored, ServiceContainer)
    assert restored.settings.min_order_total == 10.0
    orders = restored.order_service.extract_raw()
    assert len(orders) == 5


def test_json_round_trip_via_string() -> None:
    payload = encode_container(build_container())
    parsed = json.loads(payload)
    assert is_service_container_envelope(parsed)
    restored = decode_container(parsed)
    assert restored.order_service.summarize([])["currency"] == "USD"


def test_backend_serializes_nested_container_in_dict() -> None:
    container = build_container()
    payload = {"orders": [{"order_id": "1"}], "container": container}
    serialized = ServiceContainerXComBackend._serialize_item(payload)
    assert is_service_container_envelope(serialized["container"])
    raw = json.loads(ServiceContainerXComBackend.serialize_value(payload).decode("UTF-8"))
    restored = ServiceContainerXComBackend._deserialize_item(raw)
    assert isinstance(restored["container"], ServiceContainer)
    assert restored["orders"] == [{"order_id": "1"}]


def test_backend_rejects_unknown_envelope_version() -> None:
    with pytest.raises(ValueError, match="Unsupported codec version"):
        ServiceContainerCodec.decode({SERVICE_CONTAINER_KEY: True, "version": 999, "settings": {}})

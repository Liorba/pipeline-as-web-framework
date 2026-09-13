"""Round-trip tests for the documented XCom service-bundle codec."""

import json

import pytest

from pipeline_web.container import build_service_bundle
from pipeline_web.settings import Settings
from pipeline_web.xcom_serde.codec import (
    CODEC_VERSION,
    decode_service_bundle,
    encode_service_bundle,
)


def test_encode_decode_round_trip_preserves_behavior() -> None:
    settings = Settings(min_order_amount=10.0, currency="EUR")
    original = build_service_bundle(settings)
    encoded = encode_service_bundle(original)
    restored = decode_service_bundle(encoded)

    raw = restored.order_service.fetch_raw()
    normalized = restored.order_service.normalize(raw)
    valid = restored.order_service.filter_valid(normalized)
    summary = restored.order_service.summarize(valid)

    assert restored.settings.currency == "EUR"
    assert summary["currency"] == "EUR"
    assert summary["order_count"] == 2


def test_payload_is_documented_json_not_pickle() -> None:
    encoded = encode_service_bundle(build_service_bundle())
    payload = json.loads(encoded)
    assert payload["codec"] == CODEC_VERSION
    assert "pickle" not in encoded
    assert payload["services"]["order_service"]["type"] == "OrderService"


def test_unknown_codec_raises() -> None:
    with pytest.raises(ValueError, match="Unsupported codec"):
        decode_service_bundle('{"codec":"other/v9"}')

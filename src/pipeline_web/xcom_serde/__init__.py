"""Documented XCom codec for service bundles — not ad-hoc pickle."""

from pipeline_web.xcom_serde.codec import (
    CODEC_VERSION,
    decode_service_bundle,
    encode_service_bundle,
)

__all__ = [
    "CODEC_VERSION",
    "decode_service_bundle",
    "encode_service_bundle",
]

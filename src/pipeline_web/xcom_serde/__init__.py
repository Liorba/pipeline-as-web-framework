"""Custom XCom serialization for service containers.

We use an explicit JSON codec plus a thin custom XCom backend wrapper.
Pickle of arbitrary Python objects is intentionally avoided — the codec
documents every field and rebuilds services deterministically from settings.
"""

from pipeline_web.xcom_serde.codec import (
    SERVICE_CONTAINER_KEY,
    ServiceContainerCodec,
    decode_container,
    encode_container,
)

__all__ = [
    "SERVICE_CONTAINER_KEY",
    "ServiceContainerCodec",
    "decode_container",
    "encode_container",
]

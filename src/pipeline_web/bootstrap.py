"""Edge bootstrap — build the graph and hand off a portable XCom payload."""

from pipeline_web.container import ServiceBundle, build_service_bundle
from pipeline_web.settings import Settings
from pipeline_web.xcom_serde.codec import encode_service_bundle


def bootstrap_pipeline(settings: Settings | None = None) -> str:
    """Composition-root entrypoint used by the DAG bootstrap task."""
    bundle = build_service_bundle(settings)
    return encode_service_bundle(bundle)


def decode_bundle(encoded: str) -> ServiceBundle:
    """Decode a service bundle for task-local injection."""
    from pipeline_web.xcom_serde.codec import decode_service_bundle

    return decode_service_bundle(encoded)

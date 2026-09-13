"""JSON codec for passing wired service graphs through Airflow XCom.

Wire format (pipeline-web/v1)
-----------------------------
{
  "codec": "pipeline-web/v1",
  "settings": { ... pydantic Settings as dict ... },
  "services": {
    "order_store": {
      "type": "InMemoryOrderStore",
      "state": { "orders": [ ... ] }
    },
    "order_service": {
      "type": "OrderService",
      "depends_on": ["order_store"]
    }
  }
}

Services are reconstructed from typed registry entries + settings/state.
This is an explicit, documented contract — not pickle.
"""

from __future__ import annotations

import json
from typing import Any

from pipeline_web.container import ServiceBundle
from pipeline_web.services import InMemoryOrderStore, OrderService
from pipeline_web.settings import Settings

CODEC_VERSION = "pipeline-web/v1"

_SERVICE_BUILDERS = {
    "InMemoryOrderStore": lambda settings, state, _deps: InMemoryOrderStore.from_state(
        settings, state
    ),
    "OrderService": lambda settings, _state, deps: OrderService(
        deps["order_store"], settings
    ),
}


def encode_service_bundle(bundle: ServiceBundle) -> str:
    payload = {
        "codec": CODEC_VERSION,
        "settings": bundle.settings.model_dump(),
        "services": {
            "order_store": {
                "type": "InMemoryOrderStore",
                "state": bundle.order_store.to_state(),
            },
            "order_service": {
                "type": "OrderService",
                "depends_on": ["order_store"],
            },
        },
    }
    return json.dumps(payload, sort_keys=True)


def decode_service_bundle(encoded: str) -> ServiceBundle:
    payload = json.loads(encoded)
    if payload.get("codec") != CODEC_VERSION:
        raise ValueError(f"Unsupported codec: {payload.get('codec')!r}")

    settings = Settings.model_validate(payload["settings"])
    service_defs: dict[str, dict[str, Any]] = payload["services"]
    built: dict[str, Any] = {}
    pending = dict(service_defs)

    while pending:
        progress = False
        for name, definition in list(pending.items()):
            deps = definition.get("depends_on", [])
            if not all(dep in built for dep in deps):
                continue

            service_type = definition["type"]
            builder = _SERVICE_BUILDERS.get(service_type)
            if builder is None:
                raise ValueError(f"Unknown service type: {service_type!r}")

            dependencies = {dep: built[dep] for dep in deps}
            built[name] = builder(settings, definition.get("state", {}), dependencies)
            del pending[name]
            progress = True

        if not progress:
            unresolved = ", ".join(sorted(pending))
            raise ValueError(f"Unable to resolve service dependencies: {unresolved}")

    return ServiceBundle(
        settings=settings,
        order_store=built["order_store"],
        order_service=built["order_service"],
    )

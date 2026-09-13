"""TaskFlow DI — resolve services from an encoded bundle based on function signatures."""

from __future__ import annotations

import functools
import inspect
import logging
from typing import Any, Callable, TypeVar

from pipeline_web.container import ServiceBundle
from pipeline_web.services import InMemoryOrderStore, OrderService
from pipeline_web.settings import Settings
from pipeline_web.xcom_serde.codec import decode_service_bundle

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

_TYPE_TO_BUNDLE_ATTR: dict[type[Any], str] = {
    Settings: "settings",
    InMemoryOrderStore: "order_store",
    OrderService: "order_service",
}


class Inject:
    """Sentinel default marking a parameter for XCom bundle injection."""


INJECT = Inject()


def _resolve_from_bundle(bundle: ServiceBundle, annotation: Any) -> Any | None:
    if annotation is inspect.Parameter.empty:
        return None
    attr = _TYPE_TO_BUNDLE_ATTR.get(annotation)
    if attr is None:
        return None
    return getattr(bundle, attr)


def inject_services(task_fn: F) -> F:
    """Decorator: decode `service_bundle` XCom payload and inject typed dependencies."""

    signature = inspect.signature(task_fn)

    @functools.wraps(task_fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        bound = signature.bind_partial(*args, **kwargs)
        encoded_bundle = bound.arguments.get("service_bundle")
        if encoded_bundle is None:
            raise ValueError(
                f"{task_fn.__name__} requires a `service_bundle` parameter "
                "carrying the encoded ServiceBundle XCom payload."
            )

        bundle = decode_service_bundle(encoded_bundle)
        injected: list[str] = []

        for name, parameter in signature.parameters.items():
            if name == "service_bundle":
                continue
            if bound.arguments.get(name, INJECT) is not INJECT:
                continue
            if name in kwargs and kwargs[name] is not INJECT:
                continue

            service = _resolve_from_bundle(bundle, parameter.annotation)
            if service is not None:
                kwargs[name] = service
                injected.append(name)

        logger.info(
            "DI inject into %s: %s (codec round-trip via XCom)",
            task_fn.__name__,
            ", ".join(injected) or "(none)",
        )
        return task_fn(*args, **kwargs)

    return wrapper  # type: ignore[return-value]

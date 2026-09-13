"""Signature-based dependency injection for TaskFlow handlers.

Tasks declare collaborators in their function signature (like FastAPI).
The decorator resolves the service container from task inputs, deserializes
services via the custom XCom codec, and injects only what each handler needs.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar, get_type_hints

from pipeline_web.container import ServiceContainer
from pipeline_web.services.order_service import OrderService
from pipeline_web.xcom_serde.codec import decode_container

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])

_SERVICE_REGISTRY: dict[type, str] = {
    OrderService: "order_service",
}


def _resolve_container(args: tuple[Any, ...], kwargs: dict[str, Any], signature: inspect.Signature) -> ServiceContainer:
    """Find a ServiceContainer (or encoded token) in bound task arguments."""
    bound = signature.bind_partial(*args, **kwargs)
    bound.apply_defaults()

    for key in ("container", "container_token"):
        if key in bound.arguments and bound.arguments[key] is not None:
            value = bound.arguments[key]
            if isinstance(value, ServiceContainer):
                return value
            return decode_container(value)

    for value in bound.arguments.values():
        if isinstance(value, ServiceContainer):
            return value
        if isinstance(value, dict) and "container" in value:
            nested = value["container"]
            if isinstance(nested, ServiceContainer):
                return nested
            if isinstance(nested, dict):
                return decode_container(nested)

    raise ValueError("No service container found in task arguments")


def _resolve_services(container: ServiceContainer, fn: Callable[..., Any]) -> dict[str, Any]:
    hints = get_type_hints(fn)
    injected: dict[str, Any] = {}
    for name, annotation in hints.items():
        if name == "return":
            continue
        attr = _SERVICE_REGISTRY.get(annotation)
        if attr is None:
            continue
        injected[name] = getattr(container, attr)
    return injected


def inject_services(fn: F) -> F:
    """Decorator: inject declared services from an XCom-deserialized container."""

    original_signature = inspect.signature(fn)

    @wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        container = _resolve_container(args, kwargs, original_signature)
        services = _resolve_services(container, fn)
        logger.info(
            "Injected services into %s: %s",
            fn.__name__,
            sorted(services.keys()),
        )

        bound = original_signature.bind_partial(*args, **kwargs)
        bound.apply_defaults()
        call_kwargs = dict(bound.arguments)
        call_kwargs.update(services)
        for key in list(call_kwargs):
            if key in services:
                continue
            hints = get_type_hints(fn)
            if hints.get(key) in _SERVICE_REGISTRY:
                call_kwargs.pop(key, None)
        return fn(**call_kwargs)

    wrapper.__signature__ = _external_signature(fn)  # type: ignore[attr-defined]
    wrapper.__wrapped__ = fn  # type: ignore[attr-defined]
    return wrapper  # type: ignore[return-value]


def _external_signature(fn: Callable[..., Any]) -> inspect.Signature:
    """TaskFlow-visible signature: data params + optional container, no injected services."""
    hints = get_type_hints(fn)
    params: list[inspect.Parameter] = []
    for name, param in inspect.signature(fn).parameters.items():
        if hints.get(name) in _SERVICE_REGISTRY:
            continue
        params.append(param.replace(kind=inspect.Parameter.POSITIONAL_OR_KEYWORD))
    return inspect.Signature(params, return_annotation=inspect.signature(fn).return_annotation)

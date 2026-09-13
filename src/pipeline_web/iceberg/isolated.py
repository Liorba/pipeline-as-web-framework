"""Run Iceberg writes in an isolated Python when Airflow shares the interpreter.

Airflow 2.10 pins SQLAlchemy 1.4; pyiceberg's SqlCatalog requires SQLAlchemy 2.x.
The Docker stack sets ``PIPELINE_ICEBERG_ISOLATED_PYTHON`` to a dedicated venv
interpreter so ``publish_iceberg`` can still demonstrate DI while writes succeed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from pipeline_web.services.order_store import Order
from pipeline_web.settings import Settings


def isolated_python() -> str | None:
    return os.environ.get("PIPELINE_ICEBERG_ISOLATED_PYTHON")


def append_orders_isolated(settings: Settings, orders: list[Order]) -> dict[str, object]:
    """Append orders via a subprocess using the configured isolated interpreter."""
    interpreter = isolated_python()
    if not interpreter:
        raise RuntimeError("PIPELINE_ICEBERG_ISOLATED_PYTHON is not set")

    payload = {
        "settings": settings.model_dump(),
        "orders": [order.__dict__ for order in orders],
    }
    repo_src = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_src)

    proc = subprocess.run(
        [interpreter, "-m", "pipeline_web.iceberg.isolated_runner", "append"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Iceberg isolated append failed (exit {proc.returncode}): {proc.stderr or proc.stdout}"
        )
    return json.loads(proc.stdout)


def append_bad_orders_isolated(settings: Settings) -> dict[str, object]:
    interpreter = isolated_python()
    if not interpreter:
        raise RuntimeError("PIPELINE_ICEBERG_ISOLATED_PYTHON is not set")

    payload = {"settings": settings.model_dump()}
    repo_src = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_src)

    proc = subprocess.run(
        [interpreter, "-m", "pipeline_web.iceberg.isolated_runner", "append-bad"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Iceberg isolated bad append failed (exit {proc.returncode}): {proc.stderr or proc.stdout}"
        )
    return json.loads(proc.stdout)


def rollback_isolated(settings: Settings, snapshot_id: int) -> dict[str, object]:
    interpreter = isolated_python()
    if not interpreter:
        raise RuntimeError("PIPELINE_ICEBERG_ISOLATED_PYTHON is not set")

    payload = {"settings": settings.model_dump(), "snapshot_id": snapshot_id}
    repo_src = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_src)

    proc = subprocess.run(
        [interpreter, "-m", "pipeline_web.iceberg.isolated_runner", "rollback"],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Iceberg isolated rollback failed (exit {proc.returncode}): {proc.stderr or proc.stdout}"
        )
    return json.loads(proc.stdout)


def should_use_isolated() -> bool:
    return bool(isolated_python()) and isolated_python() != sys.executable

"""Shared fixtures: the strict mock DataHub and a client that launches the worker."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.request
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from vgi.client import Client

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def client() -> Iterator[Client]:
    """A client that launches the worker as a subprocess, the way DuckDB does."""
    with Client([sys.executable, "-m", "datahub_worker.worker"]) as c:
        yield c


def _start_mock(env: dict[str, str]) -> tuple[subprocess.Popen[str], str]:
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_datahub" / "server.py")],
        stdout=subprocess.PIPE,
        text=True,
        env={**os.environ, **env},
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    return proc, f"http://127.0.0.1:{line.removeprefix('PORT:')}"


@pytest.fixture(scope="session")
def mock_datahub() -> Iterator[str]:
    """The strict mock DataHub (rate-limits every third distinct request once); returns its URL."""
    proc, url = _start_mock({"MOCK_FLAKY": "1"})
    yield url
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture
def requests_log(mock_datahub: str) -> Callable[[], list[dict[str, Any]]]:
    """Clear the mock's request log; the returned callable reads it."""
    urllib.request.urlopen(urllib.request.Request(f"{mock_datahub}/__reset", data=b"", method="POST")).read()

    def read() -> list[dict[str, Any]]:
        with urllib.request.urlopen(f"{mock_datahub}/__requests") as r:
            return list(json.load(r))

    return read

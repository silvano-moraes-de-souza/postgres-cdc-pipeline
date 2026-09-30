"""Tests run against a real PostgreSQL with wal_level=logical (see bench/pg.py)."""

from __future__ import annotations

from pathlib import Path

import pytest
from shopflow_datagen import GenConfig, write

from bench.pg import NoServerError, database, server_url


@pytest.fixture(scope="session")
def server() -> str:
    try:
        return server_url()[0]
    except NoServerError:
        pytest.skip("no embedded PostgreSQL for this Python version; set TEST_DATABASE_URL")


@pytest.fixture(scope="session")
def source_url(server) -> str:
    return database(server, "cdc_source_test")


@pytest.fixture(scope="session")
def target_url(server) -> str:
    return database(server, "cdc_target_test")


@pytest.fixture(scope="session")
def data(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("shopflow")
    write(GenConfig(scale=0.02, chunk_size=1_000), out)
    return out

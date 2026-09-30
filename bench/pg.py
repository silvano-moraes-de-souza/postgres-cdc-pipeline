"""A PostgreSQL server for tests and benchmarks, with wal_level=logical.

Uses TEST_DATABASE_URL when set (CI starts postgres with -c wal_level=logical).
Otherwise starts an embedded PostgreSQL 16 with pgserver and switches it to
logical WAL, so no Docker is needed on a laptop.
"""

from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

import psycopg


class NoServerError(RuntimeError):
    """No TEST_DATABASE_URL and pgserver is not installed for this Python."""


def embedded_logical_server() -> str:
    try:
        import pgserver  # noqa: PLC0415 - dev dependency, not on every Python
    except ImportError as exc:
        raise NoServerError("set TEST_DATABASE_URL or install pgserver") from exc
    pgdata = tempfile.mkdtemp(prefix="cdc-pg-")
    server = pgserver.get_server(pgdata, cleanup_mode="stop")
    with psycopg.connect(server.get_uri(), autocommit=True) as c:
        c.execute("ALTER SYSTEM SET wal_level = logical")
    pg_ctl = Path(pgserver.__file__).parent / "pginstall" / "bin" / "pg_ctl"
    # No pipes: the restarted postmaster would inherit them and never close them.
    subprocess.run(
        [str(pg_ctl), "-D", pgdata, "stop", "-w", "-m", "fast"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        timeout=60, check=True,
    )  # fmt: skip
    server.ensure_postgres_running()
    return server.get_uri()


def server_url() -> tuple[str, str]:
    """Return (url, description of where it runs)."""
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        return url, "provided PostgreSQL"
    return embedded_logical_server(), "embedded PostgreSQL 16 (pgserver), same machine"


def database(server: str, name: str) -> str:
    """Create database ``name`` if needed and return its URL."""
    with psycopg.connect(server, autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            c.execute(f"CREATE DATABASE {name}")
    return server.rsplit("/", 1)[0] + "/" + name

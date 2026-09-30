"""Connections, schema setup and the initial load of the source database.

Source and target are separate databases on purpose: a capture method only gets
what it can read through a connection, the same as against a production system.
"""

from __future__ import annotations

import io
from importlib import resources
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

SLOT = "cdc_slot"
PUBLICATION = "cdc_pub"

ORDER_COLUMNS = ["order_id", "customer_id", "ordered_at", "status", "channel", "total_cents",
                 "delivered_at"]  # fmt: skip
PAYMENT_COLUMNS = ["payment_id", "order_id", "method", "status", "amount_cents", "paid_at"]
COLUMNS = {"orders": ORDER_COLUMNS, "payments": PAYMENT_COLUMNS}
KEYS = {"orders": "order_id", "payments": "payment_id"}


def sql_file(name: str) -> str:
    return resources.files("postgres_cdc_pipeline").joinpath("sql", name).read_text("utf-8")


def connect(url: str, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(url, autocommit=autocommit)


def ensure_logical(conn: psycopg.Connection) -> None:
    level = conn.execute("SHOW wal_level").fetchone()[0]
    if level != "logical":
        raise RuntimeError(
            f"wal_level is {level!r}; WAL capture needs wal_level=logical on the source server"
        )


def reset_source(conn: psycopg.Connection) -> None:
    """Drop everything this project creates in the source database."""
    conn.execute(
        "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
        "WHERE slot_name = %s AND database = current_database()",
        (SLOT,),
    )
    conn.execute(f"DROP PUBLICATION IF EXISTS {PUBLICATION}")
    conn.execute("DROP SCHEMA IF EXISTS cdc CASCADE")
    conn.execute("DROP SCHEMA IF EXISTS shop CASCADE")
    conn.commit()


def reset_target(conn: psycopg.Connection) -> None:
    conn.execute("DROP SCHEMA IF EXISTS rep CASCADE")
    conn.execute(sql_file("target.sql"))
    conn.commit()


def _copy_parquet(conn: psycopg.Connection, folder: Path, table: str) -> int:
    columns = COLUMNS[table]
    rows = 0
    stmt = f"COPY shop.{table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv)"
    with conn.cursor().copy(stmt) as copy:
        for part in sorted(folder.glob("part-*.parquet")):
            for batch in pq.ParquetFile(part).iter_batches(batch_size=100_000, columns=columns):
                buf = io.BytesIO()
                pacsv.write_csv(pa.Table.from_batches([batch]), buf,
                                pacsv.WriteOptions(include_header=False))  # fmt: skip
                copy.write(buf.getvalue())
                rows += batch.num_rows
    return rows


def load_source(conn: psycopg.Connection, data_dir: Path) -> dict[str, int]:
    """Create the source schema and bulk load orders and payments from ShopFlow Parquet."""
    reset_source(conn)
    conn.execute(sql_file("source.sql"))
    counts = {t: _copy_parquet(conn, data_dir / t, t) for t in COLUMNS}
    conn.execute(sql_file("source_indexes.sql"))
    conn.commit()
    conn.execute("ANALYZE shop.orders")
    conn.execute("ANALYZE shop.payments")
    conn.commit()
    return counts

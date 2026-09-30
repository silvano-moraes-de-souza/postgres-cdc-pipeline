"""Apply captured changes to the target, plus the full-reload baseline.

Every capture method produces the same ``Change`` records, so the target side
is shared and the comparison between methods is only about capture.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .db import COLUMNS, KEYS


@dataclass
class Change:
    table: str  # "orders" or "payments"
    op: str  # "U" upsert (insert or update) or "D" delete
    pk: int
    row: dict[str, Any] | None  # full row for upserts, None for deletes
    ts: str  # when it happened: row updated_at, or commit time for deletes


def _upsert_sql(table: str) -> str:
    cols = [*COLUMNS[table], "updated_at"]
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != KEYS[table])
    # The version check makes replays harmless: an older row never overwrites a newer one.
    return (
        f"INSERT INTO rep.{table} AS t SELECT * FROM jsonb_populate_record(NULL::rep.{table}, %s) "
        f"ON CONFLICT ({KEYS[table]}) DO UPDATE SET {sets} "
        f"WHERE t.updated_at <= EXCLUDED.updated_at"
    )


UPSERT = {t: _upsert_sql(t) for t in COLUMNS}
DELETE = {t: f"DELETE FROM rep.{t} WHERE {KEYS[t]} = %s" for t in COLUMNS}


def _apply_rows(cur: psycopg.Cursor, changes: list[Change]) -> None:
    """Run consecutive changes of the same table and kind as one executemany."""
    i = 0
    while i < len(changes):
        key = (changes[i].table, changes[i].op)
        j = i
        while j < len(changes) and (changes[j].table, changes[j].op) == key:
            j += 1
        batch = changes[i:j]
        if key[1] == "D":
            cur.executemany(DELETE[key[0]], [(c.pk,) for c in batch])
        else:
            cur.executemany(UPSERT[key[0]], [(Jsonb(c.row),) for c in batch])
        i = j


def _apply_history(cur: psycopg.Cursor, changes: list[Change]) -> None:
    """Turn the batch's order events into SCD2 periods, in event order."""
    events = [c for c in changes if c.table == "orders"]
    if not events:
        return
    ids = sorted({c.pk for c in events})
    cur.execute(
        "SELECT order_id, status FROM rep.order_status_history "
        "WHERE valid_to IS NULL AND order_id = ANY(%s)",
        (ids,),
    )
    current: dict[int, str | None] = dict(cur.fetchall())
    in_db = set(current)  # orders whose open period already exists in the table
    closes: list[tuple[str, int]] = []
    periods: dict[int, list[list[Any]]] = {}  # order -> [status, valid_from, valid_to]

    def close(oid: int, ts: str) -> None:
        if periods.get(oid):
            periods[oid][-1][2] = ts
        elif oid in in_db:
            closes.append((ts, oid))
            in_db.discard(oid)
        current[oid] = None

    for c in events:
        status = c.row["status"] if c.row else None
        if c.op == "D":
            if current.get(c.pk) is not None:
                close(c.pk, c.ts)
            continue
        if current.get(c.pk) == status:
            continue
        if current.get(c.pk) is not None:
            close(c.pk, c.ts)
        periods.setdefault(c.pk, []).append([status, c.ts, None])
        current[c.pk] = status

    cur.executemany(
        "UPDATE rep.order_status_history SET valid_to = %s::timestamptz "
        "WHERE order_id = %s AND valid_to IS NULL",
        closes,
    )
    rows = [(oid, st, vf, vt) for oid, ps in periods.items() for st, vf, vt in ps]
    cur.executemany(
        "INSERT INTO rep.order_status_history (order_id, status, valid_from, valid_to) "
        "VALUES (%s, %s, %s::timestamptz, %s::timestamptz)",
        rows,
    )


def apply(tgt: psycopg.Connection, changes: list[Change], consumer: str, position: str) -> None:
    """Apply one batch and record the consumer position in the same transaction."""
    with tgt.cursor() as cur:
        _apply_rows(cur, changes)
        _apply_history(cur, changes)
        cur.execute(
            "INSERT INTO rep.cdc_state (consumer, position) VALUES (%s, %s) "
            "ON CONFLICT (consumer) DO UPDATE SET position = EXCLUDED.position, updated_at = now()",
            (consumer, position),
        )
    tgt.commit()


def position(tgt: psycopg.Connection, consumer: str) -> str | None:
    row = tgt.execute("SELECT position FROM rep.cdc_state WHERE consumer = %s", (consumer,))
    found = row.fetchone()
    return found[0] if found else None


def full_reload(src: psycopg.Connection, tgt: psycopg.Connection) -> dict[str, int]:
    """Truncate the target copy and stream both tables over again with COPY."""
    counts = {}
    tgt.execute("TRUNCATE rep.orders, rep.payments")
    for table in COLUMNS:
        cols = ", ".join([*COLUMNS[table], "updated_at"])
        n = 0
        with (
            src.cursor().copy(f"COPY (SELECT {cols} FROM shop.{table}) TO STDOUT") as out,
            tgt.cursor().copy(f"COPY rep.{table} ({cols}) FROM STDIN") as inp,
        ):
            for data in out:
                inp.write(data)
                n += bytes(data).count(b"\n")
        counts[table] = n
    src.commit()
    tgt.commit()
    return counts


def snapshot(src: psycopg.Connection, tgt: psycopg.Connection) -> dict[str, int]:
    """Initial copy: full reload plus the first open period of every order."""
    counts = full_reload(src, tgt)
    tgt.execute("TRUNCATE rep.order_status_history, rep.cdc_state")
    tgt.execute(
        "INSERT INTO rep.order_status_history (order_id, status, valid_from) "
        "SELECT order_id, status, updated_at FROM rep.orders"
    )
    tgt.commit()
    return counts

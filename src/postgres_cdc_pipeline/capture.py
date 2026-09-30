"""Four ways to find out what changed in the source since the last run.

watermark      SELECT rows WHERE updated_at > last value seen. No setup on the
               source beyond an index. Cannot see deletes; sees only the latest
               state of a row; skips rows committed late with an older timestamp.
trigger_seq    Triggers write every change to cdc.change_log; the reader keeps
               the highest seq it has read. Sees deletes and every state, but a
               sequence value is taken at write time, not commit time, so a slow
               transaction still lands behind the reader's position.
trigger_queue  Same log, read as a queue: DELETE ... RETURNING takes every
               committed entry, whatever its seq. Rows of an open transaction
               are invisible to the DELETE and are taken on a later run.
wal            Logical decoding of the write-ahead log with pgoutput, the plugin
               behind native logical replication. Transactions come in commit
               order, deletes included, with no triggers on the tables.

Each capture exposes ``start`` (after the initial snapshot), ``poll`` (returns
changes and the new position) and ``ack`` (after the target committed).
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol

import psycopg

from .apply import Change
from .db import KEYS, PUBLICATION, SLOT, ensure_logical, sql_file
from .pgoutput import Decoder, format_lsn, parse_lsn


class Capture(Protocol):
    name: str

    def install(self, src: psycopg.Connection) -> None: ...
    def start(self, src: psycopg.Connection) -> str: ...
    def poll(self, src: psycopg.Connection, position: str) -> tuple[list[Change], str]: ...
    def ack(self, src: psycopg.Connection, position: str) -> None: ...


class Watermark:
    name = "watermark"

    def install(self, src: psycopg.Connection) -> None:
        pass  # the updated_at indexes are part of the source schema

    def start(self, src: psycopg.Connection) -> str:
        row = src.execute(
            "SELECT greatest((SELECT max(updated_at) FROM shop.orders), "
            "(SELECT max(updated_at) FROM shop.payments))"
        ).fetchone()
        src.commit()
        return row[0].isoformat()

    def poll(self, src: psycopg.Connection, position: str) -> tuple[list[Change], str]:
        changes: list[Change] = []
        since = newest = datetime.fromisoformat(position)
        for table in KEYS:
            rows = src.execute(
                f"SELECT to_jsonb(t), t.updated_at FROM shop.{table} t "
                f"WHERE t.updated_at > %s ORDER BY t.updated_at",
                (since,),
            ).fetchall()
            for data, _ in rows:
                changes.append(Change(table, "U", data[KEYS[table]], data, data["updated_at"]))
            if rows:
                newest = max(newest, rows[-1][1])
        src.commit()
        return changes, newest.isoformat()

    def ack(self, src: psycopg.Connection, position: str) -> None:
        pass


class _TriggerLog:
    def install(self, src: psycopg.Connection) -> None:
        src.execute(sql_file("trigger_capture.sql"))
        src.commit()

    def start(self, src: psycopg.Connection) -> str:
        return "0"

    @staticmethod
    def _to_changes(rows) -> list[Change]:
        changes = []
        for _seq, tbl, op, pk, data, ts in sorted(rows):
            ts_row = data["updated_at"] if data else ts
            changes.append(Change(tbl, "D" if op == "D" else "U", pk, data, ts_row))
        return changes


COLS = "seq, tbl, op, pk, row_data, changed_at::text"


class TriggerSeq(_TriggerLog):
    name = "trigger_seq"

    def poll(self, src: psycopg.Connection, position: str) -> tuple[list[Change], str]:
        rows = src.execute(
            f"SELECT {COLS} FROM cdc.change_log WHERE seq > %s ORDER BY seq", (int(position),)
        ).fetchall()
        src.commit()
        return self._to_changes(rows), str(rows[-1][0]) if rows else position

    def ack(self, src: psycopg.Connection, position: str) -> None:
        pass


class TriggerQueue(_TriggerLog):
    name = "trigger_queue"

    def poll(self, src: psycopg.Connection, position: str) -> tuple[list[Change], str]:
        # The DELETE stays uncommitted until ack(): if the target fails, the
        # source transaction rolls back and the entries are read again.
        rows = src.execute(f"DELETE FROM cdc.change_log RETURNING {COLS}").fetchall()
        newest = max((r[0] for r in rows), default=int(position))
        return self._to_changes(rows), str(newest)

    def ack(self, src: psycopg.Connection, position: str) -> None:
        src.commit()


class Wal:
    name = "wal"

    def __init__(self, log_snapshot: bool = True) -> None:
        self.log_snapshot = log_snapshot

    def install(self, src: psycopg.Connection) -> None:
        ensure_logical(src)
        src.execute(f"CREATE PUBLICATION {PUBLICATION} FOR TABLE shop.orders, shop.payments")
        src.commit()
        # The slot must exist before the snapshot, so no change falls in between.
        src.execute("SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (SLOT,))
        src.commit()

    def start(self, src: psycopg.Connection) -> str:
        return "0/0"

    def poll(self, src: psycopg.Connection, position: str) -> tuple[list[Change], str]:
        rows = src.execute(
            "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes("
            "%s, NULL, NULL, 'proto_version', '1', 'publication_names', %s)",
            (SLOT, PUBLICATION),
        ).fetchall()
        src.commit()
        done = parse_lsn(position)
        decoder = Decoder()
        changes: list[Change] = []
        last = done
        for lsn, data in rows:
            tx = decoder.feed(data)
            if tx is None or parse_lsn(lsn) <= done:
                continue  # not a commit, or a transaction the target already has
            last = parse_lsn(lsn)
            for rc in tx.changes:
                key = KEYS[rc.table]
                if rc.op == "D":
                    changes.append(Change(rc.table, "D", int(rc.old[key]), None,
                                          tx.commit_ts.isoformat()))  # fmt: skip
                else:
                    changes.append(Change(rc.table, "U", int(rc.new[key]), rc.new,
                                          rc.new["updated_at"]))  # fmt: skip
        return changes, format_lsn(last)

    def ack(self, src: psycopg.Connection, position: str) -> None:
        # Only now may the server recycle the WAL behind this position.
        if parse_lsn(position) > 0:
            src.execute("SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (SLOT, position))
            src.commit()
        if self.log_snapshot:
            # restart_lsn only moves when decoding passes a running-transactions
            # record. Postgres writes one every 15 s; a sync that runs more often
            # than that re-reads all WAL since the slot was created, every time.
            try:
                src.execute("SELECT pg_log_standby_snapshot()")
                src.commit()
            except psycopg.Error:
                # Before PostgreSQL 16, or without the privilege: fall back to
                # the server's own 15 s interval.
                src.rollback()
                self.log_snapshot = False


METHODS: dict[str, type] = {
    "watermark": Watermark,
    "trigger_seq": TriggerSeq,
    "trigger_queue": TriggerQueue,
    "wal": Wal,
}

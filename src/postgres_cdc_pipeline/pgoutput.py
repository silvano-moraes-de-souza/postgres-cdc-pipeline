"""Decoder for PostgreSQL's built-in logical replication output (pgoutput, protocol 1).

pg_logical_slot_peek_binary_changes() returns one message per row. The ones this
pipeline needs:

    B  Begin     final LSN, commit timestamp, xid
    R  Relation  relation id, schema, table, columns (sent before first use)
    I  Insert    relation id, new tuple
    U  Update    relation id, optional old/key tuple, new tuple
    D  Delete    relation id, old or key tuple
    C  Commit    flags, commit LSN, end LSN, timestamp

Tuple values arrive in text format. Type (Y), origin (O), truncate (T) and
logical message (M) are skipped. Reference:
https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

PG_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)


@dataclass
class Relation:
    schema: str
    table: str
    columns: list[str]
    key_columns: list[str]


@dataclass
class RowChange:
    table: str
    op: str  # "I", "U" or "D"
    new: dict[str, str | None] | None
    old: dict[str, str | None] | None


@dataclass
class Transaction:
    xid: int
    commit_lsn: int = 0
    commit_ts: datetime | None = None
    changes: list[RowChange] = field(default_factory=list)


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.pos = 0

    def take(self, fmt: str) -> int:
        (value,) = struct.unpack_from(fmt, self.data, self.pos)
        self.pos += struct.calcsize(fmt)
        return value

    def byte(self) -> str:
        value = chr(self.data[self.pos])
        self.pos += 1
        return value

    def string(self) -> str:
        end = self.data.index(b"\x00", self.pos)
        value = self.data[self.pos : end].decode("utf-8")
        self.pos = end + 1
        return value

    def raw(self, n: int) -> bytes:
        value = self.data[self.pos : self.pos + n]
        self.pos += n
        return value


def _ts(micros: int) -> datetime:
    return PG_EPOCH + timedelta(microseconds=micros)


def _tuple(r: _Reader, columns: list[str]) -> dict[str, str | None]:
    n = r.take("!h")
    row: dict[str, str | None] = {}
    for name in columns[:n]:
        kind = r.byte()
        if kind == "n":
            row[name] = None
        elif kind == "u":  # unchanged TOASTed value: not sent, leave it out
            continue
        elif kind == "t":
            row[name] = r.raw(r.take("!i")).decode("utf-8")
        else:
            raise ValueError(f"unsupported tuple value kind {kind!r}")
    return row


class Decoder:
    """Stateful: relation messages are cached and reused by later changes."""

    def __init__(self) -> None:
        self.relations: dict[int, Relation] = {}
        self._open: Transaction | None = None

    def feed(self, data: bytes) -> Transaction | None:
        """Consume one message. Returns a transaction when its Commit arrives."""
        r = _Reader(bytes(data))
        kind = r.byte()
        if kind == "B":
            r.take("!q")  # final LSN of the transaction
            r.take("!q")  # commit timestamp, also in the Commit message
            self._open = Transaction(xid=r.take("!I"))
        elif kind == "C":
            r.take("!b")
            tx = self._require_open()
            tx.commit_lsn = r.take("!q")
            r.take("!q")  # end LSN
            tx.commit_ts = _ts(r.take("!q"))
            self._open = None
            return tx
        elif kind == "R":
            rel_id = r.take("!I")
            schema, table = r.string(), r.string()
            r.byte()  # replica identity setting
            columns, keys = [], []
            for _ in range(r.take("!h")):
                flags = r.take("!b")
                name = r.string()
                r.take("!I")  # type oid
                r.take("!i")  # type modifier
                columns.append(name)
                if flags & 1:
                    keys.append(name)
            self.relations[rel_id] = Relation(schema, table, columns, keys)
        elif kind in "IUD":
            self._require_open().changes.append(self._change(kind, r))
        # Y (type), O (origin), T (truncate), M (message): not needed here.
        return None

    def _change(self, kind: str, r: _Reader) -> RowChange:
        rel = self.relations[r.take("!I")]
        old = new = None
        marker = r.byte()
        if kind == "U" and marker in "KO":
            old = _tuple(r, rel.columns)
            marker = r.byte()
        if kind == "D":
            old = _tuple(r, rel.columns)
        else:
            if marker != "N":
                raise ValueError(f"expected new tuple, got {marker!r}")
            new = _tuple(r, rel.columns)
        return RowChange(rel.table, kind, new, old)

    def _require_open(self) -> Transaction:
        if self._open is None:
            raise ValueError("change or commit outside a transaction")
        return self._open


def parse_lsn(lsn: str) -> int:
    hi, lo = lsn.split("/")
    return (int(hi, 16) << 32) + int(lo, 16)


def format_lsn(value: int) -> str:
    return f"{value >> 32:X}/{value & 0xFFFFFFFF:X}"

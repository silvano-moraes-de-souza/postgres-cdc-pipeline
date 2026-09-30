import struct
from datetime import UTC, datetime

import pytest

from postgres_cdc_pipeline.pgoutput import Decoder, format_lsn, parse_lsn


def _s(text: str) -> bytes:
    return text.encode() + b"\x00"


def _tuple(*values: str | None) -> bytes:
    out = struct.pack("!h", len(values))
    for v in values:
        if v is None:
            out += b"n"
        else:
            raw = v.encode()
            out += b"t" + struct.pack("!i", len(raw)) + raw
    return out


REL = 16400
BEGIN = b"B" + struct.pack("!qqI", 100, 0, 7)
RELATION = (
    b"R" + struct.pack("!I", REL) + _s("shop") + _s("orders") + b"d" + struct.pack("!h", 2)
    + b"\x01" + _s("order_id") + struct.pack("!Ii", 20, -1)
    + b"\x00" + _s("status") + struct.pack("!Ii", 25, -1)
)  # fmt: skip
COMMIT_TS = 1_000_000  # one second after 2000-01-01
COMMIT = b"C" + struct.pack("!bqqq", 0, 100, 120, COMMIT_TS)


def test_decodes_a_transaction_with_insert_update_and_delete():
    d = Decoder()
    assert d.feed(BEGIN) is None
    d.feed(RELATION)
    d.feed(b"I" + struct.pack("!I", REL) + b"N" + _tuple("1", "pending"))
    d.feed(b"U" + struct.pack("!I", REL) + b"K" + _tuple("1", None) + b"N" + _tuple("1", "shipped"))
    d.feed(b"D" + struct.pack("!I", REL) + b"K" + _tuple("1", None))
    tx = d.feed(COMMIT)

    assert tx.xid == 7
    assert tx.commit_lsn == 100
    assert tx.commit_ts == datetime(2000, 1, 1, 0, 0, 1, tzinfo=UTC)
    assert [c.op for c in tx.changes] == ["I", "U", "D"]
    assert tx.changes[0].new == {"order_id": "1", "status": "pending"}
    assert tx.changes[1].old == {"order_id": "1", "status": None}
    assert tx.changes[1].new["status"] == "shipped"
    assert tx.changes[2].old["order_id"] == "1"
    assert d.relations[REL].key_columns == ["order_id"]


def test_relation_is_reused_across_transactions():
    d = Decoder()
    d.feed(BEGIN)
    d.feed(RELATION)
    d.feed(COMMIT)
    d.feed(BEGIN)
    d.feed(b"I" + struct.pack("!I", REL) + b"N" + _tuple("2", "pending"))
    assert d.feed(COMMIT).changes[0].table == "orders"


def test_skips_messages_it_does_not_need():
    d = Decoder()
    d.feed(BEGIN)
    assert d.feed(b"Y" + b"anything") is None
    assert d.feed(COMMIT).changes == []


def test_change_outside_a_transaction_is_an_error():
    d = Decoder()
    d.feed(RELATION)
    with pytest.raises(ValueError, match="outside a transaction"):
        d.feed(b"I" + struct.pack("!I", REL) + b"N" + _tuple("1", "x"))


@pytest.mark.parametrize("lsn", ["0/0", "0/14AA488", "1/FF", "16/B374D848"])
def test_lsn_round_trip(lsn):
    assert format_lsn(parse_lsn(lsn)) == lsn

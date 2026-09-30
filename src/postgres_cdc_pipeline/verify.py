"""Compare the target copy with the source, row by row.

missing  in the source, not in the target
stale    in both, with different values
ghost    in the target, deleted from the source
"""

from __future__ import annotations

import psycopg

from .db import COLUMNS, KEYS


def _fingerprints(conn: psycopg.Connection, schema: str, table: str) -> dict[int, str]:
    cols = ", ".join([*COLUMNS[table], "updated_at"])
    rows = conn.execute(f"SELECT {KEYS[table]}, md5(row({cols})::text) FROM {schema}.{table}")
    result = dict(rows.fetchall())
    conn.commit()
    return result


def diff(src: psycopg.Connection, tgt: psycopg.Connection) -> dict[str, dict[str, int]]:
    report = {}
    for table in COLUMNS:
        a = _fingerprints(src, "shop", table)
        b = _fingerprints(tgt, "rep", table)
        report[table] = {
            "source_rows": len(a),
            "missing": sum(1 for k in a if k not in b),
            "stale": sum(1 for k, v in a.items() if k in b and b[k] != v),
            "ghost": sum(1 for k in b if k not in a),
        }
    return report


def history_rows(tgt: psycopg.Connection) -> int:
    n = tgt.execute("SELECT count(*) FROM rep.order_status_history").fetchone()[0]
    tgt.commit()
    return n

"""Command line.

    cdc-pipeline generate --scale 0.1 --out data/sf0.1
    cdc-pipeline run --method wal --data data/sf0.1 --cycles 24
    cdc-pipeline compare --data data/sf0.1 --cycles 24

Connection strings come from SOURCE_URL and TARGET_URL (or --source/--target).
The source server needs wal_level=logical for the wal method.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .capture import METHODS
from .runner import run
from .simulate import Rates


def _table(results: list[dict]) -> str:
    head = (
        "| method | rows wrong | missing | stale | ghost | history recorded / true | median cycle |"
    )
    lines = [head, "|---|---:|---:|---:|---:|---:|---:|"]
    for r in results:
        d = {k: sum(t[k] for t in r["diff"].values()) for k in ("missing", "stale", "ghost")}
        h = r["history"]
        lines.append(
            f"| {r['method']} | {r['rows_wrong']} | {d['missing']} | {d['stale']} | {d['ghost']} "
            f"| {h['recorded']:,} / {h['true']:,} | {r['cycle_s']['median'] * 1000:.0f} ms |"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="cdc-pipeline", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)  # fmt: skip
    sub = p.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="write ShopFlow Parquet data")
    g.add_argument("--scale", type=float, default=0.1)
    g.add_argument("--out", type=Path, required=True)

    for name in ("run", "compare"):
        s = sub.add_parser(name)
        if name == "run":
            s.add_argument("--method", choices=sorted(METHODS), required=True)
        s.add_argument("--data", type=Path, required=True)
        s.add_argument("--cycles", type=int, default=24)
        s.add_argument("--seed", type=int, default=42)
        s.add_argument("--late-rate", type=float, default=Rates.late)
        s.add_argument("--source", default=os.environ.get("SOURCE_URL"))
        s.add_argument("--target", default=os.environ.get("TARGET_URL"))
        s.add_argument("--json", type=Path, help="also write the full results here")

    a = p.parse_args(argv)
    if a.cmd == "generate":
        from shopflow_datagen import GenConfig, write  # noqa: PLC0415

        write(GenConfig(scale=a.scale), a.out)
        print(a.out)
        return
    if not a.source or not a.target:
        p.error("set SOURCE_URL and TARGET_URL, or pass --source and --target")

    rates = Rates(late=a.late_rate)
    methods = [a.method] if a.cmd == "run" else list(METHODS)
    results = [
        run(m, a.source, a.target, a.data, cycles=a.cycles, seed=a.seed, rates=rates)
        for m in methods
    ]
    print(_table(results))
    if a.json:
        a.json.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()

"""Accuracy, cost and source overhead of the four capture methods.

    uv run python -m bench.run

Writes results/*.json and the charts in docs/assets/. Data generation and the
initial load are not part of any timing.
"""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import psycopg
from shopflow_datagen import GenConfig, write

from bench.harness import CaseResult, measure, save
from bench.pg import database, server_url
from bench.plot import plot
from postgres_cdc_pipeline import apply as ap
from postgres_cdc_pipeline.capture import METHODS, Wal
from postgres_cdc_pipeline.db import load_source
from postgres_cdc_pipeline.runner import run
from postgres_cdc_pipeline.simulate import Rates, Simulator, execute

ACCURACY_SCALE = 1
CYCLES = 24
COST_SCALES = [0.1, 1, 10]
COST_METHODS = ["watermark", "trigger_queue", "wal"]
RATES = Rates()  # 2% of each cycle's orders in a late transaction


def _source(scale: float) -> Path:
    out = Path(tempfile.mkdtemp(prefix=f"cdc-sf{scale:g}-"))
    write(GenConfig(scale=scale), out)
    return out


def accuracy(src_url: str, tgt_url: str, data: Path, where: str) -> Path:
    cases = []
    for name in METHODS:
        r = run(name, src_url, tgt_url, data, cycles=CYCLES, rates=RATES)
        h = r["history"]
        extra = {
            "rows_wrong": r["rows_wrong"],
            "missing": sum(t["missing"] for t in r["diff"].values()),
            "stale": sum(t["stale"] for t in r["diff"].values()),
            "ghost": sum(t["ghost"] for t in r["diff"].values()),
            "history_recorded": h["recorded"],
            "history_true": h["true"],
            "history_pct": 100 * h["recorded"] / h["true"],
            "history_missing": h["true"] - h["recorded"],
            "changes_applied": r["changes_applied"],
            "truth": r["truth"],
            "loaded": r["loaded"],
        }
        cases.append(CaseResult(name, {"scale": ACCURACY_SCALE, "cycles": CYCLES,
                                       "late_rate": RATES.late}, len(r["cycle_times"]),
                                r["cycle_times"], [0.0], extra))  # fmt: skip
        print(f"accuracy {name}: wrong={extra['rows_wrong']} history={extra['history_pct']:.2f}%")
    path = save("accuracy", cases, notes=(
        f"Scale {ACCURACY_SCALE} ShopFlow source, {CYCLES} cycles, seed 42, "
        f"{RATES.late:.0%} of each cycle's orders written in a late transaction. "
        f"wall_s = capture + apply time of each cycle. Database: {where}."))  # fmt: skip
    plot(path, "rows_wrong", title="Rows that differ from the source after 24 cycles")
    plot(path, "history_missing", title="Status periods missing from the SCD2 history")
    return path


def cost(src_url: str, tgt_url: str, sources: dict[float, Path], where: str) -> Path:
    cases = []
    for scale in COST_SCALES:
        for name in COST_METHODS:
            r = run(name, src_url, tgt_url, sources[scale], cycles=12, rates=RATES)
            sizes = r["changes_applied"] / 13
            cases.append(CaseResult(f"{name} · scale {scale:g}", {"scale": scale, "method": name},
                                    len(r["cycle_times"]), r["cycle_times"], [0.0],
                                    {"changes_per_cycle": round(sizes)}))  # fmt: skip
            print(f"cost {name} scale {scale}: {cases[-1].median_s * 1000:.0f} ms/cycle")
        # Same method without pg_log_standby_snapshot(): restart_lsn stays at the
        # slot creation point and every read decodes all WAL written since.
        r = run("wal", src_url, tgt_url, sources[scale], cycles=12, rates=RATES,
                capture=Wal(log_snapshot=False))  # fmt: skip
        cases.append(CaseResult(f"wal, no snapshot record · scale {scale:g}",
                                {"scale": scale, "method": "wal_no_snapshot"},
                                len(r["cycle_times"]), r["cycle_times"], [0.0]))  # fmt: skip
        print(f"cost wal no snapshot scale {scale}: {cases[-1].median_s * 1000:.0f} ms/cycle")
        with psycopg.connect(src_url) as src, psycopg.connect(tgt_url) as tgt:
            case = measure(lambda src=src, tgt=tgt: ap.full_reload(src, tgt),
                           label=f"full reload · scale {scale:g}",
                           params={"scale": scale, "method": "full_reload"}, runs=3)  # fmt: skip
            case.extra["rows"] = sum(case.extra.values())
        cases.append(case)
        print(f"cost full reload scale {scale}: {case.median_s:.2f} s")
    path = save("incremental_vs_full", cases, notes=(
        "Median time of one cycle: capture + apply for incremental methods, TRUNCATE + COPY "
        "of orders and payments for full reload. 12 cycles per method, ~0.2% of orders new "
        "per cycle plus status changes. Source and target on the same server. "
        f"{where}."))  # fmt: skip
    plot(path, "median_s", title="One sync cycle: incremental vs full reload (s)")
    return path


def _writes(src_url: str, data: Path, capture: str | None) -> float:
    """Load the source, install a capture, then time only the simulated writes."""
    with psycopg.connect(src_url) as src, psycopg.connect(src_url) as w:
        load_source(src, data)
        if capture:
            METHODS[capture]().install(src)
        sim = Simulator(src, seed=42, rates=Rates(late=0.0))
        src.commit()
        plans = [sim.next_plan() for _ in range(CYCLES)]
        t0 = time.perf_counter()
        for plan in plans:
            execute(w, plan.on_time)
            w.commit()
        return time.perf_counter() - t0


def overhead(src_url: str, data: Path, where: str) -> Path:
    cases = []
    for capture, label in ((None, "no capture"), ("trigger_queue", "trigger log"),
                           ("wal", "WAL slot + publication")):  # fmt: skip
        walls = [_writes(src_url, data, capture) for _ in range(5)]
        cases.append(CaseResult(label, {"capture": capture or "none", "cycles": CYCLES}, 5,
                                walls, [0.0]))  # fmt: skip
        print(f"overhead {label}: {cases[-1].median_s:.2f} s")
    base = cases[0].median_s
    for c in cases:
        c.extra["vs_no_capture_pct"] = round(100 * (c.median_s / base - 1), 1)
    path = save("source_write_overhead", cases, notes=(
        f"Time to run {CYCLES} cycles of simulated writes on a scale {ACCURACY_SCALE} source, "
        "nothing else. wal_level=logical is on in every case (it is a server setting), so "
        f"the WAL case measures the slot and publication only. {where}."))  # fmt: skip
    plot(path, "median_s", title=f"Source write time for {CYCLES} cycles (s)")
    return path


def main() -> None:
    server, where = server_url()
    src_url = database(server, "cdc_source")
    tgt_url = database(server, "cdc_target")
    sources = {s: _source(s) for s in sorted({ACCURACY_SCALE, *COST_SCALES})}
    print(accuracy(src_url, tgt_url, sources[ACCURACY_SCALE], where))
    print(overhead(src_url, sources[ACCURACY_SCALE], where))
    print(cost(src_url, tgt_url, sources, where))


if __name__ == "__main__":
    main()

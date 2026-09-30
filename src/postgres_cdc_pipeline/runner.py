"""Run the simulated source against one capture method and score the result.

Per cycle:
    1. the late transaction starts and writes its share of the cycle
    2. the regular transaction writes the rest and commits
    3. capture + apply run (this is the timed part)
    4. the late transaction commits
After the last cycle one more capture runs, so every method gets the chance
to pick up whatever is still pending.
"""

from __future__ import annotations

import statistics
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import apply as ap
from .capture import METHODS
from .db import connect, load_source, reset_target
from .simulate import Rates, Simulator, execute
from .verify import diff, history_rows


def _capture_once(method, src, tgt) -> tuple[int, float]:
    t0 = time.perf_counter()
    changes, pos = method.poll(src, ap.position(tgt, method.name))
    ap.apply(tgt, changes, method.name, pos)
    method.ack(src, pos)
    return len(changes), time.perf_counter() - t0


def run(
    method_name: str,
    source_url: str,
    target_url: str,
    data_dir: Path,
    *,
    cycles: int = 24,
    seed: int = 42,
    rates: Rates | None = None,
    reload_every_cycle: bool = False,
    capture: Any = None,
) -> dict[str, Any]:
    method = capture or METHODS[method_name]()
    with (
        connect(source_url) as src,
        connect(source_url) as writer,
        connect(source_url) as late,
        connect(target_url) as tgt,
    ):
        loaded = load_source(src, data_dir)
        reset_target(tgt)
        method.install(src)
        ap.snapshot(src, tgt)
        tgt.execute(
            "INSERT INTO rep.cdc_state (consumer, position) VALUES (%s, %s)",
            (method.name, method.start(src)),
        )
        tgt.commit()

        sim = Simulator(src, seed=seed, rates=rates)
        src.commit()
        timings, sizes, reloads = [], [], []
        for _ in range(cycles):
            plan = sim.next_plan()
            execute(late, plan.late)  # started first: older now(), commits last
            execute(writer, plan.on_time)
            writer.commit()
            n, seconds = _capture_once(method, src, tgt)
            timings.append(seconds)
            sizes.append(n)
            if reload_every_cycle:
                t0 = time.perf_counter()
                ap.full_reload(src, tgt)
                reloads.append(time.perf_counter() - t0)
            late.commit()
        n, _ = _capture_once(method, src, tgt)
        sizes.append(n)

        report = diff(src, tgt)
        periods = history_rows(tgt)

    truth = asdict(sim.truth)
    return {
        "method": method_name,
        "cycles": cycles,
        "loaded": loaded,
        "changes_applied": sum(sizes),
        "cycle_s": {
            "median": statistics.median(timings),
            "p95": sorted(timings)[int(0.95 * (len(timings) - 1))],
            "total": sum(timings),
        },
        "cycle_times": timings,
        "full_reload_s": statistics.median(reloads) if reloads else None,
        "diff": report,
        "rows_wrong": sum(v["missing"] + v["stale"] + v["ghost"] for v in report.values()),
        "history": {"recorded": periods, "true": truth["status_periods"]},
        "truth": truth,
    }

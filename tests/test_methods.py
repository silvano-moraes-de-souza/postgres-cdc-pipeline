"""Every capture method against the same simulated source, scored against the truth."""

import psycopg
import pytest

from postgres_cdc_pipeline import apply as ap
from postgres_cdc_pipeline.db import load_source, reset_target
from postgres_cdc_pipeline.runner import run
from postgres_cdc_pipeline.simulate import Rates, Simulator

LATE = Rates(late=0.2)


@pytest.fixture(scope="module")
def results(source_url, target_url, data):
    return {
        m: run(m, source_url, target_url, data, cycles=10, rates=LATE)
        for m in ("watermark", "trigger_seq", "trigger_queue", "wal")
    }


@pytest.mark.parametrize("method", ["trigger_queue", "wal"])
def test_log_readers_match_the_source_exactly(results, method):
    r = results[method]
    assert r["truth"]["late_orders"] > 0  # the scenario really had slow commits
    assert r["rows_wrong"] == 0
    assert r["history"]["recorded"] == r["history"]["true"]


def test_watermark_keeps_every_deleted_row(results):
    r = results["watermark"]
    deleted = r["truth"]["deleted_orders"]
    assert deleted > 0
    assert r["diff"]["orders"]["ghost"] == deleted
    assert r["diff"]["payments"]["ghost"] == deleted


def test_watermark_loses_status_periods(results):
    h = results["watermark"]["history"]
    assert h["recorded"] < h["true"]


def test_sequence_position_misses_late_commits(results):
    r = results["trigger_seq"]
    assert r["rows_wrong"] > 0
    assert r["history"]["recorded"] < r["history"]["true"]


def test_sequence_position_is_exact_without_late_commits(source_url, target_url, data):
    r = run("trigger_seq", source_url, target_url, data, cycles=6, rates=Rates(late=0.0))
    assert r["rows_wrong"] == 0
    assert r["history"]["recorded"] == r["history"]["true"]


def test_simulator_is_deterministic(source_url, data):
    plans = []
    for _ in range(2):
        with psycopg.connect(source_url) as src:
            load_source(src, data)
            sim = Simulator(src, seed=7)
            plans.append([sim.next_plan() for _ in range(3)])
    assert plans[0] == plans[1]


def test_reapplying_a_batch_changes_nothing(source_url, target_url, data):
    with psycopg.connect(source_url) as src, psycopg.connect(target_url) as tgt:
        load_source(src, data)
        reset_target(tgt)
        ap.snapshot(src, tgt)
        row = src.execute("SELECT to_jsonb(o) FROM shop.orders o ORDER BY order_id LIMIT 1")
        order = row.fetchone()[0]
        src.commit()
        newer = {**order, "status": "shipped", "updated_at": "2099-01-01T00:00:00+00:00"}
        older = {**order, "status": "canceled", "updated_at": "2000-01-01T00:00:00+00:00"}
        batch = [ap.Change("orders", "U", order["order_id"], newer, newer["updated_at"])]
        ap.apply(tgt, batch, "t", "1")
        ap.apply(tgt, batch, "t", "1")
        # An older version arriving late must not overwrite the newer one.
        ap.apply(tgt, [ap.Change("orders", "U", order["order_id"], older, older["updated_at"])],
                 "t", "2")  # fmt: skip
        status = tgt.execute(
            "SELECT status FROM rep.orders WHERE order_id = %s", (order["order_id"],)
        ).fetchone()[0]
        open_periods = tgt.execute(
            "SELECT count(*) FROM rep.order_status_history "
            "WHERE order_id = %s AND valid_to IS NULL",
            (order["order_id"],),
        ).fetchone()[0]
    assert status == "shipped"
    assert open_periods == 1


def test_two_moves_in_one_batch_become_two_closed_periods(source_url, target_url, data):
    with psycopg.connect(source_url) as src, psycopg.connect(target_url) as tgt:
        load_source(src, data)
        reset_target(tgt)
        ap.snapshot(src, tgt)
        order = {"order_id": 10**9, "customer_id": 1, "ordered_at": "2026-01-01T00:00:00+00:00",
                 "status": "pending", "channel": "web", "total_cents": 100, "delivered_at": None,
                 "updated_at": "2026-01-01T00:00:00+00:00"}  # fmt: skip
        steps = [("pending", "01:00"), ("shipped", "02:00"), ("delivered", "03:00")]
        batch = []
        for status, hhmm in steps:
            ts = f"2026-01-01T{hhmm}:00+00:00"
            batch.append(ap.Change("orders", "U", 10**9, {**order, "status": status,
                                                          "updated_at": ts}, ts))  # fmt: skip
        batch.append(ap.Change("orders", "D", 10**9, None, "2026-01-01T04:00:00+00:00"))
        ap.apply(tgt, batch, "t", "1")
        rows = tgt.execute(
            "SELECT status, (valid_from AT TIME ZONE INTERVAL '+00:00')::time::text, "
            "(valid_to AT TIME ZONE INTERVAL '+00:00')::time::text "
            "FROM rep.order_status_history WHERE order_id = %s ORDER BY valid_from",
            (10**9,),
        ).fetchall()
    assert rows == [
        ("pending", "01:00:00", "02:00:00"),
        ("shipped", "02:00:00", "03:00:00"),
        ("delivered", "03:00:00", "04:00:00"),
    ]


def _restart_lag(source_url) -> int:
    with psycopg.connect(source_url) as c:
        return int(c.execute(
            "SELECT pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn) FROM pg_replication_slots "
            "WHERE slot_name = 'cdc_slot' AND database = current_database()"
        ).fetchone()[0])  # fmt: skip


def test_snapshot_record_lets_the_slot_release_wal(source_url, target_url, data):
    from postgres_cdc_pipeline.capture import Wal  # noqa: PLC0415

    held = {}
    for flag in (False, True):
        r = run("wal", source_url, target_url, data, cycles=6, capture=Wal(log_snapshot=flag))
        assert r["rows_wrong"] == 0  # correctness never depends on it
        held[flag] = _restart_lag(source_url)
    assert held[True] < held[False]

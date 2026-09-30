<p align="center">
  <img src="docs/assets/banner.svg" alt="Postgres CDC Pipeline" width="100%">
</p>

<p align="center">
  <a href="https://github.com/silvano-moraes-de-souza/postgres-cdc-pipeline/actions/workflows/ci.yml"><img src="https://github.com/silvano-moraes-de-souza/postgres-cdc-pipeline/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <img src="https://img.shields.io/badge/python-3.11%2B-2a78d6" alt="Python 3.11+">
  <img src="https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white" alt="PostgreSQL 16">
  <img src="https://img.shields.io/badge/CDC-pgoutput-38bdf8" alt="pgoutput">
  <img src="https://img.shields.io/badge/license-MIT-52514e" alt="MIT">
  <a href="https://github.com/silvano-moraes-de-souza/30-days-data-eng"><img src="https://img.shields.io/badge/30%20days-day%2003-0b0b0b" alt="30 Days of Data & Software Engineering"></a>
</p>

> Four ways to copy only what changed from a PostgreSQL database: a timestamp watermark, two readings of a trigger log, and logical decoding of the write-ahead log. All four run against the same stream of inserts, updates, deletes and slow commits, and each one is scored row by row against the source.

<table>
<tr>
<td align="center"><b>0 rows wrong</b><br/>WAL and trigger queue after 24 cycles<br/>of deletes and slow commits</td>
<td align="center"><b>11,684 ghosts</b><br/>deleted rows the watermark<br/>kept in the copy</td>
<td align="center"><b>5x faster</b><br/>WAL sync 2.4 s vs full reload<br/>11.8 s on 2M rows</td>
<td align="center"><b>2.2x</b><br/>faster WAL reads after letting<br/>the slot release old WAL</td>
</tr>
</table>

<sub>All numbers come from <a href="bench/run.py">bench/run.py</a> and <a href="results/">results/</a>.</sub>

**Contents:** [Problem](#problem) · [Architecture](#architecture) · [Quickstart](#quickstart) · [Results](#results) · [How it works](#how-it-works) · [Engineering decisions](#engineering-decisions) · [Tests](#tests) · [Limitations](#limitations)

## Problem

Copying a whole table every night stops working when the table gets big or the business wants fresher data. The usual next step is an incremental load: `WHERE updated_at > last_run`. It is simple and it runs, but it is wrong in three ways that no error message reports:

1. A deleted row is not returned by any query, so it stays in the copy forever.
2. If an order goes from pending to shipped to delivered between two runs, the copy only ever sees delivered. Any "time spent in each status" metric is off.
3. `updated_at DEFAULT now()` stores when the transaction *started*. A transaction that commits after the reader moved its watermark lands behind it and is never read.

Change data capture (CDC) exists to fix this. This project measures how much each approach loses.

## Architecture

```mermaid
flowchart LR
    SIM[Simulator<br/>seeded, counts the truth] -->|inserts, updates,<br/>deletes, late commits| SRC[(cdc_source<br/>shop.orders<br/>shop.payments)]
    SRC -->|updated_at &gt; mark| WM[watermark]
    SRC -->|triggers| LOG[(cdc.change_log)]
    LOG -->|seq &gt; position| TS[trigger_seq]
    LOG -->|DELETE ... RETURNING| TQ[trigger_queue]
    SRC -->|logical slot, pgoutput| WAL[wal]
    WM & TS & TQ & WAL --> AP[apply<br/>versioned upsert,<br/>SCD2 history,<br/>position in same tx]
    AP --> TGT[(cdc_target<br/>rep.orders<br/>rep.payments<br/>rep.order_status_history)]
    TGT --> V{verify<br/>vs source<br/>and truth}
```

Source and target are separate databases, so every method only gets what it can read through a connection, as it would against a production system.

## Quickstart

```bash
git clone https://github.com/silvano-moraes-de-souza/postgres-cdc-pipeline
cd postgres-cdc-pipeline
docker compose up --build    # Postgres with wal_level=logical, then all four methods
```

Without Docker (Python 3.11 or 3.12, an embedded PostgreSQL 16 is started for the tests):

```bash
uv sync
uv run pytest                                        # 27 tests against a real Postgres
uv run cdc-pipeline generate --scale 0.1 --out data/sf0.1
export SOURCE_URL=postgresql://user:pass@localhost:5432/cdc_source
export TARGET_URL=postgresql://user:pass@localhost:5432/cdc_target
uv run cdc-pipeline compare --data data/sf0.1 --cycles 24
uv run python -m bench.run                           # rebuilds results/ and the charts
```

## Results

Measured on a laptop (Intel 11th gen Tiger Lake, 6 cores, 24 GB RAM, Windows 11) against an embedded PostgreSQL 16 with `wal_level=logical`. Source and target are two databases on that server. Every JSON in [`results/`](results/) records the machine and the commit it ran on.

### Accuracy: who keeps an exact copy

Scale 1 source (100,000 orders and 100,000 payments), 24 cycles, seed 42, 2% of each cycle's orders in a late transaction. Over the run the simulator inserted 4,709 orders, made 12,682 status moves, hard-deleted 5,842 canceled orders and wrote 266 orders in slow transactions.

| Method | Rows wrong | Stale | Ghosts | Status periods recorded | Missing periods |
|---|---:|---:|---:|---:|---:|
| `watermark` | 11,893 | 209 | 11,684 | 112,554 of 117,391 | 4,837 |
| `trigger_seq` | 417 | 209 | 208 | 117,217 of 117,391 | 174 |
| `trigger_queue` | **0** | 0 | 0 | 117,391 of 117,391 | 0 |
| `wal` | **0** | 0 | 0 | 117,391 of 117,391 | 0 |

![Rows that differ from the source](docs/assets/accuracy_rows_wrong.png)

The watermark's 11,684 ghosts are exactly two per deleted order (the order and its payment): it never learns about a delete. Its 4,837 missing periods are the moves that happened twice between two reads. The 209 stale rows are the slow commits.

`trigger_seq` is the interesting one. It has a log of every change and still ends with 417 wrong rows, all from slow commits: those rows were written with a lower `seq` than rows the reader had already passed. Reading the same log as a queue fixes it with no other change.

### Cost: one sync cycle against a full reload

Median time of one cycle over 12 cycles (whiskers: min and max). Each cycle changes about 1% of the rows, which is a busy source.

| Scale (orders + payments) | `watermark` | `trigger_queue` | `wal` | `wal`, no snapshot record | Full reload |
|---|---:|---:|---:|---:|---:|
| 0.1 (20k rows) | 9 ms | 14 ms | 50 ms | 52 ms | 107 ms |
| 1 (200k rows) | 63 ms | 109 ms | 131 ms | 291 ms | 863 ms |
| 10 (2M rows) | 989 ms | 1,995 ms | 2,381 ms | 3,093 ms | 11,803 ms |

![Incremental vs full reload](docs/assets/incremental_vs_full_median_s.png)

The watermark is the fastest because it applies about half as many changes: it only sees the last state of each row. At 2M rows the WAL reader keeps an exact copy in a fifth of the time of a reload.

The "no snapshot record" column is the same WAL reader without `pg_log_standby_snapshot()`. At scale 1 the slot held back 50 to 64 MB of WAL (measured with `pg_wal_lsn_diff(confirmed_flush_lsn, restart_lsn)`), all of it decoded on every read. With the record the backlog stayed around 2 MB and the cycle dropped from 291 to 131 ms. At scale 10 the gain is smaller in relative terms because applying 18,770 changes per cycle dominates.

### Cost on the source

Time for 24 cycles of the simulated writes on the scale 1 source, nothing else running, 5 runs each.

| Capture installed | Runs (s) | Median |
|---|---|---:|
| none | 1.41 to 1.66 | 1.56 s |
| trigger log | 2.10 to 2.33 | 2.24 s (+44%) |
| WAL slot and publication | 1.07 to 1.58 | 1.13 s |

![Source write overhead](docs/assets/source_write_overhead_median_s.png)

The trigger log costs 44% more write time: every change writes a second row. The WAL case did not come out slower than no capture at all; its runs are spread more widely, so this benchmark shows no measurable cost. `wal_level=logical` itself is on in all three cases, since it is a server setting.

## How it works

### The source keeps changing

[`simulate.py`](src/postgres_cdc_pipeline/simulate.py) loads a [ShopFlow](https://github.com/silvano-moraes-de-souza/shopflow-datagen) dataset (orders and payments) and then runs cycles. Each cycle adds new orders, moves orders through pending, shipped, delivered, canceled and returned (an order can take two steps in one cycle), and hard-deletes old canceled orders with their payments. Everything is seeded, so a run is repeatable.

### Slow commits are real

A share of each cycle's orders is written by a second connection that opens its transaction first and commits only after the capture step has run. `now()` gives those rows an older `updated_at` and the trigger log gives them lower sequence numbers, the same as a slow request in production.

### The truth is counted while it is created

The simulator records every status period that really existed and every row it deleted. After the last cycle, [`verify.py`](src/postgres_cdc_pipeline/verify.py) compares an md5 of every row in source and target, and the SCD2 history row count with the true number of periods.

### Four capture methods, one apply

Every method in [`capture.py`](src/postgres_cdc_pipeline/capture.py) returns the same `Change` records, so the target code is shared and the comparison is only about capture:

| Method | Reads | Position kept | What it cannot see |
|---|---|---|---|
| `watermark` | `WHERE updated_at > mark` | latest `updated_at` | deletes, intermediate states, slow commits |
| `trigger_seq` | trigger log `WHERE seq > position` | highest `seq` read | slow commits: their `seq` was taken before the reader's position moved |
| `trigger_queue` | `DELETE FROM cdc.change_log RETURNING *` | none needed | nothing measured; entries of an open transaction are invisible and taken next time |
| `wal` | `pg_logical_slot_peek_binary_changes` with `pgoutput` | commit LSN | nothing measured; transactions arrive in commit order |

### Decoding the WAL

[`pgoutput.py`](src/postgres_cdc_pipeline/pgoutput.py) is a decoder for PostgreSQL's own logical replication protocol (Begin, Relation, Insert, Update, Delete, Commit), written against the [protocol docs](https://www.postgresql.org/docs/16/protocol-logicalrep-message-formats.html) and tested with hand-built messages. Nothing has to be installed on the server: `pgoutput` ships with PostgreSQL, the same one Debezium and native replication use.

### Applying safely

[`apply.py`](src/postgres_cdc_pipeline/apply.py) upserts with a version check (`WHERE target.updated_at <= incoming.updated_at`), so replaying a batch or receiving an old version is harmless. The consumer position is written in the same transaction as the data. For `wal`, the slot is only advanced after the target commits; for `trigger_queue`, the `DELETE` on the log commits only after the target does. A crash in between means the batch is read again, never lost.

### Letting the slot release WAL

A replication slot remembers `restart_lsn`, the point from which decoding has to start. It only moves forward when the decoder passes a running-transactions record, which PostgreSQL writes every 15 seconds or at a checkpoint. A sync that runs more often than that never passes one, so every read decoded all WAL written since the slot was created, including the initial load, and threw most of it away. Calling `pg_log_standby_snapshot()` after each advance writes that record on demand. The effect is measured in the results.

### History

Order events of a batch become SCD2 periods (`valid_from`, `valid_to`) in event order, computed in Python and written with two `executemany` calls, so a move from pending to shipped to delivered in one batch still produces three periods.

## Engineering decisions

| Decision | Alternative | Why |
|---|---|---|
| Read the WAL through the SQL functions (`pg_logical_slot_peek_binary_changes` + `pg_replication_slot_advance`) | Streaming replication protocol | psycopg 3 does not speak the replication protocol. Peek + advance gives at-least-once delivery with plain SQL: the slot moves only after the target commits. |
| `pgoutput` and a decoder written here | `wal2json` or `test_decoding` | `pgoutput` ships with every PostgreSQL 10+ server, so nothing has to be installed. `wal2json` needs an extension installed; `test_decoding` is a debugging plugin with a text format meant for humans. |
| `pg_log_standby_snapshot()` after every advance | Wait for the server's own record every 15 s | Without it, `restart_lsn` never moved during a run and every read decoded the whole WAL since the slot was created (about 50 MB at scale 1, growing). See the results. |
| Trigger log read as a queue (`DELETE ... RETURNING`) | Keep the log, remember the highest `seq` | A sequence value is taken when a row is written, not when it commits. The queue read measured 0 wrong rows; the `seq` position measured the misses. |
| Version check in the upsert (`updated_at <=`) | Blind `ON CONFLICT DO UPDATE` | Makes redelivery and replays harmless, which at-least-once delivery requires. |
| Source and target as separate databases | Two schemas in one database | Each method only gets what it can read over a connection. It also means WAL from the target shares the server; the benchmark says so in its notes. |
| md5 of every row to compare | Row counts | Counts hide stale rows and ghosts that cancel each other out. |

## Tests

27 tests, run in CI on Python 3.11, 3.12 and 3.13 against PostgreSQL 16 started with `wal_level=logical`. Locally an embedded PostgreSQL (pgserver) is switched to logical WAL, so no Docker is needed.

| File | What it proves |
|---|---|
| `test_methods.py` | `trigger_queue` and `wal` end with 0 wrong rows and an exact history under slow commits; `watermark` keeps exactly one ghost per deleted row; `trigger_seq` is exact without slow commits and wrong with them; replays and old versions change nothing; two moves in one batch give correct SCD2 periods; with the snapshot record the slot holds back less WAL, and correctness does not depend on it |
| `test_pgoutput.py` | decoding of every message type used, relation reuse across transactions, LSN parsing |
| `test_cli.py` | `compare`, `generate` and argument errors |

A `compose-e2e` job builds the image and runs the full comparison against a real `postgres:16-alpine` container.

## Limitations

- Source and target run on the same server during the benchmarks, so the WAL the target writes is decoded (and skipped) by the slot too. Separate servers would make the `wal` numbers a bit lower.
- `pg_log_standby_snapshot()` exists from PostgreSQL 16 and needs superuser or a grant. Without it the code falls back to the server's 15 s interval, which is fine for syncs that run less often than that.
- A replication slot keeps WAL until it is advanced. If the consumer stops, the source disk fills up. Production needs `max_slot_wal_keep_size` and an alert on slot lag; neither is set up here.
- The trigger log adds a write to every change on the source (measured above) and grows if nobody reads it.
- Schema changes on the source are not handled: a new column reaches the decoder but not the target table.
- A delete followed by a replay of an older upsert would bring the row back. It only happens if a batch is redelivered after the target applied a later delete; tombstones would fix it.
- One consumer per method. Several consumers need one slot (or one log reader) each.

## Project structure

```
src/postgres_cdc_pipeline/
  simulate.py    seeded change stream and ground truth
  capture.py     watermark, trigger_seq, trigger_queue, wal
  pgoutput.py    logical replication protocol decoder
  apply.py       versioned upsert, SCD2 history, full reload
  verify.py      row-by-row comparison with the source
  runner.py      one method, N cycles, scored
  cli.py         cdc-pipeline generate | run | compare
  sql/           source schema, trigger log, target schema
bench/           benchmarks, charts, embedded Postgres helper
results/         benchmark output (JSON, with machine and commit)
tests/
```

## Part of the series

Day 03 of [30 Days of Data & Software Engineering](https://github.com/silvano-moraes-de-souza/30-days-data-eng). Data from [shopflow-datagen](https://github.com/silvano-moraes-de-souza/shopflow-datagen) (day 00); the star schema of [ecommerce-data-pipeline](https://github.com/silvano-moraes-de-souza/ecommerce-data-pipeline) (day 01) is the kind of target this feeds.

## Author

**Silvano Moraes de Souza** · [LinkedIn](https://www.linkedin.com/in/silvano-moraes-de-souza) · [Portfolio](https://silvanomsouza.vercel.app/)

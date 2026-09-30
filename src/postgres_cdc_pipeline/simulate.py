"""Deterministic change stream for the source database.

Each cycle stands for a slice of business time:

- new orders arrive as ``pending`` with a ``pending`` payment
- pending orders ship (payment approved) or get canceled (payment refused)
- shipped orders are delivered, a few delivered orders are returned
- a retention job hard-deletes old canceled orders and their payments

An order can move twice in one cycle (created and shipped, shipped and
delivered), which is exactly the intermediate state a polling reader never sees.

A share of the cycle's orders is written in a *late* transaction: it starts
before the regular one and commits only after the capture step has run. With
``updated_at DEFAULT now()`` those rows carry a timestamp older than rows that
were already captured.

The simulator also counts the ground truth: every status period that really
existed, and which rows were deleted.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import psycopg

INSERT_ORDER = (
    "INSERT INTO shop.orders (order_id, customer_id, ordered_at, status, channel, total_cents) "
    "VALUES (%s, %s, now(), 'pending', %s, %s)"
)
INSERT_PAYMENT = (
    "INSERT INTO shop.payments (payment_id, order_id, method, status, amount_cents) "
    "VALUES (%s, %s, %s, 'pending', %s)"
)
SET_STATUS = "UPDATE shop.orders SET status = %s WHERE order_id = %s"
SET_DELIVERED = (
    "UPDATE shop.orders SET status = 'delivered', delivered_at = now() WHERE order_id = %s"
)
SET_PAYMENT = (
    "UPDATE shop.payments SET status = %s, "
    "paid_at = CASE WHEN %s = 'approved' THEN now() ELSE paid_at END WHERE order_id = %s"
)
DELETE_PAYMENTS = "DELETE FROM shop.payments WHERE order_id = %s"
DELETE_ORDER = "DELETE FROM shop.orders WHERE order_id = %s"

CHANNELS = ["web", "app", "marketplace", "social"]
METHODS = ["pix", "card", "boleto", "debit"]


@dataclass(frozen=True)
class Rates:
    new_orders: float = 0.002  # share of the initial order count, per cycle
    ship: float = 0.55
    cancel: float = 0.05
    deliver: float = 0.40
    return_: float = 0.001
    purge: float = 0.10  # share of canceled orders hard-deleted per cycle
    late: float = 0.02  # share of the cycle's orders written in a late transaction


@dataclass
class Plan:
    """Statements for one cycle, split by the transaction that will run them."""

    on_time: list[tuple[str, tuple]] = field(default_factory=list)
    late: list[tuple[str, tuple]] = field(default_factory=list)


@dataclass
class Truth:
    status_periods: int = 0  # rows a perfect SCD2 history would hold
    transitions: int = 0
    inserted_orders: int = 0
    deleted_orders: int = 0
    late_orders: int = 0


class Simulator:
    def __init__(self, conn: psycopg.Connection, seed: int = 42, rates: Rates | None = None):
        self.seed = seed
        self.rates = rates or Rates()
        self.cycle = 0
        rows = conn.execute("SELECT order_id, status FROM shop.orders").fetchall()
        self.status = {oid: st for oid, st in rows}
        self.next_order = max(self.status, default=0) + 1
        self.next_payment = (
            conn.execute("SELECT coalesce(max(payment_id), 0) FROM shop.payments").fetchone()[0] + 1
        )
        self.base = len(self.status)
        self.truth = Truth(status_periods=len(self.status))

    def _ids(self, status: str) -> np.ndarray:
        return np.array(sorted(k for k, v in self.status.items() if v == status), dtype=np.int64)

    def next_plan(self) -> Plan:
        """Plan the next cycle and advance the in-memory state as if it committed."""
        rng = np.random.default_rng([self.seed, self.cycle])
        self.cycle += 1
        r = self.rates
        steps: dict[int, list[tuple[str, tuple]]] = {}

        def add(order_id: int, stmt: str, params: tuple) -> None:
            steps.setdefault(order_id, []).append((stmt, params))

        def move(order_id: int, new: str) -> None:
            self.status[order_id] = new
            self.truth.transitions += 1
            self.truth.status_periods += 1

        n_new = int(rng.poisson(max(r.new_orders * self.base, 1)))
        for _ in range(n_new):
            oid, pid = self.next_order, self.next_payment
            self.next_order += 1
            self.next_payment += 1
            total = int(rng.integers(2_000, 60_000))
            add(oid, INSERT_ORDER, (oid, int(rng.integers(1, 10_000)),
                                    CHANNELS[rng.integers(len(CHANNELS))], total))  # fmt: skip
            add(oid, INSERT_PAYMENT, (pid, oid, METHODS[rng.integers(len(METHODS))], total))
            self.status[oid] = "pending"
            self.truth.inserted_orders += 1
            self.truth.status_periods += 1

        # Pending first, then shipped: an order can take both steps in one cycle.
        pending = self._ids("pending")
        u = rng.random(len(pending))
        for oid in pending[u < r.ship]:
            add(int(oid), SET_STATUS, ("shipped", int(oid)))
            add(int(oid), SET_PAYMENT, ("approved", "approved", int(oid)))
            move(int(oid), "shipped")
        for oid in pending[(u >= r.ship) & (u < r.ship + r.cancel)]:
            add(int(oid), SET_STATUS, ("canceled", int(oid)))
            add(int(oid), SET_PAYMENT, ("refused", "refused", int(oid)))
            move(int(oid), "canceled")

        shipped = self._ids("shipped")
        for oid in shipped[rng.random(len(shipped)) < r.deliver]:
            add(int(oid), SET_DELIVERED, (int(oid),))
            move(int(oid), "delivered")

        delivered = self._ids("delivered")
        for oid in delivered[rng.random(len(delivered)) < r.return_]:
            add(int(oid), SET_STATUS, ("returned", int(oid)))
            add(int(oid), SET_PAYMENT, ("refunded", "refunded", int(oid)))
            move(int(oid), "returned")

        # Retention: only orders already canceled before this cycle, so a delete
        # never shares a cycle with the change that canceled the order.
        canceled = np.array(
            sorted(k for k, v in self.status.items() if v == "canceled" and k not in steps),
            dtype=np.int64,
        )
        for oid in canceled[rng.random(len(canceled)) < r.purge]:
            add(int(oid), DELETE_PAYMENTS, (int(oid),))
            add(int(oid), DELETE_ORDER, (int(oid),))
            del self.status[int(oid)]
            self.truth.deleted_orders += 1

        # Late transaction: whole orders only, and never ones inserted this cycle
        # (the late transaction starts first, their rows would not exist yet).
        plan = Plan()
        existing = [o for o in steps if o < self.next_order - n_new]
        late = set(rng.choice(existing, int(len(existing) * r.late), replace=False).tolist()) \
            if existing and r.late > 0 else set()  # fmt: skip
        self.truth.late_orders += len(late)
        # Step k of every order runs before step k+1 of any order: each order keeps
        # its own sequence, and equal statements end up next to each other.
        for oid, stmts in steps.items():
            target = plan.late if oid in late else plan.on_time
            target.extend((k, stmt, oid, params) for k, (stmt, params) in enumerate(stmts))
        plan.on_time = [(s, p) for _, s, _, p in sorted(plan.on_time, key=lambda x: x[:3])]
        plan.late = [(s, p) for _, s, _, p in sorted(plan.late, key=lambda x: x[:3])]
        return plan


def execute(conn: psycopg.Connection, statements: list[tuple[str, tuple]]) -> None:
    """Run statements in order, batching consecutive runs of the same SQL."""
    cur = conn.cursor()
    i = 0
    while i < len(statements):
        stmt = statements[i][0]
        j = i
        while j < len(statements) and statements[j][0] == stmt:
            j += 1
        cur.executemany(stmt, [p for _, p in statements[i:j]])
        i = j

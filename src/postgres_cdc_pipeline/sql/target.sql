-- The analytical copy. Same columns as the source, plus the status history that
-- only a capture method which sees every change can fill in correctly.

CREATE SCHEMA rep;

CREATE TABLE rep.orders (
    order_id     bigint PRIMARY KEY,
    customer_id  bigint NOT NULL,
    ordered_at   timestamptz NOT NULL,
    status       text NOT NULL,
    channel      text,
    total_cents  bigint NOT NULL,
    delivered_at timestamptz,
    updated_at   timestamptz NOT NULL
);

CREATE TABLE rep.payments (
    payment_id   bigint PRIMARY KEY,
    order_id     bigint NOT NULL,
    method       text NOT NULL,
    status       text NOT NULL,
    amount_cents bigint NOT NULL,
    paid_at      timestamptz,
    updated_at   timestamptz NOT NULL
);

-- SCD type 2: one row per period an order spent in a status.
CREATE TABLE rep.order_status_history (
    id         bigserial PRIMARY KEY,
    order_id   bigint NOT NULL,
    status     text NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to   timestamptz
);
CREATE UNIQUE INDEX order_status_open ON rep.order_status_history (order_id)
    WHERE valid_to IS NULL;

-- Where each consumer stopped. Written in the same transaction as the data it
-- describes, so a crash never leaves the position ahead of the data.
CREATE TABLE rep.cdc_state (
    consumer   text PRIMARY KEY,
    position   text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

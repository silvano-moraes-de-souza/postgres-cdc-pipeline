-- The operational database: two tables that keep changing.
-- updated_at defaults to now(), which is the transaction START time. That is what
-- most applications do, and it is the reason a watermark can miss a slow commit.

CREATE SCHEMA shop;

CREATE TABLE shop.orders (
    order_id     bigint PRIMARY KEY,
    customer_id  bigint NOT NULL,
    ordered_at   timestamptz NOT NULL,
    status       text NOT NULL,
    channel      text,
    total_cents  bigint NOT NULL,
    delivered_at timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shop.payments (
    payment_id   bigint PRIMARY KEY,
    order_id     bigint NOT NULL,
    method       text NOT NULL,
    status       text NOT NULL,
    amount_cents bigint NOT NULL,
    paid_at      timestamptz,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE FUNCTION shop.touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;

CREATE TRIGGER orders_touch BEFORE UPDATE ON shop.orders
    FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();
CREATE TRIGGER payments_touch BEFORE UPDATE ON shop.payments
    FOR EACH ROW EXECUTE FUNCTION shop.touch_updated_at();

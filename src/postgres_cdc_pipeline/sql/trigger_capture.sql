-- Trigger-based capture: every insert, update and delete writes one row to a log.
-- seq orders the log; txid lets a reader tell which transaction wrote the row.

CREATE SCHEMA cdc;

CREATE TABLE cdc.change_log (
    seq        bigserial PRIMARY KEY,
    txid       bigint NOT NULL DEFAULT txid_current(),
    tbl        text NOT NULL,
    op         char(1) NOT NULL,
    pk         bigint NOT NULL,
    row_data   jsonb,
    changed_at timestamptz NOT NULL DEFAULT now()
);

-- TG_ARGV[0] is the primary key column of the table the trigger is attached to.
CREATE FUNCTION cdc.log_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO cdc.change_log (tbl, op, pk, row_data)
        VALUES (TG_TABLE_NAME, 'D', (to_jsonb(OLD) ->> TG_ARGV[0])::bigint, NULL);
        RETURN OLD;
    END IF;
    INSERT INTO cdc.change_log (tbl, op, pk, row_data)
    VALUES (TG_TABLE_NAME, left(TG_OP, 1), (to_jsonb(NEW) ->> TG_ARGV[0])::bigint, to_jsonb(NEW));
    RETURN NEW;
END $$;

CREATE TRIGGER orders_cdc AFTER INSERT OR UPDATE OR DELETE ON shop.orders
    FOR EACH ROW EXECUTE FUNCTION cdc.log_change('order_id');
CREATE TRIGGER payments_cdc AFTER INSERT OR UPDATE OR DELETE ON shop.payments
    FOR EACH ROW EXECUTE FUNCTION cdc.log_change('payment_id');

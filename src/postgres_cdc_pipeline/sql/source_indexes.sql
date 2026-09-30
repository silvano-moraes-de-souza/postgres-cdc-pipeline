-- Created after the bulk load, so COPY does not maintain them row by row.
-- The watermark query needs these; trigger and WAL capture do not.
CREATE INDEX orders_updated_at ON shop.orders (updated_at);
CREATE INDEX payments_updated_at ON shop.payments (updated_at);
CREATE INDEX payments_order ON shop.payments (order_id);

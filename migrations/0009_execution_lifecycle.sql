ALTER TABLE orders ADD COLUMN client_order_id TEXT;
ALTER TABLE orders ADD COLUMN tx_signature TEXT;
ALTER TABLE orders ADD COLUMN simulation_status TEXT;
ALTER TABLE orders ADD COLUMN simulation_error TEXT;
ALTER TABLE orders ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_client_order_id
    ON orders(client_order_id);

CREATE INDEX IF NOT EXISTS idx_orders_signal_id
    ON orders(signal_id);

CREATE INDEX IF NOT EXISTS idx_copy_signals_status_ts
    ON copy_signals(status, ts);

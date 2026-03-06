DELETE FROM orders
WHERE signal_id NOT IN (
    SELECT signal_id FROM copy_signals
);

DELETE FROM fills
WHERE order_id NOT IN (
    SELECT order_id FROM orders
);

ALTER TABLE orders RENAME TO orders_old;

CREATE TABLE orders (
    order_id TEXT PRIMARY KEY,
    signal_id TEXT NOT NULL REFERENCES copy_signals(signal_id) ON DELETE RESTRICT,
    route TEXT NOT NULL,
    submit_ts TEXT NOT NULL,
    confirm_ts TEXT,
    status TEXT NOT NULL,
    err_code TEXT,
    client_order_id TEXT,
    tx_signature TEXT,
    simulation_status TEXT,
    simulation_error TEXT,
    attempt INTEGER NOT NULL DEFAULT 1,
    applied_tip_lamports INTEGER,
    ata_create_rent_lamports INTEGER,
    network_fee_lamports_hint INTEGER,
    base_fee_lamports_hint INTEGER,
    priority_fee_lamports_hint INTEGER
);

INSERT INTO orders(
    order_id,
    signal_id,
    route,
    submit_ts,
    confirm_ts,
    status,
    err_code,
    client_order_id,
    tx_signature,
    simulation_status,
    simulation_error,
    attempt,
    applied_tip_lamports,
    ata_create_rent_lamports,
    network_fee_lamports_hint,
    base_fee_lamports_hint,
    priority_fee_lamports_hint
)
SELECT
    order_id,
    signal_id,
    route,
    submit_ts,
    confirm_ts,
    status,
    err_code,
    client_order_id,
    tx_signature,
    simulation_status,
    simulation_error,
    attempt,
    applied_tip_lamports,
    ata_create_rent_lamports,
    network_fee_lamports_hint,
    base_fee_lamports_hint,
    priority_fee_lamports_hint
FROM orders_old;

DROP TABLE orders_old;

CREATE INDEX IF NOT EXISTS idx_orders_status_submit_ts
    ON orders(status, submit_ts);

CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_client_order_id
    ON orders(client_order_id);

CREATE INDEX IF NOT EXISTS idx_orders_signal_id
    ON orders(signal_id);

CREATE TRIGGER IF NOT EXISTS trg_orders_client_order_id_not_null_insert
BEFORE INSERT ON orders
FOR EACH ROW
WHEN NEW.client_order_id IS NULL OR TRIM(NEW.client_order_id) = ''
BEGIN
    SELECT RAISE(ABORT, 'orders.client_order_id must be non-empty');
END;

CREATE TRIGGER IF NOT EXISTS trg_orders_client_order_id_not_null_update
BEFORE UPDATE OF client_order_id ON orders
FOR EACH ROW
WHEN NEW.client_order_id IS NULL OR TRIM(NEW.client_order_id) = ''
BEGIN
    SELECT RAISE(ABORT, 'orders.client_order_id must be non-empty');
END;

CREATE TRIGGER IF NOT EXISTS trg_orders_fee_breakdown_non_negative_insert
BEFORE INSERT ON orders
FOR EACH ROW
WHEN (NEW.applied_tip_lamports IS NOT NULL AND NEW.applied_tip_lamports < 0)
   OR (NEW.ata_create_rent_lamports IS NOT NULL AND NEW.ata_create_rent_lamports < 0)
   OR (NEW.network_fee_lamports_hint IS NOT NULL AND NEW.network_fee_lamports_hint < 0)
   OR (NEW.base_fee_lamports_hint IS NOT NULL AND NEW.base_fee_lamports_hint < 0)
   OR (NEW.priority_fee_lamports_hint IS NOT NULL AND NEW.priority_fee_lamports_hint < 0)
BEGIN
    SELECT RAISE(ABORT, 'orders fee breakdown lamports must be non-negative');
END;

CREATE TRIGGER IF NOT EXISTS trg_orders_fee_breakdown_non_negative_update
BEFORE UPDATE OF
    applied_tip_lamports,
    ata_create_rent_lamports,
    network_fee_lamports_hint,
    base_fee_lamports_hint,
    priority_fee_lamports_hint ON orders
FOR EACH ROW
WHEN (NEW.applied_tip_lamports IS NOT NULL AND NEW.applied_tip_lamports < 0)
   OR (NEW.ata_create_rent_lamports IS NOT NULL AND NEW.ata_create_rent_lamports < 0)
   OR (NEW.network_fee_lamports_hint IS NOT NULL AND NEW.network_fee_lamports_hint < 0)
   OR (NEW.base_fee_lamports_hint IS NOT NULL AND NEW.base_fee_lamports_hint < 0)
   OR (NEW.priority_fee_lamports_hint IS NOT NULL AND NEW.priority_fee_lamports_hint < 0)
BEGIN
    SELECT RAISE(ABORT, 'orders fee breakdown lamports must be non-negative');
END;

ALTER TABLE fills RENAME TO fills_old;

CREATE TABLE fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL REFERENCES orders(order_id) ON DELETE RESTRICT,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_price REAL NOT NULL,
    fee REAL NOT NULL DEFAULT 0,
    slippage_bps REAL NOT NULL DEFAULT 0
);

INSERT INTO fills(id, order_id, token, qty, avg_price, fee, slippage_bps)
SELECT id, order_id, token, qty, avg_price, fee, slippage_bps
FROM fills_old;

DROP TABLE fills_old;

CREATE UNIQUE INDEX IF NOT EXISTS idx_fills_order_id
    ON fills(order_id);

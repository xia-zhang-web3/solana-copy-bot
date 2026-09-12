-- Rebuild fills without changing historical values, IDs or legacy semantics.
-- The migration runner restores additional indexes/triggers. Incoming FKs keep the name.
-- Fail before writes in old/raw runners that cannot preserve FK actions and custom DDL.
SELECT enabled FROM temp.execution_receipt_cash_rebuild_guard;
-- The runner disables FK actions outside the transaction and validates them before commit.
CREATE TEMP TABLE b07_fill_sequence AS SELECT seq FROM sqlite_sequence WHERE name = 'fills';
CREATE TABLE fills_cash_upgrade (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL REFERENCES orders(order_id) ON DELETE RESTRICT,
    token TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_price REAL,
    fee REAL DEFAULT 0,
    slippage_bps REAL DEFAULT 0,
    notional_lamports INTEGER,
    fee_lamports INTEGER,
    qty_raw TEXT,
    qty_decimals INTEGER,
    accounting_basis TEXT NOT NULL DEFAULT 'legacy_unclassified'
        CHECK(accounting_basis IN ('legacy_unclassified','receipt_native_cash')),
    position_id TEXT,
    wallet_native_delta_lamports INTEGER,
    entry_basis_lamports INTEGER,
    cash_result_delta_lamports INTEGER,
    accumulated_cash_result_lamports INTEGER,
    remaining_qty_raw TEXT,
    remaining_cost_lamports INTEGER,
    settlement_ts TEXT,
    CHECK(accounting_basis != 'receipt_native_cash' OR (
        avg_price IS NULL AND fee IS NULL AND fee_lamports IS NULL
        AND slippage_bps IS NULL AND notional_lamports IS NULL
        AND position_id IS NOT NULL AND qty_raw IS NOT NULL
        AND typeof(qty_decimals) = 'integer' AND qty_decimals BETWEEN 0 AND 255
        AND typeof(wallet_native_delta_lamports) = 'integer'
        AND typeof(entry_basis_lamports) = 'integer' AND entry_basis_lamports >= 0
        AND typeof(cash_result_delta_lamports) = 'integer'
        AND typeof(accumulated_cash_result_lamports) = 'integer'
        AND remaining_qty_raw IS NOT NULL
        AND typeof(remaining_cost_lamports) = 'integer' AND remaining_cost_lamports >= 0
        AND settlement_ts IS NOT NULL
    ))
);
INSERT INTO fills_cash_upgrade(id,order_id,token,qty,avg_price,fee,slippage_bps,
    notional_lamports,fee_lamports,qty_raw,qty_decimals)
SELECT id,order_id,token,qty,avg_price,fee,slippage_bps,
    notional_lamports,fee_lamports,qty_raw,qty_decimals FROM fills;
DROP TABLE fills;
ALTER TABLE fills_cash_upgrade RENAME TO fills;
CREATE UNIQUE INDEX idx_fills_order_id ON fills(order_id);
UPDATE sqlite_sequence SET seq = MAX(seq, COALESCE((SELECT MAX(seq) FROM b07_fill_sequence),0))
    WHERE name = 'fills';
INSERT INTO sqlite_sequence(name,seq)
    SELECT 'fills',MAX(seq) FROM b07_fill_sequence
    HAVING MAX(seq) IS NOT NULL AND NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name = 'fills');
DROP TABLE b07_fill_sequence;

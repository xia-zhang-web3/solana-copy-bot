-- Non-runnable preparation owned solely by the storage staging API.
-- No FK to observed_swaps, positions or witnesses: retention must not delete
-- or rebind this immutable event/generation snapshot. Promotion is a future API.
CREATE TABLE execution_source_sell_intents (
    intent_id TEXT PRIMARY KEY NOT NULL,
    event_signature TEXT NOT NULL UNIQUE,
    source_wallet TEXT NOT NULL,
    dex TEXT NOT NULL,
    token TEXT NOT NULL,
    token_out TEXT NOT NULL,
    amount_in REAL NOT NULL,
    amount_out REAL NOT NULL,
    slot INTEGER NOT NULL CHECK(typeof(slot)='integer' AND slot>=0),
    event_ts TEXT NOT NULL,
    amount_in_raw TEXT,
    amount_in_decimals INTEGER,
    amount_out_raw TEXT,
    amount_out_decimals INTEGER,
    position_id TEXT NOT NULL,
    buy_fill_id INTEGER NOT NULL CHECK(typeof(buy_fill_id)='integer'),
    buy_order_id TEXT NOT NULL,
    buy_signal_id TEXT NOT NULL,
    buy_tx_signature TEXT NOT NULL,
    buy_execution_wallet TEXT NOT NULL,
    staged_at TEXT NOT NULL,
    CHECK(length(intent_id)>0 AND length(event_signature)>0 AND length(source_wallet)>0
        AND length(position_id)>0 AND length(buy_order_id)>0 AND length(buy_signal_id)>0
        AND length(buy_tx_signature)>0 AND length(buy_execution_wallet)>0),
    CHECK((amount_in_raw IS NULL AND amount_in_decimals IS NULL
            AND amount_out_raw IS NULL AND amount_out_decimals IS NULL)
        OR (amount_in_raw IS NOT NULL AND amount_out_raw IS NOT NULL
            AND typeof(amount_in_decimals)='integer' AND amount_in_decimals BETWEEN 0 AND 255
            AND typeof(amount_out_decimals)='integer' AND amount_out_decimals BETWEEN 0 AND 255))
);

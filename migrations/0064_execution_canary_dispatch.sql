-- Identity and fair recovery only; no monetary reservation or historical backfill.
CREATE TABLE execution_canary_dispatch (
    order_id TEXT PRIMARY KEY REFERENCES orders(order_id) ON DELETE RESTRICT,
    signal_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    route TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK(attempt >= 1),
    wallet TEXT NOT NULL CHECK(length(wallet) > 0),
    token TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy','sell')),
    tx_signature TEXT NOT NULL UNIQUE CHECK(length(tx_signature) > 0),
    transaction_sha256 TEXT NOT NULL CHECK(length(transaction_sha256) = 64),
    message_sha256 TEXT NOT NULL CHECK(length(message_sha256) = 64),
    claimed_at TEXT NOT NULL,
    transport_note TEXT NOT NULL DEFAULT 'dispatch_outcome_unknown'
);
CREATE TABLE execution_canary_reconcile_attempts (
    order_id TEXT PRIMARY KEY REFERENCES orders(order_id) ON DELETE RESTRICT,
    last_attempt_at TEXT NOT NULL
);
-- Explicit legacy policy: unsigned submitted stays unknown; signed expired/failed
-- without completed accounting returns to recovery. Unsigned pre-dispatch states
-- have no recoverable identity: do not invent one from hashes or backfill money.
CREATE VIEW execution_canary_unresolved_dispatch AS
SELECT o.order_id, o.route, COALESCE(d.side, lower(s.side), 'buy') AS side,
       COALESCE(d.token, s.token) AS token, o.tx_signature
FROM orders o
LEFT JOIN execution_canary_dispatch d ON d.order_id=o.order_id
LEFT JOIN copy_signals s ON s.signal_id=o.signal_id
WHERE o.order_id LIKE 'exec-canary:%'
  AND (d.order_id IS NOT NULL OR o.status IN
       ('execution_canary_submitted','execution_canary_confirmed_unreconciled','execution_canary_confirmed')
       OR (o.status='execution_canary_simulated' AND o.simulation_error LIKE 'retry_after_unknown_submit_timeout%')
       OR (o.status IN ('execution_canary_expired','execution_canary_failed')
           AND length(trim(COALESCE(o.tx_signature,''))) > 0))
  AND NOT EXISTS (SELECT 1 FROM fills f WHERE f.order_id=o.order_id)
  AND NOT EXISTS (SELECT 1 FROM execution_failed_expense_tasks t
       JOIN execution_failed_expense_ledger l ON l.order_id=t.order_id AND l.tx_signature=t.tx_signature
       WHERE t.order_id=o.order_id AND t.status='complete' AND t.attempt=o.attempt
         AND t.tx_signature=o.tx_signature AND l.payer=t.wallet
         AND (d.order_id IS NULL OR (d.wallet=t.wallet AND d.tx_signature=t.tx_signature)));

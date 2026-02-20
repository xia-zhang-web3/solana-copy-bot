# Execution Manual Reconcile Runbook

This runbook defines mandatory operator actions for execution incidents where runtime had to use fallback pricing during confirm/reconcile.

## 1) Trigger Events

Run this procedure immediately when any of these `risk_events.type` values appear:

1. `execution_price_unavailable_fallback_used`
2. `execution_confirm_price_unavailable_manual_reconcile_required`
3. `execution_confirm_price_unavailable`
4. `execution_confirm_failed_manual_reconcile_required`
5. `execution_confirm_timeout_manual_reconcile_required`

## 2) Immediate Containment (<= 2 minutes)

1. Pause new BUY submits via operator emergency-stop flag:

```bash
mkdir -p state
printf "execution_manual_reconcile_in_progress\n" > state/operator_emergency_stop.flag
```

2. Keep runtime running: SELL and confirm/recovery paths must continue.
3. Confirm pause state in logs (`execution BUY submission paused ...`).

## 3) Evidence Collection

1. Capture runtime snapshot:

```bash
CONFIG_PATH=configs/paper.toml ./tools/runtime_snapshot.sh 24 180
```

2. Capture fallback-event report:

```bash
CONFIG_PATH=configs/paper.toml ./tools/execution_price_fallback_report.sh 24
```

3. Persist both outputs in incident artifacts before any manual DB correction.

## 4) Triage Rules

1. Treat as `P0` if any event has `reason=missing_latest_price_no_fallback`.
2. Treat as `P0` if `tx_signature` is missing or cannot be verified from your execution telemetry source.
3. Otherwise classify as `P1` and continue manual reconcile.
4. Create/update incident note with: `signal_id`, `order_id`, `token`, `route`, `fallback_source`, `tx_signature`, timestamp.

## 5) Manual Reconcile Workflow

1. For each affected order, fetch actual executed values (`qty`, `avg_price`, `fee`) from your execution telemetry source.
2. Create SQLite backup before edits:

```bash
sqlite3 "$DB_PATH" ".backup '${DB_PATH}.bak.$(date -u +%Y%m%dT%H%M%SZ)'"
```

3. Apply correction only in explicit transaction and only after peer review:

```sql
BEGIN IMMEDIATE TRANSACTION;
-- 1) update fills row for affected order_id
-- 2) update corresponding position exposure/PnL state
-- 3) verify orders/fills/positions consistency for token
COMMIT;
```

4. If reconcile cannot be completed safely, keep emergency stop active and escalate.

## 6) Exit Criteria (Resume BUY Submit)

Resume BUY submit only when all are true:

1. Every new fallback event in incident window has reconcile status (`done` or explicitly accepted risk with owner+deadline).
2. Exposure view (`positions`) is consistent with corrected fills for impacted tokens.
3. Incident log is updated with exact SQL/change evidence and reviewer sign-off.

Clear emergency-stop flag:

```bash
rm -f state/operator_emergency_stop.flag
```

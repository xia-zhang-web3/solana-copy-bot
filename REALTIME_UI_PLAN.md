# Realtime UI Incident Plan (Postmortem + Recovery)

## Scope
This document summarizes the production issues observed after enabling the embedded realtime UI and defines immediate and follow-up actions.

## What Broke
1. Startup blocking during migrations.
2. High ingestion backpressure and `risk_infra` buy blocking.
3. Discovery cycles became too heavy and stale.
4. Discovery API frequently timed out in UI.
5. Event/log spam from risk and feed updates.
6. UI rendered too many rows/events at once (high client load).
7. Operational confusion from mixed causes (UI, discovery load, risk guard, infra queue saturation).

## Evidence From Logs
1. Discovery cycle duration reached ~198s.
2. Discovery processed very large window deltas (`swaps_delta_fetched` around 1.8M, `metrics_written` around 206k).
3. Ingestion queue stayed near max (`ws_to_fetch_queue_depth` near capacity).
4. `ws_notifications_replaced_oldest / ws_notifications_enqueued` stayed very high (~0.92+).
5. `shadow risk infra stop activated` repeatedly blocked new BUY decisions.
6. RPC limit signals were not primary (`rpc_429=0`, `rpc_5xx=0` in sampled metrics).

## Root Causes
1. Discovery workload was too expensive for current runtime/DB profile and window size.
2. Ingestion queue pressure triggered infra guard thresholds by design.
3. UI integration added extra local read/write/event pressure (SQLite/UI-event path), amplifying contention.
4. Early migration strategy included heavy index creation on startup path.
5. API/UI timeout budget for discovery view was too tight for worst-case cycle duration.

## User-Visible Symptoms
1. UI reachable but API shown as `loading`/`degraded`.
2. Tables/event feed appeared to flood.
3. Discovery tab often failed to load.
4. New positions not opening while open positions could still close.
5. Frequent `risk_infra` warnings in logs.

## Emergency Actions Executed
1. Full Git rollback to pre-UI state (all UI commits reverted, no force-push rewrite).
2. `.env` cleanup: removed `SOLANA_COPY_BOT_WEB_*` runtime overrides.
3. Preserved bot core path without embedded web server dependencies.

## Immediate Recovery Checklist
1. Deploy rollback build and restart service.
2. Confirm no web endpoints are expected in this rollback mode.
3. Monitor ingestion metrics for 15-30 minutes.
4. Verify `ingestion_lag_ms_p95`, queue depth, and replaced ratio return to stable baseline.

## Hard Requirements Before Any Re-Enable
1. Discovery must have bounded workload per cycle.
2. Discovery and ingestion contention must be isolated (or rate-limited).
3. UI event persistence must be sampled/throttled by design.
4. Heavy index migrations must not block startup.
5. Clear guard telemetry must distinguish cause: discovery load vs ingestion source pressure.

## Reintroduction Strategy (Phase-Based)
1. Phase A: backend-only instrumentation and strict budgets (no UI).
2. Phase B: read-only lightweight endpoints without event feed.
3. Phase C: staged UI with pagination + hard caps + adaptive refresh.
4. Phase D: enable live feed only with server-side sampling and backpressure policies.

## Acceptance Criteria
1. `ingestion_lag_ms_p95` remains within agreed baseline under normal load.
2. `replaced_ratio` remains below guard threshold for sustained windows.
3. Discovery cycle duration remains below refresh interval budget.
4. No startup-blocking migrations in hot path.
5. No unbounded UI/event/log growth.

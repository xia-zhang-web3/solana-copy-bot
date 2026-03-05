# UI Monitoring Design Brief (Desktop + Mobile)

## 1) Purpose

This document defines exactly what the monitoring UI must show for `solana-copy-bot` so operators can:

1. Detect failures early.
2. Understand impact fast.
3. Decide the next action without opening logs first.
4. Keep high signal density without interface overload.

Date of baseline: **2026-03-04**.

---

## 2) Scope

In scope:

1. Runtime monitoring for `copybot-app`, `copybot-executor`, `copybot-adapter`.
2. Source health (Yellowstone/Helius), discovery quality, execution lifecycle, risk, infra.
3. Operator actions (pause/reconcile/failover/preflight checks).
4. Mobile-first triage behavior.

Out of scope:

1. Strategy research UI.
2. Manual trade-entry terminal.
3. Full BI dashboard for historical analytics.

---

## 3) Design Principles (Hard Requirements)

1. **Anomaly-first**: show abnormal states first, normal states compressed.
2. **One-screen triage**: on mobile, operator must diagnose top incident in <30s.
3. **Progressive disclosure**: summary -> details -> raw evidence.
4. **No secret exposure**: never render secrets/tokens/private keys; only `configured/not configured`.
5. **Action safety**: dangerous actions require explicit confirmation + reason text + audit trail.
6. **Fail-closed visibility**: if a guard blocks execution, UI explains which contract/gate fired.

---

## 4) Runtime Topology To Visualize

Core runtime nodes:

1. `copybot-app` (orchestrator): ingestion + discovery + shadow + execution loop.
2. `copybot-executor` (execution backend): `/simulate`, `/submit`, `/healthz`.
3. `copybot-adapter` (execution gateway): `/simulate`, `/submit`, `/healthz`, fallback/send-rpc path.
4. `sqlite` storage for swaps/signals/orders/fills/positions/risk/system events.

Data flow (visual sequence):

1. Ingestion receives swaps (`yellowstone_grpc` or `helius_ws`).
2. Swaps persisted to `observed_swaps`.
3. Discovery cycle recomputes wallet metrics/followlist/quality.
4. Shadow creates `copy_signals` with `shadow_recorded`.
5. Execution pipeline advances statuses through simulate/submit/confirm.
6. Finalize updates `orders + fills + positions + signal status`.
7. Risk events and heartbeat events are emitted for monitoring.

---

## 5) Operators and Core Questions

Primary user:

1. On-call operator (desktop + mobile).

Secondary users:

1. Risk owner.
2. Runtime engineer.

The UI must answer these questions immediately:

1. Are we healthy right now?
2. If not, where is the bottleneck (source/discovery/execution/infra/risk)?
3. Is trading blocked by policy, risk gate, outage gate, or operator stop?
4. Which signals/orders are stuck, failing, or timing out?
5. Is failover active and why?
6. What action is safe to run now?

---

## 6) Canonical State Models

### 6.1 Execution Signal Lifecycle

Main statuses to support:

1. `shadow_recorded`
2. `execution_pending`
3. `execution_simulated`
4. `execution_submitted`
5. `execution_confirmed`
6. `execution_failed`
7. `execution_dropped`

Critical transitions:

1. `shadow_recorded -> execution_pending`
2. `execution_pending -> execution_simulated` (if simulation enabled)
3. `execution_simulated -> execution_submitted`
4. `execution_submitted -> execution_confirmed`
5. Any pre-submit state -> `execution_dropped` on stale/risk blocked
6. Active execution states -> `execution_failed` on terminal failure/time budget exhaustion

### 6.2 Incident State Model

1. `HEALTHY`
2. `DEGRADED_BACKPRESSURE`
3. `DEGRADED_DISCOVERY_SATURATED`
4. `DEGRADED_DISCOVERY_STALLED`
5. `FAILOVER_ARMED`
6. `FAILOVER_COOLDOWN`
7. `CRITICAL_RUNTIME_OOM`
8. `RECOVERING`

---

## 7) Information Architecture

### 7.1 Desktop Navigation

Main sections:

1. `Overview`
2. `Execution`
3. `Sources & Ingestion`
4. `Discovery Quality`
5. `Risk & Exposure`
6. `Incidents`
7. `Runtime Config`
8. `Actions`

Global top status bar:

1. `env`, `execution_mode`, `execution_enabled`
2. Active source + override state
3. Emergency stop status
4. Global health severity
5. Last snapshot time

### 7.2 Mobile Navigation

Tabs:

1. `Overview`
2. `Actions`
3. `Incidents`

Sticky banners (always on top when active):

1. Emergency stop active
2. Failover armed/cooldown
3. Preflight fail
4. Critical incident count

---

## 8) Card-Level Specification (What to Show)

## 8.1 Overview (P0 screen)

Card `Runtime Health`:

1. Health of app/executor/adapter.
2. Restart count (windowed).
3. OOM/recent kill signals.
4. Last heartbeat age per component.

Card `Execution Throughput`:

1. Signals in each lifecycle status.
2. Orders submitted/confirmed/failed (rolling windows).
3. Confirm latency p50/p95/p99.
4. Retry rate and timeout count.

Card `Source Health`:

1. `source_current`.
2. `source_override_active`, reason, timestamp.
3. Ingestion rate.
4. Lag p95/p99.
5. Drop/backpressure/replaced ratios.

Card `Discovery Quality`:

1. `cycle_duration_ms` (p50/p95/max).
2. `still_running_count`.
3. `swaps_fetch_limit_reached_ratio`.
4. `swaps_evicted_due_cap`.
5. `eligible_wallets`, `active_follow_wallets`, `follow_coverage`.

Card `Risk Snapshot`:

1. Open exposure (SOL).
2. Open positions count.
3. Unrealized/realized PnL windows.
4. Drawdown window.
5. Recent risk event count by severity.

## 8.2 Execution Screen

Section `Pipeline Funnel`:

1. Count per stage.
2. Stage-to-stage conversion.
3. Drop-off reasons by stage.

Section `Failure Taxonomy`:

1. Top reason codes (pretrade/simulate/submit/confirm/risk/idempotency).
2. Retryable vs terminal split.
3. Manual reconcile required count.

Section `Live Queue`:

1. Recent active signals/orders.
2. Age, attempts, route, current stage, last error.
3. Stuck detector (age > stage budget).

Section `Transport Path`:

1. `upstream_signature` vs `adapter_send_rpc`.
2. Route chosen and fallback usage.
3. Policy echo status.

## 8.3 Sources & Ingestion Screen

Section `Ingestion SLO`:

1. Rate, lag p95/p99.
2. Queue depths.
3. Backpressured/replaced/drop ratios.
4. Reconnect deltas.
5. RPC 429/5xx deltas.

Section `Failover`:

1. Current source.
2. Override source + reason + age.
3. Cooldown remaining.
4. Last failover trigger event.

Section `Decode/Parser Quality`:

1. Parse rejected.
2. gRPC decode errors.
3. Stream gap indicator.
4. Prefetch stale drops.

## 8.4 Discovery Quality Screen

Section `Cycle Health`:

1. Duration distribution.
2. Running/skipped cycles.
3. Cap pressure indicators.

Section `Followlist Effectiveness`:

1. `wallets_seen`
2. `eligible_wallets`
3. `active_follow_wallets`
4. Promotion/demotion counts.

Section `Score Quality`:

1. Score distribution p50/p90/p99.
2. Tradable ratio distribution.
3. Rug ratio distribution.
4. Top-N wallets by score with quality flags.

Section `Quality Cache`:

1. RPC attempted/success ratio.
2. Budget exhausted count.
3. Cache miss count.
4. RPC spend ms.

## 8.5 Risk & Exposure Screen

Section `Exposure`:

1. Total open exposure.
2. Exposure by token.
3. Max exposure limit usage.

Section `P/L & Drawdown`:

1. Realized PnL by window.
2. Unrealized PnL.
3. Max drawdown by window.

Section `Risk Events`:

1. Event count by type and severity.
2. Last critical events timeline.
3. Links to affected signals/orders.

Section `Policy Gates`:

1. Current state of critical gates.
2. Last trigger reason and timestamp.

## 8.6 Incidents Screen

Section `Incident Timeline`:

1. OOM/restart events.
2. Failover arm/clear events.
3. Manual reconcile required events.
4. High-severity risk events.

Section `Incident Details Drawer`:

1. Detection time.
2. Affected domain.
3. Impacted metrics.
4. Suggested action.
5. Links to evidence/snapshot.

## 8.7 Runtime Config Screen

Read-only effective config (redacted):

1. System identity (`env`, active config path, db path, service names).
2. Ingestion source precedence (TOML, env override, override file, effective).
3. Execution mode + route policy + allowed routes.
4. Simulation/submit/verify budgets.
5. Risk limits and killswitch flags.
6. Auth settings as `configured/not configured`.

---

## 9) Alerts Catalog (Must Have)

Severity levels:

1. `Critical`
2. `High`
3. `Medium`
4. `Info`

Priority alerts:

1. `healthz.status != ok` or idempotency store unhealthy -> Critical.
2. `executor_submit_timeout_budget_exceeded` -> Critical.
3. `confirm_timeout_manual_reconcile_required` or `confirm_error_manual_reconcile_required` -> Critical.
4. `execution_signal_stale` above freshness budget -> High.
5. `execution_submit_fallback_blocked` -> High.
6. `upstream_submit_signature_unseen` in strict verify mode -> High.
7. `network_fee_unavailable_fallback_used` or `price_unavailable_fallback_used` -> Medium.
8. OOM restart loop detection -> Critical.
9. Discovery saturation (`fetch_limit_reached_ratio` sustained high) -> High.

Alert UX behavior:

1. Critical alerts pinned in top bar and mobile sticky.
2. Every alert has `what happened`, `why it matters`, `what to do now`.
3. Ack is separate from resolve; both audited.

---

## 10) Mobile Adaptation Rules

Use three information tiers:

1. P0 (always visible): health, source/failover, critical incidents, execution blocked state.
2. P1 (one tap): lag/backpressure, execution funnel summary, risk snapshot.
3. P2 (drilldown): detailed distributions, per-wallet/per-order tables, raw evidence.

Mobile card constraints:

1. Max 5 primary KPI cards on first viewport.
2. Each card shows max 3 numeric lines + 1 trend sparkline.
3. No dense tables on first layer; use row drawer.

Interaction:

1. Tap card -> details sheet.
2. Swipe incident row -> quick actions (`ack`, `open runbook`, `open evidence`).
3. Long press critical banner -> emergency actions panel.

---

## 11) Anti-Overload Rules

1. Hide green metrics by default when incident is active.
2. Group correlated metrics into one health score tile with expandable internals.
3. Show rates and deltas, not raw counters only.
4. Prefer top-N reasons over full reason list.
5. Collapse low-severity alerts after acknowledgement.
6. Use uniform units and explicit windows (`5m`, `15m`, `1h`, `24h`).

---

## 12) Refresh Policy

Recommended refresh:

1. P0 critical status cards: 2-5 seconds.
2. Throughput/lag/queue metrics: 5 seconds.
3. Execution funnel and failure taxonomy: 10-15 seconds.
4. Risk/PnL aggregates: 30-60 seconds.
5. Runtime config: on open + manual refresh.
6. Incidents timeline: push or 5 seconds poll fallback.

Timestamp discipline:

1. Every card has `last_update_ts`.
2. If stale > expected TTL, card enters `data stale` warning state.

---

## 13) Operator Actions Required in UI

Action group `Safety`:

1. Toggle emergency stop BUY.
2. View reason and operator note.

Action group `Recovery`:

1. Start manual reconcile flow.
2. Open reconcile checklist.

Action group `Source Control`:

1. Arm failover override.
2. Clear override.
3. View cooldown state.

Action group `Runtime Checks`:

1. Run preflight.
2. Run snapshot.
3. Probe `/healthz` and `/simulate` auth.

Action group `Service Ops`:

1. Restart hints and order (`executor -> adapter -> app` for execution incidents).
2. Quick links to relevant logs/evidence outputs.

Each action must capture:

1. Who triggered.
2. Why.
3. When.
4. Result.

---

## 14) Data Inventory for UI

Primary entities:

1. `copy_signals` (signal queue + lifecycle).
2. `orders` (execution status, attempts, error semantics, fee hints).
3. `fills` (finalized execution fill and fee).
4. `positions` (open/closed exposure).
5. `shadow_lots` and `shadow_closed_trades`.
6. `wallet_metrics` (discovery quality windows and scores).
7. `observed_swaps` (ingestion feed).
8. `risk_events` (risk/incident channel).
9. `system_heartbeat` (component health heartbeat).
10. Runtime snapshots and computed summaries from `ops/server_reports/raw/*/computed_summary.json`.

Known gap to plan for:

1. Storage currently has limited read APIs for full heartbeat/risk event timeline; dedicated read endpoints or query layer should be defined for UI.

---

## 15) Suggested API/View-Model Contract (for frontend/backend alignment)

Return compact normalized objects:

1. `runtime_status`
2. `source_health`
3. `execution_funnel`
4. `execution_failures_top`
5. `risk_snapshot`
6. `discovery_quality`
7. `incident_feed`
8. `effective_config`
9. `operator_actions_audit`

Each object should include:

1. `window`
2. `value`
3. `threshold_warning`
4. `threshold_critical`
5. `status` (`ok|warn|critical|stale`)
6. `last_update_ts`
7. `source` (table/module/report)

---

## 16) Visual Priority for Designer

Priority order for attention design:

1. Safety state (can we trade safely?).
2. Source health and lag.
3. Execution stuck/fail states.
4. Discovery saturation and followlist effectiveness.
5. Risk exposure and drawdown.
6. Infra stability (OOM/restarts).

Color semantics:

1. Neutral for normal.
2. Amber for degradations.
3. Red for critical.
4. Blue only for informational/config context.

Do not encode severity by color only; always add icon + label.

---

## 17) Current Observed Runtime Snapshot (Reference For Mockups)

Reference observations on **2026-03-04**:

1. Ingestion stabilized after patch around `17:10:52Z` with high throughput and low lag.
2. Main active concern shifted to discovery saturation:
   1. High `swaps_fetch_limit_reached` frequency.
   2. High cap eviction (`swaps_evicted_due_cap`).
   3. `active_follow_wallets` and `eligible_wallets` observed at zero in latest reports.
3. Earlier infrastructure incident around `11:34:06Z` showed OOM restart loop behavior.

Mockups must include states for both:

1. Healthy ingestion + degraded discovery.
2. Critical OOM/restart incident.

---

## 18) Source-of-Truth File Groups Used

Architecture and runtime:

1. `README.md`
2. `ARCHITECTURE_BLUEPRINT.md`
3. `ROAD_TO_PRODUCTION.md`
4. `REALTIME_UI_PLAN.md`

Core code:

1. `crates/app/src/*`
2. `crates/execution/src/*`
3. `crates/executor/src/*`
4. `crates/adapter/src/*`
5. `crates/ingestion/src/*`
6. `crates/discovery/src/*`
7. `crates/shadow/src/*`
8. `crates/storage/src/*`
9. `crates/config/src/*`

Schema and runtime reports:

1. `migrations/*.sql`
2. `ops/server_reports/*.md`
3. `ops/server_reports/raw/*/computed_summary.json`
4. `ops/*runbook*.md`
5. `tools/runtime_snapshot.sh`
6. `tools/runtime_watch.sh`
7. `tools/ingestion_failover_watchdog.sh`
8. `configs/*.toml`

---

## 19) Handoff Checklist For Designer

Design is acceptable only if all items are covered:

1. Desktop IA includes all 8 sections from this brief.
2. Mobile IA includes `Overview`, `Actions`, `Incidents` tabs.
3. Critical sticky banners exist on mobile.
4. Execution lifecycle states are visualized end-to-end.
5. Incident timeline supports drilldown and action context.
6. Runtime config screen shows effective values and source precedence.
7. Anti-overload rules are implemented (progressive disclosure and anomaly-first behavior).
8. Every KPI card includes freshness timestamp and status color+label+icon.
9. Operator actions include confirmation and audit metadata fields.
10. No secrets are displayed in any view.


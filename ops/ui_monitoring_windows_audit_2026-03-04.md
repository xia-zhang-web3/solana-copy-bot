# UI Monitoring Windows Audit (Pencil)

Date: `2026-03-04`  
Target: active Pencil document `new`  
Spec baseline: `UI_MONITORING_DESIGN_BRIEF_2026-03-04.md`

## 1) Audit Scope

1. Full audit of all desktop windows.
2. Full audit of mobile IA and triage coverage.
3. Full audit against `.md` requirements for:
   1. reports visibility,
   2. metrics collection visibility,
   3. trade/execution information visibility.
4. Explicit check that UI model does not require extra chain RPC polling.

## 2) Desktop Audit (against section 7.1 + section 8)

1. `Desktop - Overview` (`6mhGo`)  
   Status: `PASS`  
   Covered:
   1. Runtime health.
   2. Execution throughput.
   3. Source health.
   4. Discovery quality.
   5. Risk snapshot.
   6. Recent incidents.
   7. Reports & metrics references added.

2. `Desktop - Execution` (`okZw6`)  
   Status: `PASS`  
   Covered:
   1. Pipeline funnel summary.
   2. Failure taxonomy summary.
   3. Transport path/policy echo summary.
   4. Orders/trades table with aligned columns.

3. `Desktop - Sources & Ingestion` (`vKnjy`)  
   Status: `PASS`  
   Covered:
   1. Ingestion SLO (rate/lag/queue/rpc errors).
   2. Failover status.
   3. Decode/parser quality.
   4. Explicit no-direct-RPC UI note.

4. `Desktop - Discovery Quality` (`2OEsK`)  
   Status: `PASS`  
   Covered:
   1. Cycle health.
   2. Followlist effectiveness.
   3. Quality cache.
   4. Followlist wallet table.

5. `Desktop - Risk & Exposure` (`XHR6Q`)  
   Status: `PASS`  
   Covered:
   1. Exposure.
   2. P/L and drawdown.
   3. Critical risk events timeline.
   4. Policy gates block added.

6. `Desktop - Incidents` (`NFPK3`)  
   Status: `PASS`  
   Covered:
   1. Incident timeline table.
   2. Incident details drawer block (detection/domain/impact/action/evidence).

7. `Desktop - Runtime Config` (`egsTA`)  
   Status: `PASS`  
   Covered:
   1. Read-only key/value/source table.
   2. Effective config and source-precedence summary.
   3. Reports and computed summary references.
   4. Explicit UI query policy (local metrics/DB only).

8. `Desktop - Actions` (`vFwV3`)  
   Status: `PASS`  
   Covered:
   1. Safety actions.
   2. Recovery actions.
   3. Source control actions.
   4. Runtime checks.
   5. Explicit read-only/no-direct-RPC note.

## 3) Mobile Audit (against section 7.2 + section 10)

1. Mobile bottom navigation (`OESf6`)  
   Status: `PASS`  
   Tabs now:
   1. `Overview`
   2. `Actions`
   3. `Incidents`

2. `Mobile - P0 Overview` (`CySh2`)  
   Status: `PASS`  
   Covered:
   1. Sticky emergency/critical banners.
   2. Runtime health.
   3. Execution throughput.
   4. Source health.
   5. Discovery quality.

3. `Mobile - Actions` (`LrTqe`)  
   Status: `PASS`  
   Covered:
   1. Safety controls.
   2. Recovery controls.
   3. Latest trade event card.
   4. Explicit no-direct-RPC UI mode line.

4. `Mobile - Incidents` (`ZZ58w`)  
   Status: `PASS`  
   Covered:
   1. Incident card with severity and operator action.

5. Additional mobile drilldowns retained (`rZZoz`, `q5c5f`, `nxZ6o`, `G85RT`)  
   Status: `PASS (optional P2 drilldown)`  
   Note: these screens preserve deeper triage detail and can be reached from menu/secondary flows.

## 4) Required Information Coverage

1. Reports: visible via Overview + Runtime Config references to `ops/server_reports/*.md`.
2. Metrics collection: visible via Sources/Overview cards and script references (`tools/runtime_snapshot.sh`, `tools/runtime_watch.sh`).
3. Trade information: covered by Desktop Execution orders table + mobile latest trade event block.

## 5) RPC Load Safety Check

UI model documented in screens as:

1. Read-only monitoring.
2. Data source = existing service metrics + local DB + snapshot artifacts.
3. Direct chain RPC polling from UI = disabled.

Operational safety conclusion:

1. The designed UI does not require additional direct Solana RPC requests.
2. It should not increase chain RPC load if implementation follows this contract.

## 6) Remaining Implementation Notes (Non-blocking)

1. Confirmation dialogs, reason capture, and action audit trail are represented in copy, but backend wiring is implementation-stage work.
2. For production frontend, keep strict polling budgets from brief section 12 and anomaly-first collapse rules from section 11.

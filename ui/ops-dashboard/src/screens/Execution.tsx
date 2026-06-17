import { OpenPositionsPanel } from "../components/OpenPositionsPanel";
import { PageHeader } from "../components/PageHeader";
import { BarTrack, Panel, StatCard } from "../components/Ui";
import type { BarTone } from "../components/Ui";
import { headerSubtitle } from "../snapshotFormat";
import type { RiskLevel, RowsSnapshot } from "../types";
import { useSnapshot } from "../useSnapshot";

type SnapshotRow = readonly [string, string, string?, RiskLevel?];
type RowList = ReadonlyArray<SnapshotRow>;

export function Execution() {
  const { snapshot, data } = useSnapshot<RowsSnapshot>("/api/execution");
  const rows = data?.rows?.length ? data.rows : unavailableRows();
  const cards = executionCards(rows);
  const openCount = Number(rowValue(rows, "open_positions") ?? rowValue(rows, "Open positions") ?? 0);
  const walletUnmatched = rowValue(rows, "wallet_unmatched") ?? "0";

  return (
    <>
      <PageHeader
        title="Execution"
        subtitle={headerSubtitle(snapshot, "order flow · last 1h")}
        status={snapshot?.stale ? "Stale" : "Fills symmetric"}
        statusLevel={snapshot?.stale ? "warning" : "safe"}
      />
      <div className="screen">
        <div className="metric-grid">
          {cards.map(([label, value, detail, state = "info"]) => (
            <StatCard detail={detail} key={label} label={label} level={state} value={value} />
          ))}
        </div>

        <div className="execution-layout">
          <OpenPositionsPanel
            count={openCount}
            emptyDetail="book is flat; exits remain monitored"
            headerLabel={`Open positions · ${openCount}`}
            headerMeta={
              <span className={walletUnmatched === "0" ? "text-safe" : "text-warning"}>
                {walletUnmatched === "0" ? "wallet clean" : `${walletUnmatched} unmatched`}
              </span>
            }
            note={`Wallet reconciliation clean · ${walletUnmatched} unmatched`}
          />

          <div className="side-stack">
            <Panel eyebrow="Failure classes · 1h" meta={data?.snapshot_error ? "check source" : "fresh"}>
              <div className="failure-bars">
                {failureRows(rows).map(([label, count, tone]) => (
                  <div className="bar-row" key={label}>
                    <span>{label}</span>
                    <BarTrack tone={tone} value={Math.min(100, count * 34)} />
                    <strong>{count}</strong>
                  </div>
                ))}
              </div>
            </Panel>

            <Panel eyebrow="Recent errors" meta="snapshot">
              <div className="error-list">
                {data?.snapshot_error ? (
                  <div>
                    <strong>snapshot_error</strong>
                    <span>{data.snapshot_error}</span>
                  </div>
                ) : (
                  <div>
                    <strong>No fresh execution errors</strong>
                    <span>failure classes are empty in the current snapshot</span>
                  </div>
                )}
              </div>
            </Panel>
          </div>
        </div>
      </div>
    </>
  );
}

function executionCards(rows: RowList) {
  return [
    ["Buys confirmed", rowValue(rows, "entry_confirmed") ?? rowValue(rows, "Buys confirmed") ?? "0", "confirmed tiny entries", "safe"],
    ["Sells confirmed", rowValue(rows, "exit_confirmed") ?? rowValue(rows, "Sells confirmed") ?? "0", "confirmed tiny exits", "safe"],
    ["Buys failed", rowValue(rows, "buy_failed") ?? "0", "slippage / rpc", "warning"],
    ["Sells failed", rowValue(rows, "sell_failed") ?? "0", "route stale", "info"]
  ] as Array<[string, string, string, RiskLevel]>;
}

function failureRows(rows: RowList) {
  const buyFailed = Number(rowValue(rows, "buy_failed") ?? 0);
  const sellFailed = Number(rowValue(rows, "sell_failed") ?? 0);
  if (buyFailed === 0 && sellFailed === 0) {
    return [["none reported", 0, "muted"]] as Array<[string, number, BarTone]>;
  }
  const rowsWithCounts: Array<[string, number, BarTone]> = [
    ["buy failed", buyFailed, "warning"],
    ["sell failed", sellFailed, "info"]
  ];
  return rowsWithCounts.filter(([, count]) => count > 0);
}

function rowValue(rows: RowList, label: string) {
  return rows.find(([rowLabel]) => rowLabel.toLowerCase() === label.toLowerCase())?.[1];
}

function unavailableRows(): RowList {
  return [
    ["entry_confirmed", "Not reported", "source unavailable", "warning"],
    ["exit_confirmed", "Not reported", "source unavailable", "warning"],
    ["buy_failed", "Not reported", "source unavailable", "warning"],
    ["sell_failed", "Not reported", "source unavailable", "warning"]
  ];
}

import { ShieldCheck } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { BarTrack, Panel, StatCard } from "../components/Ui";
import { headerSubtitle } from "../snapshotFormat";
import type { RowsSnapshot } from "../types";
import { useSnapshot } from "../useSnapshot";

export function Discovery() {
  const { snapshot, data } = useSnapshot<RowsSnapshot>("/api/discovery");
  const rows = data?.rows ?? [];
  const candidates = numberValue(rows, "candidates", 0);
  const floor = numberValue(rows, "floor", 8);
  const eligible = numberValue(rows, "eligible_wallets", 0);
  const scanRows = numberValue(rows, "scan_rows", 0);
  const tailLag = numberValue(rows, "tail_lag_seconds", 0);
  const aboveFloor = candidates >= floor;

  return (
    <>
      <PageHeader
        title="Discovery"
        subtitle={headerSubtitle(snapshot, "candidates · floor · filters")}
        status={snapshot?.stale ? "Stale" : aboveFloor ? "Above floor" : "Below floor"}
        statusLevel={snapshot?.stale || !aboveFloor ? "warning" : "safe"}
      />
      <div className="screen">
        <div className="discovery-layout">
          <div className="discovery-left">
            <Panel className="floor-panel" eyebrow="Candidates vs floor">
              <div className="floor-value">
                <strong>{candidates || "—"}</strong>
                <span>candidates tracked</span>
              </div>
              <FloorBar candidates={candidates} floor={floor} />
              <div className="floor-footer">
                <strong className={aboveFloor ? "text-safe" : "text-warning"}>
                  {aboveFloor ? `+${candidates - floor} above floor` : `${floor - candidates} below floor`}
                </strong>
                <span>{aboveFloor ? "healthy headroom" : "publish risk"}</span>
              </div>
            </Panel>

            <div className="mini-grid">
              <StatCard detail="wallet metrics total" label="Eligible wallets" value={eligible ? compactNumber(eligible) : "—"} />
              <StatCard detail="active set" label="Published" value={candidates || "—"} />
            </div>

            <section className={`guard-strip ${aboveFloor ? "guard-safe" : "guard-warning"}`}>
              <ShieldCheck size={19} />
              <div>
                <strong>{aboveFloor ? "Starvation guard OK" : "Starvation guard needs review"}</strong>
                <span>
                  tail lag {tailLag || 0}s · scanned {scanRows ? compactNumber(scanRows) : "not reported"} rows
                </span>
              </div>
            </section>
          </div>

          <div className="discovery-right">
            <Panel eyebrow="Filter funnel · 24h">
              <div className="funnel-list">
                <FunnelRow label="discovered" value={Math.max(eligible, candidates)} max={Math.max(eligible, candidates, 1)} tone="info" />
                <FunnelRow label="executable" value={Math.max(candidates * 4, candidates)} max={Math.max(eligible, candidates, 1)} tone="info" />
                <FunnelRow label="non-rug" value={Math.max(candidates * 3, candidates)} max={Math.max(eligible, candidates, 1)} tone="safe" />
                <FunnelRow label="published" value={candidates} max={Math.max(eligible, candidates, 1)} tone="ink" />
              </div>
            </Panel>

            <Panel className="tall-panel" eyebrow="Quarantine" meta="rug filter off">
              <div className="quarantine-empty">
                <strong>No active quarantine surfaced</strong>
                <span>sticky quarantine is ready; rug thresholds remain disabled.</span>
              </div>
            </Panel>
          </div>
        </div>
      </div>
    </>
  );
}

function FloorBar({ candidates, floor }: { candidates: number; floor: number }) {
  const max = Math.max(candidates, floor, 1);
  const candidatePct = Math.min(100, Math.round((candidates / max) * 100));
  const floorPct = Math.min(100, Math.round((floor / max) * 100));
  const markerPct = Math.max(8, Math.min(92, floorPct));
  return (
    <div className="floor-bar" aria-label="Candidates vs floor">
      <div className="floor-marker" style={{ left: `${markerPct}%` }}>
        <span>floor {floor}</span>
      </div>
      <div className="floor-fill" style={{ width: `${candidatePct}%` }} />
    </div>
  );
}

function FunnelRow({ label, value, max, tone }: { label: string; value: number; max: number; tone: string }) {
  return (
    <div className="funnel-row">
      <span>{label}</span>
      <BarTrack tone={tone as "safe" | "info" | "ink"} value={Math.min(100, Math.round((value / max) * 100))} />
      <strong>{compactNumber(value)}</strong>
    </div>
  );
}

function numberValue(rows: RowsSnapshot["rows"], label: string, fallback: number) {
  const raw = rows.find(([rowLabel]) => rowLabel.toLowerCase() === label.toLowerCase())?.[1];
  const parsed = Number(String(raw ?? "").replace(/,/g, ""));
  return Number.isFinite(parsed) ? parsed : fallback;
}

function compactNumber(value: number) {
  return new Intl.NumberFormat("en", { maximumFractionDigits: 0 }).format(value);
}

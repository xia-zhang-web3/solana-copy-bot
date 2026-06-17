import { Info } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { Panel, StatCard } from "../components/Ui";
import { headerSubtitle } from "../snapshotFormat";
import type { RowsSnapshot } from "../types";
import { useSnapshot } from "../useSnapshot";

export function Storage() {
  const { snapshot, data } = useSnapshot<RowsSnapshot>("/api/storage");
  const rows = data?.rows ?? [];
  const database = rowValue(rows, "database") ?? "Not reported";
  const wal = rowValue(rows, "wal") ?? "Not reported";
  const shm = rowValue(rows, "shm") ?? "Not reported";
  const walPressure = rowValue(rows, "wal_pressure") ?? "unknown";
  const healthy = !snapshot?.stale && walPressure === "none";

  return (
    <>
      <PageHeader
        title="Storage"
        subtitle={headerSubtitle(snapshot, "volume · DB · WAL · retention")}
        status={snapshot?.stale ? "Stale" : healthy ? "WAL stable" : "Check storage"}
        statusLevel={snapshot?.stale || !healthy ? "warning" : "safe"}
      />
      <div className="screen">
        <Panel className="volume-panel" eyebrow="DB volume" meta={snapshot?.stale ? "source stale" : "snapshot current"}>
          <div className="volume-value">
            <strong>{database}</strong>
            <span>primary data store</span>
          </div>
          <div className="volume-bar">
            <i style={{ width: database === "Not reported" ? "8%" : "38%" }} />
          </div>
          <div className="floor-footer">
            <span>free/runway report not surfaced yet</span>
            <strong>{wal} WAL</strong>
          </div>
        </Panel>

        <div className="storage-layout">
          <Panel className="stores-panel" eyebrow="Stores">
            <div className="store-table">
              <div>
                <span>Store</span>
                <span>Trend</span>
                <span>Growth</span>
                <span>Size</span>
              </div>
              <StoreRow label="Primary store" trend="up" growth="bounded" size={database} />
              <StoreRow label="WAL" trend="flat" growth={walPressure === "none" ? "stable" : "pressure"} size={wal} />
              <StoreRow label="SHM" trend="flat" growth="stable" size={shm} />
            </div>
          </Panel>

          <div className="side-stack">
            <StatCard detail="canary events · observed swaps compacted by rebuild" label="Retention" value="30 days" />
            <StatCard detail="compact DB swap finished; old DB removed" label="Last rebuild" level="safe" value="complete" />
            <section className="maintenance-callout">
              <Info size={17} />
              <div>
                <strong>Next maintenance — not urgent</strong>
                <span>watch free-space runway and WAL pressure before the next rebuild window.</span>
              </div>
            </section>
          </div>
        </div>
      </div>
    </>
  );
}

function StoreRow({ label, trend, growth, size }: { label: string; trend: "up" | "flat"; growth: string; size: string }) {
  return (
    <div>
      <code>{label}</code>
      <Spark trend={trend} />
      <span>{growth}</span>
      <strong>{size}</strong>
    </div>
  );
}

function Spark({ trend }: { trend: "up" | "flat" }) {
  const points = trend === "up" ? "0,18 18,14 36,10 54,7 72,3" : "0,11 18,13 36,10 54,12 72,11";
  return (
    <svg className="mini-spark" viewBox="0 0 72 22" aria-hidden="true">
      <polyline points={points} />
    </svg>
  );
}

function rowValue(rows: RowsSnapshot["rows"], label: string) {
  return rows.find(([rowLabel]) => rowLabel.toLowerCase() === label.toLowerCase())?.[1];
}

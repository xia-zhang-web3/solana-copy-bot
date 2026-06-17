import { ShieldCheck } from "lucide-react";
import { Checklist } from "../components/Checklist";
import { MetricCard } from "../components/MetricCard";
import { OpenPositionsPanel } from "../components/OpenPositionsPanel";
import { PageHeader } from "../components/PageHeader";
import { headerSubtitle } from "../snapshotFormat";
import type { ChecklistItem, Metric, OverviewSnapshot, RowsSnapshot } from "../types";
import { useSnapshot } from "../useSnapshot";

export function Overview() {
  const { snapshot, data } = useSnapshot<OverviewSnapshot>("/api/overview");
  const { data: storageData } = useSnapshot<RowsSnapshot>("/api/storage");
  const metrics = overviewFromSnapshot(data, storageData, snapshot?.source ?? "snapshot");
  const checklist = checklistFromSnapshot(data, snapshot?.stale ?? true);
  const overviewSafe = isOverviewSafe(data, snapshot?.stale ?? true);
  const heroTone = overviewSafe ? "hero-safe" : "hero-warning";
  const headline = snapshot?.stale
    ? "Check sources before stepping away"
    : overviewSafe
      ? "All clear — safe to step away"
      : "Check first — source coverage incomplete";

  return (
    <>
      <PageHeader
        title="Overview"
        subtitle={headerSubtitle(snapshot, "production · waiting for snapshot")}
        status={snapshot?.stale ? "Stale" : overviewSafe ? "All clear" : "Check first"}
        statusLevel={snapshot?.stale || !overviewSafe ? "warning" : "safe"}
      />
      <div className="screen">
        <section className={`hero hero-overview ${heroTone}`}>
          <div className="hero-lead">
            <div className="hero-icon">
              <ShieldCheck size={29} />
            </div>
            <div>
              <span className="eyebrow">System status</span>
              <h2>{headline}</h2>
            </div>
          </div>
          <p>
            Strict AND of every check passes only when service health, risk state, data freshness, data flow,
            storage runway and reconciliation are all inside limits.
          </p>
        </section>

        <div className="metric-grid">
          {metrics.map((metric) => (
            <MetricCard key={metric.label} metric={metric} />
          ))}
        </div>

        <div className="overview-layout">
          <Checklist items={checklist} />
          <OpenPositionsPanel
            count={Number(data?.open_positions ?? 0)}
            emptyDetail="entries paused · sells remain available"
            note="Wallet reconciliation clean when unmatched stays at zero."
          />
        </div>
      </div>
    </>
  );
}

function overviewFromSnapshot(
  data: OverviewSnapshot | null,
  storageData: RowsSnapshot | null,
  source: string
): Metric[] {
  if (!data) {
    return unavailableOverviewMetrics(source);
  }
  const runway = formatRunway(data.disk_runway_days);
  const candidates = numberOrNull(data.candidates);
  const floor = numberOrNull(data.candidate_floor);
  const wal = rowValue(storageData, "wal") ?? rowValue(storageData, "WAL") ?? "Not reported";
  return [
    {
      label: "Open positions",
      value: String(data.open_positions),
      detail: `entries ${formatState(data.entries)} · sells ${formatState(data.sells)}`,
      source
    },
    {
      label: "Disk runway",
      value: runway.value,
      detail: "from storage snapshot",
      source,
      level: runway.level
    },
    {
      label: "Candidates",
      value: candidates === null || floor === null ? "Not reported" : `${candidates} / floor ${floor}`,
      detail: "discovery publication",
      source,
      level: candidates === null || floor === null || candidates < floor ? "warning" : "safe"
    },
    {
      label: "WAL",
      value: wal,
      detail: "storage pressure",
      source: "storage",
      level: wal === "Not reported" ? "warning" : "safe"
    }
  ];
}

function checklistFromSnapshot(data: OverviewSnapshot | null, stale: boolean): ChecklistItem[] {
  if (!data) {
    return unavailableChecklist();
  }
  const ok = data.status === "green" && !stale;
  const candidates = numberOrNull(data.candidates);
  const floor = numberOrNull(data.candidate_floor);
  const runway = formatRunway(data.disk_runway_days);
  return [
    { label: "Overview source fresh", detail: stale ? "stale or missing" : "fresh", state: stale ? "fail" : "pass" },
    { label: "No hard stop / risk freeze", detail: formatBlocker(data.latest_blocker), state: ok ? "pass" : "warn" },
    {
      label: "Candidates above floor",
      detail: candidates === null || floor === null ? "not reported" : `${candidates} >= ${floor}`,
      state: candidates !== null && floor !== null && candidates >= floor ? "pass" : "warn"
    },
    { label: "Entries state explicit", detail: formatState(data.entries), state: data.entries === "unknown" ? "warn" : "pass" },
    { label: "Sells state explicit", detail: formatState(data.sells), state: data.sells === "unknown" ? "warn" : "pass" },
    { label: "Disk runway OK", detail: runway.detail, state: runway.level === "safe" ? "pass" : "warn" }
  ];
}

function unavailableOverviewMetrics(source: string): Metric[] {
  return [
    {
      label: "Open positions",
      value: "Not reported",
      detail: "source unavailable",
      source,
      level: "warning"
    },
    {
      label: "Disk runway",
      value: "Not reported",
      detail: "storage snapshot missing",
      source,
      level: "warning"
    },
    {
      label: "Candidates",
      value: "Not reported",
      detail: "publication snapshot missing",
      source,
      level: "warning"
    },
    {
      label: "WAL",
      value: "Not reported",
      detail: "storage pressure unknown",
      source,
      level: "warning"
    }
  ];
}

function unavailableChecklist(): ChecklistItem[] {
  return [
    { label: "Overview data fresh", detail: "stale or missing", state: "fail" },
    { label: "No hard stop / risk freeze", detail: "not reported", state: "warn" },
    { label: "Candidates above floor", detail: "not reported", state: "warn" },
    { label: "Entries state explicit", detail: "not reported", state: "warn" },
    { label: "Sells state explicit", detail: "not reported", state: "warn" },
    { label: "Disk runway OK", detail: "not reported", state: "warn" }
  ];
}

function isOverviewSafe(data: OverviewSnapshot | null, stale: boolean) {
  if (!data || stale || data.status !== "green") {
    return false;
  }
  const candidates = numberOrNull(data.candidates);
  const floor = numberOrNull(data.candidate_floor);
  return (
    data.latest_blocker === "none_active" &&
    data.entries !== "unknown" &&
    data.sells !== "unknown" &&
    formatRunway(data.disk_runway_days).level === "safe" &&
    candidates !== null &&
    floor !== null &&
    candidates >= floor
  );
}

function formatRunway(value: string | number) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return {
      value: `${value} days`,
      detail: `${value}d reported`,
      level: "safe" as const
    };
  }
  return {
    value: "Not reported",
    detail: "storage snapshot missing",
    level: "warning" as const
  };
}

function formatState(value: string) {
  if (value === "unknown") {
    return "unknown";
  }
  return value.replace(/_/g, " ");
}

function formatBlocker(value: string) {
  return value === "none_active" ? "clear" : value.replace(/_/g, " ");
}

function numberOrNull(value: string | number) {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function rowValue(snapshot: RowsSnapshot | null, label: string) {
  return snapshot?.rows.find(([rowLabel]) => rowLabel.toLowerCase() === label.toLowerCase())?.[1];
}

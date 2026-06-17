import type { ApiEnvelope, Metric, RiskLevel, RowsSnapshot } from "./types";

export function headerSubtitle<T>(snapshot: ApiEnvelope<T> | null, _fallback: string) {
  if (!snapshot) {
    return "waiting for data";
  }
  return `updated ${formatAge(snapshot.freshness_age_ms)} ago`;
}

export function headerStatus<T>(snapshot: ApiEnvelope<T> | null, status?: string) {
  if (!snapshot) {
    return "loading";
  }
  if (snapshot.stale) {
    return "stale";
  }
  return status ?? "fresh";
}

export function rowsToMetrics(data: RowsSnapshot | null, source: string, fallback: Metric[]) {
  if (!data?.rows?.length) {
    return fallback;
  }
  return data.rows.map(([label, value, detail, level]) => ({
    label: titleize(label),
    value,
    detail: detail ?? data.snapshot_error ?? "snapshot",
    source,
    level: level as RiskLevel | undefined
  }));
}

export function formatAge(ms: number) {
  const seconds = Math.max(0, Math.round(ms / 1000));
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }
  return `${Math.round(minutes / 60)}h`;
}

function titleize(value: string) {
  return value.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

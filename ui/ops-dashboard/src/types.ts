export type NavKey =
  | "overview"
  | "execution"
  | "strategy"
  | "discovery"
  | "storage"
  | "alerts";

export type RiskLevel = "safe" | "info" | "warning" | "dangerous" | "blocked";

export interface Metric {
  label: string;
  value: string;
  detail: string;
  source: string;
  level?: RiskLevel;
  trend?: number[];
}

export interface ChecklistItem {
  label: string;
  detail: string;
  state: "pass" | "warn" | "fail";
}

export interface TimelineEvent {
  title: string;
  detail: string;
  state: "active" | "resolved" | "planned";
  level: RiskLevel;
}

export interface ApiEnvelope<T> {
  source: string;
  generated_at: string;
  freshness_age_ms: number;
  stale: boolean;
  data: T;
}

export interface RowsSnapshot {
  status: string;
  rows: Array<[string, string, string?, RiskLevel?]>;
  snapshot_error?: string;
}

export interface OverviewSnapshot {
  status: string;
  entries: string;
  sells: string;
  open_positions: string | number;
  disk_runway_days: string | number;
  candidates: string | number;
  candidate_floor: string | number;
  latest_blocker: string;
  snapshot_error?: string;
}

export interface AlertsSnapshot {
  status: string;
  events: TimelineEvent[];
  snapshot_error?: string;
}

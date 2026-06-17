import type { RiskLevel } from "../types";

interface StatusPillProps {
  label: string;
  level?: RiskLevel;
}

export function StatusPill({ label, level }: StatusPillProps) {
  const inferredLevel = level ?? inferLevel(label);
  return (
    <span className={`status-pill status-${inferredLevel}`}>
      <span className="status-dot" />
      {label}
    </span>
  );
}

function inferLevel(label: string): RiskLevel {
  const normalized = label.toLowerCase();
  if (normalized.includes("stale") || normalized.includes("not ready") || normalized.includes("check")) {
    return "warning";
  }
  if (normalized.includes("critical") || normalized.includes("blocked")) {
    return "dangerous";
  }
  return "safe";
}

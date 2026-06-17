import type { ReactNode } from "react";
import type { RiskLevel } from "../types";

interface PanelProps {
  children: ReactNode;
  className?: string;
  eyebrow?: string;
  meta?: ReactNode;
}

interface StatCardProps {
  children?: ReactNode;
  className?: string;
  detail?: ReactNode;
  label: string;
  level?: RiskLevel;
  value: ReactNode;
}

export type BarTone = "safe" | "info" | "warning" | "dangerous" | "muted" | "ink";

interface BarTrackProps {
  tone?: BarTone;
  value: number;
}

export function Panel({ children, className = "", eyebrow, meta }: PanelProps) {
  return (
    <section className={["panel", className].filter(Boolean).join(" ")}>
      {eyebrow || meta ? (
        <div className="panel-header">
          {eyebrow ? <span className="eyebrow">{eyebrow}</span> : <span />}
          {meta ? <span>{meta}</span> : null}
        </div>
      ) : null}
      {children}
    </section>
  );
}

export function StatCard({ children, className = "", detail, label, level, value }: StatCardProps) {
  return (
    <section className={["metric-card", className].filter(Boolean).join(" ")}>
      <div className="eyebrow">{label}</div>
      <div className={["metric-value", level ? `text-${level}` : ""].filter(Boolean).join(" ")}>{value}</div>
      {children}
      {detail ? <div className="metric-detail">{detail}</div> : null}
    </section>
  );
}

export function BarTrack({ tone = "safe", value }: BarTrackProps) {
  return (
    <div className="bar-track">
      <i className={`bar-fill bar-${tone}`} style={{ width: `${Math.max(0, Math.min(100, value))}%` }} />
    </div>
  );
}

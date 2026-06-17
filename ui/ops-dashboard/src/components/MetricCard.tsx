import type { Metric } from "../types";
import { Sparkline } from "./Sparkline";
import { StatCard } from "./Ui";

interface MetricCardProps {
  metric: Metric;
}

export function MetricCard({ metric }: MetricCardProps) {
  const tone = metric.level === "warning" ? "warning" : metric.level === "dangerous" ? "danger" : "safe";

  return (
    <StatCard detail={metric.detail} label={metric.label} level={metric.level} value={metric.value}>
      {metric.trend ? <Sparkline values={metric.trend} tone={tone} /> : null}
    </StatCard>
  );
}

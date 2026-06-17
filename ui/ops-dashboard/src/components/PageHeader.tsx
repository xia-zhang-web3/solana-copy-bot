import { RefreshCw } from "lucide-react";
import type { RiskLevel } from "../types";
import { StatusPill } from "./StatusPill";

interface PageHeaderProps {
  title: string;
  subtitle: string;
  status?: string;
  statusLevel?: RiskLevel;
}

export function PageHeader({ title, subtitle, status = "live · 8s", statusLevel }: PageHeaderProps) {
  return (
    <header className="page-header">
      <div className="page-header-inner">
        <div>
          <h1>{title}</h1>
          <span>{subtitle}</span>
        </div>
        <div className="page-header-actions">
          <StatusPill label={status} level={statusLevel} />
          <RefreshCw size={17} />
        </div>
      </div>
    </header>
  );
}

import { CheckCircle2, TriangleAlert, XCircle } from "lucide-react";
import type { ChecklistItem } from "../types";

const iconByState = {
  pass: CheckCircle2,
  warn: TriangleAlert,
  fail: XCircle
};

interface ChecklistProps {
  items: ChecklistItem[];
}

export function Checklist({ items }: ChecklistProps) {
  const passed = items.filter((item) => item.state === "pass").length;

  return (
    <section className="panel checklist">
      <header className="panel-header">
        <span className="eyebrow">Step-away checklist</span>
        <span className={passed === items.length ? "text-safe" : "text-warning"}>
          {passed} / {items.length} pass
        </span>
      </header>
      <div className="checklist-grid">
        {items.map((item) => {
          const Icon = iconByState[item.state];
          return (
            <div className={`check-row check-${item.state}`} key={item.label}>
              <Icon size={17} strokeWidth={2.2} />
              <span>{item.label}</span>
              <em>{item.detail}</em>
            </div>
          );
        })}
      </div>
    </section>
  );
}

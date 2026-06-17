import { CheckCircle2 } from "lucide-react";
import { Panel } from "./Ui";

interface OpenPositionsPanelProps {
  count: number;
  emptyDetail: string;
  headerLabel?: string;
  headerMeta?: React.ReactNode;
  note: string;
}

export function OpenPositionsPanel({
  count,
  emptyDetail,
  headerLabel = "Open positions",
  headerMeta,
  note
}: OpenPositionsPanelProps) {
  const hasPositions = count > 0;
  const meta = headerMeta ?? (hasPositions ? `${count} tracked` : "none");

  return (
    <Panel className="positions-panel" eyebrow={headerLabel} meta={meta}>
      {hasPositions ? (
        <div className="data-table">
          <div className="table-head">
            <span>Token</span>
            <span>Size</span>
            <span>Held</span>
            <span>PnL</span>
          </div>
          {positionRows(count).map(([token, size, held, pnl]) => (
            <div className="table-row" key={token}>
              <code data-label="Token">{token}</code>
              <span data-label="Size">{size}</span>
              <span data-label="Held">{held}</span>
              <strong className={pnl.startsWith("+") ? "text-safe" : "text-warning"} data-label="PnL">
                {pnl}
              </strong>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty-state">
          <CheckCircle2 size={18} />
          <strong>No open tiny positions</strong>
          <span>{emptyDetail}</span>
        </div>
      )}
      <div className="inline-note">
        <CheckCircle2 size={15} />
        {note}
      </div>
    </Panel>
  );
}

function positionRows(count: number) {
  return Array.from({ length: Math.max(0, count) }, (_, index) => [
    `Position ${index + 1}`,
    "Not reported",
    "Not reported",
    "Not reported"
  ]);
}

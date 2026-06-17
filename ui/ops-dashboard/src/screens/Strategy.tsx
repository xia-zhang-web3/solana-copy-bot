import { AlertTriangle } from "lucide-react";
import { PageHeader } from "../components/PageHeader";
import { BarTrack, Panel, StatCard } from "../components/Ui";
import { headerSubtitle } from "../snapshotFormat";
import type { RowsSnapshot } from "../types";
import { useSnapshot } from "../useSnapshot";

export function Strategy() {
  const { snapshot, data } = useSnapshot<RowsSnapshot>("/api/strategy");
  const rows = data?.rows?.length ? data.rows : [];
  const shadow = rowValue(rows, "shadow_pnl") ?? rowValue(rows, "Shadow PnL") ?? "Not reported";
  const quote = rowValue(rows, "quote_after_priority") ?? rowValue(rows, "Quote after priority") ?? "Not reported";
  const delta = rowValue(rows, "quote_vs_shadow_delta") ?? rowValue(rows, "Limit-matched delta") ?? "Not reported";
  const green = rowValue(rows, "green_criterion") ?? "not_ready";
  const hasStrategyData = Boolean(data?.rows?.length && !snapshot?.stale);

  return (
    <>
      <PageHeader
        title="Strategy"
        subtitle={headerSubtitle(snapshot, "limit-matched executable economics")}
        status={snapshot?.stale ? "Stale" : "Not ready"}
        statusLevel="warning"
      />
      <div className="screen">
        <section className="hero hero-warning hero-overview">
          <div className="hero-lead">
            <div className="hero-icon">
              <AlertTriangle size={29} />
            </div>
            <div>
              <span className="eyebrow">Green criterion</span>
              <h2>Entries remain paused</h2>
            </div>
          </div>
          <p>One good window is not enough. Decisions use multi-window executable edge, not headline canary PnL.</p>
        </section>

        <div className="strategy-top-grid">
          <StatCard
            detail={hasStrategyData ? "limit-matched source set" : "source unavailable"}
            label="Shadow PnL"
            level={hasStrategyData ? "safe" : "warning"}
            value={shadow}
          />
          <StatCard
            detail={hasStrategyData ? "executable estimate" : "source unavailable"}
            label="Quote after priority"
            level={hasStrategyData ? "safe" : "warning"}
            value={quote}
          />
          <StatCard detail="gate closed manually" label="Tiny PnL" level="info" value="Paused" />
        </div>

        <div className="strategy-main-grid">
          <Panel
            className="delta-panel"
            eyebrow="Limit-matched executable delta"
            meta={<strong className={delta.startsWith("-") || !hasStrategyData ? "text-warning" : "text-safe"}>{delta}</strong>}
          >
            <p>
              Executable PnL vs shadow on the same limit-matched set. The dashboard treats tail drag as the decision
              signal, not a single headline canary number.
            </p>
            <div className="delta-bars">
              <div>
                <span>Shadow</span>
                <BarTrack value={100} />
                <strong>{shadow}</strong>
              </div>
              <div>
                <span>Executable</span>
                <BarTrack tone="info" value={52} />
                <strong>{quote}</strong>
              </div>
            </div>
          </Panel>

          <div className="side-stack">
            <StatCard detail="median is fine; losses concentrate in the tail" label="Follower gap" value="tail" />
            <StatCard
              detail="rate-led thresholds remain off pending validation"
              label="Rug / stale tail"
              level="warning"
              value="watch"
            />
          </div>
        </div>

        <Panel className="criterion-panel" eyebrow="Green criterion · multi-window" meta={formatGreen(green)}>
          <div className="criterion-table">
            <div className="criterion-head">
              <span>Check</span>
              <span>Value</span>
              <span>Samples</span>
              <span>Verdict</span>
            </div>
            <div>
              <strong data-label="Check">Source</strong>
              <span data-label="Value">{hasStrategyData ? "loaded" : "not reported"}</span>
              <span data-label="Samples">{hasStrategyData ? `n=${rows.length}` : "n/a"}</span>
              <em data-label="Verdict">{hasStrategyData ? "Review" : "Check"}</em>
            </div>
            <div>
              <strong data-label="Check">Multi-window</strong>
              <span data-label="Value">not reported</span>
              <span data-label="Samples">n/a</span>
              <em data-label="Verdict">Not ready</em>
            </div>
            <div>
              <strong data-label="Check">Thresholds</strong>
              <span data-label="Value">pending</span>
              <span data-label="Samples">n/a</span>
              <em data-label="Verdict">Not ready</em>
            </div>
            <div>
              <strong data-label="Check">Decision</strong>
              <span data-label="Value">paused</span>
              <span data-label="Samples">manual</span>
              <em data-label="Verdict">Hold</em>
            </div>
          </div>
          <p className="muted">
            Entries stay paused until multiple active windows agree and the executable edge clears the target-size
            break-even threshold.
          </p>
        </Panel>
      </div>
    </>
  );
}

function rowValue(rows: RowsSnapshot["rows"], label: string) {
  return rows.find(([rowLabel]) => rowLabel.toLowerCase() === label.toLowerCase())?.[1];
}

function formatGreen(value: string) {
  return value === "not_ready" ? "not ready" : value.replace(/_/g, " ");
}

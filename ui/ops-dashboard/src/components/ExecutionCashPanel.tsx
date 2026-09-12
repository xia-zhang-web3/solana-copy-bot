import { Panel } from "./Ui";

type Rows = ReadonlyArray<readonly [string, unknown, ...unknown[]]>;

// Money remains a decimal string from exporter through React. Missing values
// and numeric JSON payloads cannot silently become rounded amounts or zero.
function exact(rows: Rows, key: string, signed = true): string {
  const value = rows.find(([label]) => label === key)?.[1];
  return typeof value === "string" && (signed ? /^-?\d+$/ : /^\d+$/).test(value)
    ? value : "Unknown";
}

export function ExecutionCashPanel({ rows }: { rows: Rows }) {
  const settled = exact(rows, "cash_settled_orders", false);
  const unsettled = exact(rows, "cash_unsettled_orders", false);
  const cohortKnown = settled !== "Unknown" && !/^0+$/.test(settled) && /^0+$/.test(unsettled);
  const fields = [
    ["cash_settled_orders", "Settled SELL orders", settled, "receipt-native SELL, including owned-only"],
    ["cash_unsettled_orders", "Unsettled SELL orders", unsettled, "pending or unsupported accounting; legacy orders counted separately"],
    ["known_cash_result_lamports", "Known subset cash result · lamports",
      exact(rows, "known_cash_result_lamports"), "native cash minus allocated entry basis; settled subset only"],
    ["cohort_cash_result_lamports", "Cohort cash result · lamports",
      cohortKnown ? exact(rows, "cohort_cash_result_lamports") : "Unknown",
      "unknown for empty, legacy or unsettled cohorts"],
    ["economic_pnl", "Economic PnL", "Unknown", "fees/rent/WSOL decomposition unresolved"],
  ];
  return (
    <Panel eyebrow="SELL cash settlement" meta="receipt basis · snapshot window">
      <dl className="execution-cash-grid">
        {fields.map(([key, label, value, detail]) => (
          <div key={key}>
            <dt>{label}</dt>
            <dd data-cash-field={key}>{value}</dd>
            <p>{detail}</p>
          </div>
        ))}
      </dl>
    </Panel>
  );
}

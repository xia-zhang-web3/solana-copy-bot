import { Panel } from "./Ui";

type Rows = ReadonlyArray<readonly [string, unknown, ...unknown[]]>;
export function ExecutionFailedExpensePanel({ rows }: { rows: Rows }) {
  const read = (key: string) => rows.find(([label]) => label === key)?.[1];
  const exact = (key: string, signed = false) => {
    const value = read(key);
    return typeof value === "string" && (signed ? /^(0|-?[1-9]\d*)$/ : /^(0|[1-9]\d*)$/).test(value)
      ? value : "Unknown";
  };
  const label = (key: string) => typeof read(key) === "string" && read(key) !== "unknown" ? String(read(key)) : "Unknown";
  const complete = read("failed_expense_coverage") === "complete_selected_cohort"
    && /^[1-9]\d*$/.test(exact("failed_expense_orders"))
    && exact("failed_expense_unknown_orders") === "0"
    && exact("failed_expense_unresolved_orders") === "0";
  const fields = [
    ["failed_expense_known_lamports", "Known failed wallet fees · lamports", exact("failed_expense_known_lamports")],
    ["failed_expense_cohort_lamports", "Cohort failed wallet fees · lamports", complete ? exact("failed_expense_cohort_lamports") : "Unknown"],
    ["failed_expense_orders", "Submitted failed orders", exact("failed_expense_orders")],
    ["failed_expense_unknown_orders", "Unknown wallet fee", exact("failed_expense_unknown_orders")],
    ["failed_expense_unresolved_orders", "Unresolved orders", exact("failed_expense_unresolved_orders")],
    ["failed_expense_legacy_orders", "Uncovered historical orders", exact("failed_expense_legacy_orders")],
    ["failed_expense_native_delta_lamports", "Known native delta · lamports", exact("failed_expense_native_delta_lamports", true)],
    ["failed_expense_unexplained_lamports", "Unexplained native residual · lamports", exact("failed_expense_unexplained_lamports", true)],
    ["failed_expense_coverage", "Coverage", label("failed_expense_coverage")],
    ["failed_expense_basis", "Source", label("failed_expense_basis")],
  ];
  return <Panel eyebrow="Failed transaction expenses" meta="receipt fees · economic PnL unknown">
    <p>Window: {label("failed_expense_since")} ≤ original submit time &lt; {label("failed_expense_as_of")}. Late receipts keep the original window.</p>
    <dl className="execution-cash-grid">
      {fields.map(([key, title, value]) => <div key={key}>
        <dt>{title}</dt><dd data-expense-field={key}>{value}</dd>
      </div>)}
    </dl>
    <p>Native movement and fee are separate observations. Historical coverage: {label("failed_expense_history")}.</p>
    <p>Economic PnL remains Unknown. Fees are recorded once; no second deduction from positions. Loss caps and SOL reserve policy are unchanged.</p>
  </Panel>;
}

import { Panel } from "./Ui";

type ObjectValue = Record<string, unknown>;
const object = (value: unknown): ObjectValue => value !== null && typeof value === "object" && !Array.isArray(value) ? value as ObjectValue : {};
const list = (value: unknown, max: number): ObjectValue[] => Array.isArray(value) ? value.slice(0, max).map(object) : [];
const text = (value: unknown) => typeof value === "string" && value.length <= 256 ? value : "Unknown";
function exact(value: unknown, signed = false) {
  if (typeof value !== "string" || value.length > 40 || !/^(0|-?[1-9]\d*)$/.test(value)) return "Unknown";
  const n = BigInt(value);
  return signed ? (n >= -(1n << 127n) && n < (1n << 127n) ? value : "Unknown") : (n >= 0n && n <= 18446744073709551615n ? value : "Unknown");
}
function observation(value: unknown, numeric = false, signed = false) {
  const v = object(value);
  const known = v.coverage === "known" && ["rpc_account_key", "rpc_native_balance", "rpc_token_balance", "parsed_instruction", "proven_lifecycle"].includes(String(v.source));
  return `${known ? (numeric ? exact(v.value, signed) : text(v.value)) : "Unknown"} · ${text(v.coverage)} · ${text(v.source)}`;
}
function endpoint(value: unknown) {
  const e = object(value);
  return <dl className="execution-cash-grid">
    {[["mint", "Mint"], ["token_owner", "Token owner"], ["token_program", "Token program"], ["decimals", "Decimals"], ["raw", "Raw amount"]].map(([key, label]) =>
      <div key={key}><dt>{label}</dt><dd>{observation(e[key], key === "raw" || key === "decimals")}</dd></div>)}
  </dl>;
}
export function ExecutionNativeObservationsPanel({ report }: { report: unknown }) {
  const r = object(report);
  const operations = list(r.rows, 100);
  return <Panel eyebrow="Token accounts / WSOL observations" meta="exact receipt observations · decomposition unresolved">
    <p>Coverage: {text(r.coverage)}. Source: {text(r.source_basis)}.</p>
    <p>Window: {text(r.since)} ≤ original submit time &lt; {text(r.as_of)}.</p>
    <dl className="execution-cash-grid">
      {[["total_orders", "Operations"], ["covered_orders", "Covered observations"], ["partial_orders", "Partial"], ["uncovered_orders", "Uncovered history"], ["conflict_orders", "Conflicts"], ["account_rows", "Account observations"], ["instruction_rows", "Instruction observations"]].map(([key, label]) =>
        <div key={key}><dt>{label} · full window</dt><dd data-native-count={key}>{exact(r[key])}</dd></div>)}
    </dl>
    <p>Display limited: {r.rows_truncated === true ? "yes" : r.rows_truncated === false ? "no" : "Unknown"}. Counts retain the full operation window.</p>
    {operations.length === 0 && <p>No account observations displayed. Empty history does not establish zero costs.</p>}
    {operations.map((op, index) => {
      const bundle = object(op.observations);
      return <details key={index}>
        <summary>{text(op.side)} · {text(op.order_id)} · {text(op.coverage)}</summary>
        <p>Operation: {text(op.operation_at)}. Reason: {op.reason == null ? "none recorded" : text(op.reason)}.</p>
        <p>Signature: {text(bundle.tx_signature)} · slot {exact(bundle.slot)} · wallet {text(bundle.wallet_pubkey)}.</p>
        <p>Accounts: {text(bundle.accounts_coverage)}. Instructions: {text(bundle.instructions_coverage)}.</p>
        <p>{Array.isArray(bundle.reasons) ? bundle.reasons.slice(0, 16).map(text).join(" · ") : "Unknown"}</p>
        {list(bundle.accounts, 64).map((a, n) => <details key={n}>
          <summary>Account {String(a.account_index ?? "Unknown")} · {text(a.pubkey)}</summary>
          <p>Selection: {Array.isArray(a.relevance) ? a.relevance.map(text).join(" · ") : "Unknown"}. Selection does not prove ownership.</p>
          <dl className="execution-cash-grid">
            {[["native_pre", "Native before"], ["native_post", "Native after"], ["native_delta", "Native delta"]].map(([key, label]) => <div key={key}><dt>{label} · lamports</dt><dd data-native-value={key}>{observation(a[key], true, key === "native_delta")}</dd></div>)}
          </dl>
          <p>Token state before</p>{endpoint(a.pre_token)}<p>Token state after</p>{endpoint(a.post_token)}
        </details>)}
        {list(bundle.instructions, 64).map((i, n) => <details key={n}>
          <summary>Instruction {String(i.outer_index ?? "Unknown")} / {String(i.inner_index ?? "outer")} · {observation(i.instruction_type)}</summary>
          <p>Program: {observation(i.program_id)} · stack height: {observation(i.stack_height, true)}.</p>
          <p>Instruction roles do not establish token ownership or close-refund entitlement.</p>
          <dl className="execution-cash-grid">{Object.entries(object(i.fields)).slice(0, 16).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{observation(value, ["lamports", "amount", "decimals", "space"].includes(key))}</dd></div>)}</dl>
        </details>)}
      </details>;
    })}
    <p>RPC outer/inner positions do not prove total CPI ordering or intermediate balances. A lifecycle zero is an inference, distinct from an RPC row.</p>
    <p>Account deltas and instruction amounts are separate observations, never summed as wallet movement, rent, proceeds or profit. Economic PnL remains Unknown.</p>
  </Panel>;
}

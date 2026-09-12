# Standalone failed-attempt expenses in quote-only replay

Both `copybot_execution_canary_quote_pnl` and `copybot_execution_tiny_economics`
accept this explicit action in the existing version 1 `--portfolio-input` file:

```json
{
  "id": "scenario-failed-attempt-1",
  "position_id": "portfolio-fee",
  "sequence": "2",
  "unix_ms": "1780401600002",
  "identity_provenance": {"synthetic": "ordered scenario event"},
  "action": {
    "kind": "failed_attempt_expense",
    "amount": {
      "value": {"known": "5"},
      "provenance": {"assumed": "failed landing cost scenario"}
    }
  }
}
```

Amounts are exact unsigned lamport decimal strings. `position_id` remains a
required valid envelope identity but is only a correlation label for this action.
The event does not require or create a position. Known amounts, including zero,
debit portfolio cash and increase `flows.expenses` once. No raw, slots, principal,
entry allocations/remainders, marks, or rent balances change.

Use `{"unknown":"unmeasured"}` instead of `{"known":"5"}` when the amount is
unknown. An omitted/null amount is also Unknown, never Known(0). Missing/invalid
provenance, malformed amounts and unknown fields are not accepted. Adapter input
errors make the report unavailable; no partial replay is presented as complete.

An event refused because its charge is unknown, unreliable, out of order,
conflicting or unrepresentable leaves the committed money/inventory unchanged.
It adds incomplete coverage. Every later new BUY receives
`cash_availability_unknown` with the first affected `expense_event_id`, even if
the retained cash subtotal would otherwise suffice. This condition is sticky
until a fresh replay; no deposit, later known expense or successful SELL clears it.
Full net/equity remain Unknown. This rule is specific to the new action and does
not change legacy `unsupported_expense` behavior.

An identical event id and all identical operands return the original historical
outcome without a second debit. Changing amount, provenance, order, position label
or action for the same id conflicts. Caller-only metadata conflicts retain the
adapter's existing unavailable-report contract. Different ids are separate
scenario assertions, not proof of distinct real payments; callers must reconcile
actual payments separately and must not duplicate a trade's bundled costs here.
A pre-submit skip does not automatically generate an expense.

For initial cash100/cap2, BUY A60, fee5, BUY B35 succeeds with cash0; BUY B40 is
skipped with cash35. Fee0 permits either BUY. Each BUY and remaining position mark
requires its own exact-size quote. Without positions, fee5 yields cash95,
expenses5 and signed scenario net−5. With unmarked positions full valuation remains
Unknown.

Failed landing costs in quote-only experiments remain assumed/synthetic until
actual debit is independently proved. Caller `observed` is only an assertion.
Known0 retains its provenance, and assumed/synthetic operands retain that aggregate
basis. `production_green` stays false and dataset coverage/net stay Unknown even
when arithmetic for the supplied scenario is complete. No fee importer, receipt
verification, full history reconstruction, joint liquidation or runtime execution
is provided by this action.

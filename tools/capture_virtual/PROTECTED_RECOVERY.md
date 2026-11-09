# Protected complete-block delivery

This is an observation-only daemon path. It uses one sequentially owned complete
Yellowstone subscription and the existing confirmed HTTP recovery broker. It does
not authorize a provider window, deployment or execution.

## Explicit configuration

Use `yellowstone_delivery_mode="durable_association_v1"` with a separately created
`capture_scope_db`, explicit association limits and `yellowstone_http_recovery`.
The `capture_recovery` table requires every field below; none has an implicit
budget or default policy:

- `policy="protected_complete_recovery_v1"`.
- `provenance_policy="historical_missed_no_execution_v1"`.
- `checkpoint_directory`: canonical absolute path in owned writable task storage.
- `migrations_directory`: absolute path to the matching installed migrations.
- `max_scope_wallets`: explicit union bound, at most 128.
- `max_scope_revisions`: explicit persistent revision bound, at most 128.

Keep the fixed `yellowstone_replay_wallets` list empty. Financial activation,
native BUY, owner BUY/exit and technical-cohort authority are forbidden for this
combination. Legacy capture remains available with no `capture_recovery` table.

The controller creates a fresh capture database from both schema files. Opening
an old database never upgrades it. Each immutable scope revision has its own
inbox named by persistent capture generation and revision in the checkpoint
directory. The existing migration runner initializes only this owned inbox.
Include these inboxes, WAL files and raw evidence in the task's disk envelope;
the association inbox budget applies to each retained revision. Do not reuse a
consumed capture/virtual database or a previous task's checkpoint directory.

## Capture, forward and checkpoint

Scope preparation fences the old member ACK in the same transaction that reads
the requested members plus every unsettled pin. An already committed pin remains
protected after demotion. Pending members gain no admission from a prior scope's
cursor. A prior checkpoint may bootstrap chain verification; only a new scope's
own complete-block checkpoint can ACK that scope.

Transaction frames preserve their genuine created-at and existing live admission
rules. Complete blocks also enumerate protected observations, including frames
omitted without reconnect. HTTP replay uses the same count, order, parent,
overlap and anchor checks. Original live and RPC anchor representations, times
and checked selection relation are retained separately.

Exact raw evidence and identity/outbox commits precede the inbox handoff. The
inbox stores a generation/revision/hash/receipt reference rather than a second
copy of the raw block. A crash between the two commits replays the immutable
identity. Unbound live receipts are reconciled before intake so a previously
delivered stream event cannot become recovery-first.

The actual Python observer drains recovery outbox entries through
`CaptureVirtualAdapter.next_sequence()`. It records historical supported actions
as `MISSED_UNEXECUTED` and does not call the financial ledger. A separately checked
exact token leg may exist while the full swap's `exact_amounts` remains unknown.
No historical timestamp is promoted to fresh stream time.

Before checkpoint ACK the Rust writer independently verifies the committed
forward disposition receipt, identity and evidence. Pending, conflicting or
unknown outcomes retain the obligation. Verification progresses in bounded
batches; terminal state alone is insufficient. Transport/end/reset/rejection
fences the old ACK and requires a verified checkpoint and a new scope ACK.

## Evidence limits

Local saved transactions inside explicitly synthetic block wrappers prove the
code path and fault handling. They do not prove historical full blocks, provider
delivery completeness, prospective latency or trading profitability. Deployment
requires a separately accepted matching `copybot-app` CI artifact and stopped
package. Preserve the accepted inventory adapter pair and actual recursive
import/hash binding when preparing that package.

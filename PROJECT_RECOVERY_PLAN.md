# Project Recovery Plan

## Current decision: first owner BUY to SELL cycle confirmed

Decision: yes. The daemon sold the single confirmed owner BUY position without
a source SELL, resolved an unknown dispatch through its finalized receipt, and
closed and classified the trade without a second send.

The original run04 remains stopped and unchanged. Its confirmed BUY order is
`exec-canary:owner-buy:copybot-owner-buy-20260924-04-usdc-01`; its source
snapshot recorded an open position of 1,167,085 raw USDC (6 decimals) in wallet
`BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk`. The BUY receipt shows a
10,000,000 lamport swap input, 5,000 lamport transaction fee, 1,488,440
lamports retained as USDC ATA rent, and a total wallet debit of 11,493,440
lamports. The original receipt and fill remain historical facts.

The one-position owner-exit implementation uses the daemon's quote, build,
simulation, submit, receipt and accounting path. The isolated exit package
inherits the confirmed BUY database through a SQLite backup. It is prepared
with STOP and no active financial authority. An UNKNOWN dispatch is a
reconciliation obligation, not permission to resend. A confirmed partial fill
remains partial.

Independent money-path review found and resolved concrete position-binding,
route, WSOL, native-floor, pre-dispatch recovery and priority-fee proof issues.
The scoped synthetic daemon test passed for a full confirmed SELL and for
UNKNOWN reconciliation after restart without a second send. A separate test
proved metadata-only priority-fee tampering remains unclassified. On 2026-09-25,
independent review found one missing-price case: Jupiter may omit CU-price when
total priority fee is zero. The owner-exit SELL assembler now encodes explicit
price zero for that bound request. A causal test passed through the real SELL
assembler and local simulation without send; malformed, duplicate and nonzero
prices and missing CU-limit were refused. The existing restart and generic SELL
boundary tests passed. Independent review accepted this narrow repair.

The accepted repair is commit `2f3094775537256d1e4e913f57f56803f1a173ee`.
Its matching `copybot-app/release` artifact from GitHub Actions run
`36110384699` was installed. The user's first one-shot command for run05 exited
at config validation: durable association requires both execution flags false,
while owner exit needs tiny submit enabled. No SELL order, dispatch, receipt or
fill was created; its copied position remained open. The run05 ledger records six RPC
attempts, 60 CU and $0.0000315 at the provider model rate; stream use was zero.
Run05 and its control files remain stopped and consumed.

Run06 was a new stopped local package using the accepted binary and supported
legacy Yellowstone delivery mode. It carries run05's provider attempts into
its new ledger, so the $5 total model ceiling is not reset. The source run04
BUY facts remain unchanged; the copied database passed migrations 0086/0087
offline. The installed binary passed an offline startup check with tiny submit
enabled and no network. An independent review accepted the changed config,
budget carryover and one-position bounds; all five containers were created and
the final local preflight passed with STOP and no authority or clock.

The owner ran the one-shot run06 command. Its single SELL signature
`3kV7UXd46zqio44QHdynYUKdiejPr778WGiu7QQDZGMopd1sydzsDXSBoGqxSrrcLsRetEHASYg3ABv4bnbjPAPx`
was finalized at slot 450303335. The 1,167,085 raw USDC position is closed
with quantity zero. The broker ledger records one `sendTransaction`; no
unresolved dispatch or pending failed expense remains. BUY swap input was
10,000,000 lamports, SELL swap output 9,936,399, and each transaction fee was
5,000. Priority fees were zero. The economic cycle result is −73,601 lamports;
wallet cash changed by −1,562,041 lamports because 1,488,440 remains as rent
in the now-empty USDC ATA. The historical receipt decomposition flags remain
unresolved; the new separate cash-component rows classify both sides with zero
unclassified residual. Independent read-only postflight accepted this result.
STOP is present and all five run06 containers have exited.

One secondary final DB read in the launcher recorded `OutcomeReadError` without
its reason. The same isolated reader and direct DB inspection now return
`CLOSED_CONFIRMED`, and DB integrity checks pass. The next action is to record
this first measured cycle and assess actual provider billing and a broader
sample before any strategy profitability decision. No further trade is implied.

Limits: this single negative cycle proves neither future route availability nor
the profitability of a trading strategy. Provider budget accounting is a model;
the actual provider invoice is not established by this run.

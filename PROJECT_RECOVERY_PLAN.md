# Project Recovery Plan

## Current decision: one owner exit for the confirmed run04 position

Decision question: can the daemon sell the single confirmed owner BUY position
without a source SELL, then close and classify the trade from confirmed receipts
without resending an unknown transaction?

The original run04 remains stopped. Its confirmed BUY order is
`exec-canary:owner-buy:copybot-owner-buy-20260924-04-usdc-01`; its open position
holds 1,167,085 raw USDC (6 decimals) in wallet
`BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk`. The BUY receipt shows a
10,000,000 lamport swap input, 5,000 lamport transaction fee, 1,488,440
lamports retained as USDC ATA rent, and a total wallet debit of 11,493,440
lamports. The original receipt and fill remain historical facts.

The one-position owner-exit implementation uses the daemon's quote, build,
simulation, submit, receipt and accounting path. The isolated exit package
inherits the confirmed BUY database through a SQLite backup and has a fresh
fee/provider ledger and authority window. It is prepared with STOP and no
active financial authority. An UNKNOWN dispatch is a reconciliation obligation,
not permission to resend. A confirmed partial fill remains partial.

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

Next authorized action: publish the accepted repair, build the matching
`copybot-app` release artifact in GitHub Actions, verify and install it only in
the stopped exit package, then run local preflight. The user alone starts the
bounded live SELL.

Limits: a live V1 direct Raydium quote is still unknown until activation; an
unsupported route fails closed. Local tests and artifact checks do not prove a
live SELL, transaction receipt or profitability.

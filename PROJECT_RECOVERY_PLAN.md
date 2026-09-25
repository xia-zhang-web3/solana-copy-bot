# Project Recovery Plan

## Current decision: run 09 capacity repair accepted offline

Run 08 was used once and stopped after `BlockCapacity`; it made no order or
send. The saved main outcome remains `APP_EXITED`, with `NO_SIGNAL` as the
financial state. Run 07 and Run 08 evidence, ledgers, databases, clocks and
authorities remain untouched. The prepared technical cohort 09 package is
configured to carry all prior provider accounting and retain the authorized
$50 total provider model, four-hour window, one BUY at most 0.01 SOL and one
source SELL. No paid ingress, signature or live trade was used in preparation.

The offline capacity repair was independently accepted on 2026-09-25 for the
specified synthetic profile: 450 full blocks across 180 simulated seconds,
36,000 continuous parent blocks across 14,400 seconds, bot BUY anchor and a
source SELL through daemon receipt/accounting, restart without a second send,
and process RSS under the unchanged 2 GiB app limit. The old 32-block profile
reproduced `BlockCapacity`. See
[Run09 offline acceptance](audit/2026-09-25/technical-cohort-09-capacity/OFFLINE_ACCEPTANCE_RU.md)
for measured cache, durable meter, memory and their limits. Run 08 full block
payloads were unavailable; live maximum and strategy profitability remain
unproved.

The source decision authorizes a matching `copybot-app/release` artifact and
isolated Run09 installation. Current launch authority is determined by the
new package's STOP, matching artifact binding and final local preflight; the
owner command may be used only after that package is sealed. No new financial
activation is performed by this repair.

## Prior decision: run 08 four-hour upgrade accepted before publication

The run 07 stream association and SQLite reader repairs were installed in the
stopped, unused run 08 package with a matching one-hour artifact. The owner
authorized a provider model of at most $50 and an overall window of at most
four hours, retaining one BUY of at most 0.01 SOL and one source SELL. Run 07's
seven RPC attempts, 80 compute units, 42,000 nanoUSD, and 47,881,652 stream
bytes remain charged in the new package budget. The original run 07 UNKNOWN,
database, and ledger remain untouched.

The narrow upgrade extends the technical cohort validator and the protected
native tiny budget to the same immutable four-hour cohort deadline. Ordinary
tiny experiments remain limited to 3,600 seconds. The package derives its
active config deadline from the first outbound session clock, so reconnect or
restart cannot extend the authority. Trading caps, native floor, fee caps,
one-send rule, and source ownership remain in force. Independent review
accepted this code and package diff on 2026-09-25 after scoped config, storage,
daemon and helper checks. Its acceptance is local: publication of a matching
`copybot-app/release` artifact, installation in run 08, disposable offline
startup, package seal, and final activation preflight remain required. STOP is
set; no new provider use, signature, or transaction has occurred.

Next action: publish this accepted diff, install and verify the exact CI
artifact, then finish the stopped package and return one future owner command.

## Current decision: run 07 repair accepted offline; new stopped package pending

The scoped local admission filter and live/stopped WAL reader passed independent
review on 2026-09-25. Targeted tests include 742 irrelevant swaps, a causal
source BUY→bot BUY→source SELL through the daemon scheduler and accounting,
restart without a second send, missing-anchor and missing-parent refusals, and
live/stopped/crash WAL reads. A matching CI release and isolated stopped package
remain necessary before another owner command. The paid provider stream is still
broad; local filtering only limits retained association state.

The owner-authorized one-shot command ran once on 2026-09-25. The daemon exited
about 26 seconds after startup because the durable association inbox reached its
128 MiB logical byte cap. The source stream had admitted 742 swaps from 713
distinct wallets; none belonged to the three authorized cohort wallets. The
helper's live read-only SQLite reader also returned `SQLITE_CANTOPEN`, so its
preserved `LIVE_RESULT.json` says `UNKNOWN`. A stopped, direct read of the same
DB returned `NO_SIGNAL`: no cohort decision, order, unresolved dispatch,
receipt, fill or position. That read narrows the incident but does not rewrite
the original result or prove an on-chain balance. The broker ledger contains
seven RPC requests and a $0.000042 modeled charge. STOP and all five containers
are stopped. Run 07 must never be activated again.

The next technical blocker is the broad DEX subscription and persistence of
unrelated wallets; a second blocker is the live outcome reader's SQLite access.
A proposed leader-only subscription passed narrow local tests, but independent
review rejected it: it would omit the bot BUY receipt anchor and the continuous
parent-block chain required to authorize a later source SELL. That draft code
was removed. A bounded source design must retain both anchors and parent
continuity, then pass a causal multi-block BUY→bot BUY→source SELL test. An
offline Docker reproducer established that the read-only SQLite bind works
while a WAL writer is open and returns `SQLITE_CANTOPEN` after the last writer
closes; the reader must distinguish those states without ignoring live WAL.
Only after independent review, a matching new artifact and a fresh stopped
package may the owner consider one new activation. No live copy or strategy
profitability was established by run 07.

## Prior decision: stopped technical cohort package ready for one owner launch

The first owner BUY→SELL cycle below remains confirmed and stopped. The next
source-driven canary uses three preselected source wallets, at most one fresh
source BUY, one protected bot BUY, and one source SELL of receipt-owned quantity.
It does not publish Discovery GREEN. Its fixed 3,600-second authority, 120-second
source age, processed-slot epochs, BUY1/SELL1 caps and restart reconciliation
passed scoped checks. The first admitted source BUY consumes the only slot before
mint/quote/build; refusal can yield `NO_EXECUTED_BUY` without a second candidate.

Independent review found that the initial `e684ab88…` release suppressed the
strict source SELL scheduler in active cohort and that the one-shot helper stopped
on a normal pending BUY. The daemon condition and helper were repaired. A causal
offline test drives BUY receipt→source SELL→main runner tick→quote→build→simulation
→one send→receipt/accounting, then restart without another send. It fails with
the original scheduler condition and passes with the fix. Pending work and
unknown sends retain the original deadline; incomplete orders remain `UNKNOWN`
at STOP. Independent code/helper review accepted this scope.

The accepted repair is commit `7f644eec4a3ff5c1abb19d263b9d7a54c9fd6c28`.
Matching `copybot-app/release` GitHub Actions run `36124811451` passed and its
artifact is installed as current in isolated `technical-cohort-07`. The prior
`2f309477…` rollback and defective `e684ab88…` release remain stored. The new
binary passed exact active-config startup in a disposable DB with STOP,
`network none`, and no signer; orders, receipts, signatures and provider calls
were zero. A new 44-file seal and local activation preflight passed. All five
final containers remain created and never started; final state is empty, ledger
is zero, STOP remains, and no financial authority or clock was consumed.
Independent package review accepted the stopped package for one future owner
command. Live copying and strategy profitability are not yet proved.

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
`CLOSED_CONFIRMED`, and DB integrity checks pass. The new cohort helper preserves
the safe failure fact in `finally`. Its next user action, if authorized, is one
bounded launch command; an open position at deadline remains open until a
separate decision. No source event or profitable outcome is promised.

Limits: this single negative cycle proves neither future route availability nor
the profitability of a trading strategy. Provider budget accounting is a model;
the actual provider invoice is not established by this run.

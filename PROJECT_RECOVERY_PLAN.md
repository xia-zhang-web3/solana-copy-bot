# Project Recovery Plan

## Current decision: Run15 stopped; repair installed, read-only owner decision next
Run15 stopped after 23 reconnects: STOP, five containers exited, NO_SIGNAL;
orders/receipts/fills/positions/dispatch/decisions0. Supervisor normal_stop;
launcher LIVE_RESULT absent. Original DB/authority/clocks/ledger retained.
Cumulative HTTP996/19020CU/$0.0099855 model; stream83,722,698,801 bytes.
Historical live Internal cause UNKNOWN; no new financial run is authorized.
Bounded redacted diagnostics and relay EOF drain are independently accepted.
SQLite0089 scoped cursor/replay checks exact parent/hash/full Info before fresh
Admission, preserves first facts/UNKNOWN, and restores commit before ACK/restart.
New controls6/6, checkpoint4/4 and migration rollback2/2 independently PASS.
Genuine tonic→both relays→daemon model2.96s: BUY1/SELL1, partial remainder,
UNKNOWN/restart/receipt reconciliation, no second send. Live execution unproved.
CI followup fixed single-family scope regression: new scope3/affected daemon4 PASS.
App fd4 artifact installed; Storage b992 full gate752PASS/job7m4, architecturePASS.
Production deps unchanged; two existing locked crates added only as test deps.
Deleted obsolete isolated debug cache: freed41.8GB; financial history retained.
Installed stopped read-only probe; local preflightPASS, separate owner decision: ≤480s/4GiB/
$0.40 model/3 upstream attempts, HTTP/CU/sign/submit0, permanent STOP.
No new paid action; do not repeat Run15 or issue a new financial launch. Evidence: [transport preparation](audit/2026-09-28/run15-full-path/TRANSPORT_RECOVERY_PREPARATION_RU.md).

## Run14 incident baseline: no repeat or identical paid run

The owner activated Run14 once. STOP appeared after about 3,590 seconds,
before its four-hour deadline; the launcher finalized after 3,606 seconds
with `REFUSED_OR_UNKNOWN`, retaining only `ValueError`. The supervisor's
`normal_stop` means it observed STOP, not that the window expired normally.
The origin of STOP and the exact failed helper call are unproved. The stopped database
independently reads `NO_SIGNAL` with zero decisions, orders, dispatch, receipts,
fills and positions. STOP is set and all five containers are exited. Run14
must not be reactivated. Its complete incident decision and evidence are in
[`audit/2026-09-27/run14-systemic-review/RUN14_LIVE_INCIDENT_RU.md`](audit/2026-09-27/run14-systemic-review/RUN14_LIVE_INCIDENT_RU.md).

The daemon observed 902,865 transactions and 241,162 decoded swaps, but no
selected source or Admission across its saved one-hour stream; 18 reconnects
prevent a claim of complete observation. The terminal's repeated `AWAITING`
was a local parser error: app logs have flat tracing JSON, while the helper
expects nested fields. Offline fault injection also shows a failed Docker
inspect can escape the monitoring loop as bare `ValueError`; the exact Run14
failure call is not preserved. The monitor starts a Docker reader roughly every
0.5 seconds, a concrete VM load source. The brief observed VirtualMachine CPU
spike subsided after STOP; its precise cause is unproved.

The incident ledger holds 593 cumulative attempts, 8,510 CU and 4,467,750 nanoUSD;
the Run14 relay observed 19,184,389,247 upstream bytes plus 19,922,944
connection-headroom bytes. The $50 cumulative provider model and unchanged
trade limits are not reset. The offline helper repair above addresses the
reproduced observer faults; it does not establish the exact historical cause
of Run14's `ValueError`. Do not create or activate another trading package
until fresh source activity is evidenced and its scope/budget is explicit.
Automatic copying and profitability remain unproved.

## Prior decision: Run13 sealed under STOP after Run12 resource probe failure

The owner activated Run12 once. After about 94 minutes it returned
`REFUSED_OR_UNKNOWN`: the supervisor's `vm_resources` probe failed when its
Python `docker exec` returned 137. The saved final financial outcome is
`NO_SIGNAL`, not a submitted transaction of unknown outcome. The stopped Run12
database passes SQLite `quick_check` and has no cohort decision, order,
dispatch, unresolved dispatch, receipt, fill or position. STOP is set, all
five containers are stopped, and Run12 must not be reactivated. The historical
cause of exit 137 is not established. Its ledger ends at 308 cumulative RPC
attempts, 4,390 CU and 2,304,750 nanoUSD; accounted stream use through Run12
is 26,598,748,080 bytes including connection headroom.

Run13 changes only the package's resource probe helper: native `grep` and
`df` replace a Python child in the 256 MiB stream backend. The same memory
and disk floors apply. One immediate retry is allowed only for exit 137;
another 137, any other error, or malformed output stops the run. Targeted
helper tests passed 4/4 and a real offline probe in a 256 MiB container
passed. The unchanged, successful CI `copybot-app/release` artifact from commit
`b1cf0816a71a922e23c39ee3c84314af593a0fcd` and all 88 migrations were
installed and verified with rollback retained. No daemon rebuild or paid
provider call was needed.

The new package carries all 308 prior RPC attempts and stream obligations;
remaining caps are 488,797,327,440 stream bytes, 994 connection attempts,
2,495,610 RPC CU and 1,497,695,250 nanoUSD for HTTP/RPC within the existing
$50 provider model. Its four-hour limit, one BUY at most 0.01 SOL and one
linked source SELL remain unchanged. Independent package review accepted the
changed helper and carryover. The 52-file seal and final local activation
preflight passed. Run13 has empty financial state, STOP, and five containers
created but never started. Next authorized action: the owner may execute the
Run13 one-shot activation command once from its private `README_RU.md`.
Automatic copying and profitability remain unproved until live execution;
the repaired probe has not yet run through a live four-hour window.

## Prior decision: Run12 sealed under STOP after Run11 block capacity exit

The owner activated Run11 once. It returned `APP_EXITED` after about 126 seconds
with `association input rejected: BlockCapacity`; `NO_SIGNAL` is only its
financial state. Its durable inbox contains 377 parent events and exactly 192
accepted parents within the last 60 seconds, reaching the configured count cap
of 192. No cohort decision, order, dispatch, receipt, fill or position exists.
STOP is set, all five Run11 containers are stopped, and Run11 must not be
reactivated. Its ledger ended at 44 attempts, 540 CU and 283,500 nanoUSD;
stream use plus connection headroom brought cumulative Run07-Run11 accounted
stream bytes to 2,440,489,717.

A separate Run12 package keeps the same accepted `copybot-app/release` artifact
from commit `b1cf0816a71a922e23c39ee3c84314af593a0fcd` and successful CI
run `36194787410`; the artifact and 88 migrations were freshly installed and
verified in its isolated package. Only its runtime association cache changes
to 384 blocks / 768 MiB with the same 60-second TTL. The app container memory
cap rises from 2 to 3 GiB; four broker containers remain at 256 MiB each.
The total cap is 4 GiB within the 5,157,683,200-byte Docker VM. No trading or
provider cap changes. The new ledger carries all previous use, and new state
is empty under STOP with five never-started containers.

A scoped offline bridge replay admitted 201 full synthetic blocks per 60
seconds at 3,145,222 encoded bytes each, then settled a partial source SELL
with one mock send and no duplicate after restart. The first harness attempt
failed on test-thread stack size; the direct process with a 16 MiB test stack
passed. No paid provider call, signature or financial submit was made. An
independent review accepted the config, carryover, artifact and package. The
48-file seal and final local activation preflight passed. The macOS replay
peak RSS does not prove Linux live memory, and actual Run11 encoded block
sizes were not retained. Next authorized action: the owner may run Run12's
one-shot activation command once, listed in its private `README_RU.md`.
Automatic copying and profitability remain unproved until live receipt.

## Prior decision: Run11 prepared under STOP for one owner launch

The owner activated Run10 once. Its preserved `LIVE_RESULT.json` reports
`APP_EXITED` after about five minutes; `NO_SIGNAL` describes the financial
state, not completion of the four-hour observation. The daemon exited with
`owned_sell_rpc_transport` at 21:12:05 UTC. Five processed-slot fence epochs
had been saved through 21:11:03 UTC; the next periodic fence failed before
writing an epoch. All five containers are stopped with STOP set. The DB has
no cohort decision, order, dispatch, receipt, fill or position; no trade was
submitted. The Run10 ledger ended at 33 RPC attempts, 400 CU and 210,000
nanoUSD, inclusive of prior carryover. Its stream backend counted
1,581,808,029 received bytes and 2,097,152 connection headroom bytes. Run10
is used and must not be reactivated.

The narrow daemon repair keeps the consumer alive after transport, body or
deadline failure on the periodic fence and retries after 15 seconds. It
does not write a successful epoch for a failed request. Initial session
envelopes remain unacknowledged until their fence succeeds. Existing cohort
eligibility still requires a same-session epoch no older than 120 seconds;
proof mismatch, STOP and the immutable cohort deadline remain hard failures.
Scoped periodic recovery, initial cancellation/retry and actual offline
daemon BUY-to-SELL tests passed. An independent review accepted the changed
path and file/dependency constraints. The `copybot-app/release` artifact from
commit `b1cf0816a71a922e23c39ee3c84314af593a0fcd` and CI run
`36194787410` was installed into a separate Run11 package with all 88
migrations. The prior release remains available for rollback. Run11 carried
33 RPC attempts, 400 CU, 210,000 nanoUSD and 1,717,783,779 accounted stream
bytes from Run07-Run10. Its own financial state is empty, STOP is set, and
five containers were created but never started. Network-none startup without
a signer, 18 helper tests, independent package review, 43-file seal and final
local activation preflight passed. The remaining provider allowances and all
trading caps are listed in the private Run11 README. Next authorized action:
the owner may run its one-shot activation command once. A live automatic
copy or real RPC recovery remains unproved until that run's receipt/evidence.

## Prior decision: run 09 pre-start refusal; stopped run 10 prepared

The owner used Run09 once. Its preserved `LIVE_RESULT.json` reports
`REFUSED_OR_UNKNOWN`, `FileExistsError` and a missing database. The helper
tried to exclusively create `control/settings.json`, which the stopped
package had already prepared. All five Run09 containers remained created and
never started; no authority, lease, financial clock, database, order or send
was created. STOP is set. Its provider ledger stayed at the carried 14 RPC
attempts, 160 CU and 84,000 nanoUSD. The one-shot attempt marker remains;
Run09 must not be retried or reset.

An isolated Run10 package now validates the prepared settings before writing
authority or lease. Its preflight checks and seal bind that file. Targeted
helper/limit tests passed 13/13, reader lifecycle 5/5, and an independent
review accepted the helper and Run09-to-Run10 no-spend carryover. Deployable
code, config limits and migrations did not change, so Run10 reuses the
verified `copybot-app/release` artifact from commit `70a937d9` and CI run
`36180646007`. Its disposable network-none/no-signer startup, 44-file seal,
local activation preflight and separate independent package review passed.
Run10 has STOP, empty financial state, no activation marker/authority/clocks,
the same carried ledger obligations and five never-started containers. The
four-hour, $50 provider model, one BUY at most 0.01 SOL and one source SELL
limits remain unchanged. Preparation made no provider call, signature or
trade. The next authorized action is one owner execution of Run10's sealed
`activate_cohort_once.py`. Actual copying and profitability are unproved.
The exact package decision and command are in the private Run10 README and
`evidence/INDEPENDENT_PACKAGE_REVIEW_RU.md`.

## Prior decision: run 09 concurrent SELL repair accepted offline, artifact pending

Independent review accepted the narrow concurrent SELL repair on 2026-09-25.
Unrelated parent-block commits after a durable SELL reserve or complete no
longer cause a false terminal failure. Full ownership, amount and parent-graph
checks remain atomic at reserve, complete and final dispatch; a relevant
conflict still refuses the SELL. An undispatched hold stays explicit and
cannot silently trigger a second send. The accepted release-profile offline
replay used 36,000 synthetic parent blocks representing 14,400 logical
seconds, including full blocks in the final TTL. Parent commits continued
during simulation and blockhash acquisition. One mock SELL settled and was
accounted for; restart did not send again. This was a 167-second local test,
not four hours of live provider traffic. See the
[Run09 concurrent SELL decision](audit/2026-09-25/technical-cohort-09-concurrent/ACCEPTANCE_RU.md).

The changed deployable code now requires one matching `copybot-app/release`
GitHub Actions artifact installed into the same unused, stopped Run09
package, with the accepted `0ed4176` release retained for rollback. Update
binding and seal, then pass final local package preflight before presenting
the one-shot owner command. This preparation must make no provider call,
signature or trade. The accepted $50/four-hour/trading/resource caps and
Run07/Run08 accounting remain unchanged. Live SELL timing, actual block-size
maximum and profitability remain unknown.

## Prior decision: run 09 capacity repair accepted offline

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

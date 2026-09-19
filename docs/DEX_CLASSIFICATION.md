# Canonical observation DEX labels

`SwapParser` labels the configured protocol families present in an observation.
The label is not a claim that one venue executed the transaction, that a particular
CPI succeeded, or that an aggregate wallet balance change belongs to one route leg.
Quantities, supported-instruction checks and admission policy are separate.

| Recognized program memberships | Canonical `SwapEvent.dex` |
| --- | --- |
| Raydium only, including several configured Raydium programs | `raydium` |
| PumpSwap only | `pumpswap` |
| Both Raydium and PumpSwap | `multi_dex` |
| Neither | Existing hint fallback: Raydium substring, then Pump substring, otherwise reject |

Program order and duplicate program IDs have no effect. An identifier configured
in both families also yields `multi_dex`: the configuration does not support a
single-family claim. Unknown programs do not become additional recognized families.
Explicit recognized program evidence takes priority over hints, as before.

The existing upstream decoder still extracts program IDs and determines its hint,
including the existing Raydium-first hint precedence. Its supported/rejected gates
are unchanged. This change affects the canonical `SwapEvent` classifier used by
legacy ingestion and scoped capture; it does not redefine the separate association
facts' `dex_hint` field or perform execution-venue attribution.

DEX participates in event identity and persisted equality checks. Existing rows and
historical capture payloads are never relabelled in place. Reprocessing an old
multi-family envelope under this policy intentionally produces `multi_dex` where
the old order-dependent classifier stored either family. That is an explicit
semantic transition, not an identical mixed-version event or an accounting repair.
Future consumers must use the corresponding source/artifact policy consistently.

Offline verification uses `copybot-ingestion --test dex_policy_replay` and the
saved mixed-family envelope. To check the closed 1146-row September 19 export, set
`DEX_POLICY_REPLAY_DB` and `DEX_POLICY_REPLAY_CONFIG` (JSON ingestion settings),
and select `dex_policy_live_export_preserves_every_non_policy_field -- --ignored`.
The export test is explicitly ignored without this opt-in; missing input is an error.
`DEX_POLICY_REPLAY_REPORT` optionally names a new report file; existing files are
not overwritten. The oracle independently reads configured family evidence from
protobuf program references/logs, checks every new DEX label, records each intended
old-to-new change, and compares all remaining receipt and canonical fields exactly.
It opens the source database read-only and writes only temporary replay state.
The older strict replay and its RED evidence remain unchanged.

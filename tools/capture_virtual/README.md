# Scoped observation capture

This adapter connects a new, task-owned virtual ledger to the legacy Yellowstone
consumer's separate capture database. It grants no execution authority. The current
batch is locally accepted; publication, daemon launch and provider access are separate
owner decisions. It does not repair the completed September 14 portfolio.

## Boundary and configuration

`ingestion.capture_scope_db` defaults to `None`. When explicitly set, configuration
requires `source="yellowstone_grpc"`, `yellowstone_delivery_mode="legacy"`, and all
three `execution.enabled`, `execution.canary_tiny_submit_enabled`, and
`execution.tiny_experiment.activate` flags false. No followlist or runtime position
is published. An updated matching app artifact is needed before live use.

Create a new separate database with `CaptureControl.create(path, max_rows=...,
max_bytes=...)`. It refuses existing files; `CaptureControl(path)` and the Rust
consumer refuse missing databases. Keep capture and virtual DB identities together
across restart. Initial binding requires an empty owned ledger. Never pass the old
experiment's ledger to this adapter to repair it retrospectively.

Both processes must see the same SQLite database and sidecars on storage with valid
SQLite locking. Sharing a copied file or stale host export is not a consumer ACK.
The SQL schema is `crates/storage-core/src/capture_scope/schema.sql`; it is separate
from runtime migrations and from the virtual ledger schema.

## Publication and consumption

1. Construct `CaptureVirtualAdapter(control, ledger)` with the virtual ledger object.
2. `publish_decision(decision)` persists a request and returns
   `PENDING_CONSUMER_ACK`. It does not publish an available virtual admission.
3. The real consumer restores persisted protection and unfinished receives, then
   commits scope installation with an epoch, gap version and receive watermark.
4. `poll_ack(request_id)` checks that ACK, publishes the ledger decision and saves
   a later availability watermark/time. Merely writing the request is insufficient.
5. Read `next_sequence()`, then `observe(seq)` in order. An unfinished RECEIVED row
   waits for decoding. DURABLE rows contain the canonical decoded swap; REJECTED
   rows remain explicit terminal decode evidence. New delivery cannot skip an
   earlier unfinished event. DONE/SKIPPED retries are idempotent.

The controller also checks the current consumer epoch/gap, expiry, membership,
receive sequence, receive time and provider observation time before a BUY. A frame
received before activation cannot gain admission from a later duplicate. Provider
created-at is observation time; this does not establish chain execution time.

Before calling the real ledger's BUY path, the adapter commits a wallet/mint pin.
Demotion and reservation serialize in the same SQLite database. A BUY demoted
before reservation is skipped permanently. If demotion occurs after reservation,
the pin already protects its risk. A crash before/after ledger commit is reconciled
against the canonical ledger event; it does not produce a second lot.

Membership protects all mints of that wallet. An unfinished pin conservatively
keeps capture for the whole wallet, including rejected transactions whose mint
cannot be decoded. Pending/open/unresolved pins survive demotion, expiry, temporary
membership absence and restart. No pin-release/settlement API is provided in this
batch; it never infers settlement from absent membership or an absent ledger row.

## Durability, pressure and evidence

The real stream awaits one blocking capture operation before its legacy
signature-dedupe/reorder/output queue. SQLite does not run on the async receive
thread. For a scoped transaction, the exact delivered protobuf envelope commits as
RECEIVED before the real decoder and SwapParser run; finish commits DURABLE with
canonical JSON or REJECTED with a bounded reason, then verifies readback.

Dedupe uses signature plus an immutable transaction fingerprint. Different envelope
filters/created-at on redelivery keep the first receipt. Changed transaction facts
for the same signature fail capture. Restart decodes the exact saved envelope,
including missing/invalid timestamp, before accepting new requests or input.

There is at most one capture operation in flight. Limits are explicit, persisted
and checked before accepting another row: at most 1,000,000 events, 1 GiB charged
payload, 8 MiB per envelope, 16 KiB reserved decoded JSON per event, 128 wallets per
request and 128 pending requests. Controller requests consume their own bounded
payload charge. These are hard validation ceilings, not recommended session sizes
or measured process RSS; SQLite indexes/pages and protobuf/transport overhead also
consume space. There is no eviction and no global backpressure-disable switch.

Capacity, I/O or integrity failure stops the capture path. A successful persistent
failure mark and every observed provider discontinuity advance the gap version,
invalidating old admission ACKs. If the disk cannot save the failure, the consumer
still stops; absence of further receipts is not complete coverage. A restart always
records a coverage discontinuity and fences prior consumer epochs. Existing pins
remain even when new BUYs are denied.

`capture_events` links receive epoch/sequence, signature, original bytes,
received/source time, decode disposition and durable JSON. `capture_meta.gap` and
`reason` describe consumer/upstream discontinuity separately. Missing or malformed
wallet identity cannot prove an update is outside scope and advances the gap.
An upstream frame never delivered by tonic has no per-signature receive evidence.
This database never claims complete provider or pre-admission Discovery coverage.

The new adapter provides the guarded ordered API. A future bounded runner still
has to connect it to the approved stream lifecycle and budget accounting; no runner
or economic experiment is started by importing these modules.

## Offline live-capture reconciliation

After automatic shutdown, export a consistent capture SQLite database from Linux.
The dedicated `copybot-ingestion` integration test `capture_live_replay` accepts
`CAPTURE_REPLAY_DB` and `CAPTURE_REPLAY_CONFIG`. The latter is a JSON object of the
actual ingestion settings, including the exact Raydium/PumpSwap program lists;
exclude endpoint credentials. The test opens the export read-only and feeds each
saved envelope through the real decoder into a temporary capture database. It
compares signature, wallet, slot, source time, raw envelope, transaction fingerprint,
terminal stage, reason and canonical event JSON. It never opens a network stream.
Without these environment variables, the test uses two saved supported envelopes
and a distinct missing-time rejection. This oracle complements the controller's
ACK/receive timing, interval, config/artifact and shutdown evidence; it does not
establish upstream completeness or economic results.

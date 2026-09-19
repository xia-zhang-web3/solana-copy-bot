# Scoped virtual forward runner

Accepted local runtime helpers for the bounded, no-trade Discovery/capture experiment.
The daemon source in this commit includes matching deterministic DEX classification
and capture capacity validation. Deployment is separate from local acceptance.

Copy helpers and capacity config into a fresh task-owned evidence layout. Supply
CONTEXT.json with explicit repo, source, runtime and evidence roots. Live DBs belong
to the named Linux volume; the host does not open capture/virtual SQLite. The
accepted proof decoder and its public protocol IDL are bundled under helpers/accepted_proof.
Machine bindings, credentials, prior evidence, DBs, artifacts and launch permissions
are intentionally not part of this source directory.

The task layout requires a verified MATCHING_ARTIFACT.json, explicit LAUNCH_SCOPE.json,
applicable prestart approval and resource checks before session_ctl.py start.
All execution flags remain false. The durable global deadline is28800seconds;
startup/reconnect do not extend it. The combined provider ceiling is100USD including
prior reservations. No per-provider small subcaps are introduced.

This is versioned reproducibility source, not a standalone portable launch package.
The local layout still binds accepted Discovery operator binaries, schema seed, CA,
private HTTP policy and provider environment through the explicit context.

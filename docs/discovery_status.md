# Discovery Status Command

Use `discovery_status` to inspect the persisted discovery/runtime state without
opening logs.

Build and run from the repo root:

```bash
cargo run -p copybot-discovery --quiet --bin discovery_status -- --config /path/to/config.toml
```

Optional flags:

- `--db-path /path/to/runtime.db`: override the SQLite path from config
- `--json`: print deterministic JSON instead of the default compact text view
- `--now 2026-03-23T12:00:00Z`: pin time for deterministic local fixture checks

Default output fields:

- `runtime_state`
- `runtime_mode`
- `scoring_source`
- `active_follow_wallets`
- `recent_swaps_window`
- `raw_window_state`
- `latest_publication_ts`
- `publication_age_seconds`
- `publication_runtime_mode`
- `publication_scoring_source`
- `recent_publication_truth_available`
- `persisted_rebuild_phase`
- `bounded_rebuild_cursor`
- `offline_recovery_state`
- `offline_recovery_cursor`
- `aggregate_covered_through_cursor`

Interpretation:

- `runtime_state=healthy_runtime_truth`: raw window is healthy and drives runtime truth
- `runtime_state=degraded_recent_publication_truth`: runtime is degraded onto recent publication truth
- `runtime_state=fail_closed_rebuild_in_progress`: runtime is fail-closed and a persisted bounded rebuild is present
- `offline_recovery_state=backfill_in_progress`: offline aggregate recovery is advancing independently of runtime truth

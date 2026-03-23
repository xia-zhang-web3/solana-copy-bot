# Discovery Cutover Readiness

Use `discovery_cutover_readiness` to answer whether the system is ready for a
future post-recovery re-enable step without confusing runtime truth with
offline recovery progress.

Run from the repo root:

```bash
cargo run -p copybot-discovery --quiet --bin discovery_cutover_readiness -- --config /path/to/config.toml
```

Optional flags:

- `--db-path /path/to/runtime.db`: override the SQLite path from config
- `--json`: print deterministic JSON instead of the default compact text view
- `--now 2026-03-23T12:00:00Z`: pin time for deterministic local fixture checks

Default output sections:

- `verdict`
- `blockers`
- `runtime_truth.*`
- `publication_truth.*`
- `offline_recovery.*`

The command is intentionally read-only and uses offline recovery only as an
operator readiness signal. It does not wire aggregate recovery state back into
runtime truth or runtime selection.

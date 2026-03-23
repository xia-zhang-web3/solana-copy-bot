# Discovery Perf Harness

Standard command:

```bash
RUSTFLAGS='-Awarnings' cargo run -p copybot-discovery --quiet --bin discovery_perf_harness
```

What it runs:

- stale `CollectBuyMints` convergence on an exact carry-forward checkpoint
- bounded `Replay` on a local deterministic noise fixture

What the JSON report includes:

- scenario and fixture metadata
- phase name
- rows processed
- pages processed
- chunk count
- cursor progress markers
- pending-batch progress for stale `CollectBuyMints`
- wallet-stats cursor/progress for bounded `Replay`
- timing fields (`cycle_elapsed_ms`, `total_elapsed_ms`, `quality_rpc_spent_ms`)

Use the same command and profile across before/after runs so future discovery slices can compare progress on the same local bottleneck fixture.

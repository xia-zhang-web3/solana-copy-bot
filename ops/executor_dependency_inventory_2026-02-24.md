# Executor Dependency Inventory (Phase 0)

Date: 2026-02-24
Owner: execution-dev

## Rust Crate Dependencies (current scaffold)

1. `axum` (HTTP server)
2. `reqwest` (HTTP client with rustls)
3. `serde` / `serde_json`
4. `tokio`
5. `tracing` / `tracing-subscriber`
6. `chrono`
7. `hmac` + `sha2`
8. `base64` + `bs58`
9. `url`
10. `anyhow`

## Planned Solana/Route Dependencies (Phase 2+)

1. Solana SDK / RPC client (version pin required before Phase 2 merge)
2. Jupiter/Metis quote+swap API client (or equivalent aggregator)
3. Jito route client/integration (required for DoD)
4. Fastlane route integration (optional, feature-gated)

## Pinning Policy

1. All new route/signing dependencies must be pinned to explicit versions in `Cargo.toml`.
2. Any major version bump requires:
   1. compatibility test run,
   2. update to `ops/executor_contract_v1.md` if behavior changes,
   3. auditor note in phase evidence markdown.

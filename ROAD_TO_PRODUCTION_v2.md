# ROAD TO PRODUCTION v2

Date: 2026-05-03
Status: Active operating contract after Discovery V2 / build-architecture pivot

This file is intentionally short. Old incident logs, removed operator lanes,
and deleted proof tools are not active procedure. Do not use historical text to
recreate deleted binaries or server loops.

## Current State

Production discovery remains fail-closed until Discovery V2 proves current
raw truth, wallet eligibility, and publication safety from live evidence.

The old aggregate-materialization repair lane is no longer the primary path to
production readiness. It may remain historical projection evidence, but it must
not own the active green/red decision for Discovery V2.

The old raw-history / program-history restore and gap-fill lane has been
removed from active code and config. Do not restore those tools, config
sections, or loop wrappers as the production discovery path.

The old execution / devnet / activation package lane has been removed from
`copybot-app`. Future execution work must be redesigned as dedicated operator
crates after Discovery V2 is production-trustworthy.

## Non-Negotiables

1. `execution.enabled` remains disabled unless the user explicitly authorizes a
   separate execution rollout.
2. Fail-closed behavior must not be weakened.
3. `scoring_window_days` must not be reduced to manufacture a green result.
4. Local-only, stale, devnet, or non-production evidence does not authorize
   production discovery.
5. Production hosts must not be used as normal build machines. Build artifacts
   locally or in CI and deploy artifacts.
6. `copybot-app` must not receive new operator binaries.
7. New diagnostics belong in dedicated operator crates or existing lightweight
   V2 binaries.
8. Removed legacy bins, config sections, and runbooks must not be recreated
   without a fresh architecture decision.

## Active Discovery V2 Contract

Discovery V2 must prove all of the following before publication is trusted:

1. fresh raw / recent_raw evidence is available for the evaluated horizon
2. wallet filtering uses the configured selector and quality policy
3. missing liquidity, rug, thin-market, open-position, or quality evidence
   fails closed instead of producing a false green
4. publish candidates are post-filtered by eligibility and score
5. blocked `--commit` publication exits before any database mutation
6. status output explains every blocker using current evidence, not old repair
   progress
7. production config cannot silently disable required publication gates

Current active V2 surfaces:

1. `copybot-discovery-v2`
2. `discovery_v2_status`
3. `discovery_v2_publish`

Active scheduled legacy discovery surfaces retained only for runtime artifact
maintenance:

1. `discovery_runtime_export`
2. `discovery_recent_raw_snapshot`

## Build Architecture Contract

The active build-reduction plan lives in:

1. `BUILD_POLICY.md`
2. `BUILD_REFACTOR_ROADMAP.md`
3. `ARTIFACT_DEPLOY.md`
4. `BUILD_ARCHITECTURE_REDESIGN_NO_HOUR_BUILDS.md`
5. `ARCHITECTURE_WAIVERS.md`

The direction is:

1. artifact-first deploy
2. dedicated operator crates
3. storage-core facade before new read-only operators grow storage coupling
4. app quarantine
5. automated architecture guard
6. file-size and inline-test enforcement

## Active Crate Boundaries

`copybot-app`:

- live runtime only
- no new bins
- no proof, package, activation, or repair operators

`copybot-discovery-v2`:

- current production discovery proof and publication path
- status / publish operators only

`copybot-storage-core`:

- lightweight read-only SQLite access for operators
- no runtime execution dependency graph

`copybot-storage`:

- legacy runtime storage until further split
- should shrink, not grow

`copybot-operators`, `copybot-storage-ops`, `copybot-live-ops`:

- bounded operator work outside `copybot-app`
- artifact-built before production rollout

## Deleted Legacy Lanes

The following lanes are intentionally removed from active planning:

1. raw-history / program-history restore and gap-fill
2. aggregate backfill as production discovery gate owner
3. old discovery status / perf / cutover proof bins
4. app-owned activation package generators
5. app-owned devnet rehearsal operators
6. executor rollout smoke/report scripts as Discovery V2 gates
7. historical server report archive as active proof source

Historical details can be recovered from git history if needed. They should not
be kept in active roadmap text.

## Review Standard

Every accepted batch must include:

1. exact files changed
2. commands run
3. semantic risk checked
4. out-of-scope areas preserved
5. whether deployment is required

Reject a batch if it:

1. creates false green conditions
2. treats missing evidence as clean
3. reintroduces server-side release builds as normal workflow
4. adds operators to `copybot-app`
5. grows storage/app coupling for V2 without a facade
6. weakens production safety gates

## Next Work

1. Continue removing legacy execution/devnet/operator leftovers from app,
   storage, tools, docs, and server templates.
2. Keep Discovery V2 isolated and fail-closed.
3. Move any remaining required read-only runtime access behind storage-core.
4. Run architecture guard and focused cargo checks after each large cleanup
   batch.

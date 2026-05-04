# Build Architecture Redesign: No Hour-Long Production Builds

Status: index / entry point
Date: 2026-05-03

This file is intentionally short. The build redesign is split into enforceable
documents so the policy itself does not become another large unmaintainable
file.

## Required Documents

1. [BUILD_POLICY.md](BUILD_POLICY.md)
   - hard build rules
   - file-size policy
   - no-inline-tests policy
   - app quarantine
   - dependency restrictions
   - automated guard contract

2. [BUILD_REFACTOR_ROADMAP.md](BUILD_REFACTOR_ROADMAP.md)
   - storage-core first
   - discovery-v2 extraction
   - app/main split
   - observed-swap-writer split
   - storage/discovery legacy split
   - first cutting tasks

3. [ARTIFACT_DEPLOY.md](ARTIFACT_DEPLOY.md)
   - artifact-first deployment
   - production-local build ban
   - binary manifest/checksum rules
   - install/rollback flow
   - emergency fallback

## Current Verdict

The old plan was accepted only as the first build-architecture document, not as
a complete refactor contract.

The missing pieces were:

1. hard file-size policy,
2. no inline tests in production files,
3. automated enforcement,
4. immediate `copybot-app` quarantine,
5. storage-core before Discovery V2 extraction,
6. concrete first cutting tasks for the existing large files.

Those rules now live in the required documents above.

## Non-Negotiable Summary

1. Production servers are not build machines.
2. `copybot-app` is not an operator bin bucket.
3. New operators do not go under `crates/app/src/bin`.
4. New production modules must stay small.
5. Test bodies live outside production files.
6. Discovery V2 must not depend on legacy discovery or monolithic storage after
   extraction.
7. Build budgets are architecture gates.
8. Architecture guard must run before accepting build/refactor batches.

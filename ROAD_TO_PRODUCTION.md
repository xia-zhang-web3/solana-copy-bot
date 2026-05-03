# ROAD TO PRODUCTION

Status: LEGACY

This file is intentionally reduced to a legacy pointer.

Current incident truth:

1. The acute live `recent_raw` startup deadlock has been removed.
2. The project is still not production-ready because the Discovery V2/current
   operational truth surface is still stale and Stage 3 is still non-green.

Why:

1. The old roadmap mixed together:
   - Yellowstone migration history
   - bootstrap/control-plane redesign
   - aggregate/backfill recovery archaeology
   - execution rollout
2. That made the current production path harder to understand instead of clearer.

Current source of truth:

1. Use [ROAD_TO_PRODUCTION_v2.md](/Users/tigranambarcumyan/Documents/solana-copy-bot/ROAD_TO_PRODUCTION_v2.md).

Facts preserved from the old roadmap:

1. Yellowstone gRPC is already the intended live ingestion path.
2. `copybot-app` remains the main runtime binary.
3. Execution code exists, but live execution is still disabled in current live config.
4. Aggregate/backfill recovery is not an active production path.

Do not use this file for implementation planning.

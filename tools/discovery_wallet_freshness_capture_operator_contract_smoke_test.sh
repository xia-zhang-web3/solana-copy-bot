#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
README_PATH="$ROOT_DIR/ops/server_templates/README.md"
ROAD_PATH="$ROOT_DIR/ROAD_TO_PRODUCTION_v2.md"
SERVICE_PATH="$ROOT_DIR/ops/server_templates/copybot-discovery-wallet-freshness-capture.service"
TIMER_PATH="$ROOT_DIR/ops/server_templates/copybot-discovery-wallet-freshness-capture.timer"
if [[ -e "$SERVICE_PATH" ]]; then
  echo "legacy Stage 3 capture service should be removed: $SERVICE_PATH" >&2
  exit 1
fi

if [[ -e "$TIMER_PATH" ]]; then
  echo "legacy Stage 3 capture timer should be removed: $TIMER_PATH" >&2
  exit 1
fi

python3 - "$README_PATH" "$ROAD_PATH" <<'PY'
import pathlib
import sys

readme = pathlib.Path(sys.argv[1]).read_text()
road = pathlib.Path(sys.argv[2]).read_text()

assert "solana-copy-bot.service" in readme, "README must document the in-band Stage 3 capture path"
assert "wallet_freshness_capture_state" in readme, "README must expose in-band capture observability fields"
assert "journalctl -u solana-copy-bot.service" in readme, "README must show the in-band capture inspection path"
assert "discovery_wallet_freshness_report --config /etc/solana-copy-bot/live.server.toml" in readme, "README must show report command"
assert "latest_capture_age_seconds" in readme, "README must explain current report freshness fields"
assert "manual/debug" in readme, "README must downgrade the standalone capture path to manual/debug"
assert "discovery_wallet_freshness_capture --config /etc/solana-copy-bot/live.server.toml --recent-cycles 1 --shadow-evidence-lookback-seconds 960 --json" in readme, "README must keep the exact manual/debug capture command"
assert "--shadow-evidence-lookback-seconds 960" in readme, "README must explain the standalone debug evidence lookback"
assert "mode=manual_debug" in readme, "README must surface the manual/debug capture mode"
assert "execution.enabled = false" in readme, "README must restate no execution activation"
assert "copybot-discovery-wallet-freshness-capture.service" not in readme, "README must not keep removed Stage 3 capture service in the primary docs"
assert "copybot-discovery-wallet-freshness-capture.timer" not in readme, "README must not keep removed Stage 3 capture timer in the primary docs"

assert "solana-copy-bot.service" in road, "ROAD must mention the in-band Stage 3 accumulation path"
assert "wallet_freshness_capture_state" in road, "ROAD must record in-band capture observability fields"
assert "manual/debug" in road, "ROAD must keep the standalone capture command explicitly secondary"
assert "--recent-cycles 1" in road, "ROAD must retain the standalone manual/debug capture command"
assert "--shadow-evidence-lookback-seconds 960" in road, "ROAD must retain the manual/debug gap-free evidence lookback"
assert "copybot-discovery-wallet-freshness-capture.service" not in road, "ROAD must not keep removed Stage 3 capture service in the accepted architecture"
assert "copybot-discovery-wallet-freshness-capture.timer" not in road, "ROAD must not keep removed Stage 3 capture timer in the accepted architecture"
PY

echo "discovery_wallet_freshness_capture_operator_contract_smoke_test: ok"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
README_PATH="$ROOT_DIR/ops/server_templates/README.md"
ROAD_PATH="$ROOT_DIR/ROAD_TO_PRODUCTION_v2.md"
SERVICE_PATH="$ROOT_DIR/ops/server_templates/copybot-discovery-wallet-freshness-capture.service"
TIMER_PATH="$ROOT_DIR/ops/server_templates/copybot-discovery-wallet-freshness-capture.timer"

require_line() {
  local pattern="$1"
  local file_path="$2"
  grep -Fq -- "$pattern" "$file_path" || {
    echo "missing required pattern '$pattern' in $file_path" >&2
    exit 1
  }
}

require_line "ExecStart=/var/www/solana-copy-bot/target/release/discovery_wallet_freshness_capture --config /etc/solana-copy-bot/live.server.toml --recent-cycles 3 --json" "$SERVICE_PATH"
require_line "TimeoutStartSec=5min" "$SERVICE_PATH"
require_line "StandardOutput=journal" "$SERVICE_PATH"
require_line "StandardError=journal" "$SERVICE_PATH"
require_line "SyslogIdentifier=discovery_wallet_freshness_capture" "$SERVICE_PATH"

require_line "OnBootSec=12m" "$TIMER_PATH"
require_line "OnUnitActiveSec=15m" "$TIMER_PATH"
require_line "RandomizedDelaySec=60s" "$TIMER_PATH"
require_line "Persistent=true" "$TIMER_PATH"
require_line "Unit=copybot-discovery-wallet-freshness-capture.service" "$TIMER_PATH"

python3 - "$README_PATH" "$ROAD_PATH" <<'PY'
import pathlib
import sys

readme = pathlib.Path(sys.argv[1]).read_text()
road = pathlib.Path(sys.argv[2]).read_text()

assert "copybot-discovery-wallet-freshness-capture.service" in readme, "README must list Stage 3 capture service"
assert "copybot-discovery-wallet-freshness-capture.timer" in readme, "README must list Stage 3 capture timer"
assert "discovery_wallet_freshness_capture --config /etc/solana-copy-bot/live.server.toml" in readme, "README must show exact capture command"
assert "discovery_wallet_freshness_report --config /etc/solana-copy-bot/live.server.toml" in readme, "README must show report command"
assert "latest_capture_age_seconds" in readme, "README must explain current report freshness fields"
assert "journalctl -u copybot-discovery-wallet-freshness-capture.service" in readme, "README must show failure inspection path"
assert "15 minute capture cadence" in readme, "README must explain capture cadence"
assert "execution.enabled = false" in readme, "README must restate no execution activation"

assert "copybot-discovery-wallet-freshness-capture.timer" in road, "ROAD must mention scheduled Stage 3 capture path"
assert "15 minute cadence" in road, "ROAD must record the accepted cadence"
PY

echo "discovery_wallet_freshness_capture_operator_contract_smoke_test: ok"

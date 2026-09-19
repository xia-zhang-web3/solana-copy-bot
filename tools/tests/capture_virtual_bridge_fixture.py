"""Offline integration fixture: only caller-owned temporary databases, no HTTP workers."""
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "tools/tests/fixtures/capture_ledger"))
from capture_virtual import CaptureControl, CaptureVirtualAdapter
from virtual_engine import VirtualLedger

directory, wallet, phase = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
if phase == "init":
    control = CaptureControl.create(directory / "capture.db", max_rows=100, max_bytes=16_777_216)
else:
    control = CaptureControl(directory / "capture.db")
ledger = VirtualLedger(directory / "virtual.db")
bridge = CaptureVirtualAdapter(control, ledger)
if phase == "init":
    result = bridge.publish_decision({"decision_id": "one", "available_at": time.time()-1,
                                     "valid_until": time.time()+300, "admissible": True, "wallets": [wallet]})
    assert result["status"] == "PENDING_CONSUMER_ACK"
    assert ledger.db.execute("SELECT count(*) FROM decisions").fetchone()[0] == 0
elif phase == "admit":
    result = bridge.poll_ack(1)
    assert result["status"] == "ACKED"
elif phase == "observe":
    result = bridge.observe(int(sys.argv[4]))
elif phase == "demote":
    result = bridge.publish_decision({"decision_id": "two", "available_at": time.time()-1,
                                     "valid_until": time.time()+300, "admissible": False, "wallets": []})
else:
    raise AssertionError(phase)
print(json.dumps(result, default=str))
control.close()
ledger.close()

"""Offline Rust→SQLite→actual adapter seam; no network or fabricated source clocks."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser()
parser.add_argument('capture_db', type=Path)
parser.add_argument('ledger_db', type=Path)
parser.add_argument('--helpers', type=Path, default=ROOT / 'tools/tests/fixtures/capture_ledger')
parser.add_argument('--limit', type=int, default=128)
args = parser.parse_args()
if not 1 <= args.limit <= 128:
    parser.error('limit must be 1..128')
sys.path[:0] = [str(ROOT / 'tools'), str(args.helpers.resolve(strict=True))]
from capture_virtual import CaptureControl, CaptureVirtualAdapter
import virtual_engine

control = CaptureControl(args.capture_db)
ledger = virtual_engine.VirtualLedger(args.ledger_db)
try:
    adapter = CaptureVirtualAdapter(control, ledger)
    before = list(ledger.db.iterdump())
    polls, live = 0, None
    # The existing CaptureSession calls next_sequence(), not this script's
    # observe_recovery: demonstrate that the real poll drains pending evidence.
    while adapter.next_recovery() is not None and polls < args.limit:
        live = adapter.next_sequence()
        polls += 1
    if adapter.next_recovery() is not None:
        raise RuntimeError('fixture recovery limit reached')
    after = list(ledger.db.iterdump())
    if before != after:
        raise AssertionError('recovered evidence modified financial ledger')
    outbox = [dict(r) for r in control.db.execute('SELECT * FROM capture_recovery_outbox ORDER BY recovery_id')]
    dispositions = [json.loads(r['disposition_json']) for r in outbox]
    print(json.dumps(dict(polls=polls, live_sequence=live, outbox=outbox, dispositions=dispositions,
                          ledger_unchanged=True, network_calls=0,
                          imports=dict(consumer=str(Path(sys.modules[CaptureVirtualAdapter.__module__].__file__).resolve()),
                                       virtual_engine=str(Path(virtual_engine.__file__).resolve())),
                          consumer_sha256=hashlib.sha256(Path(sys.modules[CaptureVirtualAdapter.__module__].__file__).read_bytes()).hexdigest()),
                     sort_keys=True))
finally:
    ledger.close()
    control.close()

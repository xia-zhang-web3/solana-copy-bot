"""LOCAL stream-control fixture; real adapter/ledger, no workers or provider calls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import sqlite3
import time

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser()
parser.add_argument('phase', choices=('prepare', 'publish', 'observe', 'repeat'))
parser.add_argument('capture_db', type=Path)
parser.add_argument('ledger_db', type=Path)
parser.add_argument('--wallet', required=True)
parser.add_argument('--helpers', type=Path,
                    default=Path(os.environ.get('CAPTURE_LIVE_TEST_HELPERS',
                                                ROOT / 'tools/tests/fixtures/capture_ledger')))
args = parser.parse_args()
os.environ["FORWARD_TRACE_PATH"] = str(args.ledger_db.with_suffix(".local-trace.jsonl"))
os.environ["FORWARD_TRACE_SESSION_ID"] = "LOCAL-positive-stream-fixture"
sys.path[:0] = [str(ROOT / 'tools'), str(args.helpers.resolve(strict=True))]
from capture_virtual import CaptureControl, CaptureVirtualAdapter
import virtual_engine
import virtual_store


def snapshot(ledger):
    return {table: [dict(row) for row in ledger.db.execute('SELECT * FROM ' + table + ' ORDER BY id')]
            for table in ('events', 'lots', 'jobs')}


control = CaptureControl(args.capture_db)
ledger = virtual_engine.VirtualLedger(args.ledger_db)
try:
    adapter = CaptureVirtualAdapter(control, ledger)
    result = None
    if args.phase == 'prepare':
        result = adapter.publish_decision(dict(decision_id='LOCAL-stream-positive',
                                              available_at=time.time()-1,
                                              valid_until=time.time()+300,
                                              admissible=True, wallets=[args.wallet]))
        assert result['request_id'] == 1 and result['status'] == 'PENDING_CONSUMER_ACK'
    elif args.phase == 'publish':
        result = adapter.poll_ack(1)
        assert result['status'] == 'ACKED', result
    elif args.phase == 'observe':
        seq = adapter.next_sequence()
        assert seq == 1, seq
        actual_observe = ledger.observe
        pin_seen = []
        def checked_observe(event, detected_at):
            # Read through a different connection before the actual ledger call:
            # this proves pin commit, rather than the writer's uncommitted view.
            with sqlite3.connect(args.capture_db) as independent:
                pin_seen.append(independent.execute(
                    "SELECT state FROM capture_obligations WHERE event_seq=1").fetchone())
            assert pin_seen[-1] == ('PENDING',)
            assert ledger.db.execute('SELECT count(*) FROM lots').fetchone()[0] == 0
            return actual_observe(event, detected_at)
        ledger.observe = checked_observe
        result = adapter.observe(seq)
        assert pin_seen == [('PENDING',)]
        assert result['status'] == 'DELIVERED', result
        assert result['ledger_event']['status'] == 'BUY_PENDING', result
        pin = control.db.execute('SELECT state,ledger_event_id FROM capture_obligations WHERE event_seq=1').fetchone()
        assert pin and pin['state'] == 'PENDING' and pin['ledger_event_id'] == result['ledger_event']['id']
        state = snapshot(ledger)
        assert len(state['events']) == len(state['lots']) == 1
        assert sorted(row['kind'] for row in state['jobs']) == ['BUY_PROOF', 'BUY_QUOTE']
        args.ledger_db.with_suffix('.positive.json').write_text(json.dumps(state, sort_keys=True))
    else:
        before = list(ledger.db.iterdump())
        assert adapter.next_sequence() is None
        result = adapter.observe(1)
        assert result['status'] == 'DELIVERED'
        assert list(ledger.db.iterdump()) == before, 'duplicate observation mutated ledger'
        assert snapshot(ledger) == json.loads(args.ledger_db.with_suffix('.positive.json').read_text())
        assert control.db.execute('SELECT count(*) FROM capture_events').fetchone()[0] == 1
        assert control.db.execute('SELECT count(*) FROM capture_recovery_outbox').fetchone()[0] == 0
    modules = dict(consumer=Path(sys.modules[CaptureVirtualAdapter.__module__].__file__).resolve(),
                   virtual_engine=Path(virtual_engine.__file__).resolve(),
                   virtual_store=Path(virtual_store.__file__).resolve())
    state = snapshot(ledger)
    print(json.dumps(dict(result=result, counts={key: len(value) for key, value in state.items()},
                          network_calls=0, imports={key: str(value) for key, value in modules.items()},
                          hashes={key: hashlib.sha256(value.read_bytes()).hexdigest()
                                  for key, value in modules.items()}), sort_keys=True))
finally:
    ledger.close()
    control.close()

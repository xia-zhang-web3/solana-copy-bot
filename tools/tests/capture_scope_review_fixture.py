"""Independent admission probe. Seeded durable BUY; no stream/block proof claim."""
import hashlib
import json
import os
from pathlib import Path
import sys
from datetime import datetime, timezone

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
HELPERS = Path(os.environ.get('PROTECTED_REVIEW_ACTUAL_HELPERS', str(ROOT/'tools/tests/fixtures/capture_ledger')))
for name in ('FORWARD_TRACE_PATH', 'FORWARD_TRACE_SESSION_ID'):
    os.environ.pop(name, None)
sys.path[:0] = [str(ROOT/'tools'), str(HELPERS)]
from capture_virtual import CaptureControl, CaptureVirtualAdapter
from capture_virtual.control import encoded
import virtual_engine

operation, capture_path, ledger_path, request_id = sys.argv[1:]
request_id = int(request_id)
control = CaptureControl(capture_path)
ledger = virtual_engine.VirtualLedger(ledger_path)
adapter = CaptureVirtualAdapter(control, ledger, clock=lambda: 2_000_000_002.)
try:
    if operation == 'setup':
        ack = adapter.poll_ack(request_id)
        assert ack['status'] == 'ACKED', ack
        swap = json.loads((ROOT/'crates/app/tests/fixtures/capture/saved_sell_446979602.json').read_text())['swap']
        mint = swap['token_in']
        swap.update(signature='independent-queued-buy', slot=8,
                    token_in='So11111111111111111111111111111111111111112', token_out=mint,
                    amount_in=.01, amount_out=.000012,
                    ts_utc=datetime.fromtimestamp(2_000_000_003., timezone.utc).isoformat(),
                    exact_amounts=dict(amount_in_raw='10000000', amount_out_raw='12',
                                       amount_in_decimals=9, amount_out_decimals=6))
        raw=b'independent synthetic DURABLE BUY fixture'
        fingerprint=hashlib.sha256(raw).hexdigest()
        epoch=control.db.execute('SELECT epoch FROM capture_meta').fetchone()[0]
        revision=control.db.execute("SELECT revision FROM capture_scope_revisions WHERE state='ACTIVE'").fetchone()[0]
        with control.atomic():
            control.db.execute('''INSERT INTO capture_events(signature,wallet,slot,epoch,request_id,
                received_at,source_at,raw,fingerprint,stage,event_json) VALUES(?,?,?,?,?,2000000003.,2000000003.,?,?, 'DURABLE',?)''',
                (swap['signature'],swap['wallet'],'8',epoch,request_id,raw,fingerprint,encoded(swap)))
            seq=control.db.execute('SELECT last_insert_rowid()').fetchone()[0]
            # Explicit storage fixture for an already-bound live receive. The
            # production decoder/raw identity is tested by the source E2E suite.
            provenance=encoded(dict(source_kind='stream',slot=8,sha256=fingerprint,
                                    available_at_micros=2000000003000000,block_time=None))
            control.db.execute('''INSERT INTO capture_recovery_evidence(evidence_id,scope_revision,
                source_kind,slot,raw,raw_hash,provenance_json) VALUES(?,?,'stream','8',?,?,?)''',
                (fingerprint,revision,raw,fingerprint,provenance))
            control.db.execute('''INSERT INTO capture_recovery(identity_key,signature,wallet,slot,
                scope_revision,event_json,info_json,provenance_json,observation_json,raw,raw_hash,evidence_id,live_seq)
                VALUES(?,?,?,'8',?,'null','{}',?,'{}',?,?,?,?)''',
                (swap['signature'],swap['signature'],swap['wallet'],revision,provenance,raw,fingerprint,fingerprint,seq))
            rid=control.db.execute('SELECT last_insert_rowid()').fetchone()[0]
            control.db.execute("INSERT INTO capture_recovery_links VALUES(?,?,'stream',?)",(rid,fingerprint,provenance))
        result=dict(status='QUEUED',capture_seq=seq,admission_was_current=True)
    else:
        seq=control.db.execute("SELECT seq FROM capture_events WHERE signature='independent-queued-buy'").fetchone()[0]
        result=adapter.observe(seq)
        result={k:v for k,v in result.items() if k!='ledger_event'}
    counts={table:ledger.db.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in ('events','lots','jobs')}
    result.update(ledger_counts=counts,
                  unsettled_pins=control.db.execute("SELECT count(*) FROM capture_obligations WHERE state!='SETTLED'").fetchone()[0],
                  network_calls=0, ledger_source=str(Path(virtual_engine.__file__).resolve()),
                  ledger_sha256=hashlib.sha256(Path(virtual_engine.__file__).read_bytes()).hexdigest())
    print(json.dumps(result,sort_keys=True))
finally:
    ledger.close()
    control.close()

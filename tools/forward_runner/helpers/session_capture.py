"""Linux-owned durable capture/ledger bridge; no provider or execution access."""
import json
import time
from capture_virtual.control import CaptureControl
from capture_virtual.adapter import CaptureVirtualAdapter
from virtual_engine import VirtualLedger

class CaptureSession:
    def __init__(self,capture_path,ledger_path,*,clock=time.time,max_protected_wallets=128):
        if type(max_protected_wallets) is not int or not 0<max_protected_wallets<=128:raise ValueError("invalid_protected_wallet_limit")
        self.max_protected_wallets=max_protected_wallets
        self.control=CaptureControl(capture_path)
        self.ledger=VirtualLedger(ledger_path)
        self.adapter=CaptureVirtualAdapter(self.control,self.ledger,clock=clock)
    def check_scope(self,proposed=()):
        wallets={r[0] for r in self.control.db.execute("SELECT wallet FROM capture_members m JOIN capture_requests r ON r.id=m.request_id WHERE r.state IN ('PENDING','ACKED') UNION SELECT wallet FROM capture_obligations WHERE state!='SETTLED'")}
        if len(wallets|set(proposed))>self.max_protected_wallets:
            raise ValueError('capture_protected_wallet_capacity')
    def publish(self,decision):
        with self.control.lock:
            self.check_scope(decision['wallets'] if decision['admissible'] else ())
            effective=self.adapter.publish_decision(decision)
        return {**effective,'decision':decision}
    def poll(self):
        # The request is the durable scheduler handoff; no volatile pending queue.
        with self.control.lock:
            self.check_scope()
            row=self.control.db.execute('SELECT id,payload FROM capture_requests ORDER BY id DESC LIMIT 1').fetchone()
        if not row:return {'status':'NO_DECISION'}
        return {**self.adapter.poll_ack(row['id']),'decision':json.loads(row['payload'])}
    def observe_once(self):
        seq=self.adapter.next_sequence()
        with self.control.lock:
            rejected=self.control.db.execute("SELECT count(*) FROM capture_events WHERE stage='REJECTED'").fetchone()[0]
            meta=dict(self.control.db.execute('SELECT epoch,gap,status,reason FROM capture_meta WHERE id=1').fetchone())
            row=self.control.db.execute('SELECT stage FROM capture_events WHERE seq=?',(seq,)).fetchone() if seq else None
        if not row:result={'status':'IDLE'}
        elif row['stage']=='RECEIVED':result={'status':'WAITING_RECEIVED','capture_seq':seq}
        else:result=self.adapter.observe(seq)
        return {**result,'rejected_receipts':rejected,'capture':meta}
    def summary(self):return self.ledger.summary()
    def export(self):return self.ledger.export()
    def stop_pending(self,reason):return self.ledger.stop_pending(reason)
    def close(self):
        self.ledger.close();self.control.close()

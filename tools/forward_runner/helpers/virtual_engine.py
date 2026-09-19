"""Causal membership snapshots and durable virtual signal/lot jobs; network-free."""
from datetime import datetime
from fractions import Fraction
from virtual_store import Store,encoded,decoded,now
from virtual_proof import WSOL


def epoch(value):
    return float(value) if isinstance(value,(int,float)) else datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()

class VirtualLedger(Store):
    def publish_decision(self,decision):
        d=dict(decision);identifier=str(d.get('decision_id',d.get('id')))
        proposed=epoch(d['available_at']);expires=epoch(d['valid_until']);wallets=d.get('wallets',[])
        if not isinstance(d.get('admissible'),bool) or not isinstance(wallets,list) or not all(isinstance(w,str) for w in wallets):raise ValueError('invalid_decision_types')
        # The first transaction durably saves the decision while it is unavailable.
        # Keep the same process lock across both commits so no observer sees the gap.
        with self.lock:
            with self.atomic():
                old=self.db.execute('SELECT available,payload FROM decisions WHERE id=?',(identifier,)).fetchone()
                if old:
                    if decoded(old['payload'])!=d:raise ValueError('decision_id_reused_with_different_payload')
                    if old['available']<1e99:return {'decision_id':identifier,'available_at':old['available'],'valid_until':expires}
                else:self.db.execute('INSERT INTO decisions VALUES(?,?,?,?,?,?)',(identifier,1e100,expires,bool(d['admissible']),encoded(wallets),encoded(d)))
            available=max(proposed,now())
            with self.atomic():self.db.execute('UPDATE decisions SET available=? WHERE id=?',(available,identifier))
            return {'decision_id':identifier,'available_at':available,'valid_until':expires}
    def observe(self,event,detected_at,cursor=None):
        e=dict(event);detected=epoch(detected_at)
        for key in ('signature','wallet','mint','side','slot'):
            if key not in e:raise ValueError('event_missing_'+key)
        if e['side'] not in ('BUY','SELL'):raise ValueError('unsupported_event_side')
        key='|'.join(str(e[k]) for k in ('signature','wallet','mint','side'))
        with self.atomic():
            existing=self.db.execute('SELECT id FROM events WHERE key=?',(key,)).fetchone()
            if existing:
                original=decoded(self.event(existing[0])['payload'])
                if original['slot']!=e['slot'] or (original.get('token_qty_raw') is not None and e.get('token_qty_raw') is not None and str(original['token_qty_raw'])!=str(e['token_qty_raw'])):
                    raise ValueError('duplicate_event_integrity_conflict')
                if cursor is not None:self.advance_cursor(cursor)
                return self.event(existing[0])
            d=self.db.execute('SELECT * FROM decisions WHERE available<=? ORDER BY available DESC,id DESC LIMIT 1',(detected,)).fetchone()
            self.db.execute('INSERT INTO events(key,signature,wallet,mint,side,detected,decision_id,status,payload) VALUES(?,?,?,?,?,?,?,?,?)',(key,e['signature'],e['wallet'],e['mint'],e['side'],detected,d['id'] if d else None,'DETECTED',encoded(e)))
            event_id=self.db.execute('SELECT last_insert_rowid()').fetchone()[0]
            binding={**e,'event_id':event_id,'decision_id':d['id'] if d else None,'detected_at':detected}
            if e['side']=='BUY':
                reason=('admissions_stopped' if self.meta('admissions_stopped') else 'no_available_decision' if not d else 'decision_stale_or_red' if not d['admissible'] or d['expires']<detected else 'wallet_not_member' if e['wallet'] not in decoded(d['wallets']) else None)
                if reason:self.set_event(event_id,'SKIPPED',reason)
                else:
                    self.db.execute('INSERT INTO lots(event_id,wallet,mint) VALUES(?,?,?)',(event_id,e['wallet'],e['mint']))
                    self.put_job(event_id,'BUY_QUOTE',{**binding,'inputMint':WSOL,'outputMint':e['mint'],'amount':'10000000','slippageBps':150})
                    self.put_job(event_id,'BUY_PROOF',binding);self.set_event(event_id,'BUY_PENDING')
            else:
                active=self.db.execute("SELECT count(*) FROM lots WHERE wallet=? AND mint=? AND (quote_status='PENDING' OR remaining_raw!='0')",(e['wallet'],e['mint'])).fetchone()[0]
                if active:self.put_job(event_id,'SELL_PROOF',binding);self.set_event(event_id,'SELL_PENDING')
                else:self.set_event(event_id,'SKIPPED','no_virtual_lot')
            if cursor is not None:self.advance_cursor(cursor)
            self.exposure();return self.event(event_id)
    def apply_quote(self,job_id,outcome):
        from virtual_apply import apply_quote
        return apply_quote(self,job_id,outcome)
    def apply_transaction(self,job_id,body,evidence=None):
        from virtual_apply import apply_transaction
        return apply_transaction(self,job_id,body,evidence)
    def apply_history(self,job_id,context):
        from virtual_apply import apply_history
        return apply_history(self,job_id,context)
    def apply_inventory(self,job_id,context):
        from virtual_apply import apply_inventory
        return apply_inventory(self,job_id,context)
    def apply_failure(self,job_id,reason):
        from virtual_apply import apply_failure
        return apply_failure(self,job_id,reason)
    def progress_sells(self):
        from virtual_apply import progress_sells
        with self.atomic():progress_sells(self)
    def summary(self):
        from virtual_report import summary
        with self.lock:return summary(self)
    def export(self):
        with self.lock:
            return {name:[dict(r) for r in self.db.execute('SELECT * FROM '+name)] for name in ('decisions','events','lots','jobs','proofs','allocations','metadata')}

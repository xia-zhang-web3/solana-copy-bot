"""Causal bridge. A committed consumer ACK precedes every virtual admission."""

import json
from pathlib import Path
import time
import uuid

from .control import encoded, epoch

WSOL = "So11111111111111111111111111111111111111112"
IDENTITY_KEY = "capture_identity"


class CaptureVirtualAdapter:
    def __init__(self, control, ledger, *, clock=time.time):
        self.control, self.ledger, self.clock = control, ledger, clock
        self._bind_ledger()

    def _bind_ledger(self):
        path = str(Path(self.ledger.path).resolve(strict=True))
        if path == str(self.control.path):
            raise ValueError("capture_and_ledger_must_be_separate")
        with self.control.atomic():
            binding = self.control.db.execute("SELECT * FROM capture_binding WHERE id=1").fetchone()
            with self.ledger.atomic():
                identity = self.ledger.meta(IDENTITY_KEY)
                if binding:
                    if binding["ledger_path"] != path or binding["ledger_identity"] != identity:
                        raise ValueError("capture_ledger_identity_mismatch")
                else:
                    occupied = any(self.ledger.db.execute("SELECT EXISTS(SELECT 1 FROM " + table + ")")
                                   .fetchone()[0] for table in ("decisions", "events", "lots", "jobs"))
                    if occupied:
                        raise ValueError("initial_capture_binding_requires_empty_owned_ledger")
                    identity = identity or str(uuid.uuid4())
                    self.ledger.set_meta(IDENTITY_KEY, identity)
                    self.control.db.execute("INSERT INTO capture_binding VALUES(1,?,?)", (path, identity))

    def publish_decision(self, decision):
        request_id = self.control.request(decision)
        return {"request_id": request_id, "status": "PENDING_CONSUMER_ACK"}

    def poll_ack(self, request_id):
        """Writing a request is not ACK; publication occurs under the consumer fence."""
        with self.control.atomic():
            row = self.control.db.execute("SELECT * FROM capture_requests WHERE id=?",
                                          (request_id,)).fetchone()
            if not row:
                raise ValueError("unknown_capture_request")
            meta = self.control.db.execute("SELECT * FROM capture_meta WHERE id=1").fetchone()
            if not self._valid_ack(row, meta):
                return {"request_id": request_id, "status": "PENDING_CONSUMER_ACK"}
            if epoch(self.clock()) > row["expires"]:
                return {"request_id": request_id, "status": "EXPIRED"}
            if row["available_seq"] is None:
                decision = json.loads(row["payload"])
                published = self.ledger.publish_decision(decision)
                available = max(epoch(self.clock()), epoch(published["available_at"]),
                                epoch(row["ack_at"]), epoch(decision["available_at"]))
                seq = self.control.db.execute("SELECT coalesce(max(seq),0) FROM capture_events").fetchone()[0]
                self.control.db.execute("UPDATE capture_requests SET available_seq=?,available_at=? WHERE id=?",
                                        (max(seq, row["ack_seq"]), available, request_id))
            row = self.control.db.execute("SELECT * FROM capture_requests WHERE id=?", (request_id,)).fetchone()
            return {"request_id": request_id, "status": "ACKED", "available_seq": row["available_seq"],
                    "available_at": row["available_at"], "epoch": row["epoch"], "gap": row["gap"]}

    @staticmethod
    def _valid_ack(row, meta):
        return (row["state"] == "ACKED" and row["epoch"] == meta["epoch"]
                and row["gap"] == meta["gap"] and meta["status"] == "running"
                and row["ack_seq"] is not None and row["ack_at"] is not None)

    def _canonical(self, seq):
        if type(seq) is not int or seq <= 0:
            raise ValueError("capture_seq_required")
        row = self.control.db.execute("SELECT * FROM capture_events WHERE seq=?", (seq,)).fetchone()
        if row is None or row["stage"] != "DURABLE" or not row["event_json"]:
            raise ValueError("capture_event_not_durable")
        swap = json.loads(row["event_json"])
        if (swap["signature"] != row["signature"] or swap["wallet"] != row["wallet"]
                or str(swap["slot"]) != row["slot"] or row["source_at"] is None
                or abs(epoch(swap["ts_utc"]) - epoch(row["source_at"])) > 0.000001):
            raise ValueError("capture_canonical_binding_mismatch")
        side = ("BUY" if swap["token_in"] == WSOL and swap["token_out"] != WSOL else
                "SELL" if swap["token_out"] == WSOL and swap["token_in"] != WSOL else None)
        if side is None:
            raise ValueError("capture_pair_not_sol")
        mint = swap["token_out"] if side == "BUY" else swap["token_in"]
        exact = swap.get("exact_amounts") or {}
        qty = exact.get("amount_out_raw" if side == "BUY" else "amount_in_raw")
        event = {"signature": swap["signature"], "wallet": swap["wallet"], "mint": mint,
                 "side": side, "slot": swap["slot"], "source_ts": swap["ts_utc"],
                 "source_ts_kind": "provider_created_at_not_chain_execution_time",
                 "capture_seq": seq, "capture_epoch": row["epoch"],
                 "token_qty_raw": str(qty) if qty is not None else None}
        return row, event

    def _buy_refusal(self, row, event):
        request = self.control.db.execute("SELECT * FROM capture_requests WHERE id=?",
                                          (row["request_id"],)).fetchone()
        meta = self.control.db.execute("SELECT * FROM capture_meta WHERE id=1").fetchone()
        if not request or not self._valid_ack(request, meta) or row["epoch"] != meta["epoch"]:
            return "capture_ack_not_current"
        if request["available_seq"] is None or request["available_at"] is None:
            return "capture_admission_not_published"
        if (row["seq"] <= request["available_seq"] or row["received_at"] < request["available_at"]
                or row["source_at"] < request["available_at"]):
            return "capture_pre_admission_event"
        if max(epoch(self.clock()), row["received_at"], row["source_at"]) > request["expires"]:
            return "capture_decision_expired"
        member = self.control.db.execute("SELECT EXISTS(SELECT 1 FROM capture_members WHERE request_id=? AND wallet=?)",
                                         (request["id"], event["wallet"])).fetchone()[0]
        if not member or not json.loads(request["payload"])["admissible"]:
            return "capture_wallet_not_member"
        return None

    def _skip(self, seq, reason):
        self.control.db.execute("INSERT INTO capture_delivery(event_seq,state,reason) VALUES(?,'SKIPPED',?) "
                                "ON CONFLICT(event_seq) DO UPDATE SET state='SKIPPED',reason=excluded.reason",
                                (seq, reason))
        return {"capture_seq": seq, "status": "SKIPPED", "reason": reason}

    def _existing_ledger_event(self, event):
        key = "|".join(str(event[k]) for k in ("signature", "wallet", "mint", "side"))
        with self.ledger.lock:
            found = self.ledger.db.execute("SELECT * FROM events WHERE key=?", (key,)).fetchone()
            if found:
                prior = json.loads(found["payload"])
                if any(prior.get(k) != event[k] for k in event):
                    raise ValueError("capture_ledger_event_integrity_conflict")
                return dict(found)
        return None

    def _record_delivery(self, seq, event, delivered):
        self.control.db.execute("UPDATE capture_delivery SET state='DONE',reason=NULL,ledger_event_id=? WHERE event_seq=?",
                                (delivered["id"], seq))
        if event["side"] == "BUY":
            self.control.db.execute("UPDATE capture_obligations SET ledger_event_id=? WHERE event_seq=?",
                                    (delivered["id"], seq))
        return {"capture_seq": seq, "status": "DELIVERED", "ledger_event": delivered}

    def next_sequence(self):
        """Earliest unfinished receive/durable row; RECEIVED waits for its decoder."""
        with self.control.lock:
            row = self.control.db.execute("""SELECT e.seq FROM capture_events e
                LEFT JOIN capture_delivery d ON d.event_seq=e.seq
                WHERE e.stage IN ('RECEIVED','DURABLE')
                AND coalesce(d.state,'PENDING') NOT IN ('DONE','SKIPPED')
                ORDER BY e.seq LIMIT 1""").fetchone()
            return row[0] if row else None

    def observe(self, seq):
        """Only durable canonical rows enter the ledger; pins commit before BUY."""
        with self.control.atomic():
            row, event = self._canonical(seq)
            delivery = self.control.db.execute("SELECT * FROM capture_delivery WHERE event_seq=?", (seq,)).fetchone()
            if delivery and delivery["state"] == "SKIPPED":
                return {"capture_seq": seq, "status": "SKIPPED", "reason": delivery["reason"]}
            if (not delivery or delivery["state"] != "DONE") and self.next_sequence() != seq:
                raise ValueError("capture_prior_event_unfinished")
            if delivery:
                existing = self._existing_ledger_event(event)
                if existing:
                    return self._record_delivery(seq, event, existing)
                if delivery["state"] == "DONE":
                    raise ValueError("capture_committed_ledger_event_missing")
            if event["side"] == "BUY":
                refusal = self._buy_refusal(row, event)
                if refusal:
                    return self._skip(seq, refusal)
                self.control.db.execute("INSERT OR IGNORE INTO capture_obligations(event_seq,wallet,mint) VALUES(?,?,?)",
                                        (seq, event["wallet"], event["mint"]))
            else:
                pin = self.control.db.execute("SELECT EXISTS(SELECT 1 FROM capture_obligations WHERE wallet=? AND mint=? AND state!='SETTLED')",
                                               (event["wallet"], event["mint"])).fetchone()[0]
                if not pin:
                    return self._skip(seq, "capture_no_persisted_lot_obligation")
            self.control.db.execute("INSERT OR IGNORE INTO capture_delivery(event_seq,state) VALUES(?,'PENDING')", (seq,))
        # This separate commit is the pin-before-lot boundary. If ledger.observe
        # fails or the process dies, the pin survives and protection stays active.
        with self.control.atomic():
            row, event = self._canonical(seq)
            existing = self._existing_ledger_event(event)
            if existing:
                return self._record_delivery(seq, event, existing)
            if event["side"] == "BUY":
                refusal = self._buy_refusal(row, event)
                if refusal:
                    return self._skip(seq, refusal)
            delivered = self.ledger.observe(event, row["received_at"])
            return self._record_delivery(seq, event, delivered)

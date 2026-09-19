"""Real saved VirtualLedger; modeled consumer ACKs only, no daemon or network."""

import json
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
HELPERS = ROOT / "tools/tests/fixtures/capture_ledger"
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(HELPERS))
from capture_virtual import CaptureControl, CaptureVirtualAdapter
from capture_virtual.adapter import WSOL
import virtual_engine

SAVED = json.loads((ROOT / "crates/app/tests/fixtures/capture/saved_sell_446979602.json").read_text())
WALLET = SAVED["swap"]["wallet"]
MINT = SAVED["swap"]["token_in"]


class CaptureVirtualTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="capture-virtual-test-")
        self.dir = Path(self.tmp.name)
        self.now = 2_000_000_000.0
        self.clock_patch = patch.object(virtual_engine, "now", lambda: self.now)
        self.clock_patch.start()
        self.control = CaptureControl.create(self.dir / "capture.db", max_rows=100, max_bytes=1_048_576)
        self.ledger = virtual_engine.VirtualLedger(self.dir / "virtual.db")
        self.adapter = CaptureVirtualAdapter(self.control, self.ledger, clock=lambda: self.now)
        self.model_consumer_start()

    def tearDown(self):
        self.ledger.close()
        self.control.close()
        self.clock_patch.stop()
        self.tmp.cleanup()

    def model_consumer_start(self):
        # SQL is a test model, not evidence that the real Rust consumer started.
        self.control.db.execute("UPDATE capture_meta SET epoch=epoch+1,gap=gap+1,status='running' WHERE id=1")

    def decision(self, key="one", wallets=None, admissible=True):
        return {"decision_id": key, "available_at": self.now, "valid_until": self.now + 120,
                "wallets": [WALLET] if wallets is None else wallets, "admissible": admissible}

    def model_ack(self, request_id):
        with self.control.atomic():
            self.control.db.execute("UPDATE capture_requests SET state='DEMOTED' WHERE state='ACKED'")
            self.control.db.execute("""UPDATE capture_requests SET state='ACKED',
                epoch=(SELECT epoch FROM capture_meta),gap=(SELECT gap FROM capture_meta),
                ack_seq=(SELECT coalesce(max(seq),0) FROM capture_events),ack_at=? WHERE id=?""",
                                    (self.now, request_id))

    def admit(self, key="one", wallets=None):
        request = self.adapter.publish_decision(self.decision(key, wallets))["request_id"]
        self.model_ack(request)
        result = self.adapter.poll_ack(request)
        self.assertEqual(result["status"], "ACKED")
        return request

    def captured(self, request_id, *, side="BUY", source=None, received=None, stage="DURABLE", mint=MINT):
        self.now += 1
        source = self.now if source is None else source
        received = self.now if received is None else received
        seq = self.control.db.execute("SELECT coalesce(max(seq),0)+1 FROM capture_events").fetchone()[0]
        swap = dict(SAVED["swap"], signature=f"saved-wallet-test-{seq}",
                    token_in=WSOL if side == "BUY" else mint,
                    token_out=mint if side == "BUY" else WSOL,
                    ts_utc=source, exact_amounts={"amount_in_raw": "10000000", "amount_in_decimals": 9,
                                               "amount_out_raw": "1144131279", "amount_out_decimals": 6})
        epoch = self.control.db.execute("SELECT epoch FROM capture_meta").fetchone()[0]
        self.control.db.execute("""INSERT INTO capture_events
            (seq,signature,wallet,slot,epoch,request_id,received_at,source_at,raw,fingerprint,stage,event_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                                (seq, swap["signature"], WALLET, str(swap["slot"]), epoch, request_id,
                                 received, source, b"modeled envelope", f"modeled-{seq}", stage, json.dumps(swap)))
        return seq

    def count(self, table):
        return self.ledger.db.execute("SELECT count(*) FROM " + table).fetchone()[0]

    def test_publication_is_pending_and_pre_ack_event_never_admitted(self):
        request = self.adapter.publish_decision(self.decision())["request_id"]
        self.assertEqual(self.adapter.poll_ack(request)["status"], "PENDING_CONSUMER_ACK")
        self.assertEqual(self.count("decisions"), 0)
        before_ack = self.captured(request)
        self.assertEqual(self.adapter.observe(before_ack)["status"], "SKIPPED")
        self.model_ack(request)
        ack = self.adapter.poll_ack(request)
        self.assertGreaterEqual(ack["available_seq"], before_ack)
        self.assertEqual(self.adapter.observe(before_ack)["status"], "SKIPPED")
        self.assertEqual(self.count("lots"), 0)

    def test_event_between_ack_and_publication_is_excluded_by_sequence(self):
        request = self.adapter.publish_decision(self.decision())["request_id"]
        self.model_ack(request)
        between = self.captured(request)
        self.assertEqual(self.adapter.poll_ack(request)["available_seq"], between)
        self.assertEqual(self.adapter.observe(between)["reason"], "capture_pre_admission_event")
        after = self.captured(request)
        self.assertEqual(self.adapter.observe(after)["ledger_event"]["status"], "BUY_PENDING")
        self.assertEqual(self.count("lots"), 1)
        self.assertEqual(self.count("jobs"), 2)

    def test_pin_commit_precedes_real_virtual_buy_and_duplicate_is_idempotent(self):
        request = self.admit()
        seq = self.captured(request)
        original = self.ledger.observe

        def checked(event, detected):
            independent = sqlite3.connect(self.control.path)
            try:
                self.assertEqual(independent.execute("SELECT wallet,mint FROM capture_obligations WHERE event_seq=?",
                                                     (seq,)).fetchone(), (WALLET, MINT))
                self.assertEqual(self.count("lots"), 0)
            finally:
                independent.close()
            return original(event, detected)

        with patch.object(self.ledger, "observe", checked):
            first = self.adapter.observe(seq)
        second = self.adapter.observe(seq)
        self.assertEqual(first["ledger_event"]["id"], second["ledger_event"]["id"])
        self.assertEqual(self.count("lots"), 1)
        self.assertEqual(self.count("events"), 1)
        self.assertEqual(self.count("jobs"), 2)
        payload = json.loads(first["ledger_event"]["payload"])
        self.assertEqual(payload["token_qty_raw"], "1144131279")

    def test_real_lot_and_capture_pin_survive_ledger_reply_loss(self):
        request = self.admit()
        seq = self.captured(request)
        original = self.ledger.observe

        def lost_reply(event, detected):
            original(event, detected)
            raise RuntimeError("reply_lost_after_real_ledger_commit")

        with patch.object(self.ledger, "observe", lost_reply):
            with self.assertRaisesRegex(RuntimeError, "reply_lost"):
                self.adapter.observe(seq)
        self.assertEqual(self.count("lots"), 1)
        self.assertEqual(self.control.db.execute("SELECT state FROM capture_delivery").fetchone()[0], "PENDING")
        self.model_consumer_start()
        self.adapter = CaptureVirtualAdapter(self.control, self.ledger, clock=lambda: self.now)
        recovered = self.adapter.observe(seq)
        self.assertEqual(recovered["status"], "DELIVERED")
        self.assertEqual(self.count("lots"), 1)
        self.assertEqual(self.control.db.execute("SELECT ledger_event_id FROM capture_obligations").fetchone()[0],
                         recovered["ledger_event"]["id"])

    def test_pin_survives_before_ledger_failure_and_restart_blocks_late_buy(self):
        request = self.admit()
        seq = self.captured(request)
        with patch.object(self.ledger, "observe", side_effect=RuntimeError("before_ledger_commit")):
            with self.assertRaisesRegex(RuntimeError, "before_ledger_commit"):
                self.adapter.observe(seq)
        self.assertEqual(self.count("lots"), 0)
        self.model_consumer_start()
        self.assertEqual(self.adapter.observe(seq)["reason"], "capture_ack_not_current")
        self.assertEqual(self.control.db.execute("SELECT count(*) FROM capture_obligations WHERE state!='SETTLED'")
                         .fetchone()[0], 1)

    def test_demotion_and_restart_do_not_forget_sell_obligation(self):
        request = self.admit()
        self.adapter.observe(self.captured(request))
        demotion = self.admit("demotion", wallets=[])
        self.assertNotEqual(demotion, request)
        self.model_consumer_start()
        self.ledger.close()
        self.control.close()
        self.ledger = virtual_engine.VirtualLedger(self.dir / "virtual.db")
        self.control = CaptureControl(self.dir / "capture.db")
        self.adapter = CaptureVirtualAdapter(self.control, self.ledger, clock=lambda: self.now)
        sell = self.captured(None, side="SELL")
        result = self.adapter.observe(sell)
        self.assertEqual(result["ledger_event"]["status"], "SELL_PENDING")
        self.assertEqual(self.adapter.observe(sell)["ledger_event"]["id"], result["ledger_event"]["id"])
        self.assertEqual(self.count("jobs"), 3)
        self.assertEqual(self.control.db.execute("SELECT count(*) FROM capture_obligations").fetchone()[0], 1)
        new_buy = self.captured(demotion)
        self.assertEqual(self.adapter.observe(new_buy)["reason"], "capture_ack_not_current")

    def test_demotion_before_reserve_permanently_skips_undelivered_buy(self):
        request = self.admit()
        old_buy = self.captured(request)
        self.admit("demoted-before-reserve", wallets=[])
        self.assertEqual(self.adapter.observe(old_buy)["reason"], "capture_ack_not_current")
        self.assertEqual(self.control.db.execute("SELECT count(*) FROM capture_obligations").fetchone()[0], 0)
        self.admit("readmitted-later")
        self.assertEqual(self.adapter.observe(old_buy)["status"], "SKIPPED")
        self.assertEqual(self.count("lots"), 0)

    def test_reserve_before_demotion_commits_pin_before_losing_membership(self):
        request = self.admit()
        old_buy = self.captured(request)
        new_request = self.adapter.publish_decision(self.decision("demote-after-reserve", wallets=[]))["request_id"]
        atomic = self.control.atomic
        commits = []

        @contextmanager
        def demote_after_pin_commit():
            with atomic():
                yield
            commits.append(True)
            if len(commits) == 1:
                independent = sqlite3.connect(self.control.path)
                try:
                    self.assertEqual(independent.execute("SELECT count(*) FROM capture_obligations WHERE event_seq=?",
                                                         (old_buy,)).fetchone()[0], 1)
                    self.assertEqual(self.count("lots"), 0)
                finally:
                    independent.close()
                # The consumer can now demote; the committed pin already covers
                # future SELLs even if virtual lot creation is interrupted.
                self.model_ack(new_request)

        with patch.object(self.control, "atomic", demote_after_pin_commit):
            self.assertEqual(self.adapter.observe(old_buy)["reason"], "capture_ack_not_current")
        self.assertEqual(self.control.db.execute("SELECT count(*) FROM capture_obligations WHERE state!='SETTLED'")
                         .fetchone()[0], 1)
        self.assertEqual(self.count("lots"), 0)

    def test_membership_covers_every_mint_and_unpinned_sell_is_skipped(self):
        request = self.admit()
        other_mint = "11111111111111111111111111111111"
        self.assertEqual(self.adapter.observe(self.captured(request, mint=other_mint))["status"], "DELIVERED")
        unpinned = self.captured(request, side="SELL", mint=MINT)
        self.assertEqual(self.adapter.observe(unpinned)["reason"], "capture_no_persisted_lot_obligation")

    def test_later_sell_cannot_skip_before_earlier_buy_creates_pin(self):
        request = self.admit()
        buy = self.captured(request)
        sell = self.captured(request, side="SELL")
        self.assertEqual(self.adapter.next_sequence(), buy)
        with self.assertRaisesRegex(ValueError, "prior_event_unfinished"):
            self.adapter.observe(sell)
        self.assertEqual(self.count("events"), 0)
        self.assertEqual(self.control.db.execute("SELECT count(*) FROM capture_delivery WHERE event_seq=?",
                                                 (sell,)).fetchone()[0], 0)
        self.assertEqual(self.adapter.observe(buy)["ledger_event"]["status"], "BUY_PENDING")
        self.assertEqual(self.adapter.next_sequence(), sell)
        self.assertEqual(self.adapter.observe(sell)["ledger_event"]["status"], "SELL_PENDING")
        self.assertIsNone(self.adapter.next_sequence())
        self.assertEqual(self.adapter.observe(buy)["status"], "DELIVERED")

    def test_unfinished_received_row_blocks_later_delivery_until_decoded_or_rejected(self):
        request = self.admit()
        receiving = self.captured(request, stage="RECEIVED")
        later = self.captured(request)
        self.assertEqual(self.adapter.next_sequence(), receiving)
        with self.assertRaisesRegex(ValueError, "prior_event_unfinished"):
            self.adapter.observe(later)
        self.control.db.execute("UPDATE capture_events SET stage='REJECTED',event_json=NULL WHERE seq=?",
                                (receiving,))
        self.assertEqual(self.adapter.next_sequence(), later)
        self.assertEqual(self.adapter.observe(later)["status"], "DELIVERED")
        self.assertIsNone(self.adapter.next_sequence())

    def test_source_and_received_times_must_both_follow_admission(self):
        request = self.admit()
        available = self.adapter.poll_ack(request)["available_at"]
        for times in ({"source": available - 1}, {"received": available - 1}):
            with self.subTest(times=times):
                seq = self.captured(request, **times)
                self.assertEqual(self.adapter.observe(seq)["reason"], "capture_pre_admission_event")
        self.assertEqual(self.count("lots"), 0)

    def test_gap_failure_and_expiry_block_new_buy_without_false_coverage(self):
        request = self.admit()
        seq = self.captured(request)
        self.control.db.execute("UPDATE capture_meta SET gap=gap+1,reason='upstream_reconnect'")
        self.assertEqual(self.adapter.observe(seq)["reason"], "capture_ack_not_current")
        self.assertEqual(self.adapter.poll_ack(request)["status"], "PENDING_CONSUMER_ACK")
        request = self.admit("after-gap")
        seq = self.captured(request)
        self.control.db.execute("UPDATE capture_meta SET status='failed',reason='capture_capacity'")
        self.assertEqual(self.adapter.observe(seq)["reason"], "capture_ack_not_current")
        self.assertEqual(self.control.db.execute("SELECT reason FROM capture_meta").fetchone()[0], "capture_capacity")
        self.model_consumer_start()
        request = self.admit("after-restart")
        seq = self.captured(request)
        self.now += 121
        self.assertEqual(self.adapter.observe(seq)["reason"], "capture_decision_expired")
        self.assertEqual(self.count("lots"), 0)

    def test_only_durable_canonical_event_can_enter_adapter(self):
        request = self.admit()
        for bad in ({"signature": "arbitrary"}, 0, True, "1", 999):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                self.adapter.observe(bad)
        for stage in ("RECEIVED", "REJECTED"):
            seq = self.captured(request, stage=stage)
            with self.assertRaisesRegex(ValueError, "not_durable"):
                self.adapter.observe(seq)
        seq = self.captured(request)
        self.control.db.execute("UPDATE capture_events SET wallet='tampered' WHERE seq=?", (seq,))
        with self.assertRaisesRegex(ValueError, "binding_mismatch"):
            self.adapter.observe(seq)
        self.assertEqual(self.count("lots"), 0)

    def test_crash_after_actual_ledger_publication_never_backdates_admission(self):
        request = self.adapter.publish_decision(self.decision())["request_id"]
        self.model_ack(request)
        original = self.ledger.publish_decision

        def lost_reply(decision):
            original(decision)
            raise RuntimeError("publication_reply_lost")

        with patch.object(self.ledger, "publish_decision", lost_reply):
            with self.assertRaisesRegex(RuntimeError, "reply_lost"):
                self.adapter.poll_ack(request)
        between = self.captured(request)
        self.now += 2
        ack = self.adapter.poll_ack(request)
        self.assertEqual(ack["available_at"], self.now)
        self.assertEqual(ack["available_seq"], between)
        self.assertEqual(self.adapter.observe(between)["status"], "SKIPPED")
        self.assertEqual(self.count("decisions"), 1)

    def test_request_immutability_and_row_and_byte_bounds(self):
        decision = self.decision()
        first = self.control.request(decision)
        used = self.control.db.execute("SELECT used_bytes FROM capture_meta").fetchone()[0]
        self.assertEqual(self.control.request(decision), first)
        self.assertEqual(self.control.db.execute("SELECT used_bytes FROM capture_meta").fetchone()[0], used)
        with self.assertRaisesRegex(ValueError, "reused"):
            self.control.request(dict(decision, admissible=False))
        self.control.db.execute("UPDATE capture_meta SET max_rows=1")
        with self.assertRaisesRegex(ValueError, "request_capacity"):
            self.control.request(self.decision("two"))
        self.control.db.execute("UPDATE capture_meta SET max_rows=100,max_bytes=used_bytes")
        with self.assertRaisesRegex(ValueError, "request_capacity"):
            self.control.request(self.decision("three"))
        self.assertEqual(self.control.db.execute("SELECT gap FROM capture_meta").fetchone()[0], 1)

    def test_controller_does_not_overwrite_or_migrate_existing_databases(self):
        with self.assertRaises(FileExistsError):
            CaptureControl.create(self.dir / "capture.db", max_rows=1, max_bytes=4096)
        with self.assertRaisesRegex(ValueError, "schema_missing"):
            CaptureControl(self.dir / "virtual.db")
        for rows, bytes_ in ((0, 1), (1_000_001, 1), (1, 1_073_741_825), (True, 1)):
            with self.subTest(bounds=(rows, bytes_)), self.assertRaises(ValueError):
                CaptureControl.create(self.dir / "invalid.db", max_rows=rows, max_bytes=bytes_)
        self.assertFalse((self.dir / "invalid.db").exists())

    def test_binding_rejects_changed_ledger_identity_at_same_path(self):
        with self.ledger.atomic():
            self.ledger.set_meta("capture_identity", "another-database")
        with self.assertRaisesRegex(ValueError, "identity_mismatch"):
            CaptureVirtualAdapter(self.control, self.ledger)

    def test_nonempty_ledger_cannot_be_adopted_without_prior_capture_identity(self):
        other_control = CaptureControl.create(self.dir / "other-capture.db", max_rows=100, max_bytes=1_048_576)
        other_ledger = virtual_engine.VirtualLedger(self.dir / "old-virtual.db")
        try:
            other_ledger.publish_decision(self.decision())
            with self.assertRaisesRegex(ValueError, "requires_empty_owned_ledger"):
                CaptureVirtualAdapter(other_control, other_ledger)
        finally:
            other_control.close()
            other_ledger.close()

    def test_malformed_and_unbounded_requests_are_rejected(self):
        for change in ({"wallets": [WALLET] * 129}, {"wallets": [WALLET, WALLET]},
                       {"wallets": [[WALLET]]}, {"admissible": 1}, {"valid_until": float("inf")},
                       {"available_at": "2026-01-01T00:00:00"}, {"decision_id": ""}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.control.request(dict(self.decision(), **change))


if __name__ == "__main__":
    unittest.main()

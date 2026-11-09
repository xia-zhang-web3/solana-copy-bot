"""Network-free real adapter/ledger tests; receipt SQL is an explicit storage fixture."""
import hashlib
import json
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "tools"), str(ROOT / "tools/tests/fixtures/capture_ledger")]
from capture_virtual import CaptureControl, CaptureVirtualAdapter
from capture_virtual.control import encoded
from virtual_engine import VirtualLedger

SAVED = json.loads((ROOT / "crates/app/tests/fixtures/capture/saved_sell_446979602.json").read_text())["swap"]
WALLET, MINT = SAVED["wallet"], SAVED["token_in"]
WSOL = "So11111111111111111111111111111111111111112"


class CommitFault:
    def __init__(self, db, *, after):
        self.db, self.after, self.once = db, after, True

    def __getattr__(self, key):
        return getattr(self.db, key)

    def execute(self, sql, *args):
        if sql == "COMMIT" and self.once:
            self.once = False
            if self.after:
                self.db.execute(sql, *args)
            raise RuntimeError("lost_commit_reply" if self.after else "before_commit")
        return self.db.execute(sql, *args)


class ReadbackFault(CommitFault):
    def execute(self, sql, *args):
        if sql.startswith("SELECT state,reason,disposition_json"):
            raise RuntimeError("commit_readback_unavailable")
        return self.db.execute(sql, *args)


class RecoveryForwardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="recovery-forward-")
        self.directory = Path(self.temp.name)
        self.control = CaptureControl.create(self.directory / "capture.db", max_rows=100, max_bytes=16_777_216)
        self.ledger = VirtualLedger(self.directory / "virtual.db")
        self.adapter = CaptureVirtualAdapter(self.control, self.ledger)
        self.control.db.execute("UPDATE capture_meta SET epoch=1,status='running' WHERE id=1")
        self.control.db.execute("INSERT INTO capture_scope_revisions(revision,epoch,wallets_json,state) VALUES(1,1,?,'ACTIVE')", (encoded([WALLET]),))

    def tearDown(self):
        self.control.close()
        self.ledger.close()
        self.temp.cleanup()

    def snapshot(self):
        return list(self.ledger.db.iterdump())

    def recovered(self, side="SELL", *, signature="recovered-fixture-signature", kind="complete_live_block", facts=True):
        checked = dict(signature=signature, wallet=WALLET, slot=446979602,
                       token_in=MINT if side == "SELL" else WSOL,
                       token_out=WSOL if side == "SELL" else MINT,
                       amount_in_bits=0, amount_out_bits=0,
                       exact_amounts=dict(amount_in_raw="12", amount_out_raw="34",
                                          amount_in_decimals=6, amount_out_decimals=9),
                       programs=[], dex="test-fixture", program_fallback=False) if facts else None
        raw = b"explicit synthetic complete-block wrapper fixture"
        sha = hashlib.sha256(raw).hexdigest()
        evidence = dict(source_kind=kind, slot=446979602, sha256=sha,
                        available_at_micros=2_000_000_000_000_000, block_time=1_900_000_000)
        block = dict(blockhash="synthetic-block", parent_slot=446979601,
                     parent_blockhash="synthetic-parent", transaction_index=0,
                     executed_transaction_count=1, block_time=1_900_000_000)
        info = dict(encoded=list(b"synthetic-normalized-info"), float_bits=[])
        observation = dict(signature=signature, wallet=WALLET, slot=446979602,
                           facts=checked, info=info, info_hash="fixture-checked-identity",
                           message_time={"RecoveredBlock": {"block_time": 1_900_000_000}},
                           source_kind=kind, evidence_sha256=sha, block=block,
                           transaction_fingerprint="fixture-transaction-fingerprint")
        provenance = {**{k: v for k, v in observation.items() if k not in ("facts", "info")}, "evidence": evidence}
        self.control.db.execute("""INSERT OR IGNORE INTO capture_recovery_evidence
            (evidence_id,scope_revision,source_kind,slot,raw,raw_hash,provenance_json)
            VALUES(?,1,?,?,?,?,?)""", (sha, kind, "446979602", raw, sha, encoded(evidence)))
        info_raw = bytes(info["encoded"])
        row = self.control.db.execute("""INSERT INTO capture_recovery
            (identity_key,signature,wallet,slot,scope_revision,event_json,info_json,provenance_json,
             observation_json,raw,raw_hash,evidence_id) VALUES(?,?,?,?,1,?,?,?,?,?,?,?)""",
             (signature, signature, WALLET, "446979602", encoded(checked), encoded(info), encoded(provenance),
              encoded(observation), info_raw, hashlib.sha256(info_raw).hexdigest(), sha))
        recovery_id = row.lastrowid
        self.control.db.execute("INSERT INTO capture_recovery_outbox(recovery_id) VALUES(?)", (recovery_id,))
        return recovery_id

    def test_actual_observer_poll_drains_once_and_does_not_touch_financial_ledger(self):
        recovery_id = self.recovered()
        before = self.snapshot()
        self.assertIsNone(self.adapter.next_sequence())
        first = self.adapter.observe_recovery(recovery_id)
        self.assertEqual(first["status"], "MISSED_UNEXECUTED")
        self.assertEqual(first["reason"], "recovered_historical_sell_unexecuted")
        self.assertEqual(first["token_qty_raw"], "12")
        self.assertFalse(first["trading_admission"])
        self.assertEqual(first["timeliness"], "not_established")
        self.assertFalse({"source_ts", "source_at", "capture_epoch", "capture_seq", "ts_utc"} & first.keys())
        self.assertEqual(self.adapter.observe_recovery(recovery_id), first)
        self.assertIsNone(self.adapter.next_recovery())
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.control.db.execute("SELECT count(*) FROM capture_recovery_outbox WHERE state='MISSED'").fetchone()[0], 1)

    def test_buy_and_undecodable_evidence_never_create_lot_or_job(self):
        before = self.snapshot()
        buy = self.recovered("BUY", signature="buy")
        no_swap = self.recovered(signature="no-swap", facts=False)
        self.assertEqual(self.adapter.observe_recovery(buy)["reason"], "recovered_historical_buy_no_admission")
        self.assertEqual(self.adapter.observe_recovery(no_swap)["status"], "EVIDENCE_ONLY")
        self.assertEqual(self.control.db.execute("SELECT state FROM capture_recovery_outbox WHERE recovery_id=?", (no_swap,)).fetchone()[0], "REJECTED")
        self.assertIsNone(self.adapter.next_recovery())
        self.assertEqual(self.snapshot(), before)

    def test_before_commit_crash_leaves_pending_and_restart_finishes_once(self):
        recovery_id = self.recovered(kind="confirmed_http")
        before = self.snapshot()
        self.control.db = CommitFault(self.control.db, after=False)
        with self.assertRaisesRegex(RuntimeError, "before_commit"):
            self.adapter.observe_recovery(recovery_id)
        self.assertEqual(self.control.db.execute("SELECT state FROM capture_recovery_outbox").fetchone()[0], "PENDING")
        self.control.close()
        self.control = CaptureControl(self.directory / "capture.db")
        self.adapter = CaptureVirtualAdapter(self.control, self.ledger)
        self.assertIsNone(self.adapter.next_sequence())
        self.assertEqual(self.adapter.observe_recovery(recovery_id)["status"], "MISSED_UNEXECUTED")
        self.assertEqual(self.snapshot(), before)

    def test_after_commit_unknown_fences_connection_then_readback_reconciles_no_reapply(self):
        recovery_id = self.recovered()
        before = self.snapshot()
        self.control.db = CommitFault(self.control.db, after=True)
        with self.assertRaisesRegex(RuntimeError, "lost_commit_reply"):
            self.adapter.observe_recovery(recovery_id)
        with self.assertRaisesRegex(RuntimeError, "outcome_unknown"):
            self.adapter.next_sequence()
        self.control.close()
        self.control = CaptureControl(self.directory / "capture.db")
        self.adapter = CaptureVirtualAdapter(self.control, self.ledger)
        self.assertEqual(self.adapter.observe_recovery(recovery_id)["status"], "MISSED_UNEXECUTED")
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.control.db.execute("SELECT count(*) FROM capture_recovery_outbox").fetchone()[0], 1)

    def test_failed_committed_readback_stays_fenced_until_reconciliation(self):
        recovery_id = self.recovered()
        self.control.db = ReadbackFault(self.control.db, after=True)
        with self.assertRaisesRegex(RuntimeError, "readback_unavailable"):
            self.adapter.observe_recovery(recovery_id)
        with self.assertRaisesRegex(RuntimeError, "outcome_unknown"):
            self.adapter.next_recovery()
        self.control.close()
        self.control = CaptureControl(self.directory / "capture.db")
        self.adapter = CaptureVirtualAdapter(self.control, self.ledger)
        self.assertEqual(self.adapter.observe_recovery(recovery_id)["status"], "MISSED_UNEXECUTED")

    def test_conflicting_identity_raw_or_provenance_keeps_pending(self):
        for column, value in (("wallet", "conflicting-wallet"), ("raw", b"tampered"),
                              ("event_json", encoded({**SAVED, "ts_utc": 2_000_000_000})),
                              ("provenance_json", encoded({"source_kind": "stream"}))):
            with self.subTest(column=column):
                recovery_id = self.recovered(signature="conflict-" + column)
                self.control.db.execute("UPDATE capture_recovery SET " + column + "=? WHERE id=?", (value, recovery_id))
                with self.assertRaises(ValueError):
                    self.adapter.observe_recovery(recovery_id)
                self.assertEqual(self.control.db.execute("SELECT state FROM capture_recovery_outbox WHERE recovery_id=?", (recovery_id,)).fetchone()[0], "PENDING")
        self.assertEqual(self.ledger.db.execute("SELECT count(*) FROM jobs").fetchone()[0], 0)

    def test_late_stream_duplicate_cannot_become_fresh(self):
        recovery_id = self.recovered(signature="late-stream")
        self.adapter.observe_recovery(recovery_id)
        checked = json.loads(self.control.db.execute("SELECT event_json FROM capture_recovery").fetchone()[0])
        swap = {**SAVED, **checked, "ts_utc": 2_000_000_000}
        self.control.db.execute("""INSERT INTO capture_events(signature,wallet,slot,epoch,received_at,
            source_at,raw,fingerprint,stage,event_json) VALUES(?,?,?,1,2000000000,2000000000,?,?,'DURABLE',?)""",
            (swap["signature"], WALLET, str(swap["slot"]), b"late-envelope", "fixture", encoded(swap)))
        seq = self.control.db.execute("SELECT max(seq) FROM capture_events").fetchone()[0]
        before = self.snapshot()
        self.assertEqual(self.adapter.observe(seq)["reason"], "capture_recovered_identity_not_fresh")
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.adapter.observe(seq)["status"], "SKIPPED")

    def test_stream_without_current_admission_stays_evidence_only_path(self):
        recovery_id = self.recovered(kind="stream")
        row = self.control.db.execute("SELECT observation_json,provenance_json FROM capture_recovery WHERE id=?", (recovery_id,)).fetchone()
        observation, provenance = map(json.loads, row)
        observation["message_time"] = {"CreatedAt": {"seconds": 2_000_000_000, "nanos": 0}}
        observation["block"] = None
        provenance["message_time"], provenance["block"] = observation["message_time"], None
        self.control.db.execute("UPDATE capture_recovery SET observation_json=?,provenance_json=? WHERE id=?",
                                (encoded(observation), encoded(provenance), recovery_id))
        before = self.snapshot()
        self.assertIsNone(self.adapter.next_sequence())
        result = self.adapter.observe_recovery(recovery_id)
        self.assertEqual(result["reason"], "protected_stream_no_admission")
        self.assertEqual(result["provenance"]["message_time"], observation["message_time"])
        self.assertEqual(self.snapshot(), before)

    def test_cross_process_consumer_reads_committed_outbox_through_poll(self):
        self.recovered(kind="confirmed_http")
        before = self.snapshot()
        result = subprocess.run([sys.executable, "-B", str(ROOT / "tools/tests/capture_recovery_consumer_fixture.py"),
                                 str(self.control.path), str(self.ledger.path)],
                                check=True, capture_output=True, text=True)
        report = json.loads(result.stdout)
        self.assertTrue(report["ledger_unchanged"])
        self.assertEqual(report["polls"], 1)
        self.assertEqual(report["dispositions"][0]["status"], "MISSED_UNEXECUTED")
        self.assertEqual(report["network_calls"], 0)
        self.assertEqual(self.snapshot(), before)

    def test_conflicting_terminal_disposition_is_not_rewritten(self):
        recovery_id = self.recovered()
        self.adapter.observe_recovery(recovery_id)
        self.control.db.execute("UPDATE capture_recovery_outbox SET disposition_json='{}' WHERE recovery_id=?", (recovery_id,))
        with self.assertRaisesRegex(ValueError, "disposition_conflict"):
            self.adapter.observe_recovery(recovery_id)
        self.assertEqual(self.control.db.execute("SELECT disposition_json FROM capture_recovery_outbox").fetchone()[0], "{}")

    def test_existing_unknown_and_history_rows_are_preserved(self):
        # No constructor/restart of this ledger: consumed RUNNING jobs must not reset.
        self.ledger.db.execute("INSERT INTO events(id,key,signature,wallet,mint,side,detected,status,payload) VALUES(1,'old','old',?,?,'SELL',1,'UNKNOWN','{}')", (WALLET, MINT))
        self.ledger.db.execute("INSERT INTO jobs(id,event_id,kind,state,created,payload,outcome) VALUES(1,1,'HISTORY_PROOF','RUNNING',1,'{}','{\"outcome_unknown\":true}')")
        self.ledger.db.execute("INSERT INTO lots(event_id,wallet,mint,remaining_raw,remaining_cost,risk) VALUES(1,?,?,'100','200','UNKNOWN')", (WALLET, MINT))
        self.ledger.db.execute("INSERT INTO proofs VALUES(1,'HISTORY','UNRESOLVED','{\"reason\":\"unobserved_leader_sell\"}')")
        before = self.snapshot()
        self.recovered()
        self.adapter.next_sequence()
        self.assertEqual(self.snapshot(), before)

    def test_exact_token_debit_without_exact_native_leg_is_historical_evidence(self):
        recovery_id = self.recovered()
        row = self.control.db.execute("SELECT event_json,observation_json,provenance_json FROM capture_recovery WHERE id=?", (recovery_id,)).fetchone()
        facts, observation, provenance = map(json.loads, row)
        facts["exact_amounts"] = None
        observation["facts"] = facts
        observation["token_quantity"] = dict(side="SELL", mint=MINT, raw="12", decimals=6)
        provenance["token_quantity"] = observation["token_quantity"]
        self.control.db.execute("UPDATE capture_recovery SET event_json=?,observation_json=?,provenance_json=? WHERE id=?",
                                (encoded(facts), encoded(observation), encoded(provenance), recovery_id))
        before = self.snapshot()
        result = self.adapter.observe_recovery(recovery_id)
        self.assertEqual(result["token_qty_raw"], "12")
        self.assertEqual(result["status"], "MISSED_UNEXECUTED")
        self.assertEqual(self.snapshot(), before)
        self.assertIsNone(facts["exact_amounts"])

    def test_partial_token_quantity_conflict_does_not_ack_or_change_ledger(self):
        recovery_id = self.recovered()
        row = self.control.db.execute("SELECT observation_json,provenance_json FROM capture_recovery WHERE id=?", (recovery_id,)).fetchone()
        observation, provenance = map(json.loads, row)
        observation["token_quantity"] = dict(side="SELL", mint=MINT, raw="99", decimals=6)
        provenance["token_quantity"] = observation["token_quantity"]
        self.control.db.execute("UPDATE capture_recovery SET observation_json=?,provenance_json=? WHERE id=?",
                                (encoded(observation), encoded(provenance), recovery_id))
        before = self.snapshot()
        with self.assertRaisesRegex(ValueError, "token_quantity_amount_conflict"):
            self.adapter.observe_recovery(recovery_id)
        self.assertEqual(self.control.db.execute("SELECT state FROM capture_recovery_outbox").fetchone()[0], "PENDING")
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()

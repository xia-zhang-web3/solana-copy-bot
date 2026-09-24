"""Offline carry-forward checks for the single confirmed owner BUY."""

import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest


MODULE = Path(__file__).resolve().parents[1] / "prepare_owner_exit_package.py"
SPEC = importlib.util.spec_from_file_location("prepare_owner_exit_package", MODULE)
carry = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(carry)


class OwnerExitSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "old"
        (self.source / "state").mkdir(parents=True)
        (self.source / "control").mkdir()
        (self.source / "control/STOP").touch()
        self.database = self.source / "state/live_runtime.db"
        with sqlite3.connect(self.database) as db:
            db.executescript("""
                CREATE TABLE orders(order_id,status,tx_signature,simulation_status,attempt);
                CREATE TABLE positions(position_id,token,qty_raw,qty_decimals,state,closed_ts);
                CREATE TABLE fills(order_id,position_id,token,qty_raw,qty_decimals,accounting_basis);
                CREATE TABLE execution_order_sources(identity_id,copy_signal_id,owned_sell_intent_id,owner_buy_intent_id);
                CREATE TABLE execution_canary_receipt_facts(order_id,tx_signature,wallet_pubkey,token,side,token_delta_raw,token_decimals,fee_coverage,transaction_fee,decomposition);
                CREATE TABLE execution_canary_receipt_proofs(order_id,tx_signature,wallet_pubkey,token,side,confirmation_status);
                CREATE TABLE execution_canary_dispatch(order_id,tx_signature,wallet,token,side,attempt);
                CREATE TABLE execution_canary_unresolved_dispatch(order_id);
                CREATE TABLE execution_tiny_reservations(order_id,tx_signature,wallet,side,outcome);
                CREATE TABLE historical_obligations(value TEXT);
                INSERT INTO historical_obligations VALUES('keep');
            """)
            db.execute("INSERT INTO orders VALUES(?,?,?,?,?)", (carry.ORDER,
                "execution_canary_confirmed", carry.SIGNATURE, "passed", 1))
            db.execute("INSERT INTO positions VALUES(?,?,?,?,?,?)", (carry.POSITION,
                carry.MINT, str(carry.RAW_AMOUNT), carry.DECIMALS, "open", None))
            db.execute("INSERT INTO fills VALUES(?,?,?,?,?,?)", (carry.ORDER,
                carry.POSITION, carry.MINT, str(carry.RAW_AMOUNT), carry.DECIMALS,
                "legacy_unclassified"))
            db.execute("INSERT INTO execution_order_sources VALUES(?,?,?,?)", (
                "owner-buy:" + carry.BUY_INTENT, None, None, carry.BUY_INTENT))
            db.execute("INSERT INTO execution_canary_receipt_facts VALUES(?,?,?,?,?,?,?,?,?,?)",
                (carry.ORDER, carry.SIGNATURE, carry.WALLET, carry.MINT, "buy",
                 str(carry.RAW_AMOUNT), carry.DECIMALS, "known", "5000", "unresolved"))
            db.execute("INSERT INTO execution_canary_receipt_proofs VALUES(?,?,?,?,?,?)",
                (carry.ORDER, carry.SIGNATURE, carry.WALLET, carry.MINT, "buy", "finalized"))
            db.execute("INSERT INTO execution_canary_dispatch VALUES(?,?,?,?,?,?)",
                (carry.ORDER, carry.SIGNATURE, carry.WALLET, carry.MINT, "buy", 1))
            db.execute("INSERT INTO execution_tiny_reservations VALUES(?,?,?,?,?)",
                (carry.ORDER, carry.SIGNATURE, carry.WALLET, "buy", "successful"))
        self.package = self.root / "new"

    def snapshot(self):
        return carry.prepare(self.source, self.package,
                             "copybot-owner-exit-test", "copybot-owner-exit-test-usdc-01")

    def test_preserves_all_history_and_source_and_stays_stopped(self):
        before = carry.sha256(self.database)
        self.snapshot()
        self.assertEqual(before, carry.sha256(self.database))
        self.assertEqual(carry.verify(self.package)["status"], "INACTIVE_SNAPSHOT_VALID")
        with sqlite3.connect(self.package / "state/live_runtime.db") as db:
            self.assertEqual(db.execute("SELECT value FROM historical_obligations").fetchone(),
                             ("keep",))
        self.assertEqual(sorted(p.name for p in (self.package / "control").iterdir()), ["STOP"])
        with self.assertRaisesRegex(ValueError, "new_isolated_package_required"):
            self.snapshot()

    def test_wrong_owner_amount_refuses_before_package_creation(self):
        with sqlite3.connect(self.database) as db:
            db.execute("UPDATE positions SET qty_raw='1167084'")
        with self.assertRaisesRegex(ValueError, "position_identity_or_quantity"):
            self.snapshot()
        self.assertFalse(self.package.exists())

    def test_nonempty_wal_refuses_even_with_stop(self):
        Path(str(self.database) + "-wal").write_bytes(b"pending")
        with self.assertRaisesRegex(ValueError, "source_wal_not_checkpointed"):
            self.snapshot()
        self.assertFalse(self.package.exists())

    def test_new_ledger_binds_policy_and_starts_at_zero(self):
        self.snapshot()
        policy = self.root / "policy.json"
        policy.write_text(json.dumps({"run_id": "copybot-owner-exit-test"}))
        self.assertEqual(carry.init_ledger(self.package, policy)["attempts"], 0)
        self.assertTrue(carry.verify(self.package)["fresh_ledger"])
        with self.assertRaisesRegex(ValueError, "ledger_already_initialized"):
            carry.init_ledger(self.package, policy)


if __name__ == "__main__":
    unittest.main()

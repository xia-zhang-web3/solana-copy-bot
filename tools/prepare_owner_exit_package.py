#!/usr/bin/env python3
"""Carry the confirmed owner BUY into a stopped, isolated owner EXIT package.

This helper makes no provider calls, reads no signer, and creates no authority or
clock. The daemon's normal migration runner upgrades the copied DB later.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3


BUY_INTENT = "copybot-owner-buy-20260924-04-usdc-01"
ORDER = "exec-canary:owner-buy:" + BUY_INTENT
POSITION = "exec-canary-pos:" + ORDER
SIGNATURE = "3xntvoGPvGKx8SKjDjhxfaQoiAkX36p2gxseEx1oCg7oX5zNnK8zMMkj3qgrZiRLpjTfb57zJpvHz9hSUHnoCbTZ"
WALLET = "BwVw8ncEpWU7TwMTgysvwjQ85eEhKAMVbd7WU1iTE9Mk"
MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
RAW_AMOUNT = 1_167_085
DECIMALS = 6


def sha256(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def require_regular(path):
    if path.is_symlink() or not path.is_file():
        raise ValueError("regular_file_required:" + path.name)


def source_state(source):
    require_regular(source / "control/STOP")
    database = source / "state/live_runtime.db"
    require_regular(database)
    wal = Path(str(database) + "-wal")
    if wal.exists() and (wal.is_symlink() or wal.stat().st_size):
        raise ValueError("source_wal_not_checkpointed")
    return database


def read_only(database):
    # A stopped, checkpointed source is required. immutable avoids sidecar writes.
    connection = sqlite3.connect(database.as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.execute("PRAGMA query_only=ON")
    return connection


def checked_facts(db):
    if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise ValueError("database_integrity")
    if db.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("database_foreign_keys")
    if db.execute("SELECT count(*) FROM orders").fetchone()[0] != 1:
        raise ValueError("buy_order_count")
    if db.execute("SELECT count(*) FROM positions").fetchone()[0] != 1:
        raise ValueError("position_count")
    if db.execute("SELECT count(*) FROM fills").fetchone()[0] != 1:
        raise ValueError("fill_count")
    if db.execute("SELECT count(*) FROM execution_canary_receipt_facts").fetchone()[0] != 1:
        raise ValueError("receipt_count")
    order = db.execute(
        "SELECT order_id,status,tx_signature,simulation_status,attempt "
        "FROM orders"
    ).fetchone()
    if order != (ORDER, "execution_canary_confirmed", SIGNATURE, "passed", 1):
        raise ValueError("buy_order_identity_or_confirmation")
    source = db.execute(
        "SELECT identity_id,copy_signal_id,owned_sell_intent_id,owner_buy_intent_id "
        "FROM execution_order_sources"
    ).fetchone()
    if source != ("owner-buy:" + BUY_INTENT, None, None, BUY_INTENT):
        raise ValueError("buy_source_identity")
    receipt = db.execute(
        "SELECT order_id,tx_signature,wallet_pubkey,token,side,token_delta_raw,"
        "token_decimals,fee_coverage,transaction_fee,decomposition "
        "FROM execution_canary_receipt_facts"
    ).fetchone()
    if receipt != (ORDER, SIGNATURE, WALLET, MINT, "buy", str(RAW_AMOUNT),
                   DECIMALS, "known", "5000", "unresolved"):
        raise ValueError("buy_receipt_identity")
    proof = db.execute(
        "SELECT order_id,tx_signature,wallet_pubkey,token,side,confirmation_status "
        "FROM execution_canary_receipt_proofs"
    ).fetchone()
    if proof != (ORDER, SIGNATURE, WALLET, MINT, "buy", "finalized"):
        raise ValueError("buy_finality_proof")
    position = db.execute(
        "SELECT position_id,token,qty_raw,qty_decimals,state,closed_ts "
        "FROM positions"
    ).fetchone()
    if position != (POSITION, MINT, str(RAW_AMOUNT), DECIMALS, "open", None):
        raise ValueError("position_identity_or_quantity")
    fill = db.execute(
        "SELECT order_id,position_id,token,qty_raw,qty_decimals,accounting_basis "
        "FROM fills"
    ).fetchone()
    if fill != (ORDER, POSITION, MINT, str(RAW_AMOUNT), DECIMALS,
                "legacy_unclassified"):
        raise ValueError("buy_fill_identity")
    dispatch = db.execute(
        "SELECT order_id,tx_signature,wallet,token,side,attempt "
        "FROM execution_canary_dispatch"
    ).fetchone()
    if dispatch != (ORDER, SIGNATURE, WALLET, MINT, "buy", 1):
        raise ValueError("buy_dispatch_identity")
    if db.execute("SELECT count(*) FROM execution_canary_unresolved_dispatch").fetchone()[0]:
        raise ValueError("unresolved_dispatch")
    reservations = db.execute(
        "SELECT order_id,tx_signature,wallet,side,outcome "
        "FROM execution_tiny_reservations"
    ).fetchall()
    if reservations != [(ORDER, SIGNATURE, WALLET, "buy", "successful")]:
        raise ValueError("buy_reservation")
    tables = [r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )]
    counts = {name: db.execute('SELECT count(*) FROM "' + name.replace('"', '""') + '"').fetchone()[0]
              for name in tables}
    return {"buy_intent_id": BUY_INTENT, "buy_order_id": ORDER,
            "buy_signature": SIGNATURE, "wallet": WALLET, "mint": MINT,
            "position_id": POSITION, "raw_amount": RAW_AMOUNT,
            "decimals": DECIMALS, "table_counts": counts}


def write_json(path, value):
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def prepare(source, package, run_id, exit_intent_id):
    source = source.resolve(strict=True)
    package = package.absolute()
    if not run_id.startswith("copybot-owner-exit-") or exit_intent_id != run_id + "-usdc-01":
        raise ValueError("exit_run_identity")
    if package.exists() or package.is_symlink() or source == package or source in package.parents:
        raise ValueError("new_isolated_package_required")
    database = source_state(source)
    initial_sha = sha256(database)
    with read_only(database) as src:
        facts = checked_facts(src)
        package.mkdir(mode=0o700, parents=False)
        (package / "state").mkdir(mode=0o700)
        (package / "control").mkdir(mode=0o700)
        copied = package / "state/live_runtime.db"
        with sqlite3.connect(copied) as dst:
            src.backup(dst)
        copied.chmod(0o600)
    if sha256(database) != initial_sha:
        raise ValueError("source_changed_during_snapshot")
    with read_only(copied) as dst:
        copied_facts = checked_facts(dst)
    if facts != copied_facts:
        raise ValueError("snapshot_facts_changed")
    write_json(package / "control/STOP", {"reason": "inactive_owner_exit_package"})
    manifest = {"run_id": run_id, "exit_intent_id": exit_intent_id,
                "source_package": str(source), "source_database_sha256": initial_sha,
                "snapshot_database_sha256": sha256(copied),
                "source_state": facts,
                "activation_state": "STOP; no new authority, clock, or ledger",
                "snapshot_method": "sqlite_backup_from_stopped_checkpointed_immutable_source"}
    write_json(package / "SOURCE_BUY_BINDING.json", manifest)
    return manifest


def init_ledger(package, policy_path):
    package = package.resolve(strict=True)
    verify(package)
    binding_path = package / "SOURCE_BUY_BINDING.json"
    require_regular(binding_path)
    require_regular(package / "control/STOP")
    binding = json.loads(binding_path.read_text())
    policy_path = policy_path.resolve(strict=True)
    require_regular(policy_path)
    policy = json.loads(policy_path.read_text())
    if policy.get("run_id") != binding["run_id"]:
        raise ValueError("ledger_run_identity")
    control = package / "control"
    if any((control / name).exists() for name in (
        "AUTHORIZATION.json", "TECHNICAL_AUTHORITY.json", "SESSION_CLOCK.json",
        "ACTIVATE_ATTEMPT.json", "LEASE.json", "CLOCK_ATTEMPT.json"
    )):
        raise ValueError("prior_or_active_control_present")
    if any((control / name).exists() for name in (
        "broker-ledger.sqlite3", "broker-ledger.binding.json", "policy.json"
    )):
        raise ValueError("ledger_already_initialized")
    write_json(control / "policy.json", policy)
    policy_hash = hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()
    ledger = control / "broker-ledger.sqlite3"
    with sqlite3.connect(ledger) as db:
        db.execute("CREATE TABLE head (id INTEGER PRIMARY KEY CHECK(id=1), "
                   "run_id TEXT NOT NULL, policy_hash TEXT NOT NULL, usd_nano INTEGER NOT NULL, "
                   "rpc_cu INTEGER NOT NULL, attempts INTEGER NOT NULL)")
        db.execute("CREATE TABLE attempts (n INTEGER PRIMARY KEY, route TEXT NOT NULL, "
                   "method TEXT NOT NULL, usd_nano INTEGER NOT NULL, rpc_cu INTEGER NOT NULL, "
                   "at_unix REAL NOT NULL)")
        db.execute("INSERT INTO head VALUES (1,?,?,0,0,0)", (binding["run_id"], policy_hash))
    ledger.chmod(0o600)
    write_json(control / "broker-ledger.binding.json",
               {"run_id": binding["run_id"], "policy_hash": policy_hash})
    return {"run_id": binding["run_id"], "attempts": 0, "usd_nano": 0, "rpc_cu": 0}


def verify(package):
    package = package.resolve(strict=True)
    manifest_path = package / "SOURCE_BUY_BINDING.json"
    require_regular(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    source = Path(manifest["source_package"])
    source_database = source_state(source)
    if sha256(source_database) != manifest["source_database_sha256"]:
        raise ValueError("source_database_changed")
    database = package / "state/live_runtime.db"
    require_regular(database)
    with read_only(database) as db:
        facts = checked_facts(db)
    expected = manifest["source_state"]
    if any(facts[key] != value for key, value in expected.items() if key != "table_counts"):
        raise ValueError("snapshot_state_changed")
    for table, count in expected["table_counts"].items():
        if table != "schema_migrations" and facts["table_counts"].get(table) != count:
            raise ValueError("snapshot_history_count_changed:" + table)
    require_regular(package / "control/STOP")
    control = package / "control"
    if any((control / name).exists() for name in (
        "AUTHORIZATION.json", "TECHNICAL_AUTHORITY.json", "SESSION_CLOCK.json",
        "ACTIVATE_ATTEMPT.json", "LEASE.json", "CLOCK_ATTEMPT.json"
    )):
        raise ValueError("financial_control_activated")
    ledger_path = control / "broker-ledger.sqlite3"
    if ledger_path.exists():
        require_regular(ledger_path)
        require_regular(control / "policy.json")
        require_regular(control / "broker-ledger.binding.json")
        policy = json.loads((control / "policy.json").read_text())
        binding = json.loads((control / "broker-ledger.binding.json").read_text())
        policy_hash = hashlib.sha256(json.dumps(policy, sort_keys=True).encode()).hexdigest()
        if policy.get("run_id") != manifest["run_id"] or binding != {
            "run_id": manifest["run_id"], "policy_hash": policy_hash
        }:
            raise ValueError("ledger_policy_binding")
        with read_only(ledger_path) as db:
            head = db.execute(
                "SELECT run_id,policy_hash,usd_nano,rpc_cu,attempts FROM head WHERE id=1"
            ).fetchone()
            attempts = db.execute("SELECT count(*) FROM attempts").fetchone()[0]
        if head != (manifest["run_id"], policy_hash, 0, 0, 0) or attempts:
            raise ValueError("ledger_not_fresh")
    return {"status": "INACTIVE_SNAPSHOT_VALID", "run_id": manifest["run_id"],
            "position_id": facts["position_id"], "raw_amount": facts["raw_amount"],
            "source_unchanged": True, "stop_present": True,
            "fresh_ledger": ledger_path.exists()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    snapshot = sub.add_parser("snapshot")
    snapshot.add_argument("--source", type=Path, required=True)
    snapshot.add_argument("--package", type=Path, required=True)
    snapshot.add_argument("--run-id", required=True)
    snapshot.add_argument("--exit-intent-id", required=True)
    ledger = sub.add_parser("ledger")
    ledger.add_argument("--package", type=Path, required=True)
    ledger.add_argument("--policy", type=Path, required=True)
    preflight = sub.add_parser("verify")
    preflight.add_argument("--package", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    if args.command == "snapshot":
        result = prepare(args.source, args.package, args.run_id, args.exit_intent_id)
        print(json.dumps({k: result[k] for k in (
            "run_id", "exit_intent_id", "source_database_sha256", "snapshot_database_sha256"
        )}, sort_keys=True))
    elif args.command == "ledger":
        print(json.dumps(init_ledger(args.package, args.policy), sort_keys=True))
    else:
        print(json.dumps(verify(args.package), sort_keys=True))


if __name__ == "__main__":
    main()

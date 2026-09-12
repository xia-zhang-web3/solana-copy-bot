use anyhow::Result;
use chrono::{DateTime, Utc};
use copybot_core_types::CopySignalRow;
use copybot_storage_core::*;
use rusqlite::{params, Connection};

struct Db {
    _dir: tempfile::TempDir,
    store: SqliteStore,
    other: SqliteStore,
    sql: Connection,
    signal: CopySignalRow,
    order: ExecutionCanaryOrder,
    now: DateTime<Utc>,
}
impl Db {
    fn new(status: &str) -> Result<Self> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("retry.sqlite");
        let mut store = SqliteStore::open(&path)?;
        store.run_migrations(std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../migrations"
        )))?;
        let now = "2026-09-09T00:00:10Z".parse()?;
        let signal = CopySignalRow {
            signal_id: "stale-sell".into(),
            wallet_id: "leader".into(),
            token: "token".into(),
            side: "sell".into(),
            notional_sol: 0.1,
            notional_lamports: None,
            notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.into(),
            ts: now,
            status: "shadow_recorded".into(),
        };
        store.insert_copy_signal(&signal)?;
        let mut order = store
            .reserve_execution_canary_order(&signal.signal_id, "tiny", now)?
            .order;
        if status != EXECUTION_STATUS_CANARY_CANDIDATE {
            order = store.mark_execution_canary_built(&order.order_id, now)?;
        }
        if status == EXECUTION_STATUS_CANARY_SIMULATED {
            order = store.mark_execution_canary_simulated(
                &order.order_id,
                now,
                EXECUTION_SIMULATION_STATUS_PASSED,
                Some("retry_after_rpc_not_sent"),
            )?;
        }
        Ok(Self {
            _dir: dir,
            other: SqliteStore::open(&path)?,
            sql: Connection::open(path)?,
            store,
            signal,
            order,
            now,
        })
    }
    fn retire(&self) -> Result<bool> {
        self.store.mark_execution_canary_stale_sell_amount_failed(
            &self.order,
            &self.signal,
            self.now,
        )
    }
}

#[test]
fn stale_sell_retry_only_retires_exact_unsigned_attempt_once() -> Result<()> {
    for status in [
        EXECUTION_STATUS_CANARY_CANDIDATE,
        EXECUTION_STATUS_CANARY_BUILT,
        EXECUTION_STATUS_CANARY_SIMULATED,
    ] {
        let db = Db::new(status)?;
        assert!(db.retire()?);
        let retired = db
            .other
            .load_execution_canary_order(&db.order.order_id)?
            .unwrap();
        assert_eq!(retired.attempt, db.order.attempt);
        assert_eq!(retired.simulation_status, db.order.simulation_status);
        assert_eq!(
            retired.err_code.as_deref(),
            Some(EXECUTION_ERROR_BUILD_FAILED)
        );
        assert_eq!(
            retired.simulation_error.as_deref(),
            Some("source_sell_amount_stale")
        );
        assert!(!db.retire()?);
        let retry = db
            .other
            .mark_execution_canary_failed_build_retry_candidate(
                &db.order.order_id,
                db.now,
                "retry_failed_sell_with_owned_position_amount",
            )?;
        assert_eq!(retry.attempt, db.order.attempt + 1);
        assert!(!db.retire()?);
        assert_eq!(
            db.store.load_execution_canary_order(&db.order.order_id)?,
            Some(retry)
        );
    }
    Ok(())
}

#[test]
fn stale_sell_retry_checks_concurrent_state_and_durable_obligations() -> Result<()> {
    for arm in [
        "signature",
        "attempt",
        "source",
        "dispatch",
        "receipt",
        "pending",
    ] {
        let db = Db::new(EXECUTION_STATUS_CANARY_SIMULATED)?;
        match arm {
            "signature" => {
                db.other.mark_execution_canary_submitted(
                    &db.order.order_id,
                    db.now,
                    "synthetic-known-signature",
                )?;
            }
            "attempt" => {
                db.other.mark_execution_canary_retry_after_submit_not_sent(
                    &db.order.order_id,
                    db.now,
                    "retry_after_rpc_not_sent",
                )?;
            }
            "source" => {
                db.sql
                    .execute("UPDATE copy_signals SET wallet_id='changed'", [])?;
            }
            "dispatch" => {
                // Orphan durable obligation: the order still looks unsigned.
                db.sql.execute("INSERT INTO execution_canary_dispatch(order_id, signal_id, client_order_id,
                    route, attempt, wallet, token, side, tx_signature, transaction_sha256, message_sha256, claimed_at)
                    VALUES(?1,?2,?3,?4,?5,'wallet','token','sell','synthetic-dispatch',?6,?7,?8)",
                    params![db.order.order_id, db.order.signal_id, db.order.client_order_id,
                        db.order.route, db.order.attempt, "a".repeat(64), "b".repeat(64), db.now.to_rfc3339()])?;
            }
            "receipt" => {
                db.sql.execute("INSERT INTO execution_canary_receipt_proofs(order_id,tx_signature,
                    wallet_pubkey,token,side,confirmation_status,confirmed_at,last_attempt_at,reason)
                    VALUES(?1,'synthetic-receipt','wallet','token','sell','confirmed',?2,?2,'pending')",
                    params![db.order.order_id, db.now.to_rfc3339()])?;
            }
            "pending" => {
                let signal = CopySignalRow {
                    signal_id: "other-sell".into(),
                    ..db.signal.clone()
                };
                db.other.insert_copy_signal(&signal)?;
                let o = db
                    .other
                    .reserve_execution_canary_order(&signal.signal_id, "tiny", db.now)?
                    .order;
                db.other.mark_execution_canary_built(&o.order_id, db.now)?;
                db.other.mark_execution_canary_simulated(
                    &o.order_id,
                    db.now,
                    EXECUTION_SIMULATION_STATUS_PASSED,
                    None,
                )?;
                db.other.mark_execution_canary_submitted(
                    &o.order_id,
                    db.now,
                    "synthetic-pending",
                )?;
                db.other.mark_execution_canary_confirmed_unreconciled(
                    &o.order_id,
                    &ExecutionCanaryReceiptProof {
                        tx_signature: "synthetic-pending".into(),
                        wallet_pubkey: "wallet".into(),
                        token: "token".into(),
                        side: "sell".into(),
                        confirmation_status: "confirmed".into(),
                        slot: Some(1),
                        confirmed_at: db.now,
                        reason: "pending".into(),
                    },
                    db.now,
                )?;
            }
            _ => unreachable!(),
        }
        let before = db.other.load_execution_canary_order(&db.order.order_id)?;
        assert!(!db.retire()?, "{arm}");
        assert_eq!(
            db.store.load_execution_canary_order(&db.order.order_id)?,
            before,
            "{arm}"
        );
    }
    Ok(())
}

#[test]
fn stale_sell_retry_failed_write_rolls_back() -> Result<()> {
    let db = Db::new(EXECUTION_STATUS_CANARY_BUILT)?;
    db.sql.execute_batch("CREATE TRIGGER refuse_retry BEFORE UPDATE ON orders BEGIN SELECT RAISE(ABORT,'synthetic failure'); END;")?;
    assert!(db.retire().is_err());
    assert_eq!(
        db.other.load_execution_canary_order(&db.order.order_id)?,
        Some(db.order)
    );
    Ok(())
}

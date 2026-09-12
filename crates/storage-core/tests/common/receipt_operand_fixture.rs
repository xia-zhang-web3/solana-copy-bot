#![allow(dead_code)]
use chrono::{DateTime, Utc};
use copybot_core_types::{CopySignalRow, Lamports};
use copybot_storage_core::*;
use serde_json::json;
use std::path::{Path, PathBuf};

// Canonical base58 encodings of synthetic zero bytes, never claimed as chain identities.
pub const WALLET: &str = "11111111111111111111111111111111";
pub const SIGNATURE: &str = "1111111111111111111111111111111111111111111111111111111111111111";
pub const TS: &str = "2026-06-02T12:00:00Z";
pub struct Db {
    pub path: PathBuf,
    pub store: SqliteStore,
    pub at: DateTime<Utc>,
    pub id: String,
    _temp: Option<tempfile::TempDir>,
}
impl Db {
    pub fn new(label: &str) -> Self {
        let (dir, temp) = if let Some(root) = std::env::var_os("BATCH125_OUTPUT") {
            let dir = PathBuf::from(root).join(label);
            std::fs::create_dir_all(&dir).unwrap();
            (dir, None)
        } else {
            let dir = tempfile::tempdir().unwrap();
            (dir.path().to_path_buf(), Some(dir))
        };
        let path = dir.join("writer.db");
        let mut store = SqliteStore::open(&path).unwrap();
        store
            .run_migrations(Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../migrations"
            )))
            .unwrap();
        let mut db = Self {
            path,
            store,
            at: TS.parse().unwrap(),
            id: String::new(),
            _temp: temp,
        };
        db.id = db.seed("receipt-a", SIGNATURE);
        db
    }
    pub fn conn(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(&self.path).unwrap()
    }
    pub fn seed(&self, signal: &str, signature: &str) -> String {
        self.store
            .insert_copy_signal(&CopySignalRow {
                signal_id: signal.into(),
                wallet_id: WALLET.into(),
                token: WALLET.into(),
                side: "sell".into(),
                notional_sol: 0.000001,
                notional_lamports: Some(Lamports::new(1000)),
                notional_origin: copybot_core_types::COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS
                    .into(),
                ts: self.at,
                status: "shadow_recorded".into(),
            })
            .unwrap();
        let id = self
            .store
            .reserve_execution_canary_order(signal, "tiny", self.at)
            .unwrap()
            .order
            .order_id;
        self.store
            .mark_execution_canary_built(&id, self.at)
            .unwrap();
        self.store
            .mark_execution_canary_simulated(&id, self.at, EXECUTION_SIMULATION_STATUS_PASSED, None)
            .unwrap();
        self.store
            .mark_execution_canary_submitted(&id, self.at, signature)
            .unwrap();
        self.store
            .detect_failed_expense(
                &id,
                WALLET,
                "signature_status",
                "confirmed",
                Some(42),
                &json!({"InstructionError":[0,{"Custom":125}]}),
                self.at,
            )
            .unwrap();
        id
    }
    pub fn facts(&self, signature: &str, fee: Option<u64>, native: bool) -> FailedTransactionFacts {
        FailedTransactionFacts {
            tx_signature: signature.into(),
            wallet: WALLET.into(),
            slot: 42,
            commitment: "confirmed".into(),
            transaction_error: json!({"InstructionError":[0,{"Custom":125}]}),
            transaction_fee_lamports: fee.map(|v| v.to_string()),
            fee_coverage: if fee.is_some() {
                FailedExpenseCoverage::Known
            } else {
                FailedExpenseCoverage::Missing
            },
            payer: Some(WALLET.into()),
            payer_coverage: FailedExpenseCoverage::Known,
            wallet_native_pre_lamports: native.then(|| "1000".into()),
            wallet_native_post_lamports: native.then(|| (1000 - fee.unwrap_or(0)).to_string()),
            native_coverage: if native {
                FailedExpenseCoverage::Known
            } else {
                FailedExpenseCoverage::Missing
            },
        }
    }
    pub fn pay(&self, fee: Option<u64>, native: bool) {
        self.store
            .apply_failed_expense(&self.id, &self.facts(SIGNATURE, fee, native), self.at)
            .unwrap();
    }
    pub fn freeze(&self) -> PathBuf {
        self.conn()
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        let path = self.path.with_file_name("immutable.db");
        if path.exists() {
            assert_eq!(
                std::fs::read(&path).unwrap(),
                std::fs::read(&self.path).unwrap()
            );
            return path;
        }
        std::fs::copy(&self.path, &path).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&path, permissions).unwrap();
        path
    }
}

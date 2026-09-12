use super::fresh_buy_size_runtime_fixture::RuntimeFixture;
use anyhow::Result;
use copybot_core_types::TokenQuantity;
use copybot_storage_core::SqliteStore;
use rusqlite::{params, Connection};

#[derive(Clone, Copy, Debug)]
pub(super) enum Block {
    Disabled,
    Loss,
    Open,
}

impl Block {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Disabled => "entry_submit_disabled",
            Self::Loss => "max_daily_loss",
            Self::Open => "max_open_positions",
        }
    }
    pub fn apply(self, f: &mut RuntimeFixture) -> Result<()> {
        match self {
            Self::Disabled => f.config.canary_entry_submit_enabled = false,
            Self::Loss | Self::Open => {
                let previous_open = f.store.execution_canary_open_position_count()?;
                f.store.record_execution_canary_open_position(
                    "other-order",
                    "OtherMint",
                    100.0,
                    Some(TokenQuantity::new(100, 0)),
                    0.05,
                    f.now,
                )?;
                if matches!(self, Self::Loss) {
                    Connection::open(&f.db_path)?.execute(
                        "UPDATE positions SET state='closed', closed_ts=?1, pnl_lamports=-20000000, pnl_sol=-0.02 WHERE token='OtherMint'",
                        [(f.now + chrono::Duration::seconds(4)).to_rfc3339()])?;
                    f.config.canary_max_daily_loss_sol = 0.02;
                    assert_eq!(
                        f.store
                            .execution_canary_entry_safety_loss_sol_since(f.now)?,
                        0.02
                    );
                    assert_eq!(
                        f.store.execution_canary_open_position_count()?,
                        previous_open
                    );
                } else {
                    f.config.canary_max_open_positions = 1;
                    assert_eq!(
                        f.store.execution_canary_open_position_count()?,
                        previous_open + 1
                    );
                }
            }
        }
        Ok(())
    }
    pub fn remove(self, f: &mut RuntimeFixture) -> Result<()> {
        match self {
            Self::Disabled => f.config.canary_entry_submit_enabled = true,
            Self::Loss | Self::Open => {
                Connection::open(&f.db_path)?
                    .execute("DELETE FROM positions WHERE token='OtherMint'", [])?;
            }
        }
        Ok(())
    }
}

pub(super) async fn fixture(name: &str) -> Result<RuntimeFixture> {
    RuntimeFixture::new(name, 20_000_000, 200, 10_000_000, 100, true).await
}

pub(super) fn reopen(f: &mut RuntimeFixture) -> Result<()> {
    // Close the real file connection before reopening it; the temporary store is empty.
    let old = std::mem::replace(
        &mut f.store,
        SqliteStore::open(std::path::Path::new(":memory:"))?,
    );
    drop(old);
    f.store = SqliteStore::open(&f.db_path)?;
    Ok(())
}

pub(super) fn rows(f: &RuntimeFixture) -> Result<serde_json::Value> {
    let conn = Connection::open(&f.db_path)?;
    let mut dump = serde_json::Map::new();
    // Whole business tables: catches state, amount, IDs, timestamps and phantom expenses/fills.
    for table in [
        "orders",
        "execution_canary_build_plan_metadata",
        "positions",
        "fills",
        "execution_canary_receipt_proofs",
        "execution_failed_expense_tasks",
        "execution_failed_expense_facts",
        "execution_failed_expense_ledger",
    ] {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            [table],
            |r| r.get(0),
        )?;
        if !exists {
            continue;
        }
        let mut stmt = conn.prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))?;
        let cols = stmt.column_count();
        let values = stmt
            .query_map(params![], |row| {
                (0..cols)
                    .map(|i| row.get_ref(i).map(|v| format!("{v:?}")))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        dump.insert(table.into(), serde_json::json!(values));
    }
    Ok(dump.into())
}

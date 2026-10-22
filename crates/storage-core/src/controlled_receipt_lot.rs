//! Immutable controlled scenario identity. Wallet balances never define its lots.
use crate::{association_inbox::AssociationInbox, SqliteStore};
use anyhow::{ensure, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
pub const POLICY: &str = "controlled_receipt_lot_custody_v1";
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub policy: String,
    pub source_run_id: String,
    pub bot_run_id: String,
    pub source_wallet: String,
    pub bot_wallet: String,
    pub source_buy_intent_id: String,
    pub source_state_db_path: String,
}
impl Scope {
    pub fn run<'a>(&'a self, wallet: &str) -> Result<&'a str> {
        ensure!(
            self.policy == POLICY
                && self.source_wallet != self.bot_wallet
                && self.source_run_id != self.bot_run_id,
            "receipt_lot_scope_identity"
        );
        for id in [
            &self.source_run_id,
            &self.bot_run_id,
            &self.source_buy_intent_id,
        ] {
            ensure!(
                (1..=110).contains(&id.len())
                    && id
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c)),
                "receipt_lot_scope_id"
            );
        }
        for key in [&self.source_wallet, &self.bot_wallet] {
            ensure!(
                (32..=44).contains(&key.len()) && key.bytes().all(|c| c.is_ascii_alphanumeric()),
                "receipt_lot_scope_wallet"
            );
        }
        ensure!(
            std::path::Path::new(&self.source_state_db_path).is_absolute()
                && !self.source_state_db_path.contains('\0'),
            "receipt_lot_source_path"
        );
        if wallet == self.source_wallet {
            Ok(&self.source_run_id)
        } else {
            ensure!(wallet == self.bot_wallet, "receipt_lot_wallet");
            Ok(&self.bot_run_id)
        }
    }
    pub fn same_scenario(&self, other: &Self) -> bool {
        self.policy == other.policy
            && self.source_run_id == other.source_run_id
            && self.bot_run_id == other.bot_run_id
            && self.source_wallet == other.source_wallet
            && self.bot_wallet == other.bot_wallet
            && self.source_buy_intent_id == other.source_buy_intent_id
    }
}
pub(crate) fn load(c: &Connection) -> Result<Option<(String, String, Scope)>> {
    if !c.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='controlled_receipt_lot_scope')",
        [],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(None);
    }
    let row = c
        .query_row(
            "SELECT wallet,run_id,scope_json FROM controlled_receipt_lot_scope WHERE singleton=1",
            [],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    row.map(|(wallet, run, raw)| {
        let scope: Scope = serde_json::from_str(&raw)?;
        ensure!(scope.run(&wallet)? == run, "receipt_lot_scope_corrupt");
        Ok((wallet, run, scope))
    })
    .transpose()
}
fn register(c: &Connection, scope: &Scope, wallet: &str) -> Result<()> {
    let run = scope.run(wallet)?;
    let wire = serde_json::to_string(scope)?;
    ensure!(
        wire.len() <= 8192 && std::path::Path::new(&scope.source_state_db_path).is_absolute(),
        "receipt_lot_scope_bound"
    );
    if let Some((w, r, s)) = load(c)? {
        ensure!(
            w == wallet && r == run && s == *scope,
            "receipt_lot_scope_changed"
        );
        return Ok(());
    }
    c.execute(
        "INSERT INTO controlled_receipt_lot_scope VALUES(1,?1,?2,?3)",
        params![wallet, run, wire],
    )?;
    ensure!(
        load(c)?.as_ref() == Some(&(wallet.into(), run.into(), scope.clone())),
        "receipt_lot_scope_readback"
    );
    Ok(())
}
impl SqliteStore {
    pub fn register_controlled_receipt_lot_scope(&self, scope: &Scope, wallet: &str) -> Result<()> {
        self.with_immediate_transaction_retry("register receipt lot scope", |c| {
            register(c, scope, wallet)
        })
    }
    pub fn controlled_receipt_lot_scope(&self) -> Result<Option<(String, String, Scope)>> {
        load(&self.conn)
    }
}
impl AssociationInbox {
    pub fn register_controlled_receipt_lot_scope(
        &mut self,
        scope: &Scope,
        wallet: &str,
    ) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        register(&tx, scope, wallet)?;
        tx.commit()?;
        Ok(())
    }
}
pub(crate) fn source_scoped(c: &Connection, run: &str, wallet: &str) -> Result<bool> {
    Ok(load(c)?.is_some_and(|(w, r, s)| w == wallet && r == run && s.source_wallet == wallet))
}
pub(crate) fn receipt_native_floor(c: &Connection, run: &str, wallet: &str) -> Result<bool> {
    Ok(load(c)?.is_some_and(|(w, r, _)| w == wallet && r == run))
}
#[path = "controlled_receipt_lot_source.rs"]
mod source;
pub(crate) use source::event_allowed;
pub use source::{LotReceipt, SourceReceipts};

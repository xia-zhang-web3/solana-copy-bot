//! Bounded observation store. No financial table mutations or trade authority.
use anyhow::{ensure, Result};
#[path = "association_budget.rs"]
mod budget;
use copybot_core_types::association_delivery::*;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::{path::Path, time::Duration};
#[path = "association_inbox_schema.rs"]
pub mod schema;
#[path = "association_inbox_write.rs"]
mod write;
/// Explicit consumer behavior; merely applying 0072 never enables staging.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum ConsumerMode {
    #[default]
    ObservationOnly,
    ProviderOrderStrictV1,
}
#[path = "association_inbox_ordered.rs"]
pub(crate) mod ordered;
#[derive(Debug, Clone, Copy)]
pub struct InboxLimits {
    pub count: usize,
    pub bytes: usize,
    pub busy_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxIdentity {
    pub admission: AdmissionFacts,
    pub candidate: CandidateGeneration,
    pub first_session: String,
    pub first_sequence: u64,
    pub terminal: Option<Terminal>,
    pub conflict: bool,
    pub recovery: bool,
}
pub struct AssociationInbox {
    pub(crate) conn: Connection,
    pub(crate) limits: InboxLimits,
    pub(crate) mode: ConsumerMode,
}
impl AssociationInbox {
    /// Requires the separately migrated main DB. Never creates or repairs schema.
    pub fn open(path: impl AsRef<Path>, limits: InboxLimits) -> Result<Self> {
        Self::open_mode(path, limits, ConsumerMode::ObservationOnly)
    }
    /// Automatic strict staging is confined to this explicit consumer entrypoint.
    /// The caller must keep execution disabled; these records carry no authority.
    pub fn open_ordered_sell_consumer(path: impl AsRef<Path>, limits: InboxLimits) -> Result<Self> {
        Self::open_mode(path, limits, ConsumerMode::ProviderOrderStrictV1)
    }
    fn open_mode(path: impl AsRef<Path>, limits: InboxLimits, mode: ConsumerMode) -> Result<Self> {
        ensure!(
            limits.count > 0 && limits.bytes > 0 && (1..=60_000).contains(&limits.busy_ms),
            "invalid inbox limits"
        );
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        conn.busy_timeout(Duration::from_millis(limits.busy_ms))?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        schema::required(&conn)?;
        crate::association_sell_preparation::schema::required(&conn)?;
        crate::association_sell_preparation::parent_graph::schema::required(&conn)?;
        ordered::required(&conn, mode)?;
        let mut result = Self { conn, limits, mode };
        result.recover()?;
        Ok(result)
    }
    pub fn new_session_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }
    pub fn identity(&self, signature: &str) -> Result<Option<InboxIdentity>> {
        identity(&self.conn, signature)
    }
    pub fn usage(&self) -> Result<(usize, usize)> {
        usage(&self.conn, self.mode)
    }
    fn recover(&mut self) -> Result<()> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        budget::validate_open(&tx)?;
        tx.execute(
            "UPDATE association_inbox_identities SET recovery=1 WHERE terminal IS NULL",
            [],
        )?;
        let pending:i64=tx.query_row("SELECT count(*) FROM association_inbox_identities WHERE terminal IS NULL AND recovery!=1",[],|r|r.get(0))?;
        ensure!(pending == 0, "inbox recovery ignored");
        let mut proof = crate::association_sell_preparation::Readback::new(self.mode);
        crate::association_sell_preparation::initialize(&tx, &mut proof)?;
        check_budget(&tx, self.limits, self.mode)?;
        proof.verify(&tx)?;
        tx.commit()?;
        proof.verify(&self.conn)?;
        let pending:i64=self.conn.query_row("SELECT count(*) FROM association_inbox_identities WHERE terminal IS NULL AND recovery!=1",[],|r|r.get(0))?;
        ensure!(pending == 0, "inbox recovery readback failed");
        Ok(())
    }
}
pub(crate) fn identity(c: &Connection, s: &str) -> Result<Option<InboxIdentity>> {
    let row:Option<(String,String,String,i64,Option<String>,bool,bool)>=c.query_row(
        "SELECT admission,candidate,first_session,first_sequence,terminal,conflict,recovery FROM association_inbox_identities WHERE signature=?1",[s],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?))).optional()?;
    row.map(|(a, c, s, n, t, conflict, recovery)| {
        Ok(InboxIdentity {
            admission: serde_json::from_str(&a)?,
            candidate: serde_json::from_str(&c)?,
            first_session: s,
            first_sequence: n.try_into()?,
            terminal: t.map(|s| serde_json::from_str(&s)).transpose()?,
            conflict,
            recovery,
        })
    })
    .transpose()
}
fn usage(c: &Connection, mode: ConsumerMode) -> Result<(usize, usize)> {
    budget::usage(c, mode)
}
pub(crate) fn check_budget(c: &Connection, l: InboxLimits, mode: ConsumerMode) -> Result<()> {
    let (n, b) = usage(c, mode)?;
    ensure!(
        n <= l.count && b <= l.bytes,
        "association inbox full: count/byte limit; delivery NOT acknowledged"
    );
    Ok(())
}

use super::{fractional_fixture::Fixture, fractional_tests as f};
use crate::execution_owned_sell_rpc::{
    body,
    fractional::transport::{self, Check, Pending, Transport},
};
use anyhow::Result;
use copybot_storage_core::{
    ordered_sell_quote::{fractional::inventory::Evidence, QuoteClaim},
    SqliteStore,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub(super) struct Mock {
    pub evidence: Evidence,
    pub fault: &'static str,
    pub db: String,
    pub budget: body::Budget,
    pub calls: Arc<AtomicUsize>,
    pub body_chunks: Arc<AtomicUsize>,
    pub hold: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
}
impl Mock {
    pub fn new(f: &Fixture, fault: &'static str) -> Result<Self> {
        let mut e = f::evidence()?;
        e.block["capacity_padding"] = json!("x".repeat(3 << 20));
        Ok(Self {
            evidence: e,
            fault,
            db: f.db.path.to_string_lossy().into_owned(),
            budget: Default::default(),
            calls: Arc::new(AtomicUsize::new(0)),
            body_chunks: Arc::new(AtomicUsize::new(0)),
            hold: None,
        })
    }
}
impl Transport for Mock {
    fn read<'a>(&'a mut self, request: Value, check: &'a mut Check<'_>) -> Pending<'a> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some((started, release)) = self.hold.take() {
                started.notify_one();
                release.notified().await;
            }
            let full = request["method"] == "getBlock"
                && request["params"][1]["transactionDetails"] == "full";
            let result = match request["method"].as_str().unwrap() {
                "getGenesisHash" => json!("11111111111111111111111111111111"),
                "getBlock" if full => self.evidence.block.clone(),
                "getBlock" => self.evidence.parent.clone(),
                "getTokenAccountsByOwnerAtSlot" => self
                    .evidence
                    .pages
                    .iter()
                    .find(|p| request["params"][1]["programId"] == p.program)
                    .unwrap()
                    .response
                    .clone(),
                "getTokenAccountsByOwner" => self.evidence.execution_accounts.clone(),
                _ => panic!("unexpected method"),
            };
            let mut value = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
            if full && self.fault == "id" {
                value["id"] = json!("wrong");
            }
            if full && self.fault == "result" {
                value["result"] = Value::Null;
            }
            let mut raw = serde_json::to_vec(&value)?;
            drop(value);
            if full && self.fault == "truncated" {
                raw.pop();
            }
            if full && self.fault == "invalid" {
                raw[0] = b'!';
            }
            if full && self.fault == "chunked" {
                raw = vec![b' '; body::FULL_BLOCK_BYTES + 1];
            }
            let head = body::Head {
                endpoint_matches: self.fault != "endpoint" || !full,
                status: if full && self.fault == "status" {
                    500
                } else {
                    200
                },
                content_length: if full && self.fault == "declared" {
                    Some((body::FULL_BLOCK_BYTES + 1) as u64)
                } else if full && matches!(self.fault, "chunked" | "no_length") {
                    None
                } else {
                    Some(raw.len() as u64)
                },
            };
            let mut steps = Steps {
                parts: chunks(&raw),
                number: 0,
                full,
                fault: self.fault,
                db: self.db.clone(),
                observed: self.body_chunks.clone(),
            };
            drop(raw);
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(1500);
            transport::receive(
                &request,
                head,
                deadline,
                &mut self.budget,
                check,
                &mut steps,
                |s| {
                    Box::pin(async move {
                        s.number += 1;
                        if s.full {
                            s.observed.fetch_add(1, Ordering::SeqCst);
                        }
                        if s.full && s.number == 2 {
                            match s.fault {
                                "stale" => {
                                    let c = rusqlite::Connection::open(&s.db)?;
                                    c.execute(
                                        "UPDATE fractional_sell_decisions SET base_binding='{}'",
                                        [],
                                    )?;
                                }
                                "parent_conflict" => {
                                    let c = rusqlite::Connection::open(&s.db)?;
                                    anyhow::ensure!(c.execute(
                                        "UPDATE association_parent_blocks SET contradiction='test parent conflict' WHERE EXISTS(SELECT 1 FROM association_parent_dependencies d WHERE block_key LIKE '%'||d.block_hash||'%')",
                                        [],
                                    )? > 0, "fixture parent dependency missing");
                                }
                                "expired" => {
                                    tokio::time::sleep(std::time::Duration::from_millis(1100)).await
                                }
                                "stopped" => anyhow::bail!("mock_body_cancelled"),
                                _ => {}
                            }
                        }
                        tokio::task::yield_now().await;
                        Ok(s.parts.next())
                    })
                },
            )
            .await
        })
    }
}
struct Steps {
    parts: std::vec::IntoIter<Vec<u8>>,
    number: usize,
    full: bool,
    fault: &'static str,
    db: String,
    observed: Arc<AtomicUsize>,
}
pub(super) fn chunks(raw: &[u8]) -> std::vec::IntoIter<Vec<u8>> {
    raw.chunks(8192)
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>()
        .into_iter()
}
pub(super) fn next(parts: &mut std::vec::IntoIter<Vec<u8>>) -> body::Chunk<'_, Vec<u8>> {
    Box::pin(std::future::ready(Ok(parts.next())))
}
pub(super) async fn bind(f: &mut Fixture, claim: QuoteClaim, mock: Mock) -> Result<QuoteClaim> {
    let c = f::config(f)?;
    crate::execution_owned_sell_rpc::fractional::bind_transport(
        &mut f.db.store,
        &c,
        claim,
        super::association_parent_fixture::limits(),
        mock,
    )
    .await
}
pub(super) fn refused(f: &Fixture, original: &QuoteClaim) -> Result<()> {
    let state: String =
        f.db.sql
            .query_row("SELECT state FROM fractional_sell_decisions", [], |r| {
                r.get(0)
            })?;
    assert_eq!(state, "collecting");
    assert!(!f.db.store.has_owned_sell_handoff(&original.intent_id)?);
    let count: i64 = f.db.sql.query_row(
        "SELECT count(*) FROM ordered_sell_quote_results WHERE record IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(count, 0);
    let reopened = SqliteStore::open(&f.db.path)?;
    assert!(reopened
        .begin_fractional_sell(
            original,
            super::association_parent_fixture::limits(),
            chrono::Utc::now(),
            &"a".repeat(64)
        )
        .is_err());
    assert_eq!(
        reopened
            .load_execution_canary_open_position(&original.binding.mint)?
            .unwrap()
            .qty_exact
            .unwrap()
            .raw(),
        1000
    );
    Ok(())
}

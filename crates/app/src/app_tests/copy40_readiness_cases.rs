//! Raw saved HTTP chunks and explicitly synthetic wait clocks, with one byte budget.
use super::copy40_readiness_fixture::{start, Case};
use crate::execution_owned_sell_rpc::{
    body,
    fractional::{readiness, transport},
};
use anyhow::{ensure, Result};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(super) struct SyntheticClock {
    pub now: Arc<Mutex<DateTime<Utc>>>,
    db: PathBuf,
    kill: PathBuf,
    fault: &'static str,
    fired: bool,
}
impl SyntheticClock {
    pub fn new(case: &Case, fault: &'static str) -> Self {
        Self {
            now: Arc::new(Mutex::new(start())),
            db: case.path.clone(),
            kill: case.config.canary_kill_switch_path.clone().into(),
            fault,
            fired: false,
        }
    }
}
impl readiness::Clock for SyntheticClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
    fn sleep(&mut self, duration: Duration) -> readiness::Sleep<'_> {
        Box::pin(async move {
            *self.now.lock().unwrap() += chrono::Duration::from_std(duration).unwrap();
            if !self.fired {
                self.fired = true;
                match self.fault {
                    "deadline_wait" => *self.now.lock().unwrap() += chrono::Duration::seconds(30),
                    "cutoff_wait" => *self.now.lock().unwrap() += chrono::Duration::seconds(26),
                    "stop_wait" => std::fs::write(&self.kill, b"SYNTHETIC OFFLINE STOP").unwrap(),
                    "generation_wait" => {
                        let sql = rusqlite::Connection::open(&self.db).unwrap();
                        assert_eq!(
                            sql.execute(
                                "UPDATE positions SET qty_raw='1041504' WHERE state='open'",
                                []
                            )
                            .unwrap(),
                            1
                        );
                    }
                    "pending_wait" => {
                        let sql = rusqlite::Connection::open(&self.db).unwrap();
                        assert_eq!(sql.execute("UPDATE orders SET status='execution_canary_confirmed_unreconciled'", []).unwrap(), 1);
                    }
                    "unknown_wait" => inject_unknown(&self.db).unwrap(),
                    _ => {}
                }
            }
        })
    }
}
fn clone_first(
    sql: &rusqlite::Connection,
    table: &str,
    changes: &[(&str, rusqlite::types::Value)],
) -> Result<()> {
    let mut query = sql.prepare(&format!("SELECT * FROM {table} LIMIT 1"))?;
    let names = query
        .column_names()
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>();
    let mut values = query.query_row([], |r| {
        (0..names.len())
            .map(|i| r.get::<_, rusqlite::types::Value>(i))
            .collect::<rusqlite::Result<Vec<_>>>()
    })?;
    drop(query);
    for (name, value) in changes {
        let index = names
            .iter()
            .position(|n| n == name)
            .expect("mock clone field");
        values[index] = value.clone();
    }
    let args = std::iter::repeat_n("?", values.len())
        .collect::<Vec<_>>()
        .join(",");
    sql.execute(
        &format!("INSERT INTO {table} VALUES({args})"),
        rusqlite::params_from_iter(values),
    )?;
    Ok(())
}
fn inject_unknown(path: &std::path::Path) -> Result<()> {
    use rusqlite::types::Value::Text;
    let sql = rusqlite::Connection::open(path)?;
    let signal = "MOCK-readiness-unknown-signal".to_string();
    let order = "exec-canary:MOCK-readiness-unknown".to_string();
    let client = "MOCK-readiness-unknown-client".to_string();
    let signature = "1".repeat(64); // Explicit synthetic unknown identity, never submitted.
    clone_first(&sql, "copy_signals", &[("signal_id", Text(signal.clone()))])?;
    clone_first(
        &sql,
        "orders",
        &[
            ("order_id", Text(order.clone())),
            ("signal_id", Text(signal.clone())),
            ("client_order_id", Text(client.clone())),
            ("status", Text("execution_canary_submitted".into())),
            ("confirm_ts", rusqlite::types::Value::Null),
            ("simulation_error", Text("dispatch_outcome_unknown".into())),
            ("tx_signature", Text(signature.clone())),
        ],
    )?;
    clone_first(
        &sql,
        "execution_canary_dispatch",
        &[
            ("order_id", Text(order)),
            ("signal_id", Text(signal)),
            ("client_order_id", Text(client)),
            ("tx_signature", Text(signature)),
            ("message_sha256", Text("e".repeat(64))),
            ("transaction_sha256", Text("f".repeat(64))),
            ("transport_note", Text("dispatch_outcome_unknown".into())),
        ],
    )?;
    ensure!(
        sql.query_row(
            "SELECT count(*) FROM execution_canary_unresolved_dispatch",
            [],
            |r| r.get::<_, u64>(0)
        )? == 1,
        "mock unknown identity must remain unresolved"
    );
    Ok(())
}
pub(super) struct RawSaved {
    requests: Vec<Value>,
    originals: Vec<Vec<u8>>,
    late: Vec<u8>,
    pub calls: Vec<Value>,
    pub budget: body::Budget,
    pub remaining_before: Vec<usize>,
    pub observations: usize,
    pub decision_ids: Vec<String>,
    db: PathBuf,
    fault: &'static str,
}
impl RawSaved {
    pub fn new(case: &Case, fault: &'static str) -> Self {
        Self {
            requests: case.rpc.iter().map(|r| r.request.clone()).collect(),
            originals: case.rpc.iter().map(|r| r.raw.clone()).collect(),
            late: case.late.raw.clone(),
            calls: vec![],
            budget: Default::default(),
            remaining_before: vec![],
            observations: 0,
            decision_ids: vec![],
            db: case.path.clone(),
            fault,
        }
    }
    pub fn precharge_for_second_observation(&mut self) -> Result<()> {
        let first_round = self.originals.iter().map(Vec::len).sum::<usize>();
        ensure!(
            first_round + 1 < body::AGGREGATE_WIRE_BYTES,
            "saved40 resource precondition"
        );
        // Explicit resource-boundary mock: first complete observation fits; any
        // additional response after the wait cannot fit the original same budget.
        self.budget
            .charge(&vec![b' '; body::AGGREGATE_WIRE_BYTES - first_round - 1])
    }
    fn raw(&mut self, request: &Value) -> Result<Vec<u8>> {
        let n = self.calls.len();
        let index = if n < 3 { n } else { 3 + (n - 3) % 3 };
        ensure!(
            *request == self.requests[index],
            "saved40 immutable RPC operands changed"
        );
        if index == 3 {
            self.observations += 1;
        }
        let mut raw = if index == 4
            && self.fault != "never"
            && (self.observations == 3
                || matches!(self.fault, "owner" | "program" | "slot" | "wrong_nonzero"))
        {
            self.late.clone()
        } else {
            self.originals[index].clone()
        };
        if (index == 4 && matches!(self.fault, "owner" | "program" | "slot" | "wrong_nonzero"))
            || (index == 2 && self.fault == "bank")
            || (index == 1 && self.fault == "native_bank")
            || (index == 5 && matches!(self.fault, "short_bot" | "excess_bot"))
        {
            let mut value: Value = serde_json::from_slice(&raw)?;
            match self.fault {
                "owner" => {
                    value["result"]["value"][0]["account"]["data"]["parsed"]["info"]["owner"] =
                        json!(super::copy40_readiness_fixture::BOT)
                }
                "program" => {
                    value["result"]["value"][0]["account"]["owner"] = json!(
                        copybot_storage_core::ordered_sell_quote::fractional::inventory::TOKEN22
                    )
                }
                "slot" => value["result"]["context"]["slot"] = json!(456954132),
                "wrong_nonzero" => {
                    value["result"]["value"][0]["account"]["data"]["parsed"]["info"]
                        ["tokenAmount"]["amount"] = json!("1")
                }
                "bank" => value["result"]["blockhash"] = json!("11111111111111111111111111111111"),
                "native_bank" => {
                    value["result"]["blockhash"] = json!("11111111111111111111111111111111")
                }
                "short_bot" => {
                    value["result"]["value"][0]["account"]["data"]["parsed"]["info"]
                        ["tokenAmount"]["amount"] = json!("1041604")
                }
                "excess_bot" => {
                    value["result"]["value"][0]["account"]["data"]["parsed"]["info"]
                        ["tokenAmount"]["amount"] = json!("1041606")
                }
                _ => unreachable!(),
            }
            raw = serde_json::to_vec(&value)?;
        }
        if index == 4 && self.fault == "malformed_body" {
            raw[0] = b'!';
        }
        if index == 4 && self.fault == "wrong_id" {
            let mut value: Value = serde_json::from_slice(&raw)?;
            value["id"] = json!("wrong request binding");
            raw = serde_json::to_vec(&value)?;
        }
        Ok(raw)
    }
}
impl transport::Transport for RawSaved {
    fn read<'a>(
        &'a mut self,
        request: Value,
        check: &'a mut transport::Check<'_>,
    ) -> transport::Pending<'a> {
        Box::pin(async move {
            let raw = self.raw(&request)?;
            let sql = rusqlite::Connection::open(&self.db)?;
            let (state, decision_id): (String, String) = sql.query_row(
                "SELECT state,decision_id FROM fractional_sell_decisions",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(
                state == "collecting",
                "saved40 prematurely proven during acquisition"
            );
            self.decision_ids.push(decision_id);
            self.calls.push(request.clone());
            self.remaining_before.push(self.budget.remaining_wire());
            if self.calls.len() == 5 && self.fault == "transport_unknown" {
                anyhow::bail!("EXPLICIT MOCK unknown transport outcome; no retry");
            }
            let head = body::Head {
                endpoint_matches: true,
                status: 200,
                content_length: Some(raw.len() as u64),
            };
            let mut chunks = raw
                .chunks(8192)
                .map(<[u8]>::to_vec)
                .collect::<Vec<_>>()
                .into_iter();
            let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
            transport::receive(
                &request,
                head,
                deadline,
                &mut self.budget,
                check,
                &mut chunks,
                |parts| Box::pin(std::future::ready(Ok(parts.next()))),
            )
            .await
        })
    }
}

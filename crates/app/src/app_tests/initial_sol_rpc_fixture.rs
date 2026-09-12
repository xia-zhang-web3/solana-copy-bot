use super::native_rpc_fixture::{Fixture, Reply};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct FundingRpc {
    pub balance: u64,
    pub fee: Option<u64>,
    pub rent: u64,
    pub delay_ms: u64,
    pub rows: HashMap<String, Value>,
}
impl Default for FundingRpc {
    fn default() -> Self {
        Self {
            balance: 1_000_000_000,
            fee: Some(19_000),
            rent: 2_039_280,
            delay_ms: 0,
            rows: HashMap::new(),
        }
    }
}
pub(super) fn system(lamports: u64) -> Value {
    json!({"lamports":lamports,"owner":"11111111111111111111111111111111",
        "executable":false,"data":[STANDARD.encode([]),"base64"]})
}
impl FundingRpc {
    pub fn reply(&self, request: &Value) -> Value {
        let result = match request["method"].as_str().unwrap() {
            "getFeeForMessage" => json!({"context":{"slot":70},"value":self.fee}),
            "getMinimumBalanceForRentExemption" => json!(self.rent),
            "getMultipleAccounts" => {
                let rows: Vec<_> = request["params"][0]
                    .as_array()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(i, key)| {
                        self.rows
                            .get(key.as_str().unwrap())
                            .cloned()
                            .unwrap_or_else(|| {
                                if i == 0 {
                                    system(self.balance)
                                } else {
                                    Value::Null
                                }
                            })
                    })
                    .collect();
                json!({"context":{"slot":72},"value":rows})
            }
            "sendTransaction" => json!("synthetic-initial-sol-submit"),
            "getSignatureStatuses" => json!({"value":[null]}),
            other => panic!("unexpected initial SOL fixture method {other}"),
        };
        json!({"jsonrpc":"2.0","id":request["id"],"result":result})
    }
    pub async fn server(state: Arc<Mutex<Self>>) -> anyhow::Result<Fixture> {
        Fixture::start(false, move |r| Reply::json(state.lock().unwrap().reply(r))).await
    }
}

pub(super) fn buy_config(wallet: &str) -> copybot_config::ExecutionConfig {
    let mut config = copybot_config::ExecutionConfig::default();
    config.canary_tiny_submit_enabled = true;
    config.canary_wallet_pubkey = wallet.into();
    config.execution_signer_pubkey = wallet.into();
    config
}

/// Explicit prelude for positive BUY mocks only. SELL mocks never call this.
pub(super) async fn serve_three(listener: &tokio::net::TcpListener) -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut methods = std::collections::HashSet::new();
    for _ in 0..3 {
        let (mut socket, _) =
            tokio::time::timeout(std::time::Duration::from_secs(3), listener.accept()).await??;
        let request = super::native_rpc_fixture::read_request(&mut socket).await?;
        let method = request["method"].as_str().unwrap();
        anyhow::ensure!(
            [
                "getFeeForMessage",
                "getMultipleAccounts",
                "getMinimumBalanceForRentExemption"
            ]
            .contains(&method),
            "expected funding RPC, got {method}"
        );
        anyhow::ensure!(methods.insert(method.to_owned()), "duplicate funding RPC");
        let body = FundingRpc::default().reply(&request).to_string();
        socket
            .write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await?;
    }
    Ok(())
}

/// Exact tiny BUY trace: funding fee/accounts/rent, then a separately bound final fee.
pub(super) fn assert_funded_buy_trace(actual: &[String], business: &[&str]) {
    let methods = [
        "getFeeForMessage",
        "getMultipleAccounts",
        "getMinimumBalanceForRentExemption",
    ];
    let simulation = actual
        .iter()
        .position(|m| m == "simulateTransaction")
        .unwrap();
    let send = actual.iter().position(|m| m == "sendTransaction").unwrap();
    for method in methods {
        let occurrences: Vec<_> = actual
            .iter()
            .enumerate()
            .filter(|(_, m)| m.as_str() == method)
            .collect();
        assert_eq!(
            occurrences.len(),
            if method == "getFeeForMessage" { 2 } else { 1 },
            "{actual:?}"
        );
        for (index, _) in occurrences {
            assert!(index > simulation && index < send, "{actual:?}");
        }
    }
    let rest: Vec<_> = actual
        .iter()
        .map(String::as_str)
        .filter(|m| !methods.contains(m))
        .collect();
    assert_eq!(rest, business);
}

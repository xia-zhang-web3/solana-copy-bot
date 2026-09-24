//! Real adapter fixture with synthetic signer and loopback-only provider facts.
use super::{owner_buy_fixture as base, owner_buy_wire_fixture as wire};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use copybot_config::{ExecutionConfig, TinyPolicyMode};
use copybot_storage_core::{owner_technical_buy_order_id, SqliteStore};
use ed25519_dalek::SigningKey;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) struct Case {
    pub root: super::temporary_output_fixture::OutputRoot,
    pub store: SqliteStore,
    pub config: ExecutionConfig,
    pub state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<Result<()>>,
}
#[derive(Default)]
pub(super) struct State {
    pub calls: Vec<String>,
    pub mode: &'static str,
    pub simulated: Option<String>,
    pub sent: Option<String>,
}
impl Drop for Case {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Case {
    pub async fn new() -> Result<Self> {
        let root = super::temporary_output_fixture::OutputRoot::new("owner-protected")?;
        let key = SigningKey::from_bytes(&[11; 32]);
        let payer = key.verifying_key().to_bytes();
        let wallet = bs58::encode(payer).into_string();
        let signer = root.path().join("synthetic-key.json");
        std::fs::write(
            &signer,
            serde_json::to_vec(&key.to_keypair_bytes().to_vec())?,
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let mut config = base::config(&wallet, &url, &root.path().join("stop").to_string_lossy());
        config.execution_signer_keypair_path = signer.to_string_lossy().into_owned();
        config.tiny_experiment.policy_mode = TinyPolicyMode::ProtectedNativeCapital;
        config.pretrade_min_sol_reserve = 0.985;
        config.quote_canary_buy_slippage_bps = 50;
        config.quote_canary_slippage_bps = 50;
        let owner = config.owner_technical_buy.as_mut().unwrap();
        owner.mint = wire::USDC.into();
        owner.max_slippage_bps = 50;
        owner.min_reserve_lamports = 985_000_000;
        copybot_config::validate_owner_technical_buy(&config)?;
        let mut store = SqliteStore::open(root.path().join("state.db"))?;
        store.run_migrations(
            &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../migrations"),
        )?;
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let kill = config.canary_kill_switch_path.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await?;
                    ensure!(n > 0 && bytes.len() < 1 << 20, "fixture_request_size");
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(at) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..at]);
                        let length = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= at + 4 + length {
                            break (at, length);
                        }
                    }
                };
                let header = String::from_utf8_lossy(&bytes[..end]);
                let r = if header.starts_with("GET ") {
                    let path = header.split_whitespace().nth(1).unwrap();
                    let url = reqwest::Url::parse(&format!("http://localhost{path}"))?;
                    let params: std::collections::HashMap<_, _> =
                        url.query_pairs().into_owned().collect();
                    json!({"method":"quote","params":params})
                } else {
                    serde_json::from_slice::<Value>(&bytes[end + 4..end + 4 + length])?
                };
                let response = response(&r, payer, &kill, &mut shared.lock().unwrap())?;
                let body = response.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
            }
        });
        Ok(Self {
            root,
            store,
            config,
            state,
            task,
        })
    }
    pub async fn tick(&self) -> Result<crate::execution_canary::ExecutionCanaryTickSummary> {
        crate::execution_canary::ExecutionCanaryRunner::new(self.config.clone())
            .for_ingestion(
                &base::ingestion(),
                &self.root.path().join("state.db").to_string_lossy(),
            )?
            .process_tick(&self.store, chrono::Utc::now())
            .await
    }
    pub fn order_id(&self) -> String {
        owner_technical_buy_order_id("owner-test-intent")
    }
    pub fn calls(&self, method: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|v| *v == method)
            .count()
    }
    pub fn sql(&self) -> Result<rusqlite::Connection> {
        Ok(rusqlite::Connection::open(
            self.root.path().join("state.db"),
        )?)
    }
}

fn bundle(payer: [u8; 32], mode: &str) -> Result<Value> {
    let mut all = wire::instructions(payer)?;
    let index = all.len() - 2;
    if mode == "wrong_input" {
        all[index].data[16..24].copy_from_slice(&10_000_001u64.to_le_bytes());
    }
    if mode == "wrong_destination" {
        all[index].accounts[3].pubkey = [42; 32];
    }
    if mode == "wrong_mint" {
        all[index].accounts[5].pubkey = [42; 32];
    }
    if mode == "wrong_slippage" {
        all[index].data[32..34].copy_from_slice(&51u16.to_le_bytes());
    }
    if mode == "trailing_bytes" {
        all[index].data.push(0);
    }
    let encode = |ix: &crate::execution_solana_tx::SolanaInstruction| {
        json!({
        "programId":bs58::encode(ix.program_id).into_string(),
        "accounts":ix.accounts.iter().map(|a| json!({"pubkey":bs58::encode(a.pubkey).into_string(),
            "isSigner":a.is_signer,"isWritable":a.is_writable})).collect::<Vec<_>>(),
        "data":STANDARD.encode(&ix.data)})
    };
    Ok(
        json!({"tokenLedgerInstruction":null,"computeBudgetInstructions":all[..2].iter().map(encode).collect::<Vec<_>>(),
        "setupInstructions":all[2..index].iter().map(encode).collect::<Vec<_>>(),
        "swapInstruction":encode(&all[index]),"cleanupInstruction":encode(&all[index+1]),
        "otherInstructions":[],"addressLookupTableAddresses":[],"simulationError":null,
        "blockhashWithMetadata":{"blockhash":vec![9;32],"lastValidBlockHeight":1,
        "fetchedAt":{"secs_since_epoch":1,"nanos_since_epoch":0}}}),
    )
}
fn response(r: &Value, payer: [u8; 32], kill: &str, s: &mut State) -> Result<Value> {
    let method = r["method"]
        .as_str()
        .unwrap_or(if r.get("quoteResponse").is_some() {
            "instructions"
        } else {
            "unknown"
        });
    s.calls.push(method.into());
    if method == "quote" {
        ensure!(
            r["params"]["onlyDirectRoutes"] == "true" && r["params"]["dexes"] == "Raydium",
            "fixture_direct_route"
        );
        return Ok(wire::quote());
    }
    if method == "instructions" {
        ensure!(
            r["dynamicSlippage"] == false && r["useSharedAccounts"] == false,
            "fixture_closed_instruction_request"
        );
        return bundle(payer, s.mode);
    }
    let wallet = bs58::encode(payer).into_string();
    let result = match method {
        "getGenesisHash" => json!(base::GENESIS),
        "getAccountInfo" => {
            let mut mint = [0u8; 82];
            mint[44] = 6;
            mint[45] = 1;
            json!({"context":{"slot":120},"value":{"owner":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "executable":false,"data":[STANDARD.encode(mint),"base64"]}})
        }
        "getMultipleAccounts" => {
            if s.mode == "kill_on_anchor" {
                std::fs::write(kill, "stop")?;
            }
            json!({"context":{"slot":120},"value":r["params"][0].as_array().unwrap().iter().map(|k|
                if k.as_str()==Some(&wallet) { json!({"lamports":1_000_000_000u64,
                    "owner":"11111111111111111111111111111111","executable":false,"data":["","base64"]}) }
                else { Value::Null }).collect::<Vec<_>>()})
        }
        "getFeeForMessage" => json!({"context":{"slot":120},"value":19000}),
        "getMinimumBalanceForRentExemption" => json!(2_039_280),
        "simulateTransaction" => {
            s.simulated = r["params"][0].as_str().map(str::to_owned);
            json!({"context":{"slot":120},"value":{"err":null}})
        }
        "sendTransaction" => {
            s.sent = r["params"][0].as_str().map(str::to_owned);
            let bytes = STANDARD.decode(s.sent.as_ref().unwrap())?;
            json!(if s.mode == "unknown" {
                "ambiguous-signature".into()
            } else {
                bs58::encode(&bytes[1..65]).into_string()
            })
        }
        "getSignatureStatuses" if s.mode == "unknown" => json!({"value":[null]}),
        "getSignatureStatuses" => {
            json!({"value":[{"err":null,"slot":120,"confirmationStatus":"confirmed"}]})
        }
        "getTransaction" if s.mode == "unknown" => Value::Null,
        "getTransaction" => {
            let signature = r["params"][0].as_str().unwrap();
            let mint =
                crate::execution_pumpswap_accounts::parse_pubkey(wire::USDC, "fixture_mint")?;
            let destination = crate::execution_pumpswap_accounts::associated_token_address(
                &payer,
                &mint,
                &crate::execution_pumpswap_accounts::token_program_id(),
            );
            receipt(s.sent.as_ref().unwrap(), signature, &wallet, destination)?
        }
        _ => anyhow::bail!("fixture_unexpected_method {method}"),
    };
    Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
}

fn receipt(payload: &str, signature: &str, wallet: &str, destination: [u8;32]) -> Result<Value> {
    let message = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let keys = &message.binding.accounts;
    let index = keys.iter().position(|k| k.pubkey == destination).unwrap();
    let ata = message.instructions.iter().position(|ix|
        ix.program.pubkey == crate::execution_pumpswap_accounts::associated_token_program_id()
        && ix.accounts[1].pubkey == destination).unwrap();
    let mut pre = vec![0u64;keys.len()]; pre[0] = 1_000_000_000;
    let mut post = pre.clone(); post[0] = 987_941_720; post[index] = 2_039_280;
    let token = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    Ok(json!({"slot":120,"transaction":{"signatures":[signature],"message":{
        "accountKeys":keys.iter().map(|k|json!({"pubkey":bs58::encode(k.pubkey).into_string(),
            "signer":k.is_signer,"writable":k.is_writable})).collect::<Vec<_>>(),
        "instructions":message.instructions.iter().map(|ix|json!({
            "programId":bs58::encode(ix.program.pubkey).into_string(),
            "accounts":ix.accounts.iter().map(|a|bs58::encode(a.pubkey).into_string()).collect::<Vec<_>>(),
            "data":bs58::encode(&ix.data).into_string()})).collect::<Vec<_>>() }},
        "meta":{"err":null,"fee":19000,"preBalances":pre,"postBalances":post,
            "preTokenBalances":[],"postTokenBalances":[{"accountIndex":index,"owner":wallet,
                "mint":wire::USDC,"programId":token,"uiTokenAmount":{"amount":wire::OUT.to_string(),"decimals":6}}],
            "innerInstructions":[{"index":ata,"instructions":[{"programId":token,
                "parsed":{"type":"initializeAccount3","info":{
                    "account":bs58::encode(destination).into_string(),"mint":wire::USDC,"owner":wallet}}}]}]}}))
}

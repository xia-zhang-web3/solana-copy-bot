//! Synthetic signed Raydium USDC exit and strictly local Jupiter/RPC replies.
use crate::execution_pumpswap_accounts::*;
use crate::execution_solana_tx::{PubkeyBytes, SolanaAccountMeta as A, SolanaInstruction};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(super) const USDC: &str = copybot_storage_core::OWNER_EXIT_MINT;
pub(super) const SOL: &str = crate::execution_quote_canary_helpers::SOL_MINT;
pub(super) const OUT: u64 = 12_000_000;
const JUPITER: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const RAYDIUM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

pub(super) fn key() -> SigningKey {
    SigningKey::from_bytes(&[11; 32])
}
pub(super) fn wallet() -> String {
    bs58::encode(key().verifying_key().to_bytes()).into_string()
}
pub(super) fn quote() -> Value {
    json!({"inputMint":USDC,"outputMint":SOL,"inAmount":"1167085",
        "outAmount":OUT.to_string(),"otherAmountThreshold":"11940000",
        "swapMode":"ExactIn","slippageBps":50,"instructionVersion":"V1",
        "platformFee":null,"priceImpactPct":"0.01",
        "routePlan":[{"percent":100,"swapInfo":{"label":"Raydium",
            "ammKey":bs58::encode([61;32]).into_string(),"inputMint":USDC,
            "outputMint":SOL,"inAmount":"1167085","outAmount":OUT.to_string()}}]})
}
pub(super) fn instructions(wallet: PubkeyBytes) -> Result<Vec<SolanaInstruction>> {
    let token = token_program_id();
    let mint = parse_pubkey(USDC, "exit_usdc")?;
    let source = associated_token_address(&wallet, &mint, &token);
    let destination = associated_token_address(&wallet, &wsol_mint(), &token);
    let jupiter = parse_pubkey(JUPITER, "exit_jupiter")?;
    let mut out = super::priority_fee_fixture::budget(200_000, 0);
    out.push(SolanaInstruction {
        program_id: associated_token_program_id(),
        accounts: vec![
            A::signer_writable(wallet),
            A::writable(destination),
            A::signer_writable(wallet),
            A::readonly(wsol_mint()),
            A::readonly(system_program_id()),
            A::readonly(token),
        ],
        data: vec![1],
    });
    let mut accounts = vec![
        A::readonly(token),
        A::signer_writable(wallet),
        A::writable(source),
        A::writable(destination),
        A::writable(destination),
        A::readonly(wsol_mint()),
        A::readonly(jupiter),
        A::readonly(pda(&[b"__event_authority"], &jupiter)),
        A::readonly(jupiter),
        A::readonly(parse_pubkey(RAYDIUM, "exit_dex")?),
        A::readonly(token),
        A::writable([61; 32]),
        A::readonly([62; 32]),
        A::writable([63; 32]),
        A::writable([64; 32]),
        A::writable(source),
        A::writable(destination),
        A::signer_writable(wallet),
    ];
    let mut data = vec![229, 23, 203, 151, 122, 227, 173, 42];
    data.extend(1_u32.to_le_bytes());
    data.extend([105, 100, 0, 1]);
    data.extend(1_167_085_u64.to_le_bytes());
    data.extend(OUT.to_le_bytes());
    data.extend(50_u16.to_le_bytes());
    data.push(0);
    out.push(SolanaInstruction {
        program_id: jupiter,
        accounts: std::mem::take(&mut accounts),
        data,
    });
    out.push(SolanaInstruction {
        program_id: token,
        accounts: vec![
            A::writable(destination),
            A::signer_writable(wallet),
            A::signer_writable(wallet),
        ],
        data: vec![9],
    });
    Ok(out)
}
pub(super) fn signed_payload(floor: u64) -> Result<(String, String)> {
    let signer = key();
    let wallet = signer.verifying_key().to_bytes();
    let floor = crate::execution_native_floor::prepare_final_native_floor(
        wallet,
        [9; 32],
        &instructions(wallet)?,
        floor,
    )?;
    let mut raw = STANDARD.decode(floor.payload())?;
    let signature = signer.sign(&raw[65..]);
    raw[1..65].copy_from_slice(&signature.to_bytes());
    Ok((
        STANDARD.encode(raw),
        bs58::encode(signature.to_bytes()).into_string(),
    ))
}

#[derive(Default)]
pub(super) struct State {
    pub calls: Vec<String>,
    pub mode: &'static str,
    pub sent: Option<String>,
}
pub(super) struct Server {
    pub url: String,
    pub state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<Result<()>>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    pub(super) async fn start() -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await?;
                let mut bytes = Vec::new();
                let (end, len) = loop {
                    let mut chunk = [0u8; 4096];
                    let n = socket.read(&mut chunk).await?;
                    ensure!(n > 0 && bytes.len() < 1 << 20, "exit_fixture_request_size");
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(at) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let header = String::from_utf8_lossy(&bytes[..at]);
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .and_then(|v| v.trim().parse().ok())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= at + 4 + len {
                            break (at, len);
                        }
                    }
                };
                let header = String::from_utf8_lossy(&bytes[..end]);
                let request = if header.starts_with("GET ") {
                    let path = header.split_whitespace().nth(1).unwrap();
                    let url = reqwest::Url::parse(&format!("http://localhost{path}"))?;
                    let params: std::collections::HashMap<_, _> =
                        url.query_pairs().into_owned().collect();
                    json!({"method":"quote","params":params})
                } else {
                    serde_json::from_slice::<Value>(&bytes[end + 4..end + 4 + len])?
                };
                let reply = {
                    let mut guard = shared.lock().unwrap();
                    response(&request, &mut guard)?
                };
                let body = reply.to_string();
                socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await?;
            }
        });
        Ok(Self { url, state, task })
    }
    pub(super) fn calls(&self, method: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|x| x.as_str() == method)
            .count()
    }
}
fn response(r: &Value, s: &mut State) -> Result<Value> {
    let method = r["method"]
        .as_str()
        .unwrap_or(if r.get("quoteResponse").is_some() {
            "instructions"
        } else {
            "unknown"
        });
    s.calls.push(method.into());
    if method == "quote" {
        return Ok(quote());
    }
    if method == "instructions" {
        return if s.mode == "omit-price" { bundle_without_price() } else { bundle() };
    }
    let wallet = wallet();
    let result = match method {
        "getGenesisHash" => json!(super::owner_buy_fixture::GENESIS),
        "getAccountInfo" => {
            let address = r["params"][0].as_str().unwrap_or_default();
            if address == USDC {
                let mut mint = [0u8; 82];
                mint[44] = 6;
                mint[45] = 1;
                json!({"context":{"slot":120},"value":{"owner":TOKEN,"executable":false,
                    "data":[STANDARD.encode(mint),"base64"]}})
            } else {
                let wallet_key = key().verifying_key().to_bytes();
                let usdc = parse_pubkey(USDC, "fixture_usdc")?;
                let ata = associated_token_address(&wallet_key, &usdc, &token_program_id());
                let value = if address == bs58::encode(ata).into_string() {
                    let mut data = [0u8; 165];
                    data[..32].copy_from_slice(&usdc);
                    data[32..64].copy_from_slice(&wallet_key);
                    data[64..72].copy_from_slice(&1_167_085_u64.to_le_bytes());
                    json!({"owner":TOKEN,"executable":false,"data":[STANDARD.encode(data),"base64"]})
                } else {
                    Value::Null
                };
                json!({"context":{"slot":120},"value":value})
            }
        }
        "getMultipleAccounts" => json!({"context":{"slot":120},"value":r["params"][0]
            .as_array().unwrap().iter().map(|key|if key.as_str()==Some(&wallet) {
                json!({"lamports":1_000_000_000u64,"owner":"11111111111111111111111111111111",
                    "executable":false,"data":["","base64"]})
            } else {Value::Null}).collect::<Vec<_>>()}),
        "getBalance" => json!({"context":{"slot":120},"value":1_000_000_000u64}),
        "getFeeForMessage" => json!({"context":{"slot":120},"value":5000}),
        "getMinimumBalanceForRentExemption" => json!(2_039_280),
        "simulateTransaction" => json!({"context":{"slot":120},"value":{"err":null}}),
        "sendTransaction" => {
            s.sent = r["params"][0].as_str().map(str::to_owned);
            if s.mode == "unknown" {
                json!("unrelated-signature")
            } else {
                let raw = STANDARD.decode(s.sent.as_ref().unwrap())?;
                json!(bs58::encode(&raw[1..65]).into_string())
            }
        }
        "getSignatureStatuses" if s.mode == "unknown" => json!({"value":[null]}),
        "getSignatureStatuses" => json!({"value":[{"err":null,"slot":120,
            "confirmationStatus":"finalized"}]}),
        "getTransaction" if s.mode == "unknown" => Value::Null,
        "getTransaction" => receipt(
            s.sent.as_deref().unwrap(),
            r["params"][0].as_str().unwrap(),
            &wallet,
        )?,
        _ => anyhow::bail!("exit_fixture_unexpected_method {method}"),
    };
    Ok(json!({"jsonrpc":"2.0","id":r["id"],"result":result}))
}
pub(super) fn bundle() -> Result<Value> {
    let all = instructions(key().verifying_key().to_bytes())?;
    let encode = |ix: &SolanaInstruction| {
        json!({"programId":bs58::encode(ix.program_id).into_string(),
        "accounts":ix.accounts.iter().map(|a|json!({"pubkey":bs58::encode(a.pubkey).into_string(),
            "isSigner":a.is_signer,"isWritable":a.is_writable})).collect::<Vec<_>>(),
        "data":STANDARD.encode(&ix.data)})
    };
    Ok(json!({"tokenLedgerInstruction":null,
        "computeBudgetInstructions":all[..2].iter().map(encode).collect::<Vec<_>>(),
        "setupInstructions":all[2..3].iter().map(encode).collect::<Vec<_>>(),
        "swapInstruction":encode(&all[3]),"cleanupInstruction":encode(&all[4]),
        "otherInstructions":[],"addressLookupTableAddresses":[],
        "simulationError":null,"blockhashWithMetadata":{"blockhash":vec![9;32],
            "lastValidBlockHeight":1,"fetchedAt":{"secs_since_epoch":1,"nanos_since_epoch":0}}}))
}
pub(super) fn bundle_without_price() -> Result<Value> {
    let mut value = bundle()?;
    value["computeBudgetInstructions"].as_array_mut().unwrap().remove(1);
    Ok(value)
}
fn receipt(payload: &str, signature: &str, wallet: &str) -> Result<Value> {
    let message = crate::execution_transaction_wire::decode_message(payload, |_| Ok(()))?;
    let keys = &message.binding.accounts;
    let payer = key().verifying_key().to_bytes();
    let mint = parse_pubkey(USDC, "fixture_usdc")?;
    let source = associated_token_address(&payer, &mint, &token_program_id());
    let wsol = associated_token_address(&payer, &wsol_mint(), &token_program_id());
    let index = keys
        .iter()
        .position(|k| k.pubkey == source)
        .ok_or_else(|| anyhow::anyhow!("source account missing"))?;
    let jupiter = parse_pubkey(JUPITER, "fixture_jupiter")?;
    let route = message.instructions.iter().position(|ix| ix.program.pubkey
        == jupiter).ok_or_else(||
            anyhow::anyhow!("route instruction missing"))?;
    let mut pre = vec![0u64; keys.len()];
    pre[0] = 1_000_000_000;
    pre[index] = 1_488_440;
    let mut post = pre.clone();
    post[0] = 1_011_995_000;
    Ok(
        json!({"slot":120,"transaction":{"signatures":[signature],"message":{
        "accountKeys":keys.iter().map(|k|json!({"pubkey":bs58::encode(k.pubkey).into_string(),
            "signer":k.is_signer,"writable":k.is_writable})).collect::<Vec<_>>(),
        "instructions":message.instructions.iter().map(|ix|json!({
            "programId":bs58::encode(ix.program.pubkey).into_string(),
            "accounts":ix.accounts.iter().map(|a|bs58::encode(a.pubkey).into_string()).collect::<Vec<_>>(),
            "data":bs58::encode(&ix.data).into_string()})).collect::<Vec<_>>() }},
        "meta":{"err":null,"fee":5000,"preBalances":pre,"postBalances":post,
            "preTokenBalances":[{"accountIndex":index,"owner":wallet,"mint":USDC,
                "programId":TOKEN,"uiTokenAmount":{"amount":"1167085","decimals":6}}],
            "postTokenBalances":[{"accountIndex":index,"owner":wallet,"mint":USDC,
                "programId":TOKEN,"uiTokenAmount":{"amount":"0","decimals":6}}],
            "innerInstructions":[{"index":route,"instructions":[
                {"programId":TOKEN,"parsed":{"type":"initializeAccount3","info":{
                    "account":bs58::encode(wsol).into_string(),"mint":SOL,"owner":wallet}}},
                {"programId":TOKEN,"parsed":{"type":"transfer","info":{
                    "source":bs58::encode([63;32]).into_string(),
                    "destination":bs58::encode(wsol).into_string(),"amount":OUT.to_string()}}},
                {"programId":TOKEN,"parsed":{"type":"closeAccount","info":{
                    "account":bs58::encode(wsol).into_string(),"destination":wallet,
                    "owner":wallet}}}]}]}}),
    )
}

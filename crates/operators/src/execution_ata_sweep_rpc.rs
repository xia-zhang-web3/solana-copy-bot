use crate::solana_tx::{
    serialize_unsigned_legacy_transaction, PubkeyBytes, SolanaAccountMeta, SolanaInstruction,
};
use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use ed25519_dalek::{Signer, SigningKey};
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::{json, Value};
use std::thread::sleep;
use std::time::{Duration, Instant};

pub(crate) const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub(crate) const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub(crate) const TOKEN_PROGRAMS: [&str; 2] = [SPL_TOKEN_PROGRAM, TOKEN_2022_PROGRAM];

const CLOSE_ACCOUNT_DISCRIMINATOR: u8 = 9;
const SIGNATURE_BYTES: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SweepCandidate {
    pub token_account: String,
    pub mint: String,
    pub program_id: String,
    pub lamports: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SweepBatchReport {
    pub token_accounts: Vec<String>,
    pub lamports: u64,
    pub simulation_status: String,
    pub simulation_error: Option<String>,
    pub submitted: bool,
    pub tx_signature: Option<String>,
    pub confirmation_status: Option<String>,
    pub error: Option<String>,
}

pub(crate) struct SweepSigner {
    public_key: PubkeyBytes,
    signing_key: SigningKey,
}

impl SweepSigner {
    pub(crate) fn from_keypair_bytes(bytes: &[u8], expected_pubkey: &str) -> Result<Self> {
        if bytes.len() != 64 {
            anyhow::bail!("signer keypair must contain 64 bytes");
        }
        let mut secret = [0_u8; 32];
        secret.copy_from_slice(&bytes[..32]);
        let mut public_key = [0_u8; 32];
        public_key.copy_from_slice(&bytes[32..]);
        let signing_key = SigningKey::from_bytes(&secret);
        if signing_key.verifying_key().to_bytes() != public_key {
            anyhow::bail!("signer keypair public key does not match secret key");
        }
        let actual = bs58::encode(public_key).into_string();
        if actual != expected_pubkey.trim() {
            anyhow::bail!("signer pubkey mismatch: actual={actual} expected={expected_pubkey}");
        }
        Ok(Self {
            public_key,
            signing_key,
        })
    }

    fn sign_transaction(&self, mut transaction: Vec<u8>) -> Result<SignedTransaction> {
        let message_start = legacy_message_start(&transaction)?;
        let first_key = legacy_first_account_key(&transaction[message_start..])?;
        if first_key != self.public_key {
            anyhow::bail!("transaction first signer does not match sweep signer");
        }
        let signature = self.signing_key.sign(&transaction[message_start..]);
        transaction[1..1 + SIGNATURE_BYTES].copy_from_slice(&signature.to_bytes());
        Ok(SignedTransaction {
            base64: BASE64_STANDARD.encode(transaction),
            signature: bs58::encode(signature.to_bytes()).into_string(),
        })
    }
}

struct SignedTransaction {
    base64: String,
    signature: String,
}

pub(crate) fn decode_pubkey(value: &str) -> Result<PubkeyBytes> {
    let bytes = bs58::decode(value.trim())
        .into_vec()
        .with_context(|| format!("invalid pubkey: {value}"))?;
    if bytes.len() != 32 {
        anyhow::bail!("pubkey must decode to 32 bytes: {value}");
    }
    let mut out = [0_u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub(crate) fn close_account_instruction(
    candidate: &SweepCandidate,
    owner: PubkeyBytes,
) -> Result<SolanaInstruction> {
    Ok(SolanaInstruction {
        program_id: decode_pubkey(&candidate.program_id)?,
        accounts: vec![
            SolanaAccountMeta::writable(decode_pubkey(&candidate.token_account)?),
            SolanaAccountMeta::writable(owner),
            SolanaAccountMeta::signer(owner),
        ],
        data: vec![CLOSE_ACCOUNT_DISCRIMINATOR],
    })
}

pub(crate) fn fetch_zero_token_accounts(
    client: &Client,
    rpc_url: &str,
    owner: &str,
) -> Result<Vec<SweepCandidate>> {
    let mut candidates = Vec::new();
    for program_id in TOKEN_PROGRAMS {
        let result = rpc_call(
            client,
            rpc_url,
            "getTokenAccountsByOwner",
            json!([
                owner,
                {"programId": program_id},
                {"encoding": "jsonParsed", "commitment": "confirmed"}
            ]),
        )?;
        let accounts = result
            .get("value")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("getTokenAccountsByOwner missing result.value"))?;
        for account in accounts {
            if let Some(candidate) = parse_zero_token_account(account, owner, program_id) {
                candidates.push(candidate);
            }
        }
    }
    candidates.sort_by(|left, right| left.token_account.cmp(&right.token_account));
    Ok(candidates)
}

pub(crate) fn execute_sweep_batch(
    client: &Client,
    rpc_url: &str,
    signer: &SweepSigner,
    batch: &[SweepCandidate],
    commit: bool,
) -> SweepBatchReport {
    let mut report = empty_batch_report(batch);
    let result = build_and_simulate(client, rpc_url, signer, batch).and_then(|signed| {
        report.tx_signature = Some(signed.signature.clone());
        simulate_transaction(client, rpc_url, &signed.base64)?;
        report.simulation_status = "passed".to_string();
        if !commit {
            return Ok(None);
        }
        let signature = send_transaction(client, rpc_url, &signed.base64)?;
        report.submitted = true;
        report.tx_signature = Some(signature.clone());
        let status = confirm_transaction(client, rpc_url, &signature)?;
        Ok(Some(status))
    });
    match result {
        Ok(Some(status)) => report.confirmation_status = Some(status),
        Ok(None) => {}
        Err(error) => {
            if report.simulation_status == "not_run" {
                report.simulation_status = "failed".to_string();
                report.simulation_error = Some(error.to_string());
            } else {
                report.error = Some(error.to_string());
            }
        }
    }
    report
}

fn build_and_simulate(
    client: &Client,
    rpc_url: &str,
    signer: &SweepSigner,
    batch: &[SweepCandidate],
) -> Result<SignedTransaction> {
    let owner = signer.public_key;
    let instructions = batch
        .iter()
        .map(|candidate| close_account_instruction(candidate, owner))
        .collect::<Result<Vec<_>>>()?;
    let recent_blockhash = get_latest_blockhash(client, rpc_url)?;
    let unsigned = serialize_unsigned_legacy_transaction(owner, recent_blockhash, &instructions)?;
    signer.sign_transaction(unsigned)
}

fn empty_batch_report(batch: &[SweepCandidate]) -> SweepBatchReport {
    SweepBatchReport {
        token_accounts: batch
            .iter()
            .map(|candidate| candidate.token_account.clone())
            .collect(),
        lamports: batch.iter().map(|candidate| candidate.lamports).sum(),
        simulation_status: "not_run".to_string(),
        simulation_error: None,
        submitted: false,
        tx_signature: None,
        confirmation_status: None,
        error: None,
    }
}

fn parse_zero_token_account(
    account: &Value,
    owner: &str,
    program_id: &str,
) -> Option<SweepCandidate> {
    let info = &account["account"]["data"]["parsed"]["info"];
    let token_amount = &info["tokenAmount"];
    if string_field(info, "owner").as_deref() != Some(owner) {
        return None;
    }
    if string_field(token_amount, "amount").as_deref() != Some("0") {
        return None;
    }
    Some(SweepCandidate {
        token_account: string_field(account, "pubkey")?,
        mint: string_field(info, "mint")?,
        program_id: program_id.to_string(),
        lamports: account["account"]["lamports"].as_u64().unwrap_or(0),
    })
}

fn get_latest_blockhash(client: &Client, rpc_url: &str) -> Result<PubkeyBytes> {
    let result = rpc_call(
        client,
        rpc_url,
        "getLatestBlockhash",
        json!([{"commitment": "confirmed"}]),
    )?;
    let blockhash = result
        .pointer("/value/blockhash")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("getLatestBlockhash missing result.value.blockhash"))?;
    decode_pubkey(blockhash)
}

fn simulate_transaction(client: &Client, rpc_url: &str, signed_base64: &str) -> Result<()> {
    let result = rpc_call(
        client,
        rpc_url,
        "simulateTransaction",
        json!([
            signed_base64,
            {"encoding": "base64", "sigVerify": true, "commitment": "confirmed"}
        ]),
    )?;
    if let Some(error) = result
        .pointer("/value/err")
        .filter(|error| !error.is_null())
    {
        anyhow::bail!("simulateTransaction err={error}");
    }
    Ok(())
}

fn send_transaction(client: &Client, rpc_url: &str, signed_base64: &str) -> Result<String> {
    let result = rpc_call(
        client,
        rpc_url,
        "sendTransaction",
        json!([
            signed_base64,
            {
                "encoding": "base64",
                "skipPreflight": false,
                "preflightCommitment": "confirmed",
                "maxRetries": 0
            }
        ]),
    )?;
    result
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| anyhow!("sendTransaction missing result signature"))
}

fn confirm_transaction(client: &Client, rpc_url: &str, signature: &str) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let result = rpc_call(
            client,
            rpc_url,
            "getSignatureStatuses",
            json!([[signature], {"searchTransactionHistory": true}]),
        )?;
        if let Some(status) = result
            .get("value")
            .and_then(Value::as_array)
            .and_then(|values| values.first())
            .filter(|status| !status.is_null())
        {
            if let Some(error) = status.get("err").filter(|error| !error.is_null()) {
                anyhow::bail!("confirmed transaction error: {error}");
            }
            let confirmation_status = status
                .get("confirmationStatus")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if matches!(confirmation_status, "confirmed" | "finalized") {
                return Ok(confirmation_status.to_string());
            }
        }
        sleep(Duration::from_secs(2));
    }
    Err(anyhow!("confirmation timeout for {signature}"))
}

fn rpc_call(client: &Client, rpc_url: &str, method: &str, params: Value) -> Result<Value> {
    let response = client
        .post(rpc_url)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
        .send()
        .with_context(|| format!("{method} request failed"))?;
    let status = response.status();
    let value: Value = response
        .json()
        .with_context(|| format!("{method} response json failed"))?;
    if !status.is_success() {
        anyhow::bail!("{method} returned HTTP {status}: {value}");
    }
    if let Some(error) = value.get("error") {
        anyhow::bail!("{method} returned RPC error: {error}");
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(|| anyhow!("{method} missing result"))
}

fn legacy_message_start(transaction: &[u8]) -> Result<usize> {
    if transaction.first() != Some(&1) {
        anyhow::bail!("sweep signer expects exactly one signature slot");
    }
    let message_start = 1 + SIGNATURE_BYTES;
    if transaction.len() <= message_start {
        anyhow::bail!("serialized transaction missing message");
    }
    Ok(message_start)
}

fn legacy_first_account_key(message: &[u8]) -> Result<PubkeyBytes> {
    if message.len() < 3 {
        anyhow::bail!("legacy message header truncated");
    }
    let (account_count, prefix_len) = decode_shortvec(&message[3..])?;
    if account_count == 0 {
        anyhow::bail!("legacy message has no account keys");
    }
    let start = 3 + prefix_len;
    let end = start + 32;
    if message.len() < end {
        anyhow::bail!("legacy message first account key truncated");
    }
    let mut out = [0_u8; 32];
    out.copy_from_slice(&message[start..end]);
    Ok(out)
}

fn decode_shortvec(bytes: &[u8]) -> Result<(usize, usize)> {
    let mut value = 0usize;
    let mut shift = 0usize;
    for (index, byte) in bytes.iter().enumerate() {
        value |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        shift += 7;
        if shift >= usize::BITS as usize {
            anyhow::bail!("shortvec overflow");
        }
    }
    Err(anyhow!("shortvec truncated"))
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

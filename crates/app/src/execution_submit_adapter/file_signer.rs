use crate::execution_signing_envelope::{
    validate_serialized_transaction_base64, ExecutionSerializedTransactionPayload,
    ExecutionSignedTransactionPayload,
};
use crate::execution_submit_adapter::{ExecutionSubmitRequest, ExecutionTransactionPlan};
use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use copybot_config::ExecutionConfig;
use ed25519_dalek::{Signer, SigningKey};
use std::fs;
use std::path::Path;

const PUBKEY_BYTES: usize = 32;
const SECRET_KEY_BYTES: usize = 32;
const KEYPAIR_BYTES: usize = 64;
const SIGNATURE_BYTES: usize = 64;
const MESSAGE_HEADER_BYTES: usize = 3;

pub(crate) fn sign_serialized_transaction_from_config(
    config: &ExecutionConfig,
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
    payload: &ExecutionSerializedTransactionPayload,
) -> Result<Option<ExecutionSignedTransactionPayload>> {
    if signer_config_is_empty(config) {
        return Ok(None);
    }
    validate_signer_config(config, request, plan)?;
    let signer = FileTransactionSigner::from_config(config)?;
    signer.sign_payload(payload)
}

fn signer_config_is_empty(config: &ExecutionConfig) -> bool {
    config.execution_signer_pubkey.trim().is_empty()
        && config.execution_signer_keypair_path.trim().is_empty()
}

fn validate_signer_config(
    config: &ExecutionConfig,
    request: &ExecutionSubmitRequest,
    plan: &ExecutionTransactionPlan,
) -> Result<()> {
    if config.execution_signer_pubkey.trim().is_empty()
        || config.execution_signer_keypair_path.trim().is_empty()
    {
        anyhow::bail!("execution signer requires both pubkey and keypair path");
    }
    if request.wallet_pubkey != config.execution_signer_pubkey {
        anyhow::bail!("request wallet_pubkey does not match execution signer pubkey");
    }
    if plan.wallet_pubkey != config.execution_signer_pubkey {
        anyhow::bail!("plan wallet_pubkey does not match execution signer pubkey");
    }
    Ok(())
}

struct FileTransactionSigner {
    signing_key: SigningKey,
    public_key: [u8; PUBKEY_BYTES],
}

impl FileTransactionSigner {
    fn from_config(config: &ExecutionConfig) -> Result<Self> {
        let path = Path::new(config.execution_signer_keypair_path.trim());
        let keypair_bytes = load_solana_keypair_file(path)?;
        let mut secret = [0_u8; SECRET_KEY_BYTES];
        secret.copy_from_slice(&keypair_bytes[..SECRET_KEY_BYTES]);
        let mut public_key = [0_u8; PUBKEY_BYTES];
        public_key.copy_from_slice(&keypair_bytes[SECRET_KEY_BYTES..]);
        let signing_key = SigningKey::from_bytes(&secret);
        let derived_public = signing_key.verifying_key().to_bytes();
        if derived_public != public_key {
            anyhow::bail!("execution signer keypair public key does not match secret key");
        }
        let expected_pubkey = config.execution_signer_pubkey.trim();
        let actual_pubkey = bs58::encode(public_key).into_string();
        if actual_pubkey != expected_pubkey {
            anyhow::bail!(
                "execution signer keypair pubkey mismatch: actual={} expected={}",
                actual_pubkey,
                expected_pubkey
            );
        }
        Ok(Self {
            signing_key,
            public_key,
        })
    }

    fn sign_payload(
        &self,
        payload: &ExecutionSerializedTransactionPayload,
    ) -> Result<Option<ExecutionSignedTransactionPayload>> {
        validate_serialized_transaction_base64(&payload.serialized_transaction_base64)?;
        let mut transaction = BASE64_STANDARD
            .decode(payload.serialized_transaction_base64.trim())
            .context("decode serialized transaction base64")?;
        let layout = transaction_layout(&transaction)?;
        let first_signer = first_message_account_key(&transaction[layout.message_start..])?;
        if first_signer != self.public_key {
            anyhow::bail!("serialized transaction first signer does not match execution signer");
        }
        let signature = self.signing_key.sign(&transaction[layout.message_start..]);
        let first_signature_end = layout.first_signature_start + SIGNATURE_BYTES;
        transaction[layout.first_signature_start..first_signature_end]
            .copy_from_slice(&signature.to_bytes());
        let signed_transaction_base64 = BASE64_STANDARD.encode(transaction);
        let tx_signature_hint = Some(bs58::encode(signature.to_bytes()).into_string());
        Ok(Some(ExecutionSignedTransactionPayload {
            signed_transaction_base64,
            tx_signature_hint,
        }))
    }
}

fn load_solana_keypair_file(path: &Path) -> Result<[u8; KEYPAIR_BYTES]> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("read execution signer keypair file {}", path.display()))?;
    let values: Vec<u8> = serde_json::from_str(&raw)
        .with_context(|| format!("parse execution signer keypair file {}", path.display()))?;
    if values.len() != KEYPAIR_BYTES {
        anyhow::bail!("execution signer keypair file must contain 64 bytes");
    }
    let mut keypair = [0_u8; KEYPAIR_BYTES];
    keypair.copy_from_slice(&values);
    Ok(keypair)
}

struct TransactionLayout {
    first_signature_start: usize,
    message_start: usize,
}

fn transaction_layout(transaction: &[u8]) -> Result<TransactionLayout> {
    let (signature_count, signature_prefix_len) = decode_shortvec(transaction)?;
    if signature_count == 0 {
        anyhow::bail!("serialized transaction must contain at least one signature slot");
    }
    let signature_bytes = signature_count
        .checked_mul(SIGNATURE_BYTES)
        .context("serialized transaction signature count overflow")?;
    let message_start = signature_prefix_len
        .checked_add(signature_bytes)
        .context("serialized transaction message offset overflow")?;
    if transaction.len() <= message_start {
        anyhow::bail!("serialized transaction missing message bytes");
    }
    Ok(TransactionLayout {
        first_signature_start: signature_prefix_len,
        message_start,
    })
}

fn first_message_account_key(message: &[u8]) -> Result<[u8; PUBKEY_BYTES]> {
    let header_start = usize::from(is_versioned_message(message));
    if message.len() < header_start + MESSAGE_HEADER_BYTES {
        anyhow::bail!("serialized transaction message header is truncated");
    }
    let required_signers = message[header_start];
    if required_signers == 0 {
        anyhow::bail!("serialized transaction message has no required signers");
    }
    let account_count_offset = header_start + MESSAGE_HEADER_BYTES;
    let (account_count, account_prefix_len) = decode_shortvec(&message[account_count_offset..])?;
    if account_count == 0 {
        anyhow::bail!("serialized transaction message has no account keys");
    }
    let first_key_start = account_count_offset + account_prefix_len;
    let first_key_end = first_key_start + PUBKEY_BYTES;
    if message.len() < first_key_end {
        anyhow::bail!("serialized transaction first signer pubkey is truncated");
    }
    let mut first_key = [0_u8; PUBKEY_BYTES];
    first_key.copy_from_slice(&message[first_key_start..first_key_end]);
    Ok(first_key)
}

fn is_versioned_message(message: &[u8]) -> bool {
    message.first().is_some_and(|prefix| prefix & 0x80 != 0)
}

fn decode_shortvec(raw: &[u8]) -> Result<(usize, usize)> {
    let mut value = 0usize;
    let mut shift = 0usize;
    for (index, byte) in raw.iter().enumerate() {
        let low_bits = usize::from(byte & 0x7f);
        value |= low_bits
            .checked_shl(shift as u32)
            .context("shortvec shift overflow")?;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        shift += 7;
        if shift > 28 {
            anyhow::bail!("shortvec is too long");
        }
    }
    anyhow::bail!("shortvec is truncated")
}

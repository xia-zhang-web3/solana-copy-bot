use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use copybot_config::ExecutionConfig;

use crate::execution_canary_quote_pnl_gate::TinyExecutionGateCheck;
use crate::execution_signer_adapter_preflight::run_execution_signer_preflight;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SignerKeypairPreflight {
    pub(crate) format_ok: bool,
    pub(crate) public_key: Option<String>,
    pub(crate) format_error: Option<&'static str>,
    pub(crate) permission_ok: bool,
    pub(crate) mode_octal: Option<String>,
}

pub(crate) fn push_signer_guards(
    checks: &mut Vec<TinyExecutionGateCheck>,
    config: &ExecutionConfig,
    runtime_root: Option<&Path>,
) {
    let signer_path = resolve_runtime_path(&config.execution_signer_keypair_path, runtime_root);
    let signer_path_configured = !config.execution_signer_keypair_path.trim().is_empty();
    let keypair = if signer_path_configured && signer_path.is_file() {
        Some(validate_signer_keypair_file(&signer_path))
    } else {
        None
    };
    let signer_preflight = if signer_path_configured && signer_path.is_file() {
        Some(run_execution_signer_preflight(
            &signer_path,
            &config.execution_signer_pubkey,
        ))
    } else {
        None
    };

    push_check(
        checks,
        "execution_signer_pubkey",
        valid_wallet_pubkey(&config.execution_signer_pubkey),
        redact_wallet(&config.execution_signer_pubkey),
        "valid non-system pubkey".to_string(),
        "real submit needs an explicit signer public key",
    );
    push_check(
        checks,
        "execution_signer_matches_canary_wallet",
        signer_matches_canary_wallet(config),
        format!(
            "{} -> {}",
            redact_wallet(&config.execution_signer_pubkey),
            redact_wallet(&config.canary_wallet_pubkey)
        ),
        "same pubkey".to_string(),
        "transaction builder wallet and signer wallet must match",
    );
    push_check(
        checks,
        "execution_signer_keypair_path",
        signer_path_configured,
        redact_path(&config.execution_signer_keypair_path),
        "configured".to_string(),
        "private key path must be explicit before tiny real execution",
    );
    push_check(
        checks,
        "execution_signer_keypair_file",
        signer_path_configured && signer_path.is_file(),
        redact_path(&config.execution_signer_keypair_path),
        "file exists".to_string(),
        "configured signer keypair file must be present before tiny real execution",
    );
    push_check(
        checks,
        "execution_signer_keypair_format",
        keypair.as_ref().is_some_and(|proof| proof.format_ok),
        keypair_format_value(keypair.as_ref()),
        "Solana CLI 64-byte JSON keypair".to_string(),
        "signer keypair must be parseable before tiny real execution",
    );
    push_check(
        checks,
        "execution_signer_keypair_pubkey_match",
        keypair_pubkey_matches_config(keypair.as_ref(), config),
        keypair_pubkey_match_value(keypair.as_ref(), config),
        "keypair public key == execution_signer_pubkey".to_string(),
        "configured signer pubkey must match the keypair file public key",
    );
    push_check(
        checks,
        "execution_signer_keypair_permissions",
        keypair.as_ref().is_some_and(|proof| proof.permission_ok),
        keypair_permissions_value(keypair.as_ref()),
        "owner-only file mode".to_string(),
        "signer keypair file must not be readable by group or others",
    );
    push_check(
        checks,
        "execution_signer_can_sign_preflight",
        signer_preflight
            .as_ref()
            .is_some_and(|proof| proof.signature_verified),
        signer_preflight_value(signer_preflight.as_ref()),
        "local ed25519 signature verified".to_string(),
        "signer must prove local signing before any submit adapter can be enabled",
    );
}

pub(crate) fn validate_signer_keypair_file(path: &Path) -> SignerKeypairPreflight {
    let (permission_ok, mode_octal) = keypair_file_permissions(path);
    let format = parse_solana_cli_keypair_public_key(path);
    SignerKeypairPreflight {
        format_ok: format.public_key.is_some(),
        public_key: format.public_key,
        format_error: format.error,
        permission_ok,
        mode_octal,
    }
}

fn keypair_format_value(proof: Option<&SignerKeypairPreflight>) -> String {
    match proof {
        Some(proof) if proof.format_ok => "solana_cli_json_64_bytes".to_string(),
        Some(proof) => proof
            .format_error
            .unwrap_or("invalid_keypair_format")
            .to_string(),
        None => "file_missing".to_string(),
    }
}

fn keypair_pubkey_matches_config(
    proof: Option<&SignerKeypairPreflight>,
    config: &ExecutionConfig,
) -> bool {
    proof
        .and_then(|proof| proof.public_key.as_deref())
        .is_some_and(|pubkey| pubkey == config.execution_signer_pubkey.trim())
}

fn keypair_pubkey_match_value(
    proof: Option<&SignerKeypairPreflight>,
    config: &ExecutionConfig,
) -> String {
    let keypair_pubkey = proof
        .and_then(|proof| proof.public_key.as_deref())
        .map(redact_wallet)
        .unwrap_or_else(|| "missing".to_string());
    format!(
        "{} -> {}",
        keypair_pubkey,
        redact_wallet(&config.execution_signer_pubkey)
    )
}

fn keypair_permissions_value(proof: Option<&SignerKeypairPreflight>) -> String {
    proof
        .and_then(|proof| proof.mode_octal.clone())
        .unwrap_or_else(|| "missing".to_string())
}

fn signer_preflight_value(
    proof: Option<&crate::execution_signer_adapter_preflight::ExecutionSignerPreflight>,
) -> String {
    match proof {
        Some(proof) if proof.signature_verified => "local_signature_verified".to_string(),
        Some(proof) => proof
            .error
            .unwrap_or("signature_preflight_failed")
            .to_string(),
        None => "file_missing".to_string(),
    }
}

struct ParsedKeypairPublicKey {
    public_key: Option<String>,
    error: Option<&'static str>,
}

fn parse_solana_cli_keypair_public_key(path: &Path) -> ParsedKeypairPublicKey {
    let bytes = match read_solana_cli_keypair_bytes(path) {
        Ok(bytes) => bytes,
        Err(error) => return keypair_error(error),
    };
    ParsedKeypairPublicKey {
        public_key: Some(bs58::encode(&bytes[32..64]).into_string()),
        error: None,
    }
}

pub(crate) fn read_solana_cli_keypair_bytes(path: &Path) -> Result<Vec<u8>, &'static str> {
    let raw = fs::read_to_string(path).map_err(|_| "read_failed")?;
    let bytes = serde_json::from_str::<Vec<u8>>(&raw).map_err(|_| "json_u8_array_required")?;
    if bytes.len() != 64 {
        return Err("solana_cli_keypair_must_have_64_bytes");
    }
    Ok(bytes)
}

fn keypair_error(error: &'static str) -> ParsedKeypairPublicKey {
    ParsedKeypairPublicKey {
        public_key: None,
        error: Some(error),
    }
}

fn signer_matches_canary_wallet(config: &ExecutionConfig) -> bool {
    valid_wallet_pubkey(&config.execution_signer_pubkey)
        && valid_wallet_pubkey(&config.canary_wallet_pubkey)
        && config.execution_signer_pubkey.trim() == config.canary_wallet_pubkey.trim()
}

fn valid_wallet_pubkey(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "11111111111111111111111111111111" {
        return false;
    }
    bs58::decode(trimmed)
        .into_vec()
        .is_ok_and(|bytes| bytes.len() == 32)
}

fn redact_wallet(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.chars().count() <= 12 {
        return trimmed.to_string();
    }
    let prefix: String = trimmed.chars().take(6).collect();
    let suffix: String = trimmed
        .chars()
        .rev()
        .take(6)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{prefix}...{suffix}")
}

fn redact_path(value: &str) -> String {
    if value.trim().is_empty() {
        "missing".to_string()
    } else {
        "[REDACTED_PATH]".to_string()
    }
}

fn resolve_runtime_path(raw: &str, runtime_root: Option<&Path>) -> PathBuf {
    let path = Path::new(raw.trim());
    if path.is_absolute() {
        return path.to_path_buf();
    }
    runtime_root
        .map(|root| root.join(path))
        .unwrap_or_else(|| path.to_path_buf())
}

fn push_check(
    checks: &mut Vec<TinyExecutionGateCheck>,
    name: &str,
    ok: bool,
    value: String,
    threshold: String,
    reason: impl Into<String>,
) {
    checks.push(TinyExecutionGateCheck {
        name: name.to_string(),
        status: if ok { "pass" } else { "block" }.to_string(),
        value,
        threshold,
        reason: reason.into(),
    });
}

#[cfg(unix)]
fn keypair_file_permissions(path: &Path) -> (bool, Option<String>) {
    let Ok(metadata) = fs::metadata(path) else {
        return (false, None);
    };
    let mode = metadata.permissions().mode() & 0o777;
    (mode & 0o077 == 0, Some(format!("0o{mode:03o}")))
}

#[cfg(not(unix))]
fn keypair_file_permissions(_path: &Path) -> (bool, Option<String>) {
    (true, Some("not_checked_non_unix".to_string()))
}

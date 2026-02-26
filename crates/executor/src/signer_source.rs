use anyhow::{anyhow, Context, Result};
use std::fs;
use zeroize::Zeroizing;

use crate::secret_source::secret_file_has_restrictive_permissions;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SignerSource {
    File,
    Kms,
}

impl SignerSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Kms => "kms",
        }
    }
}

pub(crate) fn resolve_signer_source_config(
    source_raw: Option<&str>,
    keypair_file_raw: Option<&str>,
    kms_key_id_raw: Option<&str>,
    signer_pubkey: &str,
) -> Result<SignerSource> {
    let source = source_raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("file")
        .to_ascii_lowercase();
    let keypair_file = keypair_file_raw
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let kms_key_id = kms_key_id_raw
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match source.as_str() {
        "file" => {
            if kms_key_id.is_some() {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be empty when COPYBOT_EXECUTOR_SIGNER_SOURCE=file"
                ));
            }
            let keypair_file = keypair_file.ok_or_else(|| {
                anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be set when COPYBOT_EXECUTOR_SIGNER_SOURCE=file"
                )
            })?;
            validate_signer_keypair_file(keypair_file, signer_pubkey)?;
            Ok(SignerSource::File)
        }
        "kms" => {
            if keypair_file.is_some() {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be empty when COPYBOT_EXECUTOR_SIGNER_SOURCE=kms"
                ));
            }
            let kms_key_id = kms_key_id.ok_or_else(|| {
                anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be set when COPYBOT_EXECUTOR_SIGNER_SOURCE=kms"
                )
            })?;
            if kms_key_id.len() > 256 {
                return Err(anyhow!(
                    "COPYBOT_EXECUTOR_SIGNER_KMS_KEY_ID must be <= 256 chars"
                ));
            }
            Ok(SignerSource::Kms)
        }
        _ => Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_SOURCE must be one of: file,kms"
        )),
    }
}

fn validate_signer_keypair_file(path: &str, signer_pubkey: &str) -> Result<()> {
    let raw = Zeroizing::new(fs::read_to_string(path).with_context(|| {
        format!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE not found/readable path={}",
            path
        )
    })?);
    if !secret_file_has_restrictive_permissions(path).with_context(|| {
        format!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE permission check failed path={}",
            path
        )
    })? {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must use owner-only permissions (0600/0400) path={}",
            path
        ));
    }
    if raw.trim().is_empty() {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE is empty after trim path={}",
            path
        ));
    }
    let keypair_bytes: Zeroizing<Vec<u8>> =
        Zeroizing::new(serde_json::from_str(raw.trim()).with_context(|| {
            format!(
                "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must be JSON array with 64 u8 values path={}",
                path
            )
        })?);
    if keypair_bytes.len() != 64 {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE must contain 64-byte keypair, got {} path={}",
            keypair_bytes.len(),
            path
        ));
    }
    let derived_pubkey = bs58::encode(&keypair_bytes[32..64]).into_string();
    if derived_pubkey != signer_pubkey.trim() {
        return Err(anyhow!(
            "COPYBOT_EXECUTOR_SIGNER_KEYPAIR_FILE pubkey mismatch: file_pubkey={} expected_pubkey={}",
            derived_pubkey,
            signer_pubkey.trim()
        ));
    }
    Ok(())
}

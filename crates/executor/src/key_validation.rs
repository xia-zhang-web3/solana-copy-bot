use anyhow::{anyhow, Result};

pub(crate) fn validate_pubkey_like(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("pubkey must be non-empty"));
    }
    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|error| anyhow!("invalid base58: {}", error))?;
    if decoded.len() != 32 {
        return Err(anyhow!("decoded pubkey length must be 32 bytes"));
    }
    Ok(())
}

pub(crate) fn validate_signature_like(value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("signature must be non-empty"));
    }
    let decoded = bs58::decode(trimmed)
        .into_vec()
        .map_err(|error| anyhow!("invalid base58: {}", error))?;
    if decoded.len() != 64 {
        return Err(anyhow!("decoded signature length must be 64 bytes"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{validate_pubkey_like, validate_signature_like};

    #[test]
    fn key_validation_accepts_pubkey_and_signature_shapes() {
        let pubkey = bs58::encode([1u8; 32]).into_string();
        let signature = bs58::encode([2u8; 64]).into_string();
        assert!(validate_pubkey_like(pubkey.as_str()).is_ok());
        assert!(validate_signature_like(signature.as_str()).is_ok());
    }

    #[test]
    fn key_validation_rejects_wrong_lengths() {
        let short_pubkey = bs58::encode([1u8; 16]).into_string();
        let short_signature = bs58::encode([2u8; 32]).into_string();
        assert!(validate_pubkey_like(short_pubkey.as_str()).is_err());
        assert!(validate_signature_like(short_signature.as_str()).is_err());
    }
}

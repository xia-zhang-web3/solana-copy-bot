use std::path::Path;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};

use crate::execution_signer_keypair_preflight::read_solana_cli_keypair_bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutionSignerPreflight {
    pub(crate) public_key: Option<String>,
    pub(crate) signature_verified: bool,
    pub(crate) error: Option<&'static str>,
}

pub(crate) fn run_execution_signer_preflight(
    path: &Path,
    expected_pubkey: &str,
) -> ExecutionSignerPreflight {
    let signer = match FileExecutionSigner::load_solana_cli_keypair(path) {
        Ok(signer) => signer,
        Err(error) => return signer_preflight_error(error),
    };
    if signer.public_key() != expected_pubkey.trim() {
        return ExecutionSignerPreflight {
            public_key: Some(signer.public_key().to_string()),
            signature_verified: false,
            error: Some("config_public_key_mismatch"),
        };
    }

    let payload = execution_signer_preflight_payload(expected_pubkey);
    let signature = signer.sign_preflight_payload(&payload);
    let signature_verified = signer.verify_preflight_signature(&payload, &signature);
    ExecutionSignerPreflight {
        public_key: Some(signer.public_key().to_string()),
        signature_verified,
        error: (!signature_verified).then_some("signature_verification_failed"),
    }
}

struct FileExecutionSigner {
    signing_key: SigningKey,
    public_key: String,
}

impl FileExecutionSigner {
    fn load_solana_cli_keypair(path: &Path) -> Result<Self, &'static str> {
        let bytes = read_solana_cli_keypair_bytes(path)?;
        let secret_key: [u8; 32] = bytes[..32]
            .try_into()
            .map_err(|_| "solana_cli_keypair_must_have_64_bytes")?;
        let stored_public_key: [u8; 32] = bytes[32..64]
            .try_into()
            .map_err(|_| "solana_cli_keypair_must_have_64_bytes")?;
        let signing_key = SigningKey::from_bytes(&secret_key);
        if signing_key.verifying_key().to_bytes() != stored_public_key {
            return Err("secret_public_key_mismatch");
        }

        Ok(Self {
            signing_key,
            public_key: bs58::encode(stored_public_key).into_string(),
        })
    }

    fn public_key(&self) -> &str {
        &self.public_key
    }

    fn sign_preflight_payload(&self, payload: &[u8]) -> Vec<u8> {
        self.signing_key.sign(payload).to_bytes().to_vec()
    }

    fn verify_preflight_signature(&self, payload: &[u8], signature: &[u8]) -> bool {
        Signature::from_slice(signature).is_ok_and(|signature| {
            self.signing_key
                .verifying_key()
                .verify(payload, &signature)
                .is_ok()
        })
    }
}

fn execution_signer_preflight_payload(expected_pubkey: &str) -> Vec<u8> {
    format!(
        "copybot:tiny-execution-signer-preflight:v1:{}",
        expected_pubkey.trim()
    )
    .into_bytes()
}

fn signer_preflight_error(error: &'static str) -> ExecutionSignerPreflight {
    ExecutionSignerPreflight {
        public_key: None,
        signature_verified: false,
        error: Some(error),
    }
}

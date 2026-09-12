use super::types::*;
use anyhow::{ensure, Result};
use copybot_core_types::{Lamports, SignedLamports};
use std::collections::HashSet;

pub(super) fn validate_value(v: &NativeObservation, numeric: bool, signed: bool) -> Result<()> {
    ensure!(
        v.value.is_some() == (v.coverage == ObservationCoverage::Known),
        "native observation coverage/value mismatch"
    );
    ensure!(
        v.value.is_some() == (v.source != ObservationSource::Unavailable),
        "native observation provenance mismatch"
    );
    if let Some(s) = &v.value {
        ensure!(
            !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control),
            "native observation value bounds"
        );
        if numeric {
            if signed {
                let n = SignedLamports::new(s.parse::<i128>()?);
                ensure!(
                    n.as_i128().to_string() == *s,
                    "native observation noncanonical signed integer"
                );
            } else {
                let n = Lamports::new(s.parse::<u64>()?);
                ensure!(
                    n.as_u64().to_string() == *s,
                    "native observation noncanonical unsigned integer"
                );
            }
        }
    }
    Ok(())
}
impl NativeAccountObservations {
    pub fn validate(&self) -> Result<()> {
        for s in [
            &self.order_id,
            &self.tx_signature,
            &self.wallet_pubkey,
            &self.token,
        ] {
            ensure!(
                !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control),
                "native observation identity bounds"
            );
        }
        ensure!(
            matches!(self.side.as_str(), "buy" | "sell"),
            "native observation side invalid"
        );
        ensure!(
            self.slot.parse::<u64>()?.to_string() == self.slot,
            "native observation slot invalid"
        );
        ensure!(
            self.accounts.len() <= MAX_NATIVE_ACCOUNTS
                && self.instructions.len() <= MAX_NATIVE_INSTRUCTIONS
                && self.reasons.len() <= 16,
            "native observation count limit"
        );
        ensure!(
            self.reasons.iter().all(|r| !r.is_empty()
                && r.len() <= 96
                && r.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')),
            "native observation reason bounds"
        );
        let mut indices = HashSet::new();
        let mut keys = HashSet::new();
        for a in &self.accounts {
            ensure!(
                indices.insert(a.account_index)
                    && keys.insert(&a.pubkey)
                    && !a.pubkey.is_empty()
                    && a.pubkey.len() <= 128,
                "native account duplicate/invalid identity"
            );
            ensure!(
                !a.relevance.is_empty()
                    && a.relevance.len() <= 3
                    && a.relevance.iter().all(|r| matches!(
                        r.as_str(),
                        "target_mint" | "wsol_mint" | "wallet_instruction_link"
                    )),
                "native account relevance invalid"
            );
            validate_value(&a.native_pre, true, false)?;
            validate_value(&a.native_post, true, false)?;
            validate_value(&a.native_delta, true, true)?;
            if let (Some(pre), Some(post), Some(delta)) = (
                &a.native_pre.value,
                &a.native_post.value,
                &a.native_delta.value,
            ) {
                ensure!(
                    i128::from(post.parse::<u64>()?) - i128::from(pre.parse::<u64>()?)
                        == delta.parse::<i128>()?,
                    "native account delta conflict"
                );
            }
            for e in [&a.pre_token, &a.post_token] {
                for v in [&e.mint, &e.token_owner, &e.token_program] {
                    validate_value(v, false, false)?;
                }
                validate_value(&e.raw, true, false)?;
                validate_value(&e.decimals, true, false)?;
                if let Some(d) = &e.decimals.value {
                    d.parse::<u8>()?;
                }
            }
        }
        let mut positions = HashSet::new();
        for i in &self.instructions {
            ensure!(
                positions.insert((i.outer_index, i.inner_index)),
                "native instruction duplicate position"
            );
            validate_value(&i.program_id, false, false)?;
            validate_value(&i.instruction_type, false, false)?;
            validate_value(&i.stack_height, true, false)?;
            ensure!(i.fields.len() <= 16, "native instruction fields bound");
            for (k, v) in &i.fields {
                ensure!(
                    matches!(
                        k.as_str(),
                        "sourceBase"
                            | "sourceOwner"
                            | "source"
                            | "destination"
                            | "account"
                            | "newAccount"
                            | "base"
                            | "owner"
                            | "authority"
                            | "newAuthority"
                            | "authorityType"
                            | "mint"
                            | "wallet"
                            | "tokenProgram"
                            | "lamports"
                            | "amount"
                            | "decimals"
                            | "space"
                    ),
                    "native instruction field unsupported"
                );
                validate_value(
                    v,
                    matches!(k.as_str(), "lamports" | "amount" | "decimals" | "space"),
                    false,
                )?;
            }
        }
        ensure!(
            serde_json::to_vec(self)?.len() <= MAX_NATIVE_OBSERVATION_BYTES,
            "native observation byte limit"
        );
        Ok(())
    }
}

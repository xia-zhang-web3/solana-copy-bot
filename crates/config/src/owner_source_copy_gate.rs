//! Optional SOURCE exit barrier; it grants no authority to execute BOT trades.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};

pub const CONFIRMED_NATIVE_BOT_BUY_V1: &str = "confirmed_native_bot_buy_v1";

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerSourceCopyGateConfig {
    pub policy: String,
    pub bot_state_db_path: String,
    pub bot_wallet_pubkey: String,
    pub bot_run_id: String,
    pub bot_policy_identity: String,
    pub bot_kill_switch_path: String,
}

pub fn validate_owner_source_copy_gate(p: &crate::OwnerTechnicalBuyConfig) -> Result<()> {
    let Some(g) = &p.source_copy_exit_gate else {
        return Ok(());
    };
    ensure!(
        p.policy == crate::OWNER_EXECUTION_ROUNDTRIP_V1 && g.policy == CONFIRMED_NATIVE_BOT_BUY_V1,
        "source_copy_gate_policy"
    );
    ensure!(
        g.bot_wallet_pubkey != p.wallet_pubkey
            && !g.bot_wallet_pubkey.trim().is_empty()
            && g.bot_run_id != p.run_id
            && (1..=128).contains(&g.bot_run_id.len())
            && g.bot_run_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
        "source_copy_gate_bot_identity"
    );
    ensure!(
        g.bot_policy_identity.len() == 64
            && g.bot_policy_identity
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "source_copy_gate_policy_identity"
    );
    ensure!(
        !p.activate || g.bot_policy_identity != "0".repeat(64),
        "source_copy_gate_unmaterialized"
    );
    for path in [&g.bot_state_db_path, &g.bot_kill_switch_path] {
        ensure!(
            std::path::Path::new(path).is_absolute() && path.trim() == path && !path.contains('\0'),
            "source_copy_gate_path"
        );
    }
    ensure!(
        g.bot_state_db_path != g.bot_kill_switch_path,
        "source_copy_gate_path"
    );
    Ok(())
}

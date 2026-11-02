//! Non-default, loopback + synthetic-genesis capability. Never a live grant.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
pub const LOCAL_SESSION_GENESIS: &str = "11111111111111111111111111111111";
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LocalSourceCycle {
    pub cycle_id: String,
    pub buy_signature: String,
    pub sell_signature: String,
    pub buy_message_sha256: String,
    pub sell_message_sha256: String,
    pub acquired_raw: u64,
    pub sold_raw: u64,
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LocalBoundedSession {
    pub policy: String,
    pub session_id: String,
    pub max_cycles: u32,
    pub gross_buy_cap: u64,
    pub fee_cap: u64,
    pub cycles: Vec<LocalSourceCycle>,
}
fn loopback(url: &str) -> bool {
    url.strip_prefix("http://127.0.0.1:")
        .and_then(|s| s.split_once('/'))
        .is_some_and(|(port, path)| port.parse::<u16>().is_ok_and(|v| v > 0) && !path.contains('@'))
}
pub fn validate_local_bounded_session(c: &crate::ExecutionConfig) -> Result<()> {
    let Some(p) = &c.local_bounded_session else {
        return Ok(());
    };
    ensure!(
        cfg!(feature = "local-bounded-session") && c.real_session.is_none(),
        "local_session_feature_required"
    );
    ensure!(
        !c.enabled
            && p.policy == "local_bounded_session_v1"
            && p.max_cycles == 2
            && p.gross_buy_cap == 20_000_000
            && p.fee_cap == 400_000
            && p.cycles.len() == 2,
        "local_session_fixed_model_caps"
    );
    let t = c
        .technical_cohort
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("local_session_cohort"))?;
    let s = c
        .owned_sell_preparation
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("local_session_rpc"))?;
    ensure!(
        t.activate
            && t.run_id == p.session_id
            && c.tiny_experiment.id.as_deref() == Some(&p.session_id)
            && s.genesis_hash == LOCAL_SESSION_GENESIS
            && c.controlled_receipt_lot.is_some()
            && loopback(&s.rpc_url)
            && loopback(&c.submit_adapter_http_url)
            && loopback(&c.quote_canary_base_url)
            && loopback(&c.quote_canary_public_base_url)
            && c.owner_technical_buy.is_none()
            && c.owner_exit.is_none(),
        "local_session_model_isolation"
    );
    let mut signatures = std::collections::HashSet::new();
    let mut ids = std::collections::HashSet::new();
    for cycle in &p.cycles {
        ensure!(
            !cycle.cycle_id.is_empty()
                && cycle.cycle_id.len() <= 96
                && ids.insert(&cycle.cycle_id)
                && cycle.acquired_raw > 0
                && cycle.sold_raw == cycle.acquired_raw,
            "local_session_cycle_identity"
        );
        for sig in [&cycle.buy_signature, &cycle.sell_signature] {
            ensure!(
                (64..=88).contains(&sig.len())
                    && sig.bytes().all(|b| b.is_ascii_alphanumeric())
                    && signatures.insert(sig),
                "local_session_source_signature"
            );
        }
        for hash in [&cycle.buy_message_sha256, &cycle.sell_message_sha256] {
            ensure!(
                hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
                "local_session_source_message"
            );
        }
    }
    Ok(())
}

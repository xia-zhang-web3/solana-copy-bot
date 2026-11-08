use super::{fixture, policy, proof, reuse, test_store};
use anyhow::Result;
use copybot_discovery_v2::load_materialized_discovery_v2_status_for_publish;

fn rejects_saved_layout(mint: bool) -> Result<()> {
    let mut status = fixture();
    let now = status.now;
    let profile = status.wallet_metrics[0]
        .live_valuation
        .as_mut()
        .unwrap()
        .token_2022_positions[0]
        .token_2022_profile
        .as_mut()
        .unwrap();
    if mint {
        profile.mint_account.data = super::profile_fixture::extended_mint(profile);
    } else {
        profile.accounts[0].raw.data.push(2);
    }
    // Keep the old supported subtotal/label: a saved v2 result must be reparsed.
    let encoded = serde_json::to_string(&status)?;
    let saved = serde_json::from_str(&encoded)?;
    assert!(
        reuse(saved, now).is_err(),
        "R1 stale v2 166-byte raw must fail revalidation; mint={mint}"
    );
    let (_dir, store) = test_store()?;
    store.persist_discovery_v2_status_snapshot(
        &status.policy_fingerprint,
        now,
        status.window_start,
        None,
        &encoded,
    )?;
    let (discovery, shadow) = policy();
    assert!(load_materialized_discovery_v2_status_for_publish(
        &store,
        &discovery,
        &shadow,
        &proof::options(now)
    )
    .is_err());
    Ok(())
}

#[test]
fn r1_saved_v2_mint166_cannot_retain_supported_valuation() -> Result<()> {
    rejects_saved_layout(true)
}

#[test]
fn r1_saved_v2_account166_cannot_retain_supported_valuation() -> Result<()> {
    rejects_saved_layout(false)
}

#[path = "common/strict_quote_fixture.rs"]
mod f;
#[path = "common/strict_quote_hooks.rs"]
mod hooks;
use anyhow::Result;
use chrono::Utc;
use f::*;
#[test]
fn strict_quote_real_pre_post_readback_and_commit_failures_never_success() -> Result<()> {
    let _hooks = hooks::Hooks::new();
    for completion in [false, true] {
        for mode in [1, 2, 3] {
            let mut f = fixture()?;
            f.db.reopen()?;
            let now = Utc::now();
            if completion {
                let c = claim(&f, now)?;
                hooks::arm(mode);
                let result = f.db.store.complete_strict_sell_quote(
                    &c,
                    limits(),
                    observation(&c, now),
                    || now,
                );
                hooks::disarm();
                assert!(result.is_err(), "completion mode{mode}");
                let durable = f.db.store.load_strict_sell_quote(ID, limits(), now)?;
                assert_eq!(
                    durable.is_some(),
                    mode == 2,
                    "only postcommit fault leaves durable result, without caller success"
                );
            } else {
                hooks::arm(mode);
                let result =
                    f.db.store
                        .claim_strict_sell_quote(limits(), ENDPOINT, || now);
                hooks::disarm();
                assert!(result.is_err(), "claim mode{mode}");
                assert_eq!(
                    count(&f, "ordered_sell_quote_results")?,
                    i64::from(mode == 2)
                );
            }
        }
    }
    Ok(())
}

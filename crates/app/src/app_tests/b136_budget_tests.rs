use super::{
    b136_config,
    b136_endpoint_tests::{finish, sends},
    b136_fixture::Fixture,
    b136_prior_sell::failed_sell,
    b136_server::Server,
};
use anyhow::Result;
#[tokio::test]
async fn b136_second_slot_transfer_and_third_sell_refusal() -> Result<()> {
    for previous in [1, 2] {
        let f = Fixture::new().await?;
        for n in 0..previous {
            failed_sell(&f, n)?;
        }
        let s = Server::new().await?;
        let c = b136_config::load(&f, &s.url, true)?;
        f.ingress(&c).await?;
        let r = f.runner(&c)?;
        if previous == 1 {
            finish(&f, &r).await?;
            assert_eq!(sends(&s), 1);
            assert_eq!(
                f.db.sql.query_row(
                    "SELECT count(*) FROM execution_tiny_reservations WHERE side='sell'",
                    [],
                    |r| r.get::<_, u64>(0)
                )?,
                2
            );
            assert!(format!("{:#}", failed_sell(&f, 9).unwrap_err()).contains("stopped"));
        } else {
            let error = super::b136_refusal_tests::refusal(&f, &r).await?;
            assert!(error.contains("stopped"), "{error}");
            assert_eq!(sends(&s), 0);
            assert!(f.rows("rpc_owned_sell_handoffs")?.is_empty());
        }
    }
    Ok(())
}
#[tokio::test]
async fn b136_protected_policy_unknown_buy_and_sell_native_floor_exemption() -> Result<()> {
    let f = Fixture::with_protected(true).await?;
    let s = Server::new().await?;
    f.db.sql.execute(
        "UPDATE execution_canary_receipt_facts SET transaction_fee=NULL,fee_coverage='missing'",
        [],
    )?;
    let c = b136_config::load(&f, &s.url, true)?;
    f.ingress(&c).await?;
    let before = f.rows("execution_tiny_native_policy")?;
    finish(&f, &f.runner(&c)?).await?;
    assert_eq!(f.rows("execution_tiny_native_policy")?, before);
    assert!(f.db.sql.query_row("SELECT actual_fee IS NULL AND fee_bound=100000 FROM execution_tiny_reservations WHERE side='buy'",[],|r|r.get::<_,bool>(0))?);
    assert!(s
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|v| v["method"] != "getAccountInfo"));
    assert_eq!(sends(&s), 1);
    Ok(())
}

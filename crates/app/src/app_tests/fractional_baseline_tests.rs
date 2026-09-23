use super::fractional_fixture::Fixture;
#[tokio::test]
async fn fractional_default_full_contract_unchanged() -> anyhow::Result<()> {
    let f = Fixture::new().await?;
    let claim = f.claim()?;
    assert_eq!(
        claim.binding.raw, 1000,
        "default old full-owned contract must not change"
    );
    assert!(claim.binding.fractional.is_none());
    Ok(())
}

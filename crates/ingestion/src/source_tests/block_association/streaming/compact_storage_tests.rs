use super::*;

#[test]
fn cached_block_preserves_late_match_and_duplicate_float_bits() -> anyhow::Result<()> {
    let (mut tx, mut block) = pair(true, true)?;
    let amount = &mut block.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount;
    *amount = -0.0;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    assert!(feed(&mut a, 0, Input::Block(&block)).is_empty());
    assert_eq!(a.block_cache_usage().0, 1);
    assert!(feed(&mut a, 1, Input::Block(&block)).is_empty());
    assert_eq!(
        a.block_cache_usage().0,
        1,
        "exact duplicate remains one block"
    );
    tx_mut(&mut tx)
        .transaction
        .as_mut()
        .unwrap()
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = -0.0;
    let outcome = feed(&mut a, 2, tx_input(&tx));
    assert!(matches!(
        terminal(&outcome[0]).1,
        Resolution::ProviderAsserted { .. }
    ));
    let mut changed = block.clone();
    changed.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount = 0.0;
    let late = feed(&mut a, 3, Input::Block(&changed));
    assert!(matches!(late.as_slice(), [Outcome::Late { .. }]));
    assert_eq!(
        a.block_cache_usage().0,
        2,
        "different float bits remain distinct"
    );
    Ok(())
}

#[test]
fn ordinary_compact_block_preserves_late_association_and_conflict() -> anyhow::Result<()> {
    let (tx, block) = pair(true, true)?;
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    assert!(feed(&mut a, 0, Input::Block(&block)).is_empty());
    let outcome = feed(&mut a, 1, tx_input(&tx));
    assert!(matches!(
        terminal(&outcome[0]).1,
        Resolution::ProviderAsserted { .. }
    ));
    let mut fork = block.clone();
    fork.blockhash = bs58::encode([88; 32]).into_string();
    let late = feed(&mut a, 2, Input::Block(&fork));
    assert!(matches!(late.as_slice(), [Outcome::Late { .. }]));
    Ok(())
}

#[test]
fn compact_cache_distinguishes_nan_payloads_without_normalizing_them() -> anyhow::Result<()> {
    let (_, mut block) = pair(true, true)?;
    let amount = &mut block.transactions[0]
        .meta
        .as_mut()
        .unwrap()
        .pre_token_balances[0]
        .ui_token_amount
        .as_mut()
        .unwrap()
        .ui_amount;
    *amount = f64::from_bits(0x7ff8_0000_0000_0001);
    assert!(!crate::source::yellowstone_block_association::needs_exact_float_storage(&block));
    let source = YellowstoneGrpcSource::new(&config())?;
    let mut a = adapter(&source.runtime_config, limits());
    assert!(feed(&mut a, 0, Input::Block(&block)).is_empty());
    assert!(feed(&mut a, 1, Input::Block(&block)).is_empty());
    assert_eq!(a.block_cache_usage().0, 1);
    let mut changed = block.clone();
    changed.transactions[0].meta.as_mut().unwrap().pre_token_balances[0]
        .ui_token_amount.as_mut().unwrap().ui_amount =
        f64::from_bits(0x7ff8_0000_0000_0002);
    assert!(feed(&mut a, 2, Input::Block(&changed)).is_empty());
    assert_eq!(a.block_cache_usage().0, 2);
    Ok(())
}

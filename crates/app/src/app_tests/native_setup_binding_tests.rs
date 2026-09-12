use super::native_setup_fixture::*;
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};

#[tokio::test]
async fn native_setup_full_requirements_identity_is_rederived_before_interpretation() -> Result<()>
{
    let payload = payload(&direct(true, true, 10_000_000)?)?;
    let original = responses(&payload)?.collect(&payload).await?;
    // Former requirements-field mutations are now compile-negative cases. Keeping a
    // setter/from_parts here to run them would defeat the production boundary.
    assert_eq!(
        interpret(&payload, PEER, &original)
            .unwrap_err()
            .to_string(),
        "native_setup_invalid_requirements"
    );
    let mut bytes = STANDARD.decode(&payload)?;
    bytes[1] = 1;
    assert_eq!(
        interpret(&STANDARD.encode(bytes), WALLET, &original)
            .unwrap_err()
            .to_string(),
        "native_setup_requirements_mismatch"
    );
    let b = super::native_funding_fixture::payload(&direct(false, false, 99)?)?;
    assert_eq!(
        interpret(&b, WALLET, &original).unwrap_err().to_string(),
        "native_setup_requirements_mismatch"
    );
    for invalid in ["payload-secret".to_owned(), "?".repeat(1645)] {
        let error = interpret(&invalid, WALLET, &original)
            .unwrap_err()
            .to_string();
        assert!(error.len() < 80 && !error.contains(&invalid));
    }
    assert_unknown(&interpret(&payload, WALLET, &original)?);
    assert_unknown(&interpret(&payload, WALLET, &original.clone())?);
    Ok(())
}

#[tokio::test]
async fn native_setup_rejects_missing_duplicate_reordered_rows_and_metadata_contract_mismatch(
) -> Result<()> {
    let payload = payload(&direct(true, false, 1)?)?;
    // Row/key reorder and commitment reassignment are compile-negative now. RPC
    // length/floor errors remain real collector rejections, before a facts can exist.
    for case in 0..5 {
        let mut raw = responses(&payload)?;
        match case {
            0 => {
                raw.rows.pop();
            }
            1 => raw.rows.push(raw.rows[0].clone()),
            2 => raw.fee_slot = 49,
            3 => raw.accounts_slot = 49,
            4 => raw.floor = Some(56),
            _ => unreachable!(),
        }
        let error = format!("{:#}", raw.collect(&payload).await.unwrap_err());
        assert!(
            error.contains(if case < 2 {
                "native_rpc_accounts_length"
            } else {
                "native_rpc_slot_below_floor"
            }),
            "{error}"
        );
    }
    for fee in [None, Some(0), Some((1_u64 << 53) + 1), Some(u64::MAX)] {
        let mut raw = responses(&payload)?;
        raw.fee = fee;
        raw.floor = None;
        raw.fee_slot = 0;
        raw.accounts_slot = u64::MAX;
        let valid = raw.collect(&payload).await?;
        let result = interpret(&payload, WALLET, &valid)?;
        assert!(std::ptr::eq(result.facts, &valid));
        assert_eq!(result.facts.fee().value, fee);
        assert_eq!(
            (result.facts.fee().slot, result.facts.accounts().slot),
            (0, u64::MAX)
        );
        assert_eq!(result.facts.commitment(), "confirmed");
        assert_eq!(result.facts.min_context_slot(), None);
        assert!(std::ptr::eq(result.facts.accounts(), valid.accounts()));
        assert!(std::ptr::eq(result.facts.fee(), valid.fee()));
        assert!(std::ptr::eq(result.facts.timing(), valid.timing()));
        assert_unknown(&result);
    }
    Ok(())
}

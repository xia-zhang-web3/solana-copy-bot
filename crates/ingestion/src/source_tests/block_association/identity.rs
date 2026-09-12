use super::*;

#[test]
fn slots_signatures_hashes_and_missing_expected_fail_explicitly() -> Result<()> {
    let (original, block) = pair(false, false)?;
    let mut expected = original.clone();
    tx_mut(&mut expected).transaction = None;
    refused(
        "absent-info",
        &expected,
        &block,
        Refusal::MissingExpectedInfo,
    );
    for len in [0, 63, 65] {
        let mut expected = original.clone();
        tx_mut(&mut expected)
            .transaction
            .as_mut()
            .unwrap()
            .signature
            .resize(len, 1);
        refused(
            &format!("signature-{len}"),
            &expected,
            &block,
            Refusal::InvalidSignatureLength(len),
        );
    }
    for (slot, block_slot, reason) in [
        (0, block.slot, Refusal::ZeroSlot),
        (block.slot, 0, Refusal::ZeroSlot),
        (block.slot + 1, block.slot, Refusal::SlotMismatch),
    ] {
        let mut expected = original.clone();
        let mut block = block.clone();
        tx_mut(&mut expected).slot = slot;
        block.slot = block_slot;
        refused(
            &format!("slots-{slot}-{block_slot}"),
            &expected,
            &block,
            reason,
        );
    }
    for (name, hash) in [
        ("empty", String::new()),
        ("short", "1".repeat(31)),
        ("long", "1".repeat(45)),
        ("alphabet", "0".repeat(32)),
        ("31-bytes", bs58::encode([255; 31]).into_string()),
        ("33-bytes", bs58::encode([0; 33]).into_string()),
        ("padded", format!(" {}", block.blockhash)),
        ("extra-leading-one", format!("1{}", block.blockhash)),
    ] {
        let mut block = block.clone();
        block.blockhash = hash;
        refused(
            &format!("hash-{name}"),
            &original,
            &block,
            Refusal::InvalidBlockhash,
        );
    }
    for bytes in [[0u8; 32], [255u8; 32]] {
        let mut block = block.clone();
        block.blockhash = bs58::encode(bytes).into_string();
        assert!(observe(
            &format!("hash-valid-{}", block.blockhash.len()),
            &original,
            &block
        )
        .is_ok());
    }
    Ok(())
}

#[test]
fn filtered_absence_duplicates_and_unrelated_entries_are_not_identity() -> Result<()> {
    let (expected, original) = pair(false, false)?;
    let target = original.transactions[0].clone();
    let mut other = SubscribeUpdateTransactionInfo {
        signature: vec![7; 64],
        ..Default::default()
    };
    let mut block = original.clone();
    block.transactions.clear();
    refused(
        "empty-filtered",
        &expected,
        &block,
        Refusal::NotFoundInMessage,
    );
    block.transactions.push(other.clone());
    refused(
        "same-slot-other",
        &expected,
        &block,
        Refusal::NotFoundInMessage,
    );
    block.transactions.extend([target.clone(), target.clone()]);
    refused(
        "duplicate-identical",
        &expected,
        &block,
        Refusal::DuplicateSignature,
    );
    block.transactions[1].index += 1;
    refused(
        "duplicate-first-mismatch",
        &expected,
        &block,
        Refusal::DuplicateSignature,
    );
    block.transactions.swap(1, 2);
    refused(
        "duplicate-second-mismatch",
        &expected,
        &block,
        Refusal::DuplicateSignature,
    );
    // Malformed, oversized non-target Info is never decoded or encoded by API.
    other.transaction = Some(yellowstone_grpc_proto::prelude::Transaction {
        signatures: vec![vec![]; MAX_INFO_ITEMS + 1],
        ..Default::default()
    });
    block.transactions = vec![other.clone(), target.clone(), other];
    block.executed_transaction_count = 0;
    assert!(observe(
        "filtered-target-with-unrelated-malformed",
        &expected,
        &block
    )
    .is_ok());
    block.transactions.reverse();
    block.executed_transaction_count = u64::MAX;
    assert!(observe("filtered-count-not-list-length", &expected, &block).is_ok());
    // Only containing blockhash has meaning here; these unrelated fields ignored.
    block.parent_blockhash.clear();
    block.accounts = vec![Default::default(); 2];
    block.rewards = Some(Default::default());
    block.entries = vec![Default::default(); 2];
    assert!(observe("ignore-parent-and-block-extras", &expected, &block).is_ok());
    Ok(())
}

#[test]
fn decoder79_refusals_and_errors_are_preserved_after_exact_match() -> Result<()> {
    for name in [
        "vote",
        "failed",
        "missing_meta",
        "missing_tx",
        "missing_message",
        "missing_keys",
        "empty_signer",
        "unsupported_instruction",
        "no_balances",
        "bad_owned_amount",
    ] {
        let (mut expected, mut block) = pair(false, true)?;
        super::super::damage::apply(&mut expected, name);
        block.transactions[0] = transaction(&expected).transaction.as_ref().unwrap().clone();
        let result = observe(&format!("facts-{name}"), &expected, &block);
        let source = YellowstoneGrpcSource::new(&config())?;
        let c = &source.runtime_config;
        let decoder = decode_yellowstone_swap_facts(
            transaction(&expected),
            &c.interested_program_ids,
            &c.raydium_program_ids,
            &c.pumpswap_program_ids,
        );
        let reason = match decoder.facts {
            Err(e) => Refusal::FactsError(format!("{e:#}")),
            Ok(None) => Refusal::NoCheckedSwapFacts,
            Ok(Some(_)) => panic!("damaged fixture accepted: {name}"),
        };
        assert_eq!(result.unwrap_err(), reason, "{name}");
    }
    Ok(())
}

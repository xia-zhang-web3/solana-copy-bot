//! Synthetic block, exact Agave 4.3.2 / solana-reward-info7 serde literal.
use super::*;
use yellowstone_grpc_proto::prelude::RewardType;

#[test]
fn alpenglow_vat_debit_normalizes_and_matches_exact_grpc_reward_money() {
    let mut raw = raw_block();
    raw["rewards"][0]["rewardType"] = json!("VATDebit");
    raw["rewards"][0]["lamports"] = json!(-800000000i64);
    raw["rewards"][0]["postBalance"] = json!(1234567u64);
    raw["transactions"][0]["meta"]["rewards"][0]["rewardType"] = json!("VATDebit");
    raw["transactions"][0]["meta"]["rewards"][0]["lamports"] = json!(-800000000i64);
    let normalized = block::parse(10, &raw).unwrap();
    let vat = &normalized.rewards.as_ref().unwrap().rewards[0];
    assert_eq!(vat.reward_type, RewardType::VatDebit as i32);
    assert_eq!(vat.lamports, -800000000);
    assert_eq!(vat.post_balance, 1234567);
    assert_eq!(vat.commission, "");
    assert_eq!(vat.commission_bps, "42");
    let transaction_vat = &normalized.transactions[0].meta.as_ref().unwrap().rewards[0];
    assert_eq!(transaction_vat.reward_type, 6);
    assert_eq!(transaction_vat.lamports, -800000000);
    assert_eq!(transaction_vat.post_balance, 55);
    let mut expected_grpc = normalized.clone();
    expected_grpc.bank_id = 42;
    assert!(identity::block_equivalent(&normalized, &expected_grpc));
    expected_grpc.rewards.as_mut().unwrap().rewards[0].lamports += 1;
    assert!(!identity::block_equivalent(&normalized, &expected_grpc));
    expected_grpc = normalized.clone();
    expected_grpc.transactions[0].meta.as_mut().unwrap().rewards[0].lamports += 1;
    assert!(!identity::block_equivalent(&normalized, &expected_grpc));
    expected_grpc = normalized.clone();
    expected_grpc.rewards.as_mut().unwrap().rewards[0].reward_type = RewardType::Fee as i32;
    assert!(!identity::block_equivalent(&normalized, &expected_grpc));
}

#[test]
fn alpenglow_vat_debit_does_not_admit_invented_types_or_invalid_amounts() {
    for transaction_reward in [false, true] {
        for kind in [
            json!("vatDebit"),
            json!("VAT_DEBIT"),
            json!("futureReward"),
            json!(6),
        ] {
            let mut raw = raw_block();
            let reward = if transaction_reward {
                &mut raw["transactions"][0]["meta"]["rewards"][0]
            } else {
                &mut raw["rewards"][0]
            };
            reward["rewardType"] = kind;
            assert!(block::parse(10, &raw).is_err());
        }
        let mut raw = raw_block();
        let reward = if transaction_reward {
            &mut raw["transactions"][0]["meta"]["rewards"][0]
        } else {
            &mut raw["rewards"][0]
        };
        reward["rewardType"] = json!("VATDebit");
        reward["lamports"] = json!(u64::MAX);
        assert!(
            block::parse(10, &raw).is_err(),
            "signed i64 boundary remains enforced"
        );
    }
}

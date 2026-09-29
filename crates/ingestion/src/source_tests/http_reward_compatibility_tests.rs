use super::*;
use yellowstone_grpc_proto::prelude::RewardType;

#[test]
fn confirmed_fee_spelling_preserves_exact_block_and_transaction_money() {
    let mut raw = raw_block();
    raw["rewards"][0]["rewardType"] = json!("Fee");
    raw["rewards"][0]["postBalance"] = json!(u64::MAX);
    raw["transactions"][0]["meta"]["rewards"][0]["rewardType"] = json!("Fee");
    let upper = block::parse(10, &raw).unwrap();
    raw["rewards"][0]["rewardType"] = json!("fee");
    raw["transactions"][0]["meta"]["rewards"][0]["rewardType"] = json!("fee");
    let lower = block::parse(10, &raw).unwrap();
    assert_eq!(upper.encode_to_vec(), lower.encode_to_vec());
    let reward = &upper.rewards.as_ref().unwrap().rewards[0];
    assert_eq!(reward.reward_type, RewardType::Fee as i32);
    assert_eq!(reward.lamports, -7);
    assert_eq!(reward.post_balance, u64::MAX);
    assert_eq!(reward.commission, "");
    assert_eq!(reward.commission_bps, "42");
    let tx = upper.transactions[0].meta.as_ref().unwrap();
    assert_eq!(tx.fee, 5);
    assert_eq!(tx.pre_balances, [100, 0]);
    assert_eq!(tx.post_balances, [95, 0]);
    assert_eq!(tx.rewards[0].lamports, 3);
    assert_eq!(tx.rewards[0].post_balance, 55);
    assert_eq!(tx.rewards[0].commission, "4");
    assert_eq!(tx.rewards[0].commission_bps, "");
}

#[test]
fn existing_known_reward_spellings_and_explicit_null_remain_supported() {
    for (kind, expected) in [
        (json!("fee"), RewardType::Fee),
        (json!("Fee"), RewardType::Fee),
        (json!("rent"), RewardType::Rent),
        (json!("staking"), RewardType::Staking),
        (json!("voting"), RewardType::Voting),
        (json!("deactivatedStake"), RewardType::DeactivatedStake),
        (Value::Null, RewardType::Unspecified),
    ] {
        let mut raw = raw_block();
        raw["rewards"][0]["rewardType"] = kind;
        assert_eq!(
            block::parse(10, &raw).unwrap().rewards.unwrap().rewards[0].reward_type,
            expected as i32
        );
    }
}

#[test]
fn unknown_reward_values_still_refuse_the_entire_block() {
    for kind in [
        json!("FEE"),
        json!("FeE"),
        json!("futureReward"),
        json!(""),
        json!(1),
        json!(true),
        json!({}),
    ] {
        for transaction_reward in [false, true] {
            let mut raw = raw_block();
            let reward = if transaction_reward {
                &mut raw["transactions"][0]["meta"]["rewards"][0]
            } else {
                &mut raw["rewards"][0]
            };
            reward["rewardType"] = kind.clone();
            let error = block::parse(10, &raw).unwrap_err().to_string();
            assert!(
                error == "http_recovery_unknown_reward_type"
                    || error == "http_recovery_invalid_reward_type"
            );
        }
    }
}

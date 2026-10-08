use super::*;

fn raw_amount(r: &Value, before: bool, account: usize) -> u64 {
    let field = if before {
        "preTokenBalances"
    } else {
        "postTokenBalances"
    };
    r["meta"][field]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["accountIndex"] == account)
        .unwrap()["uiTokenAmount"]["amount"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

fn set_raw(r: &mut Value, before: bool, account: usize, raw: u64) {
    let field = if before {
        "preTokenBalances"
    } else {
        "postTokenBalances"
    };
    let row = r["meta"][field]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|t| t["accountIndex"] == account)
        .unwrap();
    let decimals = row["uiTokenAmount"]["decimals"].as_u64().unwrap() as u32;
    let scale = 10u64.pow(decimals);
    row["uiTokenAmount"]["amount"] = raw.to_string().into();
    row["uiTokenAmount"]["uiAmount"] = json!(raw as f64 / scale as f64);
    row["uiTokenAmount"]["uiAmountString"] = format!(
        "{}.{:0width$}",
        raw / scale,
        raw % scale,
        width = decimals as usize
    )
    .into();
}

fn set_data(ix: &mut Value, offset: usize, value: u64) {
    let mut bytes = bs58::decode(ix["data"].as_str().unwrap())
        .into_vec()
        .unwrap();
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    ix["data"] = bs58::encode(bytes).into_string().into();
}

fn checked_index(r: &mut Value, number: usize) -> usize {
    group(r)
        .iter()
        .enumerate()
        .filter(|(_, ix)| {
            bs58::decode(ix["data"].as_str().unwrap())
                .into_vec()
                .unwrap()
                .first()
                == Some(&12)
        })
        .nth(number)
        .unwrap()
        .0
}

#[test]
fn both_sell_starting_wsol_balances_and_close_residual_are_not_proceeds() {
    for wallet in [6, 10] {
        let original = fixture(wallet);
        let quote = accounts(&original)[6];
        let old = raw_amount(&original, true, quote);
        for start in [0, 1, old + 2_000_000] {
            let mut r = original.clone();
            let residual = r["meta"]["preBalances"][quote].as_u64().unwrap() - old;
            set_raw(&mut r, true, quote, start);
            r["meta"]["preBalances"][quote] = json!(start + residual);
            let net = r["meta"]["postBalances"][0].as_u64().unwrap() as i128 + start as i128
                - old as i128;
            r["meta"]["postBalances"][0] = json!(u64::try_from(net).unwrap());
            assert_exact(&r, wallet);
        }
        let mut r = original;
        r["meta"]["preBalances"][quote] =
            json!(r["meta"]["preBalances"][quote].as_u64().unwrap() + 30_000);
        r["meta"]["postBalances"][0] =
            json!(r["meta"]["postBalances"][0].as_u64().unwrap() + 30_000);
        assert_exact(&r, wallet);
    }
}

#[test]
fn transaction_fee_and_signer_cash_delta_do_not_supply_swap_amounts() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        r["meta"]["fee"] = json!(900_000);
        r["meta"]["postBalances"][0] =
            json!(r["meta"]["postBalances"][0].as_u64().unwrap() + 70_000);
        assert_exact(&r, wallet);
    }
}

#[test]
fn buy_tip_and_extra_wsol_funding_do_not_inflate_executed_quote_debit() {
    let mut r = fixture(11);
    let quote = accounts(&r)[6];
    let top = r["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap();
    let funding = top
        .iter_mut()
        .find(|ix| {
            ix["accounts"]
                .as_array()
                .is_some_and(|a| a.len() == 2 && a[1] == quote)
                && bs58::decode(ix["data"].as_str().unwrap())
                    .into_vec()
                    .unwrap()
                    .starts_with(&[2, 0, 0, 0])
        })
        .unwrap();
    set_data(funding, 4, 30_000_000);
    let tip = top
        .iter_mut()
        .rev()
        .find(|ix| {
            bs58::decode(ix["data"].as_str().unwrap())
                .into_vec()
                .unwrap()
                .starts_with(&[2, 0, 0, 0])
        })
        .unwrap();
    set_data(tip, 4, 999_999);
    assert_exact(&r, 11);
}

#[test]
fn minimum_limit_is_a_constraint_and_never_an_executed_amount() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        let parent = parent_index(&r);
        set_data(
            &mut r["transaction"]["message"]["instructions"][parent],
            16,
            1,
        );
        assert_exact(&r, wallet);
        let (_, input, output, _) = expected(wallet);
        let max = if wallet == 11 {
            output
        } else {
            output.max(input)
        };
        set_data(
            &mut r["transaction"]["message"]["instructions"][parent],
            16,
            max + 1,
        );
        assert_refused(&r);
    }
}

#[test]
fn quote_sol_discriminator_direction_is_relative_to_sol() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        let parent = parent_index(&r);
        let mut data = bs58::decode(
            r["transaction"]["message"]["instructions"][parent]["data"]
                .as_str()
                .unwrap(),
        )
        .into_vec()
        .unwrap();
        let mut reversed = if wallet == 11 {
            vec![51, 230, 133, 164, 1, 127, 131, 173]
        } else {
            vec![198, 46, 21, 82, 180, 217, 232, 112]
        };
        reversed.extend(data.drain(8..24));
        if wallet != 11 {
            reversed.push(0);
        }
        r["transaction"]["message"]["instructions"][parent]["data"] =
            bs58::encode(reversed).into_string().into();
        assert_refused(&r);
    }
}

#[test]
fn wrong_wallet_owner_or_target_mint_is_terminal() {
    for wallet in [6, 10, 11, 12] {
        for operand in ["owner", "mint", "programId"] {
            let mut r = fixture(wallet);
            let target = accounts(&r)[5];
            let wrong = match operand {
                "owner" => keys(&r)[accounts(&r)[0]].clone(),
                "mint" => SOL.to_owned(),
                _ => "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb".to_owned(),
            };
            for field in ["preTokenBalances", "postTokenBalances"] {
                for row in r["meta"][field]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .filter(|t| t["accountIndex"] == target)
                {
                    row[operand] = wrong.clone().into();
                }
            }
            assert_refused(&r);
        }
    }
}

#[test]
fn wrong_wsol_owner_mint_or_classic_program_is_terminal() {
    for wallet in [6, 10, 11, 12] {
        let original = fixture(wallet);
        let quote = accounts(&original)[6];
        for field in ["owner", "mint", "programId"] {
            let mut r = original.clone();
            for name in ["preTokenBalances", "postTokenBalances"] {
                for row in r["meta"][name]
                    .as_array_mut()
                    .unwrap()
                    .iter_mut()
                    .filter(|t| t["accountIndex"] == quote)
                {
                    row[field] = match field {
                        "owner" => json!(keys(&original)[accounts(&original)[0]]),
                        "mint" => json!(keys(&original)[accounts(&original)[3]]),
                        _ => json!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"),
                    };
                }
            }
            if wallet == 11 || wallet == 12 {
                let a = accounts(&r);
                let parent = parent_index(&r);
                r["transaction"]["message"]["instructions"][parent]["accounts"][6] = json!(a[5]);
            }
            assert_refused(&r);
        }
    }
}

#[test]
fn duplicate_transfer_or_extra_user_quote_flow_is_ambiguous() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        let pos = checked_index(&mut r, 1);
        let duplicate = group(&mut r)[pos].clone();
        group(&mut r).insert(pos, duplicate);
        assert_refused(&r);
        let mut r = fixture(wallet);
        let pos = checked_index(&mut r, 1);
        let mut extra = group(&mut r)[pos].clone();
        extra["accounts"][2] = json!(accounts(&r)[5]);
        group(&mut r).insert(pos, extra);
        assert_refused(&r);
    }
}

#[test]
fn inconsistent_transfer_raw_or_decimal_is_refused() {
    for wallet in [6, 10, 11, 12] {
        for target in [0, 1] {
            let mut r = fixture(wallet);
            let pos = checked_index(&mut r, target);
            set_data(&mut group(&mut r)[pos], 1, 123);
            assert_refused(&r);
            let mut r = fixture(wallet);
            let pos = checked_index(&mut r, target);
            let mut bytes = bs58::decode(group(&mut r)[pos]["data"].as_str().unwrap())
                .into_vec()
                .unwrap();
            bytes[9] = 18;
            group(&mut r)[pos]["data"] = bs58::encode(bytes).into_string().into();
            assert_refused(&r);
        }
    }
}

#[test]
fn wrong_parent_depth_group_or_duplicate_rows_is_refused() {
    for wallet in [6, 10, 11, 12] {
        for depth in [Value::Null, json!(3)] {
            let mut r = fixture(wallet);
            let pos = checked_index(&mut r, 1);
            group(&mut r)[pos]["stackHeight"] = depth;
            assert_refused(&r);
        }
        let mut r = fixture(wallet);
        let parent = parent_index(&r);
        let g = r["meta"]["innerInstructions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|g| g["index"] == parent)
            .unwrap()
            .clone();
        r["meta"]["innerInstructions"]
            .as_array_mut()
            .unwrap()
            .push(g);
        assert_refused(&r);
        let mut r = fixture(wallet);
        let row = r["meta"]["postTokenBalances"][0].clone();
        r["meta"]["postTokenBalances"]
            .as_array_mut()
            .unwrap()
            .push(row);
        assert_refused(&r);
    }
}

#[test]
fn duplicate_parent_or_wrong_wallet_role_is_refused() {
    for wallet in [6, 10, 11, 12] {
        let mut r = fixture(wallet);
        let parent = parent_index(&r);
        let duplicate = r["transaction"]["message"]["instructions"][parent].clone();
        r["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .push(duplicate);
        assert_refused(&r);
        let mut r = fixture(wallet);
        let parent = parent_index(&r);
        let a = accounts(&r);
        r["transaction"]["message"]["instructions"][parent]["accounts"][1] = json!(a[0]);
        assert_refused(&r);
    }
}

#[test]
fn initial_nonzero_target_row_or_close_owner_cannot_be_erased() {
    for wallet in [6, 10] {
        let mut r = fixture(wallet);
        let a = accounts(&r);
        r["meta"]["preTokenBalances"]
            .as_array_mut()
            .unwrap()
            .retain(|row| row["accountIndex"] != a[5]);
        assert_refused(&r);
        let mut r = fixture(wallet);
        let a = accounts(&r);
        let close = r["transaction"]["message"]["instructions"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|ix| {
                bs58::decode(ix["data"].as_str().unwrap())
                    .into_vec()
                    .unwrap()
                    == [9]
            })
            .unwrap();
        close["accounts"][2] = json!(a[0]);
        assert_refused(&r);
    }
}

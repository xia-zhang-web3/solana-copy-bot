use super::native_setup_fixture::{associated_token_address, ata, budget, payload, PEER, WALLET};
use super::token2022_ata_collector_tests::facts;
use super::token2022_ata_inputs_tests::*;
use crate::execution_native_ata_funding::{plan_supported_ata_funding as plan, types::*};
use crate::execution_solana_tx::{SolanaAccountMeta, SolanaInstruction};
use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;

fn spec(ix: &[SolanaInstruction], mints: &[[u8; 32]]) -> Result<Spec> {
    let p = payload(ix)?;
    let d = crate::execution_transaction_wire::decode_message(&p, |_| Ok(()))?;
    let keys: Vec<_> = d
        .binding
        .accounts
        .iter()
        .map(|a| bs58::encode(a.pubkey).into_string())
        .collect();
    let mut s = Spec::from_frozen(
        &json!({"payload":p,"keys":keys,"message_base64":STANDARD.encode(d.binding.message_bytes)}),
    );
    for mint in mints {
        let mut data = include_bytes!("../../tests/fixtures/token2022_ata/mint416.bin").to_vec();
        data[202..234].copy_from_slice(mint);
        data[270..302].copy_from_slice(mint);
        // Variable metadata/order are not a hardcoded observed pubkey exception.
        let first = data[166..234].to_vec();
        let second = data[234..].to_vec();
        data.truncate(166);
        data.extend(second);
        data.extend(first);
        s.set(&bs58::encode(mint).into_string(),json!({"owner":TOKEN2022,"executable":false,"lamports":1,"data":[STANDARD.encode(data),"base64"]}));
    }
    Ok(s)
}
#[tokio::test]
async fn token2022_ata_order_dependencies_and_size_dedup() -> Result<()> {
    let m = [83; 32];
    let t = key(TOKEN2022);
    let a = associated_token_address(&WALLET, &m, &t);
    let create = ata(WALLET, m, t, a);
    for case in [
        "clean",
        "mint-write",
        "ata-write",
        "opaque",
        "duplicate",
        "two-mints",
        "wrong-address",
        "wrong-program",
        "explicit-token2022",
    ] {
        let mut ix = budget();
        match case {
            "mint-write" | "ata-write" => ix.push(super::native_funding_fixture::transfer(
                WALLET,
                if case == "mint-write" { m } else { a },
                0,
            )),
            "opaque" => ix.push(SolanaInstruction {
                program_id: PEER,
                accounts: vec![SolanaAccountMeta::writable(m)],
                data: vec![0],
            }),
            _ => {}
        }
        let mut c = create.clone();
        if case == "wrong-address" {
            c.accounts[1].pubkey = [84; 32];
        }
        if case == "wrong-program" {
            c.accounts[5].pubkey = PEER;
        }
        ix.push(c);
        let mut mints = vec![m];
        if case == "duplicate" {
            ix.push(create.clone());
        }
        if case == "two-mints" {
            let n = [85; 32];
            mints.push(n);
            ix.push(ata(WALLET, n, t, associated_token_address(&WALLET, &n, &t)));
        }
        if case == "explicit-token2022" {
            ix.push(SolanaInstruction {
                program_id: t,
                accounts: vec![SolanaAccountMeta::writable(a)],
                data: vec![17],
            });
        }
        let s = spec(&ix, &mints)?;
        let dir = output(&format!("order-{case}"));
        let (native, count) = facts(&s, &dir).await?;
        let p = plan(&s.payload, s.wallet, &native)?;
        if ["clean", "two-mints", "explicit-token2022"].contains(&case) {
            assert_eq!(p.explicit_ata_coverage, ExplicitAtaCoverage::Complete);
            assert_eq!(
                p.known_wallet_payer_lamports,
                if case == "two-mints" {
                    4_148_160
                } else {
                    2_074_080
                }
            );
            assert_eq!(count, 4); // same170 size once for different supported mints
            let result =
                crate::execution_initial_sol::check(&s.payload, s.wallet, RESERVE, &native);
            if case == "explicit-token2022" {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "initial_sol_unsupported_setup"
                );
            } else {
                result?;
            }
        } else {
            assert_eq!(
                p.explicit_ata_coverage,
                ExplicitAtaCoverage::Partial,
                "{case}"
            );
            assert_eq!(count, if case == "duplicate" { 4 } else { 3 }, "{case}");
            assert!(p.rows.last().unwrap().amount != AtaFundingAmount::Known(0));
        }
        save(
            &dir,
            "plan.json",
            &json!({"case":case,"rows":format!("{:?}",p.rows),"coverage":format!("{:?}",p.explicit_ata_coverage),"requests":count}),
        );
    }
    Ok(())
}

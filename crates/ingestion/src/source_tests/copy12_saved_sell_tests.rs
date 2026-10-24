//! Actual saved COPY12 typed Info only; no full stream/block or trade authority.
use crate::source::yellowstone_facts::decode_yellowstone_swap_facts;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{AdmissionFacts, Delivery, DeliveryEvent};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fs, path::Path};
use yellowstone_grpc_proto::prelude::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo};

const SIGNATURE: &str =
    "4vrRoeY7D5DRCnsfu75Ws8bDznVZaFw5QWms1cnHNhycWtP2RNZzUQaeekJAGVdAYtfdirmh5fvDM4sms59zvXNz";

fn pinned(root: &Path, name: &str, expected: &str) -> Result<Vec<u8>> {
    let bytes = fs::read(root.join(name)).with_context(|| format!("read COPY12 {name}"))?;
    ensure!(
        format!("{:x}", Sha256::digest(&bytes)) == expected,
        "COPY12 {name} differs from the saved immutable snapshot extraction"
    );
    Ok(bytes)
}

fn restore_ui_bits(info: &mut SubscribeUpdateTransactionInfo, bits: &[Option<u64>]) -> Result<()> {
    let meta = info.meta.as_mut().context("COPY12 metadata absent")?;
    ensure!(
        meta.pre_token_balances.len() + meta.post_token_balances.len() == bits.len(),
        "COPY12 float sidecar row count"
    );
    for (row, bits) in meta
        .pre_token_balances
        .iter_mut()
        .chain(&mut meta.post_token_balances)
        .zip(bits)
    {
        match (&mut row.ui_token_amount, bits) {
            (Some(amount), Some(bits)) => amount.ui_amount = f64::from_bits(*bits),
            (None, None) => {}
            _ => anyhow::bail!("COPY12 float sidecar optional amount changed"),
        }
    }
    let restored: Vec<_> = meta
        .pre_token_balances
        .iter()
        .chain(&meta.post_token_balances)
        .map(|row| {
            row.ui_token_amount
                .as_ref()
                .map(|amount| amount.ui_amount.to_bits())
        })
        .collect();
    assert_eq!(restored, bits);
    Ok(())
}

#[test]
#[ignore = "private actual COPY12 fixture; requires COPY12_INPUT_DIR; decoder only"]
fn copy12_saved_source_sell_decodes_exact_actual_facts() -> Result<()> {
    let root = std::env::var_os("COPY12_INPUT_DIR").context("COPY12_INPUT_DIR required")?;
    let root = Path::new(&root);
    let raw = pinned(
        root,
        "SOURCE_SELL_INFO.pb",
        "8c00a057452e31c2f3e3206284a1fd314dac162a5c1d51d692d298380bc72532",
    )?;
    let saved: AdmissionFacts = serde_json::from_slice(&pinned(
        root,
        "SOURCE_SELL_ADMISSION.json",
        "8735595f9305d27d89f553904a2310684296ff3b062423bd637b1deedf399f7e",
    )?)?;
    let bits: Vec<Option<u64>> = serde_json::from_slice(&pinned(
        root,
        "SOURCE_SELL_INFO.float_bits.json",
        "c4d906157fb1880ba590252d7be54d8295a1749a21e3407aa0a24ef59894b373",
    )?)?;
    let deliveries = pinned(
        root,
        "SOURCE_SELL_TARGET_DELIVERIES.jsonl",
        "9c9e9dd152c9d4a225679675443c1e65ed71836123ca162de109fe41cadd2893",
    )?;
    let deliveries: Vec<Delivery> = std::str::from_utf8(&deliveries)?
        .lines()
        .map(serde_json::from_str)
        .collect::<serde_json::Result<_>>()?;
    let checkpoint = deliveries
        .iter()
        .find_map(|delivery| match &delivery.event {
            DeliveryEvent::ParentCheckpoint(checkpoint) => Some(checkpoint),
            _ => None,
        })
        .context("COPY12 saved classifier scope absent")?;
    assert!(checkpoint.scope.valid());
    assert_eq!(checkpoint.observation.child.slot, saved.facts.slot);
    assert_eq!(raw.len(), 4682);
    assert_eq!(raw, saved.info.encoded);
    assert_eq!(bits, saved.info.float_bits);
    assert_eq!(saved.facts.signature, SIGNATURE);
    assert_eq!(saved.facts.slot, 460_903_583);
    assert!(!saved.facts.program_fallback);

    let mut info = SubscribeUpdateTransactionInfo::decode(raw.as_slice())?;
    restore_ui_bits(&mut info, &bits)?;
    // Slot and classifiers are saved inputs. This constructs no containing block.
    let update = SubscribeUpdateTransaction {
        slot: saved.facts.slot,
        transaction: Some(info),
        ..Default::default()
    };
    let programs: HashSet<_> = checkpoint.scope.programs.iter().cloned().collect();
    let raydium = checkpoint.scope.raydium_programs.iter().cloned().collect();
    let pumpswap = checkpoint.scope.pumpswap_programs.iter().cloned().collect();
    let decoded = decode_yellowstone_swap_facts(&update, &programs, &raydium, &pumpswap);
    ensure!(!decoded.used_program_fallback, "COPY12 program fallback");
    ensure!(decoded.miss.is_none(), "COPY12 decode miss: {:?}", decoded.miss);
    let actual = decoded.facts?.context("COPY12 actual SELL not decoded")?;
    assert_eq!(actual.signature, saved.facts.signature);
    assert_eq!(actual.slot, saved.facts.slot);
    assert_eq!(actual.signer, saved.facts.wallet);
    assert_eq!(actual.token_in, saved.facts.token_in);
    assert_eq!(actual.token_out, saved.facts.token_out);
    assert_eq!(actual.amount_in.to_bits(), saved.facts.amount_in_bits);
    assert_eq!(actual.amount_out.to_bits(), saved.facts.amount_out_bits);
    assert_eq!(actual.exact_amounts, saved.facts.exact_amounts);
    assert_eq!(actual.dex_hint, saved.facts.dex);
    let mut actual_programs = actual.program_ids;
    actual_programs.sort();
    assert_eq!(actual_programs, saved.facts.programs);
    Ok(())
}

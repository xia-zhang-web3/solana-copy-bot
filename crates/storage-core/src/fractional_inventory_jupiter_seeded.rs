//! Exact transaction-owned WSOL setup alternative. The ATA branch remains unchanged.
use super::{
    wire::{decode, exact, Ix},
    SOL, SYSTEM, TOKEN,
};
use anyhow::{ensure, Context, Result};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
pub(super) fn setup(
    outer: &[Ix],
    inner: &BTreeMap<usize, Vec<Ix>>,
    route: usize,
    wallet: &str,
    temp: &str,
) -> Result<(usize, usize)> {
    let creates = outer
        .iter()
        .enumerate()
        .filter(|(_, i)| i.program == SYSTEM && i.data.get(..4) == Some(&3u32.to_le_bytes()))
        .collect::<Vec<_>>();
    ensure!(creates.len() == 1, "jupiter_source_seeded_create_count");
    let (index, create) = creates[0];
    let d = &create.data;
    ensure!(
        d.len() == 124
            && d[4..36] == decode(wallet)?
            && u64::from_le_bytes(d[36..44].try_into()?) == 32
            && d[44..76].iter().all(|b| b.is_ascii_hexdigit())
            && u64::from_le_bytes(d[76..84].try_into()?) > 0
            && u64::from_le_bytes(d[84..92].try_into()?) == 165
            && d[92..124] == decode(TOKEN)?
            && exact(create, SYSTEM, &[wallet, temp, wallet], d, 1),
        "jupiter_source_seeded_create_identity"
    );
    let mut h = Sha256::new();
    h.update(decode(wallet)?);
    h.update(&d[44..76]);
    h.update(decode(TOKEN)?);
    ensure!(
        h.finalize().as_slice() == decode(temp)?.as_slice(),
        "jupiter_source_seeded_address"
    );
    let init = index + 1;
    let instruction = outer.get(init).context("jupiter_source_seeded_init")?;
    let mut data = vec![18];
    data.extend(decode(wallet)?);
    ensure!(
        init < route
            && exact(instruction, TOKEN, &[temp, SOL], &data, 1)
            && !outer.iter().any(|i| i.program == super::ATA)
            && inner.iter().all(|(n, c)| *n == route || c.is_empty()),
        "jupiter_source_seeded_lifecycle"
    );
    Ok((index, init))
}

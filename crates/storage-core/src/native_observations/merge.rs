use super::types::*;
use anyhow::{ensure, Result};

fn value(a: &mut NativeObservation, b: &NativeObservation) -> Result<()> {
    match (&a.value, &b.value) {
        (Some(x), Some(y)) => ensure!(x == y, "native observations known conflict"),
        (None, Some(_)) => *a = b.clone(),
        _ => (),
    }
    Ok(())
}
fn endpoint(a: &mut NativeTokenEndpoint, b: &NativeTokenEndpoint) -> Result<()> {
    value(&mut a.mint, &b.mint)?;
    value(&mut a.token_owner, &b.token_owner)?;
    value(&mut a.token_program, &b.token_program)?;
    value(&mut a.decimals, &b.decimals)?;
    value(&mut a.raw, &b.raw)
}
pub(super) fn merge(
    old: &NativeAccountObservations,
    new: &NativeAccountObservations,
) -> Result<NativeAccountObservations> {
    ensure!(
        old.order_id == new.order_id
            && old.tx_signature == new.tx_signature
            && old.wallet_pubkey == new.wallet_pubkey
            && old.token == new.token
            && old.side == new.side
            && old.slot == new.slot,
        "native observations known conflict"
    );
    let mut m = old.clone();
    for b in &new.accounts {
        if let Some(a) = m
            .accounts
            .iter_mut()
            .find(|a| a.account_index == b.account_index)
        {
            ensure!(a.pubkey == b.pubkey, "native observations known conflict");
            value(&mut a.native_pre, &b.native_pre)?;
            value(&mut a.native_post, &b.native_post)?;
            value(&mut a.native_delta, &b.native_delta)?;
            endpoint(&mut a.pre_token, &b.pre_token)?;
            endpoint(&mut a.post_token, &b.post_token)?;
            for r in &b.relevance {
                if !a.relevance.contains(r) {
                    a.relevance.push(r.clone());
                }
            }
        } else {
            ensure!(
                old.accounts_coverage != ObservationCoverage::Known,
                "native observations known conflict"
            );
            m.accounts.push(b.clone());
        }
    }
    for b in &new.instructions {
        if let Some(a) = m
            .instructions
            .iter_mut()
            .find(|a| (a.outer_index, a.inner_index) == (b.outer_index, b.inner_index))
        {
            value(&mut a.program_id, &b.program_id)?;
            value(&mut a.instruction_type, &b.instruction_type)?;
            value(&mut a.stack_height, &b.stack_height)?;
            for (key, v) in &b.fields {
                if let Some(old) = a.fields.get_mut(key) {
                    value(old, v)?;
                } else {
                    a.fields.insert(key.clone(), v.clone());
                }
            }
            if b.coverage == ObservationCoverage::Known {
                a.coverage = b.coverage;
            }
        } else {
            ensure!(
                old.instructions_coverage != ObservationCoverage::Known,
                "native observations known conflict"
            );
            m.instructions.push(b.clone());
        }
    }
    if new.accounts_coverage == ObservationCoverage::Known {
        ensure!(
            new.accounts.len() == m.accounts.len(),
            "native observations known conflict"
        );
        m.accounts_coverage = new.accounts_coverage;
    }
    if new.instructions_coverage == ObservationCoverage::Known {
        ensure!(
            new.instructions.len() == m.instructions.len(),
            "native observations known conflict"
        );
        m.instructions_coverage = new.instructions_coverage;
    }
    for r in &new.reasons {
        m.note(r);
    }
    m.accounts.sort_by_key(|a| a.account_index);
    m.instructions
        .sort_by_key(|i| (i.outer_index, i.inner_index));
    m.validate()?;
    Ok(m)
}

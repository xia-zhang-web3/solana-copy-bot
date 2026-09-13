//! Immutable experiment/policy operands across the owned SELL awaits.
use super::*;
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OwnedExperimentSnapshot {
    pub experiment: TinyExperiment,
    pub policy: Option<ProtectedNativePolicy>,
}
pub(crate) fn read(c: &Connection) -> Result<OwnedExperimentSnapshot> {
    let experiment = load(c)?.context("tiny_budget_inactive")?;
    let policy = match protected::mode(c)?.as_str() {
        "decoded_amount" => None,
        "protected_native_capital" => Some(protected::load_policy(c, &experiment)?),
        _ => anyhow::bail!("tiny_capital_mode_conflict"),
    };
    Ok(OwnedExperimentSnapshot { experiment, policy })
}
impl SqliteDiscoveryStore {
    pub fn owned_sell_experiment_snapshot(&self) -> Result<OwnedExperimentSnapshot> {
        let tx = self.conn.unchecked_transaction()?;
        let snapshot = read(&tx)?;
        tx.commit()?;
        Ok(snapshot)
    }
}

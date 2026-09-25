//! One immutable native anchor; balances received later cannot rearm the experiment.
use super::*;
pub const TINY_NATIVE_ALLOWANCE: u64 = 15_000_000;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProtectedNativePolicy {
    pub experiment_id: String,
    pub wallet: String,
    pub version: u64,
    pub initial_lamports: u64,
    pub original_reserve: u64,
    pub floor_lamports: u64,
    pub allowance: u64,
    pub observation_slot: u64,
    pub observed_at: DateTime<Utc>,
    pub activated_at: DateTime<Utc>,
    pub deadline: DateTime<Utc>,
}
#[derive(Debug, Clone)]
pub struct ProtectedCapitalClaim {
    pub policy: ProtectedNativePolicy,
    pub requested_lamports: u64,
    pub floor_lamports: u64,
    pub request_sha256: String,
}

pub(super) fn mode(conn: &Connection) -> Result<String> {
    Ok(conn.query_row(
        "SELECT policy_mode FROM execution_tiny_experiment WHERE singleton=1",
        [],
        |r| r.get(0),
    )?)
}
pub(super) fn load_policy(conn: &Connection, e: &TinyExperiment) -> Result<ProtectedNativePolicy> {
    ensure!(
        mode(conn)? == "protected_native_capital",
        "tiny_capital_mode_conflict"
    );
    let row = conn.query_row("SELECT experiment_id,wallet,version,initial_lamports,original_reserve,
        floor_lamports,allowance,observation_slot,observed_at FROM execution_tiny_native_policy WHERE experiment_id=?1",
        [&e.id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,u64>(2)?,
            r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?,r.get::<_,u64>(6)?,
            r.get::<_,String>(7)?,r.get::<_,String>(8)?))).optional()?.context("tiny_capital_policy_missing")?;
    let p = ProtectedNativePolicy {
        experiment_id: row.0,
        wallet: row.1,
        version: row.2,
        initial_lamports: row.3.parse()?,
        original_reserve: row.4.parse()?,
        floor_lamports: row.5.parse()?,
        allowance: row.6,
        observation_slot: row.7.parse()?,
        observed_at: row.8.parse()?,
        activated_at: e.activated_at,
        deadline: e.deadline,
    };
    ensure!(
        p.wallet == e.wallet && p.version == 1 && p.allowance == TINY_NATIVE_ALLOWANCE,
        "tiny_capital_policy_binding"
    );
    ensure!(
        p.floor_lamports == floor(p.initial_lamports, p.original_reserve)?
            && p.observed_at <= e.activated_at,
        "tiny_capital_policy_corrupt"
    );
    Ok(p)
}
fn floor(balance: u64, reserve: u64) -> Result<u64> {
    ensure!(reserve > 0, "tiny_capital_reserve");
    let floor = reserve.max(
        balance
            .checked_sub(TINY_NATIVE_ALLOWANCE)
            .context("tiny_capital_underflow")?,
    );
    ensure!(balance > floor, "tiny_capital_insufficient_funding");
    Ok(floor)
}
fn active(e: &TinyExperiment, id: &str, wallet: &str, now: DateTime<Utc>) -> Result<()> {
    ensure!(e.id == id && e.wallet == wallet, "tiny_capital_identity");
    ensure!(
        now >= e.activated_at && now >= e.last_decision_at && now < e.deadline,
        "tiny_budget_deadline"
    );
    ensure!(e.state == "active", "tiny_budget_stopped");
    Ok(())
}
impl SqliteDiscoveryStore {
    /// App supplies a closed ordinary payer observation before assembly. Lock first,
    /// then clock and activate exactly once. No SQLite transaction crosses an await.
    pub fn prepare_tiny_native_policy(
        &self,
        id: &str,
        wallet: &str,
        balance: u64,
        reserve: u64,
        slot: u64,
        observed_at: DateTime<Utc>,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ProtectedNativePolicy> {
        self.prepare_native_policy_inner(id, wallet, balance, reserve, slot, observed_at,
            None, None, clock)
    }

    /// First native BUY only: the current decision and reserved order are checked
    /// under the same write lock that creates the one-time protected anchor.
    pub fn prepare_tiny_native_policy_for_native_buy(
        &self,
        id: &str,
        wallet: &str,
        balance: u64,
        reserve: u64,
        slot: u64,
        observed_at: DateTime<Utc>,
        binding: &crate::native_buy::NativeBuyActivationBinding,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ProtectedNativePolicy> {
        self.prepare_native_policy_inner(id, wallet, balance, reserve, slot, observed_at,
            Some(binding), None, clock)
    }

    /// Explicit owner origin, checked with the immutable intent and reserved order
    /// under the same lock that creates (or reuses) the protected native anchor.
    pub fn prepare_tiny_native_policy_for_owner_buy(
        &self, id: &str, wallet: &str, balance: u64, reserve: u64,
        slot: u64, observed_at: DateTime<Utc>,
        intent: &crate::OwnerTechnicalBuyIntent, order: &crate::ExecutionCanaryOrder,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ProtectedNativePolicy> {
        self.prepare_native_policy_inner(id, wallet, balance, reserve, slot, observed_at,
            None, Some((intent, order)), clock)
    }

    fn prepare_native_policy_inner(
        &self,
        id: &str,
        wallet: &str,
        balance: u64,
        reserve: u64,
        slot: u64,
        observed_at: DateTime<Utc>,
        binding: Option<&crate::native_buy::NativeBuyActivationBinding>,
        owner: Option<(&crate::OwnerTechnicalBuyIntent, &crate::ExecutionCanaryOrder)>,
        clock: impl Fn() -> Result<DateTime<Utc>>,
    ) -> Result<ProtectedNativePolicy> {
        ensure!(
            !id.is_empty()
                && id.len() <= 128
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)),
            "tiny_budget_invalid_id"
        );
        ensure!(
            !wallet.is_empty() && wallet.trim() == wallet,
            "tiny_budget_invalid_wallet"
        );
        self.with_immediate_transaction_retry("prepare native anchor", |conn| {
            let now = clock()?;
            if let Some((intent, order)) = owner {
                crate::owner_technical_buy_protected::activation_current(
                    self, intent, order, id, wallet, reserve, now)?;
            }
            if let Some(e) = refresh(conn, now)? {
                active(&e, id, wallet, now)?;
                return load_policy(conn, &e);
            }
            if let Some(binding) = binding {
                ensure!(crate::native_buy::activation_current(conn, binding, now)?,
                    "native_buy_decision_changed");
            }
            ensure!(observed_at <= now && now.signed_duration_since(observed_at) <= Duration::seconds(30),
                "tiny_capital_observation_time");
            let floor = floor(balance, reserve)?;
            let deadline = if let (Some(binding), Some(authority)) =
                (binding, crate::native_buy::cohort::load(conn)?)
            {
                ensure!(authority.run_id == id
                    && authority.policy_identity == binding.policy_identity
                    && authority.activated_at <= now && now < authority.deadline
                    && authority.deadline - authority.activated_at
                        <= Duration::seconds(TECHNICAL_COHORT_MAX_HORIZON_SECONDS),
                    "tiny_budget_cohort_authority");
                authority.deadline
            } else {
                now.checked_add_signed(Duration::seconds(TINY_HORIZON_SECONDS))
                    .context("tiny_budget_clock_overflow")?
            };
            conn.execute("INSERT INTO execution_tiny_experiment(singleton,experiment_id,wallet,activated_at,deadline,last_decision_at,state,policy_mode)
                VALUES(1,?1,?2,?3,?4,?3,'active','protected_native_capital')", params![id,wallet,now.to_rfc3339(),deadline.to_rfc3339()])?;
            conn.execute("INSERT INTO execution_tiny_native_policy VALUES(?1,?2,1,?3,?4,?5,15000000,?6,?7)",
                params![id,wallet,balance.to_string(),reserve.to_string(),floor.to_string(),slot.to_string(),observed_at.to_rfc3339()])?;
            if let Some((intent, order)) = owner {
                crate::owner_technical_buy_protected::activation_current(
                    self, intent, order, id, wallet, reserve, clock()?)?;
            }
            load_policy(conn, &load(conn)?.context("tiny_capital_policy_missing")?)
        })
    }
    pub fn tiny_native_policy(
        &self,
        id: &str,
        wallet: &str,
        now: DateTime<Utc>,
    ) -> Result<ProtectedNativePolicy> {
        self.with_immediate_transaction_retry("read native anchor", |conn| {
            let e = refresh(conn, now)?.context("tiny_capital_policy_missing")?;
            active(&e, id, wallet, now)?;
            load_policy(conn, &e)
        })
    }
}
pub(super) fn validate_claim(
    conn: &Connection,
    e: &TinyExperiment,
    p: &TinyBudgetClaim,
) -> Result<bool> {
    match (mode(conn)?.as_str(), &p.protected_capital, p.buy_lamports) {
        ("decoded_amount", None, Some(amount)) => Ok((1..=TINY_BUY_LAMPORTS).contains(&amount)),
        ("protected_native_capital", Some(proof), amount) => {
            if amount.is_some() {
                crate::owner_technical_buy_protected::decoded_claim(conn, &e.id, p)?;
            }
            ensure!(
                load_policy(conn, e)? == proof.policy,
                "tiny_capital_policy_binding"
            );
            ensure!(
                proof.floor_lamports >= proof.policy.floor_lamports
                    && proof.request_sha256.len() == 64
                    && proof.request_sha256.bytes().all(|b| b.is_ascii_hexdigit()),
                "tiny_capital_claim_binding"
            );
            Ok((1..=TINY_BUY_LAMPORTS).contains(&proof.requested_lamports))
        }
        _ => anyhow::bail!("tiny_capital_evidence_mode"),
    }
}
pub(super) fn record_claim(
    conn: &Connection,
    d: &crate::ExecutionCanaryDispatch,
    p: &TinyBudgetClaim,
) -> Result<()> {
    if let Some(p) = &p.protected_capital {
        conn.execute(
            "INSERT INTO execution_tiny_capital_evidence VALUES(?1,?2,?3,?4)",
            params![
                d.order_id,
                p.requested_lamports,
                p.floor_lamports.to_string(),
                p.request_sha256
            ],
        )?;
    }
    Ok(())
}

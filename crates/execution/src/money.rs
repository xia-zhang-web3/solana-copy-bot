use anyhow::{anyhow, Result};
use copybot_core_types::Lamports;

const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

fn validate_sol_amount(sol: f64, label: &str) -> Result<f64> {
    if !sol.is_finite() || sol < 0.0 {
        return Err(anyhow!(
            "{label} must be a finite non-negative SOL amount, got {sol}"
        ));
    }
    let scaled = sol * LAMPORTS_PER_SOL;
    if !scaled.is_finite() || scaled > u64::MAX as f64 {
        return Err(anyhow!("{label} overflows lamports, got {sol}"));
    }
    Ok(scaled)
}

pub(crate) fn sol_to_lamports_ceil(sol: f64, label: &str) -> Result<Lamports> {
    let scaled = validate_sol_amount(sol, label)?;
    Ok(Lamports::new(scaled.ceil() as u64))
}

pub(crate) fn sol_to_lamports_floor(sol: f64, label: &str) -> Result<Lamports> {
    let scaled = validate_sol_amount(sol, label)?;
    Ok(Lamports::new(scaled.floor() as u64))
}

pub(crate) fn checked_add_lamports(
    left: Lamports,
    right: Lamports,
    label: &str,
) -> Result<Lamports> {
    left.checked_add(right).ok_or_else(|| {
        anyhow!(
            "{label} overflow left_lamports={} right_lamports={}",
            left.as_u64(),
            right.as_u64()
        )
    })
}

pub(crate) fn lamports_to_sol(lamports: Lamports) -> f64 {
    lamports.as_u64() as f64 / LAMPORTS_PER_SOL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sol_to_lamports_rounding_policy_is_explicit() -> Result<()> {
        let sol = 0.1000000001;
        assert_eq!(sol_to_lamports_floor(sol, "floor")?.as_u64(), 100_000_000);
        assert_eq!(sol_to_lamports_ceil(sol, "ceil")?.as_u64(), 100_000_001);
        Ok(())
    }

    #[test]
    fn checked_add_lamports_detects_overflow() {
        let overflow = checked_add_lamports(Lamports::new(u64::MAX), Lamports::new(1), "sum");
        assert!(overflow.is_err());
    }
}

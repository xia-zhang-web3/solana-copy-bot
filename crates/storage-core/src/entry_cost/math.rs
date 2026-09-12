use anyhow::{ensure, Context, Result};

pub(super) fn canonical_u128(raw: &str) -> Result<u128> {
    let value: u128 = raw.parse()?;
    ensure!(value.to_string() == raw, "noncanonical entry cost lamports");
    Ok(value)
}

/// Config is f64; its shortest round-trip decimal is the declared SOL threshold.
/// Integer lamport totals reach it at ceil(decimal * 1e9). No binary-float sum/epsilon.
pub(super) fn cap_lamports_ceiling(cap: f64) -> Result<u128> {
    ensure!(cap.is_finite() && cap >= 0.0, "invalid entry cost cap");
    if cap == 0.0 {
        return Ok(0);
    }
    let raw = cap.to_string();
    let (whole, fractional) = raw.split_once('.').unwrap_or((&raw, ""));
    let whole: u128 = whole
        .parse()
        .context("entry cost cap outside u128 lamport domain")?;
    let base = whole
        .checked_mul(1_000_000_000)
        .context("entry cost cap overflow")?;
    let padded = format!("{fractional:0<9}");
    let fraction: u128 = padded[..9].parse()?;
    let remainder = padded.as_bytes()[9..].iter().any(|v| *v != b'0');
    base.checked_add(fraction)
        .and_then(|v| v.checked_add(u128::from(remainder)))
        .context("entry cost cap overflow")
}

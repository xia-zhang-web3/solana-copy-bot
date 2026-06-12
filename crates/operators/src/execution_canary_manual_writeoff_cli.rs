use anyhow::{anyhow, Context, Result};
use std::collections::BTreeSet;
use std::path::PathBuf;

pub(crate) const DEFAULT_MAX_POSITIONS: usize = 20;
pub(crate) const HARD_MAX_POSITIONS: usize = 100;
pub(crate) const DEFAULT_MIN_AGE_MINUTES: i64 = 60;
pub(crate) const DEFAULT_MAX_POSITION_QUOTE_SOL: f64 = 0.001;
pub(crate) const DEFAULT_MAX_TOTAL_QUOTE_SOL: f64 = 0.002;
const HARD_MAX_POSITION_QUOTE_SOL: f64 = 0.01;
const HARD_MAX_TOTAL_QUOTE_SOL: f64 = 0.05;

#[derive(Debug, Clone, PartialEq)]
pub struct Cli {
    pub config_path: PathBuf,
    pub json: bool,
    pub commit: bool,
    pub max_positions: usize,
    pub min_age_minutes: i64,
    pub max_position_quote_sol: f64,
    pub max_total_quote_sol: f64,
    pub tokens: BTreeSet<String>,
    pub no_route_tokens: BTreeSet<String>,
}

pub fn parse_args_from<I>(args: I) -> Result<Cli>
where
    I: IntoIterator,
    I::Item: Into<String>,
{
    let mut config_path = None;
    let mut json = false;
    let mut commit = false;
    let mut max_positions = DEFAULT_MAX_POSITIONS;
    let mut min_age_minutes = DEFAULT_MIN_AGE_MINUTES;
    let mut max_position_quote_sol = DEFAULT_MAX_POSITION_QUOTE_SOL;
    let mut max_total_quote_sol = DEFAULT_MAX_TOTAL_QUOTE_SOL;
    let mut tokens = BTreeSet::new();
    let mut no_route_tokens = BTreeSet::new();
    let mut iter = args.into_iter().map(Into::into);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--config" => config_path = Some(PathBuf::from(next_value(&mut iter, "--config")?)),
            "--json" => json = true,
            "--commit" => commit = true,
            "--max-positions" => {
                max_positions = parse_usize(&next_value(&mut iter, "--max-positions")?)?;
            }
            "--min-age-minutes" => {
                min_age_minutes = parse_i64(&next_value(&mut iter, "--min-age-minutes")?)?;
            }
            "--max-position-quote-sol" => {
                max_position_quote_sol =
                    parse_non_negative_f64(&next_value(&mut iter, "--max-position-quote-sol")?)?;
            }
            "--max-total-quote-sol" => {
                max_total_quote_sol =
                    parse_non_negative_f64(&next_value(&mut iter, "--max-total-quote-sol")?)?;
            }
            "--token" => {
                tokens.insert(next_value(&mut iter, "--token")?);
            }
            "--allow-no-route-token" => {
                no_route_tokens.insert(next_value(&mut iter, "--allow-no-route-token")?);
            }
            other => return Err(anyhow!("unknown argument: {other}")),
        }
    }
    let config_path = config_path.ok_or_else(|| anyhow!("--config is required"))?;
    if max_positions == 0 || max_positions > HARD_MAX_POSITIONS {
        anyhow::bail!("--max-positions must be between 1 and {HARD_MAX_POSITIONS}");
    }
    if min_age_minutes < 0 {
        anyhow::bail!("--min-age-minutes must be >= 0");
    }
    if max_position_quote_sol > HARD_MAX_POSITION_QUOTE_SOL {
        anyhow::bail!("--max-position-quote-sol must be <= {HARD_MAX_POSITION_QUOTE_SOL}");
    }
    if max_total_quote_sol > HARD_MAX_TOTAL_QUOTE_SOL {
        anyhow::bail!("--max-total-quote-sol must be <= {HARD_MAX_TOTAL_QUOTE_SOL}");
    }
    Ok(Cli {
        config_path,
        json,
        commit,
        max_positions,
        min_age_minutes,
        max_position_quote_sol,
        max_total_quote_sol,
        tokens,
        no_route_tokens,
    })
}

fn next_value(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn parse_usize(value: &str) -> Result<usize> {
    value
        .parse::<usize>()
        .with_context(|| format!("invalid integer: {value}"))
}

fn parse_i64(value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .with_context(|| format!("invalid integer: {value}"))
}

fn parse_non_negative_f64(value: &str) -> Result<f64> {
    let parsed = value
        .parse::<f64>()
        .with_context(|| format!("invalid number: {value}"))?;
    if !parsed.is_finite() || parsed < 0.0 {
        anyhow::bail!("value must be finite and >= 0");
    }
    Ok(parsed)
}

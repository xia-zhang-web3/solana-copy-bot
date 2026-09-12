use super::{config, fixtures as previous, parse, SwapParser, PROVIDERS, PUMP, RAY, SOL};
use crate::source::{yellowstone, HeliusWsSource, YellowstoneGrpcSource, YellowstoneParsedUpdate};
use anyhow::Result;
use prost::Message;
use serde_json::{json, Value};

mod controls;
mod fixtures;
mod provider;
use fixtures::{Damage, Operand};

#[path = "../known_time/mod.rs"]
mod known_time;

fn owned(sell: bool, native: bool, damage: Damage, providers: &[&str]) -> Result<()> {
    let mut f = fixtures::healthy(sell, native, false, false);
    fixtures::rename(
        &mut f,
        &format!("b48-owned-{sell}-{native}-{}", damage.label()),
    );
    let operand = Operand {
        post: sell,
        foreign: false,
        damage,
    };
    for provider in providers {
        provider::check(&f, provider, Some(operand), false)?;
    }
    Ok(())
}

macro_rules! refusal {
    ($name:ident,$sell:literal,$native:literal,$damage:ident) => {
        #[test]
        fn $name() -> Result<()> {
            owned($sell, $native, Damage::$damage, &["yellowstone"])
        }
    };
}
refusal!(owned_buy_wsol_absent, false, false, Absent);
refusal!(owned_buy_wsol_text, false, false, Text);
refusal!(owned_buy_wsol_nan, false, false, Nan);
refusal!(owned_buy_wsol_infinite, false, false, Infinite);
refusal!(owned_buy_native_absent, false, true, Absent);
refusal!(owned_buy_native_text, false, true, Text);
refusal!(owned_buy_native_nan, false, true, Nan);
refusal!(owned_buy_native_infinite, false, true, Infinite);
refusal!(owned_sell_wsol_absent, true, false, Absent);
refusal!(owned_sell_wsol_text, true, false, Text);
refusal!(owned_sell_wsol_nan, true, false, Nan);
refusal!(owned_sell_wsol_infinite, true, false, Infinite);
refusal!(owned_sell_native_absent, true, true, Absent);
refusal!(owned_sell_native_text, true, true, Text);
refusal!(owned_sell_native_nan, true, true, Nan);
refusal!(owned_sell_native_infinite, true, true, Infinite);

#[test]
fn existing_json_refusal_on_corresponding_owned_operands() -> Result<()> {
    for sell in [false, true] {
        for native in [false, true] {
            for damage in fixtures::DAMAGES {
                owned(sell, native, damage, &["rpc_backfill", "helius_fetch"])?;
            }
        }
    }
    Ok(())
}

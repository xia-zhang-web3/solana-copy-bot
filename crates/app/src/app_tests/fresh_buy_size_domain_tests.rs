use super::fresh_buy_size_fixture::*;
use crate::execution_canary_entry_gate::validate_execution_canary_entry_metadata as gate;
use crate::execution_submit_adapter::ExecutionBuildPlanMetadata;
use anyhow::Result;

#[tokio::test]
async fn fresh_buy_size_integer_oracle_and_slippage_boundaries() -> Result<()> {
    // Signed integer oracle for s1: [(I1*O0*(10000+s0) - I0*O1*10000)/(I0*O1)].
    for (i0, o0, i1, o1, s0, execute) in [
        (
            10_000_000_u64,
            100_u64,
            10_000_000_u64,
            100_u64,
            0_i64,
            true,
        ),
        (10_000_000, 100, 30_000_000, 300, 0, true),
        (10_000_000, 100, 20_000_000, 200, 125, true),
        (20_000_000, 200, 10_000_000, 100, 500, true),
        (10_000_000, 100, 10_000_000, 100, 500, true),
        (20_000_000, 200, 10_000_000, 100, 499, true),
        (20_000_000, 200, 10_000_000, 100, 501, false),
        (20_000_000, 200, 10_000_000, 100, -250, true),
        (10_000_000, 100, 10_499_000, 100, 0, true),
        (10_000_000, 100, 10_500_000, 100, 0, true),
        (10_000_000, 100, 10_501_000, 100, 0, false),
        (10_000_000, 100, 10_000_000, 100, -9999, true),
    ] {
        let denominator = i128::from(i0) * i128::from(o1);
        let numerator =
            i128::from(i1) * i128::from(o0) * i128::from(10_000 + s0) - denominator * 10_000;
        let expected = numerator as f64 / denominator as f64;
        let (config, fresh) = refresh(
            metadata(i0, o0, s0 as f64),
            i1,
            quote(&i1.to_string(), &o1.to_string()),
        )
        .await?;
        assert_quote(
            &fresh,
            i1,
            o1,
            expected,
            if execute {
                "would_execute"
            } else {
                "would_skip"
            },
        );
        assert_eq!(
            gate(&config, &fresh).is_none(),
            execute,
            "{i0}/{o0} -> {i1}/{o1}, s0={s0}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_large_raw_integer_cross_products_do_not_overflow() -> Result<()> {
    for raw in [9_007_199_254_740_993_u64, u64::MAX] {
        // Products exceed u64 and raw can exceed exact f64 integers; ratio is exactly 1.
        let old = metadata(raw, raw, 0.0);
        let (_, fresh) =
            refresh(old, 10_000_000, quote(&raw.to_string(), &raw.to_string())).await?;
        close(fresh.quote_price_sol.unwrap(), 1e-9, 1e-24);
        close(fresh.slippage_bps.unwrap(), 0.0, 1e-9);
        assert_eq!(
            fresh.quote_in_amount_raw.as_deref(),
            Some(raw.to_string().as_str())
        );
        assert_eq!(fresh.decision_status.as_deref(), Some("would_execute"));
    }
    Ok(())
}

fn unknown(m: &ExecutionBuildPlanMetadata) {
    assert_eq!(m.quote_price_sol, None);
    assert_eq!(m.slippage_bps, None);
    assert_eq!(m.decision_status.as_deref(), Some("unknown"));
    assert!(matches!(
        m.decision_reason.as_deref(),
        Some("fresh_submit_quote_invalid_price" | "fresh_submit_quote_error")
    ));
}

#[tokio::test]
async fn fresh_buy_size_rejects_invalid_or_missing_each_raw_amount() -> Result<()> {
    for raw in [
        None,
        Some(""),
        Some("0"),
        Some("-1"),
        Some("+1"),
        Some("1.5"),
        Some("1e7"),
        Some("NaN"),
        Some("Infinity"),
        Some("inf"),
        Some(" 1"),
        Some("1 "),
        Some("18446744073709551616"),
        Some("999999999999999999999999999999999999999"),
    ] {
        for field in 0..4 {
            let mut old = metadata(10_000_000, 100, 0.0);
            let mut body = quote("10000000", "100");
            match field {
                0 => old.quote_in_amount_raw = raw.map(str::to_owned),
                1 => old.quote_out_amount_raw = raw.map(str::to_owned),
                _ => {
                    let key = if field == 2 { "inAmount" } else { "outAmount" };
                    if let Some(raw) = raw {
                        body[key] = raw.into();
                    } else {
                        body.as_object_mut().unwrap().remove(key);
                    }
                }
            }
            let (config, fresh) = refresh(old, 10_000_000, body).await?;
            unknown(&fresh);
            assert!(
                gate(&config, &fresh).is_some(),
                "field={field}, raw={raw:?}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_rejects_invalid_price_reference_and_nonfinite_results() -> Result<()> {
    let values = [
        None,
        Some(0.0),
        Some(-1.0),
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
    ];
    for price in values {
        let mut old = metadata(10_000_000, 100, 0.0);
        old.quote_price_sol = price;
        let (config, fresh) = refresh(old, 10_000_000, quote("10000000", "100")).await?;
        unknown(&fresh);
        assert!(gate(&config, &fresh).is_some());
    }
    for slip in [
        None,
        Some(f64::NAN),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(-10_000.0),
        Some(-20_000.0),
    ] {
        let mut old = metadata(10_000_000, 100, 0.0);
        old.slippage_bps = slip;
        let (_, fresh) = refresh(old, 10_000_000, quote("10000000", "100")).await?;
        unknown(&fresh);
    }
    for (price, slip, output) in [
        (f64::MAX, -9999.0, "100"),           // reference overflows
        (f64::from_bits(1), f64::MAX, "100"), // reference underflows
        (f64::MAX, 0.0, "1"),                 // fresh price overflows
        (f64::from_bits(1), 0.0, "1000"),     // fresh price underflows
        (1.0, f64::MAX, "1"),                 // final slippage overflows
    ] {
        let mut old = metadata(10_000_000, 100, slip);
        old.quote_price_sol = Some(price);
        let (_, fresh) = refresh(old, 10_000_000, quote("10000000", output)).await?;
        unknown(&fresh);
    }
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_repeated_refresh_preserves_reference_and_metadata() -> Result<()> {
    let old = metadata(20_000_000, 200, 125.0);
    let (_, first) = refresh(old.clone(), 10_000_000, quote("10000000", "100")).await?;
    let (_, second) = refresh(first, 30_000_000, quote("30000000", "300")).await?;
    assert_quote(&second, 30_000_000, 300, 125.0, "would_execute");
    let value: serde_json::Value =
        serde_json::from_str(second.quote_response_json.as_deref().unwrap())?;
    assert_eq!(
        value
            .pointer("/_copybot/outDecimals")
            .and_then(|v| v.as_u64()),
        Some(0)
    );
    assert_eq!(value["inAmount"], "30000000");
    assert_eq!(second.quote_event_id, old.quote_event_id);
    assert_eq!(second.priority_fee_source, old.priority_fee_source);
    assert_eq!(second.priority_fee_status, old.priority_fee_status);
    assert_eq!(second.priority_fee_lamports, old.priority_fee_lamports);
    assert_eq!(second.priority_fee_json, old.priority_fee_json);
    assert_eq!(second.route_plan_json, old.route_plan_json);
    assert!(second.quote_request_ts.is_some());
    Ok(())
}

#[tokio::test]
async fn fresh_buy_size_returned_input_mismatch_still_blocks() -> Result<()> {
    let (config, fresh) = refresh(
        metadata(20_000_000, 200, 0.0),
        10_000_000,
        quote("20000000", "200"),
    )
    .await?;
    assert_eq!(fresh.decision_status.as_deref(), Some("would_execute"));
    assert_eq!(gate(&config, &fresh), Some("quote_amount_mismatch"));
    Ok(())
}

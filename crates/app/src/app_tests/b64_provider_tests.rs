use super::{b64_case_fixture::run, b64_http_fixture::Replies};
use anyhow::Result;

#[tokio::test]
async fn b64_four_builders_overlap_and_preserve_bundle_in_both_response_orders() -> Result<()> {
    for kind in ["entry", "hot", "close", "owned"] {
        for pump_first in [false, true] {
            let c = run(
                kind,
                Replies {
                    pump_first,
                    ..Default::default()
                },
                true,
            )
            .await?;
            c.correlated(true)?;
            assert_eq!(c.event.quote_status, "ok");
            assert_eq!(
                c.event.quote_out_amount_raw.as_deref(),
                Some(if c.event.side == "buy" {
                    "2000000"
                } else {
                    "300000000"
                })
            );
            assert_eq!(c.event.decision_status.as_deref(), Some("would_execute"));
            assert_eq!(
                c.event.decision_reason.as_deref(),
                Some("within_slippage_limit")
            );
            assert_eq!(c.captures.len(), 2);
            let last_received = c.captures.iter().map(|r| r.received).max().unwrap();
            let first_replied = c.captures.iter().map(|r| r.replied).min().unwrap();
            assert!(last_received < first_replied);
        }
    }
    Ok(())
}
#[tokio::test]
async fn b64_provider_errors_are_independent_and_never_fail_fast() -> Result<()> {
    for kind in ["entry", "hot", "close", "owned"] {
        for (generic_error, pump_error) in [(true, false), (false, true), (true, true)] {
            for pump_first in [false, true] {
                let c = run(
                    kind,
                    Replies {
                        generic_error,
                        pump_error,
                        pump_first,
                        ..Default::default()
                    },
                    true,
                )
                .await?;
                let pump = c.pump.as_ref().unwrap();
                assert_eq!(
                    c.generic.quote_status,
                    if generic_error { "error" } else { "ok" }
                );
                assert_eq!(pump.quote_status, if pump_error { "error" } else { "ok" });
                for (sample, failed) in [(&c.generic, generic_error), (pump, pump_error)] {
                    if failed {
                        assert!(sample.error.as_ref().unwrap().contains("503"));
                        assert_eq!(sample.quote_response_json, None);
                        assert_eq!(sample.quote_out_amount_raw, None);
                    }
                }
                if generic_error && pump_error {
                    assert_eq!(c.event.decision_status.as_deref(), Some("unknown"));
                    assert_eq!(c.event.quote_status, "error");
                } else {
                    c.correlated(!pump_error)?;
                    assert_eq!(c.event.decision_status.as_deref(), Some("would_execute"));
                }
                assert_eq!(c.captures.len(), 2);
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn b64_flag_false_is_generic_only_in_all_builders() -> Result<()> {
    for kind in ["entry", "hot", "close", "owned"] {
        let c = run(
            kind,
            Replies {
                generic_only: true,
                ..Default::default()
            },
            true,
        )
        .await?;
        assert!(c.pump.is_none());
        assert_eq!(c.captures.len(), 1);
        c.correlated(false)?;
    }
    Ok(())
}
#[tokio::test]
async fn b64_unknown_decimals_keep_original_rpc_conditions_and_unknown() -> Result<()> {
    for kind in ["entry", "hot"] {
        for (generic_error, generic_only, decimals) in [
            (false, false, vec![Some(6)]),
            (false, false, vec![None, None]),
            (true, false, vec![None]),
            (false, true, vec![None]),
            (true, true, vec![]),
        ] {
            let count = decimals.len();
            let unknown = decimals.iter().all(Option::is_none);
            let c = run(
                kind,
                Replies {
                    generic_error,
                    generic_only,
                    decimals,
                    ..Default::default()
                },
                false,
            )
            .await?;
            assert_eq!(
                c.captures.iter().filter(|r| !r.body.is_null()).count(),
                count
            );
            assert_eq!(
                c.generic.quote_price_sol.is_none(),
                generic_error || unknown
            );
            assert_eq!(
                c.generic.slippage_bps.is_none(),
                generic_error || unknown || kind == "entry"
            );
            if generic_only {
                assert!(c.pump.is_none());
                assert_eq!(c.event.decision_status.as_deref(), Some("unknown"));
            } else {
                c.correlated(true)?;
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn b64_generic_retry_overlaps_pump_and_keeps_first_attempt_timing() -> Result<()> {
    for pump_first in [false, true] {
        let c = run(
            "entry",
            Replies {
                retry: true,
                pump_first,
                ..Default::default()
            },
            true,
        )
        .await?;
        c.correlated(true)?;
        assert_eq!(c.captures.len(), 3);
        assert!(c.generic.quote_latency_ms.unwrap() >= 125);
        let generic: Vec<_> = c
            .captures
            .iter()
            .filter(|r| !r.path.starts_with("/pump-fun/"))
            .collect();
        assert!((generic[1].received - generic[0].replied).num_milliseconds() >= 100);
    }
    Ok(())
}
#[tokio::test]
async fn b64_completed_pump_preserves_generic_selection_and_fee_gate() -> Result<()> {
    for pump_first in [false, true] {
        let c = run(
            "entry",
            Replies {
                pump_completed: true,
                pump_first,
                ..Default::default()
            },
            true,
        )
        .await?;
        c.correlated(false)?;
        let c = run(
            "entry",
            Replies {
                generic_fee: true,
                pump_first,
                ..Default::default()
            },
            true,
        )
        .await?;
        c.correlated(true)?;
        assert!(
            crate::execution_quote_provider_selection::quote_response_requires_fee_account(
                c.generic.quote_response_json.as_deref()
            )
        );
    }
    Ok(())
}

use super::token2022_ata_inputs_tests::*;
use super::token2022_ata_rpc_tests::{Hook, Server};
use anyhow::Result;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
pub(super) fn rent170(r: &Value) -> bool {
    r["method"] == "getMinimumBalanceForRentExemption" && r["params"][0] == 170
}
#[tokio::test]
async fn token2022_ata_rent_reply_binding_matrix() -> Result<()> {
    for case in [
        "swap165-id",
        "missing",
        "string",
        "negative",
        "rpc-error",
        "truncated",
        "oversized",
        "redirect",
        "http500",
    ] {
        let spec = Spec::sufficient("absent")?;
        let dir = output(&format!("rent-reply-{case}"));
        let hook: Hook = Box::new(move |r, reply| {
            if rent170(r) {
                let mut value: Value = serde_json::from_slice(&reply.body).unwrap();
                match case {
                    "swap165-id" => value["id"] = json!("native-funding-classic-ata-rent"),
                    "missing" => {
                        value.as_object_mut().unwrap().remove("result");
                    }
                    "string" => value["result"] = json!("2074080"),
                    "negative" => value["result"] = json!(-1),
                    "rpc-error" => value["error"] = json!({"code":-32602}),
                    "truncated" => {
                        reply.body = b"{".to_vec();
                        return;
                    }
                    "oversized" => {
                        reply.body = vec![b' '; 16385];
                        return;
                    }
                    "redirect" => {
                        reply.status = 302;
                        reply.location = Some("http://127.0.0.1:1/forbidden".into());
                    }
                    "http500" => reply.status = 500,
                    _ => unreachable!(),
                }
                reply.body = serde_json::to_vec(&value).unwrap();
            }
        });
        let server = Server::start(&spec, Some(hook)).await?;
        let out = crate::execution_initial_sol::collect_and_check(
            &server.rpc.endpoint,
            2000,
            &spec.payload,
            spec.wallet,
            RESERVE,
        )
        .await;
        let trace = server.finish(&dir).await?;
        save(
            &dir,
            "result.json",
            &json!({"case":case,"result":format!("{out:#?}"),
            "collection_requests":trace.len()}),
        );
        assert!(
            out.unwrap_err()
                .to_string()
                .starts_with("initial_sol_observations_unavailable:"),
            "{case}"
        );
        assert_eq!(trace.len(), 4);
        assert_eq!(trace.iter().filter(|t| rent170(&t.request)).count(), 1);
    }
    // A captured good reply cannot move even to a second invocation of the same payload.
    let captured = Arc::new(Mutex::new(Vec::new()));
    for replay in [false, true] {
        let c = captured.clone();
        let s = Spec::sufficient("absent")?;
        let dir = output(&format!("replay-{replay}"));
        let server = Server::start(
            &s,
            Some(Box::new(move |r, reply| {
                if rent170(r) {
                    if replay {
                        reply.body = c.lock().unwrap().clone();
                    } else {
                        *c.lock().unwrap() = reply.body.clone();
                    }
                }
            })),
        )
        .await?;
        let out = crate::execution_initial_sol::collect_and_check(
            &server.rpc.endpoint,
            2000,
            &s.payload,
            s.wallet,
            RESERVE,
        )
        .await;
        assert_eq!(server.finish(&dir).await?.len(), 4);
        save(
            &dir,
            "result.json",
            &json!({"replayed_rent_reply":replay,
            "result":format!("{out:#?}"),"collection_requests":4}),
        );
        if replay {
            assert!(format!("{:#}", out.unwrap_err()).contains("native_rpc_id"));
        } else {
            assert_eq!(
                out.unwrap_err().to_string(),
                super::b123_jupiter_fixture_tests::UNPROVEN
            );
            assert!(!captured.lock().unwrap().is_empty());
        }
    }
    Ok(())
}
#[tokio::test]
async fn token2022_ata_total_deadline_cancels_dependent_rent() -> Result<()> {
    let s = Spec::sufficient("absent")?;
    let dir = output("total-deadline");
    let server = Server::start(
        &s,
        Some(Box::new(|r, reply| {
            if r["method"] == "getMultipleAccounts" {
                reply.delay = Duration::from_millis(400);
            }
            if rent170(r) {
                reply.wait_for_cancel = true;
                reply.headers_before_cancel = true;
            }
        })),
    )
    .await?;
    let started = Instant::now();
    let out = crate::execution_initial_sol::collect_and_check(
        &server.rpc.endpoint,
        600,
        &s.payload,
        s.wallet,
        RESERVE,
    )
    .await;
    let elapsed = started.elapsed();
    assert!(format!("{:#}", out.unwrap_err()).contains("native_rpc_timeout"));
    assert!(
        elapsed >= Duration::from_millis(500) && elapsed < Duration::from_millis(900),
        "deadline restarted? {elapsed:?}"
    );
    let trace = server.finish(&dir).await?;
    assert_eq!(trace.len(), 4);
    let rent = trace.iter().find(|r| rent170(&r.request)).unwrap();
    assert!(rent.cancellation_seen);
    assert!(rent.received.duration_since(started) >= Duration::from_millis(350));
    save(
        &dir,
        "deadline.json",
        &json!({"timeout_ms":600,"accounts_delay_ms":400,"elapsed_ms":elapsed.as_millis(),"rent170_cancelled":true}),
    );
    Ok(())
}
#[tokio::test]
async fn token2022_ata_error_cancels_rent_siblings_and_bounds() -> Result<()> {
    let s = Spec::sufficient("absent")?;
    let dir = output("sibling-cancel");
    let server = Server::start(
        &s,
        Some(Box::new(|r, reply| {
            if r["method"] == "getFeeForMessage" {
                reply.delay = Duration::from_millis(200);
                reply.body = b"{".to_vec();
            }
            if r["method"] == "getMinimumBalanceForRentExemption" {
                reply.wait_for_cancel = true;
            }
        })),
    )
    .await?;
    let out = crate::execution_initial_sol::collect_and_check(
        &server.rpc.endpoint,
        2000,
        &s.payload,
        s.wallet,
        RESERVE,
    )
    .await;
    assert!(format!("{:#}", out.unwrap_err()).contains("native_rpc_invalid_json"));
    let trace = server.finish(&dir).await?;
    assert_eq!(trace.len(), 4);
    assert_eq!(trace.iter().filter(|t| t.cancellation_seen).count(), 2);
    for timeout in [0, 30001] {
        let server = Server::start(&s, None).await?;
        let out = crate::execution_initial_sol::collect_and_check(
            &server.rpc.endpoint,
            timeout,
            &s.payload,
            s.wallet,
            RESERVE,
        )
        .await;
        assert!(format!("{:#}", out.unwrap_err()).contains("native_rpc_timeout_bounds"));
        assert!(server
            .finish(&output(&format!("bounds-{timeout}")))
            .await?
            .is_empty());
    }
    let mut s = s;
    s.fee = None;
    let server = Server::start(&s, None).await?;
    let out = crate::execution_initial_sol::collect_and_check(
        &server.rpc.endpoint,
        2000,
        &s.payload,
        s.wallet,
        RESERVE,
    )
    .await;
    assert_eq!(out.unwrap_err().to_string(), "initial_sol_fee_unavailable");
    assert_eq!(server.finish(&output("fee-missing")).await?.len(), 4);
    Ok(())
}

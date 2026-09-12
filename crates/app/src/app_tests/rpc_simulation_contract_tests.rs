use super::rpc_simulation_http_fixture::{valid, Server, Step};
use crate::execution_transaction_rpc_simulation::{
    parse_rpc_simulation_response as parse, verify_serialized_transaction_rpc_simulation as verify,
    RpcSimulationOutcome,
};
use anyhow::Result;
use copybot_config::ExecutionConfig;
use serde_json::{json, Value};
use std::time::Duration;

fn invalid_responses() -> Vec<(String, Value)> {
    let mut cases = Vec::new();
    for path in ["", "/result", "/result/context", "/result/value"] {
        for value in [
            Value::Null,
            json!([]),
            json!(1),
            json!("object"),
            json!(false),
        ] {
            let mut body = valid();
            *body.pointer_mut(path).unwrap() = value.clone();
            cases.push((format!("{path}={value}"), body));
        }
    }
    for (parent, key) in [
        ("", "result"),
        ("", "jsonrpc"),
        ("", "id"),
        ("/result", "context"),
        ("/result", "value"),
        ("/result/context", "slot"),
        ("/result/value", "err"),
    ] {
        let mut body = valid();
        body.pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        cases.push((format!("missing {parent}/{key}"), body));
    }
    for path in ["/jsonrpc", "/id"] {
        for v in [
            json!(null),
            json!("wrong"),
            json!(2),
            json!(2.0),
            json!({}),
            json!([]),
        ] {
            let mut b = valid();
            *b.pointer_mut(path).unwrap() = v.clone();
            cases.push((format!("{path}={v}"), b));
        }
    }
    for v in [
        json!(null),
        json!("0"),
        json!(-1),
        json!(0.0),
        json!(1.5),
        json!(true),
        json!([]),
        json!({}),
        json!(18446744073709551616.0),
    ] {
        let mut b = valid();
        b["result"]["context"]["slot"] = v.clone();
        cases.push((format!("slot={v}"), b));
    }
    for v in [
        json!(false),
        json!(0),
        json!("null"),
        json!({}),
        json!([]),
        json!({"InstructionError":[6,{"Custom":6004}]}),
    ] {
        let mut b = valid();
        b["result"]["value"]["err"] = v.clone();
        cases.push((format!("err={v}"), b));
    }
    for v in [json!(null), json!({"code":-32603,"message":"internal"})] {
        let mut b = valid();
        b["error"] = v.clone();
        cases.push((format!("error+result={v}"), b.clone()));
        b.as_object_mut().unwrap().remove("result");
        cases.push((format!("error={v}"), b));
    }
    cases
}

#[test]
fn rpc_simulation_parser_rejects_incomplete_and_mistyped_data() {
    let cases = invalid_responses();
    for (name, body) in &cases {
        let e = parse(body, "metis").expect_err(name).to_string();
        assert!(e.contains("RPC simulation"), "{name}: {e}");
    }
    eprintln!("B17 parser rejected {} cases", cases.len());
}

#[test]
fn rpc_simulation_minimal_and_extra_fields_pass_with_exact_slot() -> Result<()> {
    for slot in [0, 1, u64::MAX] {
        let mut b = valid();
        b["result"]["context"]["slot"] = json!(slot);
        assert_eq!(parse(&b, "metis")?, RpcSimulationOutcome::Passed { slot });
        b["extra"] = json!({"unknown":true});
        b["result"]["value"]["extension"] = json!("anything");
        b["result"]["context"]["apiVersion"] = json!("x");
        assert_eq!(parse(&b, "metis")?, RpcSimulationOutcome::Passed { slot });
    }
    Ok(())
}

#[test]
fn rpc_simulation_error_retains_bounded_logs_and_pumpswap_annotation() {
    let mut b = valid();
    b["result"]["value"]["err"] = json!({"InstructionError":[6,{"Custom":6004}]});
    b["result"]["value"]["logs"] = json!(["z".repeat(1000)]);
    let error = parse(&b, "pumpswap_direct").unwrap_err().to_string();
    assert!(error.contains("6004:ExceededSlippage"), "{error}");
    assert!(error.contains("logs=["));
    assert!(error.chars().count() < 600);
}

#[test]
fn rpc_simulation_outcome_summary_never_truncates_status() {
    for summary in ["base".into(), "я".repeat(600)] {
        for (outcome, status) in [
            (RpcSimulationOutcome::Skipped, "skipped"),
            (RpcSimulationOutcome::Passed { slot: 0 }, "passed"),
        ] {
            let text = outcome.with_summary(&summary);
            assert!(text.chars().count() <= 500);
            assert!(text.ends_with(&format!("rpc_simulation={status}")));
        }
    }
}

async fn check_http(mut step: Step, timeout: Duration) -> Result<Result<RpcSimulationOutcome>> {
    step.method = "simulateTransaction";
    let server = Server::new(vec![step]).await?;
    let mut config = ExecutionConfig::default();
    config.canary_tiny_submit_enabled = true;
    config.submit_adapter_http_url = server.url.clone();
    let result = verify(&reqwest::Client::new(), &config, "AQID", "metis", timeout).await;
    let requests = server.finish().await?;
    assert_eq!(requests[0]["params"][0], "AQID");
    Ok(result)
}

#[tokio::test]
async fn rpc_simulation_http_matrix_uses_actual_boundary() -> Result<()> {
    for (name, body) in invalid_responses() {
        assert!(
            check_http(Step::json("", body), Duration::from_secs(2))
                .await?
                .is_err(),
            "{name}"
        );
    }
    assert_eq!(
        check_http(Step::json("", valid()), Duration::from_secs(2)).await??,
        RpcSimulationOutcome::Passed { slot: 0 }
    );
    Ok(())
}

#[tokio::test]
async fn rpc_simulation_http_decode_status_timeout_and_transport_fail() -> Result<()> {
    for (body, status, delay, timeout, expected) in [
        ("{", 200, 0, 2000, "JSON decode"),
        ("upstream", 503, 0, 2000, "HTTP 503"),
        ("", 200, 200, 30, "request failed"),
    ] {
        let step = Step {
            method: "",
            body: body.into(),
            status,
            delay: Duration::from_millis(delay),
        };
        let e = check_http(step, Duration::from_millis(timeout))
            .await?
            .unwrap_err()
            .to_string();
        assert!(e.contains(expected), "{e}");
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    drop(listener);
    let mut c = ExecutionConfig::default();
    c.canary_tiny_submit_enabled = true;
    c.submit_adapter_http_url = url;
    assert!(verify(
        &reqwest::Client::new(),
        &c,
        "x",
        "metis",
        Duration::from_secs(1)
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("request failed"));
    Ok(())
}

#[tokio::test]
async fn rpc_simulation_disabled_skips_without_rpc_and_enabled_blank_rejects() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut c = ExecutionConfig::default();
    c.canary_tiny_submit_enabled = false;
    for url in [String::new(), format!("http://{}", listener.local_addr()?)] {
        c.submit_adapter_http_url = url;
        assert_eq!(
            verify(
                &reqwest::Client::new(),
                &c,
                "x",
                "metis",
                Duration::from_secs(1)
            )
            .await?,
            RpcSimulationOutcome::Skipped
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    c.canary_tiny_submit_enabled = true;
    for url in ["", " \n\t"] {
        c.submit_adapter_http_url = url.into();
        assert!(verify(
            &reqwest::Client::new(),
            &c,
            "x",
            "metis",
            Duration::from_secs(1)
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("nonempty submit_adapter_http_url"));
    }
    Ok(())
}

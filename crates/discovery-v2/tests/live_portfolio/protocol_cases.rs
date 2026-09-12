use super::{
    fixture::*,
    rpc::{self, Reply, RpcStub, CLASSIC},
};
use anyhow::Result;
use serde_json::json;
use std::time::Duration;

#[test]
fn rpc_failures_at_each_stage_never_become_successful_empty_inventory() -> Result<()> {
    for stage in [0, 1, 2] {
        for mode in [
            "rpc-error",
            "http-error",
            "bad-json",
            "wrong-id",
            "missing-value",
            "missing-slot",
            "timeout",
            "wrong-version",
            "null-error",
        ] {
            let mut f = Fixture::new()?;
            f.discovery.live_portfolio_request_timeout_ms = 100;
            let rpc = RpcStub::start(move |request| {
                let request_stage = if request["method"] == "getBalance" {
                    0
                } else if request["params"][1]["programId"] == CLASSIC {
                    1
                } else {
                    2
                };
                let mut response = rpc::result(if request_stage == 0 {
                    json!(1_000_000_000)
                } else {
                    json!([])
                });
                if request_stage != stage {
                    return response.into();
                }
                match mode {
                    "rpc-error" => {
                        response = json!({"jsonrpc":"2.0","id":1,"error":{"message":"PRIVATE_RPC_ERROR_MUST_NOT_ESCAPE"}})
                    }
                    "null-error" => response["error"] = json!(null),
                    "wrong-version" => response["jsonrpc"] = json!("1.0"),
                    "wrong-id" => response["id"] = json!(2),
                    "missing-value" => {
                        response["result"].as_object_mut().unwrap().remove("value");
                    }
                    "missing-slot" => {
                        response["result"]["context"]
                            .as_object_mut()
                            .unwrap()
                            .remove("slot");
                    }
                    _ => {}
                }
                let mut reply = Reply::from(response);
                if mode == "http-error" {
                    reply.status = 500;
                }
                if mode == "bad-json" {
                    reply.body = "PRIVATE_RPC_ERROR_MUST_NOT_ESCAPE".into();
                }
                if mode == "timeout" {
                    reply.delay = Duration::from_millis(200);
                }
                reply
            });
            let status = f.build(&rpc)?;
            rpc::assert_requests(&rpc.finish(), &rpc::key('A'), stage + 1);
            let reason = if matches!(mode, "timeout" | "http-error") {
                "live_portfolio_rpc_unavailable"
            } else {
                "live_portfolio_rpc_invalid_response"
            };
            println!("B29 protocol={mode} stage={stage}");
            assert_outcome(&status, Some(reason), true);
            assert!(!serde_json::to_string(&status)?.contains("PRIVATE_RPC_ERROR_MUST_NOT_ESCAPE"));
        }
    }
    Ok(())
}

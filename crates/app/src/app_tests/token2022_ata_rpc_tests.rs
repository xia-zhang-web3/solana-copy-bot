use super::native_rpc_fixture::{Fixture, Reply, Trace};
use super::token2022_ata_inputs_tests::{save, Spec};
use anyhow::Result;
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

pub(super) type Hook = Box<dyn Fn(&Value, &mut Reply) + Send + Sync>;
pub(super) struct Server {
    pub rpc: Fixture,
    records: Arc<Mutex<Vec<Value>>>,
    start: Instant,
}
impl Server {
    pub async fn start(spec: &Spec, hook: Option<Hook>) -> Result<Self> {
        let spec = spec.clone();
        let records = Arc::new(Mutex::new(Vec::<Value>::new()));
        let observed = records.clone();
        let start = Instant::now();
        let rpc = Fixture::start(false, move |r| {
            let method = r["method"].as_str().unwrap();
            let (params, result) = match method {
                "getFeeForMessage" => {
                    assert_eq!(r["id"], "native-funding-fee");
                    (json!([spec.message,{"commitment":"confirmed"}]), json!({"context":{"slot":700},"value":spec.fee}))
                },
                "getMultipleAccounts" => {
                    assert_eq!(r["id"], "native-funding-accounts");
                    (json!([spec.keys,{"commitment":"confirmed","encoding":"base64"}]), json!({"context":{"slot":702},"value":spec.rows}))
                },
                "getMinimumBalanceForRentExemption" => {
                    let len = r["params"][0].as_u64().unwrap();
                    let rent = match len {
                        165 => { assert_eq!(r["id"], "native-funding-classic-ata-rent"); spec.rent },
                        170 => { assert!(r["id"].as_str().unwrap().starts_with("native-funding-token2022-ata-rent-")); spec.rent170 },
                        other => panic!("unsupported rent size {other}"),
                    };
                    (json!([len,{"commitment":"confirmed"}]), json!(rent))
                },
                other => panic!("unexpected RPC: {other}"),
            };
            assert_eq!(r["jsonrpc"],"2.0"); assert_eq!(r["params"],params);
            let response=json!({"jsonrpc":"2.0","id":r["id"],"result":result});
            let mut reply=Reply::json(response);
            if let Some(hook)=&hook { hook(r, &mut reply); }
            let mut list=observed.lock().unwrap();
            assert!(!list.iter().any(|v| v["request"]["method"]==method && v["request"]["params"]==params),"duplicate RPC");
            list.push(json!({"request":r,"response_body":String::from_utf8_lossy(&reply.body),"response_prepared_us":start.elapsed().as_micros(),"provenance":"synthetic keyed facts; mint from later117"}));
            reply
        }).await?;
        Ok(Self {
            rpc,
            records,
            start,
        })
    }
    pub async fn finish(self, dir: &Path) -> Result<Vec<Trace>> {
        let trace = self.rpc.finish().await?;
        save(
            dir,
            "http-exchanges.json",
            &json!(*self.records.lock().unwrap()),
        );
        save(dir,"http-timing.json",&json!(trace.iter().map(|r|json!({"request":r.request,
            "received_us":r.received.duration_since(self.start).as_micros(),
            "completed_us":r.completed.map(|t|t.duration_since(self.start).as_micros()),"cancelled":r.cancellation_seen})).collect::<Vec<_>>()));
        assert!(trace.len() <= 4);
        assert!(trace.iter().all(|r| r.completed.is_some()));
        Ok(trace)
    }
}

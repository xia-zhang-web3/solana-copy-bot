use super::{b53_fixture::Fixture, b53_http::Send};
use anyhow::Result;

async fn preserves(mode: Send) -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.rpc.state.lock().unwrap().mode = mode;
    f.tick(0).await?;
    let order = f.order("audit-a")?;
    let sends = f.sends();
    assert_eq!(sends.len(), 1);
    let preserved = order.tx_signature.as_deref() == sends[0]["signature"].as_str()
        && order.status == copybot_storage_core::EXECUTION_STATUS_CANARY_SUBMITTED;
    f.reopen()?;
    f.seed("audit-b", 2)?;
    f.tick(2).await?;
    let count = f.sends().len();
    f.finish().await?;
    assert!(
        preserved,
        "{mode:?} lost actual dispatch identity: {order:?}"
    );
    assert_eq!(count, 1, "{mode:?} permitted another send while A unknown");
    Ok(())
}
macro_rules! case {
    ($name:ident,$mode:ident) => {
        #[tokio::test]
        async fn $name() -> Result<()> {
            preserves(Send::$mode).await
        }
    };
}
case!(b53_case_body, BadBody);
case!(b53_case_json, BadJson);
case!(b53_case_http503, HttpError);
case!(b53_case_null, MissingResult);
case!(b53_case_rpc_error, RpcError);
case!(b53_case_disconnect, Disconnect);
case!(b53_case_empty, EmptyResult);
case!(b53_case_mismatch, Mismatch);

#[tokio::test]
async fn b53_case_order_write_failure_precedes_http() -> Result<()> {
    let mut f = Fixture::new().await?;
    f.seed("audit-a", 0)?;
    f.conn()?.execute_batch("CREATE TRIGGER reject_dispatch BEFORE UPDATE OF tx_signature ON orders
        WHEN NEW.tx_signature IS NOT NULL BEGIN SELECT RAISE(ABORT,'synthetic dispatch write failure'); END;")?;
    let result = f.tick(0).await;
    let sends = f.sends().len();
    f.finish().await?;
    assert!(result.is_err());
    assert_eq!(sends, 0);
    Ok(())
}

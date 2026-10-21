//! Only a typed established-live forward gap enters the accepted HTTP replay.
use super::*;
use crate::source::durable::recovery::LiveParentGap;

pub(super) async fn process(
    http: &ConfirmedHttpRecovery,
    config: &HttpRecoveryConfig,
    reader: &mut Reader,
    bridge: &mut Bridge<'_>,
    start: Instant,
    update: &SubscribeUpdate,
    telemetry: &DurableIngressTelemetry,
) -> Result<()> {
    let Err(error) = super::process(bridge, ns(start), update, telemetry).await else {
        return Ok(());
    };
    bridge.set_http_hold(true);
    let Some(gap) = error.downcast_ref::<LiveParentGap>() else {
        return Err(error);
    };
    let Some(subscribe_update::UpdateOneof::Block(anchor)) = update.update_oneof.as_ref() else {
        return Err(error);
    };
    ensure!(
        anchor.slot == gap.child.slot
            && anchor.blockhash == gap.child.hash
            && anchor.parent_slot == gap.parent.slot
            && anchor.parent_blockhash == gap.parent.hash,
        "live_gap_anchor_identity"
    );
    let from = bridge
        .restart_live_gap(ns(start), gap, Duration::from_millis(config.timeout_ms))
        .await?;
    // This is the independently captured offending block, not a fabricated live
    // anchor. HTTP must prove the whole overlap/chain and exact anchor Info.
    // Failure here is final, including another typed gap: it is not retried.
    catch_up(http, config, from, anchor, bridge, start, telemetry)
        .await
        .context("live_parent_gap_recovery_refused")?;
    // clear_verified also checks Reader interruption; ACK alone cannot clear a
    // hold after the live capture ended while HTTP was catching up.
    reader.clear_verified();
    Ok(())
}

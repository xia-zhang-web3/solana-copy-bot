//! A held-quote delivery boundary, separate from capture setup and EOF recovery.
use super::super::{association_fixture, association_parent_fixture, b135_fixture::Fixture};
use crate::association_consumer::AssociationConsumer;
use anyhow::{ensure, Context, Result};
use copybot_core_types::association_delivery::{Delivery, DeliveryEvent};
use copybot_ingestion::ReplayInput;
use std::time::Duration;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

pub(super) const SESSION: &str = "b135-held-quote";
pub(super) struct Replay {
    sell: Option<oneshot::Sender<()>>,
    end: Option<oneshot::Sender<()>>,
    task: JoinHandle<Result<()>>,
}
impl Drop for Replay {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Replay {
    pub(super) async fn prepare(
        f: &Fixture,
        consumer: &mut AssociationConsumer,
        tx: mpsc::Sender<ReplayInput>,
    ) -> Result<Self> {
        let names = association_parent_fixture::frames(&f.meta);
        ensure!(names[..3] == ["source", "our", "sell"], "held replay order");
        // Filesystem and the preceding two admissions are setup, not concurrent SELL delivery.
        let frames = names
            .iter()
            .map(|name| {
                std::fs::read(super::super::b135_fixture::inputs().join(format!("{name}.pb")))
            })
            .collect::<std::io::Result<Vec<_>>>()?;
        let (sell, deliver) = oneshot::channel();
        let (end, finish) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut deliver = Some(deliver);
            for (n, payload) in frames.into_iter().enumerate() {
                if n == 2 {
                    deliver.take().context("SELL gate missing")?.await?;
                }
                tx.send(ReplayInput::Update {
                    offset_ns: n as u64 + 1,
                    payload,
                })
                .await?;
            }
            finish.await?;
            tx.send(ReplayInput::End(9)).await?;
            Ok(())
        });
        let replay = Self {
            sell: Some(sell),
            end: Some(end),
            task,
        };
        tokio::time::timeout(Duration::from_secs(4), async {
            while consumer.diagnostic_snapshots().1.admissions < 2 {
                consumer.poll(&f.db.store).await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        let (ingress, stored) = consumer.diagnostic_snapshots();
        ensure!(
            stored.session_starts == 1
                && stored.admissions == 2
                && ingress.processing.durable_ack.count == 3,
            "warm session ACK boundary"
        );
        Ok(replay)
    }
    pub(super) async fn acknowledge_sell(
        &mut self,
        f: &Fixture,
        consumer: &mut AssociationConsumer,
    ) -> Result<()> {
        self.sell
            .take()
            .context("SELL already delivered")?
            .send(())
            .map_err(|_| anyhow::anyhow!("SELL producer stopped"))?;
        while consumer.diagnostic_snapshots().1.admissions < 3 {
            consumer.poll(&f.db.store).await?;
        }
        let (ingress, stored) = consumer.diagnostic_snapshots();
        ensure!(
            stored.session_starts == 1
                && stored.admissions == 3
                && stored.session_gaps == 0
                && ingress.processing.durable_ack.count == 4
                && consumer.pending.is_none(),
            "SELL must be committed, read back and ACKed"
        );
        let session = format!("{SESSION}:0");
        let (count, wire): (i64, String) = f.db.sql.query_row(
            "SELECT (SELECT count(*) FROM association_inbox_events WHERE session=?1),delivery FROM association_inbox_events WHERE session=?1 AND sequence=3",
            [&session], |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let delivery: Delivery = serde_json::from_str(&wire)?;
        let DeliveryEvent::Admission(admission) = delivery.event else {
            anyhow::bail!("specific SELL admission missing");
        };
        ensure!(
            count == 4
                && delivery.session == session
                && delivery.sequence == 3
                && admission.facts.signature == f.meta["sell"]["signature"].as_str().unwrap()
                && admission.facts.slot == f.meta["sell"]["slot"].as_u64().unwrap(),
            "new-session SELL delivery identity"
        );
        ensure!(
            self.end.is_some() && !self.task.is_finished(),
            "EOF gate must remain closed"
        );
        Ok(())
    }
    pub(super) async fn finish(
        &mut self,
        f: &Fixture,
        consumer: &mut AssociationConsumer,
    ) -> Result<()> {
        self.end
            .take()
            .context("EOF already released")?
            .send(())
            .map_err(|_| anyhow::anyhow!("EOF producer stopped"))?;
        tokio::time::timeout(Duration::from_secs(4), async {
            association_fixture::drain(consumer, &f.db).await?;
            (&mut self.task).await??;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}

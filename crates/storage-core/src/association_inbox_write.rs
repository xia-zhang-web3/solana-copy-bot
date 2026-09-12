use super::*;
impl AssociationInbox {
    /// ACK is return Ok only after commit AND exact readback. One writer operation
    /// at a time; failures stop the new path. Retained identities are never evicted.
    pub fn persist(&mut self, d: &Delivery, candidate: &CandidateGeneration) -> Result<()> {
        self.persist_at(d, candidate, chrono::Utc::now())
    }
    /// The supplied app-dequeue clock classifies message metadata only.
    pub fn persist_at(
        &mut self,
        d: &Delivery,
        candidate: &CandidateGeneration,
        observed: chrono::DateTime<chrono::Utc>,
    ) -> Result<()> {
        schema::required(&self.conn)?;
        crate::association_sell_preparation::schema::required(&self.conn)?;
        crate::association_sell_preparation::parent_graph::schema::required(&self.conn)?;
        ensure!(
            !d.session.is_empty() && d.session.len() <= 128,
            "invalid inbox session"
        );
        let seq = i64::try_from(d.sequence)?;
        let wire = serde_json::to_string(d)?;
        ensure!(
            wire.len() <= self.limits.bytes,
            "inbox event exceeds byte limit"
        );
        crate::association_inbox::ordered::required(&self.conn, self.mode)?;
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old: Option<String> = tx
            .query_row(
                "SELECT delivery FROM association_inbox_events WHERE session=?1 AND sequence=?2",
                params![d.session, seq],
                |r| r.get(0),
            )
            .optional()?;
        let signature = match &d.event {
            DeliveryEvent::Admission(a) => Some(a.facts.signature.as_str()),
            DeliveryEvent::Duplicate { original, .. } => Some(original.facts.signature.as_str()),
            DeliveryEvent::Terminal { signature, .. } | DeliveryEvent::Late { signature, .. } => {
                Some(signature.as_str())
            }
            DeliveryEvent::Session(_) | DeliveryEvent::Parent(_) => None,
        };
        let fresh = signature
            .map(|s| identity(&tx, s))
            .transpose()?
            .flatten()
            .is_none()
            && matches!(d.event, DeliveryEvent::Admission(_));
        if let Some(old) = old {
            ensure!(old == wire, "inbox event identity collision");
            // Do not rebind on a retry after commit-before-ACK, even when caller sees B.
            if let Some(s) = signature {
                ensure!(
                    identity(&tx, s)?.is_some(),
                    "durable event without identity"
                );
            }
        } else {
            apply(&tx, d, candidate)?;
            tx.execute(
                "INSERT INTO association_inbox_events(session,sequence,delivery) VALUES(?1,?2,?3)",
                params![d.session, seq, wire],
            )?;
        }
        let saved: String = tx.query_row(
            "SELECT delivery FROM association_inbox_events WHERE session=?1 AND sequence=?2",
            params![d.session, seq],
            |r| r.get(0),
        )?;
        ensure!(saved == wire, "inbox event write ignored or changed");
        let mut proof = crate::association_sell_preparation::Readback::new(self.mode);
        if matches!(&d.event, DeliveryEvent::Parent(_)) {
            crate::association_sell_preparation::parent(&tx, d, &mut proof)?;
        }
        crate::association_sell_preparation::event(
            &tx,
            signature,
            fresh,
            (observed.timestamp(), observed.timestamp_subsec_nanos()),
            self.limits,
            &mut proof,
        )?;
        let expected = signature.map(|s| identity(&tx, s)).transpose()?.flatten();
        check_budget(&tx, self.limits, self.mode)?;
        proof.verify(&tx)?;
        tx.commit()?;
        proof.verify(&self.conn)?;
        let saved: String = self.conn.query_row(
            "SELECT delivery FROM association_inbox_events WHERE session=?1 AND sequence=?2",
            params![d.session, seq],
            |r| r.get(0),
        )?;
        ensure!(saved == wire, "inbox committed event readback mismatch");
        if let Some(s) = signature {
            ensure!(
                identity(&self.conn, s)? == expected,
                "inbox committed state readback mismatch"
            );
        }
        Ok(())
    }
}
fn apply(c: &Connection, d: &Delivery, candidate: &CandidateGeneration) -> Result<()> {
    match &d.event {
        DeliveryEvent::Admission(a) => {
            let s = &a.facts.signature;
            ensure!(
                !s.is_empty() && a.facts.slot > 0 && !a.info.encoded.is_empty(),
                "invalid checked admission"
            );
            let old = identity(c, s)?;
            if let Some(old) = old {
                // Envelope message time is provenance, not transaction identity.
                if old.admission.facts != a.facts || old.admission.info != a.info {
                    set_conflict(c, s)?;
                }
            } else {
                let a = serde_json::to_string(a)?;
                let candidate = serde_json::to_string(candidate)?;
                c.execute("INSERT INTO association_inbox_identities(signature,admission,candidate,first_session,first_sequence,terminal,conflict,recovery) VALUES(?1,?2,?3,?4,?5,NULL,0,0)",params![s,a,candidate,d.session,i64::try_from(d.sequence)?])?;
                let row =
                    identity(c, s)?.ok_or_else(|| anyhow::anyhow!("inbox admission ignored"))?;
                ensure!(
                    serde_json::to_string(&row.admission)? == a
                        && serde_json::to_string(&row.candidate)? == candidate
                        && row.first_session == d.session
                        && row.first_sequence == d.sequence
                        && row.terminal.is_none()
                        && !row.conflict
                        && !row.recovery,
                    "inbox admission readback mismatch"
                );
            }
        }
        DeliveryEvent::Duplicate {
            original,
            observed_info,
            observed_slot,
            ..
        } => {
            let s = &original.facts.signature;
            let row = identity(c, s)?
                .ok_or_else(|| anyhow::anyhow!("duplicate before durable admission"))?;
            if row.admission.facts != original.facts
                || row.admission.info != *observed_info
                || row.admission.facts.slot != *observed_slot
            {
                set_conflict(c, s)?;
            }
        }
        DeliveryEvent::Terminal {
            signature,
            expected,
            result,
        } => {
            let row = identity(c, signature)?
                .ok_or_else(|| anyhow::anyhow!("terminal before durable admission"))?;
            if row.admission.facts != expected.facts || row.admission.info != expected.info {
                set_conflict(c, signature)?;
                return Ok(());
            }
            if let Some(first) = row.terminal {
                if first != *result {
                    set_conflict(c, signature)?;
                }
            } else {
                let value = serde_json::to_string(result)?;
                c.execute("UPDATE association_inbox_identities SET terminal=?2 WHERE signature=?1 AND terminal IS NULL",params![signature,value])?;
                ensure!(
                    identity(c, signature)?.is_some_and(|r| r.terminal.as_ref() == Some(result)),
                    "inbox terminal ignored"
                );
            }
        }
        DeliveryEvent::Late {
            signature,
            original,
            evidence,
        } => {
            let row = identity(c, signature)?
                .ok_or_else(|| anyhow::anyhow!("late before durable admission"))?;
            ensure!(row.terminal.is_some(), "late before durable terminal");
            if row.terminal.as_ref() != Some(original)
                || !matches!(evidence,Late::ProviderAssertion(a) if row.terminal==Some(Terminal::ProviderAsserted(a.clone())))
            {
                set_conflict(c, signature)?;
            }
        }
        DeliveryEvent::Session(_) | DeliveryEvent::Parent(_) => {}
    }
    Ok(())
}
fn set_conflict(c: &Connection, s: &str) -> Result<()> {
    c.execute(
        "UPDATE association_inbox_identities SET conflict=1 WHERE signature=?1",
        [s],
    )?;
    ensure!(
        identity(c, s)?.is_some_and(|r| r.conflict),
        "inbox conflict ignored"
    );
    Ok(())
}

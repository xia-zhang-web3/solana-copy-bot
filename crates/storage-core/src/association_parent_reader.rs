use super::*;
use copybot_core_types::association_delivery::ProviderAssertion;
use std::collections::BTreeSet;
/// One reader/budget for all relations and contributors in one SQLite snapshot.
/// Every indexed lookup and emitted path element consumes count; serialized
/// payloads/container charges consume bytes even on repeated/shared path visits.
pub(in crate::association_sell_preparation) struct Reader<'a> {
    c: &'a Connection,
    limits: InboxLimits,
    count: usize,
    bytes: usize,
    exhausted: bool,
    has_observations: bool,
    dependencies: BTreeSet<String>,
    paths: Vec<ProviderPath>,
}
impl<'a> Reader<'a> {
    pub(in crate::association_sell_preparation) fn new(
        c: &'a Connection,
        limits: InboxLimits,
    ) -> Result<Self> {
        let has_observations = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM association_parent_blocks)",
            [],
            |r| r.get(0),
        )?;
        let mut reader = Self {
            c,
            limits,
            count: 0,
            bytes: 0,
            exhausted: false,
            has_observations,
            dependencies: BTreeSet::new(),
            paths: vec![],
        };
        let _ = reader.charge(512);
        Ok(reader)
    }
    pub(in crate::association_sell_preparation) fn charge(
        &mut self,
        bytes: usize,
    ) -> std::result::Result<(), Check> {
        if self.exhausted
            || self.count >= self.limits.count
            || bytes > self.limits.bytes.saturating_sub(self.bytes)
        {
            self.exhausted = true;
            return Err(Check::Unknown(Reason::ParentTraversalBound));
        }
        self.count += 1;
        self.bytes += bytes;
        Ok(())
    }
    fn node(&mut self, k: &BlockKey) -> Result<Option<Check>> {
        if let Err(e) = self.charge(512 + k.hash.len()) {
            return Ok(Some(e));
        }
        self.dependencies.insert(k.hash.clone());
        let known:Option<(String,Option<String>)>=self.c.query_row("SELECT first_slot,contradiction_slot FROM association_parent_hashes WHERE block_hash=?1",[&k.hash],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        Ok(known
            .filter(|(slot, conflict)| slot != &k.slot.to_string() || conflict.is_some())
            .map(|_| Check::Blocked(Reason::ParentHashSlotConflict)))
    }
    pub(in crate::association_sell_preparation) fn ordered(
        &mut self,
        a: &ProviderAssertion,
        b: &ProviderAssertion,
    ) -> Result<Check> {
        if self.exhausted {
            return Ok(Check::Unknown(Reason::ParentTraversalBound));
        }
        if a.signature == b.signature {
            return Ok(Check::Blocked(Reason::SignatureSubstitution));
        }
        let earlier = BlockKey {
            slot: a.slot,
            hash: a.blockhash.clone(),
        };
        let later = BlockKey {
            slot: b.slot,
            hash: b.blockhash.clone(),
        };
        if a.slot == b.slot {
            if a.blockhash != b.blockhash {
                return Ok(Check::Unknown(Reason::DifferentBlockhash));
            }
            if a.transaction_index >= b.transaction_index {
                return Ok(Check::Blocked(Reason::NonIncreasingIndex));
            }
            // Preserve typed90 fixtures and API80: parent availability is not a
            // precondition for full-Info within-block ordering. Known slot alias
            // evidence for a canonical hash still invalidates its identity.
            if valid_hash(&earlier.hash) {
                if let Some(e) = self.node(&earlier)? {
                    return Ok(e);
                }
            }
            return Ok(Check::ProviderOrderedWithinBlock);
        }
        if !valid_hash(&earlier.hash) || !valid_hash(&later.hash) {
            return Ok(Check::Blocked(Reason::ParentEndpointMalformed));
        }
        if a.slot >= b.slot {
            return Ok(Check::Blocked(Reason::ParentNotAncestor));
        }
        for k in [&earlier, &later] {
            if let Some(e) = self.node(k)? {
                return Ok(e);
            }
        }
        let mut cursor = later.clone();
        let mut edges = vec![];
        while cursor != earlier {
            if cursor.slot <= earlier.slot {
                return Ok(Check::Blocked(Reason::ParentBranchMismatch));
            }
            if let Err(e) = self.charge(512 + cursor.hash.len()) {
                return Ok(e);
            }
            self.dependencies.insert(cursor.hash.clone());
            let wire_key = key(&cursor)?;
            let size:Option<(bool,usize,usize)>=self.c.query_row("SELECT contradiction IS NOT NULL,length(CAST(first_observation AS BLOB)),length(CAST(first_session AS BLOB)) FROM association_parent_blocks WHERE block_key=?1",[&wire_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
            let Some((conflict, n, session_bytes)) = size else {
                return Ok(Check::Unknown(Reason::ParentGap));
            };
            if conflict {
                return Ok(Check::Blocked(Reason::ParentConflict));
            }
            let Some(bytes) = n
                .checked_add(session_bytes)
                .and_then(|v| v.checked_add(512))
            else {
                return Ok(Check::Unknown(Reason::ParentTraversalBound));
            };
            if let Err(e) = self.charge(bytes) {
                return Ok(e);
            }
            let (raw,session,sequence):(String,String,u64)=self.c.query_row("SELECT first_observation,first_session,first_sequence FROM association_parent_blocks WHERE block_key=?1",[wire_key],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
            let observation: ParentObservation = serde_json::from_str(&raw)?;
            ensure!(
                observation.child == cursor && observation.issue == observation.expected_issue(),
                "corrupt parent graph identity/validation"
            );
            if let Some(issue) = observation.issue {
                use copybot_core_types::association_parent::ParentIssue;
                return Ok(match issue {
                    ParentIssue::MissingChildHash | ParentIssue::MissingParentHash => {
                        Check::Unknown(Reason::ParentMissingData)
                    }
                    _ => Check::Blocked(Reason::ParentMalformed),
                });
            }
            if let Some(e) = self.node(&observation.parent)? {
                return Ok(e);
            }
            let edge = ParentEdgeRef {
                child: cursor,
                parent: observation.parent.clone(),
                session,
                sequence,
            };
            if let Err(e) = self.charge(512 + serde_json::to_vec(&edge)?.len()) {
                return Ok(e);
            }
            cursor = observation.parent;
            edges.push(edge);
        }
        if let Err(e) = self.charge(
            512 + a.signature.len() + b.signature.len() + earlier.hash.len() + later.hash.len(),
        ) {
            return Ok(e);
        }
        self.paths.push(ProviderPath {
            earlier_signature: a.signature.clone(),
            later_signature: b.signature.clone(),
            earlier,
            later,
            edges,
        });
        Ok(Check::ProviderOrderedAcrossBlocks)
    }
    pub(in crate::association_sell_preparation) fn finish(
        self,
    ) -> (Vec<ProviderPath>, Vec<String>) {
        // Historical full inboxes need no new reservation while graph is empty.
        // The first header starts one bounded bootstrap using its existing row.
        let dependencies = if self.has_observations {
            self.dependencies.into_iter().collect()
        } else {
            vec![]
        };
        (self.paths, dependencies)
    }
}

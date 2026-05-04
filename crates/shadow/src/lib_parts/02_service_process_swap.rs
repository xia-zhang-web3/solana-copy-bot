impl ShadowService {
    pub fn process_swap(
        &self,
        store: &SqliteStore,
        swap: &SwapEvent,
        follow_snapshot: &FollowSnapshot,
        now: DateTime<Utc>,
    ) -> Result<ShadowProcessOutcome> {
        if !self.config.enabled {
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::Disabled));
        }
        let Some(candidate) = to_shadow_candidate(swap) else {
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::NotSolLeg));
        };
        let latency_ms = (now - swap.ts_utc).num_milliseconds();
        let runtime_followed = follow_snapshot.is_active(&swap.wallet);
        let temporal_followed = follow_snapshot.is_followed_at(&swap.wallet, swap.ts_utc);
        if runtime_followed && !temporal_followed {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                "shadow runtime_follow_stale_temporal_miss"
            );
        }
        if !runtime_followed && temporal_followed {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                "shadow runtime_not_followed_temporal_hit"
            );
        }
        let is_followed = temporal_followed;
        let is_unfollowed_sell_exit = !is_followed
            && candidate.side == "sell"
            && store.has_shadow_lots(&swap.wallet, &candidate.token)?;
        if is_followed {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                runtime_followed,
                temporal_followed,
                "shadow followed wallet swap reached pipeline"
            );
        } else if is_unfollowed_sell_exit {
            info!(
                wallet = %swap.wallet,
                token = %candidate.token,
                side = %candidate.side,
                leader_notional_sol = candidate.leader_notional_sol,
                latency_ms,
                "shadow unfollowed sell exit allowed"
            );
        }
        if !is_followed && !is_unfollowed_sell_exit {
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::NotFollowed));
        }
        let below_notional =
            if let (Some(exact_leader_notional_lamports), Some(min_notional_lamports)) = (
                candidate.exact_leader_notional_lamports,
                self.min_leader_notional_lamports,
            ) {
                exact_leader_notional_lamports < min_notional_lamports
            } else {
                candidate.leader_notional_sol < self.config.min_leader_notional_sol
            };
        if !is_unfollowed_sell_exit && below_notional {
            log_gate_drop(
                "notional",
                ShadowDropReason::BelowNotional,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(
                ShadowDropReason::BelowNotional,
            ));
        }

        if !is_unfollowed_sell_exit
            && latency_ms > (self.config.max_signal_lag_seconds as i64 * 1_000)
        {
            log_gate_drop(
                "lag",
                ShadowDropReason::LagExceeded,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(ShadowDropReason::LagExceeded));
        }
        if candidate.side == "buy" {
            if let Some(reason) =
                self.drop_reason_for_buy_quality_gate(store, &candidate.token, swap.ts_utc, now)?
            {
                log_gate_drop(
                    "quality",
                    reason,
                    swap,
                    &candidate,
                    latency_ms,
                    runtime_followed,
                    temporal_followed,
                    is_unfollowed_sell_exit,
                );
                return Ok(ShadowProcessOutcome::Dropped(reason));
            }
        }

        let copy_notional_lamports = match (
            self.copy_notional_lamports,
            candidate.exact_leader_notional_lamports,
        ) {
            (Some(config_copy_notional_lamports), Some(exact_leader_notional_lamports)) => {
                Some(std::cmp::min(
                    config_copy_notional_lamports,
                    exact_leader_notional_lamports,
                ))
            }
            _ => None,
        };
        let copy_notional_sol = copy_notional_lamports
            .map(lamports_to_sol)
            .unwrap_or_else(|| {
                self.config
                    .copy_notional_sol
                    .min(candidate.leader_notional_sol)
            });
        if copy_notional_sol <= EPS || candidate.price_sol_per_token <= EPS {
            log_gate_drop(
                "sizing",
                ShadowDropReason::InvalidSizing,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(
                ShadowDropReason::InvalidSizing,
            ));
        }
        let exact_qty = match scaled_exact_shadow_qty(
            candidate.exact_token_qty,
            candidate.exact_leader_notional_lamports,
            copy_notional_lamports,
            copy_notional_sol,
        ) {
            ScaledExactShadowQty::Exact(value) => Some(value),
            ScaledExactShadowQty::Approximate => None,
            ScaledExactShadowQty::InvalidZeroRaw => {
                log_gate_drop(
                    "sizing",
                    ShadowDropReason::InvalidSizing,
                    swap,
                    &candidate,
                    latency_ms,
                    runtime_followed,
                    temporal_followed,
                    is_unfollowed_sell_exit,
                );
                return Ok(ShadowProcessOutcome::Dropped(
                    ShadowDropReason::InvalidSizing,
                ));
            }
        };
        let signal_id = format!(
            "shadow:{}:{}:{}:{}",
            swap.signature, swap.wallet, candidate.side, candidate.token
        );
        let (signal_notional_lamports, signal_notional_origin) =
            if let Some(exact_notional) = copy_notional_lamports {
                (
                    Some(exact_notional),
                    COPY_SIGNAL_NOTIONAL_ORIGIN_EXACT_LAMPORTS.to_string(),
                )
            } else {
                (
                    sol_to_lamports_ceil(copy_notional_sol),
                    COPY_SIGNAL_NOTIONAL_ORIGIN_APPROXIMATE.to_string(),
                )
            };
        let signal = CopySignalRow {
            signal_id: signal_id.clone(),
            wallet_id: swap.wallet.clone(),
            side: candidate.side.clone(),
            token: candidate.token.clone(),
            notional_sol: copy_notional_sol,
            notional_lamports: signal_notional_lamports,
            notional_origin: signal_notional_origin,
            ts: swap.ts_utc,
            status: "shadow_recorded".to_string(),
        };
        if !store.insert_copy_signal(&signal)? {
            log_gate_drop(
                "dedupe",
                ShadowDropReason::DuplicateSignal,
                swap,
                &candidate,
                latency_ms,
                runtime_followed,
                temporal_followed,
                is_unfollowed_sell_exit,
            );
            return Ok(ShadowProcessOutcome::Dropped(
                ShadowDropReason::DuplicateSignal,
            ));
        }

        let (close, has_open_lots_after_signal) = match candidate.side.as_str() {
            "buy" => {
                let qty = copy_notional_sol / candidate.price_sol_per_token;
                if qty > EPS {
                    let _ = store.insert_shadow_lot_exact(
                        &swap.wallet,
                        &candidate.token,
                        qty,
                        exact_qty,
                        copy_notional_sol,
                        swap.ts_utc,
                    )?;
                }
                (copybot_storage::ShadowCloseOutcome::default(), Some(true))
            }
            "sell" => {
                let qty = copy_notional_sol / candidate.price_sol_per_token;
                let close = store.close_shadow_lots_fifo_atomic_exact(
                    &signal_id,
                    &swap.wallet,
                    &candidate.token,
                    qty,
                    exact_qty,
                    candidate.price_sol_per_token,
                    swap.ts_utc,
                )?;
                (close, Some(close.has_open_lots_after))
            }
            _ => {
                log_gate_drop(
                    "side",
                    ShadowDropReason::UnsupportedSide,
                    swap,
                    &candidate,
                    latency_ms,
                    runtime_followed,
                    temporal_followed,
                    is_unfollowed_sell_exit,
                );
                return Ok(ShadowProcessOutcome::Dropped(
                    ShadowDropReason::UnsupportedSide,
                ));
            }
        };

        Ok(ShadowProcessOutcome::Recorded(ShadowSignalResult {
            signal_id,
            wallet_id: swap.wallet.clone(),
            side: candidate.side,
            token: candidate.token,
            notional_sol: copy_notional_sol,
            latency_ms,
            closed_qty: close.closed_qty,
            realized_pnl_sol: close.realized_pnl_sol,
            has_open_lots_after_signal,
        }))
    }
}

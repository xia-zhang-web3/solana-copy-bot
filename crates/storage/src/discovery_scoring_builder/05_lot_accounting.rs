use super::*;

pub(super) fn apply_swap_lot_accounting(
    open_lots: &mut HashMap<WalletTokenKey, VecDeque<BuilderLot>>,
    swap: &SwapEvent,
) -> LotAccountingStep {
    let mut step = LotAccountingStep::default();
    if is_sol_buy(swap) {
        let token = swap.token_out.clone();
        let lot = BuilderLot {
            buy_signature: swap.signature.clone(),
            wallet_id: swap.wallet.clone(),
            token: token.clone(),
            qty: swap.amount_out.max(0.0),
            cost_sol: swap.amount_in.max(0.0),
            opened_ts: swap.ts_utc,
        };
        open_lots
            .entry(WalletTokenKey {
                wallet_id: swap.wallet.clone(),
                token,
            })
            .or_default()
            .push_back(lot.clone());
        step.mutations.push(LotMutationAction::Upsert(lot));
        return step;
    }

    if !is_sol_sell(swap) {
        return step;
    }

    let key = WalletTokenKey {
        wallet_id: swap.wallet.clone(),
        token: swap.token_in.clone(),
    };
    let lots = open_lots.entry(key.clone()).or_default();
    let mut qty_remaining = swap.amount_in.max(0.0);
    if qty_remaining <= 1e-12 || swap.amount_out <= 0.0 {
        if lots.is_empty() {
            open_lots.remove(&key);
        }
        return step;
    }
    let mut segment_index = 0i64;
    while qty_remaining > 1e-12 {
        let Some(mut lot) = lots.pop_front() else {
            break;
        };
        if lot.qty <= 1e-12 {
            step.mutations
                .push(LotMutationAction::Delete(lot.buy_signature.clone()));
            continue;
        }
        let take_qty = qty_remaining.min(lot.qty);
        let lot_fraction = take_qty / lot.qty;
        let cost_part = lot.cost_sol * lot_fraction;
        let proceeds_part = swap.amount_out * (take_qty / swap.amount_in.max(1e-12));
        let pnl_sol = proceeds_part - cost_part;
        let remaining_qty = (lot.qty - take_qty).max(0.0);
        let remaining_cost = (lot.cost_sol - cost_part).max(0.0);
        step.close_rows.push(PendingCloseFactRow {
            sell_signature: swap.signature.clone(),
            segment_index,
            wallet_id: swap.wallet.clone(),
            token: swap.token_in.clone(),
            closed_ts: swap.ts_utc,
            activity_day: swap.ts_utc.date_naive(),
            pnl_sol,
            hold_seconds: (swap.ts_utc - lot.opened_ts).num_seconds().max(0),
            win: pnl_sol > 0.0,
        });
        if remaining_qty <= 1e-12 {
            step.mutations
                .push(LotMutationAction::Delete(lot.buy_signature.clone()));
        } else {
            lot.qty = remaining_qty;
            lot.cost_sol = remaining_cost;
            step.mutations.push(LotMutationAction::Upsert(lot.clone()));
            lots.push_front(lot);
        }
        qty_remaining -= take_qty;
        segment_index += 1;
    }
    if lots.is_empty() {
        open_lots.remove(&key);
    }

    step
}

pub(super) fn ensure_builder_open_lots_loaded(
    store: &SqliteStore,
    builder: &mut DiscoveryScoringReplayBuilder,
    wallet_id: &str,
    token: &str,
) -> Result<()> {
    if !builder.lazy_open_lot_loading {
        return Ok(());
    }
    let key = WalletTokenKey {
        wallet_id: wallet_id.to_string(),
        token: token.to_string(),
    };
    if !builder.loaded_open_lot_keys.insert(key.clone()) {
        return Ok(());
    }
    let loaded = load_open_lots_for_wallet_token(&store.conn, wallet_id, token)?;
    if !loaded.is_empty() {
        builder.open_lots.insert(key, loaded);
    }
    Ok(())
}

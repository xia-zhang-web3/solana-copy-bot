{
                let diagnostics = self.snapshot_run_cycle_publication_boundary_diagnostics(
                    store,
                    "recompute",
                    publish_due,
                    true,
                )?;
                Self::log_run_cycle_publication_boundary(&diagnostics);
                (
                    publish_due,
                    followlist_activations_suppressed,
                    followlist_deactivations_suppressed,
                    metrics_persistence_suppressed,
                    self.build_wallet_snapshots_from_cached(store, &swaps, now)?,
                    swaps.len(),
                    "raw_window",
                    window_start,
                    metrics_window_start,
                )
}

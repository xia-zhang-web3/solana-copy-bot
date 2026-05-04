impl DiscoveryService {
    fn prepare_run_cycle_window_decide(
        &self,
        store: &SqliteStore,
        state: &mut DiscoveryWindowState,
        window_start: DateTime<Utc>,
        metrics_window_start: DateTime<Utc>,
        short_retention_window: bool,
        fetch_preparation: RunCycleWindowFetchPreparation,
        recent_published_follow_wallets: Option<&HashSet<String>>,
        bootstrap_degraded_follow_wallets: Option<&HashSet<String>>,
    ) -> Result<RunCycleWindowPreparation> {
        let RunCycleWindowFetchPreparation {
            publish_due,
            fetch_progress,
            delta_fetched,
            swaps_evicted_due_cap,
            swaps_warm_loaded,
        } = fetch_preparation;
        let raw_window_history_incomplete =
            raw_window_history_incomplete_for_followlist_or_metrics(state);
        let followlist_deactivations_suppressed =
            state.cap_truncation_deactivations_suppressed(raw_window_history_incomplete);
        let cap_truncation_telemetry =
            snapshot_cap_truncation_telemetry(&state, followlist_deactivations_suppressed);
        let persisted_rebuild_checkpoint_present =
            store.load_discovery_persisted_rebuild_state()?.is_some();
        if state.consume_cap_truncation_deactivation_guard_cycle() {
            maybe_warn_on_cap_truncation_deactivation_guard_expiry(
                state,
                followlist_deactivations_suppressed,
            );
        }
        state.bootstrap_from_persisted_metrics = false;
        state.truncated_warm_restore_bootstrap = false;
        state.trusted_selection_bootstrap_pending = false;
        let swaps_window = state.swaps.len();
        let persisted_raw_window_complete =
            if swaps_window == 0 || raw_window_history_incomplete || short_retention_window {
                self.persisted_observed_swaps_cover_window(store, window_start)?
            } else {
                false
            };
        let prepared_cycle = if swaps_window == 0 {
            state.clear_cap_truncation();
            state.last_snapshot_bucket = None;
            state.last_summary = None;
            state.clear_exact_current_raw_truth();
            if persisted_raw_window_complete {
                PreparedCycleState::PersistedRecompute {
                    publish_due,
                    scoring_source: "raw_window_persisted_stream",
                    empty_window_degraded_scoring_source:
                        "published_universe_raw_window_unavailable",
                    empty_window_bootstrap_degraded_scoring_source:
                        "bootstrap_degraded_publication_truth_raw_window_unavailable",
                    empty_window_unusable_scoring_source:
                        "raw_window_unusable_no_recent_published_universe",
                }
            } else if let Some(active_wallets) = recent_published_follow_wallets.cloned() {
                PreparedCycleState::Degraded {
                    publish_due,
                    active_wallets,
                    scoring_source: "published_universe_raw_window_unavailable",
                }
            } else if let Some(active_wallets) = bootstrap_degraded_follow_wallets.cloned() {
                PreparedCycleState::BootstrapDegraded {
                    active_wallets,
                    scoring_source: "bootstrap_degraded_publication_truth_raw_window_unavailable",
                }
            } else {
                PreparedCycleState::Unusable {
                    publish_due,
                    scoring_source: "raw_window_unusable_no_recent_published_universe",
                }
            }
        } else if persisted_rebuild_checkpoint_present {
            PreparedCycleState::PersistedRecompute {
                publish_due,
                scoring_source: "raw_window_persisted_stream",
                empty_window_degraded_scoring_source: if raw_window_history_incomplete {
                    "published_universe_raw_window_degraded"
                } else if short_retention_window {
                    "published_universe_short_retention_degraded"
                } else {
                    "published_universe_raw_window_unavailable"
                },
                empty_window_bootstrap_degraded_scoring_source: if raw_window_history_incomplete {
                    "bootstrap_degraded_publication_truth_raw_window_degraded"
                } else if short_retention_window {
                    "bootstrap_degraded_publication_truth_short_retention_degraded"
                } else {
                    "bootstrap_degraded_publication_truth_raw_window_unavailable"
                },
                empty_window_unusable_scoring_source: if raw_window_history_incomplete {
                    "raw_window_incomplete_no_recent_published_universe"
                } else if short_retention_window {
                    "raw_window_short_retention_no_recent_published_universe"
                } else {
                    "raw_window_unusable_no_recent_published_universe"
                },
            }
        } else if state.last_snapshot_bucket == Some(metrics_window_start)
            && state.last_summary.is_some()
        {
            PreparedCycleState::Cached {
                publish_due,
                followlist_activations_suppressed: false,
                followlist_deactivations_suppressed,
                summary: state
                    .last_summary
                    .clone()
                    .expect("checked last_summary exists above"),
                current_raw: state.last_exact_current_raw_truth.clone(),
            }
        } else if raw_window_history_incomplete || short_retention_window {
            state.bootstrap_from_persisted_metrics = false;
            state.truncated_warm_restore_bootstrap = false;
            if persisted_raw_window_complete {
                PreparedCycleState::PersistedRecompute {
                    publish_due,
                    scoring_source: "raw_window_persisted_stream",
                    empty_window_degraded_scoring_source: if raw_window_history_incomplete {
                        "published_universe_raw_window_degraded"
                    } else {
                        "published_universe_short_retention_degraded"
                    },
                    empty_window_bootstrap_degraded_scoring_source: if raw_window_history_incomplete
                    {
                        "bootstrap_degraded_publication_truth_raw_window_degraded"
                    } else {
                        "bootstrap_degraded_publication_truth_short_retention_degraded"
                    },
                    empty_window_unusable_scoring_source: if raw_window_history_incomplete {
                        "raw_window_incomplete_no_recent_published_universe"
                    } else {
                        "raw_window_short_retention_no_recent_published_universe"
                    },
                }
            } else if let Some(active_wallets) = recent_published_follow_wallets.cloned() {
                PreparedCycleState::Degraded {
                    publish_due,
                    active_wallets,
                    scoring_source: if raw_window_history_incomplete {
                        "published_universe_raw_window_degraded"
                    } else {
                        "published_universe_short_retention_degraded"
                    },
                }
            } else if let Some(active_wallets) = bootstrap_degraded_follow_wallets.cloned() {
                PreparedCycleState::BootstrapDegraded {
                    active_wallets,
                    scoring_source: if raw_window_history_incomplete {
                        "bootstrap_degraded_publication_truth_raw_window_degraded"
                    } else {
                        "bootstrap_degraded_publication_truth_short_retention_degraded"
                    },
                }
            } else {
                PreparedCycleState::Unusable {
                    publish_due,
                    scoring_source: if raw_window_history_incomplete {
                        "raw_window_incomplete_no_recent_published_universe"
                    } else {
                        "raw_window_short_retention_no_recent_published_universe"
                    },
                }
            }
        } else {
            PreparedCycleState::Recompute {
                publish_due,
                followlist_activations_suppressed: false,
                followlist_deactivations_suppressed,
                metrics_persistence_suppressed: false,
                swaps: state.swaps.clone(),
            }
        };

        return Ok(RunCycleWindowPreparation {
            swaps_window,
            fetch_progress,
            cap_truncation_telemetry,
            delta_fetched,
            swaps_evicted_due_cap,
            swaps_warm_loaded,
            prepared_cycle,
        });

    }
}

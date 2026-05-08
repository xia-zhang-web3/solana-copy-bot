    #[test]
    fn recent_swap_signature_dedupe_rejects_duplicate_until_evicted() {
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-1",
        ));
        assert!(
            !note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-1",
            ),
            "duplicate signature should be rejected while still inside bounded dedupe window"
        );

        for idx in 0..RECENT_SWAP_SIGNATURE_DEDUPE_CAPACITY {
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &format!("sig-evict-{idx}"),
            ));
        }

        assert!(
            note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-1",
            ),
            "signature should become admissible again once it is evicted from bounded dedupe state"
        );
    }

    #[test]
    fn recent_swap_signature_dedupe_allows_retry_after_forget() {
        let mut recent_signatures = HashSet::new();
        let mut recent_signature_order = VecDeque::new();

        assert!(note_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-retry",
        ));
        assert!(
            !note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-retry",
            ),
            "signature should stay deduped until the failed enqueue rollback forgets it"
        );

        forget_recent_swap_signature(
            &mut recent_signatures,
            &mut recent_signature_order,
            "sig-retry",
        );

        assert!(
            note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                "sig-retry",
            ),
            "rollback after failed enqueue should make the signature admissible again"
        );
    }

    #[test]
    fn persist_relevant_observed_swap_rolls_back_recent_signature_on_write_error() -> Result<()> {
        let (_store, db_path) = make_test_store("relevant-observed-swap-write-failure")?;
        let conn = rusqlite::Connection::open(&db_path)?;
        conn.execute_batch(
            "CREATE TRIGGER fail_relevant_observed_swap_insert
             BEFORE INSERT ON observed_swaps
             BEGIN
                 SELECT RAISE(FAIL, 'disk I/O error: Error code 4874: I/O error within the xShmMap method');
             END;",
        )?;

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start(db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string())?;
            let swap = test_swap("sig-relevant-write-failure");
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();

            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert!(
                !note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "signature should stay deduped until the failed relevant write forgets it"
            );

            let error = persist_relevant_observed_swap(
                &writer,
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap,
            )
            .await
            .expect_err("fatal relevant observed-swap write must bubble");
            let error_text = format!("{error:#}");
            assert!(
                error_text.contains("failed to insert observed swap batch with activity days"),
                "expected fatal writer batch error to survive relevant-path helper, got: {error_text}"
            );
            assert!(
                error_text.contains("xShmMap"),
                "expected fatal sqlite I/O marker to survive relevant-path helper, got: {error_text}"
            );

            assert!(
                note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "failed relevant write must forget the signature so the swap can be retried"
            );

            let shutdown_error = writer
                .shutdown()
                .expect_err("fatal writer failure should surface again on shutdown");
            assert!(
                error_chain_contains(&shutdown_error, "failed to insert observed swap batch"),
                "expected shutdown to surface the writer failure, got: {shutdown_error:#}"
            );

            Ok::<(), anyhow::Error>(())
        })?;

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn persist_relevant_observed_swap_reports_db_duplicate_without_forgetting_signature(
    ) -> Result<()> {
        let (_store, db_path) = make_test_store("relevant-observed-swap-duplicate")?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let writer = ObservedSwapWriter::start(db_path
                    .to_str()
                    .context("sqlite path must be valid utf-8")?
                    .to_string())?;
            let swap = test_swap("sig-relevant-duplicate");
            let mut recent_signatures = HashSet::new();
            let mut recent_signature_order = VecDeque::new();

            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));
            assert!(
                persist_relevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                )
                .await?,
                "first relevant write should insert the swap"
            );

            forget_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            );
            assert!(note_recent_swap_signature(
                &mut recent_signatures,
                &mut recent_signature_order,
                &swap.signature,
            ));

            assert!(
                !persist_relevant_observed_swap(
                    &writer,
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap,
                )
                .await?,
                "db duplicate should be surfaced as Ok(false)"
            );
            assert!(
                !note_recent_swap_signature(
                    &mut recent_signatures,
                    &mut recent_signature_order,
                    &swap.signature,
                ),
                "db duplicate should keep the signature inside recent dedupe state"
            );

            writer.shutdown()?;
            Ok::<(), anyhow::Error>(())
        })?;

        let _ = std::fs::remove_file(db_path);
        Ok(())
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_unclassified_swaps_irrelevant() {
        let mut swap = test_swap("sig-unclassified");
        swap.token_in = "token-a".to_string();
        swap.token_out = "token-b".to_string();

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantUnclassified
        );
    }

    #[test]
    fn observed_swap_shadow_relevance_marks_unfollowed_buy_irrelevant() {
        let swap = test_swap("sig-unfollowed-buy");

        assert_eq!(
            classify_observed_swap_shadow_relevance(
                &swap,
                &FollowSnapshot::default(),
                &ShadowScheduler::new(),
                &HashSet::new(),
            ),
            ObservedSwapShadowRelevance::IrrelevantNotFollowed(ShadowSwapSide::Buy)
        );
    }

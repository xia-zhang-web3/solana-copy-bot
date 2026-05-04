    trait TestOutcomeExt {
        fn expect_recorded(self, message: &str) -> ShadowSignalResult;
        fn expect_dropped(self, expected: ShadowDropReason, message: &str);
    }

    impl TestOutcomeExt for ShadowProcessOutcome {
        fn expect_recorded(self, message: &str) -> ShadowSignalResult {
            match self {
                ShadowProcessOutcome::Recorded(result) => result,
                ShadowProcessOutcome::Dropped(reason) => {
                    panic!("{message}: dropped with reason {}", reason.as_str())
                }
            }
        }

        fn expect_dropped(self, expected: ShadowDropReason, message: &str) {
            match self {
                ShadowProcessOutcome::Dropped(reason) => {
                    assert_eq!(
                        reason,
                        expected,
                        "{message}: expected {}, got {}",
                        expected.as_str(),
                        reason.as_str()
                    );
                }
                ShadowProcessOutcome::Recorded(_) => {
                    panic!("{message}: expected dropped, got recorded")
                }
            }
        }
    }

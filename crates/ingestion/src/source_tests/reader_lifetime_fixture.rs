//! Constructor for the external no-yield Reader lifetime test.
use super::*;
impl Reader {
    pub(in crate::source) fn pending_for_lifetime_test(scope: Arc<CaptureScope>) -> Self {
        Self::start(
            futures_util::stream::pending(),
            futures_util::sink::drain(),
            1,
            1 << 20,
            1 << 20,
            Arc::new(Default::default()),
            scope,
        )
        .unwrap()
    }
}

use super::{entry_risk_clock_fixture as clock, fresh_buy_size_runtime_fixture::RuntimeFixture};
use anyhow::Result;
use std::future::Future;
fn size<F: Future>(_: impl FnOnce() -> F) -> usize {
    std::mem::size_of::<F>()
}
#[tokio::test]
async fn entry_risk_clock_future_boundaries() -> Result<()> {
    let mut f = RuntimeFixture::new("clock-size", 20_000_000, 200, 10_000_000, 100, false).await?;
    let at = f.now;
    let hot = size(|| f.hot());
    let sweep = size(|| f.sweep());
    let wrapped_hot = size(|| clock::at(at, f.hot()));
    let sequence = size(|| clock::sequence([at], f.hot()));
    let wrapped_sweep = size(|| clock::or_at(at, f.sweep()));
    let combined = size(|| {
        clock::at(at, async {
            if true {
                f.sweep().await.map(|s| s.skipped_reason)
            } else {
                f.hot().await.map(|s| s.state_machine_skipped_reason)
            }
        })
    });
    eprintln!("FUTURE_BYTES hot={hot} sweep={sweep} at_hot={wrapped_hot} sequence={sequence} or_at_sweep={wrapped_sweep} combined={combined}");
    for bytes in [wrapped_hot, sequence, wrapped_sweep, combined] {
        assert!(
            bytes <= 256,
            "clock must own a pointer, not the caller future: {bytes}"
        );
    }
    f.finish().await?;
    assert!(
        f.calls().is_empty(),
        "type measurement does not construct or poll the measured futures"
    );
    Ok(())
}

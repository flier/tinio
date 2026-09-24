//! Shared pacing for background tasks (scanner, sweeper).

use std::time::Duration;

use tokio::{sync::watch, time::sleep};

/// Sleep at most `duration` in `chunk`-bounded steps, **waking as soon as the
/// shutdown channel turns true**.
///
/// **The wake is the point of the function, and it used to be the gap in it.**
/// A chunk-bounded sleep that re-checked the channel only *between* chunks
/// pinned a shutdown to the chunk it was inside: the scanner's chunk is its own
/// `max_wait`, `serve` passes 15 s, and a stop therefore took up to 15 s
/// whatever it did — measured 2026-09-21, three stops eleven seconds apart, one
/// exit 15.03 s after the child's start every time. The product's own stop hook
/// promises the CLI's "≤10 s, state file removed", so the chunk that was
/// supposed to keep shutdown prompt was the thing breaking that bound. The
/// `select!` below makes the wait end at whichever comes first, the timer or
/// the signal, and the pacing is otherwise unchanged.
///
/// `chunk` is kept — and bounded a single sleep, as its callers document —
/// because it is `ScannerOptions::max_wait` that reaches this parameter: a
/// published configuration knob with its own schema, tests and doc example, and
/// retiring it is a configuration-surface change rather than a pacing one. It
/// no longer decides *when* a shutdown is noticed, which is this function's
/// business now.
pub(crate) async fn sleep_checked(
    duration: Duration,
    chunk: Duration,
    shutdown: &watch::Receiver<bool>,
) {
    // A receiver of this call's own: `changed()` needs `&mut`, and cloning a
    // `watch::Receiver` is how one waits for the next change without holding a
    // borrow of the caller's for the length of the sleep.
    let mut shutdown = shutdown.clone();
    let mut remaining = duration;
    while remaining > Duration::ZERO {
        if *shutdown.borrow() {
            return;
        }
        let step = remaining.min(chunk);
        tokio::select! {
            _ = sleep(step) => {}
            changed = shutdown.changed() => {
                if changed.is_err() {
                    // The sending end is gone, so no stop can ever arrive.
                    // Sleeping the step out is what keeps this from becoming a
                    // spin: every caller is a `pass; sleep; pass` loop, and a
                    // receiver whose sender has dropped answers `changed()`
                    // with an error *immediately*, forever.
                    sleep(step).await;
                }
                if *shutdown.borrow() {
                    return;
                }
            }
        }
        remaining = remaining.saturating_sub(step);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn sleeps_the_whole_duration_when_not_shut_down() {
        let (_tx, rx) = watch::channel(false);
        let start = Instant::now();
        sleep_checked(Duration::from_millis(25), Duration::from_millis(5), &rx).await;
        // The chunked loop must sleep the full duration, not bail early.
        assert!(start.elapsed() >= Duration::from_millis(25));
    }

    #[tokio::test]
    async fn returns_immediately_when_shutdown_is_set() {
        let (_tx, rx) = watch::channel(true);
        let start = Instant::now();
        // A one-hour sleep with a 1 s chunk: the shutdown check must cut
        // it short before the first chunk is over.
        sleep_checked(Duration::from_secs(3600), Duration::from_secs(1), &rx).await;
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn a_stop_during_the_sleep_wakes_it() {
        // **The regression this function was fixed for.** `serve` passes the
        // scanner a one-hour cycle with a 15 s chunk; a stop arriving one
        // millisecond into that sleep used to wait out the whole chunk, which is
        // what made a clean exit take 15 s from the child's start whatever the
        // stop did. The chunk here is the whole duration, so nothing but the
        // wake can end it early.
        let (tx, rx) = watch::channel(false);
        let start = Instant::now();
        let sleeper = tokio::spawn(async move {
            sleep_checked(Duration::from_secs(3600), Duration::from_secs(3600), &rx).await;
        });
        sleep(Duration::from_millis(20)).await;
        tx.send(true).expect("the sleeper holds the receiver");
        sleeper.await.expect("the sleeper finishes");
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "the stop did not end the sleep: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn a_dropped_sender_still_sleeps() {
        // The other half of the wake: `changed()` answers an error *at once*
        // once the sender is gone, so a naive `select!` would return
        // immediately on every call and turn the callers' pass/sleep loops into
        // a spin with no pause at all.
        let (tx, rx) = watch::channel(false);
        drop(tx);
        let start = Instant::now();
        sleep_checked(Duration::from_millis(30), Duration::from_secs(3600), &rx).await;
        assert!(
            start.elapsed() >= Duration::from_millis(30),
            "the sleep was cut short by a dead sender: {:?}",
            start.elapsed()
        );
    }
}

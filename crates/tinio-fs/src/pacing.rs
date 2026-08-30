//! Shared pacing for background tasks (scanner, sweeper).

use std::time::Duration;

use tokio::{sync::watch, time::sleep};

/// Sleep at most `duration` in `chunk`-bounded steps, re-checking the
/// shutdown channel between chunks (shutdown stays prompt even with a
/// long sleep).
pub(crate) async fn sleep_checked(
    duration: Duration,
    chunk: Duration,
    shutdown: &watch::Receiver<bool>,
) {
    let mut remaining = duration;
    while remaining > Duration::ZERO {
        if *shutdown.borrow() {
            return;
        }
        let step = remaining.min(chunk);
        sleep(step).await;
        remaining = remaining.saturating_sub(step);
    }
}

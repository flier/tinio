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
        // it short on the first chunk boundary.
        sleep_checked(Duration::from_secs(3600), Duration::from_secs(1), &rx).await;
        assert!(start.elapsed() < Duration::from_millis(100));
    }
}

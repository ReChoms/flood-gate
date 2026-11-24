use std::time::Duration;
use tokio::time::Instant;

/// Computes the global arrival deadline for `request_index` from a shared `epoch`.
pub fn scheduled_deadline(epoch: Instant, request_index: usize, requests_per_second: u32) -> Instant {
    let offset_seconds = request_index as f64 / f64::from(requests_per_second.max(1));
    epoch + Duration::from_secs_f64(offset_seconds)
}

/// Waits until `request_index`'s global arrival slot and returns the scheduled start time.
pub async fn await_scheduled_turn(
    epoch: Instant,
    request_index: usize,
    target_requests_per_second: Option<u32>,
) -> Instant {
    let Some(requests_per_second) = target_requests_per_second.filter(|&value| value > 0) else {
        return Instant::now();
    };
    let deadline = scheduled_deadline(epoch, request_index, requests_per_second);
    tokio::time::sleep_until(deadline).await;
    deadline
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduled_deadline_spacing() {
        let epoch = Instant::now();
        assert_eq!(scheduled_deadline(epoch, 0, 20), epoch);
        assert_eq!(scheduled_deadline(epoch, 1, 20), epoch + Duration::from_millis(50));
        assert_eq!(scheduled_deadline(epoch, 3, 20), epoch + Duration::from_millis(150));
    }
}

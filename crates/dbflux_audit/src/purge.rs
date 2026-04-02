//! Purge logic for old audit events.
//!
//! This module provides functions for purging audit events based on retention policy.
//! Purge operations are performed in batches to avoid long write transactions.

use std::time::Duration;

use crate::AuditError;
use crate::store::sqlite::SqliteAuditStore;

/// Statistics from a purge operation.
#[derive(Debug, Clone)]
pub struct PurgeStats {
    /// Total number of events deleted.
    pub deleted_count: i64,
    /// Number of batch iterations performed.
    pub batches: usize,
    /// Duration of the purge operation.
    pub duration_ms: u64,
}

/// Calculates the cutoff timestamp for retention policy.
///
/// ## Arguments
///
/// * `retention_days` - Number of days to retain events
///
/// ## Returns
///
/// The Unix timestamp in milliseconds representing the cutoff time.
pub fn calculate_cutoff_ms(retention_days: u32) -> i64 {
    calculate_cutoff_ms_at(std::time::SystemTime::now(), retention_days)
}

fn calculate_cutoff_ms_at(now: std::time::SystemTime, retention_days: u32) -> i64 {
    let now = now
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let retention_duration = Duration::from_secs(60 * 60 * 24 * retention_days as u64);
    let cutoff = now.saturating_sub(retention_duration);
    cutoff.as_millis() as i64
}

/// Purges old audit events from the store.
///
/// ## Arguments
///
/// * `store` - The SQLite audit store
/// * `retention_days` - Number of days to retain events
/// * `batch_size` - Number of events to delete per batch
///
/// ## Returns
///
/// Statistics about the purge operation.
pub fn purge_old_events(
    store: &SqliteAuditStore,
    retention_days: u32,
    batch_size: usize,
) -> Result<PurgeStats, AuditError> {
    let start = std::time::Instant::now();
    let cutoff_ms = calculate_cutoff_ms(retention_days);
    let batch_size = batch_size.max(1);

    let mut total_deleted = 0i64;
    let mut batches = 0usize;
    let mut more_to_delete = true;

    while more_to_delete {
        let deleted = store.delete_older_than(cutoff_ms, batch_size)?;
        total_deleted += deleted;
        batches += 1;

        // Continue if we deleted a full batch (there might be more)
        more_to_delete = deleted >= batch_size as i64;
    }

    let duration_ms = start.elapsed().as_millis() as u64;

    Ok(PurgeStats {
        deleted_count: total_deleted,
        batches,
        duration_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_cutoff_ms() {
        let now = std::time::UNIX_EPOCH + Duration::from_secs(1700000000);
        let cutoff = calculate_cutoff_ms_at(now, 30);
        let expected = (now - Duration::from_secs(60 * 60 * 24 * 30))
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        assert_eq!(cutoff, expected.as_millis() as i64);
    }

    #[test]
    fn test_calculate_cutoff_ms_one_day() {
        let now = std::time::UNIX_EPOCH + Duration::from_secs(1700000000);
        let cutoff = calculate_cutoff_ms_at(now, 1);
        let expected = (now - Duration::from_secs(60 * 60 * 24))
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        assert_eq!(cutoff, expected.as_millis() as i64);
    }
}

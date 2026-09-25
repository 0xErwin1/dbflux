//! TTL and size of the keys shown in the key list.
//!
//! Only the rows on screen are asked for, in batches: `keys_needing_metadata`
//! picks the visible keys that have neither a cached answer nor a request in
//! flight, and one `KeyValueApi::key_metadata` call fetches them in a single
//! pipeline. Scrolling therefore never issues one round trip per key.

use dbflux_core::{DbError, KeyMetadata, KeyMetadataRequest, KeyValueFeatures};
use gpui::Context;
use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::time::{Duration, Instant};

/// Most keys one metadata pipeline asks for.
pub(super) const METADATA_BATCH_LIMIT: usize = 100;

/// TTL below which the key list shows the expiry in the danger color.
const EXPIRING_SOON: Duration = Duration::from_secs(60);

/// Expiry of a key as last read from the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum KeyExpiry {
    /// The key never expires.
    Never,
    /// The key expires at this instant.
    At(Instant),
    /// The key was gone when its metadata was read.
    Missing,
    /// The server could not report an expiry.
    Unknown,
}

impl KeyExpiry {
    pub(super) fn from_ttl_seconds(ttl_seconds: Option<i64>, now: Instant) -> Self {
        match ttl_seconds {
            None => Self::Never,
            Some(seconds) => Self::At(now + Duration::from_secs(seconds.max(0) as u64)),
        }
    }

    /// Seconds left before the key expires, or `None` when it never does.
    pub(super) fn remaining_seconds(self, now: Instant) -> Option<u64> {
        match self {
            Self::At(deadline) => Some(deadline.saturating_duration_since(now).as_secs()),
            Self::Never | Self::Missing | Self::Unknown => None,
        }
    }
}

/// What the key list knows about one key besides its name and type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CachedKeyMetadata {
    pub expiry: KeyExpiry,
    pub size_bytes: Option<u64>,
}

impl CachedKeyMetadata {
    pub(super) fn from_metadata(metadata: &KeyMetadata, now: Instant) -> Self {
        let expiry = if metadata.exists {
            KeyExpiry::from_ttl_seconds(metadata.ttl_seconds, now)
        } else {
            KeyExpiry::Missing
        };

        Self {
            expiry,
            size_bytes: metadata.size_bytes,
        }
    }

    fn unknown() -> Self {
        Self {
            expiry: KeyExpiry::Unknown,
            size_bytes: None,
        }
    }
}

/// Visible keys that still need their metadata, capped at `limit`, in the
/// order they appear on screen.
pub(super) fn keys_needing_metadata<'a>(
    visible_keys: impl IntoIterator<Item = &'a str>,
    cached: &HashMap<String, CachedKeyMetadata>,
    in_flight: &HashSet<String>,
    limit: usize,
) -> Vec<String> {
    let mut batch = Vec::new();

    for key in visible_keys {
        if batch.len() >= limit {
            break;
        }

        if cached.contains_key(key) || in_flight.contains(key) {
            continue;
        }

        if !batch.iter().any(|queued: &String| queued == key) {
            batch.push(key.to_string());
        }
    }

    batch
}

/// How the TTL cell reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TtlTone {
    /// The key expires later.
    Normal,
    /// The key expires within a minute.
    Urgent,
    /// No expiry, or nothing known.
    Muted,
}

/// Relative TTL (`3h 12m`, `59s`, `6d`), with `—` for a key that never
/// expires.
pub(super) fn format_ttl(remaining_seconds: Option<u64>) -> String {
    let Some(seconds) = remaining_seconds else {
        return "—".to_string();
    };

    const MINUTE: u64 = 60;
    const HOUR: u64 = 60 * MINUTE;
    const DAY: u64 = 24 * HOUR;

    if seconds < MINUTE {
        format!("{seconds}s")
    } else if seconds < HOUR {
        format!("{}m", seconds / MINUTE)
    } else if seconds < DAY {
        format!("{}h {:02}m", seconds / HOUR, (seconds % HOUR) / MINUTE)
    } else {
        format!("{}d", seconds / DAY)
    }
}

pub(super) fn ttl_tone(remaining_seconds: Option<u64>) -> TtlTone {
    match remaining_seconds {
        None => TtlTone::Muted,
        Some(seconds) if seconds < EXPIRING_SOON.as_secs() => TtlTone::Urgent,
        Some(_) => TtlTone::Normal,
    }
}

/// Byte size as `512 B`, `1.8 KB`, `18 KB` or `4.2 MB`: one decimal below
/// ten of a unit, none above.
pub(super) fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];

    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit_index = 0;

    while value >= 1024.0 && unit_index + 1 < UNITS.len() {
        value /= 1024.0;
        unit_index += 1;
    }

    let unit = UNITS.get(unit_index).copied().unwrap_or("B");

    if value < 10.0 {
        format!("{value:.1} {unit}")
    } else {
        format!("{value:.0} {unit}")
    }
}

impl super::KeyValueDocument {
    /// Requests metadata for the key rows in `visible_rows` that lack it.
    ///
    /// Called from the key list while it lays out the rows on screen, so it
    /// only queues work: the fetch runs on the background executor and the
    /// list re-renders once the answers arrive.
    pub(super) fn request_visible_metadata(
        &mut self,
        visible_rows: Range<usize>,
        cx: &mut Context<Self>,
    ) {
        if !self.key_features.contains(KeyValueFeatures::KEY_METADATA) {
            return;
        }

        let visible_keys: Vec<&str> = self
            .key_rows
            .get(visible_rows)
            .unwrap_or_default()
            .iter()
            .filter_map(|row| row.key_index())
            .filter_map(|index| self.keys.get(index))
            .map(|entry| entry.key.as_str())
            .collect();

        let batch = keys_needing_metadata(
            visible_keys,
            &self.key_metadata,
            &self.metadata_in_flight,
            METADATA_BATCH_LIMIT,
        );

        if batch.is_empty() {
            return;
        }

        let Some(connection) = self.get_connection(cx) else {
            return;
        };

        self.metadata_in_flight.extend(batch.iter().cloned());

        let generation = self.scan_generation;
        let request = KeyMetadataRequest {
            keys: batch,
            keyspace: self.keyspace_index(),
            include_encoding: false,
        };
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let requested = request.keys.clone();

            let result = cx
                .background_executor()
                .spawn(async move {
                    let api = connection.key_value_api().ok_or_else(|| {
                        DbError::NotSupported("Key-value API unavailable".to_string())
                    })?;
                    api.key_metadata(&request)
                })
                .await;

            cx.update(|cx| {
                entity.update(cx, |this, cx| {
                    if this.scan_generation != generation {
                        return;
                    }

                    for key in &requested {
                        this.metadata_in_flight.remove(key);
                    }

                    let now = Instant::now();

                    match result {
                        Ok(metadata) => {
                            for item in &metadata {
                                this.key_metadata.insert(
                                    item.key.clone(),
                                    CachedKeyMetadata::from_metadata(item, now),
                                );
                            }
                        }
                        Err(error) => {
                            // Passive background refresh of on-screen rows:
                            // the list shows the cells as unknown instead of
                            // retrying on every frame.
                            log::debug!("Key metadata unavailable: {error}");

                            for key in requested {
                                this.key_metadata.insert(key, CachedKeyMetadata::unknown());
                            }
                        }
                    }

                    cx.notify();
                });
            });
        })
        .detach();
    }

    /// Drops the cached metadata of `key` so the next frame fetches it again,
    /// after a write that changed its size or expiry.
    pub(super) fn invalidate_key_metadata(&mut self, key: &str) {
        self.key_metadata.remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metadata(key: &str, ttl_seconds: Option<i64>, exists: bool) -> KeyMetadata {
        KeyMetadata {
            key: key.to_string(),
            exists,
            ttl_seconds,
            size_bytes: Some(64),
            encoding: None,
        }
    }

    #[test]
    fn format_ttl_uses_the_largest_sensible_unit() {
        assert_eq!(format_ttl(None), "—");
        assert_eq!(format_ttl(Some(0)), "0s");
        assert_eq!(format_ttl(Some(59)), "59s");
        assert_eq!(format_ttl(Some(4 * 60 + 30)), "4m");
        assert_eq!(format_ttl(Some(3 * 3600 + 12 * 60)), "3h 12m");
        assert_eq!(format_ttl(Some(2 * 3600 + 3 * 60)), "2h 03m");
        assert_eq!(format_ttl(Some(6 * 86_400 + 5)), "6d");
    }

    #[test]
    fn ttl_tone_warns_only_under_a_minute() {
        assert_eq!(ttl_tone(None), TtlTone::Muted);
        assert_eq!(ttl_tone(Some(38)), TtlTone::Urgent);
        assert_eq!(ttl_tone(Some(60)), TtlTone::Normal);
    }

    #[test]
    fn format_size_matches_the_key_list_columns() {
        assert_eq!(format_size(12), "12 B");
        assert_eq!(format_size(512), "512 B");
        assert_eq!(format_size(1_843), "1.8 KB");
        assert_eq!(format_size(18 * 1024), "18 KB");
        assert_eq!(format_size(4_404_019), "4.2 MB");
    }

    #[test]
    fn keys_needing_metadata_skips_cached_and_in_flight_keys() {
        let mut cached = HashMap::new();
        cached.insert(
            "cached".to_string(),
            CachedKeyMetadata::from_metadata(&metadata("cached", None, true), Instant::now()),
        );
        let in_flight: HashSet<String> = ["pending".to_string()].into_iter().collect();

        let batch = keys_needing_metadata(
            ["cached", "fresh:1", "pending", "fresh:2", "fresh:1"],
            &cached,
            &in_flight,
            METADATA_BATCH_LIMIT,
        );

        assert_eq!(batch, vec!["fresh:1".to_string(), "fresh:2".to_string()]);
    }

    #[test]
    fn keys_needing_metadata_caps_one_pipeline() {
        let keys: Vec<String> = (0..250).map(|index| format!("key:{index}")).collect();

        let batch = keys_needing_metadata(
            keys.iter().map(String::as_str),
            &HashMap::new(),
            &HashSet::new(),
            METADATA_BATCH_LIMIT,
        );

        assert_eq!(batch.len(), METADATA_BATCH_LIMIT);
        assert_eq!(batch[0], "key:0");
    }

    #[test]
    fn cached_metadata_counts_the_expiry_down_from_when_it_was_read() {
        let read_at = Instant::now();
        let cached = CachedKeyMetadata::from_metadata(&metadata("k", Some(120), true), read_at);

        assert_eq!(
            cached
                .expiry
                .remaining_seconds(read_at + Duration::from_secs(20)),
            Some(100)
        );
        assert_eq!(
            cached
                .expiry
                .remaining_seconds(read_at + Duration::from_secs(500)),
            Some(0)
        );
    }

    #[test]
    fn missing_keys_and_persistent_keys_have_no_remaining_ttl() {
        let now = Instant::now();

        let gone = CachedKeyMetadata::from_metadata(&metadata("k", None, false), now);
        let persistent = CachedKeyMetadata::from_metadata(&metadata("k", None, true), now);

        assert_eq!(gone.expiry, KeyExpiry::Missing);
        assert_eq!(persistent.expiry, KeyExpiry::Never);
        assert_eq!(persistent.expiry.remaining_seconds(now), None);
    }
}

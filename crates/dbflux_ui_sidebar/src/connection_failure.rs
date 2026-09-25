//! The failed-connection block under a profile row (P1Sidebar "Connection
//! states"): the error the connection layer reported, and retry, edit and
//! audit actions.
//!
//! The tree is a uniform list, so every entry is one row tall. The block is
//! taller than a row, so the tree builder places [`FAILURE_ROW_SLICES`]
//! disabled entries after the failed profile, and each of them draws its
//! slice of the same block. Keyboard selection skips them.

use uuid::Uuid;

/// Tree entries that carry one failure block.
pub(crate) const FAILURE_ROW_SLICES: usize = 4;

const FAILURE_ROW_PREFIX: &str = "connect-failure|";

/// Tree item id of slice `slice` of the failure block under `profile_id`.
pub(crate) fn failure_row_id(profile_id: Uuid, slice: usize) -> String {
    format!("{FAILURE_ROW_PREFIX}{profile_id}|{slice}")
}

/// The profile and slice of a failure block entry, or `None` for any other
/// tree item.
pub(crate) fn parse_failure_row_id(id: &str) -> Option<(Uuid, usize)> {
    let rest = id.strip_prefix(FAILURE_ROW_PREFIX)?;
    let (profile, slice) = rest.split_once('|')?;
    let profile_id = Uuid::parse_str(profile).ok()?;
    let slice = slice.parse::<usize>().ok()?;

    (slice < FAILURE_ROW_SLICES).then_some((profile_id, slice))
}

pub(crate) fn is_failure_row_id(id: &str) -> bool {
    parse_failure_row_id(id).is_some()
}

/// The two lines of the failure block, taken from the connection layer's
/// error: its first line as the summary, the remaining lines joined as the
/// detail line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConnectionFailure {
    pub summary: String,
    pub detail: Option<String>,
}

impl ConnectionFailure {
    pub fn from_error(error: &str) -> Self {
        let mut lines = error.lines().map(str::trim).filter(|line| !line.is_empty());

        let summary = lines.next().unwrap_or_default().to_string();
        let rest: Vec<&str> = lines.collect();
        let detail = (!rest.is_empty()).then(|| rest.join(" \u{b7} "));

        Self { summary, detail }
    }
}

/// Index of the entry keyboard selection lands on when moving from `current`
/// by `step` (+1 or -1) over `count` entries, skipping failure block entries.
/// Stays on `current` when every entry in that direction is a failure entry.
pub(crate) fn next_selectable_index(
    current: usize,
    step: isize,
    count: usize,
    is_failure_row: impl Fn(usize) -> bool,
) -> usize {
    let mut index = current;

    loop {
        let candidate = index as isize + step;

        if candidate < 0 || candidate as usize >= count {
            return current;
        }

        index = candidate as usize;

        if !is_failure_row(index) {
            return index;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_row_ids_round_trip_and_reject_other_ids() {
        let profile_id = Uuid::new_v4();

        for slice in 0..FAILURE_ROW_SLICES {
            let id = failure_row_id(profile_id, slice);
            assert_eq!(parse_failure_row_id(&id), Some((profile_id, slice)));
            assert!(is_failure_row_id(&id));
        }

        assert_eq!(
            parse_failure_row_id(&failure_row_id(profile_id, FAILURE_ROW_SLICES)),
            None
        );
        assert_eq!(parse_failure_row_id("connect-failure|not-a-uuid|0"), None);
        assert!(!is_failure_row_id(
            &dbflux_core::SchemaNodeId::Profile { profile_id }.to_string()
        ));
    }

    #[test]
    fn failure_splits_the_error_into_summary_and_detail() {
        let failure =
            ConnectionFailure::from_error("connection refused\n10.0.4.12:27017\n  3 tries\n");
        assert_eq!(failure.summary, "connection refused");
        assert_eq!(
            failure.detail.as_deref(),
            Some("10.0.4.12:27017 \u{b7} 3 tries")
        );

        let single = ConnectionFailure::from_error("timed out");
        assert_eq!(single.summary, "timed out");
        assert_eq!(single.detail, None);
    }

    #[test]
    fn keyboard_selection_skips_failure_entries() {
        // 0 profile, 1-4 failure block, 5 next profile.
        let is_failure = |index: usize| (1..=4).contains(&index);

        assert_eq!(next_selectable_index(0, 1, 6, is_failure), 5);
        assert_eq!(next_selectable_index(5, -1, 6, is_failure), 0);
        assert_eq!(next_selectable_index(5, 1, 6, is_failure), 5);
        assert_eq!(next_selectable_index(0, -1, 6, is_failure), 0);

        let trailing = |index: usize| index >= 1;
        assert_eq!(next_selectable_index(0, 1, 5, trailing), 0);
    }
}

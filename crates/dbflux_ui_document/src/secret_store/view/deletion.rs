//! The deletion state a write confirmed in this view, which outranks any
//! listing row or describe that disagrees with it.
//!
//! The services' reads are eventually consistent in both directions: a
//! listing or describe taken right after a delete can still say active, and
//! one taken right after a restore can still say scheduled. A delete or
//! restore the view's own write landed is the authority for that secret until
//! an observation agrees with it or the user refreshes explicitly.

use super::{DetailsState, SecretView};
use dbflux_core::chrono::{DateTime, Utc};
use dbflux_core::{SecretDetails, SecretSummary};

/// The date a confirmed deletion carries when the store did not say when it
/// takes effect (its audit row was lost, so the answer never arrived). Every
/// guard reads the deletion as scheduled; nothing draws this value, see
/// [`known_deletion_date`].
const UNKNOWN_DELETION_DATE: DateTime<Utc> = DateTime::UNIX_EPOCH;

/// The date a scheduled deletion takes effect, `None` when it is scheduled
/// but the date is unknown.
pub(in crate::secret_store) fn known_deletion_date(date: DateTime<Utc>) -> Option<DateTime<Utc>> {
    (date != UNKNOWN_DELETION_DATE).then_some(date)
}

/// What this view's own write established about a secret's deletion.
#[derive(Clone, Copy, Debug)]
pub(super) enum ConfirmedDeletion {
    /// A delete was scheduled to take effect on this date, when it is known.
    Scheduled(Option<DateTime<Utc>>),

    /// A scheduled deletion was cancelled.
    Restored,
}

impl SecretView {
    /// Records that a delete of `name` was confirmed, and conforms the view's
    /// copies of the row to it.
    pub(in crate::secret_store) fn confirm_deletion_scheduled(
        &mut self,
        name: &str,
        date: Option<DateTime<Utc>>,
    ) {
        self.confirmed_deletions
            .insert(name.to_string(), ConfirmedDeletion::Scheduled(date));
        self.conform_copies(name);
    }

    /// Records that a restore of `name` was confirmed, and conforms the
    /// view's copies of the row to it.
    pub(in crate::secret_store) fn confirm_restored(&mut self, name: &str) {
        self.confirmed_deletions
            .insert(name.to_string(), ConfirmedDeletion::Restored);
        self.conform_copies(name);
    }

    /// An explicit refresh asks the store again and believes its answer.
    pub(in crate::secret_store) fn clear_confirmed_deletions(&mut self) {
        self.confirmed_deletions.clear();
    }

    /// Conforms an observed summary to a confirmed deletion state. An
    /// observation that agrees ends the override; one that disagrees is
    /// rewritten, so every guard reads the confirmed state.
    pub(in crate::secret_store) fn conform_summary(&mut self, summary: &mut SecretSummary) {
        let Some(confirmed) = self.confirmed_deletions.get(&summary.name).copied() else {
            return;
        };

        match confirmed {
            ConfirmedDeletion::Scheduled(date) => {
                if summary.deletion_scheduled_for.is_some() {
                    self.confirmed_deletions.remove(&summary.name);
                } else {
                    summary.deletion_scheduled_for = Some(date.unwrap_or(UNKNOWN_DELETION_DATE));
                }
            }
            ConfirmedDeletion::Restored => {
                if summary.deletion_scheduled_for.is_none() {
                    self.confirmed_deletions.remove(&summary.name);
                } else {
                    summary.deletion_scheduled_for = None;
                }
            }
        }
    }

    /// [`Self::conform_summary`] for a describe, whose request moment goes
    /// with a cancelled deletion.
    pub(in crate::secret_store) fn conform_details(&mut self, details: &mut SecretDetails) {
        self.conform_summary(&mut details.summary);

        if details.summary.deletion_scheduled_for.is_none() {
            details.deleted_at = None;
        }
    }

    fn conform_copies(&mut self, name: &str) {
        if !self.is_for(name) {
            return;
        }

        let mut row = self.listing_row.take();
        let mut cache = self.summary_cache.take();

        for summary in [row.as_mut(), cache.as_mut()].into_iter().flatten() {
            self.force_summary(summary);
        }

        self.listing_row = row;
        self.summary_cache = cache;

        if let DetailsState::Loaded(details) = &mut self.details {
            let mut loaded = details.clone();
            self.force_summary(&mut loaded.summary);
            if loaded.summary.deletion_scheduled_for.is_none() {
                loaded.deleted_at = None;
            }
            self.details = DetailsState::Loaded(loaded);
        }
    }

    /// Writes the confirmed state into `summary` without ending the override.
    fn force_summary(&self, summary: &mut SecretSummary) {
        match self.confirmed_deletions.get(&summary.name) {
            Some(ConfirmedDeletion::Scheduled(date)) => {
                summary.deletion_scheduled_for = Some(date.unwrap_or(UNKNOWN_DELETION_DATE));
            }
            Some(ConfirmedDeletion::Restored) => summary.deletion_scheduled_for = None,
            None => {}
        }
    }
}

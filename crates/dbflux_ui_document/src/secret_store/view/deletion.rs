//! The deletion state a write confirmed in this view, which outranks any
//! listing row or describe that disagrees with it.
//!
//! The services' reads are eventually consistent in both directions: a
//! listing or describe taken right after a delete can still say active, and
//! one taken right after a restore can still say scheduled. A delete or
//! restore the view's own write landed is the authority for that secret until
//! an observation agrees with it or the user refreshes explicitly.
//!
//! Observed summaries are stored as observed; every guard and display reads
//! the deletion through [`SecretView::deletion_of`], which applies the
//! override. A confirmed delete whose date is unknown (its audit row was
//! lost, so the answer never arrived) keeps that in the override and never
//! invents a date.

use super::SecretView;
use dbflux_core::SecretSummary;
use dbflux_core::chrono::{DateTime, Utc};

/// What this view's own write established about a secret's deletion.
#[derive(Clone, Copy, Debug)]
pub(super) enum ConfirmedDeletion {
    /// A delete was scheduled; `date` is when it takes effect, if known.
    Scheduled { date: Option<DateTime<Utc>> },

    /// A scheduled deletion was cancelled.
    Restored,
}

impl SecretView {
    /// Records that a delete of `name` was confirmed.
    pub(in crate::secret_store) fn confirm_deletion_scheduled(
        &mut self,
        name: &str,
        date: Option<DateTime<Utc>>,
    ) {
        self.confirmed_deletions
            .insert(name.to_string(), ConfirmedDeletion::Scheduled { date });
    }

    /// Records that a restore of `name` was confirmed.
    pub(in crate::secret_store) fn confirm_restored(&mut self, name: &str) {
        self.confirmed_deletions
            .insert(name.to_string(), ConfirmedDeletion::Restored);
    }

    /// An explicit refresh asks the store again and believes its answer.
    pub(in crate::secret_store) fn clear_confirmed_deletions(&mut self) {
        self.confirmed_deletions.clear();
    }

    /// Offers an observation to the override: one that agrees ends it.
    pub(in crate::secret_store) fn observe_deletion(&mut self, summary: &SecretSummary) {
        let agrees = match self.confirmed_deletions.get(&summary.name) {
            Some(ConfirmedDeletion::Scheduled { .. }) => summary.is_pending_deletion(),
            Some(ConfirmedDeletion::Restored) => !summary.is_pending_deletion(),
            None => return,
        };

        if agrees {
            self.confirmed_deletions.remove(&summary.name);
        }
    }

    /// The deletion of `summary`'s secret: `None` when it is active,
    /// `Some(None)` when it is scheduled for an unknown date, `Some(Some(d))`
    /// when it takes effect on `d`. A confirmed state wins over the summary.
    pub(in crate::secret_store) fn deletion_of(
        &self,
        summary: &SecretSummary,
    ) -> Option<Option<DateTime<Utc>>> {
        match self.confirmed_deletions.get(&summary.name) {
            Some(ConfirmedDeletion::Scheduled { date }) => {
                Some(date.or(summary.deletion_scheduled_for))
            }
            Some(ConfirmedDeletion::Restored) => None,
            None => summary.deletion_scheduled_for.map(Some),
        }
    }

    /// Whether `summary`'s secret is scheduled for deletion.
    pub(in crate::secret_store) fn is_deletion_pending(&self, summary: &SecretSummary) -> bool {
        self.deletion_of(summary).is_some()
    }
}

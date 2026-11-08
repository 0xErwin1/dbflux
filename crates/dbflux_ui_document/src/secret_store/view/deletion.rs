//! The deletion state a write confirmed in this view, which outranks any
//! listing row or describe that disagrees with it.
//!
//! The services' reads are eventually consistent in both directions: a
//! listing or describe taken right after a delete can still say active, and
//! one taken right after a restore can still say scheduled. A delete or
//! restore the view's own write landed is the authority for that secret until
//! both the listing row and the describe agree with it, or the user refreshes
//! explicitly. One agreeing read is not enough: the other may still be the
//! stale one, and a stale read must not re-enable what the write blocked.
//! A confirmation also belongs to the connection session that made it; another
//! session (a reconnect, another account) may hold a same-named secret in a
//! different state.
//!
//! Observed summaries are stored as observed; every guard and display reads
//! the deletion through [`SecretView::deletion_of`], which applies the
//! override; the rows the view stores are conformed to it on arrival. A confirmed delete whose date is unknown (its audit row was
//! lost, so the answer never arrived) keeps that in the override and never
//! invents a date.

use super::SecretView;
use dbflux_core::chrono::{DateTime, Utc};
use dbflux_core::{Connection, SecretDetails, SecretSummary};
use std::sync::Arc;

/// What this view's own write established about a secret's deletion.
#[derive(Clone, Copy, Debug)]
pub(super) enum ConfirmedDeletion {
    /// A delete was scheduled; `date` is when it takes effect, if known.
    Scheduled { date: Option<DateTime<Utc>> },

    /// A scheduled deletion was cancelled.
    Restored,
}

/// A confirmed state, the session that confirmed it and which reads have
/// caught up with it so far.
#[derive(Clone)]
pub(super) struct Confirmation {
    state: ConfirmedDeletion,
    session: Option<Arc<dyn Connection>>,
    listing_agreed: bool,
    describe_agreed: bool,
}

impl ConfirmedDeletion {
    fn agrees_with(self, summary: &SecretSummary) -> bool {
        match self {
            Self::Scheduled { .. } => summary.is_pending_deletion(),
            Self::Restored => !summary.is_pending_deletion(),
        }
    }
}

impl SecretView {
    /// Records that a delete of `name` was confirmed. A scheduled delete is
    /// confirmed whether or not the store told the date it takes effect.
    pub(in crate::secret_store) fn confirm_deletion_scheduled(
        &mut self,
        name: &str,
        date: Option<DateTime<Utc>>,
        cx: &gpui::Context<Self>,
    ) {
        self.confirm(name, ConfirmedDeletion::Scheduled { date }, cx);
    }

    /// Records that a restore of `name` was confirmed.
    pub(in crate::secret_store) fn confirm_restored(
        &mut self,
        name: &str,
        cx: &gpui::Context<Self>,
    ) {
        self.confirm(name, ConfirmedDeletion::Restored, cx);
    }

    fn confirm(&mut self, name: &str, state: ConfirmedDeletion, cx: &gpui::Context<Self>) {
        let session = self.connection(cx);

        self.confirmed_deletions.insert(
            name.to_string(),
            Confirmation {
                state,
                session,
                listing_agreed: false,
                describe_agreed: false,
            },
        );
    }

    /// An explicit refresh asks the store again and believes its answer.
    pub(in crate::secret_store) fn clear_confirmed_deletions(&mut self) {
        self.confirmed_deletions.clear();
    }

    /// Drops the confirmations another connection session made. Returns
    /// whether any went. Without a live connection nothing is dropped: a
    /// disconnect is not another session until one returns.
    pub(in crate::secret_store) fn drop_confirmations_of_replaced_session(
        &mut self,
        cx: &gpui::Context<Self>,
    ) -> bool {
        let Some(live) = self.connection(cx) else {
            return false;
        };

        let before = self.confirmed_deletions.len();

        self.confirmed_deletions.retain(|_, confirmation| {
            confirmation
                .session
                .as_ref()
                .is_some_and(|session| Arc::ptr_eq(session, &live))
        });

        self.confirmed_deletions.len() != before
    }

    /// Offers the listing's row of a secret to the override.
    pub(in crate::secret_store) fn observe_listed(&mut self, summary: &SecretSummary) {
        self.observe(summary, |confirmation| &mut confirmation.listing_agreed);
    }

    /// Offers a describe of a secret to the override.
    pub(in crate::secret_store) fn observe_described(&mut self, summary: &SecretSummary) {
        self.observe(summary, |confirmation| &mut confirmation.describe_agreed);
    }

    /// An observation that agrees counts for its own source only; the
    /// override ends once both sources agreed.
    fn observe(
        &mut self,
        summary: &SecretSummary,
        source: impl FnOnce(&mut Confirmation) -> &mut bool,
    ) {
        let Some(confirmation) = self.confirmed_deletions.get_mut(&summary.name) else {
            return;
        };

        if confirmation.state.agrees_with(summary) {
            *source(confirmation) = true;
        }

        if confirmation.listing_agreed && confirmation.describe_agreed {
            self.confirmed_deletions.remove(&summary.name);
        }
    }

    /// Rewrites `summary` to what the confirmed state says, so a stored row
    /// never carries the date a restore cancelled. A scheduled delete whose
    /// date is unknown has no summary form: `deletion_of` reports it.
    pub(in crate::secret_store) fn conform_summary(&self, summary: &mut SecretSummary) {
        match self
            .confirmed_deletions
            .get(&summary.name)
            .map(|confirmation| confirmation.state)
        {
            Some(ConfirmedDeletion::Scheduled { date: Some(date) }) => {
                summary.deletion_scheduled_for = Some(date);
            }
            Some(ConfirmedDeletion::Restored) => summary.deletion_scheduled_for = None,
            _ => {}
        }
    }

    /// [`Self::conform_summary`] for a describe, whose deletion moment too.
    pub(in crate::secret_store) fn conform_details(&self, details: &mut SecretDetails) {
        self.conform_summary(&mut details.summary);

        if matches!(
            self.confirmed_deletions
                .get(&details.summary.name)
                .map(|confirmation| confirmation.state),
            Some(ConfirmedDeletion::Restored)
        ) {
            details.deleted_at = None;
        }
    }

    /// The deletion of `summary`'s secret: `None` when it is active,
    /// `Some(None)` when it is scheduled for an unknown date, `Some(Some(d))`
    /// when it takes effect on `d`. A confirmed state wins over the summary.
    pub(in crate::secret_store) fn deletion_of(
        &self,
        summary: &SecretSummary,
    ) -> Option<Option<DateTime<Utc>>> {
        match self
            .confirmed_deletions
            .get(&summary.name)
            .map(|confirmation| confirmation.state)
        {
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

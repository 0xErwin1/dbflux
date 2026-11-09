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
//! Each source counts by its latest observation, so a read that disagrees
//! after an agreeing one takes the agreement back, and only a read issued
//! after the write can have seen it: the selection's copy of a row and the
//! rows a failed reload retained are observations of nothing.
//! A confirmation also belongs to the connection session that made it; another
//! session (a reconnect, another account) may hold a same-named secret in a
//! different state, so the summaries conformed to it go with it.
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
    listing: Source,
    describe: Source,
}

/// What one kind of read last said about a confirmed secret.
#[derive(Clone, Copy, Default)]
struct Source {
    /// The generation of the first request issued after the write; a read of
    /// an earlier generation cannot have seen it.
    floor: Option<u64>,

    /// Whether the latest counted read agreed with the confirmed state.
    agrees: bool,
}

impl Source {
    fn issued(&mut self, generation: u64) {
        self.floor.get_or_insert(generation);
    }

    fn observe(&mut self, generation: u64, agrees: bool) {
        if self.floor.is_some_and(|floor| generation >= floor) {
            self.agrees = agrees;
        }
    }
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
                listing: Source::default(),
                describe: Source::default(),
            },
        );
    }

    /// Whether a confirmed write of `name` is on record.
    pub(in crate::secret_store) fn has_confirmed_deletion_state(&self, name: &str) -> bool {
        self.confirmed_deletions.contains_key(name)
    }

    /// The kind of write `effect` was, as followers are told: a confirmed
    /// scheduled deletion or restore says so, anything else is a plain change.
    pub(in crate::secret_store) fn change_kind_of(
        &self,
        name: &str,
        effect: super::WriteEffect,
    ) -> crate::secret_store::changes::SecretChangeKind {
        use crate::secret_store::changes::SecretChangeKind;

        if effect == super::WriteEffect::Removed {
            return SecretChangeKind::Removed;
        }

        if effect == super::WriteEffect::Versions {
            match self
                .confirmed_deletions
                .get(name)
                .map(|confirmation| confirmation.state)
            {
                Some(ConfirmedDeletion::Scheduled { date }) => {
                    return SecretChangeKind::Scheduled { date };
                }
                Some(ConfirmedDeletion::Restored) => return SecretChangeKind::Restored,
                None => {}
            }
        }

        SecretChangeKind::Changed
    }

    /// An explicit refresh asks the store again and believes its answer.
    pub(in crate::secret_store) fn clear_confirmed_deletions(&mut self) {
        self.confirmed_deletions.clear();
    }

    /// Drops the confirmations another connection session made, and the row
    /// and cached summary conformed to them. Returns whether any went.
    /// Without a live connection nothing is dropped: a disconnect is not
    /// another session until one returns.
    pub(in crate::secret_store) fn drop_confirmations_of_replaced_session(
        &mut self,
        cx: &gpui::Context<Self>,
    ) -> bool {
        let Some(live) = self.connection(cx) else {
            return false;
        };

        let replaced: Vec<String> = self
            .confirmed_deletions
            .iter()
            .filter(|(_, confirmation)| {
                !confirmation
                    .session
                    .as_ref()
                    .is_some_and(|session| Arc::ptr_eq(session, &live))
            })
            .map(|(name, _)| name.clone())
            .collect();

        for name in &replaced {
            self.confirmed_deletions.remove(name);

            if self
                .listing_row
                .as_ref()
                .is_some_and(|row| &row.name == name)
            {
                self.listing_row = None;
            }

            if self
                .summary_cache
                .as_ref()
                .is_some_and(|summary| &summary.name == name)
            {
                self.summary_cache = None;
            }
        }

        !replaced.is_empty()
    }

    /// A listing request was issued: for a confirmation not yet followed by
    /// one, it is the first that can have seen the write.
    pub(in crate::secret_store) fn note_listing_issued(&mut self, generation: u64) {
        for confirmation in self.confirmed_deletions.values_mut() {
            confirmation.listing.issued(generation);
        }
    }

    /// A describe of `name` was issued.
    pub(in crate::secret_store) fn note_describe_issued(&mut self, name: &str, generation: u64) {
        if let Some(confirmation) = self.confirmed_deletions.get_mut(name) {
            confirmation.describe.issued(generation);
        }
    }

    /// Offers the listing's row of a secret, read by the listing request
    /// `generation`, to the override.
    pub(in crate::secret_store) fn observe_listed(
        &mut self,
        summary: &SecretSummary,
        generation: u64,
    ) {
        self.observe(
            summary,
            |confirmation| &mut confirmation.listing,
            generation,
        );
    }

    /// Offers every row a successful listing page returned to the override,
    /// not only the selected one: a row read while another secret was
    /// selected is still the listing's word about its own secret.
    pub(in crate::secret_store) fn observe_listed_rows(
        &mut self,
        rows: &[SecretSummary],
        generation: u64,
        cx: &gpui::Context<Self>,
    ) {
        self.drop_confirmations_of_replaced_session(cx);

        for summary in rows {
            self.observe_listed(summary, generation);
        }
    }

    /// Offers a describe of a secret, issued as `generation`, to the override.
    pub(in crate::secret_store) fn observe_described(
        &mut self,
        summary: &SecretSummary,
        generation: u64,
    ) {
        self.observe(
            summary,
            |confirmation| &mut confirmation.describe,
            generation,
        );
    }

    /// An observation replaces what its source said before; the override ends
    /// once both sources agree.
    fn observe(
        &mut self,
        summary: &SecretSummary,
        source: impl FnOnce(&mut Confirmation) -> &mut Source,
        generation: u64,
    ) {
        let Some(confirmation) = self.confirmed_deletions.get_mut(&summary.name) else {
            return;
        };

        let agrees = confirmation.state.agrees_with(summary);
        source(confirmation).observe(generation, agrees);

        if confirmation.listing.agrees && confirmation.describe.agrees {
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

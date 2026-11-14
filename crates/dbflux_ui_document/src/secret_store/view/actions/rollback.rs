//! Roll back: makes an earlier version of the selected secret the current
//! one. The Versions section draws a Roll back button on every version that
//! is not current; the button and the `b` key open a confirmation naming the
//! version, and the confirm button writes through
//! [`roll_back_secret`](crate::secret_store::roll_back_secret). No value is
//! rewritten, only which version holds the current stage.

use super::{
    DialogShell, FooterSpec, Landing, WriteDialog, WriteEffect, dialog_footer, dialog_modal,
};
use crate::secret_store::SecretOperation;
use crate::secret_store::VersionsState;
use crate::secret_store::view::SecretView;
use crate::secret_store::view::details::short_version_id;
use crate::secret_store::write::roll_back_secret;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::parts::modal_lead;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::ModalMetrics;
use dbflux_core::{SecretRollbackRequest, SecretStoreCapabilities};
use gpui::prelude::*;
use gpui::*;

/// The confirmation of one roll back.
pub(in crate::secret_store) struct RollBackDialog {
    pub name: String,

    /// The version to make current, in the selector the store accepts.
    pub version_id: String,

    /// The version that is current now, which the store keeps as the
    /// previous one.
    pub current_version: Option<String>,
}

/// The capabilities a roll back needs: the same pair the seam requires.
const ROLL_BACK_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::VERSION_LABELS);

impl SecretView {
    /// Whether the store can roll a secret back at all.
    pub(crate) fn supports_roll_back(&self) -> bool {
        self.store_allows(ROLL_BACK_CAPABILITIES)
    }

    /// Whether the history of the selected secret offers a roll back to the
    /// version at `index`: a version that is not current, of a secret
    /// nobody else manages and whose deletion is not pending.
    pub(crate) fn can_roll_back_version(&self, index: usize) -> bool {
        self.supports_roll_back()
            && self.connection_present
            && self.write_refusal_reason().is_none()
            && !self.dialog_is_open()
            && !self.write_in_flight
            && self.version_to_roll_back(index).is_some()
    }

    /// The id of the version at `index` when it is a valid roll back target.
    fn version_to_roll_back(&self, index: usize) -> Option<&str> {
        let VersionsState::Loaded(versions) = &self.versions else {
            return None;
        };

        let version = versions.get(index)?;

        (!version.is_current && !version.is_pending && !self.is_current_version(&version.id))
            .then_some(&*version.id)
    }

    /// Whether the version at `index` is staged by an unfinished change, so a
    /// roll back onto it is refused.
    pub(in crate::secret_store) fn version_is_pending(&self, index: usize) -> bool {
        match &self.versions {
            VersionsState::Loaded(versions) => versions
                .get(index)
                .is_some_and(|version| version.is_pending && !version.is_current),
            _ => false,
        }
    }

    /// The index of the most recent version created before the current one,
    /// which `b` rolls back to. The history lists newest first, so the
    /// versions after the current one are the older ones and the first that
    /// can be rolled back to is the most recent of them. The current version
    /// is not always the first row (after a roll back it is an older one), so
    /// a newer version is never "previous". The order decides, not the dates:
    /// a version with no creation date keeps its place.
    pub(in crate::secret_store) fn previous_version_index(&self) -> Option<usize> {
        let VersionsState::Loaded(versions) = &self.versions else {
            return None;
        };

        let first_older = versions
            .iter()
            .position(|version| self.is_current_version(&version.id))
            .map_or(0, |current| current + 1);

        (first_older..versions.len()).find(|index| self.version_to_roll_back(*index).is_some())
    }

    fn has_pending_earlier_version(&self) -> bool {
        let VersionsState::Loaded(versions) = &self.versions else {
            return false;
        };

        versions
            .iter()
            .any(|version| version.is_pending && !version.is_current)
    }

    /// Roll back to the most recent earlier version: the `b` key. Says why
    /// when there is nothing to roll back to.
    pub(crate) fn roll_back_to_previous(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.supports_roll_back() || self.name.is_none() {
            return;
        }

        if matches!(self.versions, VersionsState::Idle | VersionsState::Loading) {
            self.report_write_warning(
                dbflux_i18n::t!("document.secret_store.rollback.history_loading"),
                cx,
            );
            return;
        }

        match self.previous_version_index() {
            Some(index) => self.open_roll_back(index, window, cx),
            None if self.has_pending_earlier_version() => {
                self.report_write_warning(
                    dbflux_i18n::t!("document.secret_store.rollback.only_pending"),
                    cx,
                );
            }
            None => {
                let name = self
                    .selected_summary()
                    .map(|summary| summary.name.clone())
                    .unwrap_or_default();

                self.report_write_warning(
                    dbflux_i18n::t!(
                        "document.secret_store.rollback.no_earlier_version",
                        name = name.as_str()
                    ),
                    cx,
                );
            }
        }
    }

    /// Opens the confirmation of a roll back to the version at `index` of
    /// the history. Nothing is written until it is confirmed.
    pub(crate) fn open_roll_back(
        &mut self,
        index: usize,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if !self.supports_roll_back() || self.dialog_is_open() || self.write_in_flight {
            return;
        }

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        if let Some(reason) = self.write_refusal_reason() {
            self.report_write_warning(reason, cx);
            return;
        }

        // The ids belong to the session that listed them; against another
        // connection they could name a different version or none.
        if !self.history_session_is_live(cx) {
            self.adopt_connection_session(crate::secret_store::HistoryLoad::Requested, cx);
            return;
        }

        let Some(version_id) = self.version_to_roll_back(index).map(str::to_string) else {
            return;
        };

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        let current_version = self.current_version_id().map(str::to_string);

        self.open_dialog(
            WriteDialog::RollBack(RollBackDialog {
                name,
                version_id,
                current_version,
            }),
            None,
            window,
            cx,
        );
    }

    /// Writes the roll back the dialog confirmed, through the seam.
    pub(in crate::secret_store) fn submit_roll_back(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(WriteDialog::RollBack(dialog)) = &self.dialog else {
            return;
        };

        let name = dialog.name.clone();
        let version_id = dialog.version_id.clone();

        // The history the confirmation was opened over belongs to one
        // session: a replaced connection refuses the write instead of
        // landing it on a version the user did not see.
        if !self.history_session_is_live(cx) {
            self.dismiss_dialog(cx);
            self.report_write_warning(
                dbflux_i18n::t!("document.secret_store.write.connection_replaced"),
                cx,
            );
            self.adopt_connection_session(crate::secret_store::HistoryLoad::Requested, cx);
            return;
        }

        let Some(session) = self.write_session(cx) else {
            return;
        };

        let request = SecretRollbackRequest::new(name.clone(), version_id.clone());

        self.run_governed_write(
            session,
            SecretOperation::RollBack,
            name.clone(),
            move |context| Landing::of(roll_back_secret(context, &request)),
            move |doc, landing, cx| doc.land_roll_back(&name, &version_id, landing, cx),
            cx,
        );
    }

    fn land_roll_back(
        &mut self,
        name: &str,
        version_id: &str,
        landing: Landing<dbflux_core::SecretRollback>,
        cx: &mut Context<Self>,
    ) {
        match landing {
            Landing::Done(_) => {
                self.dismiss_dialog(cx);
                self.toast_written(
                    dbflux_i18n::t!(
                        "document.secret_store.rollback.done",
                        name = name,
                        version = short_version_id(version_id)
                    ),
                    cx,
                );
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Stands(failure) => {
                self.dismiss_dialog(cx);
                self.report_write_failure(&failure, cx);
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Rejected(failure) => {
                self.report_write_failure(&failure, cx);
            }
        }
    }

    pub(in crate::secret_store) fn render_roll_back_dialog(
        &self,
        dialog: &RollBackDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let lead = dbflux_i18n::t!(
            "document.secret_store.rollback.lead",
            name = dialog.name.as_str(),
            version = dialog.version_id.as_str()
        );

        let mut body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(div().id("secret-rollback-lead").child(modal_lead(lead, cx)));

        if let Some(current) = &dialog.current_version {
            body = body.child(
                div().id("secret-rollback-effect").child(
                    Text::caption(dbflux_i18n::t!(
                        "document.secret_store.rollback.effect",
                        current = current.as_str()
                    ))
                    .muted_foreground(),
                ),
            );
        }

        let footer = dialog_footer(
            FooterSpec {
                cancel_id: "secret-rollback-cancel",
                confirm_id: "secret-rollback-confirm",
                confirm_label: dbflux_i18n::t!("document.secret_store.rollback.confirm"),
                confirm_icon: AppIcon::RotateCcw,
                danger: false,
                enabled: true,
                submitting: self.write_in_flight,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-rollback-dialog",
                title: dbflux_i18n::t!(
                    "document.secret_store.rollback.title",
                    name = dialog.name.as_str()
                ),
                icon: AppIcon::RotateCcw,
                danger: false,
                confirm_enabled: !self.write_in_flight,
            },
            body,
            footer,
            self.dialog_focus.handle(),
            cx,
        )
    }
}

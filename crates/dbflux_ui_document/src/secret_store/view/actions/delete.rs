//! Delete and restore.
//!
//! A store with a recovery window (`SecretStoreCapabilities::SOFT_DELETE`)
//! schedules the deletion: the dialog offers the window, 7 to 30 days with 30
//! the default, and never a way to delete without recovery. A store without
//! one deletes with no way back, so its dialog says so and keeps the Delete
//! button disabled until the exact name is typed back, and the request
//! carries the typed text as its confirmation. Both dialogs ask for the
//! name. Nothing is written until the dialog is confirmed.
//!
//! A secret scheduled for deletion shows its state, with the date and the
//! recovery window, and a Restore action that cancels the deletion through
//! [`restore_secret`](crate::secret_store::restore_secret).

use super::{
    DialogShell, FooterSpec, Landing, WriteDialog, WriteEffect, dialog_footer, dialog_modal,
};
use crate::secret_store::SecretOperation;
use crate::secret_store::render::format_time;
use crate::secret_store::view::{DetailsState, SecretView};
use crate::secret_store::write::{SecretWriteFailureKind, delete_secret, restore_secret};
use dbflux_components::controls::{Button, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::parts::{modal_field, modal_lead};
use dbflux_components::primitives::Text;
use dbflux_components::tokens::{ModalMetrics, Spacing};
use dbflux_core::{
    DEFAULT_RECOVERY_WINDOW_DAYS, MAX_RECOVERY_WINDOW_DAYS, MIN_RECOVERY_WINDOW_DAYS,
    SecretDeleteRequest, SecretDeletion, SecretStoreCapabilities,
};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;

/// The recovery windows the dialog offers: the shortest, a middle one and the
/// longest the service accepts.
const RECOVERY_WINDOW_CHOICES: [u32; 3] = [MIN_RECOVERY_WINDOW_DAYS, 14, MAX_RECOVERY_WINDOW_DAYS];

/// What the capabilities a delete needs say: the same pair the seam requires
/// of any deletion.
const DELETE_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::DELETE);

/// What the capabilities a restore needs say.
const RESTORE_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::SOFT_DELETE);

/// How the dialog deletes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::secret_store) enum DeleteKind {
    /// Behind a recovery window of this many days.
    Scheduled { window_days: u32 },

    /// With no way back.
    Irreversible,
}

/// The confirmation of one deletion.
pub(in crate::secret_store) struct DeleteDialog {
    pub name: String,
    pub kind: DeleteKind,

    /// Where the name is typed back.
    pub confirmation: Entity<InputState>,
    _subscription: Subscription,
}

impl DeleteDialog {
    /// Whether the typed text is exactly the name.
    fn is_confirmed(&self, cx: &App) -> bool {
        self.confirmation.read(cx).value() == self.name.as_str()
    }
}

impl SecretView {
    /// Whether the store can delete at all.
    pub(crate) fn supports_delete(&self) -> bool {
        self.store_allows(DELETE_CAPABILITIES)
    }

    /// Whether the store schedules deletions behind a recovery window.
    fn deletes_with_recovery(&self) -> bool {
        self.store_allows(SecretStoreCapabilities::SOFT_DELETE)
    }

    /// Whether Delete may open its dialog for the selection now.
    pub(crate) fn can_delete_selected(&self) -> bool {
        self.supports_delete()
            && self.connection_present
            && !self.write_in_flight
            && !self.dialog_is_open()
            && self.write_refusal_reason().is_none()
            && self.selected_summary().is_some()
    }

    /// Opens the confirmation of deleting the selected secret. Nothing is
    /// written until it is confirmed.
    pub(crate) fn open_delete(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.supports_delete() || self.dialog_is_open() || self.write_in_flight {
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        if let Some(reason) = self.write_refusal_reason() {
            self.report_write_warning(reason, cx);
            return;
        }

        let kind = if self.deletes_with_recovery() {
            DeleteKind::Scheduled {
                window_days: DEFAULT_RECOVERY_WINDOW_DAYS,
            }
        } else {
            DeleteKind::Irreversible
        };

        let confirmation = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.secret_store.delete.confirm_placeholder"
            ))
        });
        let subscription = cx.subscribe(&confirmation, |_, _, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                cx.notify();
            }
        });
        let focus = confirmation.read(cx).focus_handle(cx);

        self.open_dialog(
            WriteDialog::Delete(DeleteDialog {
                name,
                kind,
                confirmation,
                _subscription: subscription,
            }),
            Some(focus),
            window,
            cx,
        );
    }

    /// Picks the recovery window of the open deletion dialog. A window the
    /// service would refuse is ignored.
    pub(crate) fn set_recovery_window(&mut self, days: u32, cx: &mut Context<Self>) {
        if !(MIN_RECOVERY_WINDOW_DAYS..=MAX_RECOVERY_WINDOW_DAYS).contains(&days) {
            return;
        }

        if let Some(WriteDialog::Delete(dialog)) = &mut self.dialog
            && let DeleteKind::Scheduled { window_days } = &mut dialog.kind
        {
            *window_days = days;
            cx.notify();
        }
    }

    /// Writes the deletion the dialog confirmed, through the seam.
    pub(in crate::secret_store) fn submit_delete(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(WriteDialog::Delete(dialog)) = &self.dialog else {
            return;
        };

        // The typed name is the confirmation; without it nothing is sent,
        // whatever key or click got here.
        if !dialog.is_confirmed(cx) {
            return;
        }

        let name = dialog.name.clone();
        let kind = dialog.kind;
        let typed = dialog.confirmation.read(cx).value().to_string();

        let Some(session) = self.write_session(cx) else {
            return;
        };

        let request = match kind {
            DeleteKind::Scheduled { window_days } => {
                SecretDeleteRequest::scheduled(name.clone(), window_days)
            }
            DeleteKind::Irreversible => SecretDeleteRequest::irreversible(name.clone(), typed),
        };

        self.run_governed_write(
            session,
            match kind {
                DeleteKind::Scheduled { .. } => SecretOperation::DeleteScheduled,
                DeleteKind::Irreversible => SecretOperation::DeleteIrreversible,
            },
            name.clone(),
            move |context| Landing::of(delete_secret(context, &request)),
            move |doc, landing, cx| doc.land_delete(&name, kind, landing, cx),
            cx,
        );
    }

    pub(in crate::secret_store) fn land_delete(
        &mut self,
        name: &str,
        kind: DeleteKind,
        landing: Landing<SecretDeletion>,
        cx: &mut Context<Self>,
    ) {
        let effect = match kind {
            DeleteKind::Scheduled { .. } => WriteEffect::Versions,
            DeleteKind::Irreversible => WriteEffect::Removed,
        };

        match landing {
            Landing::Done(deletion) => {
                self.dismiss_dialog(cx);

                let message = match (kind, deletion.deletion_scheduled_for) {
                    (DeleteKind::Scheduled { .. }, Some(date)) => dbflux_i18n::t!(
                        "document.secret_store.delete.done_scheduled",
                        name = name,
                        date = format_time(date)
                    ),
                    (DeleteKind::Scheduled { .. }, None) => dbflux_i18n::t!(
                        "document.secret_store.delete.done_scheduled_undated",
                        name = name
                    ),
                    (DeleteKind::Irreversible, _) => {
                        dbflux_i18n::t!("document.secret_store.delete.done", name = name)
                    }
                };

                self.toast_written(message, cx);

                // The refresh below must not ask for the history with the row
                // this write outdated.
                if matches!(kind, DeleteKind::Scheduled { .. }) {
                    self.confirm_deletion_scheduled(name, deletion.deletion_scheduled_for, cx);
                }
                self.refresh_after_write(name, effect, cx);
            }
            Landing::Stands(failure) => {
                self.dismiss_dialog(cx);

                // The store applied a scheduled delete and only its audit row
                // was lost: the deletion stands, with no date to show. An
                // indeterminate failure says nothing and the refresh reads
                // the store.
                if matches!(kind, DeleteKind::Scheduled { .. })
                    && failure.kind == SecretWriteFailureKind::AuditLost
                {
                    self.confirm_deletion_scheduled(name, None, cx);
                }
                self.report_write_failure(&failure, cx);
                self.refresh_after_write(name, effect, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    // -- Restore ------------------------------------------------------------

    /// Whether the store can cancel a scheduled deletion.
    pub(crate) fn supports_restore(&self) -> bool {
        self.store_allows(RESTORE_CAPABILITIES)
    }

    /// Whether the selection is a secret scheduled for deletion that Restore
    /// may cancel now.
    pub(crate) fn can_restore_selected(&self) -> bool {
        self.supports_restore()
            && self.connection_present
            && !self.write_in_flight
            && !self.dialog_is_open()
            && self
                .selected_summary()
                .is_some_and(|summary| self.is_deletion_pending(summary))
    }

    /// Cancels the scheduled deletion of the selected secret through the
    /// seam. Restoring writes nothing the user would lose, so it asks for no
    /// confirmation beyond the button.
    pub(crate) fn restore_selected(&mut self, cx: &mut Context<Self>) {
        if !self.can_restore_selected() {
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        // The restore is judged on the details the user saw, which belong to
        // one session: another one is refused, like a roll back's history.
        if !self.details_session_is_live(cx) {
            self.report_write_warning(
                dbflux_i18n::t!("document.secret_store.write.connection_replaced"),
                cx,
            );
            self.refresh_selection_for_session(cx);
            return;
        }

        let Some(session) = self.write_session(cx) else {
            return;
        };

        let request_name = name.clone();

        self.run_governed_write(
            session,
            SecretOperation::Restore,
            name.clone(),
            move |context| Landing::of(restore_secret(context, &request_name)),
            move |doc, landing, cx| doc.land_restore(&name, landing, cx),
            cx,
        );
    }

    pub(in crate::secret_store) fn land_restore(
        &mut self,
        name: &str,
        landing: Landing<()>,
        cx: &mut Context<Self>,
    ) {
        match landing {
            Landing::Done(()) => {
                self.toast_written(
                    dbflux_i18n::t!("document.secret_store.restore.done", name = name),
                    cx,
                );
                // The reads below may fail or come back stale: the guards
                // must not keep reading the deletion this write cancelled.
                self.confirm_restored(name, cx);
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Stands(failure) => {
                // The store applied the restore and only its audit row was
                // lost: the cancellation stands. An indeterminate failure
                // says nothing, and the refresh below reads the store.
                if failure.kind == SecretWriteFailureKind::AuditLost {
                    self.confirm_restored(name, cx);
                }
                self.report_write_failure(&failure, cx);
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    /// A newer listing row shows `name` active: the describe and the cached
    /// row it outdates stop saying the deletion is scheduled.
    pub(in crate::secret_store) fn lift_stale_deletion(&mut self, name: &str) {
        if !self.is_for(name) {
            return;
        }

        for summary in [self.listing_row.as_mut(), self.summary_cache.as_mut()]
            .into_iter()
            .flatten()
        {
            summary.deletion_scheduled_for = None;
        }

        if let DetailsState::Loaded(details) = &mut self.details {
            details.summary.deletion_scheduled_for = None;
            details.deleted_at = None;
        }
    }

    // -- Rendering ------------------------------------------------------------

    pub(in crate::secret_store) fn render_delete_dialog(
        &self,
        dialog: &DeleteDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let noun_key = if self.hierarchical() {
            "parameter"
        } else {
            "secret"
        };
        let confirmed = dialog.is_confirmed(cx);

        let lead = match dialog.kind {
            DeleteKind::Scheduled { .. } => dbflux_i18n::t!(
                "document.secret_store.delete.lead_scheduled",
                name = dialog.name.as_str()
            ),
            DeleteKind::Irreversible => dbflux_i18n::t!(
                "document.secret_store.delete.lead_irreversible",
                name = dialog.name.as_str()
            ),
        };

        let mut body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(modal_lead(lead, cx));

        if let DeleteKind::Scheduled { window_days } = dialog.kind {
            body = body.child(self.render_recovery_window(window_days, cx));
        }

        body = body.child(modal_field(
            dbflux_i18n::t!(&format!(
                "document.secret_store.delete.confirm_label.{noun_key}"
            )),
            Input::new(&dialog.confirmation)
                .id("secret-delete-name-input")
                .aria_label(dbflux_i18n::t!(
                    "document.secret_store.delete.confirm_aria",
                    name = dialog.name.as_str()
                ))
                .w_full(),
            cx,
        ));

        let (confirm_label, confirm_icon) = match dialog.kind {
            DeleteKind::Scheduled { .. } => (
                dbflux_i18n::t!("document.secret_store.delete.confirm_scheduled"),
                AppIcon::Delete,
            ),
            DeleteKind::Irreversible => (
                dbflux_i18n::t!("document.secret_store.delete.confirm_irreversible"),
                AppIcon::Delete,
            ),
        };

        let footer = dialog_footer(
            FooterSpec {
                cancel_id: "secret-delete-cancel",
                confirm_id: "secret-delete-confirm",
                confirm_label,
                confirm_icon,
                danger: true,
                enabled: confirmed,
                submitting: self.write_in_flight,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-delete-dialog",
                title: dbflux_i18n::t!(&format!("document.secret_store.delete.title.{noun_key}")),
                icon: AppIcon::Delete,
                danger: true,
                confirm_enabled: confirmed && !self.write_in_flight,
            },
            body,
            footer,
            self.dialog_focus.handle(),
            cx,
        )
    }

    /// The recovery window choice: 7, 14 or 30 days. Each choice is a
    /// button, so Tab reaches it and Space or Enter picks it.
    fn render_recovery_window(&self, selected: u32, cx: &mut Context<Self>) -> Div {
        let theme = cx.theme();
        let fill = theme.secondary;
        let border = theme.border;

        let choices = RECOVERY_WINDOW_CHOICES.iter().map(|days| {
            let days = *days;

            Button::new(
                SharedString::from(format!("secret-delete-window-{days}")),
                dbflux_i18n::t!("document.secret_store.delete.window_option", count = days),
            )
            .ghost()
            .selected(days == selected)
            .toggled(days == selected)
            .on_click(cx.listener(move |this, _, _, cx| this.set_recovery_window(days, cx)))
        });

        div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::HEADER_GAP)
            .child(modal_field(
                dbflux_i18n::t!("document.secret_store.delete.window_label"),
                div().flex().child(
                    div()
                        .id("secret-delete-window")
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .p(Spacing::XS)
                        .bg(fill)
                        .border_1()
                        .border_color(border)
                        .children(choices),
                ),
                cx,
            ))
            .child(
                Text::caption(dbflux_i18n::t!("document.secret_store.delete.window_hint"))
                    .muted_foreground(),
            )
    }
}

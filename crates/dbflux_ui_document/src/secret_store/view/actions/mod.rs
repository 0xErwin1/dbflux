//! The write actions of the details pane and the row menu: roll back,
//! delete and restore, rotate, edit tags and description, and the write half
//! of create.
//!
//! Every action runs through the governed write seam
//! (`crate::secret_store::write`): the view never calls the store itself. An action that needs the user's
//! input opens a dialog (`WriteDialog`) over the host; the dialog's
//! confirm button builds the request, hands it to
//! [`SecretView::run_governed_write`] on the background executor and
//! lands the answer here. A refusal or a failure is reported once, through
//! the same pieces the value editor reports a refused save with, and keeps
//! the dialog open; a write that landed closes it and refreshes what the
//! write changed — the details, the history and the listing.
//!
//! What an action offers follows the store's `SecretStoreCapabilities`,
//! never a driver id.

mod bar;
mod create;
mod delete;
mod description;
mod rollback;
mod rotation;
mod tags;

use super::editor::write_failure_to_user_facing;
use super::{DetailsState, SecretView, SecretViewEvent};
use crate::secret_store::changes::SecretChangeKind;
use crate::secret_store::write::{
    SecretOperation, SecretWriteFailure, SecretWriteFailureKind, WriteContext, WriteTarget,
    store_unavailable_failure,
};
use dbflux_audit::AuditService;
use dbflux_components::controls::Button;
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::{Modal, ModalVariant};
use dbflux_components::tokens::ModalMetrics;
use dbflux_core::observability::EventSeverity;
use dbflux_core::{
    Connection, DbError, LogErr, MutationPolicy, ReadOnlyReason, SecretStoreCapabilities,
};
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error};
use gpui::prelude::*;
use gpui::*;
use std::sync::Arc;
use uuid::Uuid;

pub(in crate::secret_store) use delete::DeleteDialog;
#[cfg(test)]
pub(in crate::secret_store) use delete::DeleteKind;
pub(in crate::secret_store) use description::DescriptionDialog;
pub(in crate::secret_store) use rollback::RollBackDialog;
pub(in crate::secret_store) use rotation::{RotateNowDialog, RotationDialog, RotationNotice};
pub(in crate::secret_store) use tags::TagsDialog;
pub(in crate::secret_store) use tags::{TagRow, new_tag_row};

/// The dialog open over the host, if any. Only one is ever open: a
/// dialog is modal, and opening another waits for it to close.
pub(in crate::secret_store) enum WriteDialog {
    RollBack(RollBackDialog),
    Delete(DeleteDialog),
    Rotation(RotationDialog),
    RotateNow(RotateNowDialog),
    Tags(TagsDialog),
    Description(DescriptionDialog),
    /// The host's New secret dialog: its fields belong to the host, the
    /// view only holds the slot so that one dialog is open at a time and the
    /// write waits for its landing.
    Create,
}

/// What an open dialog was opened on: the connection session and the row. A
/// confirm that finds either changed writes nothing, so a dialog can never
/// land on a session or a secret other than the ones it was opened on.
pub(in crate::secret_store) struct DialogPin {
    connection: Option<Arc<dyn Connection>>,
    selected: Option<String>,
}

/// Everything a governed write takes from the view, captured on the
/// foreground so the write itself can run on the background executor: the
/// connection session, the connection's resolved mutation policy and the
/// audit service.
pub(in crate::secret_store) struct WriteSession {
    connection: Arc<dyn Connection>,
    policy: MutationPolicy,
    read_only_reason: Option<ReadOnlyReason>,
    driver_id: String,
    audit: AuditService,
    store_id: String,
    store_label: String,
    profile_id: Uuid,
}

/// How a governed write ended, as the view lands it.
pub(in crate::secret_store) enum Landing<T> {
    /// The store accepted the write and its audit event was recorded.
    Done(T),

    /// The write stands, or may stand: the store accepted it and the audit
    /// event was lost, or the store failed in a way that does not say. The
    /// document refreshes what the write may have changed and reports the
    /// failure.
    Stands(SecretWriteFailure),

    /// Nothing was written: a gate refused the request or the store failed
    /// it. The dialog stays open so the request can be fixed or dropped.
    Rejected(SecretWriteFailure),
}

impl<T> Landing<T> {
    pub(in crate::secret_store) fn of(result: Result<T, SecretWriteFailure>) -> Self {
        match result {
            Ok(value) => Self::Done(value),
            Err(failure) => match failure.kind {
                SecretWriteFailureKind::AuditLost | SecretWriteFailureKind::Indeterminate => {
                    Self::Stands(failure)
                }
                SecretWriteFailureKind::Refused | SecretWriteFailureKind::StoreFailed => {
                    Self::Rejected(failure)
                }
            },
        }
    }
}

/// What a landed write changed, which decides what the view reloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteEffect {
    /// The versions of the secret: the shown value is stale.
    Versions,

    /// The secret's metadata only: the value shown stays valid.
    Metadata,

    /// The secret is gone: nothing of it stays selected.
    Removed,
}

impl SecretView {
    /// Whether a write dialog is open over the host.
    pub(crate) fn dialog_is_open(&self) -> bool {
        self.dialog.is_some()
    }

    /// Whether the open dialog is the host's New secret form.
    pub(in crate::secret_store) fn create_slot_is_open(&self) -> bool {
        matches!(self.dialog, Some(WriteDialog::Create))
    }

    /// Whether a governed write is in flight.
    pub(crate) fn write_in_flight(&self) -> bool {
        self.write_in_flight
    }

    /// The focus handle a dialog holds the keyboard with.
    pub(in crate::secret_store) fn dialog_focus_handle(&self) -> &FocusHandle {
        self.dialog_focus.handle()
    }

    /// The refusal the secret's own state imposes on every write: another
    /// service manages it, or its deletion is scheduled. The listing and the
    /// freshest describe agree on both.
    pub(in crate::secret_store) fn write_refusal_reason(&self) -> Option<String> {
        let summary = self.selected_summary()?;

        let described = match &self.details {
            DetailsState::Loaded(details) => Some(&details.summary),
            _ => None,
        };

        for candidate in std::iter::once(summary).chain(described) {
            if let Some(service) = candidate.owning_service.as_deref() {
                return Some(dbflux_i18n::t!(
                    "document.secret_store.write.refused_managed",
                    name = candidate.name.as_str(),
                    service = service
                ));
            }

            if self.is_deletion_pending(candidate) {
                return Some(dbflux_i18n::t!(
                    "document.secret_store.write.refused_deletion",
                    name = candidate.name.as_str()
                ));
            }
        }

        None
    }

    /// Whether the store advertises every capability of a write action.
    pub(in crate::secret_store) fn store_allows(&self, required: SecretStoreCapabilities) -> bool {
        self.store.supports(required)
    }

    /// Reports a write the view will not start, as a decision of the
    /// kind the editor reports a refused save with.
    pub(in crate::secret_store) fn report_write_warning(
        &self,
        reason: String,
        cx: &mut Context<Self>,
    ) {
        report_error(
            UserFacingError::new(ErrorKind::User, reason).with_severity(EventSeverity::Warn),
            cx,
        );
    }

    /// Captures what a governed write needs from the view. A connection
    /// that is gone is reported, as a save without one is.
    pub(in crate::secret_store) fn write_session(
        &mut self,
        cx: &mut Context<Self>,
    ) -> Option<WriteSession> {
        if self.region_switch_blocks_writes(cx)
            || self.editor_save_blocks_writes(cx)
            || self.region_mismatch(cx)
        {
            return None;
        }

        let connected = self.live_profile(cx);

        let Some(connected) = connected else {
            self.missing_connection_reported = true;
            report_error(
                UserFacingError::new(
                    ErrorKind::User,
                    dbflux_i18n::t!("document.secret_store.error.connection_unavailable"),
                ),
                cx,
            );
            return None;
        };

        let session = WriteSession {
            connection: connected.connection.clone(),
            policy: connected.mutation_policy,
            read_only_reason: connected.read_only_reason,
            driver_id: connected.connection.metadata().id.clone(),
            audit: self.app_state.read(cx).audit_service().clone(),
            store_id: self.store.id.clone(),
            store_label: self.store.label.clone(),
            profile_id: self.profile_id,
        };

        Some(session)
    }

    /// Runs one governed write on the background executor — the seam is
    /// synchronous — and lands its answer on the foreground through `land`.
    /// `operation` and `name` say what the write is, for the audit row of a
    /// write whose connection turns out to have no such store.
    pub(in crate::secret_store) fn run_governed_write<T: Send + 'static>(
        &mut self,
        session: WriteSession,
        operation: SecretOperation,
        name: String,
        work: impl FnOnce(&WriteContext<'_>) -> Landing<T> + Send + 'static,
        land: impl FnOnce(&mut Self, Landing<T>, &mut Context<Self>) + 'static,
        cx: &mut Context<Self>,
    ) {
        let entity = cx.entity().clone();
        let captured = session.connection.clone();
        self.write_in_flight = true;

        // The write may rewrite what the editor holds (a description change
        // on a store that rewrites the value), so an editor that holds
        // nothing unsaved closes now, and none opens while this is in flight.
        // A tag write never touches the value: its editor stays.
        if operation.may_rewrite_value() && !self.has_unsaved_value_edits(cx) {
            self.value_edit = None;
        }
        cx.notify();

        let task = cx.background_executor().spawn(async move {
            let WriteSession {
                connection,
                policy,
                read_only_reason,
                driver_id,
                audit,
                store_id,
                store_label,
                profile_id,
            } = session;

            match connection.secret_store(&store_id) {
                Some(store) => work(&WriteContext {
                    store,
                    profile_id,
                    driver_id: driver_id.as_str(),
                    policy,
                    read_only_reason,
                    audit: &audit,
                }),
                None => Landing::Rejected(store_unavailable_failure(
                    &audit,
                    operation,
                    &WriteTarget {
                        profile_id,
                        driver_id: driver_id.as_str(),
                        store_id: store_id.as_str(),
                        store_label: store_label.as_str(),
                        name: name.as_str(),
                    },
                    DbError::NotSupported(dbflux_i18n::t!(
                        "document.secret_store.error.store_unavailable",
                        store = store_id
                    )),
                )),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |view, cx| {
                    view.write_in_flight = false;

                    // The view describes the live session; a write that
                    // finished on another one changes none of it.
                    let same_session = view
                        .connection(cx)
                        .is_some_and(|live| Arc::ptr_eq(&live, &captured));

                    if same_session {
                        land(view, result, cx);
                    } else {
                        view.land_on_replaced_session(result, cx);
                    }
                });
            });
        })
        .detach();
    }

    /// Reports the outcome of a write that finished after the connection was
    /// replaced. The write happened (or failed) on the previous session, so
    /// the view is not touched beyond closing its dialog.
    fn land_on_replaced_session<T>(&mut self, landing: Landing<T>, cx: &mut Context<Self>) {
        self.dismiss_dialog(cx);
        self.adopt_details_session(cx);

        match landing {
            Landing::Done(_) => self.report_write_warning(
                dbflux_i18n::t!("document.secret_store.write.landed_on_previous_session"),
                cx,
            ),
            Landing::Stands(failure) | Landing::Rejected(failure) => {
                self.report_write_failure(&failure, cx);
            }
        }
    }

    /// Whether the details of the selection were read from the live session.
    pub(in crate::secret_store) fn details_session_is_live(&self, cx: &Context<Self>) -> bool {
        match (&self.details_session, self.connection(cx)) {
            (Some(loaded), Some(live)) => Arc::ptr_eq(loaded, &live),
            _ => false,
        }
    }

    /// Reads the selected secret again from the live session: its details,
    /// its history and its listing row.
    pub(crate) fn refresh_selection_for_session(&mut self, cx: &mut Context<Self>) {
        let Some(name) = self.name.clone() else {
            return;
        };

        self.describe(name.clone(), cx);
        self.request_versions(name, crate::secret_store::HistoryLoad::Automatic, cx);
        cx.emit(SecretViewEvent::ReloadListing);
    }

    /// After a reconnect that kept the region, the selection still describes
    /// the previous session: read it again without waiting for the user.
    pub(in crate::secret_store) fn adopt_details_session(&mut self, cx: &mut Context<Self>) {
        // Details first requested with no connection leave no session at
        // all, which is as stale as one from a replaced connection.
        let stale = self.connection(cx).is_some() && !self.details_session_is_live(cx);

        // The history follows the connection on its own
        // (`adopt_connection_session`); only the details and the row are left.
        if stale && self.write_in_flight {
            // The read waits for the write, but what was loaded from a
            // previous session must not stay on screen meanwhile; the write's
            // landing reads again. Details that were never read hold nothing
            // to drop.
            if self.details_session.take().is_some() {
                self.details_generation = self.details_generation.wrapping_add(1);
                self.details = DetailsState::Loading;
                cx.notify();
            }
        } else if stale && let Some(name) = self.name.clone() {
            self.describe(name, cx);
            cx.emit(SecretViewEvent::ReloadListing);
        }
    }

    /// An editor save is in flight, and an action write would race it.
    fn editor_save_blocks_writes(&mut self, cx: &mut Context<Self>) -> bool {
        if !self.editor_save_in_flight {
            return false;
        }

        self.report_write_warning(
            dbflux_i18n::t!("document.secret_store.write.in_progress"),
            cx,
        );
        true
    }

    /// Reports a failure the seam already audited, once.
    pub(in crate::secret_store) fn report_write_failure(
        &self,
        failure: &SecretWriteFailure,
        cx: &mut Context<Self>,
    ) {
        let user_error = write_failure_to_user_facing(failure);

        self.report_seam_write_failure(user_error, cx);
    }

    /// Tells the user a write landed.
    pub(in crate::secret_store) fn toast_written(&self, message: String, cx: &mut Context<Self>) {
        Toast::success(message).meta_right(now_hms()).push(cx);
    }

    /// Reloads what a landed write changed. The details and the history of
    /// the secret are read again when it is still the selection, and the
    /// listing is reloaded in place so a row's rotation, description or
    /// deletion date follows the store.
    pub(crate) fn refresh_after_write(
        &mut self,
        name: &str,
        effect: WriteEffect,
        cx: &mut Context<Self>,
    ) {
        // The other views learn what was written to `name`, which the
        // selection may no longer be.
        self.announce_change(name, self.change_kind_of(name, effect), cx);

        if effect == WriteEffect::Removed {
            self.forget_removed_secret(name, cx);
        } else if self.is_for(name) {
            if effect == WriteEffect::Versions {
                // A deletion or a restore keeps a draft (read-only while the
                // secret is pending); the value it was opened from goes.
                if self.has_unsaved_value_edits(cx) && self.has_confirmed_deletion_state(name) {
                    self.follow_external_change(cx);
                } else {
                    self.clear_value();
                }
            }

            self.describe(name.to_string(), cx);
            self.request_versions(
                name.to_string(),
                crate::secret_store::HistoryLoad::Automatic,
                cx,
            );
        }

        cx.emit(SecretViewEvent::ReloadListing);
        cx.notify();
    }

    // -- Dialog plumbing --------------------------------------------------

    /// Opens `dialog` over the host and moves focus into it. `focus` is
    /// the control that should hold the keyboard first, if the dialog has
    /// one.
    pub(in crate::secret_store) fn open_dialog(
        &mut self,
        dialog: WriteDialog,
        focus: Option<FocusHandle>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.region_switch_blocks_writes(cx)
            || self.editor_save_blocks_writes(cx)
            || self.region_mismatch(cx)
        {
            return;
        }

        // A dialog about an existing secret needs details read from the live
        // session: after a reconnect they describe another account's secret
        // of the same name.
        if !matches!(dialog, WriteDialog::Create) && !self.details_session_is_live(cx) {
            self.report_write_warning(
                dbflux_i18n::t!("document.secret_store.write.details_stale"),
                cx,
            );
            self.refresh_selection_for_session(cx);
            return;
        }

        cx.emit(SecretViewEvent::DialogOpened);
        self.dialog_pin = Some(DialogPin {
            connection: self.connection(cx),
            selected: self.name.clone(),
        });
        self.dialog = Some(dialog);
        self.dialog_focus.focus(focus.as_ref(), window, cx);
        cx.notify();
    }

    /// Closes the dialog without writing, and hands the keyboard back.
    pub(crate) fn close_dialog(&mut self, cx: &mut Context<Self>) {
        if self.dialog_is_submitting() {
            return;
        }

        self.dismiss_dialog(cx);
    }

    /// Drops the dialog whether or not a write is in flight: the landing of
    /// a write that already left must still close what it belongs to.
    pub(in crate::secret_store) fn dismiss_dialog(&mut self, cx: &mut Context<Self>) {
        self.dialog_pin = None;

        if self.dialog.take().is_some() {
            self.dialog_focus.restore(cx);
            cx.emit(SecretViewEvent::DialogClosed);
            cx.notify();
        }
    }

    /// Whether the open dialog's write is in flight.
    pub(in crate::secret_store) fn dialog_is_submitting(&self) -> bool {
        self.dialog.is_some() && self.write_in_flight
    }

    /// Closes the open dialog because the selection or the listing is moving
    /// on, even while a write is in flight: its landing still reports.
    pub(in crate::secret_store) fn close_dialog_for_navigation(&mut self, cx: &mut Context<Self>) {
        self.dismiss_dialog(cx);
    }

    /// A region switch is reconnecting, so the session a write would land on
    /// is about to be replaced: nothing starts, and the user is told why.
    pub(in crate::secret_store) fn region_switch_blocks_writes(
        &mut self,
        cx: &mut Context<Self>,
    ) -> bool {
        if !self.region_switching.get() {
            return false;
        }

        self.report_write_warning(
            dbflux_i18n::t!("document.secret_store.write.region_switching"),
            cx,
        );
        true
    }

    /// Closes the open dialog when the connection it was opened on is no
    /// longer the live one, without waiting for a confirm.
    pub(in crate::secret_store) fn close_dialog_if_session_replaced(
        &mut self,
        cx: &mut Context<Self>,
    ) {
        let replaced = match (&self.dialog_pin, self.connection(cx)) {
            (
                Some(DialogPin {
                    connection: Some(pinned),
                    ..
                }),
                Some(live),
            ) => !Arc::ptr_eq(pinned, &live),
            (
                Some(DialogPin {
                    connection: Some(_),
                    ..
                }),
                None,
            ) => true,
            _ => false,
        };

        if replaced && !self.write_in_flight {
            self.dismiss_dialog(cx);
        }
    }

    /// Whether the open dialog still stands on what it was opened on. A
    /// dialog whose connection was replaced, or whose secret is no longer the
    /// selection, is closed and the user told; nothing is written.
    fn dialog_pin_holds(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(pin) = &self.dialog_pin else {
            return true;
        };

        let same_session = match (&pin.connection, self.connection(cx)) {
            (Some(pinned), Some(live)) => Arc::ptr_eq(pinned, &live),
            (None, None) => true,
            _ => false,
        };
        // Creating names no existing secret, so the selection does not matter.
        let same_row =
            matches!(self.dialog, Some(WriteDialog::Create)) || pin.selected == self.name;

        if same_session && same_row {
            return true;
        }

        self.dismiss_dialog(cx);
        self.report_write_warning(
            dbflux_i18n::t!("document.secret_store.write.connection_replaced"),
            cx,
        );
        false
    }

    /// Confirms the open dialog: the one primary action of whichever dialog
    /// is up, as its button and Enter both run it.
    ///
    /// Returns whether the confirmed dialog is the host's own (New secret),
    /// which the host sends itself; every other dialog is sent from here.
    pub(crate) fn confirm_dialog(&mut self, window: &mut Window, cx: &mut Context<Self>) -> bool {
        if self.dialog_is_submitting() || !self.dialog_pin_holds(cx) {
            return false;
        }

        match &self.dialog {
            Some(WriteDialog::RollBack(_)) => self.submit_roll_back(window, cx),
            Some(WriteDialog::Delete(_)) => self.submit_delete(window, cx),
            Some(WriteDialog::Rotation(_)) => self.submit_rotation(window, cx),
            Some(WriteDialog::RotateNow(_)) => self.submit_rotate_now(window, cx),
            Some(WriteDialog::Tags(_)) => self.submit_tags(window, cx),
            Some(WriteDialog::Description(_)) => self.submit_description(window, cx),
            Some(WriteDialog::Create) => return true,
            None => {}
        }

        false
    }

    pub(crate) fn render_write_dialog(&self, cx: &mut Context<Self>) -> Option<AnyElement> {
        let dialog = self.dialog.as_ref()?;

        Some(match dialog {
            WriteDialog::Create => return None,
            WriteDialog::RollBack(dialog) => self.render_roll_back_dialog(dialog, cx),
            WriteDialog::Delete(dialog) => self.render_delete_dialog(dialog, cx),
            WriteDialog::Rotation(dialog) => self.render_rotation_dialog(dialog, cx),
            WriteDialog::RotateNow(dialog) => self.render_rotate_now_dialog(dialog, cx),
            WriteDialog::Tags(dialog) => self.render_tags_dialog(dialog, cx),
            WriteDialog::Description(dialog) => self.render_description_dialog(dialog, cx),
        })
    }
}

/// What a dialog's modal and footer call back into: the entity that owns
/// the dialog, the view for its own dialogs and a host for the one it draws
/// itself.
pub(in crate::secret_store) trait DialogHost: Sized + 'static {
    fn close_dialog_from_modal(&mut self, cx: &mut Context<Self>);
    fn confirm_dialog_from_modal(&mut self, window: &mut Window, cx: &mut Context<Self>);
}

impl DialogHost for SecretView {
    fn close_dialog_from_modal(&mut self, cx: &mut Context<Self>) {
        self.close_dialog(cx);
    }

    fn confirm_dialog_from_modal(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.confirm_dialog(window, cx);
    }
}

/// The chrome every write dialog shares.
pub(in crate::secret_store) struct DialogShell {
    /// Element id of the backdrop.
    pub id: &'static str,
    pub title: String,
    pub icon: AppIcon,

    /// A destructive dialog: the red edge and a danger primary button.
    pub danger: bool,
    pub confirm_enabled: bool,
}

/// One dialog in its `Modal`: Escape and the close button cancel it, Enter
/// confirms it while the confirm button is enabled.
pub(in crate::secret_store) fn dialog_modal<H: DialogHost>(
    shell: DialogShell,
    body: impl IntoElement,
    footer: impl IntoElement,
    focus: &FocusHandle,
    cx: &mut Context<H>,
) -> AnyElement {
    let entity = cx.entity().downgrade();

    Modal::new(shell.title)
        .id(shell.id)
        .icon(shell.icon)
        .variant(if shell.danger {
            ModalVariant::Danger
        } else {
            ModalVariant::Default
        })
        .body(body)
        .footer(footer)
        .focus_handle(focus)
        .on_close({
            let entity = entity.clone();
            move |_, cx| {
                entity
                    .update(cx, |this, cx| this.close_dialog_from_modal(cx))
                    .log_err();
            }
        })
        .on_confirm(move |window, cx| {
            entity
                .update(cx, |this, cx| this.confirm_dialog_from_modal(window, cx))
                .log_err();
        })
        .confirm_enabled(shell.confirm_enabled)
        .into_any_element()
}

/// What a dialog's footer draws.
pub(in crate::secret_store) struct FooterSpec {
    pub cancel_id: &'static str,
    pub confirm_id: &'static str,
    pub confirm_label: String,
    pub confirm_icon: AppIcon,

    /// The primary button is the danger one.
    pub danger: bool,

    /// The request can be sent.
    pub enabled: bool,

    /// A write is in flight.
    pub submitting: bool,
}

/// The Cancel and primary buttons of a dialog. Cancel sits out while a write
/// is in flight, and the primary button until the request can be sent.
pub(in crate::secret_store) fn dialog_footer<H: DialogHost>(
    spec: FooterSpec,
    cx: &mut Context<H>,
) -> Div {
    let confirm = Button::new(spec.confirm_id, spec.confirm_label)
        .icon(if spec.submitting {
            AppIcon::Loader
        } else {
            spec.confirm_icon
        })
        .disabled(!spec.enabled || spec.submitting)
        .on_click(cx.listener(|this, _, window, cx| this.confirm_dialog_from_modal(window, cx)));

    div()
        .flex()
        .items_center()
        .gap(ModalMetrics::FOOTER_GAP)
        .child(
            Button::new(
                spec.cancel_id,
                dbflux_i18n::t!("document.secret_store.dialog.cancel"),
            )
            .disabled(spec.submitting)
            .on_click(cx.listener(|this, _, _, cx| this.close_dialog_from_modal(cx))),
        )
        .child(if spec.danger {
            confirm.danger()
        } else {
            confirm.primary()
        })
}

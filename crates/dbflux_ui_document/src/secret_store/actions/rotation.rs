//! Rotation: Rotate now and the Edit rotation dialog.
//!
//! Rotate now starts a rotation through
//! [`rotate_secret_now`](crate::secret_store::rotate_secret_now). A secret
//! whose rotation is off is refused before anything is sent, with the reason,
//! because no store rotates a secret that is not set up to rotate.
//!
//! Edit rotation turns automatic rotation on, with its schedule, window and
//! function, or off, through
//! [`update_secret_rotation`](crate::secret_store::update_secret_rotation).
//! The dialog shows the seam's own validation message under the fields while
//! the request would be refused, and sends nothing until it passes. Turning
//! rotation off can leave a version holding the pending stage of a rotation
//! that never finished; the store reports it and the pane shows a warning that
//! names the version, because a later rotation can be refused until the
//! labels are reconciled.

use super::{
    DialogShell, FooterSpec, Landing, WriteDialog, WriteEffect, dialog_footer, dialog_modal,
};
use crate::secret_store::write::{rotate_secret_now, update_secret_rotation};
use crate::secret_store::{DetailsState, SecretRowKey, SecretStoreDocument};
use dbflux_components::controls::{Checkbox, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::parts::modal_field;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::ModalMetrics;
use dbflux_core::{
    SecretRotation, SecretRotationChange, SecretRotationStart, SecretRotationUpdate,
    SecretStoreCapabilities,
};
use gpui::prelude::*;
use gpui::*;

/// The capabilities a rotation action needs: the same pair the seam requires.
const ROTATION_CAPABILITIES: SecretStoreCapabilities =
    SecretStoreCapabilities::WRITE.union(SecretStoreCapabilities::ROTATION);

/// A rotation that was cancelled before it finished, which the pane keeps
/// showing while its secret stays selected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::secret_store) struct RotationNotice {
    pub name: String,

    /// The version that still holds the pending stage.
    pub version: String,
}

/// The Edit rotation dialog.
pub(in crate::secret_store) struct RotationDialog {
    pub name: String,

    /// Whether automatic rotation is on after the change.
    pub enabled: bool,

    /// Whether rotation was on when the dialog opened.
    was_enabled: bool,
    pub schedule: Entity<InputState>,
    pub window: Entity<InputState>,
    pub function: Entity<InputState>,

    /// A field was edited since the dialog opened, so an incomplete request
    /// is worth reporting.
    touched: bool,
    _subscriptions: Vec<Subscription>,
}

/// The text of an input without its surrounding spaces, `None` when empty.
fn trimmed(input: &Entity<InputState>, cx: &App) -> Option<String> {
    let text = input.read(cx).value().trim().to_string();

    (!text.is_empty()).then_some(text)
}

impl RotationDialog {
    /// The request the dialog would send: rotation off, or on with the fields
    /// that were filled in. A window or a function left empty keeps the one
    /// the secret has.
    fn request(&self, cx: &App) -> SecretRotationUpdate {
        if !self.enabled {
            return SecretRotationUpdate::disable(self.name.clone());
        }

        let mut update = SecretRotationUpdate {
            name: self.name.clone(),
            enabled: true,
            schedule: trimmed(&self.schedule, cx),
            window: None,
            function: None,
        };
        update.window = trimmed(&self.window, cx);
        update.function = trimmed(&self.function, cx);

        update
    }

    /// Why the request cannot be sent yet: the seam's own validation
    /// message, or that there is nothing to turn off.
    fn problem(&self, cx: &App) -> Option<String> {
        if !self.enabled && !self.was_enabled {
            return Some(dbflux_i18n::t!(
                "document.secret_store.rotation_edit.already_off"
            ));
        }

        self.request(cx)
            .validate()
            .err()
            .map(|error| error.to_string())
    }
}

impl SecretStoreDocument {
    /// Whether the store offers rotation actions at all.
    pub fn supports_rotation(&self) -> bool {
        self.store_allows(ROTATION_CAPABILITIES)
    }

    /// The selected secret's rotation, once its details loaded.
    fn selected_rotation(&self) -> Option<&SecretRotation> {
        match &self.details {
            DetailsState::Loaded(details) => details.summary.rotation.as_ref(),
            _ => None,
        }
    }

    /// Whether the selected secret's rotation is known to be off. A secret
    /// whose details have not loaded is not known to be off.
    fn rotation_is_off(&self) -> bool {
        matches!(self.details, DetailsState::Loaded(_))
            && !self
                .selected_rotation()
                .is_some_and(|rotation| rotation.enabled)
    }

    /// Whether a rotation action may start now.
    pub fn can_start_rotation_action(&self) -> bool {
        self.supports_rotation()
            && !self.write_in_flight
            && !self.dialog_is_open()
            && self.write_refusal_reason().is_none()
            && matches!(self.selected, Some(SecretRowKey::Secret(_)))
    }

    /// Why Rotate now cannot run for the selection, when something says so:
    /// the secret's own state, or its rotation being off. The seam refuses a
    /// rotation of a secret that does not rotate too; this keeps the button
    /// from offering what it knows will be refused.
    pub fn rotate_now_block_reason(&self) -> Option<String> {
        if let Some(reason) = self.write_refusal_reason() {
            return Some(reason);
        }

        if !self.rotation_is_off() {
            return None;
        }

        let name = self.selected_summary()?.name.clone();

        Some(dbflux_i18n::t!(
            "document.secret_store.rotate.refused_off",
            name = name.as_str()
        ))
    }

    /// Whether Rotate now may run: the rotation actions are available and the
    /// secret's rotation is not known to be off.
    pub fn can_rotate_now(&self) -> bool {
        self.can_start_rotation_action() && self.rotate_now_block_reason().is_none()
    }

    /// The rotation notice of the selected secret, if its last rotation edit
    /// left one.
    pub(in crate::secret_store) fn rotation_notice_for_selection(&self) -> Option<&RotationNotice> {
        let notice = self.rotation_notice.as_ref()?;

        (self.selected == Some(SecretRowKey::Secret(notice.name.clone()))).then_some(notice)
    }

    // -- Rotate now -----------------------------------------------------------

    /// Starts a rotation of the selected secret now. Refused, with the
    /// reason and without calling the store, when its rotation is off.
    pub fn rotate_selected_now(&mut self, cx: &mut Context<Self>) {
        if !self.can_start_rotation_action() {
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        if self.rotation_is_off() {
            self.report_write_warning(
                dbflux_i18n::t!(
                    "document.secret_store.rotate.refused_off",
                    name = name.as_str()
                ),
                cx,
            );
            return;
        }

        let Some(session) = self.write_session(cx) else {
            return;
        };

        let request_name = name.clone();

        self.run_governed_write(
            session,
            move |context| Landing::of(rotate_secret_now(context, &request_name)),
            move |doc, landing, cx| doc.land_rotate_now(&name, landing, cx),
            cx,
        );
    }

    fn land_rotate_now(
        &mut self,
        name: &str,
        landing: Landing<SecretRotationStart>,
        cx: &mut Context<Self>,
    ) {
        match landing {
            Landing::Done(start) => {
                self.rotation_notice = None;

                let message = match start.version {
                    Some(version) => dbflux_i18n::t!(
                        "document.secret_store.rotate.done_version",
                        name = name,
                        version = version
                    ),
                    None => dbflux_i18n::t!("document.secret_store.rotate.done", name = name),
                };

                self.toast_written(message, cx);
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Stands(failure) => {
                self.report_write_failure(&failure, cx);
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    // -- Edit rotation ----------------------------------------------------------

    /// Opens the Edit rotation dialog on the selected secret's current
    /// settings. Nothing is written until it is confirmed.
    pub fn open_edit_rotation(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_start_rotation_action() {
            return;
        }

        let Some(name) = self.selected_summary().map(|summary| summary.name.clone()) else {
            return;
        };

        if self.block_unsaved_value_edits(cx) {
            return;
        }

        let current = self.selected_rotation().cloned();
        let was_enabled = current.as_ref().is_some_and(|rotation| rotation.enabled);
        let field = |window: &mut Window,
                     cx: &mut Context<Self>,
                     placeholder: String,
                     value: Option<&str>| {
            cx.new(|cx| {
                let mut state = InputState::new(window, cx).placeholder(placeholder);

                if let Some(value) = value {
                    state.set_value(value, window, cx);
                }

                state
            })
        };

        let schedule = field(
            window,
            cx,
            dbflux_i18n::t!("document.secret_store.rotation_edit.schedule_placeholder"),
            current
                .as_ref()
                .and_then(|rotation| rotation.schedule.as_deref()),
        );
        let window_input = field(
            window,
            cx,
            dbflux_i18n::t!("document.secret_store.rotation_edit.window_placeholder"),
            current
                .as_ref()
                .and_then(|rotation| rotation.window.as_deref()),
        );
        let function = field(
            window,
            cx,
            dbflux_i18n::t!("document.secret_store.rotation_edit.function_placeholder"),
            current
                .as_ref()
                .and_then(|rotation| rotation.function.as_deref()),
        );

        // Every keystroke feeds the validation shown under the fields.
        let subscriptions = [&schedule, &window_input, &function]
            .into_iter()
            .map(|input| {
                cx.subscribe(input, |this, _, event: &InputEvent, cx| {
                    if matches!(event, InputEvent::Change) {
                        if let Some(WriteDialog::Rotation(dialog)) = &mut this.dialog {
                            dialog.touched = true;
                        }
                        cx.notify();
                    }
                })
            })
            .collect();

        let focus = schedule.read(cx).focus_handle(cx);

        self.open_dialog(
            WriteDialog::Rotation(RotationDialog {
                name,
                enabled: was_enabled,
                was_enabled,
                schedule,
                window: window_input,
                function,
                touched: false,
                _subscriptions: subscriptions,
            }),
            Some(focus),
            window,
            cx,
        );
    }

    /// Turns the dialog's rotation on or off.
    pub fn set_rotation_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        if let Some(WriteDialog::Rotation(dialog)) = &mut self.dialog {
            dialog.enabled = enabled;
            dialog.touched = true;
            cx.notify();
        }
    }

    /// Writes the rotation edit the dialog confirmed, through the seam.
    pub(in crate::secret_store) fn submit_rotation(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(WriteDialog::Rotation(dialog)) = &self.dialog else {
            return;
        };

        // A request the seam would refuse is not sent: the dialog already
        // shows why.
        if dialog.problem(cx).is_some() {
            return;
        }

        let update = dialog.request(cx);
        let name = dialog.name.clone();

        let Some(session) = self.write_session(cx) else {
            return;
        };

        self.run_governed_write(
            session,
            move |context| Landing::of(update_secret_rotation(context, &update)),
            move |doc, landing, cx| doc.land_rotation_edit(&name, landing, cx),
            cx,
        );
    }

    fn land_rotation_edit(
        &mut self,
        name: &str,
        landing: Landing<SecretRotationChange>,
        cx: &mut Context<Self>,
    ) {
        match landing {
            Landing::Done(change) => {
                self.dismiss_dialog(cx);
                self.toast_written(
                    dbflux_i18n::t!("document.secret_store.rotation_edit.done", name = name),
                    cx,
                );
                self.note_unfinished_rotation(name, change.unfinished_version, cx);
                self.refresh_after_write(name, WriteEffect::Metadata, cx);
            }
            Landing::Stands(failure) => {
                self.dismiss_dialog(cx);
                self.report_write_failure(&failure, cx);
                self.refresh_after_write(name, WriteEffect::Metadata, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    /// Says a cancelled rotation left a version holding the pending stage:
    /// a warning toast, and a notice in the Rotation section for as long as
    /// the secret stays selected.
    fn note_unfinished_rotation(
        &mut self,
        name: &str,
        version: Option<String>,
        cx: &mut Context<Self>,
    ) {
        let Some(version) = version else {
            self.rotation_notice = None;
            return;
        };

        self.report_write_warning(
            dbflux_i18n::t!(
                "document.secret_store.rotation_edit.unfinished",
                version = version.as_str()
            ),
            cx,
        );
        self.rotation_notice = Some(RotationNotice {
            name: name.to_string(),
            version,
        });
    }

    // -- Rendering ----------------------------------------------------------------

    pub(in crate::secret_store) fn render_rotation_dialog(
        &self,
        dialog: &RotationDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let problem = dialog.problem(cx);
        let schedule_empty = trimmed(&dialog.schedule, cx).is_none();

        // A form nobody has touched is not told off for the fields it has not
        // filled in yet.
        let shown_problem = problem
            .clone()
            .filter(|_| dialog.touched || !dialog.enabled || !schedule_empty);

        let field = |label_key: &str,
                     hint_key: &str,
                     id: &'static str,
                     input: &Entity<InputState>,
                     cx: &App| {
            modal_field(
                dbflux_i18n::t!(label_key),
                div()
                    .flex()
                    .flex_col()
                    .gap(ModalMetrics::HEADER_GAP)
                    .child(
                        Input::new(input)
                            .id(id)
                            .aria_label(dbflux_i18n::t!(label_key))
                            .disabled(!dialog.enabled)
                            .w_full(),
                    )
                    .child(Text::caption(dbflux_i18n::t!(hint_key)).muted_foreground()),
                cx,
            )
        };

        let mut body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(
                Checkbox::new("secret-rotation-enabled")
                    .label(dbflux_i18n::t!(
                        "document.secret_store.rotation_edit.enabled"
                    ))
                    .checked(dialog.enabled)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.set_rotation_enabled(*checked, cx);
                    })),
            )
            .child(field(
                "document.secret_store.rotation_edit.schedule",
                "document.secret_store.rotation_edit.schedule_hint",
                "secret-rotation-schedule-input",
                &dialog.schedule,
                cx,
            ))
            .child(field(
                "document.secret_store.rotation_edit.window",
                "document.secret_store.rotation_edit.window_hint",
                "secret-rotation-window-input",
                &dialog.window,
                cx,
            ))
            .child(field(
                "document.secret_store.rotation_edit.function",
                "document.secret_store.rotation_edit.function_hint",
                "secret-rotation-function-input",
                &dialog.function,
                cx,
            ));

        if let Some(message) = shown_problem {
            body = body.child(
                div()
                    .id("secret-rotation-problem")
                    .child(Text::caption(message).danger()),
            );
        }

        let footer = dialog_footer(
            FooterSpec {
                cancel_id: "secret-rotation-cancel",
                confirm_id: "secret-rotation-confirm",
                confirm_label: dbflux_i18n::t!("document.secret_store.rotation_edit.confirm"),
                confirm_icon: AppIcon::Check,
                danger: false,
                enabled: problem.is_none(),
                submitting: self.write_in_flight,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-rotation-dialog",
                title: dbflux_i18n::t!(
                    "document.secret_store.rotation_edit.title",
                    name = dialog.name.as_str()
                ),
                icon: AppIcon::RefreshCcw,
                danger: false,
                confirm_enabled: problem.is_none() && !self.write_in_flight,
            },
            body,
            footer,
            self.dialog_focus.handle(),
            cx,
        )
    }
}

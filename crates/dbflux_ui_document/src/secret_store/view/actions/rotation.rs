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
use crate::secret_store::DetailsState;
use crate::secret_store::SecretOperation;
use crate::secret_store::changes::SecretChangeKind;
use crate::secret_store::view::{SecretView, SecretViewEvent};
use crate::secret_store::write::{rotate_secret_now, update_secret_rotation};
use dbflux_components::controls::{Checkbox, Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::parts::{modal_field, modal_lead};
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

/// Seconds between the follow-up reads of a started rotation (cumulative
/// 3, 10 and 30).
const ROTATION_FOLLOW_UP_SECONDS: [u64; 3] = [3, 7, 20];

/// The confirmation of one Rotate now.
pub(in crate::secret_store) struct RotateNowDialog {
    pub name: String,
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

impl SecretView {
    /// Whether the store offers rotation actions at all.
    pub(crate) fn supports_rotation(&self) -> bool {
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
    pub(crate) fn can_start_rotation_action(&self) -> bool {
        self.supports_rotation()
            && self.connection_present
            && !self.write_in_flight
            && !self.dialog_is_open()
            && self.write_refusal_reason().is_none()
            && self.name.is_some()
    }

    /// Whether Edit rotation may open: the details are loaded, so the dialog
    /// starts from the rotation the store reports and not from a guess.
    pub(crate) fn can_edit_rotation(&self) -> bool {
        self.can_start_rotation_action() && matches!(self.details, DetailsState::Loaded(_))
    }

    /// Why Rotate now cannot run for the selection, when something says so:
    /// the secret's own state, or its rotation being off. The seam refuses a
    /// rotation of a secret that does not rotate too; this keeps the button
    /// from offering what it knows will be refused.
    pub(crate) fn rotate_now_block_reason(&self) -> Option<String> {
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
    pub(crate) fn can_rotate_now(&self) -> bool {
        self.can_start_rotation_action() && self.rotate_now_block_reason().is_none()
    }

    /// The rotation notice of the selected secret, if its last rotation edit
    /// left one.
    pub(in crate::secret_store) fn rotation_notice_for_selection(&self) -> Option<&RotationNotice> {
        let notice = self.rotation_notice.as_ref()?;

        self.is_for(&notice.name).then_some(notice)
    }

    // -- Rotate now -----------------------------------------------------------

    /// Starts a rotation of the selected secret now. Refused, with the
    /// reason and without calling the store, when its rotation is off.
    pub(crate) fn rotate_selected_now(&mut self, window: &mut Window, cx: &mut Context<Self>) {
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

        self.open_dialog(
            WriteDialog::RotateNow(RotateNowDialog { name }),
            None,
            window,
            cx,
        );
    }

    /// Starts the rotation the dialog confirmed, through the seam.
    pub(in crate::secret_store) fn submit_rotate_now(
        &mut self,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(WriteDialog::RotateNow(dialog)) = &self.dialog else {
            return;
        };

        let name = dialog.name.clone();

        let Some(session) = self.write_session(cx) else {
            return;
        };

        let request_name = name.clone();

        self.run_governed_write(
            session,
            SecretOperation::RotateNow,
            name.clone(),
            move |context| Landing::of(rotate_secret_now(context, &request_name)),
            move |doc, landing, cx| doc.land_rotate_now(&name, landing, cx),
            cx,
        );
    }

    pub(in crate::secret_store) fn land_rotate_now(
        &mut self,
        name: &str,
        landing: Landing<SecretRotationStart>,
        cx: &mut Context<Self>,
    ) {
        match landing {
            Landing::Done(start) => {
                self.dismiss_dialog(cx);
                self.rotation_notice = None;
                self.follow_up_rotation(name, cx);

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
                self.dismiss_dialog(cx);
                self.report_write_failure(&failure, cx);
                // The rotation started, or may have: it can finish after the
                // refresh whether or not its audit event was recorded.
                self.follow_up_rotation(name, cx);
                self.refresh_after_write(name, WriteEffect::Versions, cx);
            }
            Landing::Rejected(failure) => self.report_write_failure(&failure, cx),
        }
    }

    /// The rotation runs on the service and may finish after the refresh that
    /// follows the request, so the selected secret is read again a few times
    /// while it stays selected on the same connection.
    fn follow_up_rotation(&mut self, name: &str, cx: &mut Context<Self>) {
        let entity = cx.entity().clone();
        let name = name.to_string();
        let session = self.connection(cx);

        // A newer rotation of the same secret takes over: the older schedule
        // stops at its next tick, and the latest one still ends with its own
        // final read. Another secret's rotation leaves it alone.
        self.rotation_follow_up_counter = self.rotation_follow_up_counter.wrapping_add(1);
        let generation = self.rotation_follow_up_counter;
        let key = name.clone();
        self.rotation_follow_up_generations
            .insert(key.clone(), generation);

        cx.spawn(async move |_this, cx| {
            for seconds in ROTATION_FOLLOW_UP_SECONDS {
                cx.background_executor()
                    .timer(std::time::Duration::from_secs(seconds))
                    .await;

                let keep_going = cx.update(|cx| {
                    entity.update(cx, |doc, cx| {
                        doc.rotation_follow_up_generations.get(&key) == Some(&generation)
                            && doc.refresh_rotation_follow_up(&name, &session, cx)
                    })
                });

                if !keep_going {
                    break;
                }
            }

            // Only the schedule that still owns the entry forgets it; a
            // newer rotation of the same secret keeps its own.
            cx.update(|cx| {
                entity.update(cx, |view, _| {
                    if view.rotation_follow_up_generations.get(&key) == Some(&generation) {
                        view.rotation_follow_up_generations.remove(&key);
                    }
                })
            });
        })
        .detach();
    }

    /// One follow-up read of a rotating secret. Returns whether more should
    /// follow: not once the user moved on or the connection changed.
    fn refresh_rotation_follow_up(
        &mut self,
        name: &str,
        session: &Option<std::sync::Arc<dyn dbflux_core::Connection>>,
        cx: &mut Context<Self>,
    ) -> bool {
        let same_session = match (session, self.connection(cx)) {
            (Some(started), Some(live)) => std::sync::Arc::ptr_eq(started, &live),
            _ => false,
        };

        if !same_session || !self.is_for(name) {
            return false;
        }

        if !self.write_in_flight {
            self.describe(name.to_string(), cx);
            self.request_versions(
                name.to_string(),
                crate::secret_store::HistoryLoad::Automatic,
                cx,
            );
            self.announce_change(name, SecretChangeKind::Changed, cx);
            cx.emit(SecretViewEvent::ReloadListing);
        }

        true
    }

    pub(in crate::secret_store) fn render_rotate_now_dialog(
        &self,
        dialog: &RotateNowDialog,
        cx: &mut Context<Self>,
    ) -> AnyElement {
        let body = div()
            .flex()
            .flex_col()
            .gap(ModalMetrics::BODY_GAP)
            .child(div().id("secret-rotate-lead").child(modal_lead(
                dbflux_i18n::t!(
                    "document.secret_store.rotate.lead",
                    name = dialog.name.as_str()
                ),
                cx,
            )))
            .child(
                div().id("secret-rotate-effect").child(
                    Text::caption(dbflux_i18n::t!("document.secret_store.rotate.effect"))
                        .muted_foreground(),
                ),
            );

        let footer = dialog_footer(
            FooterSpec {
                cancel_id: "secret-rotate-cancel",
                confirm_id: "secret-rotate-confirm",
                confirm_label: dbflux_i18n::t!("document.secret_store.rotate.confirm"),
                confirm_icon: AppIcon::Zap,
                danger: false,
                enabled: true,
                submitting: self.write_in_flight,
            },
            cx,
        );

        dialog_modal(
            DialogShell {
                id: "secret-rotate-dialog",
                title: dbflux_i18n::t!(
                    "document.secret_store.rotate.title",
                    name = dialog.name.as_str()
                ),
                icon: AppIcon::Zap,
                danger: false,
                confirm_enabled: !self.write_in_flight,
            },
            body,
            footer,
            self.dialog_focus.handle(),
            cx,
        )
    }

    // -- Edit rotation ----------------------------------------------------------

    /// Opens the Edit rotation dialog on the selected secret's current
    /// settings. Nothing is written until it is confirmed.
    pub(crate) fn open_edit_rotation(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.can_edit_rotation() {
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
    pub(crate) fn set_rotation_enabled(&mut self, enabled: bool, cx: &mut Context<Self>) {
        // The request carries the form as it was when Save was pressed.
        if self.write_in_flight {
            return;
        }

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
            SecretOperation::EditRotation,
            name.clone(),
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

        let body = self.render_rotation_body(dialog, shown_problem, cx);

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

    /// The enabled switch, the three fields and the problem line.
    fn render_rotation_body(
        &self,
        dialog: &RotationDialog,
        shown_problem: Option<String>,
        cx: &mut Context<Self>,
    ) -> Div {
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
                    .disabled(self.write_in_flight)
                    .on_click(cx.listener(|this, checked: &bool, _, cx| {
                        this.set_rotation_enabled(*checked, cx);
                    })),
            )
            .child(rotation_field(
                dialog.enabled && !self.write_in_flight,
                "document.secret_store.rotation_edit.schedule",
                "document.secret_store.rotation_edit.schedule_hint",
                "secret-rotation-schedule-input",
                &dialog.schedule,
                cx,
            ))
            .child(rotation_field(
                dialog.enabled && !self.write_in_flight,
                "document.secret_store.rotation_edit.window",
                "document.secret_store.rotation_edit.window_hint",
                "secret-rotation-window-input",
                &dialog.window,
                cx,
            ))
            .child(rotation_field(
                dialog.enabled && !self.write_in_flight,
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

        body
    }
}

/// One labelled field of the Edit rotation dialog with its hint under it.
fn rotation_field(
    enabled: bool,
    label_key: &str,
    hint_key: &str,
    id: &'static str,
    input: &Entity<InputState>,
    cx: &App,
) -> impl IntoElement {
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
                    .disabled(!enabled)
                    .w_full(),
            )
            .child(Text::caption(dbflux_i18n::t!(hint_key)).muted_foreground()),
        cx,
    )
}

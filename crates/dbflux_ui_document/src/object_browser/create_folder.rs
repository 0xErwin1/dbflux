//! Folder creation: consumes the toolbar's "New folder" intent
//! (`request_new_folder` / `take_pending_new_folder`) into a small name-input
//! overlay, then creates a zero-byte object at
//! `current_prefix + name + "/"` — S3 has no real directories; a zero-byte
//! key ending in `/` with no content-type is the client convention every S3
//! console uses to represent one.
//!
//! Uses the same small ad-hoc overlay shape as the single-object delete
//! confirmation (`delete.rs`) rather than the heavier `ModalFrame` the New
//! Bucket modal uses — this is a one-field prompt, not a multi-option form.

use super::ObjectBrowserDocument;
use super::data::db_error_to_user_facing;
use dbflux_components::controls::{Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text, overlay_bg, surface_panel};
use dbflux_components::tokens::{Heights, Radii, Spacing};
use dbflux_core::DbError;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::prelude::*;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

/// Folder-name validation: non-empty, no leading/trailing slash, no
/// consecutive slashes — the folder is created directly under the current
/// prefix, so any nesting must be typed as separate creates.
pub fn folder_name_error(name: &str) -> Option<String> {
    if name.is_empty() {
        return Some("Folder name cannot be empty.".to_string());
    }

    if name.starts_with('/') || name.ends_with('/') {
        return Some("Folder name cannot start or end with a slash.".to_string());
    }

    if name.contains("//") {
        return Some("Folder name cannot contain consecutive slashes.".to_string());
    }

    None
}

/// Everything the New Folder overlay edits. Built on the render pass that
/// consumes the toolbar's intent, because the input needs a `Window`.
pub struct NewFolderState {
    pub name_input: Entity<InputState>,
    pub submitting: bool,
    pub error: Option<String>,
    _subscription: Subscription,
}

impl ObjectBrowserDocument {
    pub fn new_folder(&self) -> Option<&NewFolderState> {
        self.new_folder.as_ref()
    }

    /// Consumes the toolbar's "New folder" intent on the next render pass and
    /// builds the overlay's input.
    pub(super) fn drain_pending_new_folder(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.take_pending_new_folder() {
            return;
        }

        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("folder-name"));

        let subscription = cx.subscribe(&name_input, |_this, _input, event: &InputEvent, cx| {
            if matches!(event, InputEvent::Change) {
                cx.notify();
            }
        });

        self.new_folder = Some(NewFolderState {
            name_input,
            submitting: false,
            error: None,
            _subscription: subscription,
        });

        cx.notify();
    }

    pub(super) fn close_new_folder(&mut self, cx: &mut Context<Self>) {
        self.new_folder = None;
        cx.notify();
    }

    fn new_folder_name(&self, cx: &Context<Self>) -> String {
        self.new_folder
            .as_ref()
            .map(|state| state.name_input.read(cx).value().trim().to_string())
            .unwrap_or_default()
    }

    /// The Create button is live only for a valid name and no submission
    /// already in flight.
    fn can_create_folder(&self, cx: &Context<Self>) -> bool {
        let Some(state) = self.new_folder.as_ref() else {
            return false;
        };

        !state.submitting && folder_name_error(&self.new_folder_name(cx)).is_none()
    }

    pub(super) fn submit_new_folder(&mut self, cx: &mut Context<Self>) {
        if !self.can_create_folder(cx) {
            return;
        }

        let name = self.new_folder_name(cx);
        let key = format!("{}{name}/", self.tree.current_prefix);

        let Some(state) = self.new_folder.as_mut() else {
            return;
        };
        state.submitting = true;
        state.error = None;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            report_error(
                UserFacingError::new(ErrorKind::Storage, "Connection is no longer active"),
                cx,
            );
            self.apply_folder_created(key, Err("Connection is no longer active".to_string()), cx);
            return;
        };

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let bucket = self.bucket.clone();
        let profile_id = self.profile_id;
        let entity = cx.entity().clone();
        let key_for_task = key.clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let bucket = bucket.clone();
                    let key = key_for_task.clone();
                    async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported("Object-store API unavailable".to_string())
                        })?;
                        api.put_object(&bucket, &key, Vec::new(), None)
                    }
                })
                .await;

            record_folder_create_audit(
                &audit_service,
                profile_id,
                &bucket,
                &key_for_task,
                result.as_ref().err().map(ToString::to_string).as_deref(),
            );

            if let Err(err) = &result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            let outcome = result.map_err(|err| err.to_string());
            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_folder_created(key_for_task, outcome, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    /// Closes the overlay and refreshes the level on success, keeping it open
    /// with the inline error on failure so the user can retry.
    fn apply_folder_created(
        &mut self,
        key: String,
        result: Result<(), String>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(()) => {
                self.new_folder = None;
                Toast::success(format!("Created folder s3://{}/{key}", self.bucket))
                    .meta_right(now_hms())
                    .push(cx);
                self.reload_current_prefix(cx);
            }
            Err(message) => {
                if let Some(state) = self.new_folder.as_mut() {
                    state.submitting = false;
                    state.error = Some(message);
                }
                cx.notify();
            }
        }
    }

    /// Small name-input overlay, or nothing when it is closed.
    pub(super) fn render_new_folder_overlay(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let Some(state) = self.new_folder.as_ref() else {
            return div().into_any_element();
        };

        let name = self.new_folder_name(cx);
        let name_error = (!name.is_empty())
            .then(|| folder_name_error(&name))
            .flatten();
        let can_create = self.can_create_folder(cx);

        let mut body = div()
            .flex()
            .flex_col()
            .gap(Spacing::MD)
            .p(Spacing::MD)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap(Spacing::SM)
                    .child(Icon::new(AppIcon::Folder).size(Heights::ICON_MD).muted())
                    .child(Text::heading("New folder")),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Input::new(&state.name_input).small().w_full())
                    .child(match &name_error {
                        Some(error) => Text::caption(error.clone()).danger(),
                        None => Text::caption("Created as a zero-byte key ending in \"/\"")
                            .muted_foreground(),
                    }),
            );

        if let Some(error) = state.error.as_ref() {
            body = body.child(Text::caption(error.clone()).danger());
        }

        body = body.child(
            div()
                .flex()
                .justify_end()
                .gap(Spacing::SM)
                .child(
                    div()
                        .id("object-browser-new-folder-cancel")
                        .flex()
                        .items_center()
                        .h(Heights::CONTROL)
                        .px(Spacing::SM)
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .bg(theme.secondary)
                        .hover(|d| d.bg(theme.muted))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.close_new_folder(cx);
                        }))
                        .child(Text::caption("Cancel")),
                )
                .child(
                    div()
                        .id("object-browser-new-folder-create")
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .h(Heights::CONTROL)
                        .px(Spacing::SM)
                        .rounded(Radii::SM)
                        .bg(theme.primary)
                        .when(!can_create, |d| d.opacity(0.5))
                        .when(can_create, |d| d.cursor_pointer().hover(|d| d.opacity(0.9)))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.submit_new_folder(cx);
                        }))
                        .child(
                            Icon::new(if state.submitting {
                                AppIcon::Loader
                            } else {
                                AppIcon::Plus
                            })
                            .size(Heights::ICON_SM)
                            .color(theme.primary_foreground),
                        )
                        .child(
                            Text::caption(if state.submitting {
                                "Creating…"
                            } else {
                                "Create"
                            })
                            .color(theme.primary_foreground),
                        ),
                ),
        );

        div()
            .id("object-browser-new-folder-overlay")
            .absolute()
            .inset_0()
            .bg(overlay_bg(theme))
            .flex()
            .items_center()
            .justify_center()
            .on_mouse_down(MouseButton::Left, |_, _, cx| {
                cx.stop_propagation();
            })
            .child(
                surface_panel(cx)
                    .rounded(Radii::MD)
                    .min_w(px(360.0))
                    .child(body),
            )
            .into_any_element()
    }
}

/// Audits a folder creation. Never records the object body — folders are
/// always empty by definition.
fn record_folder_create_audit(
    audit_service: &dbflux_audit::AuditService,
    profile_id: Uuid,
    bucket: &str,
    key: &str,
    error: Option<&str>,
) {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{
        EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    };

    let (severity, outcome, action) = match error {
        Some(_) => (
            EventSeverity::Error,
            EventOutcome::Failure,
            "folder_create_failed",
        ),
        None => (EventSeverity::Info, EventOutcome::Success, "folder_create"),
    };

    let mut summary = format!("Created folder s3://{bucket}/{key}");
    if let Some(error) = error {
        summary.push_str(&format!(": {error}"));
    }

    let event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::ObjectStorage,
        outcome,
    )
    .with_action(action.to_string())
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref("object", format!("{bucket}/{key}"))
    .with_connection_context(profile_id.to_string(), bucket.to_string(), String::new());

    if let Err(e) = audit_service.record(event) {
        log::warn!("[object browser] failed to record folder-create audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::folder_name_error;

    /// T38: name validation — non-empty, no leading/trailing slash, no
    /// consecutive slashes.
    #[test]
    fn folder_name_validation_rejects_empty_and_slash_violations() {
        assert_eq!(folder_name_error("logs"), None);
        assert_eq!(folder_name_error("2026-archive"), None);

        assert!(folder_name_error("").is_some_and(|error| error.contains("empty")));
        assert!(folder_name_error("/logs").is_some_and(|error| error.contains("slash")));
        assert!(folder_name_error("logs/").is_some_and(|error| error.contains("slash")));
        assert!(folder_name_error("logs//2026").is_some_and(|error| error.contains("slash")));
    }
}

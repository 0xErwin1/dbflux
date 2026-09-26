//! Folder creation: consumes the toolbar's "New folder" intent
//! (`request_new_folder` / `take_pending_new_folder`) into a small name-input
//! overlay, then creates a zero-byte object at
//! `current_prefix + name + "/"` — S3 has no real directories; a zero-byte
//! key ending in `/` with no content-type is the client convention every S3
//! console uses to represent one.
//!
//! Renders in the shared `Modal`, like the single-object delete confirmation
//! (`delete.rs`).

use super::ObjectBrowserDocument;
use super::data::db_error_to_user_facing;
use dbflux_components::controls::Button;
use dbflux_components::controls::{Input, InputEvent, InputState};
use dbflux_components::icons::AppIcon;
use dbflux_components::modals::Modal;
use dbflux_components::primitives::Text;
use dbflux_components::tokens::Spacing;
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
        return Some(dbflux_i18n::t!(
            "document.object_browser.create_folder.error.empty"
        ));
    }

    if name.starts_with('/') || name.ends_with('/') {
        return Some(dbflux_i18n::t!(
            "document.object_browser.create_folder.error.leading_trailing_slash"
        ));
    }

    if name.contains("//") {
        return Some(dbflux_i18n::t!(
            "document.object_browser.create_folder.error.consecutive_slashes"
        ));
    }

    None
}

/// Everything the New Folder overlay edits. Built on the render pass that
/// consumes the toolbar's intent, because the input needs a `Window`.
pub struct NewFolderState {
    pub name_input: Entity<InputState>,
    /// Prefix the folder is created under. The toolbar's intent targets the
    /// level being listed; the listing's context menu targets the folder that
    /// was right-clicked, which is not necessarily that level.
    pub parent: String,
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

        let name_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.object_browser.create_folder.name_placeholder"
            ))
        });

        let subscription =
            cx.subscribe(
                &name_input,
                |this, _input, event: &InputEvent, cx| match event {
                    InputEvent::Change => cx.notify(),
                    InputEvent::PressEnter {
                        secondary: false, ..
                    } => this.submit_new_folder(cx),
                    _ => {}
                },
            );

        let parent = self
            .take_pending_new_folder_parent()
            .unwrap_or_else(|| self.tree.current_prefix.clone());

        self.new_folder = Some(NewFolderState {
            name_input,
            parent,
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

        let Some(state) = self.new_folder.as_mut() else {
            return;
        };
        let key = format!("{}{name}/", state.parent);
        state.submitting = true;
        state.error = None;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            let message = dbflux_i18n::t!("document.object_browser.error.connection_unavailable");
            report_error(
                UserFacingError::new(ErrorKind::Storage, message.clone()),
                cx,
            );
            self.apply_folder_created(key, Err(message), cx);
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
                            DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))
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
            });
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
                // The level to refresh is the one the folder was created
                // under, which the context menu can point somewhere other
                // than the level being listed.
                let parent = self
                    .new_folder
                    .as_ref()
                    .map(|state| state.parent.clone())
                    .unwrap_or_else(|| self.tree.current_prefix.clone());

                self.new_folder = None;
                Toast::success(dbflux_i18n::t!(
                    "document.object_browser.create_folder.created_toast",
                    uri = format!("s3://{}/{key}", self.bucket)
                ))
                .meta_right(now_hms())
                .push(cx);
                self.reload_prefix(parent, cx);
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
            .child(
                Text::caption(dbflux_i18n::t!(
                    "document.object_browser.create_folder.location",
                    uri = format!("s3://{}/{}", self.bucket, state.parent)
                ))
                .muted_foreground(),
            )
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Input::new(&state.name_input).small().w_full())
                    .child(match &name_error {
                        Some(error) => Text::caption(error.clone()).danger(),
                        None => Text::caption(dbflux_i18n::t!(
                            "document.object_browser.create_folder.hint"
                        ))
                        .muted_foreground(),
                    }),
            );

        if let Some(error) = state.error.as_ref() {
            body = body.child(Text::caption(error.clone()).danger());
        }

        let confirm_key = if state.submitting {
            "document.object_browser.create_folder.confirm_in_progress"
        } else {
            "document.object_browser.create_folder.confirm"
        };

        let footer = div()
            .flex()
            .gap(Spacing::SM)
            .child(
                Button::new(
                    "object-browser-new-folder-cancel",
                    dbflux_i18n::t!("document.object_browser.create_folder.cancel"),
                )
                .on_click(cx.listener(|this, _, _, cx| {
                    this.close_new_folder(cx);
                })),
            )
            .child(
                Button::new(
                    "object-browser-new-folder-create",
                    dbflux_i18n::t!(confirm_key),
                )
                .primary()
                .icon(if state.submitting {
                    AppIcon::Loader
                } else {
                    AppIcon::Plus
                })
                .disabled(!can_create)
                .on_click(cx.listener(|this, _, _, cx| {
                    this.submit_new_folder(cx);
                })),
            );

        Modal::new(dbflux_i18n::t!(
            "document.object_browser.create_folder.title"
        ))
        .id("object-browser-new-folder-overlay")
        .icon(AppIcon::Folder)
        .width(px(420.0))
        .body(body)
        .footer(footer)
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

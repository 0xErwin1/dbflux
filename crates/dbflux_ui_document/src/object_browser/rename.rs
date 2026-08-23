//! Object rename: `copy_object` then `delete_object` under the hood (DEC-13
//! — S3 has no atomic rename primitive). Triggered by `Command::Rename` on a
//! selected object row (bound to `r` with no modifier in the results keymap
//! layer, the same convention the sidebar's Rename actions already use —
//! deliberately never F2, per DEC-24).
//!
//! Renaming an object whose editor is open and dirty routes through the
//! unsaved-edits guard first, exactly like delete does.

use super::ObjectBrowserDocument;
use super::data::db_error_to_user_facing;
use super::editor::GuardedNavigation;
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

/// Everything up to and including the last `/` in `key` — the prefix the
/// object lives directly under. Empty for a bucket-root key.
pub(super) fn key_parent_prefix(key: &str) -> String {
    match key.rfind('/') {
        Some(index) => key[..=index].to_string(),
        None => String::new(),
    }
}

/// The part of `key` after its last `/` — the editable leaf name.
pub(super) fn key_leaf(key: &str) -> String {
    match key.rfind('/') {
        Some(index) => key[index + 1..].to_string(),
        None => key.to_string(),
    }
}

/// New-name validation: non-empty, no slash (rename stays within the same
/// prefix — moving to another prefix is not this flow), and different from
/// the current leaf name.
pub fn rename_name_error(current_leaf: &str, new_name: &str) -> Option<String> {
    if new_name.is_empty() {
        return Some(dbflux_i18n::t!(
            "document.object_browser.rename.error.empty"
        ));
    }

    if new_name.contains('/') {
        return Some(dbflux_i18n::t!(
            "document.object_browser.rename.error.contains_slash"
        ));
    }

    if new_name == current_leaf {
        return Some(dbflux_i18n::t!(
            "document.object_browser.rename.error.unchanged"
        ));
    }

    None
}

/// Everything the rename overlay edits. Built on the render pass that
/// consumes the request, because the input needs a `Window`.
pub struct RenameObjectState {
    pub key: String,
    pub name_input: Entity<InputState>,
    pub submitting: bool,
    pub error: Option<String>,
    _subscription: Subscription,
}

impl ObjectBrowserDocument {
    pub fn rename_object(&self) -> Option<&RenameObjectState> {
        self.rename_object.as_ref()
    }

    /// `Command::Rename` on a selected object row. Routes through the
    /// unsaved-edits guard first when the object has a dirty editor open.
    pub(super) fn request_rename_object(
        &mut self,
        key: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        if self.guard_navigation(GuardedNavigation::RenameObject(key.clone()), cx) {
            return;
        }

        self.open_rename_confirm_now(key, window, cx);
    }

    pub(super) fn open_rename_confirm_now(
        &mut self,
        key: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let leaf = key_leaf(&key);

        let name_input = cx.new(|cx| {
            let mut state = InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.object_browser.rename.name_placeholder"
            ));
            state.set_value(&leaf, window, cx);
            state
        });

        let subscription =
            cx.subscribe(
                &name_input,
                |this, _input, event: &InputEvent, cx| match event {
                    InputEvent::Change => cx.notify(),
                    InputEvent::PressEnter { secondary: false } => this.submit_rename_object(cx),
                    _ => {}
                },
            );

        self.rename_object = Some(RenameObjectState {
            key,
            name_input,
            submitting: false,
            error: None,
            _subscription: subscription,
        });

        cx.notify();
    }

    pub(super) fn close_rename_object(&mut self, cx: &mut Context<Self>) {
        self.rename_object = None;
        cx.notify();
    }

    fn rename_new_name(&self, cx: &Context<Self>) -> String {
        self.rename_object
            .as_ref()
            .map(|state| state.name_input.read(cx).value().trim().to_string())
            .unwrap_or_default()
    }

    /// The Rename button is live only for a name that satisfies the naming
    /// rule and no submission already in flight.
    fn can_rename_object(&self, cx: &Context<Self>) -> bool {
        let Some(state) = self.rename_object.as_ref() else {
            return false;
        };

        let current_leaf = key_leaf(&state.key);
        !state.submitting && rename_name_error(&current_leaf, &self.rename_new_name(cx)).is_none()
    }

    pub(super) fn submit_rename_object(&mut self, cx: &mut Context<Self>) {
        if !self.can_rename_object(cx) {
            return;
        }

        let Some(state) = self.rename_object.as_ref() else {
            return;
        };
        let source_key = state.key.clone();
        let new_name = self.rename_new_name(cx);
        let new_key = format!("{}{new_name}", key_parent_prefix(&source_key));

        let Some(state) = self.rename_object.as_mut() else {
            return;
        };
        state.submitting = true;
        state.error = None;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            let message = dbflux_i18n::t!("document.object_browser.error.connection_unavailable");
            report_error(
                UserFacingError::new(ErrorKind::Storage, message.clone()),
                cx,
            );
            self.apply_rename_outcome(source_key, new_key, Err(message), cx);
            return;
        };

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let bucket = self.bucket.clone();
        let profile_id = self.profile_id;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let copy_result = cx
                .background_executor()
                .spawn({
                    let connection = connection.clone();
                    let bucket = bucket.clone();
                    let source_key = source_key.clone();
                    let new_key = new_key.clone();
                    async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))
                        })?;
                        api.copy_object(&bucket, &source_key, &new_key)
                    }
                })
                .await;

            if let Err(err) = copy_result {
                record_rename_audit(
                    &audit_service,
                    profile_id,
                    &bucket,
                    &source_key,
                    &new_key,
                    Some(&err.to_string()),
                );
                report_error_async(db_error_to_user_facing(&err), cx);

                cx.update(|cx| {
                    entity.update(cx, |doc, cx| {
                        doc.apply_rename_outcome(source_key, new_key, Err(err.to_string()), cx);
                    });
                })
                .ok();
                return;
            }

            // The copy landed; the delete is a separate step with no rollback
            // on failure (DEC-13) — the user retries the delete manually with
            // both keys left in place.
            let delete_result = cx
                .background_executor()
                .spawn({
                    let bucket = bucket.clone();
                    let source_key = source_key.clone();
                    async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))
                        })?;
                        api.delete_object(&bucket, &source_key)
                    }
                })
                .await;

            record_rename_audit(
                &audit_service,
                profile_id,
                &bucket,
                &source_key,
                &new_key,
                delete_result
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .as_deref(),
            );

            if let Err(err) = &delete_result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            let outcome = delete_result.map_err(|err| err.to_string());
            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_rename_outcome(source_key, new_key, outcome, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    /// Closes the overlay and refreshes the level on success (re-pointing the
    /// preview if the renamed object was selected), keeping the overlay open
    /// with the inline error on failure so the user can retry.
    fn apply_rename_outcome(
        &mut self,
        source_key: String,
        new_key: String,
        result: Result<(), String>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(()) => {
                self.rename_object = None;
                Toast::success(dbflux_i18n::t!(
                    "document.object_browser.rename.renamed_toast",
                    uri = format!("s3://{}/{new_key}", self.bucket)
                ))
                .meta_right(now_hms())
                .push(cx);

                if self.preview_key.as_deref() == Some(source_key.as_str()) {
                    self.open_preview_now(new_key.clone(), cx);
                }

                self.reload_current_prefix(cx);
            }
            Err(message) => {
                if let Some(state) = self.rename_object.as_mut() {
                    state.submitting = false;
                    state.error = Some(message);
                }
                cx.notify();
            }
        }
    }

    /// Small name-input overlay, pre-filled with the current leaf name, or
    /// nothing when it is closed.
    pub(super) fn render_rename_overlay(&self, cx: &mut Context<Self>) -> impl IntoElement {
        let theme = cx.theme();

        let Some(state) = self.rename_object.as_ref() else {
            return div().into_any_element();
        };

        let can_rename = self.can_rename_object(cx);
        let current_leaf = key_leaf(&state.key);
        let new_name = self.rename_new_name(cx);
        let name_error = (!new_name.is_empty())
            .then(|| rename_name_error(&current_leaf, &new_name))
            .flatten();

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
                    .child(Icon::new(AppIcon::Pencil).size(Heights::ICON_MD).muted())
                    .child(Text::heading(dbflux_i18n::t!(
                        "document.object_browser.rename.title"
                    ))),
            )
            .child(Text::muted(format!("\"{}\"", state.key)))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .gap(Spacing::XS)
                    .child(Input::new(&state.name_input).small().w_full())
                    .when_some(name_error.clone(), |this, error| {
                        this.child(Text::caption(error).danger())
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
                        .id("object-browser-rename-cancel")
                        .flex()
                        .items_center()
                        .h(Heights::CONTROL)
                        .px(Spacing::SM)
                        .rounded(Radii::SM)
                        .cursor_pointer()
                        .bg(theme.secondary)
                        .hover(|d| d.bg(theme.muted))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.close_rename_object(cx);
                        }))
                        .child(Text::caption(dbflux_i18n::t!(
                            "document.object_browser.rename.cancel"
                        ))),
                )
                .child(
                    div()
                        .id("object-browser-rename-confirm")
                        .flex()
                        .items_center()
                        .gap(Spacing::XS)
                        .h(Heights::CONTROL)
                        .px(Spacing::SM)
                        .rounded(Radii::SM)
                        .bg(theme.primary)
                        .when(!can_rename, |d| d.opacity(0.5))
                        .when(can_rename, |d| d.cursor_pointer().hover(|d| d.opacity(0.9)))
                        .on_click(cx.listener(|this, _, _, cx| {
                            this.submit_rename_object(cx);
                        }))
                        .child(
                            Icon::new(if state.submitting {
                                AppIcon::Loader
                            } else {
                                AppIcon::Pencil
                            })
                            .size(Heights::ICON_SM)
                            .color(theme.primary_foreground),
                        )
                        .child({
                            let key = if state.submitting {
                                "document.object_browser.rename.confirm_in_progress"
                            } else {
                                "document.object_browser.rename.confirm"
                            };
                            Text::caption(dbflux_i18n::t!(key)).color(theme.primary_foreground)
                        }),
                ),
        );

        div()
            .id("object-browser-rename-overlay")
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
                    .min_w(px(400.0))
                    .child(body),
            )
            .into_any_element()
    }
}

/// Audits a rename. Records both keys and the outcome, never the object
/// body.
fn record_rename_audit(
    audit_service: &dbflux_audit::AuditService,
    profile_id: Uuid,
    bucket: &str,
    source_key: &str,
    new_key: &str,
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
            "object_rename_failed",
        ),
        None => (EventSeverity::Info, EventOutcome::Success, "object_rename"),
    };

    let mut summary = format!("Renamed s3://{bucket}/{source_key} to s3://{bucket}/{new_key}");
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
    .with_object_ref("object", format!("{bucket}/{source_key} -> {new_key}"))
    .with_connection_context(profile_id.to_string(), bucket.to_string(), String::new());

    if let Err(e) = audit_service.record(event) {
        log::warn!("[object browser] failed to record object-rename audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::{key_leaf, key_parent_prefix, rename_name_error};

    /// T43: leaf/parent split follows the last `/` in the key.
    #[test]
    fn key_parent_and_leaf_split_on_the_last_slash() {
        assert_eq!(key_parent_prefix("logs/2026/app.log"), "logs/2026/");
        assert_eq!(key_leaf("logs/2026/app.log"), "app.log");

        assert_eq!(key_parent_prefix("app.log"), "");
        assert_eq!(key_leaf("app.log"), "app.log");
    }

    /// T43: name validation — non-empty, no slash, and different from the
    /// current leaf.
    #[test]
    fn rename_name_validation_rejects_empty_slash_and_unchanged_names() {
        assert_eq!(rename_name_error("app.log", "app.v2.log"), None);

        assert!(rename_name_error("app.log", "").is_some_and(|error| error.contains("empty")));
        assert!(
            rename_name_error("app.log", "sub/app.log")
                .is_some_and(|error| error.contains("slash"))
        );
        assert!(
            rename_name_error("app.log", "app.log").is_some_and(|error| error.contains("differ"))
        );
    }
}

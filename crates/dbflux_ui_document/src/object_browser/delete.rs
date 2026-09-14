//! Single-object delete: consumes the preview action bar's `Delete` intent
//! and the `Del` key on a selected object row.
//!
//! Reuses the small confirmation-overlay shape `buckets_table` uses for its
//! (empty-only) bucket delete — key, size, Cancel/Delete. Deleting an object
//! whose editor is open and dirty routes through the unsaved-edits guard
//! first (`GuardedNavigation::DeleteObject`): the user resolves that prompt
//! before the delete confirmation ever appears.

use super::data::db_error_to_user_facing;
use super::editor::GuardedNavigation;
use super::metadata::ObjectMetadataState;
use super::tree::ObjectTreeEntry;
use super::{ObjectAction, ObjectBrowserDocument};
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

/// Object staged for the single-delete confirmation. `size_bytes` is `None`
/// only when neither the metadata panel nor the current listing page has
/// resolved the object's size yet.
#[derive(Clone, Debug, PartialEq)]
pub struct PendingObjectDelete {
    pub key: String,
    pub size_bytes: Option<u64>,
}

impl ObjectBrowserDocument {
    pub fn pending_object_delete(&self) -> Option<&PendingObjectDelete> {
        self.pending_object_delete.as_ref()
    }

    /// Drains `pending_object_action`, raised by the preview action bar, into
    /// the flow that owns it: `Delete` opens the confirmation below, `Presign`
    /// opens the presigned-URL modal.
    /// Also moves window focus back to the document: these modals have no
    /// input of their own, and a focused editor would otherwise keep eating
    /// the keystrokes (Escape included) meant for them.
    pub(super) fn drain_pending_object_action(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let action = self.take_pending_object_action();

        if action.is_some() {
            self.focus_handle.focus(window);
        }

        match action {
            Some(ObjectAction::Delete { key }) => self.request_delete_object(key, cx),
            Some(ObjectAction::Presign { key }) => self.open_presign(key, cx),
            None => {}
        }
    }

    /// `Del` key / context-menu / preview action-bar intent to delete a
    /// single object. Routes through the unsaved-edits guard first when the
    /// object has a dirty editor open.
    pub(super) fn request_delete_object(&mut self, key: String, cx: &mut Context<Self>) {
        if self.guard_navigation(GuardedNavigation::DeleteObject(key.clone()), cx) {
            return;
        }

        self.open_delete_confirm_now(key, cx);
    }

    pub(super) fn open_delete_confirm_now(&mut self, key: String, cx: &mut Context<Self>) {
        let size_bytes = self.object_size_hint(&key);
        self.pending_object_delete = Some(PendingObjectDelete { key, size_bytes });
        cx.notify();
    }

    /// Best-effort size for the confirmation dialog: the previewed object's
    /// metadata when it matches, otherwise the current listing page's row.
    fn object_size_hint(&self, key: &str) -> Option<u64> {
        if let Some(ObjectMetadataState::Loaded { metadata, .. }) = self.metadata.as_ref()
            && metadata.key == key
        {
            return Some(metadata.size_bytes);
        }

        self.tree
            .filtered_entries(&self.tree.current_prefix)
            .into_iter()
            .find_map(|entry| match entry {
                ObjectTreeEntry::Object(summary) if summary.key == key => Some(summary.size_bytes),
                _ => None,
            })
    }

    pub(super) fn cancel_delete_object(&mut self, cx: &mut Context<Self>) {
        self.pending_object_delete = None;
        cx.notify();
    }

    pub(super) fn confirm_delete_object(&mut self, cx: &mut Context<Self>) {
        let Some(pending) = self.pending_object_delete.take() else {
            return;
        };
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.object_browser.error.connection_unavailable"),
                ),
                cx,
            );
            return;
        };

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let bucket = self.bucket.clone();
        let profile_id = self.profile_id;
        let key = pending.key;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let result = cx
                .background_executor()
                .spawn({
                    let bucket = bucket.clone();
                    let key = key.clone();
                    async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))
                        })?;
                        api.delete_object(&bucket, &key)
                    }
                })
                .await;

            record_object_delete_audit(
                &audit_service,
                profile_id,
                &bucket,
                &key,
                result.as_ref().err().map(ToString::to_string).as_deref(),
            );

            match &result {
                Ok(()) => {
                    cx.update(|cx| {
                        Toast::success(dbflux_i18n::t!(
                            "document.object_browser.delete.deleted_toast",
                            uri = format!("s3://{bucket}/{key}")
                        ))
                        .meta_right(now_hms())
                        .push(cx);
                    })
                    .ok();
                }
                Err(err) => report_error_async(db_error_to_user_facing(err), cx),
            }

            if result.is_ok() {
                cx.update(|cx| {
                    entity.update(cx, |doc, cx| {
                        if doc.preview_key.as_deref() == Some(key.as_str()) {
                            doc.close_preview_now(cx);
                        }
                        doc.reload_current_prefix(cx);
                    });
                })
                .ok();
            }
        })
        .detach();
    }

    /// Confirmation overlay for a single-object delete — the same
    /// key+size/Cancel/Delete shape `buckets_table` uses for empty-bucket
    /// delete.
    pub(super) fn render_object_delete_confirm(
        &self,
        pending: &PendingObjectDelete,
        cx: &mut Context<Self>,
    ) -> impl IntoElement {
        let theme = cx.theme();
        let size_label = pending
            .size_bytes
            .map(crate::buckets_table::format_bytes)
            .unwrap_or_else(|| dbflux_i18n::t!("document.object_browser.delete.unknown_size"));

        div()
            .id("object-browser-delete-overlay")
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
                    .flex()
                    .flex_col()
                    .gap(Spacing::MD)
                    .p(Spacing::MD)
                    .child(
                        div()
                            .flex()
                            .items_center()
                            .gap(Spacing::SM)
                            .child(
                                Icon::new(AppIcon::TriangleAlert)
                                    .size(Heights::ICON_MD)
                                    .warning(),
                            )
                            .child(Text::heading(dbflux_i18n::t!(
                                "document.object_browser.delete.title"
                            ))),
                    )
                    .child(Text::muted(dbflux_i18n::t!(
                        "document.object_browser.delete.body",
                        key = pending.key.as_str(),
                        size = size_label.as_str()
                    )))
                    .child(
                        div()
                            .flex()
                            .justify_end()
                            .gap(Spacing::SM)
                            .child(
                                div()
                                    .id("object-browser-delete-cancel")
                                    .flex()
                                    .items_center()
                                    .h(Heights::CONTROL)
                                    .px(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.secondary)
                                    .hover(|d| d.bg(theme.muted))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.cancel_delete_object(cx);
                                    }))
                                    .child(Text::caption(dbflux_i18n::t!(
                                        "document.object_browser.delete.cancel"
                                    ))),
                            )
                            .child(
                                div()
                                    .id("object-browser-delete-confirm")
                                    .flex()
                                    .items_center()
                                    .gap(Spacing::XS)
                                    .h(Heights::CONTROL)
                                    .px(Spacing::SM)
                                    .rounded(Radii::SM)
                                    .cursor_pointer()
                                    .bg(theme.danger)
                                    .hover(|d| d.opacity(0.9))
                                    .on_click(cx.listener(|this, _, _, cx| {
                                        this.confirm_delete_object(cx);
                                    }))
                                    .child(
                                        Icon::new(AppIcon::Delete)
                                            .size(Heights::ICON_SM)
                                            .color(theme.background),
                                    )
                                    .child(
                                        Text::caption(dbflux_i18n::t!(
                                            "document.object_browser.delete.confirm"
                                        ))
                                        .color(theme.background),
                                    ),
                            ),
                    ),
            )
    }
}

/// Audits a single-object delete. Never records the object's body — only the
/// bucket/key and the outcome.
fn record_object_delete_audit(
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
            "object_delete_failed",
        ),
        None => (EventSeverity::Info, EventOutcome::Success, "object_delete"),
    };

    let mut summary = format!("Deleted s3://{bucket}/{key}");
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
        log::warn!("[object browser] failed to record object-delete audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::PendingObjectDelete;

    /// A missing size renders as "unknown size" rather than a placeholder
    /// number — nothing to guess it from yet.
    #[test]
    fn pending_delete_carries_key_and_optional_size() {
        let pending = PendingObjectDelete {
            key: "logs/a.txt".to_string(),
            size_bytes: Some(1024),
        };
        assert_eq!(pending.key, "logs/a.txt");
        assert_eq!(pending.size_bytes, Some(1024));

        let unknown = PendingObjectDelete {
            key: "logs/b.txt".to_string(),
            size_bytes: None,
        };
        assert_eq!(unknown.size_bytes, None);
    }
}

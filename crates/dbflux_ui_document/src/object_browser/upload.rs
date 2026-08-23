//! Consumes the toolbar's Upload intent (`request_upload` /
//! `take_pending_upload`): picks local files, streams each one to the current
//! prefix, and refreshes the level once every upload has settled.
//!
//! No drag&drop, no multipart, no transfers panel — v1 upload is fire-and-
//! forget, per DEC-18/Amendment D of the design.

use super::ObjectBrowserDocument;
use super::data::db_error_to_user_facing;
use dbflux_core::DbError;
use dbflux_ui_base::file_dialog;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::Context;
use uuid::Uuid;

impl ObjectBrowserDocument {
    /// Drains `pending_upload`, raised by the toolbar's Upload button, and
    /// runs the file-picker + per-file upload flow when it is set.
    pub(super) fn drain_pending_upload(&mut self, cx: &mut Context<Self>) {
        if !self.take_pending_upload() {
            return;
        }

        self.start_upload_flow(cx);
    }

    fn start_upload_flow(&mut self, cx: &mut Context<Self>) {
        if !file_dialog::is_native_file_dialog_available() {
            report_error(
                UserFacingError::new(
                    ErrorKind::Storage,
                    dbflux_i18n::t!("document.object_browser.upload.error.no_file_picker"),
                ),
                cx,
            );
            return;
        }

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
        let prefix = self.tree.current_prefix.clone();
        let profile_id = self.profile_id;
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let handles = rfd::AsyncFileDialog::new()
                .set_title(dbflux_i18n::t!(
                    "document.object_browser.upload.dialog_title"
                ))
                .pick_files()
                .await;

            let Some(handles) = handles else {
                // Cancelled picker: nothing happened, so no toast and no
                // audit row — same convention as the download picker.
                return;
            };

            if handles.is_empty() {
                return;
            }

            let mut uploaded = 0usize;
            let mut failed = 0usize;

            for handle in handles {
                let path = handle.path().to_path_buf();
                let file_name = path
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
                    .unwrap_or_else(|| "file".to_string());
                let key = format!("{prefix}{file_name}");

                let connection = connection.clone();
                let bucket_for_task = bucket.clone();
                let key_for_task = key.clone();
                let path_for_task = path.clone();

                let result = cx
                    .background_executor()
                    .spawn(async move {
                        let api = connection.object_store_api().ok_or_else(|| {
                            DbError::NotSupported(dbflux_i18n::t!(
                                "document.object_browser.error.api_unavailable"
                            ))
                        })?;

                        api.upload_object(&bucket_for_task, &key_for_task, &path_for_task, None)
                    })
                    .await;

                record_upload_audit(
                    &audit_service,
                    profile_id,
                    &bucket,
                    &key,
                    result.as_ref().err().map(ToString::to_string).as_deref(),
                );

                match result {
                    Ok(()) => uploaded += 1,
                    Err(err) => {
                        failed += 1;
                        report_error_async(db_error_to_user_facing(&err), cx);
                    }
                }
            }

            cx.update(|cx| {
                if uploaded > 0 {
                    let uri = format!("s3://{bucket}/{prefix}");
                    let mut message = if uploaded == 1 {
                        dbflux_i18n::t!(
                            "document.object_browser.upload.toast.uploaded.one",
                            count = uploaded,
                            uri = uri.as_str()
                        )
                    } else {
                        dbflux_i18n::t!(
                            "document.object_browser.upload.toast.uploaded.many",
                            count = uploaded,
                            uri = uri.as_str()
                        )
                    };

                    if failed > 0 {
                        let suffix = if failed == 1 {
                            dbflux_i18n::t!(
                                "document.object_browser.upload.toast.failed_suffix.one",
                                count = failed
                            )
                        } else {
                            dbflux_i18n::t!(
                                "document.object_browser.upload.toast.failed_suffix.many",
                                count = failed
                            )
                        };
                        message.push_str(&suffix);
                    }

                    Toast::success(message).meta_right(now_hms()).push(cx);
                }

                entity.update(cx, |doc, cx| doc.reload_current_prefix(cx));
            })
            .ok();
        })
        .detach();
    }
}

/// Audits a single-file upload. Never records the local source path — only
/// the destination bucket/key and the outcome.
fn record_upload_audit(
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
            "object_upload_failed",
        ),
        None => (EventSeverity::Info, EventOutcome::Success, "object_upload"),
    };

    let mut summary = format!("Uploaded s3://{bucket}/{key}");
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
        log::warn!("[object browser] failed to record upload audit event: {e}");
    }
}

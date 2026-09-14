//! Getting an object out of the bucket and onto the local machine.
//!
//! Two destinations, one transfer: a user-chosen path (Download) and a scratch
//! file handed straight to the system's default application (Open externally).
//! Both are the only way to inspect an object the preview pane cannot render —
//! archived, over the size limit, or simply not an image.

use super::ObjectBrowserDocument;
use super::preview_content::PreviewContentState;
use crate::buckets_table::format_bytes;
use dbflux_core::DbError;
use dbflux_ui_base::toast::{Toast, now_hms};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};
use dbflux_ui_base::{file_dialog, open_external};
use gpui::Context;
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Where a fetched object is written, and how the outcome is reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransferPurpose {
    /// Saved to a path the user picked (or the export fallback directory).
    Download,
    /// Written to a scratch file and handed to the system handler.
    OpenExternally,
}

impl TransferPurpose {
    fn audit_action(self, failed: bool) -> &'static str {
        match (self, failed) {
            (TransferPurpose::Download, false) => "object_download",
            (TransferPurpose::Download, true) => "object_download_failed",
            (TransferPurpose::OpenExternally, false) => "object_open_external",
            (TransferPurpose::OpenExternally, true) => "object_open_external_failed",
        }
    }
}

/// Separated from `DbError` so a local write failure is reported as a storage
/// problem and a fetch failure as a driver problem.
enum TransferError {
    /// Boxed: `DbError` is large enough that carrying it inline makes every
    /// `Result` in this module oversized.
    Driver(Box<DbError>),
    Local(String),
}

impl TransferError {
    fn to_user_facing(&self) -> UserFacingError {
        match self {
            TransferError::Driver(err) => match err.formatted() {
                Some(formatted) => {
                    UserFacingError::from_formatted(ErrorKind::Driver, formatted.clone())
                }
                None => UserFacingError::new(ErrorKind::Driver, err.to_string()),
            },
            TransferError::Local(message) => {
                UserFacingError::new(ErrorKind::Storage, message.clone())
            }
        }
    }

    fn message(&self) -> String {
        match self {
            TransferError::Driver(err) => err.to_string(),
            TransferError::Local(message) => message.clone(),
        }
    }
}

/// Last path segment of a key, used as the suggested local file name.
pub(super) fn object_file_name(key: &str) -> String {
    let name = key
        .trim_end_matches('/')
        .rsplit_once('/')
        .map(|(_, name)| name)
        .unwrap_or(key)
        .trim();

    if name.is_empty() {
        "object".to_string()
    } else {
        name.to_string()
    }
}

impl ObjectBrowserDocument {
    /// Saves the object to a path the user chooses, falling back to the shared
    /// export directory on hosts without a working file picker.
    pub(super) fn download_object(&mut self, key: String, cx: &mut Context<Self>) {
        self.run_object_transfer(key, TransferPurpose::Download, cx);
    }

    /// Downloads the object into a scratch file and opens it with whatever the
    /// desktop associates with its type.
    pub(super) fn open_object_externally(&mut self, key: String, cx: &mut Context<Self>) {
        self.run_object_transfer(key, TransferPurpose::OpenExternally, cx);
    }

    /// Resolves a destination, streams the object straight to disk on the
    /// background executor, audits the outcome, and reports it to the user.
    fn run_object_transfer(
        &mut self,
        key: String,
        purpose: TransferPurpose,
        cx: &mut Context<Self>,
    ) {
        let Some(connection) = self.get_connection(cx) else {
            self.preview_content = PreviewContentState::Failed(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            ));
            cx.notify();
            return;
        };

        let audit_service = self.app_state.read(cx).audit_service().clone();
        let bucket = self.bucket.clone();
        let profile_id = self.profile_id;
        let suggested_name = object_file_name(&key);
        let dialog_available = file_dialog::is_native_file_dialog_available();

        cx.spawn(async move |_this, cx| {
            let destination = match purpose {
                TransferPurpose::Download => {
                    match choose_download_path(&suggested_name, dialog_available).await {
                        Ok(Some(path)) => path,
                        // Cancelled picker: the user already knows nothing
                        // happened, so no toast and no audit row.
                        Ok(None) => return,
                        Err(message) => {
                            let error = TransferError::Local(message);
                            record_transfer_audit(
                                &audit_service,
                                purpose,
                                profile_id,
                                &bucket,
                                &key,
                                Some(&error.message()),
                            );
                            report_error_async(error.to_user_facing(), cx);
                            return;
                        }
                    }
                }
                TransferPurpose::OpenExternally => match open_external::external_open_dir() {
                    Ok(dir) => file_dialog::unique_path_in(&dir, &suggested_name),
                    Err(message) => {
                        let error = TransferError::Local(message);
                        record_transfer_audit(
                            &audit_service,
                            purpose,
                            profile_id,
                            &bucket,
                            &key,
                            Some(&error.message()),
                        );
                        report_error_async(error.to_user_facing(), cx);
                        return;
                    }
                },
            };

            let transfer = cx
                .background_executor()
                .spawn({
                    let bucket = bucket.clone();
                    let key = key.clone();
                    let destination = destination.clone();

                    async move { fetch_and_write(&*connection, &bucket, &key, &destination) }
                })
                .await;

            let byte_len = match transfer {
                Ok(byte_len) => byte_len,
                Err(error) => {
                    record_transfer_audit(
                        &audit_service,
                        purpose,
                        profile_id,
                        &bucket,
                        &key,
                        Some(&error.message()),
                    );
                    report_error_async(error.to_user_facing(), cx);
                    return;
                }
            };

            let handler_started = match purpose {
                TransferPurpose::Download => true,
                TransferPurpose::OpenExternally => open_external::open_external(&destination),
            };

            record_transfer_audit(&audit_service, purpose, profile_id, &bucket, &key, None);

            cx.update(|cx| {
                let name = object_file_name(&key);

                match (purpose, handler_started) {
                    (TransferPurpose::Download, _) => Toast::success(dbflux_i18n::t!(
                        "document.object_browser.transfer.toast.saved",
                        name = name.as_str(),
                        size = format_bytes(byte_len).as_str(),
                        path = destination.display().to_string().as_str()
                    )),
                    (TransferPurpose::OpenExternally, true) => Toast::success(dbflux_i18n::t!(
                        "document.object_browser.transfer.toast.opened",
                        name = name.as_str()
                    )),
                    (TransferPurpose::OpenExternally, false) => Toast::error(dbflux_i18n::t!(
                        "document.object_browser.transfer.toast.no_handler",
                        name = name.as_str(),
                        path = destination.display().to_string().as_str()
                    )),
                }
                .meta_right(now_hms())
                .push(cx);
            })
            .ok();
        })
        .detach();
    }
}

/// Save-as destination for a download: the user's choice, `None` when the
/// picker was cancelled, or the shared export directory when the host has no
/// working picker at all.
async fn choose_download_path(
    suggested_name: &str,
    dialog_available: bool,
) -> Result<Option<PathBuf>, String> {
    if !dialog_available {
        let dir = file_dialog::fallback_export_dir().map_err(|err| {
            dbflux_i18n::t!(
                "document.object_browser.transfer.error.fallback_dir_failed",
                error = err.as_str()
            )
        })?;

        return Ok(Some(file_dialog::unique_path_in(
            dir.as_path(),
            suggested_name,
        )));
    }

    let handle = rfd::AsyncFileDialog::new()
        .set_title(dbflux_i18n::t!(
            "document.object_browser.transfer.dialog_title"
        ))
        .set_file_name(suggested_name)
        .add_filter(
            dbflux_i18n::t!("document.object_browser.transfer.dialog_filter_all_files"),
            &["*"],
        )
        .save_file()
        .await;

    Ok(handle.map(|handle| handle.path().to_path_buf()))
}

fn fetch_and_write(
    connection: &dyn dbflux_core::Connection,
    bucket: &str,
    key: &str,
    destination: &Path,
) -> Result<u64, TransferError> {
    let api = connection.object_store_api().ok_or_else(|| {
        TransferError::Driver(Box::new(DbError::NotSupported(dbflux_i18n::t!(
            "document.object_browser.error.api_unavailable"
        ))))
    })?;

    api.download_object(bucket, key, destination)
        .map_err(|err| TransferError::Driver(Box::new(err)))
}

/// Audits an object transfer. Only the bucket, key, and outcome are recorded —
/// never credentials, and never a signed URL.
fn record_transfer_audit(
    audit_service: &dbflux_audit::AuditService,
    purpose: TransferPurpose,
    profile_id: Uuid,
    bucket: &str,
    key: &str,
    error: Option<&str>,
) {
    use dbflux_core::chrono::Utc;
    use dbflux_core::observability::{
        EventCategory, EventOutcome, EventRecord, EventSeverity, EventSink,
    };

    let (severity, outcome) = match error {
        Some(_) => (EventSeverity::Error, EventOutcome::Failure),
        None => (EventSeverity::Info, EventOutcome::Success),
    };

    let verb = match purpose {
        TransferPurpose::Download => "Downloaded",
        TransferPurpose::OpenExternally => "Opened externally",
    };

    let mut summary = format!("{verb} s3://{bucket}/{key}");
    if let Some(error) = error {
        summary.push_str(&format!(": {error}"));
    }

    let event = EventRecord::new(
        Utc::now().timestamp_millis(),
        severity,
        EventCategory::ObjectStorage,
        outcome,
    )
    .with_action(purpose.audit_action(error.is_some()).to_string())
    .with_summary(summary)
    .with_actor_id("ui:user")
    .with_object_ref("object", format!("{bucket}/{key}"))
    .with_connection_context(profile_id.to_string(), bucket.to_string(), String::new());

    if let Err(e) = audit_service.record(event) {
        log::warn!("[object browser] failed to record transfer audit event: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::{TransferPurpose, object_file_name};

    /// T32: the suggested local name is the object's leaf, never the full key.
    #[test]
    fn local_file_name_is_the_key_leaf() {
        assert_eq!(object_file_name("reports/2026/q1.pdf"), "q1.pdf");
        assert_eq!(object_file_name("q1.pdf"), "q1.pdf");
        assert_eq!(object_file_name(""), "object");
    }

    /// T32: successes and failures are distinguishable in the audit log.
    #[test]
    fn audit_actions_separate_success_from_failure() {
        assert_eq!(
            TransferPurpose::Download.audit_action(false),
            "object_download"
        );
        assert_eq!(
            TransferPurpose::Download.audit_action(true),
            "object_download_failed"
        );
        assert_eq!(
            TransferPurpose::OpenExternally.audit_action(false),
            "object_open_external"
        );
    }
}

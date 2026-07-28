//! Data-loading layer for `BucketsTableDocument`.
//!
//! `list_buckets` runs once on open. Region/versioning (`get_bucket_details`)
//! is fetched lazily per row right after the initial list resolves, so
//! table-open latency is not gated on N buckets x 2 calls (DEC-14).
//! `estimate_bucket_size` is a potentially billed, paginated S3 call and is
//! only ever triggered by an explicit user action — never automatically.

use super::BucketsTableDocument;
use dbflux_core::{BucketDetails, BucketInfo, BucketSizeEstimate, DbError, ObjectListingPage};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error, report_error_async};
use gpui::Context;
use std::time::Instant;

/// Cap passed to `estimate_bucket_size` — S3 `ListObjectsV2` calls are billed
/// and can be slow on large buckets, so the estimate walks at most this many
/// objects before reporting `truncated: true`.
pub const BUCKET_SIZE_ESTIMATE_CAP: u64 = 10_000;

/// One row in the buckets table: the cheap `ListBuckets` summary plus the
/// lazily-fetched details and on-demand size estimate.
#[derive(Clone, Debug)]
pub struct BucketRow {
    pub info: BucketInfo,
    pub details: BucketDetailsState,
    pub size_estimate: BucketSizeEstimateState,
}

/// Loading state of a row's `get_bucket_details` (region + versioning) call.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum BucketDetailsState {
    #[default]
    NotLoaded,
    Loading,
    Loaded(BucketDetails),
    Error(String),
}

/// Loading state of a row's on-demand `estimate_bucket_size` call.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum BucketSizeEstimateState {
    #[default]
    NotRequested,
    Loading,
    Loaded(BucketSizeEstimate),
    Error(String),
}

/// Converts a driver error into a `UserFacingError` of kind `Driver`, using
/// the driver's structured `FormattedError` when the variant carries one.
fn db_error_to_user_facing(err: &DbError) -> UserFacingError {
    match err.formatted() {
        Some(fe) => UserFacingError::from_formatted(ErrorKind::Driver, fe.clone()),
        None => UserFacingError::new(ErrorKind::Driver, err.to_string()),
    }
}

/// Returns `true` when a bucket delete is allowed: the bucket must have no
/// objects and no common prefixes in the checked listing page (Amendment A —
/// the UI blocks the destructive-confirmation dialog entirely otherwise).
pub fn bucket_delete_allowed(page: &dbflux_core::ObjectListingPage) -> bool {
    page.objects.is_empty() && page.common_prefixes.is_empty()
}

/// Message shown when the user tries to delete a bucket that still holds
/// objects. Points at the recursive prefix delete instead of silently failing.
pub(super) fn bucket_not_empty_message(bucket: &str) -> String {
    format!(
        "Bucket \"{bucket}\" is not empty. Delete its objects with a recursive prefix delete before removing the bucket."
    )
}

/// Client-side timing of the last object-store call this document made,
/// surfaced as a status-bar segment (e.g. `ListBuckets · 188 ms`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperationTiming {
    pub label: &'static str,
    pub millis: u128,
}

impl OperationTiming {
    pub fn display(&self) -> String {
        format!("{} · {} ms", self.label, self.millis)
    }
}

impl BucketsTableDocument {
    pub(super) fn get_connection(
        &self,
        cx: &Context<Self>,
    ) -> Option<std::sync::Arc<dyn dbflux_core::Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| connected.connection.clone())
    }

    /// Kicks off the initial bucket listing. Called once from `new()`.
    pub(super) fn load_buckets(&mut self, cx: &mut Context<Self>) {
        use super::super::types::DocumentState;

        self.state = DocumentState::Loading;
        self.last_error = None;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.state = DocumentState::Error;
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();

        let task = cx.background_executor().spawn(async move {
            let started = Instant::now();

            let result = match connection.object_store_api() {
                Some(api) => api.list_buckets(),
                None => Err(DbError::NotSupported(
                    "Object-store API unavailable".to_string(),
                )),
            };

            (result, started.elapsed().as_millis())
        });

        cx.spawn(async move |_this, cx| {
            let (result, elapsed_millis) = task.await;

            if let Err(ref err) = result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.last_operation = Some(OperationTiming {
                        label: "ListBuckets",
                        millis: elapsed_millis,
                    });
                    doc.apply_bucket_list(result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn apply_bucket_list(
        &mut self,
        result: Result<Vec<BucketInfo>, DbError>,
        cx: &mut Context<Self>,
    ) {
        use super::super::types::DocumentState;

        match result {
            Ok(buckets) => {
                self.buckets = buckets
                    .into_iter()
                    .map(|info| BucketRow {
                        info,
                        details: BucketDetailsState::NotLoaded,
                        size_estimate: BucketSizeEstimateState::NotRequested,
                    })
                    .collect();
                self.state = DocumentState::Clean;
                self.last_error = None;
                self.clamp_selection();
                cx.notify();

                self.load_all_bucket_details(cx);
            }
            Err(err) => {
                self.state = DocumentState::Error;
                self.last_error = Some(err.to_string());
                cx.notify();
            }
        }
    }

    /// Triggers a `get_bucket_details` fetch for every currently listed
    /// bucket. Called right after `list_buckets` resolves so the table paints
    /// immediately and region/versioning fill in as each call completes.
    fn load_all_bucket_details(&mut self, cx: &mut Context<Self>) {
        let names: Vec<String> = self
            .buckets
            .iter()
            .map(|row| row.info.name.clone())
            .collect();

        for name in names {
            self.load_bucket_details(name, cx);
        }
    }

    fn load_bucket_details(&mut self, bucket_name: String, cx: &mut Context<Self>) {
        let Some(row) = self
            .buckets
            .iter_mut()
            .find(|row| row.info.name == bucket_name)
        else {
            return;
        };
        row.details = BucketDetailsState::Loading;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.set_bucket_details_error(
                &bucket_name,
                "Connection is no longer active".to_string(),
            );
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket_for_task = bucket_name.clone();

        let task = cx.background_executor().spawn(async move {
            let api = connection
                .object_store_api()
                .ok_or_else(|| DbError::NotSupported("Object-store API unavailable".to_string()))?;
            api.get_bucket_details(&bucket_for_task)
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(ref err) = result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_bucket_details(&bucket_name, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn apply_bucket_details(
        &mut self,
        bucket_name: &str,
        result: Result<BucketDetails, DbError>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(details) => {
                if let Some(row) = self
                    .buckets
                    .iter_mut()
                    .find(|row| row.info.name == bucket_name)
                {
                    row.details = BucketDetailsState::Loaded(details);
                }
            }
            Err(err) => {
                self.set_bucket_details_error(bucket_name, err.to_string());
            }
        }

        cx.notify();
    }

    fn set_bucket_details_error(&mut self, bucket_name: &str, message: String) {
        if let Some(row) = self
            .buckets
            .iter_mut()
            .find(|row| row.info.name == bucket_name)
        {
            row.details = BucketDetailsState::Error(message);
        }
    }

    /// Triggers the on-demand, paginated `estimate_bucket_size` call for a
    /// single bucket. Never called automatically — S3 `ListObjectsV2` calls
    /// are billed and can be slow on large buckets (DEC-14). The row-action
    /// UI that wires a button to this method lands in the table-UI batch.
    pub fn estimate_bucket_size(&mut self, bucket_name: String, cx: &mut Context<Self>) {
        let Some(row) = self
            .buckets
            .iter_mut()
            .find(|row| row.info.name == bucket_name)
        else {
            return;
        };
        row.size_estimate = BucketSizeEstimateState::Loading;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.set_bucket_size_estimate_error(
                &bucket_name,
                "Connection is no longer active".to_string(),
            );
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket_for_task = bucket_name.clone();

        let task = cx.background_executor().spawn(async move {
            let api = connection
                .object_store_api()
                .ok_or_else(|| DbError::NotSupported("Object-store API unavailable".to_string()))?;
            api.estimate_bucket_size(&bucket_for_task, BUCKET_SIZE_ESTIMATE_CAP)
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(ref err) = result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_bucket_size_estimate(&bucket_name, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn apply_bucket_size_estimate(
        &mut self,
        bucket_name: &str,
        result: Result<BucketSizeEstimate, DbError>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(estimate) => {
                if let Some(row) = self
                    .buckets
                    .iter_mut()
                    .find(|row| row.info.name == bucket_name)
                {
                    row.size_estimate = BucketSizeEstimateState::Loaded(estimate);
                }
            }
            Err(err) => {
                self.set_bucket_size_estimate_error(bucket_name, err.to_string());
            }
        }

        cx.notify();
    }

    fn set_bucket_size_estimate_error(&mut self, bucket_name: &str, message: String) {
        if let Some(row) = self
            .buckets
            .iter_mut()
            .find(|row| row.info.name == bucket_name)
        {
            row.size_estimate = BucketSizeEstimateState::Error(message);
        }
    }

    // -- Delete (empty buckets only) -------------------------------------

    /// Probes the selected bucket for content before offering deletion.
    ///
    /// Amendment A: a non-empty bucket never reaches the confirmation dialog.
    /// The probe is a single `list_objects` page at the bucket root; anything
    /// in `objects` or `common_prefixes` blocks the delete.
    pub(super) fn request_delete_selected_bucket(&mut self, cx: &mut Context<Self>) {
        let Some(bucket_name) = self.selected_bucket().map(str::to_string) else {
            return;
        };

        if self.delete_probe.is_some() || self.pending_delete.is_some() {
            return;
        }

        self.delete_probe = Some(bucket_name.clone());
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.delete_probe = None;
            report_error(
                UserFacingError::new(ErrorKind::User, "Connection is no longer active"),
                cx,
            );
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket_for_task = bucket_name.clone();

        let task = cx.background_executor().spawn(async move {
            let api = connection
                .object_store_api()
                .ok_or_else(|| DbError::NotSupported("Object-store API unavailable".to_string()))?;
            api.list_objects(&bucket_for_task, "", None)
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_delete_probe(&bucket_name, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn apply_delete_probe(
        &mut self,
        bucket_name: &str,
        result: Result<ObjectListingPage, DbError>,
        cx: &mut Context<Self>,
    ) {
        self.delete_probe = None;

        match result {
            Ok(page) if bucket_delete_allowed(&page) => {
                self.pending_delete = Some(bucket_name.to_string());
            }
            Ok(_) => {
                report_error(
                    UserFacingError::new(ErrorKind::User, bucket_not_empty_message(bucket_name)),
                    cx,
                );
            }
            Err(err) => {
                report_error(db_error_to_user_facing(&err), cx);
            }
        }

        cx.notify();
    }

    pub(super) fn cancel_delete_bucket(&mut self, cx: &mut Context<Self>) {
        self.pending_delete = None;
        cx.notify();
    }

    pub(super) fn confirm_delete_bucket(&mut self, cx: &mut Context<Self>) {
        let Some(bucket_name) = self.pending_delete.take() else {
            return;
        };
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            report_error(
                UserFacingError::new(ErrorKind::User, "Connection is no longer active"),
                cx,
            );
            return;
        };

        let entity = cx.entity().clone();
        let bucket_for_task = bucket_name.clone();

        let task = cx.background_executor().spawn(async move {
            let api = connection
                .object_store_api()
                .ok_or_else(|| DbError::NotSupported("Object-store API unavailable".to_string()))?;
            api.delete_bucket(&bucket_for_task)
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(ref err) = result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.apply_bucket_deleted(&bucket_name, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn apply_bucket_deleted(
        &mut self,
        bucket_name: &str,
        result: Result<(), DbError>,
        cx: &mut Context<Self>,
    ) {
        if result.is_err() {
            return;
        }

        self.buckets.retain(|row| row.info.name != bucket_name);
        self.clamp_selection();
        cx.notify();
    }

    /// Buckets matching `query` (case-insensitive substring on the bucket
    /// name). An empty query returns every bucket. This is the filtering
    /// logic the search box (table-UI batch) will drive.
    pub fn filtered_buckets(&self, query: &str) -> Vec<&BucketRow> {
        filter_buckets(&self.buckets, query)
    }

    #[cfg(test)]
    pub(crate) fn buckets_for_test(&self) -> &[BucketRow] {
        &self.buckets
    }

    #[cfg(test)]
    pub(crate) fn set_buckets_for_test(&mut self, buckets: Vec<BucketRow>) {
        self.buckets = buckets;
    }

    #[cfg(test)]
    pub(crate) fn pending_delete_for_test(&self) -> Option<&str> {
        self.pending_delete.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn set_search_query_for_test(&mut self, query: String) {
        self.search_query = query;
    }

    #[cfg(test)]
    pub(crate) fn set_last_operation_for_test(&mut self, timing: OperationTiming) {
        self.last_operation = Some(timing);
    }
}

/// Free-standing filter so it is testable without constructing a full
/// `BucketsTableDocument` entity.
fn filter_buckets<'a>(buckets: &'a [BucketRow], query: &str) -> Vec<&'a BucketRow> {
    let query = query.trim().to_lowercase();

    if query.is_empty() {
        return buckets.iter().collect();
    }

    buckets
        .iter()
        .filter(|row| row.info.name.to_lowercase().contains(&query))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DocumentState;
    use dbflux_core::{ObjectListingPage, ObjectSummary, VersioningStatus};

    fn bucket_row(name: &str) -> BucketRow {
        BucketRow {
            info: BucketInfo {
                name: name.to_string(),
                created_at: None,
            },
            details: BucketDetailsState::NotLoaded,
            size_estimate: BucketSizeEstimateState::NotRequested,
        }
    }

    /// T20: filtering is case-insensitive and matches on substring.
    #[test]
    fn filtered_buckets_matches_case_insensitive_substring() {
        let buckets = vec![
            bucket_row("prod-logs"),
            bucket_row("staging-uploads"),
            bucket_row("PROD-backups"),
        ];

        let matches = filter_buckets(&buckets, "prod");
        let names: Vec<&str> = matches.iter().map(|row| row.info.name.as_str()).collect();

        assert_eq!(names, vec!["prod-logs", "PROD-backups"]);
    }

    /// T20: an empty (or whitespace-only) query returns every bucket, in the
    /// original order.
    #[test]
    fn filtered_buckets_empty_query_returns_all() {
        let buckets = vec![bucket_row("a"), bucket_row("b")];

        assert_eq!(filter_buckets(&buckets, "   ").len(), 2);
    }

    /// T20: no substring match returns an empty result.
    #[test]
    fn filtered_buckets_no_match_returns_empty() {
        let buckets = vec![bucket_row("prod-logs")];

        assert!(filter_buckets(&buckets, "nonexistent").is_empty());
    }

    /// T20: delete-if-empty gating — a page with no objects and no common
    /// prefixes allows delete.
    #[test]
    fn bucket_delete_allowed_when_page_is_empty() {
        let page = ObjectListingPage {
            objects: Vec::new(),
            common_prefixes: Vec::new(),
            next_continuation_token: None,
        };

        assert!(bucket_delete_allowed(&page));
    }

    /// T20: an object anywhere in the page blocks delete.
    #[test]
    fn bucket_delete_blocked_by_objects() {
        let page = ObjectListingPage {
            objects: vec![ObjectSummary {
                key: "a.txt".to_string(),
                size_bytes: 1,
                storage_class: None,
                last_modified: None,
            }],
            common_prefixes: Vec::new(),
            next_continuation_token: None,
        };

        assert!(!bucket_delete_allowed(&page));
    }

    /// T20: a common prefix (subfolder) with no direct objects still blocks
    /// delete — the bucket is not truly empty.
    #[test]
    fn bucket_delete_blocked_by_common_prefixes() {
        let page = ObjectListingPage {
            objects: Vec::new(),
            common_prefixes: vec!["logs/".to_string()],
            next_continuation_token: None,
        };

        assert!(!bucket_delete_allowed(&page));
    }

    /// T20: `apply_bucket_list` on success populates rows in `Clean` state and
    /// clears any prior error, then transitions each row's details to
    /// `Loading` (the lazy-details fetch is triggered immediately, even
    /// though it cannot resolve without a live connection in this test).
    #[gpui::test]
    fn apply_bucket_list_success_populates_rows_and_clears_error(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.state = DocumentState::Loading;
                doc.last_error = Some("stale error".to_string());
                doc.apply_bucket_list(
                    Ok(vec![BucketInfo {
                        name: "my-bucket".to_string(),
                        created_at: None,
                    }]),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            assert_eq!(doc.state, DocumentState::Clean);
            assert!(doc.last_error.is_none());
            assert_eq!(doc.buckets_for_test().len(), 1);
            assert_eq!(doc.buckets_for_test()[0].info.name, "my-bucket");
        });
    }

    /// T20: `apply_bucket_list` on failure transitions to `Error` state with
    /// the driver's message recorded, and leaves the bucket list untouched.
    #[gpui::test]
    fn apply_bucket_list_failure_sets_error_state(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.state = DocumentState::Loading;
                doc.apply_bucket_list(
                    Err(DbError::NotSupported("no object-store API".to_string())),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            assert_eq!(doc.state, DocumentState::Error);
            assert!(doc.last_error.is_some());
            assert!(doc.buckets_for_test().is_empty());
        });
    }

    /// T20: `apply_bucket_details` on success stores `Loaded(details)` on the
    /// matching row without touching other rows.
    #[gpui::test]
    fn apply_bucket_details_success_updates_matching_row(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                doc.set_buckets_for_test(vec![bucket_row("a"), bucket_row("b")]);
            });
        });

        let details = BucketDetails {
            region: "us-east-1".to_string(),
            versioning: VersioningStatus::Enabled,
        };

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.apply_bucket_details("b", Ok(details.clone()), cx);
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            let rows = doc.buckets_for_test();
            assert_eq!(rows[0].details, BucketDetailsState::NotLoaded);
            assert_eq!(rows[1].details, BucketDetailsState::Loaded(details));
        });
    }

    /// T20: `apply_bucket_details` on failure stores an `Error` state on the
    /// matching row, carrying the driver's message.
    #[gpui::test]
    fn apply_bucket_details_failure_sets_row_error(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                doc.set_buckets_for_test(vec![bucket_row("a")]);
            });
        });

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.apply_bucket_details(
                    "a",
                    Err(DbError::NotSupported("region lookup failed".to_string())),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            match &doc.buckets_for_test()[0].details {
                BucketDetailsState::Error(message) => {
                    assert!(message.contains("region lookup failed"));
                }
                other => panic!("expected Error state, got {other:?}"),
            }
        });
    }

    /// T20: `apply_bucket_size_estimate` on success stores `Loaded(estimate)`
    /// on the matching row. `estimate_bucket_size` is opt-in only (never
    /// auto-run) — this test drives the apply step directly.
    #[gpui::test]
    fn apply_bucket_size_estimate_success_updates_matching_row(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                doc.set_buckets_for_test(vec![bucket_row("a")]);
            });
        });

        let estimate = BucketSizeEstimate {
            object_count: 42,
            total_bytes: 1024,
            truncated: false,
        };

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.apply_bucket_size_estimate("a", Ok(estimate), cx);
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            assert_eq!(
                doc.buckets_for_test()[0].size_estimate,
                BucketSizeEstimateState::Loaded(estimate)
            );
        });
    }

    /// T20: an empty probe page opens the confirmation dialog — this is the
    /// only path that can arm a bucket delete.
    #[gpui::test]
    fn delete_probe_on_empty_bucket_arms_confirmation(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.set_buckets_for_test(vec![bucket_row("empty-bucket")]);
                doc.apply_delete_probe(
                    "empty-bucket",
                    Ok(ObjectListingPage {
                        objects: Vec::new(),
                        common_prefixes: Vec::new(),
                        next_continuation_token: None,
                    }),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            assert_eq!(doc.read(cx).pending_delete_for_test(), Some("empty-bucket"));
        });
    }

    /// T20: a bucket that still holds objects never reaches the confirmation
    /// dialog (Amendment A) — the user is redirected to recursive delete.
    #[gpui::test]
    fn delete_probe_on_non_empty_bucket_blocks_confirmation(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.set_buckets_for_test(vec![bucket_row("full-bucket")]);
                doc.apply_delete_probe(
                    "full-bucket",
                    Ok(ObjectListingPage {
                        objects: vec![ObjectSummary {
                            key: "a.txt".to_string(),
                            size_bytes: 1,
                            storage_class: None,
                            last_modified: None,
                        }],
                        common_prefixes: Vec::new(),
                        next_continuation_token: None,
                    }),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            assert_eq!(doc.read(cx).pending_delete_for_test(), None);
        });
    }

    /// T20: the blocked-delete message names the bucket and points at the
    /// recursive prefix delete instead of dead-ending.
    #[test]
    fn bucket_not_empty_message_points_at_recursive_delete() {
        let message = bucket_not_empty_message("logs");

        assert!(message.contains("logs"));
        assert!(message.contains("recursive prefix delete"));
    }

    /// T20: a successful delete drops the row and moves the cursor to a row
    /// that still exists.
    #[gpui::test]
    fn deleted_bucket_leaves_the_table_and_reselects(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.set_buckets_for_test(vec![bucket_row("a"), bucket_row("b")]);
                doc.select_bucket("a".to_string(), cx);
                doc.apply_bucket_deleted("a", Ok(()), cx);
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            assert_eq!(doc.buckets_for_test().len(), 1);
            assert_eq!(doc.selected_bucket(), Some("b"));
        });
    }

    /// T20: keyboard navigation walks the filtered list, and the cursor never
    /// points at a row hidden by the search box.
    #[gpui::test]
    fn selection_follows_the_filtered_rows(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.set_buckets_for_test(vec![
                    bucket_row("prod-logs"),
                    bucket_row("staging-logs"),
                    bucket_row("prod-assets"),
                ]);
                doc.set_search_query_for_test("prod".to_string());
                doc.select_bucket("prod-logs".to_string(), cx);
                doc.move_selection(1, cx);
            });
        });

        cx.update(|cx| {
            assert_eq!(doc.read(cx).selected_bucket(), Some("prod-assets"));
        });

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                doc.set_search_query_for_test("staging".to_string());
                doc.clamp_selection();
            });
        });

        cx.update(|cx| {
            assert_eq!(doc.read(cx).selected_bucket(), Some("staging-logs"));
        });
    }

    /// T20: the document contributes a bucket-count status segment and, once
    /// a call has been timed, its duration.
    #[gpui::test]
    fn status_segments_report_bucket_count_and_last_operation(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                doc.set_buckets_for_test(vec![bucket_row("a"), bucket_row("b")]);
                doc.set_last_operation_for_test(OperationTiming {
                    label: "ListBuckets",
                    millis: 188,
                });
            });
        });

        cx.update(|cx| {
            let texts: Vec<String> = doc
                .read(cx)
                .status_segments(cx)
                .into_iter()
                .map(|segment| segment.text.to_string())
                .collect();

            assert!(texts.contains(&"2 buckets".to_string()));
            assert!(texts.contains(&"ListBuckets · 188 ms".to_string()));
        });
    }

    fn new_test_entity(cx: &mut gpui::TestAppContext) -> gpui::Entity<BucketsTableDocument> {
        use dbflux_storage::bootstrap::StorageRuntime;
        use gpui::AppContext as _;

        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| dbflux_ui_base::toast::ToastHost::new());
            cx.set_global(dbflux_ui_base::toast::ToastGlobal { host });
        });

        let app_state: gpui::Entity<dbflux_ui_base::AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = StorageRuntime::in_memory().expect("in-memory storage");
                dbflux_ui_base::AppStateEntity::new_with_storage_runtime(runtime)
                    .expect("test storage setup")
            })
        });

        let profile_id = uuid::Uuid::new_v4();

        let (doc, _window_cx) = cx.add_window_view(|window, cx| {
            BucketsTableDocument::new(profile_id, app_state, window, cx)
        });

        doc
    }
}

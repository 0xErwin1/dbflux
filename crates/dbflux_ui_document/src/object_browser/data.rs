//! Background-loading layer for `ObjectBrowserDocument`.
//!
//! Mirrors `buckets_table/data.rs`'s `object_store_api()` +
//! `background_executor().spawn` + `cx.spawn` pattern: the driver call runs
//! on the background executor, the result is applied to `self.tree`
//! (`tree.rs`) on the foreground once it resolves, and any error is reported
//! once via `report_error_async`.

use super::ObjectBrowserDocument;
use super::tree::TreeModeStepOutcome;
use crate::buckets_table::OperationTiming;
use crate::types::DocumentState;
use dbflux_core::{DbError, ObjectListingPage};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};
use gpui::Context;
use std::collections::VecDeque;
use std::time::Instant;

fn db_error_to_user_facing(err: &DbError) -> UserFacingError {
    match err.formatted() {
        Some(fe) => UserFacingError::from_formatted(ErrorKind::Driver, fe.clone()),
        None => UserFacingError::new(ErrorKind::Driver, err.to_string()),
    }
}

impl ObjectBrowserDocument {
    fn get_connection(
        &self,
        cx: &Context<Self>,
    ) -> Option<std::sync::Arc<dyn dbflux_core::Connection>> {
        self.app_state
            .read(cx)
            .connections()
            .get(&self.profile_id)
            .map(|connected| connected.connection.clone())
    }

    /// Loads one page of `prefix`'s direct children (per-level pagination —
    /// expanding a prefix always loads exactly one level, one page).
    pub fn expand_prefix(&mut self, prefix: String, cx: &mut Context<Self>) {
        self.load_prefix_page(prefix, None, cx);
    }

    /// Continues pagination for `prefix`, fetching the next page after the
    /// one currently loaded (a no-op if the level has no continuation token).
    pub fn load_more(&mut self, prefix: String, cx: &mut Context<Self>) {
        if !self
            .tree
            .level(&prefix)
            .is_some_and(|level| level.has_more())
        {
            return;
        }

        let token = self.tree.continuation_token(&prefix);
        self.load_prefix_page(prefix, token, cx);
    }

    fn load_prefix_page(
        &mut self,
        prefix: String,
        continuation_token: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.tree.begin_load(&prefix);
        self.state = DocumentState::Loading;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.tree
                .apply_error(&prefix, "Connection is no longer active".to_string());
            self.state = DocumentState::Error;
            self.last_error = Some("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let prefix_for_task = prefix.clone();

        let task = cx.background_executor().spawn(async move {
            let started = Instant::now();

            let result = match connection.object_store_api() {
                Some(api) => {
                    api.list_objects(&bucket, &prefix_for_task, continuation_token.as_deref())
                }
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
                        label: "ListObjectsV2",
                        millis: elapsed_millis,
                    });
                    doc.apply_prefix_page(&prefix, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    fn apply_prefix_page(
        &mut self,
        prefix: &str,
        result: Result<ObjectListingPage, DbError>,
        cx: &mut Context<Self>,
    ) {
        match result {
            Ok(page) => {
                self.tree.apply_page(prefix, page);
                self.state = DocumentState::Clean;
                self.last_error = None;
            }
            Err(err) => {
                self.tree.apply_error(prefix, err.to_string());
                self.state = DocumentState::Error;
                self.last_error = Some(err.to_string());
            }
        }

        cx.notify();
    }

    // -- Tree mode -----------------------------------------------------------

    /// Toggles tree mode: cancels an in-flight walk when running, otherwise
    /// starts a new bounded, cancelable walk that recursively lists every
    /// level under the bucket root and flattens the result (`tree.rs`'s
    /// `TREE_MODE_PAGE_CAP`).
    pub fn toggle_tree_mode(&mut self, cx: &mut Context<Self>) {
        use super::tree::TreeModeStatus;

        if self.tree.tree_mode.status == TreeModeStatus::Running {
            self.tree.cancel_tree_mode();
            cx.notify();
            return;
        }

        let generation = self.tree.start_tree_mode();
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.tree
                .mark_tree_mode_error(generation, "Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let bucket = self.bucket.clone();
        let entity = cx.entity().clone();

        cx.spawn(async move |_this, cx| {
            let mut queue: VecDeque<(usize, String, Option<String>)> = VecDeque::new();
            queue.push_back((0, String::new(), None));

            while let Some((depth, prefix, token)) = queue.pop_front() {
                let still_current = cx
                    .update(|cx| entity.read(cx).tree.is_tree_mode_current(generation))
                    .unwrap_or(false);

                if !still_current {
                    return;
                }

                let connection = connection.clone();
                let bucket_for_call = bucket.clone();
                let prefix_for_call = prefix.clone();

                let page_result = cx
                    .background_executor()
                    .spawn(async move {
                        match connection.object_store_api() {
                            Some(api) => api.list_objects(
                                &bucket_for_call,
                                &prefix_for_call,
                                token.as_deref(),
                            ),
                            None => Err(DbError::NotSupported(
                                "Object-store API unavailable".to_string(),
                            )),
                        }
                    })
                    .await;

                let page = match page_result {
                    Ok(page) => page,
                    Err(err) => {
                        report_error_async(db_error_to_user_facing(&err), cx);
                        cx.update(|cx| {
                            entity.update(cx, |doc, cx| {
                                doc.tree.mark_tree_mode_error(generation, err.to_string());
                                cx.notify();
                            });
                        })
                        .ok();
                        return;
                    }
                };

                let outcome: Option<TreeModeStepOutcome> = cx
                    .update(|cx| {
                        entity.update(cx, |doc, cx| {
                            let outcome = doc
                                .tree
                                .apply_tree_mode_page(generation, depth, &prefix, page);
                            cx.notify();
                            outcome
                        })
                    })
                    .ok();

                let Some(outcome) = outcome else { return };

                if !outcome.applied || outcome.capped {
                    break;
                }

                if let Some(next_token) = outcome.continuation_token {
                    queue.push_back((depth, prefix.clone(), Some(next_token)));
                }

                for sub_prefix in outcome.discovered_prefixes {
                    queue.push_back((depth + 1, sub_prefix, None));
                }
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.tree.mark_tree_mode_done(generation);
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }
}

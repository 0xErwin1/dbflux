//! Background-loading layer for `ObjectBrowserDocument`.
//!
//! Mirrors `buckets_table/data.rs`'s `object_store_api()` +
//! `background_executor().spawn` + `cx.spawn` pattern: the driver call runs
//! on the background executor, the result is applied to `self.tree`
//! (`tree.rs`) on the foreground once it resolves, and any error is reported
//! once via `report_error_async`.

use super::ObjectBrowserDocument;
use super::metadata::{
    ObjectMetadataState, ObjectVersionsState, PreviewGate, evaluate_preview_gate,
};
use super::preview_content::{
    ImagePreview, PreviewContentState, decode_image_dimensions, detect_preview_kind,
};
use super::tree::TreeModeStepOutcome;
use crate::buckets_table::{BucketDetailsState, OperationTiming};
use crate::types::DocumentState;
use dbflux_core::{DbError, ObjectListingPage, ObjectMetadata};
use dbflux_ui_base::user_error::{ErrorKind, UserFacingError, report_error_async};
use gpui::{Context, Image, ImageFormat};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

fn db_error_to_user_facing(err: &DbError) -> UserFacingError {
    match err.formatted() {
        Some(fe) => UserFacingError::from_formatted(ErrorKind::Driver, fe.clone()),
        None => UserFacingError::new(ErrorKind::Driver, err.to_string()),
    }
}

impl ObjectBrowserDocument {
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

    /// Refresh: drops the current level's cached entries and reloads its
    /// first page, so a stale listing is never merged with fresh results.
    pub fn reload_current_prefix(&mut self, cx: &mut Context<Self>) {
        let prefix = self.tree.current_prefix.clone();

        self.tree.reset_level(&prefix);
        self.expand_prefix(prefix, cx);
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

    // -- Object metadata -----------------------------------------------------

    /// Fetches the selected object's metadata (`HeadObject`) and derives its
    /// preview gate. No object bytes are ever requested here — the gate
    /// decides whether a body fetch is allowed at all.
    pub(super) fn load_object_metadata(&mut self, key: String, cx: &mut Context<Self>) {
        self.metadata_generation = self.metadata_generation.wrapping_add(1);
        let generation = self.metadata_generation;

        self.metadata = Some(ObjectMetadataState::Loading);

        let Some(connection) = self.get_connection(cx) else {
            self.metadata = Some(ObjectMetadataState::Error(
                "Connection is no longer active".to_string(),
            ));
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let key_for_task = key.clone();

        let task = cx.background_executor().spawn(async move {
            let started = Instant::now();

            let result = match connection.object_store_api() {
                Some(api) => api.head_object(&bucket, &key_for_task),
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
                        label: "HeadObject",
                        millis: elapsed_millis,
                    });
                    doc.apply_object_metadata(generation, key, result, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn apply_object_metadata(
        &mut self,
        generation: u64,
        key: String,
        result: Result<ObjectMetadata, DbError>,
        cx: &mut Context<Self>,
    ) {
        let is_current =
            generation == self.metadata_generation && self.preview_key.as_deref() == Some(&key);

        if !is_current {
            return;
        }

        let mut image_request = None;

        self.metadata = Some(match result {
            Ok(metadata) => {
                let limit_bytes = self
                    .app_state
                    .read(cx)
                    .general_settings()
                    .object_preview_size_limit_bytes();

                let gate = evaluate_preview_gate(&metadata, limit_bytes);

                if gate == PreviewGate::Allowed
                    && let Some(format) =
                        detect_preview_kind(metadata.content_type.as_deref(), &metadata.key)
                            .image_format()
                {
                    image_request = Some((metadata.key.clone(), format));
                }

                ObjectMetadataState::Loaded {
                    gate,
                    metadata: Box::new(metadata),
                }
            }
            Err(err) => ObjectMetadataState::Error(err.to_string()),
        });

        if let Some((key, format)) = image_request {
            self.load_preview_image(key, format, cx);
        }

        cx.notify();
    }

    // -- Preview body --------------------------------------------------------

    /// Fetches an image object's bytes and proves they decode before handing
    /// them to the renderer. Only ever reached for objects the gate allowed, so
    /// the transfer is bounded by the configured preview size limit.
    pub(super) fn load_preview_image(
        &mut self,
        key: String,
        format: ImageFormat,
        cx: &mut Context<Self>,
    ) {
        self.preview_content_generation = self.preview_content_generation.wrapping_add(1);
        let generation = self.preview_content_generation;

        self.preview_content = PreviewContentState::Loading;

        let Some(connection) = self.get_connection(cx) else {
            self.preview_content =
                PreviewContentState::Failed("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let key_for_task = key.clone();

        let task = cx.background_executor().spawn(async move {
            let started = Instant::now();

            let bytes = match connection.object_store_api() {
                Some(api) => api.get_object(&bucket, &key_for_task),
                None => Err(DbError::NotSupported(
                    "Object-store API unavailable".to_string(),
                )),
            };

            let elapsed_millis = started.elapsed().as_millis();

            let decoded = bytes.map(|bytes| {
                decode_image_dimensions(&bytes).map(|(width, height)| ImagePreview {
                    byte_len: bytes.len() as u64,
                    image: Arc::new(Image::from_bytes(format, bytes)),
                    width,
                    height,
                })
            });

            (decoded, elapsed_millis)
        });

        cx.spawn(async move |_this, cx| {
            let (decoded, elapsed_millis) = task.await;

            // Transfer failures are the user's problem to see; a decode failure
            // is a presentation fallback and stays inside the pane.
            if let Err(ref err) = decoded {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            let state = match decoded {
                Ok(Ok(preview)) => PreviewContentState::Image(Box::new(preview)),
                Ok(Err(message)) => PreviewContentState::Failed(message),
                Err(err) => PreviewContentState::Failed(err.to_string()),
            };

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.last_operation = Some(OperationTiming {
                        label: "GetObject",
                        millis: elapsed_millis,
                    });
                    doc.apply_preview_content(generation, key, state, cx);
                });
            })
            .ok();
        })
        .detach();
    }

    pub(super) fn apply_preview_content(
        &mut self,
        generation: u64,
        key: String,
        state: PreviewContentState,
        cx: &mut Context<Self>,
    ) {
        let is_current = generation == self.preview_content_generation
            && self.preview_key.as_deref() == Some(&key);

        if !is_current {
            return;
        }

        self.preview_content = state;
        cx.notify();
    }

    /// Fetches the bucket's region/versioning once per document session. The
    /// versions row needs the versioning status to know whether an object can
    /// have history at all.
    pub(super) fn ensure_bucket_details(&mut self, cx: &mut Context<Self>) {
        if self.bucket_details != BucketDetailsState::NotLoaded {
            return;
        }

        self.bucket_details = BucketDetailsState::Loading;

        let Some(connection) = self.get_connection(cx) else {
            self.bucket_details =
                BucketDetailsState::Error("Connection is no longer active".to_string());
            return;
        };

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();

        let task = cx.background_executor().spawn(async move {
            match connection.object_store_api() {
                Some(api) => api.get_bucket_details(&bucket),
                None => Err(DbError::NotSupported(
                    "Object-store API unavailable".to_string(),
                )),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    // A failed capability probe only costs the versions row;
                    // it is not the operation the user asked for, so it stays
                    // inside the panel instead of raising a toast.
                    doc.bucket_details = match result {
                        Ok(details) => BucketDetailsState::Loaded(details),
                        Err(err) => BucketDetailsState::Error(err.to_string()),
                    };
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
    }

    /// Lists an object's versions. Only ever called from the metadata panel's
    /// explicit "View versions" action — never as part of selecting an object.
    pub(super) fn load_object_versions(&mut self, key: String, cx: &mut Context<Self>) {
        self.versions = ObjectVersionsState::Loading;
        cx.notify();

        let Some(connection) = self.get_connection(cx) else {
            self.versions =
                ObjectVersionsState::Error("Connection is no longer active".to_string());
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let key_for_task = key.clone();

        let task = cx.background_executor().spawn(async move {
            match connection.object_store_api() {
                Some(api) => api.list_object_versions(&bucket, &key_for_task),
                None => Err(DbError::NotSupported(
                    "Object-store API unavailable".to_string(),
                )),
            }
        });

        cx.spawn(async move |_this, cx| {
            let result = task.await;

            if let Err(ref err) = result {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    if doc.preview_key.as_deref() != Some(&key) {
                        return;
                    }

                    doc.versions = match result {
                        Ok(versions) => ObjectVersionsState::Loaded(versions),
                        Err(err) => ObjectVersionsState::Error(err.to_string()),
                    };
                    cx.notify();
                });
            })
            .ok();
        })
        .detach();
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

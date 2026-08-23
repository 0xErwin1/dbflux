//! Background-loading layer for `ObjectBrowserDocument`.
//!
//! Mirrors `buckets_table/data.rs`'s `object_store_api()` +
//! `background_executor().spawn` + `cx.spawn` pattern: the driver call runs
//! on the background executor, the result is applied to `self.tree`
//! (`tree.rs`) on the foreground once it resolves, and any error is reported
//! once via `report_error_async`.

use super::ObjectBrowserDocument;
use super::editor::PendingTextBody;
use super::metadata::{
    ObjectMetadataState, ObjectVersionsState, PreviewGate, evaluate_preview_gate,
};
use super::preview_content::{
    ImagePreview, PreviewContentState, PreviewKind, decode_image_dimensions, detect_preview_kind,
    validate_svg_body,
};
use super::tree::PrefixLoadState;
use crate::buckets_table::{BucketDetailsState, OperationTiming};
use crate::object_text::decode_text_body;
use crate::types::DocumentState;
use dbflux_core::{DbError, ObjectListingPage, ObjectMetadata};
use dbflux_ui_base::user_error::report_error_async;
use gpui::{Context, Image, ImageFormat};
use std::sync::Arc;
use std::time::Instant;

pub(super) use crate::object_text::db_error_to_user_facing;

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

        self.reload_prefix(prefix, cx);
    }

    /// Same refresh, for a level that is not the one being listed — a
    /// tree-mode node, or the folder a context-menu action targeted.
    pub fn reload_prefix(&mut self, prefix: String, cx: &mut Context<Self>) {
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
            self.tree.apply_error(
                &prefix,
                dbflux_i18n::t!("document.object_browser.error.connection_unavailable"),
            );
            self.state = DocumentState::Error;
            self.last_error = Some(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            ));
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
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
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
            self.metadata = Some(ObjectMetadataState::Error(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            )));
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
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
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
        let mut text_request = None;

        self.metadata = Some(match result {
            Ok(metadata) => {
                let limit_bytes = self
                    .app_state
                    .read(cx)
                    .general_settings()
                    .object_preview_size_limit_bytes();

                let gate = evaluate_preview_gate(&metadata, limit_bytes);

                if gate == PreviewGate::Allowed {
                    match detect_preview_kind(metadata.content_type.as_deref(), &metadata.key) {
                        PreviewKind::Image(format) => {
                            image_request = Some((metadata.key.clone(), format));
                        }
                        // A metadata refresh after a save-back must not reload
                        // the buffer the user is still working in.
                        PreviewKind::Text if !self.has_editor_for(&metadata.key) => {
                            text_request =
                                Some((metadata.key.clone(), metadata.content_type.clone()));
                        }
                        _ => {}
                    }
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

        if let Some((key, content_type)) = text_request {
            self.load_preview_text(key, content_type, cx);
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
            self.preview_content = PreviewContentState::Failed(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            ));
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
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
            };

            let elapsed_millis = started.elapsed().as_millis();

            let decoded = bytes.map(|bytes| {
                let dimensions = if format == ImageFormat::Svg {
                    validate_svg_body(&bytes).map(|_| None)
                } else {
                    decode_image_dimensions(&bytes).map(Some)
                };

                dimensions.map(|dimensions| ImagePreview {
                    byte_len: bytes.len() as u64,
                    image: Arc::new(Image::from_bytes(format, bytes)),
                    dimensions,
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

    /// Whether an editor is already open on `key` — used to keep a metadata
    /// refresh from pulling the buffer out from under the user.
    pub(super) fn has_editor_for(&self, key: &str) -> bool {
        self.editor_for(key).is_some()
    }

    /// Fetches a text object's bytes for the inline editor. Like the image
    /// path, only ever reached for objects the gate allowed, so the transfer is
    /// bounded by the configured preview size limit.
    ///
    /// The decoded body is parked in `pending_text_body` instead of being
    /// installed here: building the buffer's `InputState` needs a `Window`,
    /// which only the render pass has.
    pub(super) fn load_preview_text(
        &mut self,
        key: String,
        content_type: Option<String>,
        cx: &mut Context<Self>,
    ) {
        self.preview_content_generation = self.preview_content_generation.wrapping_add(1);
        let generation = self.preview_content_generation;

        self.preview_content = PreviewContentState::Loading;

        let Some(connection) = self.get_connection(cx) else {
            self.preview_content = PreviewContentState::Failed(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            ));
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
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
            };

            let elapsed_millis = started.elapsed().as_millis();

            (bytes.map(decode_text_body), elapsed_millis)
        });

        cx.spawn(async move |_this, cx| {
            let (decoded, elapsed_millis) = task.await;

            // A transfer failure is the user's problem to see; a body that is
            // not text degrades inside the pane to the download/open actions.
            if let Err(ref err) = decoded {
                report_error_async(db_error_to_user_facing(err), cx);
            }

            cx.update(|cx| {
                entity.update(cx, |doc, cx| {
                    doc.last_operation = Some(OperationTiming {
                        label: "GetObject",
                        millis: elapsed_millis,
                    });

                    match decoded {
                        Ok(Ok(body)) => {
                            doc.accept_text_body(generation, key, content_type, body, cx)
                        }
                        Ok(Err(message)) => {
                            doc.apply_preview_content(
                                generation,
                                key,
                                PreviewContentState::Failed(message),
                                cx,
                            );
                        }
                        Err(err) => {
                            doc.apply_preview_content(
                                generation,
                                key,
                                PreviewContentState::Failed(err.to_string()),
                                cx,
                            );
                        }
                    }
                });
            })
            .ok();
        })
        .detach();
    }

    /// Hands a decoded body to the next render pass, subject to the same
    /// staleness guard as every other preview response.
    pub(super) fn accept_text_body(
        &mut self,
        generation: u64,
        key: String,
        content_type: Option<String>,
        body: crate::object_text::TextBody,
        cx: &mut Context<Self>,
    ) {
        let is_current = generation == self.preview_content_generation
            && self.preview_key.as_deref() == Some(&key);

        if !is_current {
            return;
        }

        self.pending_text_body = Some(PendingTextBody {
            key,
            body,
            content_type,
        });
        cx.notify();
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
            self.bucket_details = BucketDetailsState::Error(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            ));
            return;
        };

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();

        let task = cx.background_executor().spawn(async move {
            match connection.object_store_api() {
                Some(api) => api.get_bucket_details(&bucket),
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
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
            self.versions = ObjectVersionsState::Error(dbflux_i18n::t!(
                "document.object_browser.error.connection_unavailable"
            ));
            cx.notify();
            return;
        };

        let entity = cx.entity().clone();
        let bucket = self.bucket.clone();
        let key_for_task = key.clone();

        let task = cx.background_executor().spawn(async move {
            match connection.object_store_api() {
                Some(api) => api.list_object_versions(&bucket, &key_for_task),
                None => Err(DbError::NotSupported(dbflux_i18n::t!(
                    "document.object_browser.error.api_unavailable"
                ))),
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

    /// Toggles tree mode: a pure presentation switch, never a fetch. Nodes
    /// stay collapsed until the user expands them, so flipping the toggle
    /// costs nothing beyond what the current level already has loaded.
    pub fn toggle_tree_mode(&mut self, cx: &mut Context<Self>) {
        self.tree.toggle_tree_mode();
        cx.notify();
    }

    /// Expands a prefix node in tree mode, loading its first page of children
    /// when it has never been fetched — exactly one `ListObjectsV2` call for
    /// that node, mirroring `expand_prefix`'s per-level pagination. Never
    /// touches any other node.
    pub fn expand_tree_node(&mut self, prefix: String, cx: &mut Context<Self>) {
        let needs_load = self
            .tree
            .level(&prefix)
            .is_none_or(|level| level.state == PrefixLoadState::NotLoaded);

        self.tree.expand_node(&prefix);

        if needs_load {
            self.expand_prefix(prefix, cx);
        } else {
            self.clamp_selection();
            cx.notify();
        }
    }

    /// Flips a prefix node's expansion in tree mode, the way a single click on
    /// a folder row (or its chevron) does.
    pub fn toggle_tree_node(&mut self, prefix: String, cx: &mut Context<Self>) {
        if self.tree.is_expanded(&prefix) {
            self.collapse_tree_node(&prefix, cx);
        } else {
            self.expand_tree_node(prefix, cx);
        }
    }

    /// Collapses a prefix node in tree mode. Its children stay cached, so
    /// re-expanding it is instant.
    pub fn collapse_tree_node(&mut self, prefix: &str, cx: &mut Context<Self>) {
        self.tree.collapse_node(prefix);
        self.clamp_selection();
        cx.notify();
    }
}

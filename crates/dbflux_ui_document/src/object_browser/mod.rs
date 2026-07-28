mod data;
pub mod metadata;
mod pane;
mod preview;
pub mod preview_content;
mod render;
mod transfer;
pub mod tree;

pub use metadata::{ObjectMetadataState, ObjectVersionsState, PreviewGate, evaluate_preview_gate};
pub use preview_content::{ImagePreview, PreviewContentState, PreviewKind, detect_preview_kind};
pub use tree::{
    ObjectTree, ObjectTreeEntry, ObjectTreeNodeId, PrefixLoadState, TREE_MODE_PAGE_CAP,
    TreeModeRow, TreeModeStatus,
};

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use crate::buckets_table::{BucketDetailsState, OperationTiming};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::RefreshPolicy;
use dbflux_ui_base::AppStateEntity;
use gpui::*;
use uuid::Uuid;

/// Which part of the document currently owns keyboard input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectBrowserFocusMode {
    Listing,
    Filter,
}

/// Footer action raised from the preview pane for an object. The flows that
/// consume these (presign, delete) land with their own tasks; the pane only
/// records the intent, following the same `pending_*` + `take()` convention as
/// the toolbar's upload / new-folder intents. Download acts immediately and so
/// is deliberately absent here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectAction {
    Presign { key: String },
    Delete { key: String },
}

/// One rendered listing row, flattened from either the current prefix level
/// (per-level pagination) or the tree-mode walk (`depth` drives indentation).
#[derive(Clone, Debug, PartialEq)]
pub struct VisibleRow {
    pub depth: usize,
    pub parent_prefix: String,
    pub entry: ObjectTreeEntry,
}

/// Object browser opened for a single bucket under an object-storage
/// connection (routed from `BucketsTableDocument`'s Enter-on-row and the
/// sidebar's `OpenObjectStoreBucket` event).
///
/// The tree/pagination state lives in `tree: ObjectTree` (`tree.rs`, a pure
/// data model); this entity owns the GPUI plumbing — background loading via
/// `object_store_api()`, `cx.spawn`, and `report_error_async` — layered on
/// top of it in `data.rs`, and the breadcrumb/toolbar/listing layout lives in
/// `render.rs`.
pub struct ObjectBrowserDocument {
    id: DocumentId,
    title: String,
    profile_id: Uuid,
    bucket: String,
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    refresh_policy: RefreshPolicy,
    state: DocumentState,
    last_error: Option<String>,
    tree: ObjectTree,
    last_operation: Option<OperationTiming>,
    filter_input: Entity<InputState>,
    focus_mode: ObjectBrowserFocusMode,
    preview_key: Option<String>,
    metadata: Option<ObjectMetadataState>,
    /// Guards against a slow `head_object` for a previously selected object
    /// overwriting the metadata of the object the user has since selected.
    metadata_generation: u64,
    /// Body of the previewed object. Holds at most one object's bytes: it is
    /// reset on every selection change, so the decoded image never accumulates.
    preview_content: PreviewContentState,
    /// Same stale-response guard as `metadata_generation`, for the body fetch.
    preview_content_generation: u64,
    versions: ObjectVersionsState,
    bucket_details: BucketDetailsState,
    pending_upload: bool,
    pending_new_folder: bool,
    pending_object_action: Option<ObjectAction>,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for ObjectBrowserDocument {}

impl ObjectBrowserDocument {
    pub fn new(
        profile_id: Uuid,
        bucket: String,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let tree = ObjectTree::new(bucket.clone());

        let filter_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Filter this prefix…"));

        let filter_subscription = cx.subscribe_in(
            &filter_input,
            window,
            |this, input, event: &InputEvent, _window, cx| {
                if matches!(event, InputEvent::Change) {
                    let value = input.read(cx).value().to_string();
                    let prefix = this.tree.current_prefix.clone();

                    this.tree.set_filter(&prefix, value);
                    this.clamp_selection();
                    cx.notify();
                }
            },
        );

        let mut doc = Self {
            id: DocumentId::new(),
            title: format!("s3://{bucket}"),
            profile_id,
            bucket,
            app_state,
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            refresh_policy: RefreshPolicy::Manual,
            state: DocumentState::Loading,
            last_error: None,
            tree,
            last_operation: None,
            filter_input,
            focus_mode: ObjectBrowserFocusMode::Listing,
            preview_key: None,
            metadata: None,
            metadata_generation: 0,
            preview_content: PreviewContentState::Unavailable,
            preview_content_generation: 0,
            versions: ObjectVersionsState::Idle,
            bucket_details: BucketDetailsState::NotLoaded,
            pending_upload: false,
            pending_new_folder: false,
            pending_object_action: None,
            _subscriptions: vec![filter_subscription],
        };

        doc.expand_prefix(String::new(), cx);
        doc
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        self.state
    }

    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn tree(&self) -> &ObjectTree {
        &self.tree
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    pub fn profile_id(&self) -> Uuid {
        self.profile_id
    }

    pub fn refresh_policy(&self) -> RefreshPolicy {
        self.refresh_policy
    }

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
    }

    pub fn set_refresh_policy(&mut self, policy: RefreshPolicy, cx: &mut Context<Self>) {
        self.refresh_policy = policy;
        cx.notify();
    }

    pub fn active_context(&self) -> ContextId {
        ContextId::Results
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
        self.focus_mode = ObjectBrowserFocusMode::Listing;
        cx.notify();
    }

    /// Upload intent raised by the toolbar button, drained by the upload flow
    /// owner using the same `pending_*` + `take()` convention the sibling
    /// documents use for deferred modal opens.
    pub fn take_pending_upload(&mut self) -> bool {
        std::mem::take(&mut self.pending_upload)
    }

    /// Folder-creation intent raised by the toolbar button, drained by the
    /// create-folder flow owner.
    pub fn take_pending_new_folder(&mut self) -> bool {
        std::mem::take(&mut self.pending_new_folder)
    }

    // -- Listing ---------------------------------------------------------

    /// Rows currently rendered, in display order: the filtered entries of the
    /// current prefix level, or the flattened tree-mode walk when tree mode
    /// has produced rows.
    pub(super) fn visible_rows(&self) -> Vec<VisibleRow> {
        if self.tree.tree_mode.status == TreeModeStatus::Off {
            return self
                .tree
                .filtered_entries(&self.tree.current_prefix)
                .into_iter()
                .map(|entry| VisibleRow {
                    depth: 0,
                    parent_prefix: self.tree.current_prefix.clone(),
                    entry: entry.clone(),
                })
                .collect();
        }

        let filter = self
            .tree
            .level(&self.tree.current_prefix)
            .map(|level| level.filter.trim().to_lowercase())
            .unwrap_or_default();

        self.tree
            .tree_mode
            .rows
            .iter()
            .filter(|row| {
                filter.is_empty()
                    || row
                        .entry
                        .display_name(&row.parent_prefix)
                        .to_lowercase()
                        .contains(&filter)
            })
            .map(|row| VisibleRow {
                depth: row.depth,
                parent_prefix: row.parent_prefix.clone(),
                entry: row.entry.clone(),
            })
            .collect()
    }

    fn visible_node_ids(&self) -> Vec<ObjectTreeNodeId> {
        self.visible_rows()
            .iter()
            .map(|row| row.entry.node_id())
            .collect()
    }

    /// Drops the selection when the selected node is filtered out (or gone),
    /// falling back to the first visible row so the cursor is never orphaned.
    pub(super) fn clamp_selection(&mut self) {
        let visible = self.visible_node_ids();

        let still_visible = self
            .tree
            .selected
            .as_ref()
            .is_some_and(|selected| visible.iter().any(|candidate| candidate == selected));

        if !still_visible {
            self.tree.select(visible.first().cloned());
        }
    }

    pub(super) fn select_node(&mut self, node_id: ObjectTreeNodeId, cx: &mut Context<Self>) {
        self.tree.select(Some(node_id));
        self.focus_mode = ObjectBrowserFocusMode::Listing;
        cx.notify();
    }

    pub(super) fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        let visible = self.visible_node_ids();

        if visible.is_empty() {
            return;
        }

        let current = self
            .tree
            .selected
            .as_ref()
            .and_then(|selected| visible.iter().position(|candidate| candidate == selected));

        let next = match current {
            Some(index) => (index as isize + delta).clamp(0, visible.len() as isize - 1) as usize,
            None if delta >= 0 => 0,
            None => visible.len() - 1,
        };

        self.tree.select(visible.get(next).cloned());
        self.focus_mode = ObjectBrowserFocusMode::Listing;
        cx.notify();
    }

    fn select_edge(&mut self, last: bool, cx: &mut Context<Self>) {
        let visible = self.visible_node_ids();

        self.tree.select(if last {
            visible.last().cloned()
        } else {
            visible.first().cloned()
        });
        self.focus_mode = ObjectBrowserFocusMode::Listing;
        cx.notify();
    }

    // -- Navigation ------------------------------------------------------

    /// Moves the listing to `prefix`, loading its first page when that level
    /// has never been fetched, and syncing the filter box to the (per-level)
    /// filter stored for the destination.
    pub(super) fn navigate_to_prefix(
        &mut self,
        prefix: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.tree.navigate_into(prefix.clone());
        self.preview_key = None;
        self.focus_mode = ObjectBrowserFocusMode::Listing;

        let filter = self
            .tree
            .level(&prefix)
            .map(|level| level.filter.clone())
            .unwrap_or_default();
        self.filter_input
            .update(cx, |input, cx| input.set_value(&filter, window, cx));

        let needs_load = self
            .tree
            .level(&prefix)
            .is_none_or(|level| level.state == PrefixLoadState::NotLoaded);

        if needs_load {
            self.expand_prefix(prefix, cx);
        } else {
            self.clamp_selection();
            cx.notify();
        }
    }

    pub(super) fn navigate_up(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.tree.current_prefix.is_empty() {
            return;
        }

        self.tree.navigate_up();
        let parent = self.tree.current_prefix.clone();

        self.navigate_to_prefix(parent, window, cx);
    }

    /// Enter on a row: prefixes open as the new listing level, objects open
    /// the preview pane.
    pub(super) fn activate_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.tree.selected.clone() {
            Some(ObjectTreeNodeId::Prefix(prefix)) => self.navigate_to_prefix(prefix, window, cx),
            Some(ObjectTreeNodeId::Object(key)) => self.open_preview(key, cx),
            None => {}
        }
    }

    pub(super) fn open_preview(&mut self, key: String, cx: &mut Context<Self>) {
        self.preview_key = Some(key.clone());
        self.versions = ObjectVersionsState::Idle;
        // Drops the previous object's decoded bytes before the new metadata
        // request even starts.
        self.preview_content = PreviewContentState::Unavailable;
        self.focus_mode = ObjectBrowserFocusMode::Listing;

        self.ensure_bucket_details(cx);
        self.load_object_metadata(key, cx);
        cx.notify();
    }

    pub(super) fn close_preview(&mut self, cx: &mut Context<Self>) {
        self.preview_key = None;
        self.metadata = None;
        self.preview_content = PreviewContentState::Unavailable;
        self.versions = ObjectVersionsState::Idle;
        cx.notify();
    }

    /// Body state of the previewed object, for the preview pane.
    pub(super) fn preview_content(&self) -> &PreviewContentState {
        &self.preview_content
    }

    /// Presentation of the previewed object, derived from its metadata. `None`
    /// until `head_object` resolves — the kind depends on the reported content
    /// type, so it cannot be guessed from the key alone.
    pub(super) fn preview_kind(&self) -> Option<PreviewKind> {
        let ObjectMetadataState::Loaded { metadata, .. } = self.metadata.as_ref()? else {
            return None;
        };

        Some(detect_preview_kind(
            metadata.content_type.as_deref(),
            &metadata.key,
        ))
    }

    /// Copies the previewed object's canonical `s3://bucket/key` URI. Acts
    /// immediately — unlike the other preview actions, nothing downstream is
    /// needed to make it useful.
    pub(super) fn copy_object_uri(&mut self, key: &str, cx: &mut Context<Self>) {
        let uri = format!("s3://{}/{key}", self.bucket);

        cx.write_to_clipboard(ClipboardItem::new_string(uri.clone()));
        dbflux_ui_base::toast::Toast::success(format!("Copied {uri}"))
            .meta_right(dbflux_ui_base::toast::now_hms())
            .push(cx);
    }

    pub(super) fn request_object_action(&mut self, action: ObjectAction, cx: &mut Context<Self>) {
        self.pending_object_action = Some(action);
        cx.notify();
    }

    /// Object-level intent raised by the preview action bar, drained by the
    /// download / presign / delete flow owners.
    pub fn take_pending_object_action(&mut self) -> Option<ObjectAction> {
        self.pending_object_action.take()
    }

    /// Space on a row: objects toggle the preview pane, prefixes fall back to
    /// opening the level (there is nothing to preview for a folder).
    fn toggle_preview(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.tree.selected.clone() {
            Some(ObjectTreeNodeId::Object(key)) if self.preview_key.as_deref() == Some(&key) => {
                self.close_preview(cx)
            }
            Some(ObjectTreeNodeId::Object(key)) => self.open_preview(key, cx),
            Some(ObjectTreeNodeId::Prefix(prefix)) => self.navigate_to_prefix(prefix, window, cx),
            None => {}
        }
    }

    pub(super) fn request_upload(&mut self, cx: &mut Context<Self>) {
        self.pending_upload = true;
        cx.notify();
    }

    pub(super) fn request_new_folder(&mut self, cx: &mut Context<Self>) {
        self.pending_new_folder = true;
        cx.notify();
    }

    #[cfg(test)]
    pub(crate) fn apply_page_for_test(
        &mut self,
        prefix: &str,
        page: dbflux_core::ObjectListingPage,
    ) {
        self.tree.apply_page(prefix, page);
    }

    #[cfg(test)]
    pub(crate) fn set_last_operation_for_test(&mut self, timing: OperationTiming) {
        self.last_operation = Some(timing);
    }

    #[cfg(test)]
    pub(crate) fn preview_key_for_test(&self) -> Option<&str> {
        self.preview_key.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn metadata_for_test(&self) -> Option<&ObjectMetadataState> {
        self.metadata.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn preview_content_for_test(&self) -> &PreviewContentState {
        &self.preview_content
    }

    #[cfg(test)]
    pub(crate) fn apply_preview_content_for_test(
        &mut self,
        key: &str,
        state: PreviewContentState,
        cx: &mut Context<Self>,
    ) {
        let generation = self.preview_content_generation;
        self.apply_preview_content(generation, key.to_string(), state, cx);
    }

    #[cfg(test)]
    pub(crate) fn apply_metadata_for_test(
        &mut self,
        metadata: dbflux_core::ObjectMetadata,
        cx: &mut Context<Self>,
    ) {
        let generation = self.metadata_generation;
        self.apply_object_metadata(generation, metadata.key.clone(), Ok(metadata), cx);
    }

    fn focus_filter(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = ObjectBrowserFocusMode::Filter;
        self.filter_input
            .update(cx, |input, cx| input.focus(window, cx));
        cx.notify();
    }

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::SelectNext => {
                self.move_selection(1, cx);
                true
            }
            Command::SelectPrev => {
                self.move_selection(-1, cx);
                true
            }
            Command::SelectFirst => {
                self.select_edge(false, cx);
                true
            }
            Command::SelectLast => {
                self.select_edge(true, cx);
                true
            }
            Command::Execute | Command::ColumnRight => {
                self.activate_selected(window, cx);
                true
            }
            Command::ExpandCollapse => {
                self.toggle_preview(window, cx);
                true
            }
            Command::ColumnLeft => {
                if self.preview_key.is_some() {
                    self.close_preview(cx);
                } else {
                    self.navigate_up(window, cx);
                }
                true
            }
            Command::ResultsAddRow => {
                self.request_new_folder(cx);
                true
            }
            Command::RefreshSchema => {
                self.reload_current_prefix(cx);
                true
            }
            Command::FocusSearch | Command::FocusToolbar => {
                self.focus_filter(window, cx);
                true
            }
            Command::Cancel => {
                if self.preview_key.is_some() {
                    self.close_preview(cx);
                }

                self.focus_mode = ObjectBrowserFocusMode::Listing;
                self.focus_handle.focus(window);
                cx.notify();
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    // Deliberately narrow imports: `use super::*` would pull in the module's
    // `gpui::*` glob, whose `test` attribute macro would shadow the plain
    // `#[test]` attribute.
    use super::{
        ImagePreview, ObjectAction, ObjectBrowserDocument, ObjectMetadataState, ObjectTreeNodeId,
        PreviewContentState, PreviewGate, PreviewKind,
    };
    use crate::buckets_table::OperationTiming;
    use dbflux_core::{ObjectListingPage, ObjectMetadata, ObjectSummary};

    fn object_metadata(key: &str, size_bytes: u64, storage_class: Option<&str>) -> ObjectMetadata {
        typed_object_metadata(key, size_bytes, storage_class, Some("text/plain"))
    }

    fn typed_object_metadata(
        key: &str,
        size_bytes: u64,
        storage_class: Option<&str>,
        content_type: Option<&str>,
    ) -> ObjectMetadata {
        ObjectMetadata {
            key: key.to_string(),
            size_bytes,
            content_type: content_type.map(|value| value.to_string()),
            last_modified: None,
            etag: Some("\"etag\"".to_string()),
            storage_class: storage_class.map(|class| class.to_string()),
            encryption: Some("AES256".to_string()),
            version_count: None,
        }
    }

    fn image_preview() -> PreviewContentState {
        PreviewContentState::Image(Box::new(ImagePreview {
            image: std::sync::Arc::new(gpui::Image::from_bytes(
                gpui::ImageFormat::Png,
                vec![1, 2, 3],
            )),
            width: 640,
            height: 480,
            byte_len: 3,
        }))
    }

    fn page(prefixes: &[&str], objects: &[&str]) -> ObjectListingPage {
        ObjectListingPage {
            objects: objects
                .iter()
                .map(|key| ObjectSummary {
                    key: key.to_string(),
                    size_bytes: 1024,
                    storage_class: None,
                    last_modified: None,
                })
                .collect(),
            common_prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            next_continuation_token: None,
        }
    }

    /// T24: keyboard navigation walks the visible rows of the current level,
    /// prefixes first, and clamps at both ends.
    #[gpui::test]
    fn selection_walks_the_visible_rows(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.apply_page_for_test("", page(&["logs/"], &["a.txt", "b.txt"]));
                doc.move_selection(1, cx);
            });
        });

        cx.update(|cx| {
            assert_eq!(
                doc.read(cx).tree.selected,
                Some(ObjectTreeNodeId::Prefix("logs/".to_string()))
            );
        });

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.move_selection(5, cx);
            });
        });

        cx.update(|cx| {
            assert_eq!(
                doc.read(cx).tree.selected,
                Some(ObjectTreeNodeId::Object("b.txt".to_string()))
            );
        });
    }

    /// T24: the per-prefix filter narrows the rendered rows and drags the
    /// cursor onto a row that is still visible.
    #[gpui::test]
    fn filter_narrows_the_rows_and_reclamps_the_selection(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.apply_page_for_test("", page(&[], &["alpha.txt", "beta.txt"]));
                doc.select_node(ObjectTreeNodeId::Object("alpha.txt".to_string()), cx);
                doc.tree.set_filter("", "beta".to_string());
                doc.clamp_selection();
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            assert_eq!(doc.visible_rows().len(), 1);
            assert_eq!(
                doc.tree.selected,
                Some(ObjectTreeNodeId::Object("beta.txt".to_string()))
            );
        });
    }

    /// T24: previewing an object opens the pane, and previewing the same
    /// object again closes it.
    #[gpui::test]
    fn preview_opens_and_closes_for_the_selected_object(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("logs/a.txt".to_string(), cx);
            });
        });

        cx.update(|cx| {
            assert_eq!(doc.read(cx).preview_key_for_test(), Some("logs/a.txt"));
        });

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.close_preview(cx);
            });
        });

        cx.update(|cx| {
            assert_eq!(doc.read(cx).preview_key_for_test(), None);
        });
    }

    /// T25: the document contributes the bucket path, the key count of the
    /// current level, and the last object-store call's timing.
    #[gpui::test]
    fn status_segments_report_path_key_count_and_timing(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                doc.apply_page_for_test("", page(&["logs/"], &["a.txt"]));
                doc.set_last_operation_for_test(OperationTiming {
                    label: "ListObjectsV2",
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

            assert!(texts.contains(&"s3://my-bucket/".to_string()));
            assert!(texts.contains(&"2 keys".to_string()));
            assert!(texts.contains(&"ListObjectsV2 · 188 ms".to_string()));
        });
    }

    /// T26/T28: metadata that resolves for the previewed object lands in the
    /// panel with the gate derived from the configured preview limit.
    #[gpui::test]
    fn metadata_lands_with_a_preview_gate(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("logs/a.txt".to_string(), cx);
                doc.apply_metadata_for_test(
                    object_metadata("logs/a.txt", 1024, Some("STANDARD")),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let state = doc
                .read(cx)
                .metadata_for_test()
                .cloned()
                .expect("metadata state");

            match state {
                ObjectMetadataState::Loaded { metadata, gate } => {
                    assert_eq!(metadata.key, "logs/a.txt");
                    assert_eq!(gate, PreviewGate::Allowed);
                }
                other => panic!("expected loaded metadata, got {other:?}"),
            }
        });
    }

    /// T26: an archived object never becomes previewable, whatever its size.
    #[gpui::test]
    fn archived_objects_are_gated_out_of_preview(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("cold/backup.tar".to_string(), cx);
                doc.apply_metadata_for_test(
                    object_metadata("cold/backup.tar", 8, Some("GLACIER")),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let state = doc
                .read(cx)
                .metadata_for_test()
                .cloned()
                .expect("metadata state");

            assert!(matches!(
                state,
                ObjectMetadataState::Loaded {
                    gate: PreviewGate::Archived,
                    ..
                }
            ));
        });
    }

    /// T26: metadata for a superseded selection never overwrites the panel of
    /// the object the user has since moved to.
    #[gpui::test]
    fn stale_metadata_is_discarded(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("logs/a.txt".to_string(), cx);
                doc.open_preview("logs/b.txt".to_string(), cx);
                doc.apply_metadata_for_test(
                    object_metadata("logs/a.txt", 1024, Some("STANDARD")),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            assert!(
                !matches!(
                    doc.read(cx).metadata_for_test(),
                    Some(ObjectMetadataState::Loaded { .. })
                ),
                "metadata of a superseded selection must not reach the panel"
            );
        });
    }

    /// T27: the delete action only records the intent; its flow owner drains
    /// it exactly once.
    #[gpui::test]
    fn preview_actions_are_recorded_as_drainable_intents(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.request_object_action(
                    ObjectAction::Delete {
                        key: "logs/a.txt".to_string(),
                    },
                    cx,
                );
            });
        });

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                assert_eq!(
                    doc.take_pending_object_action(),
                    Some(ObjectAction::Delete {
                        key: "logs/a.txt".to_string()
                    })
                );
                assert_eq!(doc.take_pending_object_action(), None);
            });
        });
    }

    /// T29: an image within the preview limit triggers a body fetch as soon as
    /// its metadata resolves. Without a live connection the fetch fails
    /// immediately, which is exactly the degradation path the pane must show.
    #[gpui::test]
    fn image_metadata_starts_a_body_fetch(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("shots/hero.png".to_string(), cx);
                doc.apply_metadata_for_test(
                    typed_object_metadata(
                        "shots/hero.png",
                        2048,
                        Some("STANDARD"),
                        Some("image/png"),
                    ),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);

            assert_eq!(
                doc.preview_kind(),
                Some(PreviewKind::Image(gpui::ImageFormat::Png))
            );
            assert!(
                matches!(
                    doc.preview_content_for_test(),
                    PreviewContentState::Failed(_)
                ),
                "an image body fetch must be attempted and its failure surfaced"
            );
        });
    }

    /// T32: a PDF is never fetched — it is presented as metadata plus the
    /// download / open-externally actions.
    #[gpui::test]
    fn pdf_objects_never_fetch_their_body(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("reports/q1.pdf".to_string(), cx);
                doc.apply_metadata_for_test(
                    typed_object_metadata(
                        "reports/q1.pdf",
                        2048,
                        Some("STANDARD"),
                        Some("application/pdf"),
                    ),
                    cx,
                );
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);

            assert_eq!(doc.preview_kind(), Some(PreviewKind::Pdf));
            assert_eq!(
                doc.preview_content_for_test(),
                &PreviewContentState::Unavailable
            );
        });
    }

    /// T29: the decoded image belongs to one selection only — moving to another
    /// object drops it instead of letting previews accumulate.
    #[gpui::test]
    fn selecting_another_object_drops_the_cached_image(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("shots/hero.png".to_string(), cx);
                doc.apply_preview_content_for_test("shots/hero.png", image_preview(), cx);
            });
        });

        cx.update(|cx| {
            assert!(matches!(
                doc.read(cx).preview_content_for_test(),
                PreviewContentState::Image(_)
            ));
        });

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("shots/other.bin".to_string(), cx);
            });
        });

        cx.update(|cx| {
            assert_eq!(
                doc.read(cx).preview_content_for_test(),
                &PreviewContentState::Unavailable
            );
        });
    }

    /// T29: a body that arrives for a superseded selection never reaches the
    /// pane, mirroring the metadata staleness guard.
    #[gpui::test]
    fn stale_preview_content_is_discarded(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.open_preview("shots/hero.png".to_string(), cx);
                doc.open_preview("shots/second.png".to_string(), cx);
                doc.apply_preview_content_for_test("shots/hero.png", image_preview(), cx);
            });
        });

        cx.update(|cx| {
            assert!(
                !matches!(
                    doc.read(cx).preview_content_for_test(),
                    PreviewContentState::Image(_)
                ),
                "the body of a superseded selection must not reach the pane"
            );
        });
    }

    fn new_test_entity(cx: &mut gpui::TestAppContext) -> gpui::Entity<ObjectBrowserDocument> {
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
            ObjectBrowserDocument::new(profile_id, "my-bucket".to_string(), app_state, window, cx)
        });

        doc
    }
}

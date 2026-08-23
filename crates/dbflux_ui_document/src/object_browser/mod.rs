mod context_menu;
mod create_folder;
mod data;
mod delete;
pub mod delete_prefix;
mod delete_prefix_modal;
pub mod editor;
pub mod metadata;
mod pane;
pub mod presign;
mod preview;
pub mod preview_content;
mod rename;
mod render;
mod transfer;
pub mod tree;
mod upload;

pub use create_folder::NewFolderState;
pub use delete_prefix::{
    DeletePrefixConfirmState, DeletePrefixProbeState, PrefixDeleteProbe, PrefixDeleteProbeOutcome,
};
pub use presign::{PresignExpiry, PresignMethodChoice, PresignState, PresignUrlState};
pub use rename::RenameObjectState;

pub use crate::object_text::{LineEnding, TextBody, decode_text_body};
pub use metadata::{ObjectMetadataState, ObjectVersionsState, PreviewGate, evaluate_preview_gate};
pub use preview_content::{ImagePreview, PreviewContentState, PreviewKind, detect_preview_kind};
pub use tree::{ObjectTree, ObjectTreeEntry, ObjectTreeNodeId, PrefixLoadState};

use context_menu::ObjectContextMenu;

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use crate::buckets_table::{BucketDetailsState, OperationTiming};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_components::primitives::TypeToConfirm;
use dbflux_core::RefreshPolicy;
use dbflux_ui_base::AppStateEntity;
use editor::{GuardedNavigation, ObjectEditor, PendingTextBody};
use gpui::*;
use uuid::Uuid;

pub use delete::PendingObjectDelete;

/// Which part of the document currently owns keyboard input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectBrowserFocusMode {
    Listing,
    Filter,
    /// The preview pane's inline text editor owns the keyboard.
    Editor,
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

/// One rendered entry row, carrying the prefix depth (0 outside tree mode,
/// the nesting level of the node otherwise) that drives indentation.
#[derive(Clone, Debug, PartialEq)]
pub struct VisibleRow {
    pub depth: usize,
    pub parent_prefix: String,
    pub entry: ObjectTreeEntry,
}

/// One rendered listing row: either an entry, or a per-node "load more"
/// continuation control. In tree mode every expanded node paginates
/// independently, so its "load more" row is interleaved right after that
/// node's own children instead of only ever appearing once at the bottom.
#[derive(Clone, Debug, PartialEq)]
pub enum ListingRow {
    Entry(VisibleRow),
    LoadMore {
        depth: usize,
        prefix: String,
        loading: bool,
    },
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
    /// User-chosen preview-pane width; `None` uses the mode's preferred width.
    preview_custom_width: Option<Pixels>,
    preview_resize_start: Option<(Pixels, Pixels)>,
    /// Editable buffer for the previewed text object, when there is one.
    editor: Option<ObjectEditor>,
    /// Body decoded by the fetch and waiting for a render pass to turn it into
    /// an editor — building the `InputState` needs a `Window`.
    pending_text_body: Option<PendingTextBody>,
    /// Navigation parked behind the unsaved-edits confirmation.
    pending_navigation: Option<GuardedNavigation>,
    /// Navigation cleared by a successful save, waiting for a render pass to
    /// run it (navigating between prefixes needs a `Window`).
    resume_navigation: Option<GuardedNavigation>,
    versions: ObjectVersionsState,
    bucket_details: BucketDetailsState,
    pending_upload: bool,
    pending_new_folder: bool,
    /// Prefix the pending folder creation targets, when the intent came from
    /// a folder row rather than the toolbar.
    pending_new_folder_parent: Option<String>,
    /// New Folder overlay state (name input + submission), built on the
    /// render pass that drains `pending_new_folder`.
    new_folder: Option<NewFolderState>,
    /// Rename overlay state (name input + submission), built when a rename is
    /// requested for a selected object row.
    rename_object: Option<RenameObjectState>,
    pending_object_action: Option<ObjectAction>,
    /// Object staged for the single-delete confirmation overlay.
    pending_object_delete: Option<PendingObjectDelete>,
    /// Recursive-delete confirmation state (probe + type-to-confirm phrase).
    delete_prefix_confirm: Option<DeletePrefixConfirmState>,
    /// Type-to-confirm widget backing the recursive-delete modal. Built on the
    /// first render after the modal opens (`InputState` needs a `Window`) and
    /// dropped with the modal, because the expected phrase is fixed at
    /// construction and changes with every target.
    delete_prefix_input: Option<Entity<TypeToConfirm>>,
    /// Presigned-URL modal state (method / expiry / generated URL).
    presign: Option<PresignState>,
    /// Row context menu raised by a right click, with the row it targets.
    context_menu: Option<ObjectContextMenu>,
    /// Key staged to open in its own editor tab, drained by the workspace.
    pending_open_object_editor: Option<String>,
    /// Document origin in window coordinates, captured by a canvas on every
    /// render so a click position can be placed inside the document.
    panel_origin: Point<Pixels>,
    /// Scroll state of the virtualized listing, shared with keyboard
    /// navigation so the selected row is scrolled into view.
    pub(super) listing_scroll: UniformListScrollHandle,
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

        let filter_input = cx.new(|cx| {
            InputState::new(window, cx).placeholder(dbflux_i18n::t!(
                "document.object_browser.toolbar.filter_prefix_placeholder"
            ))
        });

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
            preview_custom_width: None,
            preview_resize_start: None,
            editor: None,
            pending_text_body: None,
            pending_navigation: None,
            resume_navigation: None,
            versions: ObjectVersionsState::Idle,
            bucket_details: BucketDetailsState::NotLoaded,
            pending_upload: false,
            pending_new_folder: false,
            pending_new_folder_parent: None,
            new_folder: None,
            rename_object: None,
            pending_object_action: None,
            pending_object_delete: None,
            delete_prefix_confirm: None,
            delete_prefix_input: None,
            presign: None,
            context_menu: None,
            pending_open_object_editor: None,
            panel_origin: Point::default(),
            listing_scroll: UniformListScrollHandle::new(),
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

    /// State reported to the tab bar. Unsaved editor edits win over every
    /// other state: a failed listing is visible in the document itself, while
    /// the dirty dot is the only place the pending edit is advertised.
    pub fn state(&self) -> DocumentState {
        if self.editor_is_dirty() {
            return DocumentState::Modified;
        }

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

    /// Always closable: unsaved edits do not block the close, they route it
    /// through the workspace's unsaved-changes modal (fed by
    /// `change_summary`).
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

    /// The inline editor owns the keymap while it is focused, so typing does
    /// not reach the listing's single-letter navigation commands.
    pub fn active_context(&self) -> ContextId {
        if self.pending_navigation.is_some() {
            return ContextId::ConfirmModal;
        }

        // The row context menu owns the keyboard while it is open, so the
        // listing does not move under the row the user right-clicked.
        if self.context_menu.is_some() {
            return ContextId::ContextMenu;
        }

        // The recursive-delete modal is typed into, so its input must keep
        // every letter the listing would otherwise read as a command.
        if self.delete_prefix_confirm.is_some() {
            return ContextId::TextInput;
        }

        if self.presign.is_some() {
            return ContextId::ConfirmModal;
        }

        // The New Folder and rename overlays are typed into, same as the
        // recursive-delete modal's type-to-confirm input.
        if self.new_folder.is_some() || self.rename_object.is_some() {
            return ContextId::TextInput;
        }

        match self.focus_mode {
            // Filter is a text input, same as Editor — routing it to Results
            // would let single-letter listing commands (Delete, Rename,
            // ExpandCollapse, ...) fire while the user is typing a prefix
            // filter instead of reaching the input.
            ObjectBrowserFocusMode::Editor | ObjectBrowserFocusMode::Filter => ContextId::TextInput,
            ObjectBrowserFocusMode::Listing => ContextId::Results,
        }
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

    /// Stages `key` to open in its own editor tab. The workspace drains this
    /// through the generic `take_pending_open_object_editor` pane helper, so
    /// the browser never opens tabs itself.
    pub(super) fn request_open_object_editor(&mut self, key: String, cx: &mut Context<Self>) {
        self.pending_open_object_editor = Some(key);
        cx.notify();
    }

    /// Open-in-editor intent raised by the preview header or the row context
    /// menu, drained by the workspace.
    pub fn take_pending_open_object_editor(&mut self) -> Option<String> {
        self.pending_open_object_editor.take()
    }

    /// Refreshes the metadata panel for `key` when it is the object being
    /// previewed. Called after a standalone editor tab saves that object, so
    /// the size, last-modified and ETag rows stop showing pre-save values.
    pub fn refresh_previewed_object(&mut self, key: &str, cx: &mut Context<Self>) {
        if self.preview_key.as_deref() != Some(key) {
            return;
        }

        self.load_object_metadata(key.to_string(), cx);
    }

    /// Prefix the pending folder creation targets, when it is not the level
    /// being listed (the listing's context menu targets a specific folder).
    pub(super) fn take_pending_new_folder_parent(&mut self) -> Option<String> {
        self.pending_new_folder_parent.take()
    }

    // -- Listing ---------------------------------------------------------

    /// Rows currently rendered, in display order.
    ///
    /// Outside tree mode: the filtered entries of the current prefix level,
    /// plus that level's own "load more" row when it has a continuation
    /// token.
    ///
    /// In tree mode: the current prefix's entries, with each expanded
    /// prefix's already-loaded children recursively spliced in right after
    /// it (never fetched here — only nodes the user has actually expanded
    /// contribute rows). Every expanded node gets its own "load more" row
    /// immediately after its children when it has more to page in.
    pub(super) fn visible_rows(&self) -> Vec<ListingRow> {
        if !self.tree.is_tree_mode() {
            let prefix = self.tree.current_prefix.clone();
            let mut rows: Vec<ListingRow> = self
                .tree
                .filtered_entries(&prefix)
                .into_iter()
                .map(|entry| {
                    ListingRow::Entry(VisibleRow {
                        depth: 0,
                        parent_prefix: prefix.clone(),
                        entry: entry.clone(),
                    })
                })
                .collect();

            if let Some(level) = self.tree.level(&prefix)
                && level.has_more()
            {
                rows.push(ListingRow::LoadMore {
                    depth: 0,
                    prefix: prefix.clone(),
                    loading: level.state == PrefixLoadState::LoadingMore,
                });
            }

            return rows;
        }

        self.flatten_tree_rows(&self.tree.current_prefix, 0)
    }

    /// Recursively splices an expanded node's cached children into the
    /// flattened tree-mode listing. Never touches the network — a node with
    /// no cached level (never expanded, or expanded but still loading)
    /// simply contributes nothing beyond its own row.
    fn flatten_tree_rows(&self, prefix: &str, depth: usize) -> Vec<ListingRow> {
        let mut rows = Vec::new();

        for entry in self.tree.filtered_entries(prefix) {
            let entry = entry.clone();
            let expand_child = match &entry {
                ObjectTreeEntry::Prefix(child_prefix) if self.tree.is_expanded(child_prefix) => {
                    Some(child_prefix.clone())
                }
                _ => None,
            };

            rows.push(ListingRow::Entry(VisibleRow {
                depth,
                parent_prefix: prefix.to_string(),
                entry,
            }));

            if let Some(child_prefix) = expand_child {
                rows.extend(self.flatten_tree_rows(&child_prefix, depth + 1));
            }
        }

        if let Some(level) = self.tree.level(prefix)
            && level.has_more()
        {
            rows.push(ListingRow::LoadMore {
                depth,
                prefix: prefix.to_string(),
                loading: level.state == PrefixLoadState::LoadingMore,
            });
        }

        rows
    }

    fn visible_node_ids(&self) -> Vec<ObjectTreeNodeId> {
        self.visible_rows()
            .into_iter()
            .filter_map(|row| match row {
                ListingRow::Entry(row) => Some(row.entry.node_id()),
                ListingRow::LoadMore { .. } => None,
            })
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
        self.scroll_selected_into_view();
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
        self.scroll_selected_into_view();
        cx.notify();
    }

    /// Scrolls the virtualized listing the minimum amount needed to bring the
    /// selected row into view (a no-op when it is already fully visible).
    fn scroll_selected_into_view(&self) {
        let Some(selected) = self.tree.selected.as_ref() else {
            return;
        };

        let index = self.visible_rows().iter().position(|row| match row {
            ListingRow::Entry(visible) => &visible.entry.node_id() == selected,
            ListingRow::LoadMore { .. } => false,
        });

        if let Some(index) = index {
            self.listing_scroll
                .scroll_to_item(index, ScrollStrategy::Top);
        }
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
        if self.guard_navigation(GuardedNavigation::NavigateToPrefix(prefix.clone()), cx) {
            return;
        }

        self.navigate_to_prefix_now(prefix, window, cx);
    }

    pub(super) fn navigate_to_prefix_now(
        &mut self,
        prefix: String,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.drop_editor();
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
        // Resolved without moving the tree first: the unsaved-edits guard can
        // still refuse the navigation, and the listing must not have shifted
        // level in the meantime.
        let Some(parent) = self.tree.parent_prefix() else {
            return;
        };

        self.navigate_to_prefix(parent, window, cx);
    }

    /// Enter (or right arrow) on a row: in tree mode a prefix expands in
    /// place (loading its first page of children if it never has); outside
    /// tree mode a prefix instead navigates the listing into it. Objects
    /// always open the preview pane either way.
    pub(super) fn activate_selected(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        match self.tree.selected.clone() {
            Some(ObjectTreeNodeId::Prefix(prefix)) => {
                if self.tree.is_tree_mode() {
                    self.expand_tree_node(prefix, cx);
                } else {
                    self.navigate_to_prefix(prefix, window, cx);
                }
            }
            Some(ObjectTreeNodeId::Object(key)) => self.open_preview(key, cx),
            None => {}
        }
    }

    /// Left arrow in tree mode: collapses the selected node if it is an
    /// expanded prefix. Returns `false` when there was nothing to collapse,
    /// so the caller falls back to the existing close-preview/navigate-up
    /// behavior.
    pub(super) fn collapse_selected_tree_node(&mut self, cx: &mut Context<Self>) -> bool {
        let Some(ObjectTreeNodeId::Prefix(prefix)) = self.tree.selected.clone() else {
            return false;
        };

        if !self.tree.is_expanded(&prefix) {
            return false;
        }

        self.collapse_tree_node(&prefix, cx);
        true
    }

    pub(super) fn open_preview(&mut self, key: String, cx: &mut Context<Self>) {
        if self.guard_navigation(GuardedNavigation::OpenPreview(key.clone()), cx) {
            return;
        }

        self.open_preview_now(key, cx);
    }

    pub(super) fn open_preview_now(&mut self, key: String, cx: &mut Context<Self>) {
        self.drop_editor();
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
        if self.guard_navigation(GuardedNavigation::ClosePreview, cx) {
            return;
        }

        self.close_preview_now(cx);
    }

    pub(super) fn close_preview_now(&mut self, cx: &mut Context<Self>) {
        self.drop_editor();
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
        dbflux_ui_base::toast::Toast::success(crate::labels::object_browser_copied_uri_toast(&uri))
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

    /// Same intent, for a folder that is not the level being listed.
    pub(super) fn request_new_folder_in(&mut self, prefix: String, cx: &mut Context<Self>) {
        self.pending_new_folder_parent = Some(prefix);
        self.request_new_folder(cx);
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

    /// Sets `focus_mode` directly, bypassing `focus_filter`'s
    /// `InputState::focus` call — that call panics under `TestAppContext`
    /// (see the `Root::read` gotcha documented across this change's other
    /// overlay tests), so this is the only way to exercise
    /// `active_context()`'s `Filter` branch in a unit test.
    #[cfg(test)]
    pub(crate) fn set_focus_mode_for_test(&mut self, mode: ObjectBrowserFocusMode) {
        self.focus_mode = mode;
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
    pub(crate) fn install_editor_for_test(
        &mut self,
        key: &str,
        text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.install_text_editor(
            PendingTextBody {
                key: key.to_string(),
                body: crate::object_text::TextBody {
                    text: text.to_string(),
                    line_ending: crate::object_text::LineEnding::Lf,
                    byte_len: text.len() as u64,
                },
                content_type: Some("text/plain".to_string()),
            },
            window,
            cx,
        );
    }

    #[cfg(test)]
    pub(crate) fn type_into_editor_for_test(
        &mut self,
        text: &str,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let Some(input) = self.editor.as_ref().map(|editor| editor.input.clone()) else {
            return;
        };

        // A real edit, not `set_value`: the component replaces text silently
        // in `set_value`, so only this path emits the `Change` the dirty
        // tracking listens for.
        let text = text.to_string();
        input.update(cx, |state, cx| {
            state.replace_text_in_range(None, &text, window, cx)
        });
    }

    #[cfg(test)]
    pub(in crate::object_browser) fn pending_navigation_for_test(
        &self,
    ) -> Option<&GuardedNavigation> {
        self.pending_navigation.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn editor_text_for_test(&self, cx: &App) -> Option<String> {
        self.editor
            .as_ref()
            .map(|editor| editor.input.read(cx).value().to_string())
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
        // While the unsaved-edits confirmation is up it owns every key: the
        // listing must not move under a decision the user has not made yet.
        if self.pending_navigation.is_some() {
            if cmd == Command::Cancel {
                self.cancel_guarded_navigation(cx);
            }

            return true;
        }

        // Same for the row context menu, which navigates and executes with
        // its own command set.
        if self.context_menu.is_some() {
            return self.dispatch_menu_command(cmd, window, cx);
        }

        // The recursive-delete modal owns the keyboard while it is up: Execute
        // deletes (only once the typed phrase matches), Cancel dismisses, and
        // everything else belongs to the type-to-confirm input.
        if self.delete_prefix_confirm.is_some() {
            return match cmd {
                Command::Execute => {
                    self.confirm_delete_prefix(cx);
                    true
                }
                Command::Cancel => {
                    self.close_delete_prefix_confirm(cx);
                    true
                }
                _ => false,
            };
        }

        // The presign modal has no text input: Execute copies the generated
        // URL, Cancel dismisses.
        if self.presign.is_some() {
            return match cmd {
                Command::Execute => {
                    self.copy_presigned_url(cx);
                    true
                }
                Command::Cancel => {
                    self.close_presign(cx);
                    true
                }
                _ => true,
            };
        }

        // Same for the single-delete confirmation: Execute confirms, Cancel
        // dismisses, everything else is swallowed.
        if self.pending_object_delete.is_some() {
            return match cmd {
                Command::Execute => {
                    self.confirm_delete_object(cx);
                    true
                }
                Command::Cancel => {
                    self.cancel_delete_object(cx);
                    true
                }
                _ => true,
            };
        }

        // Same for the New Folder and rename overlays: Execute submits (only
        // once the name validates), Cancel dismisses, and everything else
        // belongs to the name input.
        if self.new_folder.is_some() {
            return match cmd {
                Command::Execute => {
                    self.submit_new_folder(cx);
                    true
                }
                Command::Cancel => {
                    self.close_new_folder(cx);
                    true
                }
                _ => false,
            };
        }

        if self.rename_object.is_some() {
            return match cmd {
                Command::Execute => {
                    self.submit_rename_object(cx);
                    true
                }
                Command::Cancel => {
                    self.close_rename_object(cx);
                    true
                }
                _ => false,
            };
        }

        // Save works from anywhere in the document while a buffer is dirty:
        // Ctrl/Cmd+S in the editor, and the unsaved-changes modal's save path
        // on tab close (`SaveFileAs`).
        if matches!(cmd, Command::SaveQuery | Command::SaveFileAs) {
            if self.editor.is_none() {
                return false;
            }

            self.save_object_edits(cx);
            return true;
        }

        // Everything below drives the listing; the editor must keep its keys.
        if self.focus_mode == ObjectBrowserFocusMode::Editor {
            // Escape returns to the listing, but only while the buffer itself
            // holds focus. The editor component's find panel is a child input
            // that takes focus and closes on its own Escape, so swallowing the
            // key here would leave the panel open and unreachable.
            if cmd == Command::Cancel && self.editor_input_is_focused(window, cx) {
                self.focus_mode = ObjectBrowserFocusMode::Listing;
                self.focus_handle.focus(window);
                cx.notify();
                return true;
            }

            return false;
        }

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
                } else if !self.collapse_selected_tree_node(cx) {
                    self.navigate_up(window, cx);
                }
                true
            }
            Command::ResultsAddRow => {
                self.request_new_folder(cx);
                true
            }
            Command::Delete => {
                match self.tree.selected.clone() {
                    Some(ObjectTreeNodeId::Object(key)) => self.request_delete_object(key, cx),
                    Some(ObjectTreeNodeId::Prefix(prefix)) => {
                        self.request_delete_prefix(prefix, cx)
                    }
                    None => {}
                }
                true
            }
            // Rename is object-only — renaming a prefix would mean recursively
            // re-keying every object under it, which is out of scope.
            Command::Rename => {
                if let Some(ObjectTreeNodeId::Object(key)) = self.tree.selected.clone() {
                    self.request_rename_object(key, window, cx);
                }
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
        PreviewContentState, PreviewGate, PreviewKind, context_menu,
    };
    use crate::buckets_table::OperationTiming;
    use crate::types::DocumentState;
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
            dimensions: Some((640, 480)),
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

    /// Remediation: toggling tree mode never fetches anything — it is a pure
    /// presentation flip over whatever the current level already has loaded.
    #[gpui::test]
    fn toggling_tree_mode_is_instant_and_keeps_the_cache(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.apply_page_for_test("", page(&["logs/"], &["a.txt"]));
                doc.toggle_tree_mode(cx);
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            assert!(doc.tree().is_tree_mode());
            assert_eq!(doc.visible_rows().len(), 2);
        });

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.toggle_tree_mode(cx);
            });
        });

        cx.update(|cx| {
            let doc = doc.read(cx);
            assert!(!doc.tree().is_tree_mode());
            assert_eq!(doc.visible_rows().len(), 2);
        });
    }

    /// Remediation: in tree mode, `Command::ColumnRight`/`Execute` on a
    /// selected prefix expands it in place — it never navigates the listing
    /// into it, and it never loads any other node.
    #[gpui::test]
    fn tree_mode_expand_never_touches_a_sibling(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.apply_page_for_test("", page(&["logs/", "assets/"], &[]));
            doc.toggle_tree_mode(cx);
            doc.select_node(ObjectTreeNodeId::Prefix("logs/".to_string()), cx);
            doc.dispatch_command(dbflux_app::keymap::Command::ColumnRight, window, cx);
        });

        doc.update(window, |doc, _cx| {
            assert_eq!(doc.tree.current_prefix, "", "expand must not navigate");
            assert!(doc.tree.is_expanded("logs/"));
            assert!(!doc.tree.is_expanded("assets/"));
        });
    }

    /// Remediation: collapsing an expanded node hides its children from the
    /// flattened listing but keeps them cached — re-expanding shows the same
    /// rows without another fetch.
    #[gpui::test]
    fn tree_mode_collapse_hides_but_keeps_cached_children(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.apply_page_for_test("", page(&["logs/"], &[]));
            doc.toggle_tree_mode(cx);
            doc.select_node(ObjectTreeNodeId::Prefix("logs/".to_string()), cx);
            doc.dispatch_command(dbflux_app::keymap::Command::ColumnRight, window, cx);
            doc.apply_page_for_test("logs/", page(&[], &["logs/a.log", "logs/b.log"]));
        });

        doc.update(window, |doc, _cx| {
            // Root row + two nested children.
            assert_eq!(doc.visible_rows().len(), 3);
        });

        doc.update_in(window, |doc, window, cx| {
            doc.select_node(ObjectTreeNodeId::Prefix("logs/".to_string()), cx);
            doc.dispatch_command(dbflux_app::keymap::Command::ColumnLeft, window, cx);
        });

        doc.update(window, |doc, _cx| {
            assert!(!doc.tree.is_expanded("logs/"));
            assert_eq!(doc.visible_rows().len(), 1, "children hidden, not fetched");
            assert_eq!(
                doc.tree.level("logs/").map(|level| level.entries.len()),
                Some(2),
                "children stay cached across a collapse"
            );
        });
    }

    /// UX remediation: a single click on a tree-mode folder row (and on its
    /// chevron) flips the node, expanding it first and collapsing it next,
    /// without ever moving the listing to another level.
    #[gpui::test]
    fn toggling_a_tree_node_expands_then_collapses_in_place(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        doc.update(cx, |doc, cx| {
            doc.apply_page_for_test("", page(&["logs/"], &[]));
            doc.toggle_tree_mode(cx);

            doc.toggle_tree_node("logs/".to_string(), cx);
            assert!(doc.tree.is_expanded("logs/"));

            doc.toggle_tree_node("logs/".to_string(), cx);
            assert!(!doc.tree.is_expanded("logs/"));
            assert_eq!(doc.tree.current_prefix, "", "toggling never navigates");
        });
    }

    /// UX remediation: right-clicking an object row targets that row and its
    /// entries dispatch through the existing intents — Presign lands in the
    /// same pending action the preview action bar raises.
    #[gpui::test]
    fn object_context_menu_acts_on_the_right_clicked_row(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update(window, |doc, cx| {
            doc.apply_page_for_test("", page(&[], &["a.txt", "b.txt"]));
            doc.select_node(ObjectTreeNodeId::Object("a.txt".to_string()), cx);
            doc.open_context_menu(
                ObjectTreeNodeId::Object("b.txt".to_string()),
                gpui::Point::default(),
                cx,
            );

            assert_eq!(
                doc.tree.selected,
                Some(ObjectTreeNodeId::Object("b.txt".to_string())),
                "the menu retargets the selection to the right-clicked row"
            );
            assert_eq!(
                doc.context_menu
                    .as_ref()
                    .map(|menu| menu.items.len())
                    .unwrap_or_default(),
                7,
                "Preview, Open in editor, Download, Rename, Presign, Copy S3 URI, Delete"
            );
        });

        doc.update_in(window, |doc, window, cx| {
            doc.execute_menu_action(
                context_menu::ObjectMenuAction::Presign,
                ObjectTreeNodeId::Object("b.txt".to_string()),
                window,
                cx,
            );
        });

        doc.update(window, |doc, _cx| {
            assert!(doc.context_menu.is_none(), "executing closes the menu");
            // The intent flows through the same drain the preview action bar
            // uses, so by now it has landed in the presign modal.
            assert_eq!(
                doc.presign().map(|presign| presign.key.as_str()),
                Some("b.txt")
            );
        });
    }

    /// UX remediation: the folder menu's first entry follows the mode, and
    /// "New folder inside" targets the right-clicked folder rather than the
    /// level being listed.
    #[gpui::test]
    fn folder_context_menu_follows_the_mode_and_the_clicked_folder(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update(window, |doc, cx| {
            doc.apply_page_for_test("", page(&["logs/"], &[]));
            doc.open_context_menu(
                ObjectTreeNodeId::Prefix("logs/".to_string()),
                gpui::Point::default(),
                cx,
            );

            let label = doc
                .context_menu
                .as_ref()
                .map(|menu| menu.items[0].label.to_string());
            assert_eq!(label.as_deref(), Some("Open"), "per-level mode opens");

            doc.toggle_tree_mode(cx);
            doc.open_context_menu(
                ObjectTreeNodeId::Prefix("logs/".to_string()),
                gpui::Point::default(),
                cx,
            );

            let label = doc
                .context_menu
                .as_ref()
                .map(|menu| menu.items[0].label.to_string());
            assert_eq!(label.as_deref(), Some("Expand"), "tree mode discloses");
        });

        doc.update_in(window, |doc, window, cx| {
            doc.execute_menu_action(
                context_menu::ObjectMenuAction::NewFolderInside,
                ObjectTreeNodeId::Prefix("logs/".to_string()),
                window,
                cx,
            );

            assert!(doc.take_pending_new_folder());
            assert_eq!(
                doc.take_pending_new_folder_parent().as_deref(),
                Some("logs/"),
                "the new folder lands under the right-clicked folder"
            );
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

    /// T41: the preview action bar's Presign intent is drained on the next
    /// render pass into the presigned-URL modal, for the object it named.
    #[gpui::test]
    fn presign_action_is_drained_into_the_presign_modal(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, cx| {
                doc.request_object_action(
                    ObjectAction::Presign {
                        key: "logs/a.txt".to_string(),
                    },
                    cx,
                );
            });
        });

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                assert_eq!(doc.take_pending_object_action(), None);
                assert_eq!(
                    doc.presign().map(|presign| presign.key.as_str()),
                    Some("logs/a.txt")
                );
            });
        });
    }

    /// T34: the preview action bar's Delete intent is drained on the next
    /// render pass, which turns it into the single-delete confirmation.
    #[gpui::test]
    fn delete_action_is_drained_into_the_confirmation(cx: &mut gpui::TestAppContext) {
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
                assert_eq!(doc.take_pending_object_action(), None);
                assert_eq!(
                    doc.pending_object_delete()
                        .map(|pending| pending.key.as_str()),
                    Some("logs/a.txt")
                );
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

    /// T30: editing the buffer marks the document modified and gives the tab
    /// bar a summary of what is pending.
    #[gpui::test]
    fn editing_the_buffer_marks_the_document_modified(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.open_preview("logs/app.log".to_string(), cx);
            doc.install_editor_for_test("logs/app.log", "first line\n", window, cx);
        });

        doc.update(window, |doc, _cx| {
            assert_ne!(doc.state(), DocumentState::Modified);
            assert_eq!(doc.change_summary(), None);
        });

        doc.update_in(window, |doc, window, cx| {
            doc.type_into_editor_for_test("second line\n", window, cx);
        });

        doc.update(window, |doc, _cx| {
            assert_eq!(doc.state(), DocumentState::Modified);
            assert_eq!(
                doc.change_summary(),
                Some("Unsaved edits to logs/app.log".to_string())
            );
        });
    }

    /// T31: selecting another object with unsaved edits parks the request
    /// behind the confirmation instead of switching and losing the buffer.
    #[gpui::test]
    fn navigating_away_while_dirty_is_parked(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.open_preview("logs/app.log".to_string(), cx);
            doc.install_editor_for_test("logs/app.log", "before", window, cx);
            doc.type_into_editor_for_test("edited ", window, cx);
        });

        // The buffer's change event is delivered on flush, exactly as it is
        // between two real user interactions.
        window.run_until_parked();

        doc.update(window, |doc, cx| {
            doc.open_preview("logs/other.log".to_string(), cx);
        });

        doc.update(window, |doc, _cx| {
            assert_eq!(doc.preview_key_for_test(), Some("logs/app.log"));
            assert!(doc.pending_navigation_for_test().is_some());
        });

        // Cancelling leaves the user exactly where they were.
        doc.update(window, |doc, cx| doc.cancel_guarded_navigation(cx));

        doc.update(window, |doc, _cx| {
            assert_eq!(doc.preview_key_for_test(), Some("logs/app.log"));
            assert!(doc.pending_navigation_for_test().is_none());
            assert_eq!(doc.state(), DocumentState::Modified);
        });
    }

    /// T31: discarding restores the loaded content and then lets the parked
    /// navigation through.
    #[gpui::test]
    fn discarding_reverts_the_buffer_and_navigates(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.open_preview("logs/app.log".to_string(), cx);
            doc.install_editor_for_test("logs/app.log", "before", window, cx);
            doc.type_into_editor_for_test("edited ", window, cx);
        });

        window.run_until_parked();

        // Discard on its own only reverts the buffer.
        doc.update_in(window, |doc, window, cx| {
            doc.discard_object_edits(window, cx);
        });

        doc.update(window, |doc, cx| {
            assert_eq!(doc.editor_text_for_test(cx).as_deref(), Some("before"));
            assert_eq!(doc.change_summary(), None);
        });

        // With a parked navigation, discarding also releases it.
        doc.update_in(window, |doc, window, _cx| {
            doc.type_into_editor_for_test("edited ", window, _cx);
        });

        window.run_until_parked();

        doc.update_in(window, |doc, window, cx| {
            doc.close_preview(cx);
            doc.discard_and_navigate(window, cx);
        });

        doc.update(window, |doc, _cx| {
            assert_eq!(doc.preview_key_for_test(), None);
            assert!(doc.pending_navigation_for_test().is_none());
            assert_eq!(doc.change_summary(), None);
        });
    }

    /// T31: moving to another prefix is guarded too, and the listing does not
    /// shift level while the confirmation is up.
    #[gpui::test]
    fn prefix_navigation_while_dirty_is_parked(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.apply_page_for_test("", page(&["logs/"], &["app.log"]));
            doc.open_preview("app.log".to_string(), cx);
            doc.install_editor_for_test("app.log", "before", window, cx);
            doc.type_into_editor_for_test("edited ", window, cx);
        });

        window.run_until_parked();

        doc.update_in(window, |doc, window, cx| {
            doc.navigate_to_prefix("logs/".to_string(), window, cx);
        });

        doc.update(window, |doc, _cx| {
            assert_eq!(doc.tree.current_prefix, "");
            assert_eq!(
                doc.pending_navigation_for_test(),
                Some(
                    &crate::object_browser::editor::GuardedNavigation::NavigateToPrefix(
                        "logs/".to_string()
                    )
                )
            );
        });
    }

    /// Known fix: while the filter input owns keyboard focus (`Filter` mode,
    /// entered via `Command::FocusSearch`), listing commands (Delete,
    /// Rename, ExpandCollapse, ...) must not fire — the context must route
    /// to the text-input layer, same as the inline editor.
    #[gpui::test]
    fn filter_focus_mode_routes_to_text_input_context(cx: &mut gpui::TestAppContext) {
        let doc = new_test_entity(cx);

        cx.update(|cx| {
            doc.update(cx, |doc, _cx| {
                doc.set_focus_mode_for_test(super::ObjectBrowserFocusMode::Filter);
            });
        });

        cx.update(|cx| {
            assert_eq!(
                doc.read(cx).active_context(),
                dbflux_app::keymap::ContextId::TextInput
            );
        });
    }

    /// T38: the toolbar's New Folder intent turns into an open overlay on the
    /// next render pass, pre-empty and ready to type into.
    #[gpui::test]
    fn new_folder_intent_opens_the_overlay(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update(window, |doc, cx| doc.request_new_folder(cx));

        doc.update_in(window, |doc, window, cx| {
            doc.drain_pending_new_folder(window, cx);
        });

        doc.update(window, |doc, cx| {
            let state = doc.new_folder().expect("the overlay must be open");
            assert_eq!(state.name_input.read(cx).value(), "");
            assert!(!state.submitting);
        });
    }

    /// T43: `Command::Rename` on a selected object row opens the rename
    /// overlay pre-filled with the object's leaf name.
    #[gpui::test]
    fn rename_command_opens_the_overlay_prefilled_with_the_leaf_name(
        cx: &mut gpui::TestAppContext,
    ) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.apply_page_for_test("", page(&[], &["logs/app.log"]));
            doc.select_node(ObjectTreeNodeId::Object("logs/app.log".to_string()), cx);
            doc.dispatch_command(dbflux_app::keymap::Command::Rename, window, cx);
        });

        doc.update(window, |doc, cx| {
            let state = doc.rename_object().expect("the overlay must be open");
            assert_eq!(state.key, "logs/app.log");
            assert_eq!(state.name_input.read(cx).value(), "app.log");
        });
    }

    /// T43: renaming an object whose editor is open and dirty parks the
    /// request behind the unsaved-edits guard, same as delete does.
    #[gpui::test]
    fn renaming_a_dirty_object_is_parked_behind_the_guard(cx: &mut gpui::TestAppContext) {
        let (doc, window) = new_test_entity_with_window(cx);

        doc.update_in(window, |doc, window, cx| {
            doc.open_preview("logs/app.log".to_string(), cx);
            doc.install_editor_for_test("logs/app.log", "before", window, cx);
            doc.type_into_editor_for_test("edited ", window, cx);
        });

        window.run_until_parked();

        doc.update_in(window, |doc, window, cx| {
            doc.request_rename_object("logs/app.log".to_string(), window, cx);
        });

        doc.update(window, |doc, _cx| {
            assert!(doc.rename_object().is_none());
            assert!(doc.pending_navigation_for_test().is_some());
        });
    }

    fn new_test_entity_with_window(
        cx: &mut gpui::TestAppContext,
    ) -> (
        gpui::Entity<ObjectBrowserDocument>,
        &mut gpui::VisualTestContext,
    ) {
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

        cx.add_window_view(|window, cx| {
            ObjectBrowserDocument::new(profile_id, "my-bucket".to_string(), app_state, window, cx)
        })
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

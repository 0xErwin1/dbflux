mod data;
pub mod new_bucket;
mod pane;
mod render;

pub use data::{
    BUCKET_SIZE_ESTIMATE_CAP, BucketDetailsState, BucketRow, BucketSizeEstimateState,
    OperationTiming, bucket_delete_allowed,
};
pub use new_bucket::{BucketEncryptionChoice, NewBucketState, bucket_name_error};
pub(crate) use render::format_bytes;

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::controls::{InputEvent, InputState};
use dbflux_core::RefreshPolicy;
use dbflux_ui_base::AppStateEntity;
use gpui::*;
use uuid::Uuid;

/// Which part of the document currently owns keyboard input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketsFocusMode {
    Table,
    Search,
}

/// Searchable buckets table opened for an object-storage connection root
/// (`DatabaseCategory::ObjectStorage`).
///
/// Rows come from `list_buckets`; region and versioning fill in lazily per row
/// and the object count / total size are only ever computed on explicit user
/// action (see `data.rs`). The table itself is keyboard-first: every action in
/// the footer hint bar is reachable without the mouse.
pub struct BucketsTableDocument {
    id: DocumentId,
    title: String,
    profile_id: Uuid,
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    refresh_policy: RefreshPolicy,
    state: DocumentState,
    last_error: Option<String>,
    buckets: Vec<BucketRow>,
    search_input: Entity<InputState>,
    search_query: String,
    focus_mode: BucketsFocusMode,
    selected_bucket: Option<String>,
    show_details: bool,
    delete_probe: Option<String>,
    pending_delete: Option<String>,
    pending_new_bucket: bool,
    /// New Bucket modal, built on the render pass that drains
    /// `pending_new_bucket` (its inputs need a `Window`).
    new_bucket: Option<NewBucketState>,
    pending_open_bucket: Option<String>,
    last_operation: Option<OperationTiming>,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for BucketsTableDocument {}

impl BucketsTableDocument {
    pub fn new(
        profile_id: Uuid,
        app_state: Entity<AppStateEntity>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let connection_name = app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_default();

        let search_input =
            cx.new(|cx| InputState::new(window, cx).placeholder("Find buckets by name…"));

        let search_subscription = cx.subscribe_in(
            &search_input,
            window,
            |this, input, event: &InputEvent, _window, cx| {
                if matches!(event, InputEvent::Change) {
                    this.search_query = input.read(cx).value().to_string();
                    this.clamp_selection();
                    cx.notify();
                }
            },
        );

        let mut doc = Self {
            id: DocumentId::new(),
            title: format!("Buckets — {connection_name}"),
            profile_id,
            app_state,
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            refresh_policy: RefreshPolicy::Manual,
            state: DocumentState::Loading,
            last_error: None,
            buckets: Vec::new(),
            search_input,
            search_query: String::new(),
            focus_mode: BucketsFocusMode::Table,
            selected_bucket: None,
            show_details: false,
            delete_probe: None,
            pending_delete: None,
            pending_new_bucket: false,
            new_bucket: None,
            pending_open_bucket: None,
            last_operation: None,
            _subscriptions: vec![search_subscription],
        };

        doc.load_buckets(cx);
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

    pub fn buckets(&self) -> &[BucketRow] {
        &self.buckets
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
        // The New Bucket modal is typed into, so its inputs must keep every
        // letter the table would otherwise read as a command.
        if self.new_bucket.is_some() {
            return ContextId::TextInput;
        }

        if self.focus_mode == BucketsFocusMode::Search {
            return ContextId::TextInput;
        }

        ContextId::Results
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
        self.focus_mode = BucketsFocusMode::Table;
        cx.notify();
    }

    /// The bucket currently under the row cursor, if any.
    pub fn selected_bucket(&self) -> Option<&str> {
        self.selected_bucket.as_deref()
    }

    /// Bucket creation intent raised by the toolbar's "New bucket" button.
    ///
    /// Drained by the create-bucket modal owner using the same `pending_*` +
    /// `take()` convention the other documents use for deferred modal opens.
    pub fn take_pending_new_bucket(&mut self) -> bool {
        std::mem::take(&mut self.pending_new_bucket)
    }

    /// Browse intent raised by Enter (or a double click) on a bucket row,
    /// drained by the object-browser owner. Same `pending_*` + `take()`
    /// convention as `take_pending_new_bucket`.
    pub fn take_pending_open_bucket(&mut self) -> Option<String> {
        self.pending_open_bucket.take()
    }

    // -- Selection -------------------------------------------------------

    /// Bucket names currently visible under the search filter, in display
    /// order. Selection and keyboard navigation operate on this list, not on
    /// the unfiltered `buckets` vector.
    pub(super) fn visible_bucket_names(&self) -> Vec<String> {
        self.filtered_buckets(&self.search_query)
            .into_iter()
            .map(|row| row.info.name.clone())
            .collect()
    }

    pub(super) fn select_bucket(&mut self, name: String, cx: &mut Context<Self>) {
        self.selected_bucket = Some(name);
        self.focus_mode = BucketsFocusMode::Table;
        cx.notify();
    }

    /// Drops the selection when the selected bucket is filtered out (or gone),
    /// falling back to the first visible row so the cursor is never orphaned.
    pub(super) fn clamp_selection(&mut self) {
        let visible = self.visible_bucket_names();

        let still_visible = self
            .selected_bucket
            .as_ref()
            .is_some_and(|name| visible.iter().any(|candidate| candidate == name));

        if !still_visible {
            self.selected_bucket = visible.first().cloned();
        }
    }

    pub(super) fn move_selection(&mut self, delta: isize, cx: &mut Context<Self>) {
        let visible = self.visible_bucket_names();

        if visible.is_empty() {
            return;
        }

        let current = self
            .selected_bucket
            .as_ref()
            .and_then(|name| visible.iter().position(|candidate| candidate == name));

        let next = match current {
            Some(index) => (index as isize + delta).clamp(0, visible.len() as isize - 1) as usize,
            None if delta >= 0 => 0,
            None => visible.len() - 1,
        };

        self.selected_bucket = visible.get(next).cloned();
        self.focus_mode = BucketsFocusMode::Table;
        cx.notify();
    }

    fn select_edge(&mut self, last: bool, cx: &mut Context<Self>) {
        let visible = self.visible_bucket_names();

        self.selected_bucket = if last {
            visible.last().cloned()
        } else {
            visible.first().cloned()
        };
        self.focus_mode = BucketsFocusMode::Table;
        cx.notify();
    }

    // -- Row actions -----------------------------------------------------

    pub(super) fn toggle_details(&mut self, cx: &mut Context<Self>) {
        self.show_details = !self.show_details;
        cx.notify();
    }

    pub(super) fn request_new_bucket(&mut self, cx: &mut Context<Self>) {
        self.pending_new_bucket = true;
        cx.notify();
    }

    pub(super) fn open_selected_bucket(&mut self, cx: &mut Context<Self>) {
        if let Some(name) = self.selected_bucket.clone() {
            self.pending_open_bucket = Some(name);
            cx.notify();
        }
    }

    pub(super) fn estimate_selected_bucket_size(&mut self, cx: &mut Context<Self>) {
        if let Some(name) = self.selected_bucket.clone() {
            self.estimate_bucket_size(name, cx);
        }
    }

    fn focus_search(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_mode = BucketsFocusMode::Search;
        self.search_input
            .update(cx, |input, cx| input.focus(window, cx));
        cx.notify();
    }

    // -- Command dispatch ------------------------------------------------

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // The New Bucket modal owns the keyboard while it is up: Execute
        // submits (only when the form is valid), Cancel dismisses, and
        // everything else belongs to its inputs.
        if self.new_bucket.is_some() {
            return match cmd {
                Command::Execute => {
                    self.submit_new_bucket(cx);
                    true
                }
                Command::Cancel => {
                    self.close_new_bucket(cx);
                    true
                }
                _ => false,
            };
        }

        if self.pending_delete.is_some() {
            return match cmd {
                Command::Execute => {
                    self.confirm_delete_bucket(cx);
                    true
                }
                Command::Cancel => {
                    self.cancel_delete_bucket(cx);
                    true
                }
                _ => true,
            };
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
            Command::ExpandCollapse => {
                self.toggle_details(cx);
                true
            }
            Command::Execute => {
                self.open_selected_bucket(cx);
                true
            }
            Command::Delete => {
                self.request_delete_selected_bucket(cx);
                true
            }
            Command::ResultsAddRow => {
                self.request_new_bucket(cx);
                true
            }
            Command::RefreshSchema => {
                self.load_buckets(cx);
                true
            }
            Command::FocusSearch | Command::FocusToolbar => {
                self.focus_search(window, cx);
                true
            }
            Command::Cancel => {
                self.focus_mode = BucketsFocusMode::Table;
                self.focus_handle.focus(window);
                cx.notify();
                true
            }
            _ => false,
        }
    }
}

mod data;
mod pane;
pub mod tree;

pub use tree::{
    ObjectTree, ObjectTreeEntry, ObjectTreeNodeId, PrefixLoadState, TREE_MODE_PAGE_CAP,
    TreeModeRow, TreeModeStatus,
};

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use crate::buckets_table::OperationTiming;
use dbflux_app::keymap::{Command, ContextId};
use dbflux_core::RefreshPolicy;
use dbflux_ui_base::AppStateEntity;
use gpui::*;
use uuid::Uuid;

/// Object browser opened for a single bucket under an object-storage
/// connection (routed from `BucketsTableDocument`'s Enter-on-row and the
/// sidebar's `OpenObjectStoreBucket` event).
///
/// The tree/pagination state lives in `tree: ObjectTree` (`tree.rs`, a pure
/// data model); this entity owns the GPUI plumbing — background loading via
/// `object_store_api()`, `cx.spawn`, and `report_error_async` — layered on
/// top of it in `data.rs`. Rendering (breadcrumb bar, rows, toolbar) lands in
/// a later batch; this skeleton renders a minimal placeholder.
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
}

impl EventEmitter<DocumentEvent> for ObjectBrowserDocument {}

impl ObjectBrowserDocument {
    pub fn new(
        profile_id: Uuid,
        bucket: String,
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let tree = ObjectTree::new(bucket.clone());

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
        cx.notify();
    }

    /// Minimal placeholder — the breadcrumb/row/toolbar layout lands in a
    /// later batch. Shows the current prefix and a loading/error state so the
    /// pending consumers wired in this batch (row activation, sidebar) have
    /// something visible to land on.
    fn render_placeholder(&self, cx: &Context<Self>) -> impl IntoElement {
        use gpui_component::ActiveTheme;

        let status = match &self.state {
            DocumentState::Loading => "Loading…".to_string(),
            DocumentState::Error => self
                .last_error
                .clone()
                .unwrap_or_else(|| "Unknown error".to_string()),
            _ => format!(
                "s3://{}/{} — {} entries",
                self.bucket,
                self.tree.current_prefix,
                self.tree
                    .level(&self.tree.current_prefix)
                    .map(|l| l.entries.len())
                    .unwrap_or(0)
            ),
        };

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .flex_col()
            .items_center()
            .justify_center()
            .bg(cx.theme().background)
            .child(status)
    }

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        match cmd {
            Command::RefreshSchema => {
                self.expand_prefix(self.tree.current_prefix.clone(), cx);
                true
            }
            Command::Cancel => {
                self.focus_handle.focus(window);
                cx.notify();
                true
            }
            _ => false,
        }
    }
}

impl Render for ObjectBrowserDocument {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        self.render_placeholder(cx)
    }
}

mod data;
mod pane;

pub use data::{
    BUCKET_SIZE_ESTIMATE_CAP, BucketDetailsState, BucketRow, BucketSizeEstimateState,
    bucket_delete_allowed,
};

use super::handle::DocumentEvent;
use super::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::icons::AppIcon;
use dbflux_components::primitives::{Icon, Text};
use dbflux_core::RefreshPolicy;
use dbflux_ui_base::AppStateEntity;
use gpui::*;
use gpui_component::ActiveTheme;
use uuid::Uuid;

/// Searchable buckets table opened for an object-storage connection root
/// (`DatabaseCategory::ObjectStorage`).
///
/// This is the plumbing + data-loading layer (entity + `into_pane` +
/// workspace routing + `list_buckets`/`get_bucket_details`/
/// `estimate_bucket_size`). The `data_table`-backed UI (search, refresh,
/// new-bucket, footer) lands in the batch that builds on top of this seam —
/// `render` stays a minimal placeholder until then.
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
}

impl EventEmitter<DocumentEvent> for BucketsTableDocument {}

impl BucketsTableDocument {
    pub fn new(
        profile_id: Uuid,
        app_state: Entity<AppStateEntity>,
        _window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let connection_name = app_state
            .read(cx)
            .connections()
            .get(&profile_id)
            .map(|connected| connected.profile.name.clone())
            .unwrap_or_default();

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
        ContextId::Results
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window);
        cx.notify();
    }

    /// No commands are handled yet — the toolbar (search/refresh/new-bucket)
    /// and row navigation land with the table-UI batch.
    pub fn dispatch_command(
        &mut self,
        _cmd: Command,
        _window: &mut Window,
        _cx: &mut Context<Self>,
    ) -> bool {
        false
    }
}

impl Render for BucketsTableDocument {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let status = match (self.state, &self.last_error) {
            (DocumentState::Loading, _) => "Loading buckets…".to_string(),
            (DocumentState::Error, Some(err)) => format!("Failed to list buckets: {err}"),
            (DocumentState::Error, None) => "Failed to list buckets".to_string(),
            _ => format!(
                "{} bucket(s) loaded — table UI loads in a later batch",
                self.buckets.len()
            ),
        };

        div()
            .track_focus(&self.focus_handle)
            .size_full()
            .flex()
            .items_center()
            .justify_center()
            .bg(cx.theme().background)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .items_center()
                    .gap_2()
                    .child(Icon::new(AppIcon::Box).size(px(32.0)))
                    .child(Text::body(self.title.clone()))
                    .child(Text::muted(status)),
            )
    }
}

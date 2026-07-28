mod pane;

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
/// This is the plumbing skeleton (entity + `into_pane` + workspace routing).
/// Data loading (`list_buckets`, lazy `get_bucket_details`) and the
/// `data_table`-backed UI (search, refresh, new-bucket, footer) land in the
/// batch that builds on top of this seam.
pub struct BucketsTableDocument {
    id: DocumentId,
    title: String,
    profile_id: Uuid,
    // Retained for the bucket-listing data-loading batch built on top of this
    // plumbing seam; unused until that batch calls `object_store_api()` on
    // the resolved connection.
    #[allow(dead_code)]
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    refresh_policy: RefreshPolicy,
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

        Self {
            id: DocumentId::new(),
            title: format!("Buckets — {connection_name}"),
            profile_id,
            app_state,
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            refresh_policy: RefreshPolicy::Manual,
        }
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn state(&self) -> DocumentState {
        DocumentState::Clean
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
    /// and row navigation land with the data-loading and table-UI batches.
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
                    .child(Text::muted("Bucket listing loads in a later batch")),
            )
    }
}

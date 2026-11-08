//! One secret of a secret-store connection in its own tab.
//!
//! The document owns no write logic: it hosts the same `SecretView` the
//! listing's details pane does, so the write gates, the session pin, the
//! unsaved-change prompt and the audited Retrieve are the view's own. What the
//! document adds is the identity of the tab (the connection, the store, the
//! name, the version and the region of its key), the two states in which it
//! must not touch the store (the secret was removed, the connection is on
//! another region), and the layout of artboards 02 and 06.

mod pane;
mod render;
#[cfg(test)]
mod tests;

use crate::handle::DocumentEvent;
use crate::secret_store::changes::{self, SecretChange, SecretChangeKind};
use crate::secret_store::{ConnectionFacts, DetailsState, SecretView, SecretViewEvent, ViewHost};
use crate::types::{DocumentId, DocumentState};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_core::SecretStoreDescriptor;
use dbflux_ui_base::{AppStateChanged, AppStateEntity};
use gpui::*;
use std::cell::Cell;
use std::rc::Rc;
use uuid::Uuid;

/// Why the tab does not touch the store, when it does not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SecretDocumentBlock {
    /// The secret was deleted or never existed under this key.
    Removed,

    /// The profile is connected, on another region than the key's.
    RegionMismatch { live: Option<String> },
}

pub struct SecretDocument {
    id: DocumentId,
    title: String,
    profile_id: Uuid,
    store: SecretStoreDescriptor,
    connection_name: String,
    name: String,
    version: Option<String>,
    region: Option<String>,
    app_state: Entity<AppStateEntity>,
    focus_handle: FocusHandle,
    is_active_tab: bool,
    view: Entity<SecretView>,
    /// The view was told to show the secret. It waits while the connection is
    /// on another region, and shows it once the region matches.
    loaded: bool,
    removed: bool,
    /// An Edit value asked for before the details landed: the editor opens on
    /// the render pass that finds the secret ready, retrieving the value
    /// first (audited) when it is not loaded.
    edit_when_ready: bool,
    _subscriptions: Vec<Subscription>,
}

impl EventEmitter<DocumentEvent> for SecretDocument {}

impl SecretDocument {
    pub fn new(
        profile_id: Uuid,
        store_id: String,
        name: String,
        version: Option<String>,
        region: Option<String>,
        app_state: Entity<AppStateEntity>,
        cx: &mut Context<Self>,
    ) -> Self {
        let facts = ConnectionFacts::read(&app_state, profile_id, &store_id, cx);
        let store = facts.descriptor;

        let view = cx.new(|cx| {
            SecretView::new(
                profile_id,
                store.clone(),
                app_state.clone(),
                Rc::new(Cell::new(false)),
                ViewHost::Tab,
                cx,
            )
        });
        view.update(cx, |view, cx| {
            view.set_region_pin(region.clone(), cx);
            view.set_opened_version(version.clone());
        });

        let mut subscriptions = vec![
            // The view redraws itself on its own state; the document draws it.
            cx.observe(&view, |_, _, cx| cx.notify()),
            cx.subscribe(&view, Self::on_view_event),
            changes::follow(cx, Self::on_secret_change),
        ];
        subscriptions.push(
            cx.subscribe(&app_state, |this, _, _: &AppStateChanged, cx| {
                this.on_app_state_changed(cx)
            }),
        );

        let mut document = Self {
            id: DocumentId::new(),
            title: name.clone(),
            profile_id,
            store,
            connection_name: facts.name,
            name,
            version,
            region,
            app_state,
            focus_handle: cx.focus_handle(),
            is_active_tab: true,
            view,
            loaded: false,
            removed: false,
            edit_when_ready: false,
            _subscriptions: subscriptions,
        };

        document.load_when_ready(cx);
        document
    }

    pub fn id(&self) -> DocumentId {
        self.id
    }

    pub fn title(&self) -> String {
        self.title.clone()
    }

    pub fn connection_id(&self) -> Option<Uuid> {
        Some(self.profile_id)
    }

    pub fn store_id(&self) -> &str {
        &self.store.id
    }

    pub fn secret_name(&self) -> &str {
        &self.name
    }

    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// The region of the document's key.
    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn view(&self) -> &Entity<SecretView> {
        &self.view
    }

    pub fn can_close(&self) -> bool {
        true
    }

    pub fn set_active_tab(&mut self, active: bool) {
        self.is_active_tab = active;
    }

    /// Whether the tab must leave the store alone, and why.
    pub fn block(&self, cx: &App) -> Option<SecretDocumentBlock> {
        if self.removed {
            return Some(SecretDocumentBlock::Removed);
        }

        self.view
            .read(cx)
            .region_mismatch(cx)
            .then(|| SecretDocumentBlock::RegionMismatch {
                live: self
                    .app_state
                    .read(cx)
                    .connections()
                    .get(&self.profile_id)
                    .and_then(|connected| connected.connection.current_region()),
            })
    }

    pub fn state(&self, cx: &App) -> DocumentState {
        let view = self.view.read(cx);

        if view.has_unsaved_value_edits(cx) {
            DocumentState::Modified
        } else if self.block(cx).is_some() {
            DocumentState::Error
        } else if matches!(view.details(), crate::secret_store::DetailsState::Loading) {
            DocumentState::Loading
        } else if matches!(view.details(), crate::secret_store::DetailsState::Failed(_)) {
            DocumentState::Error
        } else {
            DocumentState::Clean
        }
    }

    pub fn active_context(&self, cx: &App) -> ContextId {
        let view = self.view.read(cx);

        // A dialog owns the keyboard, then the editor: both keep every letter
        // a command would otherwise take.
        if view.dialog_is_open() || view.value_editor_focused() {
            return ContextId::TextInput;
        }

        ContextId::Results
    }

    pub fn focus(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.focus_handle.focus(window, cx);
        cx.notify();
    }

    /// Shows the secret in the view once the connection is on the key's
    /// region. Until then the view holds nothing of it.
    fn load_when_ready(&mut self, cx: &mut Context<Self>) {
        if self.loaded || self.removed || self.view.read(cx).region_mismatch(cx) {
            return;
        }

        self.loaded = true;
        let name = self.name.clone();
        self.view
            .update(cx, |view, cx| view.show_secret(name, None, cx));
        cx.notify();
    }

    fn on_app_state_changed(&mut self, cx: &mut Context<Self>) {
        self.view
            .update(cx, |view, cx| view.begin_connection_change(cx));
        self.load_when_ready(cx);
        self.view
            .update(cx, |view, cx| view.finish_connection_change(cx));
        cx.notify();
    }

    /// Follows what another view of the store did to this secret.
    fn on_secret_change(&mut self, change: &SecretChange, cx: &mut Context<Self>) {
        if !change.is_in_scope(self.profile_id, &self.store.id, self.region.as_deref())
            || change.name != self.name
        {
            return;
        }

        match change.kind {
            SecretChangeKind::Removed if !self.removed => {
                let name = self.name.clone();
                self.removed = true;
                self.view
                    .update(cx, |view, cx| view.forget_removed_secret(&name, cx));
            }
            SecretChangeKind::Created if self.removed => {
                // The name exists again: the tab shows the new secret.
                self.removed = false;
                self.loaded = false;
                self.load_when_ready(cx);
            }
            SecretChangeKind::Changed if !self.removed && self.loaded => {
                let name = self.name.clone();
                self.view.update(cx, |view, cx| {
                    view.describe(name.clone(), cx);
                    view.load_versions(name, cx);
                });
            }
            _ => {}
        }

        cx.notify();
    }

    /// Reacts to what the view reports.
    fn on_view_event(
        &mut self,
        _view: Entity<SecretView>,
        event: &SecretViewEvent,
        cx: &mut Context<Self>,
    ) {
        match event {
            SecretViewEvent::Removed { name } => {
                self.removed = true;
                self.announce(name.clone(), SecretChangeKind::Removed, cx);
            }
            SecretViewEvent::ReloadListing => {
                self.announce(self.name.clone(), SecretChangeKind::Changed, cx);
            }
            SecretViewEvent::CloseRequested => cx.emit(DocumentEvent::RequestClose),
            SecretViewEvent::StoreCalled(_)
            | SecretViewEvent::DialogOpened
            | SecretViewEvent::DialogClosed
            | SecretViewEvent::OpenInTab { .. }
            | SecretViewEvent::Created { .. } => {}
        }

        cx.notify();
    }

    fn announce(&self, name: String, kind: SecretChangeKind, cx: &mut Context<Self>) {
        changes::publish(
            SecretChange {
                profile_id: self.profile_id,
                store_id: self.store.id.clone(),
                region: self.region.clone(),
                name,
                kind,
                origin: cx.entity_id(),
            },
            cx,
        );
    }

    /// Asks for the value editor: it opens as soon as the secret is ready,
    /// and a value that is not loaded is retrieved first, never only shown.
    pub fn begin_edit(&mut self, cx: &mut Context<Self>) {
        self.edit_when_ready = true;
        cx.notify();
    }

    /// Opens the editor a pending Edit value asked for, on the render pass
    /// because the editor is built with a window. The request is dropped when
    /// the tab can never serve it: blocked, or the details failed.
    fn open_requested_edit(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if !self.edit_when_ready {
            return;
        }

        if self.block(cx).is_some() {
            self.edit_when_ready = false;
            return;
        }

        match self.view.read(cx).details() {
            DetailsState::Loaded(_) => {
                self.edit_when_ready = false;
                self.view
                    .update(cx, |view, cx| view.request_edit_value(window, cx));
            }
            DetailsState::Failed(_) => self.edit_when_ready = false,
            DetailsState::Idle | DetailsState::Loading => {}
        }
    }

    /// Reads the secret again.
    pub fn refresh(&mut self, cx: &mut Context<Self>) {
        if self.block(cx).is_some() {
            return;
        }

        self.view.update(cx, |view, cx| view.refresh_shown(cx));
    }

    pub fn dispatch_command(
        &mut self,
        cmd: Command,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> bool {
        // The dialog handles its own keys.
        if self.view.read(cx).dialog_is_open() {
            return true;
        }

        // A blocked tab neither reads nor writes: every command that would
        // reach the store stops here, and the state says why.
        if self.block(cx).is_some() {
            return !matches!(cmd, Command::Cancel);
        }

        match cmd {
            Command::RefreshSchema => self.refresh(cx),
            Command::EditValue => self.begin_edit(cx),
            Command::Cancel => {
                self.focus_handle.focus(window, cx);
                cx.notify();
            }
            _ => {
                return self
                    .view
                    .update(cx, |view, cx| view.dispatch_command(cmd, window, cx));
            }
        }

        true
    }
}

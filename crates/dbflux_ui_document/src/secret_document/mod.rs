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
    /// A close the unsaved-changes dialog asked for waits for the save it
    /// started: the tab closes only once that write lands and nothing is left
    /// unsaved.
    close_after_save: bool,
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

        let origin = cx.entity_id();
        let view = cx.new(|cx| {
            let mut view = SecretView::new(
                profile_id,
                store.clone(),
                app_state.clone(),
                Rc::new(Cell::new(false)),
                ViewHost::Tab,
                cx,
            );
            view.set_change_origin(origin);
            view
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
            close_after_save: false,
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
            SecretChangeKind::Changed
            | SecretChangeKind::Scheduled { .. }
            | SecretChangeKind::Restored
                if !self.removed && self.loaded =>
            {
                let (name, kind) = (self.name.clone(), change.kind);
                self.view
                    .update(cx, |view, cx| view.follow_external_write(&name, kind, cx));
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
            SecretViewEvent::Removed => self.removed = true,
            SecretViewEvent::SaveFinished { succeeded } => self.finish_save(*succeeded, cx),
            // The view announces its own writes where they land.
            SecretViewEvent::ReloadListing => {}
            SecretViewEvent::CloseRequested => cx.emit(DocumentEvent::RequestClose),
            SecretViewEvent::StoreCalled(_)
            | SecretViewEvent::DialogOpened
            | SecretViewEvent::DialogClosed
            | SecretViewEvent::OpenInTab { .. }
            | SecretViewEvent::Created { .. } => {}
        }

        cx.notify();
    }

    /// Reports a finished save to the workspace's close and quit flows. A tab
    /// asked to close closes only when the write landed and no text is left
    /// unsaved; any other outcome keeps it open with its text.
    fn finish_save(&mut self, succeeded: bool, cx: &mut Context<Self>) {
        let close = std::mem::take(&mut self.close_after_save);

        cx.emit(DocumentEvent::SaveFinished { succeeded });

        if close && succeeded && !self.view.read(cx).has_unsaved_value_edits(cx) {
            cx.emit(DocumentEvent::RequestClose);
        }
    }

    /// Starts the save of a quit or a close the user confirmed, through the
    /// view's own gates. `false` means nothing was sent: a refusal is already
    /// reported, no result follows, and the caller keeps the draft.
    pub fn save_for_quit(&mut self, cx: &mut Context<Self>) -> bool {
        if self.block(cx).is_some() {
            return false;
        }

        self.view
            .update(cx, |view, cx| view.try_save_value_edit(cx))
    }

    /// [`Self::save_for_quit`] that also closes the tab once it landed.
    pub fn save_for_close(&mut self, cx: &mut Context<Self>) -> bool {
        self.close_after_save = true;

        let started = self.save_for_quit(cx);

        if !started {
            self.close_after_save = false;
        }

        started
    }

    /// What quitting means for the tab: unsaved text needs the user's call,
    /// because saving it goes through the write gates and may be refused.
    pub fn quit_disposition(&self, cx: &App) -> crate::pane::QuitDisposition {
        if self.view.read(cx).has_unsaved_value_edits(cx) {
            crate::pane::QuitDisposition::NeedsDecision
        } else {
            crate::pane::QuitDisposition::Clean
        }
    }

    /// Drops the draft for a quit the user confirmed without saving it.
    pub fn discard_for_quit(&mut self, cx: &mut Context<Self>) {
        self.view.update(cx, |view, cx| view.discard_value_edit(cx));
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

        // An editor that is already open takes the keyboard back; it is not
        // rebuilt, so its text is kept.
        if self.view.read(cx).is_editing_value() {
            self.edit_when_ready = false;
            self.view
                .update(cx, |view, cx| view.focus_value_editor(window, cx));
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
            // The menu commands go on to the workspace, whose actions menu
            // lists Close; every command that would reach the store stops here.
            return !matches!(
                cmd,
                Command::Cancel | Command::OpenPaneActions | Command::OpenContextMenu
            );
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

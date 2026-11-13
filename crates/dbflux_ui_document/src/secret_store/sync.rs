//! How the listing hands a secret to its own tab and follows what the open
//! tabs of its secrets do.

use super::changes::{self, SecretChange, SecretChangeKind};
use super::{SecretOpenRequest, SecretRowKey, SecretStoreDocument};
use crate::handle::DocumentEvent;
use gpui::Context;

impl SecretStoreDocument {
    /// Tells the other views of the store that `name` changed here.
    pub(super) fn announce(&self, name: String, kind: SecretChangeKind, cx: &mut Context<Self>) {
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

    /// Follows what another view of the same store did: a secret deleted,
    /// created or written elsewhere reads from the store again, and a shown
    /// secret that disappeared is dropped.
    pub(super) fn on_secret_change(&mut self, change: &SecretChange, cx: &mut Context<Self>) {
        if !change.is_in_scope(self.profile_id, &self.store.id, self.region.as_deref()) {
            return;
        }

        let shown = self.view.read(cx).name() == Some(change.name.as_str());

        if shown && change.kind == SecretChangeKind::Removed {
            let name = change.name.clone();
            self.view
                .update(cx, |view, cx| view.forget_removed_secret(&name, cx));
        }

        self.reload_listing_in_place(cx);

        if shown
            && matches!(
                change.kind,
                SecretChangeKind::Changed
                    | SecretChangeKind::Scheduled { .. }
                    | SecretChangeKind::Restored
            )
        {
            let (name, kind) = (change.name.clone(), change.kind);
            self.view
                .update(cx, |view, cx| view.follow_external_write(&name, kind, cx));
        }
    }

    /// Queues the request to open `name` in its own tab and wakes the
    /// workspace, which drains it on its next render.
    pub(super) fn queue_open_secret(&mut self, name: String, edit: bool, cx: &mut Context<Self>) {
        self.pending_open_secret = Some(SecretOpenRequest {
            store_id: self.store.id.clone(),
            // The tab opens on what the pane shows: an older version stays
            // the tab's identity and what it retrieves.
            version: self.view.read(cx).shown_version(),
            name,
            region: self.region.clone(),
            edit,
        });
        cx.emit(DocumentEvent::RequestFocus);
        cx.notify();
    }

    /// Opens the selected secret in its own tab. The one implementation
    /// behind the row menu's Open in tab and the pane action.
    pub fn open_selected_in_tab(&mut self, cx: &mut Context<Self>) {
        if let Some(SecretRowKey::Secret(name)) = self.selected.clone() {
            self.queue_open_secret(name, false, cx);
        }
    }

    pub fn take_pending_open_secret(&mut self) -> Option<SecretOpenRequest> {
        self.pending_open_secret.take()
    }
}

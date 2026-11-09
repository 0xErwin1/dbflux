//! `PaneHandle` constructor for `SecretDocument`.

use super::{SecretDocument, SecretDocumentBlock};
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneAction, PaneHandle, StatusSegment};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use dbflux_app::keymap::{Command, ContextId};
use dbflux_components::icons::AppIcon;
use gpui::{App, Entity, IntoElement};

impl SecretDocument {
    /// The controls of the tab and the secret it shows, for the menu `m`
    /// opens. The secret's own actions come from its view.
    pub(crate) fn pane_actions(&self, this: &Entity<Self>, cx: &App) -> Vec<PaneAction> {
        if self.block(cx) == Some(SecretDocumentBlock::Removed) {
            return vec![Self::close_pane_action(this)];
        }

        let view = self.view.read(cx);
        let target = self.view.downgrade();

        let mut actions = view.value_pane_actions(&target);
        actions.extend(view.copy_name_pane_actions(&target));
        actions.extend(view.history_pane_actions(&target));
        actions.extend(view.edit_pane_actions(&target));
        actions.extend(view.write_pane_actions(&target));
        actions.push(
            PaneAction::command(
                "secret-document-refresh",
                dbflux_i18n::t!("document.secret_store.toolbar.refresh"),
                Command::RefreshSchema,
                ContextId::Results,
            )
            .icon(AppIcon::RefreshCcw),
        );

        // A tab on another region than the connection's reads and writes
        // nothing, so nothing in its menu can be chosen.
        if self.block(cx).is_some() {
            return actions
                .into_iter()
                .map(|action| action.enabled(false))
                .collect();
        }

        actions
    }

    fn close_pane_action(this: &Entity<Self>) -> PaneAction {
        let target = this.downgrade();

        PaneAction::callback(
            "secret-document-close",
            dbflux_i18n::t!("document.secret.close"),
            move |_window, cx| {
                if let Some(document) = target.upgrade() {
                    document.update(cx, |_, cx| cx.emit(DocumentEvent::RequestClose));
                }
            },
        )
        .icon(AppIcon::X)
    }

    /// Status-bar segments: the connection and where the secret lives.
    pub fn status_segments(&self, cx: &App) -> Vec<StatusSegment> {
        let mut segments = Vec::new();

        if let Some(connected) = self.app_state.read(cx).connections().get(&self.profile_id) {
            segments.push(StatusSegment {
                text: format!(
                    "{} · {}",
                    connected.profile.name,
                    connected.connection.metadata().display_name
                )
                .into(),
                tooltip: None,
            });
        }

        segments
    }

    /// Wrap a typed `Entity<SecretDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::Secret,
            // render
            {
                let e = entity.clone();
                Box::new(move |_w, _cx| e.clone().into_any_element())
            },
            // focus
            {
                let e = entity.clone();
                Box::new(move |w, cx| e.update(cx, |d, cx| d.focus(w, cx)))
            },
            // dispatch_command
            {
                let e = entity.clone();
                Box::new(move |cmd, w, cx| e.update(cx, |d, cx| d.dispatch_command(cmd, w, cx)))
            },
            // meta_snapshot
            {
                let e = entity.clone();
                Box::new(move |cx| {
                    let d = e.read(cx);
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::Secret,
                        title: d.title(),
                        icon: DocumentIcon::Secret,
                        state: d.state(cx),
                        closable: true,
                        connection_id: d.connection_id(),
                    }
                })
            },
            // tab_title
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).title())
            },
            // can_close
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).can_close())
            },
            // connection_id
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).connection_id())
            },
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context(cx))
            },
            // change_summary — the open editor holds unsaved value edits while
            // it differs from the stored text: the tab's dirty dot and the
            // close prompt read it from here.
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).view.read(cx).unsaved_change_summary(cx))
            },
            // refresh_policy — a secret is read again on demand only
            Box::new(|_cx| dbflux_core::RefreshPolicy::Manual),
            // flush_auto_save — nothing to save
            Box::new(|_cx| {}),
            // set_active_tab
            {
                let e = entity.clone();
                Box::new(move |active, cx| e.update(cx, |d, _cx| d.set_active_tab(active)))
            },
            // set_refresh_policy — there is none to set
            Box::new(|_policy, _cx| {}),
            // matches_dedup_key — one tab per (profile, store, name, version, region)
            {
                let e = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::Secret {
                        profile_id,
                        store_id,
                        name,
                        version,
                        region,
                    } => {
                        let d = e.read(cx);
                        d.connection_id() == Some(*profile_id)
                            && d.store_id() == store_id
                            && d.secret_name() == name
                            && d.version() == version.as_deref()
                            && d.region() == region.as_deref()
                    }
                    _ => false,
                })
            },
            // subscribe — the document emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        );

        pane.pane_actions = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).pane_actions(&e, cx))
        });

        pane.quit_disposition = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).quit_disposition(cx))
        });

        pane.save_for_quit = Some({
            let e = entity.clone();
            Box::new(move |_w, cx| e.update(cx, |d, cx| d.save_for_quit(cx)))
        });

        pane.discard_for_quit = Some({
            let e = entity.clone();
            Box::new(move |cx| e.update(cx, |d, cx| d.discard_for_quit(cx)))
        });

        // The close dialog's Save: the tab closes once the write lands.
        pane.save_for_close = Some({
            let e = entity.clone();
            Box::new(move |_w, cx| e.update(cx, |d, cx| d.save_for_close(cx)))
        });

        pane.status_segments = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).status_segments(cx))
        });

        // A retrieved value lives in this tab only: closing it drops the
        // value before the entity goes.
        pane.on_close = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                let view = e.read(cx).view.clone();
                view.update(cx, |view, cx| {
                    view.clear_value();
                    cx.notify();
                })
            })
        });

        pane
    }
}

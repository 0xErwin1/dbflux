//! `PaneHandle` constructor for `DelimitedDocument`.

use super::document::DelimitedDocument;
use crate::dedup::DocumentKey;
use crate::dedup::FileDocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, CodeSessionTabSnapshot, PaneHandle};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl DelimitedDocument {
    /// Wrap a typed `Entity<DelimitedDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::Delimited,
            // render
            {
                let entity = entity.clone();
                Box::new(move |_window, _cx| entity.clone().into_any_element())
            },
            // focus
            {
                let entity = entity.clone();
                Box::new(move |window, cx| {
                    entity.update(cx, |document, cx| document.focus(window, cx))
                })
            },
            // dispatch_command
            {
                let entity = entity.clone();
                Box::new(move |command, window, cx| {
                    entity.update(cx, |document, cx| {
                        document.dispatch_command(command, window, cx)
                    })
                })
            },
            // meta_snapshot
            {
                let entity = entity.clone();
                Box::new(move |cx| {
                    let document = entity.read(cx);
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::Delimited,
                        title: document.title(),
                        icon: DocumentIcon::Table,
                        state: document.state(),
                        closable: true,
                        connection_id: document.connection_id(),
                    }
                })
            },
            // tab_title
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).title())
            },
            // can_close
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).can_close())
            },
            // connection_id — the profile of an object, `None` for a local file
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).connection_id())
            },
            // active_context
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).active_context())
            },
            // change_summary — `Some` while there are unsaved changes
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).change_summary())
            },
            // refresh_policy
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).refresh_policy())
            },
            // flush_auto_save — no auto-save
            Box::new(|_cx| {}),
            // set_active_tab
            {
                let entity = entity.clone();
                Box::new(move |active, cx| {
                    entity.update(cx, |document, _cx| document.set_active_tab(active))
                })
            },
            // set_refresh_policy
            {
                let entity = entity.clone();
                Box::new(move |policy, cx| {
                    entity.update(cx, |document, cx| document.set_refresh_policy(policy, cx))
                })
            },
            // matches_dedup_key — one tab per local path or per
            // (profile, bucket, key)
            {
                let entity = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::FileDocument(file) => entity.read(cx).file() == file,
                    _ => false,
                })
            },
            // subscribe — DelimitedDocument emits DocumentEvent directly
            {
                let entity = entity.clone();
                Box::new(move |cx, callback: BoxedDocEventCallback| {
                    cx.subscribe(&entity, move |_, event: &DocumentEvent, cx| {
                        callback(event, cx)
                    })
                })
            },
        );

        // The interrupted-close save: the tab closes once the save lands.
        pane.save_for_close = Some({
            let entity = entity.clone();
            Box::new(move |_window, cx| {
                entity.update(cx, |document, cx| document.save_for_close(cx))
            })
        });

        // A value still in the cell editor, committed before a close or a
        // shutdown reads the pending changes.
        pane.commit_pending_input = Some({
            let entity = entity.clone();
            Box::new(move |cx| entity.update(cx, |document, cx| document.commit_pending_input(cx)))
        });

        // The quit check: whether quitting saves the pending edits on its
        // own or asks first, the save and the discard of a confirmed quit,
        // and the save the shutdown flush runs.
        pane.quit_disposition = Some({
            let entity = entity.clone();
            Box::new(move |cx| entity.read(cx).quit_disposition(cx))
        });

        pane.save_for_quit = Some({
            let entity = entity.clone();
            Box::new(move |_window, cx| {
                entity.update(cx, |document, cx| document.save_for_quit(cx))
            })
        });

        pane.discard_for_quit = Some({
            let entity = entity.clone();
            Box::new(move |cx| entity.update(cx, |document, cx| document.discard_for_quit(cx)))
        });

        pane.flush_for_shutdown = Some({
            let entity = entity.clone();
            Box::new(move |cx| entity.update(cx, |document, cx| document.flush_for_shutdown(cx)))
        });

        // The Vim mode of the text view's editor.
        pane.key_context_entries = Some({
            let entity = entity.clone();
            Box::new(move |cx| entity.read(cx).key_context_entries(cx))
        });

        // The dialect and edit controls, which the pane actions menu
        // reaches from the keyboard.
        pane.pane_actions = Some({
            let entity = entity.clone();
            Box::new(move |cx| entity.read(cx).pane_actions(&entity))
        });

        // The workspace session reopens a local file by its path, with the
        // dialect detected again. An object is left out: it needs the live
        // connection of its profile, which is not there at startup, and the
        // object editor's tabs are left out for the same reason. Dialect
        // overrides, the view and unsaved edits are not recorded.
        pane.session_tab_snapshot = Some({
            let entity = entity.clone();
            Box::new(move |cx| {
                let document = entity.read(cx);

                let FileDocumentKey::Local { path } = document.file() else {
                    return None;
                };

                Some(CodeSessionTabSnapshot {
                    kind: DelimitedDocument::SESSION_TAB_KIND,
                    id: document.id(),
                    title: document.title(),
                    language: dbflux_core::QueryLanguage::Sql,
                    exec_ctx: dbflux_core::ExecutionContext::default(),
                    file_path: Some(path.clone()),
                    scratch_path: None,
                    shadow_path: None,
                })
            })
        });

        pane
    }
}

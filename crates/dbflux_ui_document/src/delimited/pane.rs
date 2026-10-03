//! `PaneHandle` constructor for `DelimitedDocument`.

use super::document::DelimitedDocument;
use crate::dedup::DelimitedFileKey;
use crate::dedup::DocumentKey;
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
                        kind: DocumentKind::Delimited,
                        title: d.title(),
                        icon: DocumentIcon::Table,
                        state: d.state(),
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
            // connection_id — the profile of an object, `None` for a local file
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).connection_id())
            },
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context())
            },
            // change_summary — `Some` while there are unsaved changes
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).change_summary())
            },
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save — no auto-save
            Box::new(|_cx| {}),
            // set_active_tab
            {
                let e = entity.clone();
                Box::new(move |active, cx| e.update(cx, |d, _cx| d.set_active_tab(active)))
            },
            // set_refresh_policy
            {
                let e = entity.clone();
                Box::new(move |policy, cx| e.update(cx, |d, cx| d.set_refresh_policy(policy, cx)))
            },
            // matches_dedup_key — one tab per local path or per
            // (profile, bucket, key)
            {
                let e = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::Delimited(file) => e.read(cx).file() == file,
                    _ => false,
                })
            },
            // subscribe — DelimitedDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        );

        // The interrupted-close save: the tab closes once the save lands.
        pane.save_for_close = Some({
            let e = entity.clone();
            Box::new(move |_w, cx| e.update(cx, |d, cx| d.save_for_close(cx)))
        });

        // The quit check: whether quitting saves the pending edits on its
        // own or asks first, the save and the discard of a confirmed quit,
        // and the save the shutdown flush runs.
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

        pane.flush_for_shutdown = Some({
            let e = entity.clone();
            Box::new(move |cx| e.update(cx, |d, cx| d.flush_for_shutdown(cx)))
        });

        // The Vim mode of the text view's editor.
        pane.key_context_entries = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).key_context_entries(cx))
        });

        // The dialect and edit controls, which the pane actions menu
        // reaches from the keyboard.
        pane.pane_actions = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).pane_actions(&e))
        });

        // The workspace session reopens a local file by its path, with the
        // dialect detected again. An object is left out: it needs the live
        // connection of its profile, which is not there at startup, and the
        // object editor's tabs are left out for the same reason. Dialect
        // overrides, the view and unsaved edits are not recorded.
        pane.session_tab_snapshot = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                let d = e.read(cx);

                let DelimitedFileKey::Local { path } = d.file() else {
                    return None;
                };

                Some(CodeSessionTabSnapshot {
                    kind: DelimitedDocument::SESSION_TAB_KIND,
                    id: d.id(),
                    title: d.title(),
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

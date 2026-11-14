//! `PaneHandle` constructor for `ConsoleDocument`.

use super::ConsoleDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use dbflux_core::RefreshPolicy;
use gpui::{App, Entity, IntoElement};

impl ConsoleDocument {
    /// Wrap a typed `Entity<ConsoleDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        PaneHandle::new_chart(
            id,
            DocumentKind::Console,
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
                        kind: DocumentKind::Console,
                        title: d.title(),
                        icon: DocumentIcon::Terminal,
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
            // change_summary
            Box::new(|_cx| None),
            // refresh_policy
            Box::new(|_cx| RefreshPolicy::default()),
            // flush_auto_save
            Box::new(|_cx| {}),
            // set_active_tab
            Box::new(|_active, _cx| {}),
            // set_refresh_policy
            Box::new(|_policy, _cx| {}),
            // matches_dedup_key
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::Console {
                            profile_id,
                            database,
                        } => {
                            d.connection_id() == Some(*profile_id)
                                && d.database() == database.as_deref()
                        }
                        _ => false,
                    }
                })
            },
            // subscribe
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        )
    }
}

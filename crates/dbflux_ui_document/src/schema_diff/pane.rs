//! `PaneHandle` constructor for `SchemaDiffDocument`.
//!
//! Mirrors `AuditDocument::into_pane`: converts the typed entity into the
//! type-erased `PaneHandle` shell with all operations captured as closures.
//! Dedup is routed through `DocumentKey::SchemaDiff`.

use super::view::SchemaDiffDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl SchemaDiffDocument {
    /// Wrap a typed `Entity<SchemaDiffDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        PaneHandle::new_chart(
            id,
            DocumentKind::SchemaDiff,
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
                        kind: DocumentKind::SchemaDiff,
                        title: d.title().to_string(),
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
                Box::new(move |cx| e.read(cx).title().to_string())
            },
            // can_close
            Box::new(|_cx| true),
            // connection_id
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).connection_id())
            },
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context())
            },
            // change_summary — no unsaved buffer state
            Box::new(|_cx| None),
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).current_refresh_policy())
            },
            // flush_auto_save — no auto-save
            Box::new(|_cx| {}),
            // set_active_tab — no per-tab active state
            Box::new(|_active, _cx| {}),
            // set_refresh_policy
            {
                let e = entity.clone();
                Box::new(move |policy, cx| e.update(cx, |d, cx| d.apply_refresh_policy(policy, cx)))
            },
            // matches_dedup_key
            {
                let e = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::SchemaDiff {
                        profile_id,
                        database,
                    } => e
                        .read(cx)
                        .matches_schema_diff(*profile_id, database.as_deref()),
                    _ => false,
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

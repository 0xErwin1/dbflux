//! `PaneHandle` constructor for `SchemaVizDocument`.
//!
//! `SchemaVizDocument::into_pane` converts a typed `Entity<SchemaVizDocument>`
//! into the type-erased `PaneHandle` shell. All closures capture the entity by
//! clone; `Window` and `App` are always passed as per-call parameters.

use super::{SchemaVizDocument, SchemaVizMode};
use crate::ui::document::dedup::DocumentKey;
use crate::ui::document::handle::DocumentEvent;
use crate::ui::document::pane::{BoxedDocEventCallback, PaneHandle};
use crate::ui::document::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use dbflux_core::RefreshPolicy;
use gpui::{App, Entity, IntoElement};

impl SchemaVizDocument {
    /// Wrap a typed `Entity<SchemaVizDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID synchronously from `cx` then seals all operations
    /// behind `Box<dyn Fn>` closures that capture `entity` by clone.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        PaneHandle::new_chart(
            id,
            DocumentKind::SchemaViz,
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
                    let title = match d.table_name() {
                        Some(t) => format!("Schema: {}", t),
                        None => {
                            format!("Schema: {}", d.database.as_deref().unwrap_or("database"))
                        }
                    };
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::SchemaViz,
                        title,
                        icon: DocumentIcon::SchemaViz,
                        state: d.state(),
                        closable: true,
                        connection_id: Some(d.profile_id),
                    }
                })
            },
            // tab_title
            {
                let e = entity.clone();
                Box::new(move |cx| {
                    let d = e.read(cx);
                    match d.table_name() {
                        Some(t) => format!("Schema: {}", t),
                        None => format!("Schema: {}", d.database.as_deref().unwrap_or("database")),
                    }
                })
            },
            // can_close — SchemaViz is always closable (no unsaved state)
            Box::new(|_cx| true),
            // connection_id
            {
                let e = entity.clone();
                Box::new(move |cx| Some(e.read(cx).profile_id))
            },
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context())
            },
            // change_summary — SchemaViz has no unsaved-change tracking
            Box::new(|_cx| None),
            // refresh_policy — SchemaViz refreshes manually only
            Box::new(|_cx| RefreshPolicy::Manual),
            // flush_auto_save — SchemaViz has no auto-save
            Box::new(|_cx| {}),
            // set_active_tab — SchemaViz does not track active-tab state
            Box::new(|_active, _cx| {}),
            // set_refresh_policy — SchemaViz does not use a refresh policy
            Box::new(|_policy, _cx| {}),
            // matches_dedup_key
            {
                let e = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::SchemaViz {
                        profile_id,
                        database,
                        schema,
                        table,
                    } => {
                        let d = e.read(cx);
                        if d.profile_id != *profile_id
                            || d.database.as_deref() != database.as_deref()
                        {
                            return false;
                        }
                        match (&d.mode, table) {
                            (SchemaVizMode::Focused { table: t, schema: s }, Some(qt)) => {
                                t == qt && s.as_deref() == schema.as_deref()
                            }
                            (SchemaVizMode::Global, None) => true,
                            _ => false,
                        }
                    }
                    _ => false,
                })
            },
            // subscribe — SchemaVizDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        )
    }
}

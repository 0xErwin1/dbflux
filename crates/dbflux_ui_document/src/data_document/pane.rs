//! `PaneHandle` constructor for `DataDocument`.
//!
//! `DataDocument::into_pane` converts a typed `Entity<DataDocument>` into the
//! type-erased `PaneHandle` shell. All closures capture the entity by clone;
//! `Window` and `App` are always passed as per-call parameters.

use super::DataDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle};
use crate::types::{
    DataSourceKind, DocumentIcon, DocumentKind, DocumentMetaSnapshot,
};
use gpui::{App, Entity, IntoElement};

impl DataDocument {
    /// Wrap a typed `Entity<DataDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID synchronously from `cx` then seals all operations
    /// behind `Box<dyn Fn>` closures capturing `entity` by clone.
    ///
    /// The `matches_dedup_key` closure handles both `Table` and `Collection`
    /// keys, mirroring the former `is_table_with_database` and `is_collection`
    /// predicates in `DocumentHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        PaneHandle::new_chart(
            id,
            DocumentKind::Data,
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
                    let icon = match d.source_kind() {
                        DataSourceKind::Table => DocumentIcon::Table,
                        DataSourceKind::Collection => DocumentIcon::Collection,
                        DataSourceKind::QueryResult => DocumentIcon::Table,
                    };
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::Data,
                        title: d.title(),
                        icon,
                        state: d.state(),
                        closable: true,
                        connection_id: d.connection_id(cx),
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
                Box::new(move |_cx| e.read(_cx).can_close())
            },
            // connection_id
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).connection_id(cx))
            },
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context(cx))
            },
            // change_summary
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).change_summary(cx))
            },
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy(cx))
            },
            // flush_auto_save — DataDocument has no auto-save
            Box::new(|_cx| {}),
            // set_active_tab
            {
                let e = entity.clone();
                Box::new(move |active, cx| e.update(cx, |d, cx| d.set_active_tab(active, cx)))
            },
            // set_refresh_policy
            {
                let e = entity.clone();
                Box::new(move |policy, cx| e.update(cx, |d, cx| d.set_refresh_policy(policy, cx)))
            },
            // matches_dedup_key — handles Table and Collection variants
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::Table {
                            profile_id,
                            database,
                            table,
                        } => {
                            d.connection_id(cx) == Some(*profile_id)
                                && d.table_ref(cx).as_ref() == Some(table)
                                && (database.is_none()
                                    || d.database(cx).as_deref() == database.as_deref())
                        }
                        DocumentKey::Collection {
                            profile_id,
                            collection,
                        } => {
                            d.connection_id(cx) == Some(*profile_id)
                                && d.collection_ref(cx).as_ref() == Some(collection)
                        }
                        _ => false,
                    }
                })
            },
            // subscribe — DataDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        )
    }
}

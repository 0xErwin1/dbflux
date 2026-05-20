//! `PaneHandle` constructor for `KeyValueDocument`.
//!
//! `KeyValueDocument::into_pane` converts a typed `Entity<KeyValueDocument>`
//! into the type-erased `PaneHandle` shell. All closures capture the entity
//! by clone; `Window` and `App` are always passed as per-call parameters.

use super::KeyValueDocument;
use crate::ui::document::dedup::DocumentKey;
use crate::ui::document::handle::DocumentEvent;
use crate::ui::document::pane::{BoxedDocEventCallback, PaneHandle};
use crate::ui::document::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl KeyValueDocument {
    /// Wrap a typed `Entity<KeyValueDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID synchronously from `cx` then seals all operations
    /// behind `Box<dyn Fn>` closures capturing `entity` by clone.
    ///
    /// `KeyValueDocument` self-renders (its `Render` impl lives in
    /// `key_value/render.rs`), so the render closure calls `into_any_element()`
    /// directly on the entity — the same pattern used by `CodeDocument::into_pane`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        PaneHandle::new_chart(
            id,
            DocumentKind::RedisKeyBrowser,
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
                        kind: DocumentKind::RedisKeyBrowser,
                        title: d.title(),
                        icon: DocumentIcon::Redis,
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
            // change_summary — KeyValueDocument has no unsaved changes
            Box::new(|_cx| None),
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save — KeyValueDocument has no auto-save
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
            // matches_dedup_key — handles KeyValueDb variant
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::KeyValueDb {
                            profile_id,
                            database,
                        } => {
                            d.connection_id() == Some(*profile_id)
                                && d.database_name() == database.as_str()
                        }
                        _ => false,
                    }
                })
            },
            // subscribe — KeyValueDocument now emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        )
    }
}

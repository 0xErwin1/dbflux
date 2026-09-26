//! `PaneHandle` constructor for `MigrateWizard`.

use super::MigrateWizard;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use dbflux_core::RefreshPolicy;
use dbflux_core::keymap_types::ContextId;
use gpui::{App, Entity, IntoElement};

impl MigrateWizard {
    /// Wrap a typed `Entity<MigrateWizard>` in a `PaneHandle`.
    ///
    /// Closing the tab never cancels a live run: the run finalizes its own
    /// task, so it keeps going in the Tasks panel, where it can still be
    /// cancelled.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        PaneHandle::new_chart(
            id,
            DocumentKind::MigrateWizard,
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
            // dispatch_command — every phase handles its own keys.
            Box::new(|_cmd, _w, _cx| false),
            // meta_snapshot
            {
                let e = entity.clone();
                Box::new(move |cx| {
                    let d = e.read(cx);
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::MigrateWizard,
                        title: d.title(),
                        icon: DocumentIcon::Migrate,
                        state: d.state(cx),
                        closable: true,
                        connection_id: None,
                    }
                })
            },
            // tab_title
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).title())
            },
            // can_close
            Box::new(|_cx| true),
            // connection_id — the wizard spans a source and a target connection
            Box::new(|_cx| None),
            // active_context
            Box::new(|_cx| ContextId::Global),
            // change_summary
            Box::new(|_cx| None),
            // refresh_policy
            Box::new(|_cx| RefreshPolicy::Manual),
            // flush_auto_save
            Box::new(|_cx| {}),
            // set_active_tab
            Box::new(|_active, _cx| {}),
            // set_refresh_policy
            Box::new(|_policy, _cx| {}),
            // matches_dedup_key
            {
                let e = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::MigrateWizard {
                        profile_id,
                        database,
                        tables,
                    } => e
                        .read(cx)
                        .matches_source(*profile_id, database.as_deref(), tables),
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

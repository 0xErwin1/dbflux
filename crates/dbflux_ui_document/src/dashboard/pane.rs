//! `PaneHandle` constructor for `DashboardDocument`.
//!
//! `DashboardDocument::into_pane` converts a typed `Entity<DashboardDocument>`
//! into the type-erased `PaneHandle` shell. All closures capture the entity by
//! clone; `Window` and `App` are always passed as per-call parameters.

use super::DashboardDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl DashboardDocument {
    /// Wrap a typed `Entity<DashboardDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID and dashboard ID synchronously from `cx`, then
    /// seals all operations behind `Box<dyn Fn>` closures that capture `entity`
    /// by clone. Uses `PaneHandle::new_chart` because `DashboardDocument` is a
    /// simple document (no audit-specific helpers needed).
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let doc = entity.read(cx);
        let id = doc.id();
        let dashboard_id = doc.dashboard_id();
        let is_read_only = doc.read_only;

        PaneHandle::new_chart(
            id,
            DocumentKind::Dashboard,
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
                        kind: DocumentKind::Dashboard,
                        title: d.title(),
                        icon: DocumentIcon::Dashboard,
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
                Box::new(move |cx| e.read(cx).active_context())
            },
            // change_summary — DashboardDocument has no unsaved-change tracking
            Box::new(|_cx| None),
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save — DashboardDocument has no auto-save
            Box::new(|_cx| {}),
            // set_active_tab
            {
                let e = entity.clone();
                Box::new(move |active, cx| e.update(cx, |d, _| d.set_active_tab(active)))
            },
            // set_refresh_policy
            {
                let e = entity.clone();
                Box::new(move |policy, cx| e.update(cx, |d, cx| d.set_refresh_policy(policy, cx)))
            },
            // matches_dedup_key — deduplicated by `dashboard_id` for normal dashboards,
            // or by the `InstanceOverview` key for read-only catalog-synthesized dashboards.
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    if is_read_only {
                        // Read-only overview: deduplicate by profile_id embedded in
                        // the connection_id() (which is the profile this catalog belongs to).
                        if let DocumentKey::InstanceOverview { profile_id } = key {
                            return d.connection_id() == Some(*profile_id);
                        }
                        return false;
                    }
                    matches!(
                        key,
                        DocumentKey::Dashboard { dashboard_id: kid } if *kid == dashboard_id
                    )
                })
            },
            // subscribe — DashboardDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        )
    }
}

//! `PaneHandle` constructor for `McpApprovalsView`.

use super::McpApprovalsView;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use dbflux_core::RefreshPolicy;
use gpui::{App, Entity, IntoElement};

impl McpApprovalsView {
    /// Wrap a typed `Entity<McpApprovalsView>` in a `PaneHandle`.
    ///
    /// The approvals document is a singleton: it answers only
    /// `DocumentKey::McpApprovals`, carries no connection, and has nothing to
    /// save, so every close goes through without a prompt.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::McpApprovals,
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
                        kind: DocumentKind::McpApprovals,
                        title: d.title(),
                        icon: DocumentIcon::McpApprovals,
                        state: d.state(),
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
            // connection_id
            Box::new(|_cx| None),
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context())
            },
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
            Box::new(|key, _cx| matches!(key, DocumentKey::McpApprovals)),
            // subscribe
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        );

        pane.pane_actions = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).pane_actions(&e))
        });

        pane
    }
}

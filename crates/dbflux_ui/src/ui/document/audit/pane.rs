//! `PaneHandle` constructor for `AuditDocument`.
//!
//! `AuditDocument::into_pane` converts a typed `Entity<AuditDocument>`
//! into the type-erased `PaneHandle` shell. All closures capture the entity
//! by clone; `Window` and `App` are always passed as per-call parameters.

use super::AuditDocument;
use crate::ui::document::dedup::DocumentKey;
use crate::ui::document::handle::DocumentEvent;
use crate::ui::document::pane::{BoxedDocEventCallback, PaneHandle};
use crate::ui::document::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl AuditDocument {
    /// Wrap a typed `Entity<AuditDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID synchronously from `cx` then seals all operations
    /// behind `Box<dyn Fn>` closures capturing `entity` by clone.
    ///
    /// `AuditDocument` self-renders, so the render closure calls
    /// `into_any_element()` directly on the entity — the same pattern used by
    /// `CodeDocument::into_pane` and `KeyValueDocument::into_pane`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::Audit,
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
                        kind: DocumentKind::Audit,
                        title: d.title().to_string(),
                        icon: DocumentIcon::Audit,
                        state: d.state(),
                        closable: true,
                        connection_id: None,
                    }
                })
            },
            // tab_title
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).title().to_string())
            },
            // can_close — AuditDocument is always closable
            Box::new(|_cx| true),
            // connection_id — AuditDocument is not connection-scoped
            Box::new(|_cx| None),
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context())
            },
            // change_summary — AuditDocument has no unsaved changes
            Box::new(|_cx| None),
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).current_refresh_policy())
            },
            // flush_auto_save — AuditDocument has no auto-save
            Box::new(|_cx| {}),
            // set_active_tab — AuditDocument does not need tab-active state
            Box::new(|_active, _cx| {}),
            // set_refresh_policy
            {
                let e = entity.clone();
                Box::new(move |policy, cx| e.update(cx, |d, cx| d.apply_refresh_policy(policy, cx)))
            },
            // matches_dedup_key — handles Audit (singleton) and EventStream variants
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::Audit => d.is_internal(),
                        DocumentKey::EventStream { profile_id, target } => {
                            d.matches_event_stream(*profile_id, target)
                        }
                        _ => false,
                    }
                })
            },
            // subscribe — AuditDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        );

        // ── Optional pane extensions for AuditDocument ───────────────────

        // set_category_filter — resets the filter when the existing audit tab
        // is focused via open_audit_viewer (e.g., "clear to show all events").
        pane.set_category_filter = {
            use dbflux_core::observability::EventCategory;
            let e = entity.clone();
            Some(Box::new(move |cat: Option<String>, cx: &mut App| {
                let category = cat.and_then(|s| EventCategory::from_str_repr(&s));
                e.update(cx, |d, cx| d.set_category_filter(category, cx));
            }))
        };

        // matches_event_stream — used for deduplication of event-stream tabs
        // when opening via open_event_stream_document.
        pane.matches_event_stream = {
            let e = entity.clone();
            Some(Box::new(
                move |profile_id: uuid::Uuid,
                      target: &dbflux_core::EventStreamTarget,
                      cx: &App| {
                    e.read(cx).matches_event_stream(profile_id, target)
                },
            ))
        };

        pane
    }
}

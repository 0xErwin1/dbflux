//! `PaneHandle` constructor for `SecretStoreDocument`.

use super::SecretStoreDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle, StatusSegment};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl SecretStoreDocument {
    /// Status-bar segments: the connection, how many secrets are listed and
    /// the timing of the last store call.
    pub fn status_segments(&self, cx: &App) -> Vec<StatusSegment> {
        let mut segments = Vec::new();

        if let Some(connected) = self.app_state.read(cx).connections().get(&self.profile_id) {
            // The region is not repeated here: the breadcrumb of the document
            // already carries it, and two places showing it can disagree for
            // the moment a switch lands.
            let text = format!(
                "{} · {}",
                connected.profile.name,
                connected.connection.metadata().display_name
            );

            segments.push(StatusSegment {
                text: text.into(),
                tooltip: None,
            });
        }

        segments.push(StatusSegment {
            text: super::render::loaded_label(self.listing.entries.len()).into(),
            tooltip: None,
        });

        if let Some(timing) = self.last_call {
            segments.push(StatusSegment {
                text: super::render::call_timing_label(timing).into(),
                tooltip: Some(
                    dbflux_i18n::t!("document.secret_store.status.duration_tooltip").into(),
                ),
            });
        }

        segments
    }

    /// Wrap a typed `Entity<SecretStoreDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();
        let store_id = entity.read(cx).store_id().to_string();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::SecretStore,
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
                        kind: DocumentKind::SecretStore,
                        title: d.title(),
                        icon: DocumentIcon::SecretStore,
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
            // change_summary — the document never holds unsaved changes
            Box::new(|_cx| None),
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save — nothing to save
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
            // matches_dedup_key — one tab per (profile, store)
            {
                let e = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::SecretStore {
                        profile_id,
                        store_id: key_store,
                    } => e.read(cx).connection_id() == Some(*profile_id) && *key_store == store_id,
                    _ => false,
                })
            },
            // subscribe — the document emits DocumentEvent directly
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

        pane.status_segments = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).status_segments(cx))
        });

        pane.navigate_secret_path = Some({
            let e = entity.clone();
            Box::new(move |path, cx| e.update(cx, |d, cx| d.navigate_to(path, cx)))
        });

        // A retrieved value lives in this tab only: closing it drops the
        // value before the entity goes.
        pane.on_close = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                e.update(cx, |d, cx| {
                    d.clear_value();
                    cx.notify();
                })
            })
        });

        pane
    }
}

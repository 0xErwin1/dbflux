//! `PaneHandle` constructor for `BucketsTableDocument`.

use super::BucketsTableDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle, StatusSegment};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl BucketsTableDocument {
    /// Status-bar segments contributed by this document (DEC-23): the engine
    /// behind the connection, how many buckets are listed, and the timing of
    /// the last object-store call.
    pub fn status_segments(&self, cx: &App) -> Vec<StatusSegment> {
        let mut segments = Vec::new();

        if let Some(connected) = self.app_state.read(cx).connections().get(&self.profile_id) {
            segments.push(StatusSegment {
                text: connected.connection.metadata().display_name.clone().into(),
                tooltip: None,
            });
        }

        segments.push(StatusSegment {
            text: format!("{} buckets", self.buckets().len()).into(),
            tooltip: None,
        });

        if let Some(timing) = self.last_operation {
            segments.push(StatusSegment {
                text: timing.display().into(),
                tooltip: Some("Client-side duration of the last object-store call".into()),
            });
        }

        segments
    }

    /// Wrap a typed `Entity<BucketsTableDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::ObjectStorageBuckets,
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
                        kind: DocumentKind::ObjectStorageBuckets,
                        title: d.title(),
                        icon: DocumentIcon::Buckets,
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
            // change_summary — BucketsTableDocument has no unsaved changes
            Box::new(|_cx| None),
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save — BucketsTableDocument has no auto-save
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
            // matches_dedup_key — handles the ObjectStoreBucketsRoot variant
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::ObjectStoreBucketsRoot { profile_id } => {
                            d.connection_id() == Some(*profile_id)
                        }
                        _ => false,
                    }
                })
            },
            // subscribe — BucketsTableDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        );

        pane.status_segments = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).status_segments(cx))
        });

        pane.take_pending_open_bucket = Some({
            let e = entity.clone();
            Box::new(move |cx| e.update(cx, |d, _cx| d.take_pending_open_bucket()))
        });

        pane
    }
}

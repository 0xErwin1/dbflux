//! `PaneHandle` constructor for `ObjectBrowserDocument`.

use super::ObjectBrowserDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, ObjectEditorRequest, PaneHandle, StatusSegment};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};
use std::rc::Rc;

impl ObjectBrowserDocument {
    /// Status-bar segments contributed by this document (DEC-23): the engine
    /// behind the connection, the current `s3://bucket/prefix/` path, the key
    /// count of the current level, and the last object-store call's timing.
    pub fn status_segments(&self, cx: &App) -> Vec<StatusSegment> {
        let mut segments = Vec::new();

        if let Some(connected) = self.app_state.read(cx).connections().get(&self.profile_id) {
            segments.push(StatusSegment {
                text: connected.connection.metadata().display_name.clone().into(),
                tooltip: None,
            });
        }

        segments.push(StatusSegment {
            text: format!("s3://{}/{}", self.bucket, self.tree.current_prefix).into(),
            tooltip: None,
        });

        let key_count = self
            .tree
            .level(&self.tree.current_prefix)
            .map(|level| level.entries.len())
            .unwrap_or(0);
        let key_word = if key_count == 1 { "key" } else { "keys" };
        segments.push(StatusSegment {
            text: format!("{key_count} {key_word}").into(),
            tooltip: Some("Keys listed at the current prefix level".into()),
        });

        if let Some(timing) = self.last_operation {
            segments.push(StatusSegment {
                text: timing.display().into(),
                tooltip: Some("Client-side duration of the last object-store call".into()),
            });
        }

        segments
    }

    /// Wrap a typed `Entity<ObjectBrowserDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();
        let bucket = entity.read(cx).bucket().to_string();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::ObjectBrowser,
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
                        kind: DocumentKind::ObjectBrowser,
                        title: d.title(),
                        icon: DocumentIcon::ObjectBrowser,
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
            // change_summary — unsaved inline-editor edits, which also route a
            // tab close through the workspace's unsaved-changes modal
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).change_summary())
            },
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save — no auto-save yet
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
            // matches_dedup_key — handles the ObjectBrowser variant
            {
                let e = entity.clone();
                let bucket = bucket.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::ObjectBrowser {
                            profile_id,
                            bucket: key_bucket,
                        } => d.connection_id() == Some(*profile_id) && *key_bucket == bucket,
                        _ => false,
                    }
                })
            },
            // subscribe — ObjectBrowserDocument emits DocumentEvent directly
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

        // Open-in-editor intent. `on_saved` points back at this browser so a
        // save in the standalone tab refreshes the metadata panel here when
        // the same object is still being previewed.
        pane.take_pending_open_object_editor = Some({
            let e = entity.clone();
            let bucket = bucket.clone();

            Box::new(move |cx| {
                let key = e.update(cx, |d, _cx| d.take_pending_open_object_editor())?;

                let refresh_target = e.downgrade();

                Some(ObjectEditorRequest {
                    bucket: bucket.clone(),
                    key,
                    on_saved: Rc::new(move |key: &str, cx: &mut App| {
                        let key = key.to_string();

                        refresh_target
                            .update(cx, |browser, cx| {
                                browser.refresh_previewed_object(&key, cx);
                            })
                            .ok();
                    }),
                })
            })
        });

        pane
    }
}

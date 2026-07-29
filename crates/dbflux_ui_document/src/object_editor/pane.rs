//! `PaneHandle` constructor for `ObjectEditorDocument`.

use super::ObjectEditorDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, PaneHandle, StatusSegment};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl ObjectEditorDocument {
    /// Status-bar segments: the bucket, the full key (which the tab title
    /// truncates to its leaf), and the object's current size.
    pub fn status_segments(&self, _cx: &App) -> Vec<StatusSegment> {
        let mut segments = vec![
            StatusSegment {
                text: self.bucket().to_string().into(),
                tooltip: Some("Bucket".into()),
            },
            StatusSegment {
                text: self.key().to_string().into(),
                tooltip: Some("Object key".into()),
            },
        ];

        if let Some(byte_len) = self.byte_len() {
            segments.push(StatusSegment {
                text: crate::buckets_table::format_bytes(byte_len).into(),
                tooltip: Some("Size of the object as last loaded or saved".into()),
            });
        }

        segments
    }

    /// Wrap a typed `Entity<ObjectEditorDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();
        let bucket = entity.read(cx).bucket().to_string();
        let key = entity.read(cx).key().to_string();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::ObjectEditor,
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
                        kind: DocumentKind::ObjectEditor,
                        title: d.title(),
                        // The tab is a code editor on a remote file; the Sql
                        // icon is the app's generic code-buffer glyph.
                        icon: DocumentIcon::Sql,
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
            // change_summary — unsaved edits, which also route a tab close
            // through the workspace's unsaved-changes modal
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).change_summary())
            },
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save — no auto-save: a save is a `put_object`
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
            // matches_dedup_key — one tab per (profile, bucket, key)
            {
                let e = entity.clone();
                Box::new(move |dedup_key, cx| {
                    let d = e.read(cx);
                    match dedup_key {
                        DocumentKey::ObjectEditor {
                            profile_id,
                            bucket: key_bucket,
                            key: key_key,
                        } => {
                            d.connection_id() == Some(*profile_id)
                                && *key_bucket == bucket
                                && *key_key == key
                        }
                        _ => false,
                    }
                })
            },
            // subscribe — ObjectEditorDocument emits DocumentEvent directly
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

        pane
    }
}

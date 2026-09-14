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
                tooltip: Some(
                    dbflux_i18n::t!("document.object_editor.status.bucket_tooltip").into(),
                ),
            },
            StatusSegment {
                text: self.key().to_string().into(),
                tooltip: Some(dbflux_i18n::t!("document.object_editor.status.key_tooltip").into()),
            },
        ];

        if let Some(byte_len) = self.byte_len() {
            segments.push(StatusSegment {
                text: crate::buckets_table::format_bytes(byte_len).into(),
                tooltip: Some(dbflux_i18n::t!("document.object_editor.status.size_tooltip").into()),
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

#[cfg(test)]
mod tests {
    /// The three status-bar tooltips resolve in both locales.
    ///
    /// `bucket_tooltip` is excluded from the divergence check: "bucket"
    /// stays "bucket" in Spanish per the change's vocabulary rule (see
    /// `document.object_browser.empty.bucket` for the same precedent), so
    /// its `en` and `es` values are identical by design, not a translation
    /// gap.
    #[test]
    fn status_tooltip_keys_resolve_in_both_locales() {
        for key in [
            "document.object_editor.status.bucket_tooltip",
            "document.object_editor.status.key_tooltip",
            "document.object_editor.status.size_tooltip",
        ] {
            let en = dbflux_i18n::t!(key, locale = "en");
            let es = dbflux_i18n::t!(key, locale = "es");

            assert!(!en.is_empty());
            assert_ne!(en, key);
            assert_ne!(en, format!("en.{key}"));

            if key != "document.object_editor.status.bucket_tooltip" {
                assert_ne!(en, es);
            }
        }
    }
}

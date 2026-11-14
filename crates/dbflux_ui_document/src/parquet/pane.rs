//! `PaneHandle` constructor for `ParquetDocument`.
//!
//! The document is read-only, so the pane has no save, quit or pending-input
//! hooks.

use super::document::ParquetDocument;
use crate::dedup::DocumentKey;
use crate::dedup::FileDocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, CodeSessionTabSnapshot, PaneHandle};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl ParquetDocument {
    /// Wrap a typed `Entity<ParquetDocument>` in a `PaneHandle`.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut pane = PaneHandle::new_chart(
            id,
            DocumentKind::Parquet,
            // render
            {
                let entity = entity.clone();
                Box::new(move |_window, _cx| entity.clone().into_any_element())
            },
            // focus
            {
                let entity = entity.clone();
                Box::new(move |window, cx| {
                    entity.update(cx, |document, cx| document.focus(window, cx))
                })
            },
            // dispatch_command
            {
                let entity = entity.clone();
                Box::new(move |command, window, cx| {
                    entity.update(cx, |document, cx| {
                        document.dispatch_command(command, window, cx)
                    })
                })
            },
            // meta_snapshot
            {
                let entity = entity.clone();
                Box::new(move |cx| {
                    let document = entity.read(cx);
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::Parquet,
                        title: document.title(),
                        icon: DocumentIcon::Table,
                        state: document.state(),
                        closable: true,
                        connection_id: document.connection_id(),
                    }
                })
            },
            // tab_title
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).title())
            },
            // can_close — nothing is ever unsaved
            Box::new(|_cx| true),
            // connection_id — the profile of an object, `None` for a local file
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).connection_id())
            },
            // active_context
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).active_context())
            },
            // change_summary — read-only, never dirty
            Box::new(|_cx| None),
            // refresh_policy
            {
                let entity = entity.clone();
                Box::new(move |cx| entity.read(cx).refresh_policy())
            },
            // flush_auto_save — no auto-save
            Box::new(|_cx| {}),
            // set_active_tab — nothing depends on it
            Box::new(|_active, _cx| {}),
            // set_refresh_policy
            {
                let entity = entity.clone();
                Box::new(move |policy, cx| {
                    entity.update(cx, |document, cx| document.set_refresh_policy(policy, cx))
                })
            },
            // matches_dedup_key — one tab per local path or per
            // (profile, bucket, key)
            {
                let entity = entity.clone();
                Box::new(move |key, cx| match key {
                    DocumentKey::FileDocument(file) => entity.read(cx).file() == file,
                    _ => false,
                })
            },
            // subscribe — ParquetDocument emits DocumentEvent directly
            {
                let entity = entity.clone();
                Box::new(move |cx, callback: BoxedDocEventCallback| {
                    cx.subscribe(&entity, move |_, event: &DocumentEvent, cx| {
                        callback(event, cx)
                    })
                })
            },
        );

        // The workspace session reopens a local file by its path, with the
        // default projection picked again. An object is left out: it needs the
        // live connection of its profile, which is not there at startup. The
        // applied projection and the rows loaded are not recorded.
        pane.session_tab_snapshot = Some({
            let entity = entity.clone();
            Box::new(move |cx| {
                let document = entity.read(cx);

                let FileDocumentKey::Local { path } = document.file() else {
                    return None;
                };

                Some(CodeSessionTabSnapshot {
                    kind: ParquetDocument::SESSION_TAB_KIND,
                    id: document.id(),
                    title: document.title(),
                    language: dbflux_core::QueryLanguage::Sql,
                    exec_ctx: dbflux_core::ExecutionContext::default(),
                    file_path: Some(path.clone()),
                    scratch_path: None,
                    shadow_path: None,
                })
            })
        });

        pane
    }
}

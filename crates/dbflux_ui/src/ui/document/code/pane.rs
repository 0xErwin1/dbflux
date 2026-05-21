//! `PaneHandle` constructor for `CodeDocument`.
//!
//! `CodeDocument::into_pane` converts a typed `Entity<CodeDocument>` into the
//! type-erased `PaneHandle` shell. All closures capture the entity by clone;
//! `Window` and `App` are always passed as per-call parameters.

use super::CodeDocument;
use crate::ui::document::dedup::DocumentKey;
use crate::ui::document::handle::DocumentEvent;
use crate::ui::document::pane::{BoxedDocEventCallback, CodeSessionTabSnapshot, PaneHandle};
use crate::ui::document::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl CodeDocument {
    /// Wrap a typed `Entity<CodeDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID synchronously from `cx` then seals all operations
    /// behind `Box<dyn Fn>` closures capturing `entity` by clone.
    ///
    /// The optional `is_file_backed_empty` and `session_tab_snapshot` helpers
    /// are populated so that `write_session_manifest` and the empty-file-close
    /// cleanup in `actions.rs` can operate without pattern-matching on the
    /// `DocumentHandle::Code` variant.
    pub fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        let id = entity.read(cx).id();

        let mut handle = PaneHandle::new_chart(
            id,
            DocumentKind::Script,
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
                    let icon = if d.is_file_backed() {
                        DocumentIcon::Script
                    } else {
                        DocumentIcon::Sql
                    };
                    DocumentMetaSnapshot {
                        id,
                        kind: DocumentKind::Script,
                        title: d.title(),
                        icon,
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
                Box::new(move |cx| e.read(cx).can_close(cx))
            },
            // connection_id
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).connection_id())
            },
            // active_context
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).active_context(cx))
            },
            // change_summary
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).change_summary(cx))
            },
            // refresh_policy
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).refresh_policy())
            },
            // flush_auto_save
            {
                let e = entity.clone();
                Box::new(move |cx| e.read(cx).flush_auto_save(cx))
            },
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
            // matches_dedup_key
            {
                let e = entity.clone();
                Box::new(move |key, cx| {
                    let d = e.read(cx);
                    match key {
                        DocumentKey::File { path } => {
                            d.path().map(|p| p.as_path()) == Some(path.as_path())
                        }
                        DocumentKey::Routine {
                            profile_id,
                            schema,
                            specific_name,
                        } => d.routine_dedup.as_ref().is_some_and(|(pid, s, sn)| {
                            pid == profile_id && s == schema && sn == specific_name
                        }),
                        _ => false,
                    }
                })
            },
            // subscribe — CodeDocument emits DocumentEvent directly
            {
                let e = entity.clone();
                Box::new(move |cx, cb: BoxedDocEventCallback| {
                    cx.subscribe(&e, move |_, ev: &DocumentEvent, cx| cb(ev, cx))
                })
            },
        );

        // Populate optional helper: empty file-backed detection used by the
        // cleanup path in actions.rs that deletes empty script files on close.
        handle.is_file_backed_empty = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                let d = e.read(cx);
                if d.is_file_backed() && d.is_content_empty(cx) {
                    d.path().cloned()
                } else {
                    None
                }
            })
        });

        // Populate optional helper: session manifest serialization data.
        // Returns `None` for unsaved scratch tabs (no path or scratch_path), unless
        // this is a routine document, which is always persisted as `"Routine"` kind
        // so it can be reconstructed on next launch without a file backing.
        handle.session_tab_snapshot = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                let d = e.read(cx);

                // Routine documents: persisted with their descriptor encoded in
                // exec_ctx (connection_id=profile_id, schema, container=specific_name).
                if let Some((profile_id, schema, specific_name)) = d.routine_dedup.as_ref() {
                    use dbflux_core::ExecutionContext;

                    let exec_ctx = ExecutionContext {
                        connection_id: Some(*profile_id),
                        schema: Some(schema.clone()),
                        container: Some(specific_name.clone()),
                        ..d.exec_ctx().clone()
                    };

                    return Some(CodeSessionTabSnapshot {
                        kind: "Routine",
                        id: d.id(),
                        title: d.title(),
                        language: d.query_language(),
                        exec_ctx,
                        file_path: None,
                        scratch_path: None,
                        shadow_path: None,
                    });
                }

                let kind = if d.path().is_some() {
                    "FileBacked"
                } else if d.scratch_path().is_some() {
                    "Scratch"
                } else {
                    // Tab has neither a file path nor a scratch path — skip.
                    return None;
                };

                Some(CodeSessionTabSnapshot {
                    kind,
                    id: d.id(),
                    title: d.title(),
                    language: d.query_language(),
                    exec_ctx: d.exec_ctx().clone(),
                    file_path: d.path().cloned(),
                    scratch_path: d.scratch_path().cloned(),
                    shadow_path: d.shadow_path().cloned(),
                })
            })
        });

        handle
    }
}

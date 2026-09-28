//! `PaneHandle` constructor for `CodeDocument`.
//!
//! `CodeDocument::into_pane` converts a typed `Entity<CodeDocument>` into the
//! type-erased `PaneHandle` shell. All closures capture the entity by clone;
//! `Window` and `App` are always passed as per-call parameters.

use super::CodeDocument;
use crate::dedup::DocumentKey;
use crate::handle::DocumentEvent;
use crate::pane::{BoxedDocEventCallback, CodeSessionTabSnapshot, PaneHandle, StatusSegment};
use crate::types::{DocumentIcon, DocumentKind, DocumentMetaSnapshot};
use gpui::{App, Entity, IntoElement};

impl CodeDocument {
    /// Status-bar segment of the editor: the text encoding and the cursor
    /// position, 1-based ("UTF-8 · Ln 16, Col 23").
    pub fn status_segments(&self, cx: &App) -> Vec<StatusSegment> {
        let position = self.editor.input_state.read(cx).cursor_position();

        vec![StatusSegment {
            text: format!(
                "UTF-8 \u{b7} {}",
                crate::object_text::cursor_label(position)
            )
            .into(),
            tooltip: None,
        }]
    }

    /// Wrap a typed `Entity<CodeDocument>` in a `PaneHandle`.
    ///
    /// Reads the document ID synchronously from `cx` then seals all operations
    /// behind `Box<dyn Fn>` closures capturing `entity` by clone.
    ///
    /// The optional `empty_script_cleanup` and `session_tab_snapshot` helpers
    /// are populated so that `write_session_manifest` and the empty-script
    /// cleanup in `actions/documents.rs` can operate without pattern-matching on
    /// the `DocumentHandle::Code` variant.
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
                Box::new(move |active, cx| e.update(cx, |d, cx| d.set_active_tab(active, cx)))
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

        handle.mark_inspector_closed = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                e.update(cx, |d, cx| d.mark_inspector_closed(cx));
            })
        });

        handle.on_close = Some({
            let entity = entity.clone();
            Box::new(move |cx| {
                entity.update(cx, |document, cx| document.invalidate_execution_session(cx));
            })
        });

        // Populate optional helper: the interrupted-close save. The tab closes
        // only when the document reports that the write landed.
        handle.save_for_close = Some({
            let e = entity.clone();
            Box::new(move |w, cx| e.update(cx, |document, cx| document.save_for_close(w, cx)))
        });

        // Populate optional helper: the graceful-shutdown flush. It persists the
        // pending edits without closing the tab, so quitting never drops the
        // content typed inside the autosave debounce window.
        handle.flush_for_shutdown = Some({
            let e = entity.clone();
            Box::new(move |cx| e.update(cx, |document, cx| document.flush_for_shutdown(cx)))
        });

        // Populate optional helper: the close policy. Every close route asks the
        // document what closing means instead of removing the tab over pending
        // edits: a clean, idle buffer closes now, a pending buffer flushes
        // (conflict-checked on a file-backed script) and closes once the write
        // lands, and a buffer that cannot be persisted keeps the tab open.
        handle.resolve_close = Some({
            let e = entity.clone();
            Box::new(move |w, cx| e.update(cx, |document, cx| document.resolve_close(w, cx)))
        });

        // Populate optional helper: whether the close policy applies right now.
        // A code document persists its pending edits on close only when it has
        // a file to persist to; an untitled buffer has no save target short of
        // Save As, so it keeps the unsaved-changes dialog instead.
        handle.decides_own_close = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).path().is_some())
        });

        // Populate optional helper: the only backing file the cleanup path in
        // actions.rs may delete as it closes a tab — an empty, file-backed script
        // whose file is expected to hold exactly the bytes this document last
        // loaded or wrote. A missing or different-path baseline reports `None`, so
        // cleanup keeps the file; whether the file still holds those bytes is
        // verified away from the UI thread, together with the removal.
        handle.status_segments = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).status_segments(cx))
        });

        handle.key_context_entries = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).key_context_entries(cx))
        });

        handle.tab_tooltip = Some({
            let e = entity.clone();
            Box::new(move |cx| {
                e.read(cx)
                    .editor
                    .path
                    .as_ref()
                    .map(|path| path.display().to_string().into())
            })
        });

        handle.empty_script_cleanup = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).pending_empty_script_cleanup(cx))
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

        handle.side_panels = Some({
            let e = entity.clone();
            Box::new(move |_window, cx| e.update(cx, |d, cx| d.side_panels(cx)))
        });

        handle.pane_actions = Some({
            let e = entity.clone();
            Box::new(move |cx| e.read(cx).pane_actions(&e))
        });

        handle
    }
}

#[cfg(test)]
mod tests {
    use crate::code::CodeDocument;
    use crate::pane::{PaneAction, PaneActionRun};
    use dbflux_app::keymap::Command;
    use dbflux_components::theme;
    use dbflux_core::QueryLanguage;
    use dbflux_storage::bootstrap::StorageRuntime;
    use dbflux_ui_base::AppStateEntity;
    use dbflux_ui_base::keymap::init_keymap;
    use dbflux_ui_base::toast::{ToastGlobal, ToastHost};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::rc::Rc;

    fn open_sql_document(
        cx: &mut TestAppContext,
    ) -> (Entity<CodeDocument>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(theme::init);
        cx.update(|cx| {
            let host = cx.new(|_cx| ToastHost::new());
            cx.set_global(ToastGlobal { host });
            init_keymap(cx);
        });

        let app_state = cx.update(|cx| {
            cx.new(|_| {
                let runtime = StorageRuntime::in_memory().expect("isolated storage runtime");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let slot: Rc<RefCell<Option<Entity<CodeDocument>>>> = Rc::default();
        let (_, window) = cx.add_window_view({
            let slot = slot.clone();
            move |window, cx| {
                let document = cx.new(|cx| {
                    CodeDocument::new_with_language(
                        app_state.clone(),
                        None,
                        QueryLanguage::Sql,
                        window,
                        cx,
                    )
                });
                slot.replace(Some(document.clone()));
                gpui_component::Root::new(document, window, cx)
            }
        });
        window.run_until_parked();

        let document = slot.borrow().clone().expect("document created");
        (document, window)
    }

    fn actions(document: &Entity<CodeDocument>, window: &mut VisualTestContext) -> Vec<PaneAction> {
        let document = document.clone();
        window.update(|_, cx| document.read(cx).pane_actions(&document))
    }

    fn command_of(action: &PaneAction) -> Option<Command> {
        match action.run {
            PaneActionRun::Command(command) => Some(command),
            PaneActionRun::Callback(_) => None,
        }
    }

    #[gpui::test]
    fn the_pane_actions_list_every_sql_toolbar_button(cx: &mut TestAppContext) {
        let (document, window) = open_sql_document(cx);
        let actions = actions(&document, window);

        let ids: Vec<&str> = actions.iter().map(|action| action.id.as_ref()).collect();
        assert_eq!(
            ids,
            [
                "run",
                "run-in-new-tab",
                "save",
                "format",
                "history",
                "explain",
                "chart",
                "refresh",
                "auto-refresh",
            ]
        );

        let command = |id: &str| {
            actions
                .iter()
                .find(|action| action.id == id)
                .and_then(command_of)
        };
        assert_eq!(command("run"), Some(Command::RunQuery));
        assert_eq!(command("run-in-new-tab"), Some(Command::RunQueryInNewTab));
        assert_eq!(command("save"), Some(Command::SaveQuery));
        assert_eq!(command("history"), Some(Command::ToggleHistoryDropdown));
        assert_eq!(command("refresh"), Some(Command::RunQuery));

        let format = actions.iter().find(|action| action.id == "format");
        assert!(
            format.is_some_and(|action| !action.enabled),
            "the formatter is unavailable, as on the toolbar"
        );

        let run = actions.iter().find(|action| action.id == "run");
        assert!(
            run.is_some_and(|action| action.shortcut.is_some()),
            "an entry with a key binding shows its keys"
        );
    }

    #[gpui::test]
    fn the_auto_refresh_entry_hands_the_keyboard_to_its_dropdown(cx: &mut TestAppContext) {
        let (document, window) = open_sql_document(cx);

        let document_focus = document.clone();
        window.update(|window, cx| {
            document_focus.update(cx, |document, cx| {
                document.focus_handle.focus(window, cx);
            });
        });
        window.run_until_parked();

        let auto_refresh = actions(&document, window)
            .into_iter()
            .find(|action| action.id == "auto-refresh")
            .expect("an auto-refresh entry");
        let PaneActionRun::Callback(open_interval) = auto_refresh.run else {
            panic!("the auto-refresh entry runs a callback");
        };

        window.update(|window, cx| open_interval(window, cx));
        window.run_until_parked();

        let dropdown = window.update(|_, cx| document.read(cx).refresh.refresh_dropdown.clone());
        let (open, focused) = window.update(|window, cx| {
            let dropdown = dropdown.read(cx);
            (dropdown.is_open(), dropdown.is_focused(window))
        });
        assert!(open && focused, "the interval dropdown opens with focus");

        window.simulate_keystrokes("j escape");
        window.run_until_parked();

        let (open, document_focused) = window.update(|window, cx| {
            (
                dropdown.read(cx).is_open(),
                document.read(cx).focus_handle.is_focused(window),
            )
        });
        assert!(!open, "Escape closes the interval list");
        assert!(document_focused, "focus returns to the editor pane");
    }
}

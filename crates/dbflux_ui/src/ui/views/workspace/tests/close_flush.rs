use crate::ui::document::pane::CloseDisposition;
use crate::ui::document::{CodeDocument, InspectorPanel, Tab, TabBarEvent};
use crate::ui::overlays::modals::UnsavedChangesOutcome;
use crate::ui::views::workspace::{DocumentFlushOutcome, Workspace, await_document_flush};
use dbflux_core::QueryLanguage;
use dbflux_core::document_id::DocumentId;
use dbflux_ui_base::AppStateEntity;
use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use dbflux_ui_base::{SaveTargetOutcome, SaveTargetProvider};

use super::{new_workspace, new_workspace_with, open_code_tab};

// === Close-route unification and close flush (T2a) ===

fn temp_close_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("dbflux-close-{name}-{}.sql", uuid::Uuid::new_v4()))
}

/// Opens a real file-backed code tab whose buffer holds `buffer` while the
/// file on disk holds `on_disk`, with a real loaded baseline.
///
/// When `dirty` is set the tab reports pending edits (through the same
/// `restore_dirty` session restore uses), so closing it must persist them.
fn open_file_backed_code_tab(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
    app_state: &Entity<AppStateEntity>,
    path: &std::path::Path,
    buffer: &str,
    on_disk: &str,
    dirty: bool,
) -> DocumentId {
    std::fs::write(path, on_disk).expect("seed the backing file");
    let path = path.to_path_buf();
    let buffer = buffer.to_string();
    let on_disk = on_disk.to_string();

    let document = window.update(|window, cx| {
        cx.new(|cx| {
            let mut document = CodeDocument::new_with_language(
                app_state.clone(),
                None,
                QueryLanguage::Sql,
                window,
                cx,
            )
            .with_path(path.clone());
            document.set_content(&buffer, window, cx);
            document.seed_file_baseline(path.clone(), on_disk);
            if dirty {
                document.restore_dirty(cx);
            }
            document
        })
    });
    let document_id = window.update(|_, cx| document.read(cx).id());

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            let pane = CodeDocument::into_pane(document.clone(), cx);
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
        });
    });

    document_id
}

/// The single close funnel persists a dirty code tab's newest content to the
/// real file before the tab is removed.
#[gpui::test]
fn closing_a_dirty_code_tab_persists_before_removing_it(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_close_path("persist");
    let id = open_file_backed_code_tab(
        window, &workspace, &app_state, &path, "NEWEST;", "OLD;", true,
    );

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.close_tab(id, window, cx);
            assert!(
                workspace.tab_manager.read(cx).document(id).is_some(),
                "a deferred close must not remove the tab before its flush lands"
            );
        });
    });

    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the flush must land on the real file"),
        "NEWEST;",
        "closing a dirty tab must persist its newest content"
    );
    window.update(|_, cx| {
        assert!(
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .is_none(),
            "the tab closes only once its flush lands"
        );
    });

    std::fs::remove_file(&path).ok();
}

/// A close whose flush is refused leaves the tab open with its buffer, and
/// the foreign bytes on disk stay untouched.
#[gpui::test]
fn a_close_flush_refused_over_a_foreign_change_keeps_the_tab_open(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_close_path("conflict");
    let id =
        open_file_backed_code_tab(window, &workspace, &app_state, &path, "MINE;", "OLD;", true);

    std::fs::write(&path, "THEIRS;").expect("the external rewrite must succeed");

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.close_tab(id, window, cx);
        });
    });
    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the foreign content must survive"),
        "THEIRS;",
        "a refused flush must never overwrite the foreign change"
    );
    window.update(|_, cx| {
        assert!(
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .is_some(),
            "a refused flush leaves the tab open with its buffer"
        );
    });

    std::fs::remove_file(&path).ok();
}

/// A clean, idle tab closes immediately: no flush, no pointless write.
#[gpui::test]
fn a_clean_idle_code_tab_closes_without_a_flush(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_close_path("clean");
    let id = open_file_backed_code_tab(
        window, &workspace, &app_state, &path, "SEEDED;", "SEEDED;", false,
    );

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(workspace.close_tab(id, window, cx));
            assert!(
                workspace.tab_manager.read(cx).document(id).is_none(),
                "a clean, idle tab closes immediately"
            );
        });
    });
    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the file must be intact"),
        "SEEDED;",
        "a clean close must not rewrite the file"
    );

    std::fs::remove_file(&path).ok();
}

/// A dirty code tab closes by persisting, not by asking: the unsaved-changes
/// dialog is only for documents that need an explicit user save.
#[gpui::test]
fn closing_a_dirty_code_tab_persists_instead_of_asking(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_close_path("no-dialog");
    let id = open_file_backed_code_tab(
        window, &workspace, &app_state, &path, "NEWEST;", "OLD;", true,
    );

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(
                workspace.close_active_tab(window, cx),
                "a code document's close is accepted and deferred"
            );
            assert!(
                !workspace.modal_unsaved_changes.read(cx).is_visible(),
                "a code document persists its edits instead of asking"
            );
            assert!(
                workspace.tab_manager.read(cx).document(id).is_some(),
                "the tab stays open until the flush lands"
            );
        });
    });
    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the flush must land"),
        "NEWEST;"
    );
    window.update(|_, cx| {
        assert!(
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .is_none()
        );
    });

    std::fs::remove_file(&path).ok();
}

/// Batch close applies the same per-document policy: clean tabs close, a
/// tab whose flush is refused stays open.
#[gpui::test]
fn close_others_closes_the_clean_tabs_and_keeps_a_blocked_one(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let kept_path = temp_close_path("kept");
    let clean_path = temp_close_path("other-clean");
    let blocked_path = temp_close_path("other-blocked");

    let kept = open_file_backed_code_tab(
        window, &workspace, &app_state, &kept_path, "KEEP;", "KEEP;", false,
    );
    let clean = open_file_backed_code_tab(
        window,
        &workspace,
        &app_state,
        &clean_path,
        "CLEAN;",
        "CLEAN;",
        false,
    );
    let blocked = open_file_backed_code_tab(
        window,
        &workspace,
        &app_state,
        &blocked_path,
        "MINE;",
        "OLD;",
        true,
    );

    // A foreign change makes the blocked tab's flush refuse.
    std::fs::write(&blocked_path, "THEIRS;").expect("the external rewrite must succeed");

    // Route through the tab bar exactly as the context menu does.
    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.tab_bar.update(cx, |_bar, cx| {
                cx.emit(TabBarEvent::CloseOtherTabs(kept));
            });
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let manager = workspace.read(cx).tab_manager.read(cx);
        assert!(manager.document(kept).is_some(), "the reference tab stays");
        assert!(manager.document(clean).is_none(), "a clean tab closes");
        assert!(
            manager.document(blocked).is_some(),
            "a blocked tab stays open"
        );
    });
    assert_eq!(
        std::fs::read_to_string(&blocked_path).expect("the foreign content must survive"),
        "THEIRS;"
    );

    std::fs::remove_file(&kept_path).ok();
    std::fs::remove_file(&clean_path).ok();
    std::fs::remove_file(&blocked_path).ok();
}

/// A Save As picker that records that it was invoked, then reports the
/// user dismissing the dialog. Used to prove a close route never started
/// a Save As flow: no picker call, no file created anywhere.
fn cancelled_recording_picker() -> (SaveTargetProvider, Arc<std::sync::atomic::AtomicBool>) {
    use std::sync::atomic::{AtomicBool, Ordering};

    let invoked = Arc::new(AtomicBool::new(false));
    let flag = invoked.clone();
    let provider: SaveTargetProvider = Arc::new(move |_request| {
        flag.store(true, Ordering::SeqCst);
        gpui::Task::ready(SaveTargetOutcome::Cancelled)
    });
    (provider, invoked)
}

/// Opens an untitled code tab holding edits the user typed: no backing file,
/// a buffer that differs from the content it loaded, and no baseline.
///
/// The text enters through the editor's own input path rather than a
/// fixture-only setter, so the document is dirty by the same predicate the
/// close flow consults. The assertion is what keeps this fixture from
/// looking dirty to a test while looking clean to the code.
fn open_dirty_untitled_code_tab(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
    app_state: &Entity<AppStateEntity>,
) -> DocumentId {
    let document = window.update(|window, cx| {
        cx.new(|cx| {
            CodeDocument::new_with_language(app_state.clone(), None, QueryLanguage::Sql, window, cx)
        })
    });
    let document_id = window.update(|_, cx| document.read(cx).id());

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            let pane = CodeDocument::into_pane(document.clone(), cx);
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
            workspace.set_focus(crate::keymap::FocusTarget::Document, window, cx);
        });
    });
    window.run_until_parked();

    window.simulate_input("SELECT 1;");
    window.run_until_parked();

    window.update(|_, cx| {
        assert!(
            document.read(cx).change_summary(cx).is_some(),
            "the fixture must look dirty to the close flow's own predicate"
        );
    });

    document_id
}

/// Only code documents decide their own close; every other document keeps
/// the unsaved-changes dialog and reports `CloseNow` to the funnel. A code
/// document decides its own close only while it has a file to persist to:
/// an untitled buffer has no save target short of Save As, so the dialog
/// guards it.
#[gpui::test]
fn only_code_documents_decide_their_own_close(cx: &mut TestAppContext) {
    let (_workspace, app_state, window) = new_workspace(cx);

    let untitled = window.update(|window, cx| {
        cx.new(|cx| {
            CodeDocument::new_with_language(app_state.clone(), None, QueryLanguage::Sql, window, cx)
        })
    });
    let untitled_pane = window.update(|_, cx| CodeDocument::into_pane(untitled, cx));
    let untitled_keeps_dialog = window.update(|_, cx| !untitled_pane.has_close_policy(cx));
    assert!(
        untitled_keeps_dialog,
        "an untitled code document has no file to persist to, so the dialog guards it"
    );

    let backing_path =
        std::env::temp_dir().join(format!("dbflux-policy-{}.sql", uuid::Uuid::new_v4()));
    let file_backed = window.update(|window, cx| {
        cx.new(|cx| {
            CodeDocument::new_with_language(app_state.clone(), None, QueryLanguage::Sql, window, cx)
                .with_path(backing_path.clone())
        })
    });
    let file_backed_pane = window.update(|_, cx| CodeDocument::into_pane(file_backed, cx));
    let file_backed_decides = window.update(|_, cx| file_backed_pane.has_close_policy(cx));
    assert!(
        file_backed_decides,
        "a file-backed code document persists its own edits on close"
    );

    let inspector = window.update(|_, cx| {
        cx.new(|cx| {
            InspectorPanel::new(
                uuid::Uuid::new_v4(),
                "metric".to_string(),
                app_state.clone(),
                cx,
            )
        })
    });
    let inspector_pane = window.update(|_, cx| InspectorPanel::into_pane(inspector, cx));
    let inspector_keeps_dialog = window.update(|_, cx| !inspector_pane.has_close_policy(cx));
    assert!(
        inspector_keeps_dialog,
        "documents without a close policy keep the unsaved-changes dialog"
    );
    assert_eq!(
        window.update(|window, cx| inspector_pane.resolve_close(window, cx)),
        CloseDisposition::CloseNow,
        "a document without a policy is always closable by the funnel"
    );
}

/// Closing a dirty untitled tab through the funnel must ask before
/// discarding: the confirmation opens, the tab stays open, and no Save As
/// flow starts, so no file is ever created.
#[gpui::test]
fn a_dirty_untitled_tab_closed_through_the_funnel_asks_first(cx: &mut TestAppContext) {
    let (picker, picker_invoked) = cancelled_recording_picker();
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime)
                .expect("test storage setup")
                .with_save_target_override(picker)
        })
    });
    let (workspace, app_state, window) = new_workspace_with(cx, app_state);
    let id = open_dirty_untitled_code_tab(window, &workspace, &app_state);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            let accepted = workspace.close_tab(id, window, cx);
            assert!(!accepted, "the close must wait for the user's decision");
            assert!(
                workspace.modal_unsaved_changes.read(cx).is_visible(),
                "a dirty untitled tab must raise the unsaved-changes confirmation"
            );
            assert!(
                workspace.tab_manager.read(cx).document(id).is_some(),
                "the tab stays open until the user chooses"
            );
        });
    });

    assert!(
        !picker_invoked.load(std::sync::atomic::Ordering::SeqCst),
        "the funnel must ask, not start a Save As behind the dialog"
    );
}

/// "Don't save" must remove the tab directly: routing the discard back
/// through the funnel would re-open the same confirmation forever, and a
/// funnel flush would save the very changes the user chose to drop.
#[gpui::test]
fn discarding_a_dirty_untitled_tab_removes_it_without_re_asking(cx: &mut TestAppContext) {
    let (picker, picker_invoked) = cancelled_recording_picker();
    let app_state = cx.update(|cx| {
        cx.new(|_| {
            let runtime =
                dbflux_storage::bootstrap::StorageRuntime::in_memory().expect("in-memory storage");
            AppStateEntity::new_with_storage_runtime(runtime)
                .expect("test storage setup")
                .with_save_target_override(picker)
        })
    });
    let (workspace, app_state, window) = new_workspace_with(cx, app_state);
    let id = open_dirty_untitled_code_tab(window, &workspace, &app_state);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.close_tab(id, window, cx);
        });
    });

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            workspace.modal_unsaved_changes.update(cx, |modal, cx| {
                // The buttons emit and then close, so the confirmation is
                // already gone by the time the workspace handles the choice.
                cx.emit(UnsavedChangesOutcome::DiscardAll(vec![id]));
                modal.close(cx);
            });
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        assert!(
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .is_none(),
            "discard removes the listed tab"
        );
        assert!(
            !workspace
                .read(cx)
                .modal_unsaved_changes
                .read(cx)
                .is_visible(),
            "discard must not re-open the confirmation"
        );
    });
    assert!(
        !picker_invoked.load(std::sync::atomic::Ordering::SeqCst),
        "discard must not save the edits the user chose to drop"
    );
}

/// A batch close asks once about every document that needs it: one modal
/// carrying all their entries, none of them closed by the batch, while the
/// documents that need no decision still close.
#[gpui::test]
fn a_batch_close_asks_once_about_all_documents_that_need_it(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let first = open_dirty_untitled_code_tab(window, &workspace, &app_state);
    let second = open_dirty_untitled_code_tab(window, &workspace, &app_state);
    let clean = open_code_tab(window, &workspace, &app_state);

    window.update(|window, cx| {
        workspace.update(cx, |workspace, cx| {
            let all = [first, second, clean];
            workspace.close_tabs_by(window, cx, move |ids| {
                ids.iter().copied().filter(|id| all.contains(id)).collect()
            });
        });
    });
    window.run_until_parked();

    window.update(|_, cx| {
        let manager = workspace.read(cx).tab_manager.read(cx);
        assert!(
            workspace
                .read(cx)
                .modal_unsaved_changes
                .read(cx)
                .is_visible(),
            "a batch with documents that must be asked about opens the confirmation"
        );
        assert!(manager.document(first).is_some(), "asked tabs stay open");
        assert!(manager.document(second).is_some(), "asked tabs stay open");
        assert!(
            manager.document(clean).is_none(),
            "a clean tab still closes"
        );
    });
}

// === Graceful-shutdown flush (T2b) ===

fn temp_shutdown_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "dbflux-shutdown-{name}-{}.sql",
        uuid::Uuid::new_v4()
    ))
}

/// Opens a code tab backed by `path` with its session shadow at `shadow` —
/// the shape a graceful shutdown flushes. `dirty` marks pending edits the way
/// the session restore does, and `seed_baseline` controls whether the document
/// owns a trustworthy on-disk baseline.
#[allow(clippy::too_many_arguments)]
fn open_shutdown_code_tab(
    window: &mut VisualTestContext,
    workspace: &Entity<Workspace>,
    app_state: &Entity<AppStateEntity>,
    path: &std::path::Path,
    shadow: &std::path::Path,
    buffer: &str,
    on_disk: &str,
    dirty: bool,
    seed_baseline: bool,
) -> DocumentId {
    std::fs::write(path, on_disk).expect("seed the backing file");
    let path = path.to_path_buf();
    let shadow = shadow.to_path_buf();
    let buffer = buffer.to_string();
    let on_disk = on_disk.to_string();

    let document = window.update(|window, cx| {
        cx.new(|cx| {
            let mut document = CodeDocument::new_with_language(
                app_state.clone(),
                None,
                QueryLanguage::Sql,
                window,
                cx,
            )
            .with_path(path.clone());
            document.set_content(&buffer, window, cx);
            document.set_session_paths(None, Some(shadow.clone()));
            if seed_baseline {
                document.seed_file_baseline(path.clone(), on_disk);
            }
            if dirty {
                document.restore_dirty(cx);
            }
            document
        })
    });
    let document_id = window.update(|_, cx| document.read(cx).id());

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            let pane = CodeDocument::into_pane(document.clone(), cx);
            workspace.tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });
        });
    });

    document_id
}

/// Counts the tabs persisted in the workspace session manifest.
fn manifest_tab_count(window: &mut VisualTestContext, app_state: &Entity<AppStateEntity>) -> usize {
    window.update(|_, cx| {
        let runtime = app_state.read(cx).storage_runtime();
        let repo = runtime.sessions();
        let artifacts = runtime.artifacts();
        repo.restore_session(artifacts)
            .ok()
            .flatten()
            .map(|session| session.tabs.len())
            .unwrap_or(0)
    })
}

/// A dirty file-backed document flushed at exit lands its newest content in
/// the real file, without closing its tab or emptying the session manifest.
#[gpui::test]
fn a_shutdown_flush_lands_pending_edits_and_keeps_the_tab(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_shutdown_path("land");
    let shadow = temp_shutdown_path("land-shadow");
    let id = open_shutdown_code_tab(
        window, &workspace, &app_state, &path, &shadow, "NEWEST;", "OLD;", true, true,
    );

    assert_eq!(
        manifest_tab_count(window, &app_state),
        1,
        "opening a file-backed tab records it in the session manifest"
    );

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(
                workspace.flush_pending_document_edits(cx),
                "a dirty document reports its flush as outstanding"
            );
            assert!(
                workspace.tab_manager.read(cx).document(id).is_some(),
                "the shutdown flush must not close the tab"
            );
        });
    });

    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the flush must land on the real file"),
        "NEWEST;",
        "a shutdown flush persists the newest content"
    );
    window.update(|_, cx| {
        assert!(
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .is_some(),
            "the tab survives a shutdown flush"
        );
    });
    assert_eq!(
        manifest_tab_count(window, &app_state),
        1,
        "the session manifest still holds the tab after a shutdown flush"
    );

    std::fs::remove_file(&path).ok();
    std::fs::remove_file(&shadow).ok();
}

/// The workspace reports a flush as outstanding until its write lands, then
/// reports nothing outstanding.
#[gpui::test]
fn a_shutdown_flush_reports_nothing_outstanding_once_it_lands(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_shutdown_path("outstanding");
    let shadow = temp_shutdown_path("outstanding-shadow");
    open_shutdown_code_tab(
        window, &workspace, &app_state, &path, &shadow, "NEWEST;", "OLD;", true, true,
    );

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(
                workspace.flush_pending_document_edits(cx),
                "the write is outstanding before it runs"
            );
            // Polling again before the executor runs must neither block nor
            // drop the queued write.
            assert!(
                workspace.flush_pending_document_edits(cx),
                "a queued write keeps reporting outstanding"
            );
        });
    });

    window.run_until_parked();

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(
                !workspace.flush_pending_document_edits(cx),
                "nothing is outstanding once the flush lands"
            );
        });
    });

    assert_eq!(
        std::fs::read_to_string(&path).expect("the flush must land"),
        "NEWEST;"
    );

    std::fs::remove_file(&path).ok();
    std::fs::remove_file(&shadow).ok();
}

/// A shutdown flush refused over an external change overwrites nothing, still
/// writes the session shadow, and never closes the tab.
#[gpui::test]
fn a_shutdown_flush_refused_over_a_foreign_change_writes_only_the_shadow(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_shutdown_path("conflict");
    let shadow = temp_shutdown_path("conflict-shadow");
    let id = open_shutdown_code_tab(
        window, &workspace, &app_state, &path, &shadow, "MINE;", "OLD;", true, true,
    );

    std::fs::write(&path, "THEIRS;").expect("the external rewrite must succeed");

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(workspace.flush_pending_document_edits(cx));
        });
    });
    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the foreign content must survive"),
        "THEIRS;",
        "a refused shutdown flush must never overwrite the foreign change"
    );
    assert_eq!(
        std::fs::read_to_string(&shadow).expect("the shadow is the safety net"),
        "MINE;",
        "the session shadow still carries the pending edits"
    );

    let entity = workspace.clone();
    window.update(|_, cx| {
        assert!(
            entity.read(cx).tab_manager.read(cx).document(id).is_some(),
            "a refused flush never closes the tab"
        );
        assert!(
            !entity.update(cx, |workspace, cx| workspace
                .flush_pending_document_edits(cx)),
            "a refusal is reported and not retried forever"
        );
    });

    std::fs::remove_file(&path).ok();
    std::fs::remove_file(&shadow).ok();
}

/// A shutdown flush refused because the file was deleted keeps it deleted and
/// writes the shadow instead.
#[gpui::test]
fn a_shutdown_flush_refused_over_a_deleted_file_keeps_it_deleted(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_shutdown_path("deleted");
    let shadow = temp_shutdown_path("deleted-shadow");
    open_shutdown_code_tab(
        window, &workspace, &app_state, &path, &shadow, "MINE;", "OLD;", true, true,
    );

    std::fs::remove_file(&path).expect("the backing file must be removable");

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(workspace.flush_pending_document_edits(cx));
        });
    });
    window.run_until_parked();

    assert!(
        !path.exists(),
        "a refused shutdown flush must not recreate the deleted file"
    );
    assert_eq!(
        std::fs::read_to_string(&shadow).expect("the shadow is the safety net"),
        "MINE;"
    );

    std::fs::remove_file(&shadow).ok();
}

/// A shutdown flush with no trustworthy baseline refuses rather than creating
/// or overwriting the file, and still writes the shadow.
#[gpui::test]
fn a_shutdown_flush_without_a_baseline_refuses_and_writes_the_shadow(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_shutdown_path("no-baseline");
    let shadow = temp_shutdown_path("no-baseline-shadow");
    // No baseline is seeded, so the shutdown flush has nothing to compare the
    // on-disk bytes against and must refuse.
    open_shutdown_code_tab(
        window, &workspace, &app_state, &path, &shadow, "MINE;", "OLD;", true, false,
    );

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(workspace.flush_pending_document_edits(cx));
        });
    });
    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the file must survive untouched"),
        "OLD;",
        "a shutdown flush with no baseline must not overwrite the file"
    );
    assert_eq!(
        std::fs::read_to_string(&shadow).expect("the shadow is the safety net"),
        "MINE;"
    );

    std::fs::remove_file(&path).ok();
    std::fs::remove_file(&shadow).ok();
}

/// A clean, idle document is skipped by the shutdown flush: no physical write
/// and no session shadow.
#[gpui::test]
fn a_clean_idle_document_is_skipped_by_the_shutdown_flush(cx: &mut TestAppContext) {
    let (workspace, app_state, window) = new_workspace(cx);
    let path = temp_shutdown_path("clean");
    let shadow = temp_shutdown_path("clean-shadow");
    open_shutdown_code_tab(
        window, &workspace, &app_state, &path, &shadow, "SEEDED;", "SEEDED;", false, true,
    );

    window.update(|_, cx| {
        workspace.update(cx, |workspace, cx| {
            assert!(
                !workspace.flush_pending_document_edits(cx),
                "a clean, idle document has nothing outstanding"
            );
        });
    });
    window.run_until_parked();

    assert_eq!(
        std::fs::read_to_string(&path).expect("the file must be intact"),
        "SEEDED;",
        "a clean, idle document must not be rewritten"
    );
    assert!(
        !shadow.exists(),
        "a clean, idle document must not write a session shadow"
    );

    std::fs::remove_file(&path).ok();
}

/// A flush that never finishes is bounded by the deadline rather than hanging
/// the shutdown sequence.
#[gpui::test]
fn the_shutdown_flush_deadline_bounds_a_write_that_never_completes(cx: &mut TestAppContext) {
    let timeout = Duration::from_millis(100);
    let poll_interval = Duration::from_millis(50);

    let result = Rc::new(Cell::new(None));
    let result_out = result.clone();

    let task = cx.spawn(move |mut app_cx| async move {
        let outcome = await_document_flush(
            &mut app_cx,
            timeout,
            poll_interval,
            |_app_cx| true, // the write never finishes
        )
        .await;
        result_out.set(Some(outcome));
    });

    cx.executor().advance_clock(Duration::from_millis(300));
    cx.run_until_parked();

    assert_eq!(
        result.get(),
        Some(DocumentFlushOutcome::TimedOut),
        "the deadline must stop a flush that never finishes"
    );

    drop(task);
}

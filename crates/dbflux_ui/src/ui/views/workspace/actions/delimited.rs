use super::*;
use crate::ui::document::{DelimitedDocument, DelimitedFileKey, DocumentKey, ObjectSavedCallback};
use crate::ui::labels::{NoActiveConnectionKind, documents_no_active_connection_message};

impl Workspace {
    /// Opens a CSV or TSV file as a table in its own tab, or focuses the tab
    /// that already shows it: one tab per local path and per
    /// `(profile_id, bucket, key)`.
    ///
    /// A local path is resolved first, so two spellings of one file and a
    /// symlink to it share a tab. A path that resolves is kept in recent
    /// files, as a script is once it was read. A path that cannot be resolved
    /// is opened as given, and the document reports why it cannot be read.
    ///
    /// An object is read through the live connection of its profile, so a
    /// profile that is not connected is reported and opens nothing.
    /// `on_object_saved` is told the object's key after each save that
    /// replaces it.
    ///
    /// The keyboard moves to the new tab on the next render, so callers
    /// without a window can open one.
    pub(in crate::ui::views::workspace) fn open_delimited_file(
        &mut self,
        file: DelimitedFileKey,
        on_object_saved: Option<ObjectSavedCallback>,
        cx: &mut Context<Self>,
    ) {
        let file = match file {
            DelimitedFileKey::Local { path } => match std::fs::canonicalize(&path) {
                Ok(resolved) => {
                    self.app_state.update(cx, |state, cx| {
                        state.record_recent_file(resolved.clone());
                        cx.emit(AppStateChanged);
                    });

                    DelimitedFileKey::Local { path: resolved }
                }
                Err(_) => DelimitedFileKey::Local { path },
            },
            object @ DelimitedFileKey::Object { .. } => object,
        };

        let existing_id = self
            .tab_manager
            .read(cx)
            .find_by_key(&DocumentKey::Delimited(file.clone()), cx);

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            return;
        }

        let doc = match file {
            DelimitedFileKey::Local { path } => {
                cx.new(|cx| DelimitedDocument::open_local(path, cx))
            }

            DelimitedFileKey::Object {
                profile_id,
                bucket,
                key,
            } => {
                let connection = self
                    .app_state
                    .read(cx)
                    .connections()
                    .get(&profile_id)
                    .map(|connected| connected.connection.clone());

                let Some(connection) = connection else {
                    report_error(
                        UserFacingError::new(
                            ErrorKind::User,
                            documents_no_active_connection_message(NoActiveConnectionKind::Object),
                        ),
                        cx,
                    );
                    return;
                };

                let app_state = self.app_state.clone();

                cx.new(|cx| {
                    let mut document = DelimitedDocument::open_object(
                        app_state, profile_id, connection, bucket, key, cx,
                    );

                    if let Some(on_saved) = on_object_saved {
                        document.set_on_object_saved(on_saved);
                    }

                    document
                })
            }
        };
        let pane = DelimitedDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.pending_focus = Some(FocusTarget::Document);
        cx.notify();
    }
}

/// A value typed into a cell of a CSV tab and not yet committed with Enter
/// must reach the close gate: every close route asks about it instead of
/// dropping it.
#[cfg(test)]
mod pending_cell_input_close_tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::keymap::{Command, CommandDispatcher, FocusTarget};
    use crate::ui::document::tab_bar::TAB_MENU_CLOSE;
    use crate::ui::document::{DelimitedDocument, DocumentId, Tab};
    use crate::ui::views::workspace::Workspace;
    use dbflux_components::components::data_table::selection::CellCoord;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{
        AppContext as _, Bounds, Entity, Modifiers, MouseButton, Pixels, Point, TestAppContext,
        VisualTestContext, point,
    };
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::rc::Rc;

    fn new_workspace(cx: &mut TestAppContext) -> (Entity<Workspace>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);
        cx.update(dbflux_ui_base::keymap::init_keymap);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::new(RefCell::new(None));
        let workspace_ref = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
            workspace_ref.replace(Some(workspace.clone()));
            gpui_component::Root::new(workspace, window, cx)
        });

        let workspace = holder
            .borrow()
            .clone()
            .expect("workspace should be created");

        // Test windows open inactive, and gpui hides focus paths of an
        // inactive window from its focus listeners, which would keep the cell
        // editor from ever seeing its blur.
        window.update(|window, _| window.activate_window());
        window.run_until_parked();

        (workspace, window)
    }

    /// A CSV file in a private directory that is removed when the test ends.
    struct TestFile {
        directory: PathBuf,
        path: PathBuf,
    }

    impl TestFile {
        fn new() -> Self {
            Self::with_content(b"name,city\nAna,Lima\n")
        }

        fn with_content(content: &[u8]) -> Self {
            let directory = std::env::temp_dir()
                .join(format!("dbflux-delimited-close-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&directory).expect("the test directory must be creatable");

            let path = directory.join("cities.csv");
            std::fs::write(&path, content).expect("the test file must be writable");

            Self { directory, path }
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.directory).ok();
        }
    }

    /// Opens the file in a tab, as `open_delimited_file` does, keeping the
    /// typed entity, and waits for its first page.
    fn open_csv_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        file: &TestFile,
    ) -> (Entity<DelimitedDocument>, DocumentId) {
        let path = file.path.clone();

        let document = window.update(|_, cx| {
            let document = cx.new(|cx| DelimitedDocument::open_local(path, cx));
            let pane = DelimitedDocument::into_pane(document.clone(), cx);
            let tab_manager = workspace.read(cx).tab_manager.clone();

            tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });

            document
        });
        window.run_until_parked();

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.set_focus(FocusTarget::Document, window, cx);
            });
        });
        window.run_until_parked();

        let id = window.update(|_, cx| document.read(cx).id());

        (document, id)
    }

    /// Opens the inline editor on the city of the first record and replaces
    /// its text with `text`, without pressing Enter.
    fn type_into_city(
        window: &mut VisualTestContext,
        document: &Entity<DelimitedDocument>,
        text: &str,
    ) {
        let table_state = window.update(|_, cx| {
            document
                .read(cx)
                .table_state()
                .expect("the file is loaded")
                .clone()
        });

        window.update(|window, cx| {
            table_state.update(cx, |state, cx| {
                assert!(state.start_editing(CellCoord::new(0, 1), window, cx));

                let input = state
                    .cell_input()
                    .cloned()
                    .expect("a short cell is edited inline");

                input.update(cx, |input, cx| {
                    input.set_value(text.to_string(), window, cx)
                });
            });
        });
        window.run_until_parked();

        assert!(
            window.update(|_, cx| table_state.read(cx).is_editing()),
            "the editor is still open with the typed value"
        );
        assert!(
            !is_dirty(window, document),
            "the value is not committed yet"
        );
    }

    fn is_dirty(window: &mut VisualTestContext, document: &Entity<DelimitedDocument>) -> bool {
        window.update(|_, cx| document.read(cx).is_dirty())
    }

    fn tab_is_open(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        id: DocumentId,
    ) -> bool {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .document(id)
                .is_some()
        })
    }

    fn prompt_entries(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Option<usize> {
        window.update(|_, cx| {
            let modal = workspace.read(cx).modal_unsaved_changes.read(cx);
            modal.is_visible().then(|| modal.selected_count())
        })
    }

    fn center(bounds: Bounds<Pixels>) -> Point<Pixels> {
        point(
            bounds.origin.x + bounds.size.width / 2.0,
            bounds.origin.y + bounds.size.height / 2.0,
        )
    }

    fn rendered_bounds(window: &mut VisualTestContext, selector: String) -> Bounds<Pixels> {
        let selector: &'static str = selector.leak();
        window.update(|window, _| window.refresh());
        window.run_until_parked();
        window
            .debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} was not rendered"))
    }

    fn assert_close_asks_about_the_typed_value(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        document: &Entity<DelimitedDocument>,
        id: DocumentId,
        route: &str,
    ) {
        assert!(
            tab_is_open(window, workspace, id),
            "{route}: the tab must stay open while the typed value is unsaved"
        );
        assert_eq!(
            prompt_entries(window, workspace),
            Some(1),
            "{route}: the unsaved-changes prompt must list the file"
        );
        assert!(
            is_dirty(window, document),
            "{route}: the typed value must be a pending change"
        );
    }

    #[gpui::test]
    fn the_close_button_asks_about_a_value_typed_into_a_csv_cell(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new();
        let (document, id) = open_csv_tab(window, &workspace, &file);
        type_into_city(window, &document, "Cusco");

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, &document, id, "close button");
    }

    #[gpui::test]
    fn the_tab_menu_close_asks_about_a_value_typed_into_a_csv_cell(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new();
        let (document, id) = open_csv_tab(window, &workspace, &file);
        type_into_city(window, &document, "Cusco");

        let tab = center(rendered_bounds(window, format!("tab-{}", id.0)));
        window.simulate_mouse_down(tab, MouseButton::Right, Modifiers::none());
        window.simulate_mouse_up(tab, MouseButton::Right, Modifiers::none());
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.tab_bar.update(cx, |bar, cx| {
                    bar.context_menu_execute_at(TAB_MENU_CLOSE, cx)
                });
            });
        });
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, &document, id, "tab menu");
    }

    #[gpui::test]
    fn the_close_tab_command_asks_about_a_value_typed_into_a_csv_cell(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new();
        let (document, id) = open_csv_tab(window, &workspace, &file);
        type_into_city(window, &document, "Cusco");

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::CloseCurrentTab, window, cx);
            });
        });
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(window, &workspace, &document, id, "close command");
    }

    /// A city too long for the inline editor opens in the cell editor dialog,
    /// which leaves the tab bar reachable while it holds an edited value.
    #[gpui::test]
    fn the_close_button_asks_about_a_value_edited_in_the_cell_editor_dialog(
        cx: &mut TestAppContext,
    ) {
        let long_city = "abcdefghij".repeat(15);
        let content = format!("name,city\nAna,{long_city}\n");

        let (workspace, window) = new_workspace(cx);
        let file = TestFile::with_content(content.as_bytes());
        let (document, id) = open_csv_tab(window, &workspace, &file);

        let table_state = window.update(|_, cx| {
            document
                .read(cx)
                .table_state()
                .expect("the file is loaded")
                .clone()
        });

        window.update(|window, cx| {
            table_state.update(cx, |state, cx| {
                state.start_editing(CellCoord::new(0, 1), window, cx);
            });
        });
        window.run_until_parked();
        window.update(|window, _| window.refresh());
        window.run_until_parked();

        assert!(
            window.update(|_, cx| {
                document
                    .read(cx)
                    .cell_editor()
                    .is_some_and(|editor| editor.read(cx).is_visible())
            }),
            "the long city opens in the dialog"
        );

        window.simulate_input("X");
        window.run_until_parked();
        assert!(
            !is_dirty(window, &document),
            "the dialog's value is not staged yet"
        );

        let close_button = rendered_bounds(window, format!("tab-close-{}", id.0));
        window.simulate_click(center(close_button), Modifiers::none());
        window.run_until_parked();

        assert_close_asks_about_the_typed_value(
            window,
            &workspace,
            &document,
            id,
            "cell editor dialog",
        );

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace
                    .modal_unsaved_changes
                    .update(cx, |modal, cx| modal.confirm(cx));
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read(&file.path).expect("the file reads"),
            format!("name,city\nAna,X{long_city}\n").into_bytes(),
            "the dialog opens with the cursor at the start of the value"
        );
    }

    /// Saving from the prompt writes the typed value, so it is not lost.
    #[gpui::test]
    fn saving_from_the_prompt_writes_the_typed_value(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new();
        let (document, id) = open_csv_tab(window, &workspace, &file);
        type_into_city(window, &document, "Cusco");

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.dispatch(Command::CloseCurrentTab, window, cx);
            });
        });
        window.run_until_parked();

        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace
                    .modal_unsaved_changes
                    .update(cx, |modal, cx| modal.confirm(cx));
            });
        });
        window.run_until_parked();

        assert_eq!(
            std::fs::read(&file.path).expect("the file reads"),
            b"name,city\nAna,Cusco\n"
        );
        assert!(!tab_is_open(window, &workspace, id));
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::ui::document::{DelimitedFileKey, DocumentKind, DocumentState};
    use crate::ui::views::workspace::Workspace;
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::rc::Rc;

    fn new_workspace(cx: &mut TestAppContext) -> (Entity<Workspace>, &mut VisualTestContext) {
        cx.update(gpui_component::init);
        cx.update(dbflux_components::theme::init);

        let app_state: Entity<AppStateEntity> = cx.update(|cx| {
            cx.new(|_| {
                let runtime = dbflux_storage::bootstrap::StorageRuntime::in_memory()
                    .expect("in-memory storage");
                AppStateEntity::new_with_storage_runtime(runtime).expect("test storage setup")
            })
        });

        let holder: Rc<RefCell<Option<Entity<Workspace>>>> = Rc::new(RefCell::new(None));
        let workspace_ref = holder.clone();

        let (_, window) = cx.add_window_view(|window, cx| {
            let workspace = cx.new(|cx| Workspace::new(app_state.clone(), window, cx));
            workspace_ref.replace(Some(workspace.clone()));
            gpui_component::Root::new(workspace, window, cx)
        });

        let workspace = holder
            .borrow()
            .clone()
            .expect("workspace should be created");
        (workspace, window)
    }

    /// A CSV file in a private directory that is removed when the test ends.
    struct TestFile {
        directory: PathBuf,
        path: PathBuf,
    }

    impl TestFile {
        fn new(name: &str) -> Self {
            let directory = std::env::temp_dir()
                .join(format!("dbflux-open-delimited-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&directory).expect("the test directory must be creatable");

            let path = directory.join(name);
            std::fs::write(&path, b"name,city\nAna,Lima\n")
                .expect("the test file must be writable");

            Self { directory, path }
        }

        fn key(&self) -> DelimitedFileKey {
            DelimitedFileKey::Local {
                path: self.path.clone(),
            }
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.directory).ok();
        }
    }

    fn open(window: &mut VisualTestContext, workspace: &Entity<Workspace>, file: DelimitedFileKey) {
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_delimited_file(file, None, cx);
            });
        });
        window.run_until_parked();
    }

    fn toast_count(window: &mut VisualTestContext) -> usize {
        window.update(|_, cx| {
            cx.global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .read(cx)
                .toast_count()
        })
    }

    fn tab_titles(window: &mut VisualTestContext, workspace: &Entity<Workspace>) -> Vec<String> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .documents()
                .iter()
                .map(|tab| tab.tab_title(cx))
                .collect()
        })
    }

    #[gpui::test]
    fn opening_a_delimited_file_adds_one_tab_of_its_kind(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.csv");

        open(window, &workspace, file.key());

        assert_eq!(tab_titles(window, &workspace), ["cities.csv"]);

        let kind = window.update(|_, cx| {
            let tab_manager = workspace.read(cx).tab_manager.read(cx);
            tab_manager.documents()[0].kind()
        });
        assert_eq!(kind, DocumentKind::Delimited);
    }

    #[gpui::test]
    fn opening_the_same_file_again_focuses_its_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let first = TestFile::new("cities.csv");
        let second = TestFile::new("people.csv");

        open(window, &workspace, first.key());
        open(window, &workspace, second.key());
        open(window, &workspace, first.key());

        assert_eq!(tab_titles(window, &workspace), ["cities.csv", "people.csv"]);

        let active_title = window.update(|_, cx| {
            let tab_manager = workspace.read(cx).tab_manager.read(cx);
            let active_id = tab_manager.active_id();

            tab_manager
                .documents()
                .iter()
                .find(|tab| Some(tab.id()) == active_id)
                .map(|tab| tab.tab_title(cx))
        });
        assert_eq!(active_title.as_deref(), Some("cities.csv"));
    }

    #[gpui::test]
    fn an_object_of_a_profile_that_is_not_connected_opens_no_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);

        open(
            window,
            &workspace,
            DelimitedFileKey::Object {
                profile_id: uuid::Uuid::new_v4(),
                bucket: "reports".to_string(),
                key: "2026/cities.csv".to_string(),
            },
        );

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(toast_count(window), 1);
    }

    #[cfg(unix)]
    #[gpui::test]
    fn two_spellings_of_one_file_share_a_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.csv");

        let dotted = file.directory.join(".").join("cities.csv");
        let link = file.directory.join("link.csv");
        std::os::unix::fs::symlink(&file.path, &link).expect("the test link must be creatable");

        open(window, &workspace, file.key());
        open(window, &workspace, DelimitedFileKey::Local { path: dotted });
        open(window, &workspace, DelimitedFileKey::Local { path: link });

        assert_eq!(tab_titles(window, &workspace), ["cities.csv"]);
        assert_eq!(toast_count(window), 0);
    }

    /// A path that cannot be resolved is opened as given, so the document
    /// reports why it cannot be read.
    #[gpui::test]
    fn a_missing_file_still_opens_a_tab_that_reports_the_failure(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.csv");
        let missing = file.directory.join("absent.csv");

        open(
            window,
            &workspace,
            DelimitedFileKey::Local { path: missing },
        );

        assert_eq!(tab_titles(window, &workspace), ["absent.csv"]);
        assert_eq!(toast_count(window), 1);
    }

    // -- Entry points ----------------------------------------------------------

    /// Opens `path` the way recent files, the command palette, the scripts
    /// sidebar, the settings window, IPC and the file dialog do.
    fn open_path(window: &mut VisualTestContext, workspace: &Entity<Workspace>, path: PathBuf) {
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_script_from_path(path, cx);
            });
        });
        window.run_until_parked();
    }

    fn tab_kinds(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Vec<DocumentKind> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .documents()
                .iter()
                .map(|tab| tab.kind())
                .collect()
        })
    }

    fn tab_states(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Vec<DocumentState> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .tab_manager
                .read(cx)
                .documents()
                .iter()
                .map(|tab| tab.meta_snapshot(cx).state)
                .collect()
        })
    }

    fn close_every_tab(window: &mut VisualTestContext, workspace: &Entity<Workspace>) {
        window.update(|_, cx| {
            let tab_manager = workspace.read(cx).tab_manager.clone();
            let ids: Vec<_> = tab_manager
                .read(cx)
                .documents()
                .iter()
                .map(|tab| tab.id())
                .collect();

            tab_manager.update(cx, |tab_manager, cx| {
                for id in ids {
                    tab_manager.close(id, cx);
                }
            });
        });
        window.run_until_parked();
    }

    fn recent_paths(window: &mut VisualTestContext, workspace: &Entity<Workspace>) -> Vec<PathBuf> {
        window.update(|_, cx| {
            workspace
                .read(cx)
                .app_state
                .read(cx)
                .recent_files()
                .iter()
                .map(|recent| recent.path.clone())
                .collect()
        })
    }

    #[gpui::test]
    fn a_path_ending_in_csv_or_tsv_opens_the_delimited_document_and_others_the_editor(
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let lower = TestFile::new("a.csv");
        let upper = TestFile::new("B.TSV");
        let text = TestFile::new("notes.txt");

        open_path(window, &workspace, lower.path.clone());
        open_path(window, &workspace, upper.path.clone());
        open_path(window, &workspace, text.path.clone());

        assert_eq!(
            tab_titles(window, &workspace),
            ["a.csv", "B.TSV", "notes.txt"]
        );
        assert_eq!(
            tab_kinds(window, &workspace),
            [
                DocumentKind::Delimited,
                DocumentKind::Delimited,
                DocumentKind::Script
            ]
        );
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn a_csv_opened_through_two_entry_points_shares_one_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.csv");
        let other = TestFile::new("notes.txt");

        open_path(window, &workspace, file.path.clone());
        open_path(window, &workspace, other.path.clone());
        open(window, &workspace, file.key());

        assert_eq!(tab_titles(window, &workspace), ["cities.csv", "notes.txt"]);

        let active_title = window.update(|_, cx| {
            let tab_manager = workspace.read(cx).tab_manager.read(cx);
            let active_id = tab_manager.active_id();

            tab_manager
                .documents()
                .iter()
                .find(|tab| Some(tab.id()) == active_id)
                .map(|tab| tab.tab_title(cx))
        });
        assert_eq!(active_title.as_deref(), Some("cities.csv"));
    }

    #[gpui::test]
    fn a_csv_is_kept_in_recent_files_and_reopens_from_there_as_a_table(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.csv");

        open(window, &workspace, file.key());

        let canonical = std::fs::canonicalize(&file.path).expect("the test file resolves");
        let recent = recent_paths(window, &workspace);
        assert_eq!(recent, [canonical]);

        close_every_tab(window, &workspace);
        assert!(tab_titles(window, &workspace).is_empty());

        open_path(window, &workspace, recent[0].clone());

        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Delimited]);
        assert_eq!(tab_states(window, &workspace), [DocumentState::Clean]);
    }

    /// The scripts sidebar lists every file of the managed folder, a CSV
    /// included, and opening one used to be refused as an unsupported type.
    #[gpui::test]
    fn a_csv_opened_from_the_scripts_sidebar_opens_the_delimited_document(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.csv");

        window.update(|_, cx| {
            let sidebar = workspace.read(cx).sidebar.clone();

            sidebar.update(cx, |_, cx| {
                cx.emit(dbflux_ui_sidebar::SidebarEvent::OpenScript {
                    path: file.path.clone(),
                });
            });
        });
        window.run_until_parked();

        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Delimited]);
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn a_csv_sent_over_ipc_opens_the_delimited_document(cx: &mut TestAppContext) {
        use dbflux_ipc::framing;
        use dbflux_ipc::protocol::{
            AppControlRequest, AppControlResponse, IpcMessage, IpcResponse,
        };
        use interprocess::local_socket::{
            GenericNamespaced, ListenerNonblockingMode, ListenerOptions, Stream as IpcStream,
            prelude::*,
        };

        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.csv");

        let socket = format!("dbflux-test-{}.sock", uuid::Uuid::new_v4());
        let listener = ListenerOptions::new()
            .name(
                socket
                    .clone()
                    .to_ns_name::<GenericNamespaced>()
                    .expect("a valid socket name"),
            )
            .nonblocking(ListenerNonblockingMode::Accept)
            .create_sync()
            .expect("the test socket binds");

        window.update(|window, cx| {
            crate::ipc_server::IpcServer::start_with_listener(
                listener,
                workspace.clone(),
                window.window_handle(),
                "token".to_string(),
                cx,
            );
        });

        let mut stream = IpcStream::connect(
            socket
                .to_ns_name::<GenericNamespaced>()
                .expect("a valid socket name"),
        )
        .expect("the test socket accepts");
        let request = AppControlRequest::new(
            1,
            Some("token".to_string()),
            IpcMessage::OpenScript {
                path: file.path.clone(),
            },
        );
        framing::send_msg(&mut stream, &request).expect("the request is sent");
        let response: AppControlResponse =
            framing::recv_msg(&mut stream).expect("the response arrives");
        assert!(matches!(response.body, IpcResponse::Ok));

        window
            .executor()
            .advance_clock(std::time::Duration::from_millis(50));
        window.run_until_parked();

        assert_eq!(tab_titles(window, &workspace), ["cities.csv"]);
        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Delimited]);
    }

    // -- Objects ---------------------------------------------------------------

    const BUCKET: &str = "reports";

    /// A connection whose only working part is an in-memory object store.
    struct ObjectStoreFake {
        objects: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
    }

    impl ObjectStoreFake {
        fn with_objects(objects: &[(&str, &[u8])]) -> std::sync::Arc<Self> {
            std::sync::Arc::new(Self {
                objects: std::sync::Mutex::new(
                    objects
                        .iter()
                        .map(|(key, bytes)| (key.to_string(), bytes.to_vec()))
                        .collect(),
                ),
            })
        }

        #[allow(clippy::result_large_err)]
        fn bytes(&self, bucket: &str, key: &str) -> Result<Vec<u8>, dbflux_core::DbError> {
            if bucket != BUCKET {
                return Err(dbflux_core::DbError::query_failed("NoSuchBucket"));
            }

            self.objects
                .lock()
                .expect("the object map")
                .get(key)
                .cloned()
                .ok_or_else(|| dbflux_core::DbError::query_failed("NoSuchKey"))
        }
    }

    #[allow(clippy::result_large_err)]
    fn not_used<T>() -> Result<T, dbflux_core::DbError> {
        Err(dbflux_core::DbError::NotSupported(
            "not used by these tests".to_string(),
        ))
    }

    impl dbflux_core::ObjectStoreConnection for ObjectStoreFake {
        fn list_buckets(&self) -> Result<Vec<dbflux_core::BucketInfo>, dbflux_core::DbError> {
            not_used()
        }

        fn list_objects(
            &self,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<&str>,
        ) -> Result<dbflux_core::ObjectListingPage, dbflux_core::DbError> {
            not_used()
        }

        fn head_object(
            &self,
            bucket: &str,
            key: &str,
        ) -> Result<dbflux_core::ObjectMetadata, dbflux_core::DbError> {
            let bytes = self.bytes(bucket, key)?;

            Ok(dbflux_core::ObjectMetadata {
                key: key.to_string(),
                size_bytes: bytes.len() as u64,
                content_type: None,
                last_modified: None,
                etag: Some("\"one\"".to_string()),
                storage_class: None,
                encryption: None,
                version_count: None,
            })
        }

        fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, dbflux_core::DbError> {
            self.bytes(bucket, key)
        }

        fn download_object(
            &self,
            _bucket: &str,
            _key: &str,
            _dest: &std::path::Path,
        ) -> Result<u64, dbflux_core::DbError> {
            not_used()
        }

        fn put_object(
            &self,
            _bucket: &str,
            _key: &str,
            _bytes: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<(), dbflux_core::DbError> {
            not_used()
        }

        fn upload_object(
            &self,
            _bucket: &str,
            _key: &str,
            _source_path: &std::path::Path,
            _content_type: Option<&str>,
        ) -> Result<(), dbflux_core::DbError> {
            not_used()
        }

        fn delete_object(&self, _bucket: &str, _key: &str) -> Result<(), dbflux_core::DbError> {
            not_used()
        }

        fn delete_prefix(
            &self,
            _bucket: &str,
            _prefix: &str,
        ) -> Result<dbflux_core::DeletePrefixOutcome, dbflux_core::DbError> {
            not_used()
        }

        fn copy_object(
            &self,
            _bucket: &str,
            _src_key: &str,
            _dest_key: &str,
        ) -> Result<(), dbflux_core::DbError> {
            not_used()
        }

        fn presign(
            &self,
            _bucket: &str,
            _key: &str,
            _method: dbflux_core::PresignMethod,
            _expiry: std::time::Duration,
        ) -> Result<String, dbflux_core::DbError> {
            not_used()
        }

        fn get_bucket_details(
            &self,
            _bucket: &str,
        ) -> Result<dbflux_core::BucketDetails, dbflux_core::DbError> {
            not_used()
        }

        fn estimate_bucket_size(
            &self,
            _bucket: &str,
            _object_cap: u64,
        ) -> Result<dbflux_core::BucketSizeEstimate, dbflux_core::DbError> {
            not_used()
        }

        fn list_object_versions(
            &self,
            _bucket: &str,
            _key: &str,
        ) -> Result<Vec<dbflux_core::ObjectVersionSummary>, dbflux_core::DbError> {
            not_used()
        }

        fn create_bucket(
            &self,
            _bucket: &str,
            _options: dbflux_core::BucketCreateOptions,
        ) -> Result<dbflux_core::BucketCreateOutcome, dbflux_core::DbError> {
            not_used()
        }

        fn delete_bucket(&self, _bucket: &str) -> Result<(), dbflux_core::DbError> {
            not_used()
        }
    }

    struct ObjectConnection {
        store: std::sync::Arc<ObjectStoreFake>,
    }

    impl dbflux_core::Connection for ObjectConnection {
        fn metadata(&self) -> &dbflux_core::DriverMetadata {
            static METADATA: std::sync::OnceLock<dbflux_core::DriverMetadata> =
                std::sync::OnceLock::new();

            METADATA.get_or_init(|| {
                dbflux_core::DriverMetadataBuilder::new(
                    "workspace-object-test",
                    "Workspace Object Test",
                    dbflux_core::DatabaseCategory::Relational,
                    dbflux_core::QueryLanguage::Sql,
                )
                .build()
            })
        }

        fn ping(&self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn close(&mut self) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn execute(
            &self,
            _request: &dbflux_core::QueryRequest,
        ) -> Result<dbflux_core::QueryResult, dbflux_core::DbError> {
            not_used()
        }

        fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), dbflux_core::DbError> {
            Ok(())
        }

        fn schema(&self) -> Result<dbflux_core::SchemaSnapshot, dbflux_core::DbError> {
            not_used()
        }

        fn kind(&self) -> dbflux_core::DbKind {
            dbflux_core::DbKind::SQLite
        }

        fn schema_loading_strategy(&self) -> dbflux_core::SchemaLoadingStrategy {
            dbflux_core::SchemaLoadingStrategy::SingleDatabase
        }

        fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
            &dbflux_core::DefaultSqlDialect
        }

        fn object_store_api(&self) -> Option<&dyn dbflux_core::ObjectStoreConnection> {
            Some(self.store.as_ref())
        }
    }

    /// Connects one profile of the workspace's app state to `store`, and
    /// sets the preview size limit to 0 bytes, so every object is over it.
    fn connect_object_store(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        store: std::sync::Arc<ObjectStoreFake>,
    ) -> uuid::Uuid {
        let profile_id = uuid::Uuid::new_v4();

        window.update(|_, cx| {
            let app_state = workspace.read(cx).app_state.clone();

            app_state.update(cx, |state, _| {
                let mut settings = state.general_settings().clone();
                settings.object_preview_size_limit_mib = 0;
                state.update_general_settings(settings);

                let profile = dbflux_core::ConnectionProfile::new(
                    "objects",
                    dbflux_core::DbConfig::SQLite {
                        path: PathBuf::from(":memory:"),
                        connection_id: None,
                    },
                );

                state.connections_mut().insert(
                    profile_id,
                    dbflux_core::ConnectedProfile {
                        profile,
                        connection: std::sync::Arc::new(ObjectConnection { store }),
                        schema: None,
                        mutation_policy: dbflux_core::MutationPolicy::default(),
                        read_only_reason: None,
                        database_schemas: Default::default(),
                        table_details: Default::default(),
                        collection_children: Default::default(),
                        schema_types: Default::default(),
                        schema_columns: Default::default(),
                        schema_indexes: Default::default(),
                        schema_foreign_keys: Default::default(),
                        schema_routines: Default::default(),
                        dependents_cache: Default::default(),
                        active_database: None,
                        redis_key_cache: Default::default(),
                        database_connections: Default::default(),
                        proxy_tunnel: None,
                    },
                );
            });
        });

        profile_id
    }

    /// Opens `key` the way the object browser's "Open in editor" does, through
    /// the request the workspace drains from the browser's pane.
    fn open_in_editor(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        profile_id: uuid::Uuid,
        key: &str,
        on_saved: crate::ui::document::ObjectSavedCallback,
    ) {
        let request = crate::ui::document::ObjectEditorRequest {
            bucket: BUCKET.to_string(),
            key: key.to_string(),
            on_saved,
        };

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_object_editor(profile_id, request, window, cx);
            });
        });
        window.run_until_parked();
    }

    fn ignore_saves() -> crate::ui::document::ObjectSavedCallback {
        std::rc::Rc::new(|_key: &str, _cx: &mut gpui::App| {})
    }

    #[gpui::test]
    fn open_in_editor_shows_a_csv_object_over_the_preview_limit_as_a_table(
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let store = ObjectStoreFake::with_objects(&[(
            "2026/cities.csv",
            b"name,city\nAna,Lima\nBo,Quito\n",
        )]);
        let profile_id = connect_object_store(window, &workspace, store);

        open_in_editor(
            window,
            &workspace,
            profile_id,
            "2026/cities.csv",
            ignore_saves(),
        );

        assert_eq!(tab_titles(window, &workspace), ["cities.csv"]);
        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Delimited]);
        assert_eq!(tab_states(window, &workspace), [DocumentState::Clean]);
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn open_in_editor_keeps_a_json_object_in_the_object_editor(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let store = ObjectStoreFake::with_objects(&[("2026/cities.json", b"{\"a\": 1}")]);
        let profile_id = connect_object_store(window, &workspace, store);

        open_in_editor(
            window,
            &workspace,
            profile_id,
            "2026/cities.json",
            ignore_saves(),
        );

        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::ObjectEditor]);
    }
}

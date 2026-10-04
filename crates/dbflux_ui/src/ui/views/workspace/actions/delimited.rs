use super::*;
use crate::ui::document::{DelimitedDocument, DelimitedFileKey, DocumentKey};
use crate::ui::labels::{NoActiveConnectionKind, documents_no_active_connection_message};

impl Workspace {
    /// Opens a CSV or TSV file as a table in its own tab, or focuses the tab
    /// that already shows it: one tab per local path and per
    /// `(profile_id, bucket, key)`.
    ///
    /// A local path is resolved first, so two spellings of one file and a
    /// symlink to it share a tab. A path that cannot be resolved is opened as
    /// given, and the document reports why it cannot be read.
    ///
    /// An object is read through the live connection of its profile, so a
    /// profile that is not connected is reported and opens nothing.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "no command, menu or dialog opens a delimited file yet"
        )
    )]
    pub(in crate::ui::views::workspace) fn open_delimited_file(
        &mut self,
        file: DelimitedFileKey,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let file = match file {
            DelimitedFileKey::Local { path } => DelimitedFileKey::Local {
                path: std::fs::canonicalize(&path).unwrap_or(path),
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
                    DelimitedDocument::open_object(
                        app_state, profile_id, connection, bucket, key, cx,
                    )
                })
            }
        };
        let pane = DelimitedDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.set_focus(FocusTarget::Document, window, cx);
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
    use crate::ui::document::{DelimitedFileKey, DocumentKind};
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
        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_delimited_file(file, window, cx);
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
}

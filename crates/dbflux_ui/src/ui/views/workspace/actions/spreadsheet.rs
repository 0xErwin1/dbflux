use super::*;
use crate::ui::document::{DocumentKey, FileDocumentKey, ObjectSavedCallback, SpreadsheetDocument};
use crate::ui::labels::{NoActiveConnectionKind, documents_no_active_connection_message};

impl Workspace {
    /// Opens a spreadsheet one sheet at a time in its own tab, or focuses the
    /// tab that already shows it: one tab per local path and per
    /// `(profile_id, bucket, key)`.
    ///
    /// A local path is resolved first, so two spellings of one file and a
    /// symlink to it share a tab. A path that resolves is kept in recent
    /// files. A path that cannot be resolved is opened as given, and the
    /// document reports why it cannot be read.
    ///
    /// An object is read through the live connection of its profile, so a
    /// profile that is not connected is reported and opens nothing. An
    /// object of a store that cannot read a byte range opens a tab that asks
    /// whether to download it whole, and closes when the user declines.
    /// `on_object_saved` is told the object's key after each save that
    /// replaces it.
    ///
    /// The keyboard moves to the new tab on the next render, so callers
    /// without a window can open one.
    pub(in crate::ui::views::workspace) fn open_spreadsheet_file(
        &mut self,
        file: FileDocumentKey,
        on_object_saved: Option<ObjectSavedCallback>,
        cx: &mut Context<Self>,
    ) {
        let file = match file {
            FileDocumentKey::Local { path } => match std::fs::canonicalize(&path) {
                Ok(resolved) => {
                    self.app_state.update(cx, |state, cx| {
                        state.record_recent_file(resolved.clone());
                        cx.emit(AppStateChanged);
                    });

                    FileDocumentKey::Local { path: resolved }
                }
                Err(_) => FileDocumentKey::Local { path },
            },
            object @ FileDocumentKey::Object { .. } => object,
        };

        let existing_id = self
            .tab_manager
            .read(cx)
            .find_by_key(&DocumentKey::FileDocument(file.clone()), cx);

        if let Some(id) = existing_id {
            self.tab_manager.update(cx, |manager, cx| {
                manager.activate(id, cx);
            });
            return;
        }

        let document = match file {
            FileDocumentKey::Local { path } => {
                cx.new(|cx| SpreadsheetDocument::open_local(path, cx))
            }

            FileDocumentKey::Object {
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
                    let mut document = SpreadsheetDocument::open_object(
                        app_state, profile_id, connection, bucket, key, cx,
                    );

                    if let Some(on_saved) = on_object_saved {
                        document.set_on_object_saved(on_saved);
                    }

                    document
                })
            }
        };
        let pane = SpreadsheetDocument::into_pane(document, cx);

        self.tab_manager.update(cx, |manager, cx| {
            manager.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.pending_focus = Some(FocusTarget::Document);
        cx.notify();
    }
}

#[cfg(test)]
mod tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::ui::document::{DocumentKind, DocumentState, FileDocumentKey};
    use crate::ui::views::workspace::Workspace;
    use crate::ui::views::workspace::actions::delimited::tests::{
        BUCKET, ObjectStoreFake, connect_object_store,
    };
    use crate::ui::views::workspace::actions::local_file::tests::{
        close_every_tab, new_workspace, open_path, recent_paths, tab_kinds, tab_states, tab_titles,
        toast_count,
    };
    use gpui::{Entity, TestAppContext, VisualTestContext};
    use std::path::PathBuf;

    /// The bytes of a workbook checked in under
    /// `dbflux_spreadsheet/tests/fixtures/`.
    fn fixture(name: &str) -> Vec<u8> {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../dbflux_spreadsheet/tests/fixtures")
            .join(name);

        std::fs::read(&path).expect("the fixture must be readable")
    }

    /// A copy of the `in.ods` fixture in a private directory that is removed
    /// when the test ends.
    struct TestFile {
        directory: PathBuf,
        path: PathBuf,
    }

    impl TestFile {
        fn new(name: &str) -> Self {
            let directory = std::env::temp_dir()
                .join(format!("dbflux-open-spreadsheet-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&directory).expect("the test directory must be creatable");

            let path = directory.join(name);
            std::fs::write(&path, fixture("in.ods")).expect("the test file must be writable");

            Self { directory, path }
        }

        fn key(&self) -> FileDocumentKey {
            FileDocumentKey::Local {
                path: self.path.clone(),
            }
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.directory).ok();
        }
    }

    /// Opens `file` the way the object browser and every other caller of the
    /// format dispatch do.
    fn open(window: &mut VisualTestContext, workspace: &Entity<Workspace>, file: FileDocumentKey) {
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_file_document(file, None, cx);
            });
        });
        window.run_until_parked();
    }

    fn active_title(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Option<String> {
        window.update(|_, cx| {
            let tab_manager = workspace.read(cx).tab_manager.read(cx);
            let active_id = tab_manager.active_id();

            tab_manager
                .documents()
                .iter()
                .find(|tab| Some(tab.id()) == active_id)
                .map(|tab| tab.tab_title(cx))
        })
    }

    #[gpui::test]
    fn opening_the_same_file_twice_focuses_one_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let first = TestFile::new("budget.ods");
        let second = TestFile::new("plan.ods");

        open(window, &workspace, first.key());
        open(window, &workspace, second.key());
        open(window, &workspace, first.key());

        assert_eq!(tab_titles(window, &workspace), ["budget.ods", "plan.ods"]);
        assert_eq!(
            tab_kinds(window, &workspace),
            [DocumentKind::Spreadsheet, DocumentKind::Spreadsheet]
        );
        assert_eq!(
            tab_states(window, &workspace),
            [DocumentState::Clean, DocumentState::Clean]
        );
        assert_eq!(
            active_title(window, &workspace).as_deref(),
            Some("budget.ods")
        );
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn a_spreadsheet_path_in_any_case_opens_from_recent_files(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("BUDGET.ODS");

        open_path(window, &workspace, file.path.clone());

        let canonical = std::fs::canonicalize(&file.path).expect("the test file resolves");
        let recent = recent_paths(window, &workspace);
        assert_eq!(recent, [canonical]);

        close_every_tab(window, &workspace);
        assert!(tab_titles(window, &workspace).is_empty());

        open_path(window, &workspace, recent[0].clone());

        assert_eq!(tab_titles(window, &workspace), ["BUDGET.ODS"]);
        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Spreadsheet]);
    }

    #[gpui::test]
    fn a_spreadsheet_object_opens_in_its_own_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = fixture("in.ods");
        let store = ObjectStoreFake::with_ranged_objects(&[("2026/budget.ods", &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store);
        let object = FileDocumentKey::Object {
            profile_id,
            bucket: BUCKET.to_string(),
            key: "2026/budget.ods".to_string(),
        };

        open(window, &workspace, object.clone());
        open(window, &workspace, object);

        assert_eq!(tab_titles(window, &workspace), ["budget.ods"]);
        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Spreadsheet]);
        assert_eq!(tab_states(window, &workspace), [DocumentState::Clean]);
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn a_spreadsheet_object_of_a_profile_that_is_not_connected_opens_no_tab(
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);

        open(
            window,
            &workspace,
            FileDocumentKey::Object {
                profile_id: uuid::Uuid::new_v4(),
                bucket: BUCKET.to_string(),
                key: "2026/budget.ods".to_string(),
            },
        );

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(toast_count(window), 1);
    }
}

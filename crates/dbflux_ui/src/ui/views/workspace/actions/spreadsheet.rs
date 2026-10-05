use super::*;
use crate::ui::document::{DocumentKey, FileDocumentKey, SpreadsheetDocument};
use crate::ui::labels::spreadsheet_objects_unsupported_message;

impl Workspace {
    /// Opens a local spreadsheet read-only, one sheet at a time, in its own
    /// tab, or focuses the tab that already shows it: one tab per local path.
    ///
    /// The path is resolved first, so two spellings of one file and a
    /// symlink to it share a tab. A path that resolves is kept in recent
    /// files. A path that cannot be resolved is opened as given, and the
    /// document reports why it cannot be read.
    ///
    /// A spreadsheet object of an object store is refused with a message
    /// that says to download it: objects do not open in this document yet.
    ///
    /// The keyboard moves to the new tab on the next render, so callers
    /// without a window can open one.
    pub(in crate::ui::views::workspace) fn open_spreadsheet_file(
        &mut self,
        file: FileDocumentKey,
        cx: &mut Context<Self>,
    ) {
        let path = match file {
            FileDocumentKey::Local { path } => path,

            FileDocumentKey::Object { key, .. } => {
                let name = key.rsplit('/').next().unwrap_or(&key);

                report_error(
                    UserFacingError::new(
                        ErrorKind::User,
                        spreadsheet_objects_unsupported_message(name),
                    ),
                    cx,
                );
                return;
            }
        };

        let path = match std::fs::canonicalize(&path) {
            Ok(resolved) => {
                self.app_state.update(cx, |state, cx| {
                    state.record_recent_file(resolved.clone());
                    cx.emit(AppStateChanged);
                });

                resolved
            }
            Err(_) => path,
        };

        let key = DocumentKey::FileDocument(FileDocumentKey::Local { path: path.clone() });

        if let Some(id) = self.tab_manager.read(cx).find_by_key(&key, cx) {
            self.tab_manager.update(cx, |manager, cx| {
                manager.activate(id, cx);
            });
            return;
        }

        let document = cx.new(|cx| SpreadsheetDocument::open_local(path, cx));
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

    fn last_toast_title(window: &mut VisualTestContext) -> Option<String> {
        window.update(|_, cx| {
            cx.global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .read(cx)
                .last_toast_title()
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
    fn a_spreadsheet_object_is_refused_until_objects_are_supported(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = fixture("in.ods");
        let store = ObjectStoreFake::with_ranged_objects(&[("2026/budget.ods", &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store);

        open(
            window,
            &workspace,
            FileDocumentKey::Object {
                profile_id,
                bucket: BUCKET.to_string(),
                key: "2026/budget.ods".to_string(),
            },
        );

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(toast_count(window), 1);
        assert_eq!(
            last_toast_title(window),
            Some(dbflux_i18n::t!(
                "document.spreadsheet.error.objects_unsupported",
                name = "budget.ods"
            ))
        );
    }
}

use super::*;
use crate::ui::document::{
    DelimitedDocument, DocumentKey, FileDocumentKey, PaneHandle, ParquetDocument,
};
use crate::ui::labels::{NoActiveConnectionKind, documents_no_active_connection_message};
use dbflux_storage::repositories::state::sessions::RestoredTab;
use dbflux_ui_base::AppStateEntity;
use std::path::PathBuf;
use std::sync::Arc;

/// A document that shows one local file or one stored object in a tab, keyed
/// by [`DocumentKey::FileDocument`].
pub(super) trait LocalFileDocument: Sized + 'static {
    /// How restore log lines name a tab of this document.
    const LOG_LABEL: &'static str;

    fn open_local(path: PathBuf, cx: &mut Context<Self>) -> Self;

    fn open_object(
        app_state: Entity<AppStateEntity>,
        profile_id: uuid::Uuid,
        connection: Arc<dyn dbflux_core::Connection>,
        bucket: String,
        key: String,
        cx: &mut Context<Self>,
    ) -> Self;

    fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle;
}

impl LocalFileDocument for DelimitedDocument {
    const LOG_LABEL: &'static str = "Delimited";

    fn open_local(path: PathBuf, cx: &mut Context<Self>) -> Self {
        DelimitedDocument::open_local(path, cx)
    }

    fn open_object(
        app_state: Entity<AppStateEntity>,
        profile_id: uuid::Uuid,
        connection: Arc<dyn dbflux_core::Connection>,
        bucket: String,
        key: String,
        cx: &mut Context<Self>,
    ) -> Self {
        DelimitedDocument::open_object(app_state, profile_id, connection, bucket, key, cx)
    }

    fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        DelimitedDocument::into_pane(entity, cx)
    }
}

impl LocalFileDocument for ParquetDocument {
    const LOG_LABEL: &'static str = "Parquet";

    fn open_local(path: PathBuf, cx: &mut Context<Self>) -> Self {
        ParquetDocument::open_local(path, cx)
    }

    fn open_object(
        app_state: Entity<AppStateEntity>,
        profile_id: uuid::Uuid,
        connection: Arc<dyn dbflux_core::Connection>,
        bucket: String,
        key: String,
        cx: &mut Context<Self>,
    ) -> Self {
        ParquetDocument::open_object(app_state, profile_id, connection, bucket, key, cx)
    }

    fn into_pane(entity: Entity<Self>, cx: &App) -> PaneHandle {
        ParquetDocument::into_pane(entity, cx)
    }
}

impl Workspace {
    /// Opens `file` as a `D` in its own tab, or focuses the tab that already
    /// shows it: one tab per local path and per `(profile_id, bucket, key)`.
    ///
    /// A local path is resolved first, so two spellings of one file and a
    /// symlink to it share a tab. A path that resolves is kept in recent
    /// files. A path that cannot be resolved is opened as given, and the
    /// document reports why it cannot be read.
    ///
    /// An object is read through the live connection of its profile, so a
    /// profile that is not connected is reported and opens nothing.
    /// `configure_object` runs on a document opened for an object, before it
    /// is shown.
    ///
    /// The keyboard moves to the new tab on the next render, so callers
    /// without a window can open one.
    pub(super) fn open_local_file_tab<D: LocalFileDocument>(
        &mut self,
        file: FileDocumentKey,
        configure_object: impl FnOnce(&mut D),
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
            self.tab_manager.update(cx, |mgr, cx| {
                mgr.activate(id, cx);
            });
            return;
        }

        let doc = match file {
            FileDocumentKey::Local { path } => cx.new(|cx| D::open_local(path, cx)),

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
                    let mut document =
                        D::open_object(app_state, profile_id, connection, bucket, key, cx);

                    configure_object(&mut document);

                    document
                })
            }
        };
        let pane = D::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });

        self.pending_focus = Some(FocusTarget::Document);
        cx.notify();
    }

    /// Reopens a local file the workspace session recorded as a `D`.
    ///
    /// A file that cannot be opened any more is skipped with a log line, as a
    /// file-backed script that cannot be read is, so startup raises no toast
    /// for a failure the user did not just cause. The path is resolved as
    /// [`Self::open_local_file_tab`] resolves it, because it is the tab's
    /// dedup key: a session written by an earlier build or by hand can spell
    /// one file two ways. Unlike opening, restoring does not record the file
    /// in recent files and does not take the keyboard.
    pub(super) fn restore_local_file_tab<D: LocalFileDocument>(
        &mut self,
        tab: &RestoredTab,
        cx: &mut Context<Self>,
    ) {
        let Some(stored_path) = tab.file_path.as_ref() else {
            log::warn!(
                "{} tab '{}' has no file_path in restored session — skipping",
                D::LOG_LABEL,
                tab.title
            );
            return;
        };

        let path = match std::fs::canonicalize(stored_path) {
            Ok(resolved) => resolved,
            Err(error) => {
                log::warn!(
                    "{} tab '{}' cannot resolve {}: {error} — skipping",
                    D::LOG_LABEL,
                    tab.title,
                    stored_path.display()
                );
                return;
            }
        };

        if let Err(error) = std::fs::File::open(&path) {
            log::warn!(
                "{} tab '{}' cannot open {}: {error} — skipping",
                D::LOG_LABEL,
                tab.title,
                path.display()
            );
            return;
        }

        let key = DocumentKey::FileDocument(FileDocumentKey::Local { path: path.clone() });

        if self.tab_manager.read(cx).find_by_key(&key, cx).is_some() {
            return;
        }

        let doc = cx.new(|cx| D::open_local(path, cx));
        let pane = D::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
        });
    }
}

/// Helpers shared by the tests of every local file document, and the
/// scenarios of the open and restore flow, run once per format.
#[cfg(test)]
pub(super) mod tests {
    // Explicit imports, not `use super::*`: the parent glob together with
    // `#[gpui::test]` sends the macro expansion into unbounded recursion.
    use crate::ui::document::{DocumentKind, DocumentState, FileDocumentKey};
    use crate::ui::views::workspace::Workspace;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use dbflux_ui_base::AppStateEntity;
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::sync::Arc;

    /// A small Parquet file of ids and city names.
    pub(in crate::ui::views::workspace::actions) fn cities_parquet() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("city", DataType::Utf8, false),
        ]));

        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let cities: ArrayRef = Arc::new(StringArray::from(vec!["Lima", "Quito"]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![ids, cities]).expect("the batch must build");

        let mut buffer = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, schema, None)
            .expect("the writer must open");
        writer.write(&batch).expect("the batch must be written");
        writer.close().expect("the writer must close");

        buffer
    }

    /// A file in a private directory that is removed when the test ends. A
    /// name ending in `.parquet`, in any case, holds [`cities_parquet`]. Any
    /// other name holds a small CSV of names and cities.
    pub(in crate::ui::views::workspace::actions) struct TestFile {
        pub(in crate::ui::views::workspace::actions) directory: PathBuf,
        pub(in crate::ui::views::workspace::actions) path: PathBuf,
    }

    impl TestFile {
        pub(in crate::ui::views::workspace::actions) fn new(name: &str) -> Self {
            let directory =
                std::env::temp_dir().join(format!("dbflux-open-file-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&directory).expect("the test directory must be creatable");

            let is_parquet = std::path::Path::new(name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"));
            let content = if is_parquet {
                cities_parquet()
            } else {
                b"name,city\nAna,Lima\n".to_vec()
            };

            let path = directory.join(name);
            std::fs::write(&path, content).expect("the test file must be writable");

            Self { directory, path }
        }

        pub(in crate::ui::views::workspace::actions) fn key(&self) -> FileDocumentKey {
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

    pub(in crate::ui::views::workspace::actions) fn new_workspace(
        cx: &mut TestAppContext,
    ) -> (Entity<Workspace>, &mut VisualTestContext) {
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

    pub(in crate::ui::views::workspace::actions) fn toast_count(
        window: &mut VisualTestContext,
    ) -> usize {
        window.update(|_, cx| {
            cx.global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .read(cx)
                .toast_count()
        })
    }

    pub(in crate::ui::views::workspace::actions) fn tab_titles(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Vec<String> {
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

    /// Opens `path` the way recent files, the command palette, the scripts
    /// sidebar, the settings window, IPC and the file dialog do.
    pub(in crate::ui::views::workspace::actions) fn open_path(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        path: PathBuf,
    ) {
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_script_from_path(path, cx);
            });
        });
        window.run_until_parked();
    }

    pub(in crate::ui::views::workspace::actions) fn tab_kinds(
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

    pub(in crate::ui::views::workspace::actions) fn tab_states(
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

    pub(in crate::ui::views::workspace::actions) fn close_every_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) {
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

    pub(in crate::ui::views::workspace::actions) fn recent_paths(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Vec<PathBuf> {
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

    /// The tabs the workspace session holds in its storage, in their order.
    pub(in crate::ui::views::workspace::actions) fn session_tabs(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
    ) -> Vec<dbflux_storage::repositories::state::sessions::RestoredTab> {
        window.update(|_, cx| {
            let runtime = workspace.read(cx).app_state.read(cx).storage_runtime();

            runtime
                .sessions()
                .restore_session(runtime.artifacts())
                .expect("the session must be readable")
                .map(|session| session.tabs)
                .unwrap_or_default()
        })
    }

    /// A session tab of `tab_kind` on `file_path`, as `write_session_manifest`
    /// records it.
    pub(in crate::ui::views::workspace::actions) fn session_tab(
        tab_kind: &str,
        title: &str,
        file_path: Option<PathBuf>,
        position: usize,
    ) -> dbflux_storage::repositories::state::sessions::WorkspaceTab {
        dbflux_storage::repositories::state::sessions::WorkspaceTab {
            id: uuid::Uuid::new_v4().to_string(),
            tab_kind: tab_kind.to_string(),
            language: "sql".to_string(),
            exec_ctx: dbflux_core::ExecutionContext::default(),
            scratch_path: None,
            shadow_path: None,
            file_path,
            title: title.to_string(),
            position,
            is_pinned: false,
        }
    }

    /// Stores a session holding `tabs`, then restores it into the workspace
    /// the way startup does.
    pub(in crate::ui::views::workspace::actions) fn restore(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        tabs: Vec<dbflux_storage::repositories::state::sessions::WorkspaceTab>,
        active_index: Option<usize>,
    ) {
        window.update(|_, cx| {
            let runtime = workspace.read(cx).app_state.read(cx).storage_runtime();

            runtime
                .sessions()
                .save_workspace_session(
                    &dbflux_storage::repositories::state::sessions::WorkspaceSessionManifest {
                        version: 1,
                        active_index,
                        tabs,
                    },
                )
                .expect("the session must be storable");
        });

        window.update(|window, cx| {
            workspace.update(cx, |workspace, cx| workspace.restore_session(window, cx));
        });
        window.run_until_parked();
    }

    pub(in crate::ui::views::workspace::actions) fn active_title(
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

    // -- Scenarios run for every format --------------------------------------

    /// What a scenario needs to know about one local file format.
    struct Format {
        extension: &'static str,
        kind: DocumentKind,
        session_tab_kind: &'static str,
    }

    const DELIMITED: Format = Format {
        extension: "csv",
        kind: DocumentKind::Delimited,
        session_tab_kind: "Delimited",
    };

    const PARQUET: Format = Format {
        extension: "parquet",
        kind: DocumentKind::Parquet,
        session_tab_kind: "Parquet",
    };

    impl Format {
        fn file_name(&self, stem: &str) -> String {
            format!("{stem}.{}", self.extension)
        }
    }

    /// Runs `scenario` once for every format, each in its own test named
    /// after the format.
    macro_rules! for_each_format {
        ($(#[$attribute:meta])* $scenario:ident) => {
            $(#[$attribute])*
            mod $scenario {
                #[gpui::test]
                fn delimited(cx: &mut gpui::TestAppContext) {
                    super::$scenario(&super::DELIMITED, cx);
                }

                #[gpui::test]
                fn parquet(cx: &mut gpui::TestAppContext) {
                    super::$scenario(&super::PARQUET, cx);
                }
            }
        };
    }

    /// Opens `file` the way the object browser and every other caller of the
    /// format dispatch do.
    pub(in crate::ui::views::workspace::actions) fn open(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        file: FileDocumentKey,
    ) {
        window.update(|_, cx| {
            workspace.update(cx, |workspace, cx| {
                workspace.open_file_document(file, None, cx);
            });
        });
        window.run_until_parked();
    }

    fn opening_the_same_file_again_focuses_its_tab(format: &Format, cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let first = TestFile::new(&format.file_name("cities"));
        let second = TestFile::new(&format.file_name("people"));

        open(window, &workspace, first.key());
        open(window, &workspace, second.key());
        open(window, &workspace, first.key());

        assert_eq!(
            tab_titles(window, &workspace),
            [format.file_name("cities"), format.file_name("people")]
        );
        assert_eq!(tab_kinds(window, &workspace), [format.kind, format.kind]);
        assert_eq!(
            active_title(window, &workspace),
            Some(format.file_name("cities"))
        );
        assert_eq!(toast_count(window), 0);
    }

    for_each_format!(opening_the_same_file_again_focuses_its_tab);

    fn an_object_of_a_profile_that_is_not_connected_opens_no_tab(
        format: &Format,
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);

        open(
            window,
            &workspace,
            FileDocumentKey::Object {
                profile_id: uuid::Uuid::new_v4(),
                bucket: "reports".to_string(),
                key: format!("2026/{}", format.file_name("cities")),
            },
        );

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(toast_count(window), 1);
    }

    for_each_format!(an_object_of_a_profile_that_is_not_connected_opens_no_tab);

    fn a_file_is_kept_in_recent_files_and_reopens_from_there(
        format: &Format,
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new(&format.file_name("cities"));

        open(window, &workspace, file.key());

        let canonical = std::fs::canonicalize(&file.path).expect("the test file resolves");
        let recent = recent_paths(window, &workspace);
        assert_eq!(recent, [canonical]);

        close_every_tab(window, &workspace);
        assert!(tab_titles(window, &workspace).is_empty());

        open_path(window, &workspace, recent[0].clone());

        assert_eq!(tab_kinds(window, &workspace), [format.kind]);
        assert_eq!(tab_states(window, &workspace), [DocumentState::Clean]);
    }

    for_each_format!(a_file_is_kept_in_recent_files_and_reopens_from_there);

    /// The scripts sidebar lists every file of the managed folder, a table
    /// file included, and opening one used to be refused as an unsupported
    /// type.
    fn a_file_opened_from_the_scripts_sidebar_opens_its_document(
        format: &Format,
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new(&format.file_name("cities"));

        window.update(|_, cx| {
            let sidebar = workspace.read(cx).sidebar.clone();

            sidebar.update(cx, |_, cx| {
                cx.emit(dbflux_ui_sidebar::SidebarEvent::OpenScript {
                    path: file.path.clone(),
                });
            });
        });
        window.run_until_parked();

        assert_eq!(tab_kinds(window, &workspace), [format.kind]);
        assert_eq!(toast_count(window), 0);
    }

    for_each_format!(a_file_opened_from_the_scripts_sidebar_opens_its_document);

    fn a_file_sent_over_ipc_opens_its_document(format: &Format, cx: &mut TestAppContext) {
        use dbflux_ipc::framing;
        use dbflux_ipc::protocol::{
            AppControlRequest, AppControlResponse, IpcMessage, IpcResponse,
        };
        use interprocess::local_socket::{
            GenericNamespaced, ListenerNonblockingMode, ListenerOptions, Stream as IpcStream,
            prelude::*,
        };

        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new(&format.file_name("cities"));

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

        assert_eq!(tab_titles(window, &workspace), [format.file_name("cities")]);
        assert_eq!(tab_kinds(window, &workspace), [format.kind]);
    }

    for_each_format!(a_file_sent_over_ipc_opens_its_document);

    fn an_open_local_file_is_recorded_in_the_session_by_its_path(
        format: &Format,
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new(&format.file_name("cities"));

        open(window, &workspace, file.key());

        let tabs = session_tabs(window, &workspace);
        let resolved = std::fs::canonicalize(&file.path).expect("the test file must resolve");

        assert_eq!(tabs.len(), 1);
        assert_eq!(tabs[0].tab_kind, format.session_tab_kind);
        assert_eq!(tabs[0].file_path.as_deref(), Some(resolved.as_path()));
        assert_eq!(tabs[0].title, format.file_name("cities"));
    }

    for_each_format!(an_open_local_file_is_recorded_in_the_session_by_its_path);

    /// A file that is gone at startup is skipped, as a file-backed script is,
    /// without a toast for a failure the user did not just cause.
    fn a_file_missing_at_restore_is_skipped_without_a_report(
        format: &Format,
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new(&format.file_name("cities"));
        let missing = file.directory.join(format.file_name("absent"));

        restore(
            window,
            &workspace,
            vec![
                session_tab(
                    format.session_tab_kind,
                    &format.file_name("absent"),
                    Some(missing),
                    0,
                ),
                session_tab(
                    format.session_tab_kind,
                    &format.file_name("no-path"),
                    None,
                    1,
                ),
                session_tab(
                    format.session_tab_kind,
                    &format.file_name("cities"),
                    Some(file.path.clone()),
                    2,
                ),
            ],
            Some(0),
        );

        assert_eq!(tab_titles(window, &workspace), [format.file_name("cities")]);
        assert_eq!(toast_count(window), 0);
    }

    for_each_format!(a_file_missing_at_restore_is_skipped_without_a_report);

    /// A session can hold a path spelled differently from the one opening the
    /// file uses, written by an earlier build or by hand. Restoring resolves
    /// it, so the file keeps one tab and a later open focuses that tab.
    #[cfg(unix)]
    fn a_restored_link_and_the_file_it_names_share_one_tab(
        format: &Format,
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new(&format.file_name("cities"));

        let link = file.directory.join(format.file_name("link"));
        std::os::unix::fs::symlink(&file.path, &link).expect("the test link must be creatable");
        let dotted = file.directory.join(".").join(format.file_name("cities"));

        restore(
            window,
            &workspace,
            vec![
                session_tab(
                    format.session_tab_kind,
                    &format.file_name("link"),
                    Some(link),
                    0,
                ),
                session_tab(
                    format.session_tab_kind,
                    &format.file_name("cities"),
                    Some(dotted),
                    1,
                ),
            ],
            Some(0),
        );

        assert_eq!(tab_titles(window, &workspace), [format.file_name("cities")]);

        open(window, &workspace, file.key());

        assert_eq!(tab_titles(window, &workspace), [format.file_name("cities")]);
        assert_eq!(toast_count(window), 0);
    }

    for_each_format!(
        #[cfg(unix)]
        a_restored_link_and_the_file_it_names_share_one_tab
    );
}

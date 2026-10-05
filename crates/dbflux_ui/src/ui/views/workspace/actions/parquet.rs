use super::*;
use crate::ui::document::{DocumentKey, FileDocumentKey, ParquetDocument};
use crate::ui::labels::{NoActiveConnectionKind, documents_no_active_connection_message};

impl Workspace {
    /// Opens a Parquet file read-only as a table in its own tab, or focuses
    /// the tab that already shows it: one tab per local path and per
    /// `(profile_id, bucket, key)`.
    ///
    /// A local path is resolved first, so two spellings of one file and a
    /// symlink to it share a tab. A path that resolves is kept in recent
    /// files. A path that cannot be resolved is opened as given, and the
    /// document reports why it cannot be read.
    ///
    /// An object is read through the live connection of its profile, so a
    /// profile that is not connected is reported and opens nothing. A store
    /// that cannot read a byte range of an object is refused with a message:
    /// every footer and page read would download the whole object.
    ///
    /// The keyboard moves to the new tab on the next render, so callers
    /// without a window can open one.
    pub(in crate::ui::views::workspace) fn open_parquet_file(
        &mut self,
        file: FileDocumentKey,
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
            FileDocumentKey::Local { path } => cx.new(|cx| ParquetDocument::open_local(path, cx)),

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

                let reads_ranges = connection
                    .object_store_api()
                    .is_some_and(|api| api.supports_range_reads());

                if !reads_ranges {
                    let name = key.rsplit('/').next().unwrap_or(&key);

                    report_error(
                        UserFacingError::new(
                            ErrorKind::User,
                            dbflux_i18n::t!("document.parquet.error.open_failed", name = name),
                        )
                        .with_cause(dbflux_i18n::t!(
                            "document.parquet.error.object_without_range_reads"
                        )),
                        cx,
                    );
                    return;
                }

                let app_state = self.app_state.clone();

                cx.new(|cx| {
                    ParquetDocument::open_object(app_state, profile_id, connection, bucket, key, cx)
                })
            }
        };
        let pane = ParquetDocument::into_pane(doc, cx);

        self.tab_manager.update(cx, |mgr, cx| {
            mgr.open(Tab::Pane(Box::new(pane)), cx);
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
        BUCKET, ObjectStoreFake, close_every_tab, connect_object_store, new_workspace, open_path,
        recent_paths, tab_kinds, tab_states, tab_titles, toast_count,
    };
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use gpui::{Entity, TestAppContext, VisualTestContext};
    use std::path::PathBuf;
    use std::sync::Arc;

    /// A small Parquet file of ids and city names.
    fn cities_parquet() -> Vec<u8> {
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

    /// A Parquet file in a private directory that is removed when the test
    /// ends.
    struct TestFile {
        directory: PathBuf,
        path: PathBuf,
    }

    impl TestFile {
        fn new(name: &str) -> Self {
            let directory =
                std::env::temp_dir().join(format!("dbflux-open-parquet-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&directory).expect("the test directory must be creatable");

            let path = directory.join(name);
            std::fs::write(&path, cities_parquet()).expect("the test file must be writable");

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
        let first = TestFile::new("cities.parquet");
        let second = TestFile::new("people.parquet");

        open(window, &workspace, first.key());
        open(window, &workspace, second.key());
        open(window, &workspace, first.key());

        assert_eq!(
            tab_titles(window, &workspace),
            ["cities.parquet", "people.parquet"]
        );
        assert_eq!(
            tab_kinds(window, &workspace),
            [DocumentKind::Parquet, DocumentKind::Parquet]
        );
        assert_eq!(
            active_title(window, &workspace).as_deref(),
            Some("cities.parquet")
        );
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn a_path_ending_in_parquet_opens_the_parquet_document_in_any_case(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let lower = TestFile::new("a.parquet");
        let upper = TestFile::new("B.PARQUET");

        open_path(window, &workspace, lower.path.clone());
        open_path(window, &workspace, upper.path.clone());

        assert_eq!(tab_titles(window, &workspace), ["a.parquet", "B.PARQUET"]);
        assert_eq!(
            tab_kinds(window, &workspace),
            [DocumentKind::Parquet, DocumentKind::Parquet]
        );
        assert_eq!(
            tab_states(window, &workspace),
            [DocumentState::Clean, DocumentState::Clean]
        );
    }

    #[gpui::test]
    fn a_parquet_file_is_kept_in_recent_files_and_reopens_from_there(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.parquet");

        open(window, &workspace, file.key());

        let canonical = std::fs::canonicalize(&file.path).expect("the test file resolves");
        let recent = recent_paths(window, &workspace);
        assert_eq!(recent, [canonical]);

        close_every_tab(window, &workspace);
        assert!(tab_titles(window, &workspace).is_empty());

        open_path(window, &workspace, recent[0].clone());

        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Parquet]);
        assert_eq!(tab_states(window, &workspace), [DocumentState::Clean]);
    }

    #[gpui::test]
    fn a_parquet_file_opened_from_the_scripts_sidebar_opens_the_parquet_document(
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.parquet");

        window.update(|_, cx| {
            let sidebar = workspace.read(cx).sidebar.clone();

            sidebar.update(cx, |_, cx| {
                cx.emit(dbflux_ui_sidebar::SidebarEvent::OpenScript {
                    path: file.path.clone(),
                });
            });
        });
        window.run_until_parked();

        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Parquet]);
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn a_parquet_file_sent_over_ipc_opens_the_parquet_document(cx: &mut TestAppContext) {
        use dbflux_ipc::framing;
        use dbflux_ipc::protocol::{
            AppControlRequest, AppControlResponse, IpcMessage, IpcResponse,
        };
        use interprocess::local_socket::{
            GenericNamespaced, ListenerNonblockingMode, ListenerOptions, Stream as IpcStream,
            prelude::*,
        };

        let (workspace, window) = new_workspace(cx);
        let file = TestFile::new("cities.parquet");

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

        assert_eq!(tab_titles(window, &workspace), ["cities.parquet"]);
        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Parquet]);
    }

    fn object_key(profile_id: uuid::Uuid) -> FileDocumentKey {
        FileDocumentKey::Object {
            profile_id,
            bucket: BUCKET.to_string(),
            key: "2026/cities.parquet".to_string(),
        }
    }

    #[gpui::test]
    fn an_object_of_a_store_with_ranged_reads_opens_the_parquet_document(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = cities_parquet();
        let store = ObjectStoreFake::with_ranged_objects(&[("2026/cities.parquet", &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store);

        open(window, &workspace, object_key(profile_id));

        assert_eq!(tab_titles(window, &workspace), ["cities.parquet"]);
        assert_eq!(tab_kinds(window, &workspace), [DocumentKind::Parquet]);
        assert_eq!(tab_states(window, &workspace), [DocumentState::Clean]);
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn an_object_of_a_store_without_ranged_reads_is_refused_with_a_message(
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let bytes = cities_parquet();
        let store = ObjectStoreFake::with_objects(&[("2026/cities.parquet", &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store);

        open(window, &workspace, object_key(profile_id));

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(toast_count(window), 1);

        let last_toast = window.update(|_, cx| {
            cx.global::<dbflux_ui_base::toast::ToastGlobal>()
                .host
                .read(cx)
                .last_toast_title()
        });
        assert!(
            last_toast
                .as_deref()
                .is_some_and(|title| title.contains("cities.parquet")),
            "{last_toast:?}"
        );
    }

    #[gpui::test]
    fn an_object_of_a_profile_that_is_not_connected_opens_no_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);

        open(window, &workspace, object_key(uuid::Uuid::new_v4()));

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(toast_count(window), 1);
    }
}

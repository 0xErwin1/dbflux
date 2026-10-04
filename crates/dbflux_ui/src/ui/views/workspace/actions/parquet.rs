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
    /// profile that is not connected is reported and opens nothing. An
    /// object of a store that cannot read a byte range opens a tab that asks
    /// whether to download it whole, and closes when the user declines.
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
    use crate::ui::document::file_source::{LocationSource, ObjectReads, download_whole_object};
    use crate::ui::document::{DocumentKind, DocumentState, FileDocumentKey, ParquetDocument, Tab};
    use crate::ui::views::workspace::Workspace;
    use crate::ui::views::workspace::actions::delimited::tests::{
        BUCKET, ObjectStoreFake, close_every_tab, connect_object_store, new_workspace, open_path,
        recent_paths, tab_kinds, tab_states, tab_titles, toast_count,
    };
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use gpui::{AppContext as _, Entity, TestAppContext, VisualTestContext};
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

    /// A Parquet file of `rows` ids, written in one row group.
    fn ids_parquet(rows: i64) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let ids: ArrayRef = Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()));
        let batch = RecordBatch::try_new(schema.clone(), vec![ids]).expect("the batch must build");

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
    fn an_object_of_a_profile_that_is_not_connected_opens_no_tab(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);

        open(window, &workspace, object_key(uuid::Uuid::new_v4()));

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(toast_count(window), 1);
    }

    const OBJECT_KEY: &str = "2026/cities.parquet";

    /// Opens the object `key` of `profile_id` the way the workspace does, and
    /// returns the document so the test can answer its prompt.
    fn open_object_tab(
        window: &mut VisualTestContext,
        workspace: &Entity<Workspace>,
        profile_id: uuid::Uuid,
        key: &str,
    ) -> Entity<ParquetDocument> {
        let key = key.to_string();

        let document = window.update(|_, cx| {
            let app_state = workspace.read(cx).app_state.clone();
            let connection = app_state
                .read(cx)
                .connections()
                .get(&profile_id)
                .map(|connected| connected.connection.clone())
                .expect("the profile is connected");

            let document = cx.new(|cx| {
                ParquetDocument::open_object(
                    app_state,
                    profile_id,
                    connection,
                    BUCKET.to_string(),
                    key,
                    cx,
                )
            });
            let pane = ParquetDocument::into_pane(document.clone(), cx);
            let tab_manager = workspace.read(cx).tab_manager.clone();

            tab_manager.update(cx, |manager, cx| {
                manager.open(Tab::Pane(Box::new(pane)), cx);
            });

            document
        });
        window.run_until_parked();

        document
    }

    fn prompt_message(
        window: &mut VisualTestContext,
        document: &Entity<ParquetDocument>,
    ) -> Option<String> {
        window.update(|_, cx| document.read(cx).download_prompt_message())
    }

    fn confirm_download(window: &mut VisualTestContext, document: &Entity<ParquetDocument>) {
        window.update(|_, cx| document.update(cx, |document, cx| document.confirm_download(cx)));
        window.run_until_parked();
    }

    fn reload(window: &mut VisualTestContext, document: &Entity<ParquetDocument>) {
        window.update(|_, cx| document.update(cx, |document, cx| document.reload(cx)));
        window.run_until_parked();
    }

    fn row_counts(
        window: &mut VisualTestContext,
        document: &Entity<ParquetDocument>,
    ) -> Option<(u64, u64)> {
        window.update(|_, cx| document.read(cx).row_counts())
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
    fn object_without_range_reads_asks_before_download(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = cities_parquet();
        let store = ObjectStoreFake::with_objects(&[(OBJECT_KEY, &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store.clone());

        open(window, &workspace, object_key(profile_id));

        assert_eq!(tab_titles(window, &workspace), ["cities.parquet"]);
        assert_eq!(tab_states(window, &workspace), [DocumentState::Clean]);

        let document = open_object_tab(window, &workspace, profile_id, OBJECT_KEY);
        let message = prompt_message(window, &document).expect("the prompt is open");
        let size = dbflux_components::components::column_facts::format_bytes(bytes.len() as u64);

        assert!(message.contains("cities.parquet"), "{message}");
        assert!(message.contains(&size), "{message} names {size}");
        assert_eq!(store.full_reads(), 0, "nothing is read before confirming");
        assert_eq!(row_counts(window, &document), None);
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn confirming_downloads_once_and_opens_the_table(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = ids_parquet(1200);
        let store = ObjectStoreFake::with_objects(&[(OBJECT_KEY, &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store.clone());

        let document = open_object_tab(window, &workspace, profile_id, OBJECT_KEY);
        confirm_download(window, &document);

        assert_eq!(store.full_reads(), 1);
        assert_eq!(prompt_message(window, &document), None);
        assert_eq!(row_counts(window, &document), Some((500, 1200)));
        assert_eq!(
            window.update(|_, cx| document.read(cx).object_reads()),
            Some(ObjectReads::Downloaded)
        );

        window.update(|_, cx| document.update(cx, |document, cx| document.load_more(cx)));
        window.run_until_parked();

        assert_eq!(row_counts(window, &document), Some((1000, 1200)));
        assert_eq!(store.full_reads(), 1, "paging reads the downloaded copy");
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn cancelling_opens_nothing_and_reports_nothing(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = cities_parquet();
        let store = ObjectStoreFake::with_objects(&[(OBJECT_KEY, &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store.clone());

        let document = open_object_tab(window, &workspace, profile_id, OBJECT_KEY);
        assert!(prompt_message(window, &document).is_some());

        window.update(|_, cx| {
            document.update(cx, |document, cx| document.dismiss_download_prompt(cx))
        });
        window.run_until_parked();

        assert!(tab_titles(window, &workspace).is_empty());
        assert_eq!(store.full_reads(), 0);
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn object_with_range_reads_opens_without_asking(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = cities_parquet();
        let store = ObjectStoreFake::with_ranged_objects(&[(OBJECT_KEY, &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store);

        let document = open_object_tab(window, &workspace, profile_id, OBJECT_KEY);

        assert_eq!(prompt_message(window, &document), None);
        assert_eq!(row_counts(window, &document), Some((2, 2)));
        assert_eq!(
            window.update(|_, cx| document.read(cx).object_reads()),
            Some(ObjectReads::ByRange)
        );
        assert_eq!(toast_count(window), 0);
    }

    #[gpui::test]
    fn reload_of_a_downloaded_object_reports_unchanged_or_asks_again(cx: &mut TestAppContext) {
        let (workspace, window) = new_workspace(cx);
        let bytes = cities_parquet();
        let store = ObjectStoreFake::with_objects(&[(OBJECT_KEY, &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store.clone());

        let document = open_object_tab(window, &workspace, profile_id, OBJECT_KEY);
        confirm_download(window, &document);
        assert_eq!(row_counts(window, &document), Some((2, 2)));

        reload(window, &document);

        assert_eq!(prompt_message(window, &document), None);
        assert_eq!(
            store.full_reads(),
            1,
            "an unchanged object is not downloaded again"
        );
        assert_eq!(row_counts(window, &document), Some((2, 2)));
        assert_eq!(toast_count(window), 1);
        let unchanged = last_toast_title(window);
        assert!(
            unchanged
                .as_deref()
                .is_some_and(|title| title.contains("cities.parquet")),
            "{unchanged:?}"
        );

        let changed = ids_parquet(3);
        store.replace(OBJECT_KEY, &changed);

        reload(window, &document);

        let message = prompt_message(window, &document).expect("a changed object asks again");
        let size = dbflux_components::components::column_facts::format_bytes(changed.len() as u64);
        assert!(message.contains(&size), "{message} names {size}");
        assert_eq!(store.full_reads(), 1);
        assert_eq!(
            row_counts(window, &document),
            Some((2, 2)),
            "the rows stay while the prompt is open"
        );

        confirm_download(window, &document);

        assert_eq!(store.full_reads(), 2);
        assert_eq!(row_counts(window, &document), Some((3, 3)));
        assert_eq!(toast_count(window), 1);
    }

    #[gpui::test]
    fn a_download_over_the_memory_limit_reads_a_temporary_file_removed_after_use(
        cx: &mut TestAppContext,
    ) {
        let (workspace, window) = new_workspace(cx);
        let bytes = cities_parquet();
        let store = ObjectStoreFake::with_objects(&[(OBJECT_KEY, &bytes)]);
        let profile_id = connect_object_store(window, &workspace, store.clone());

        let connection = window.update(|_, cx| {
            workspace
                .read(cx)
                .app_state
                .read(cx)
                .connections()
                .get(&profile_id)
                .map(|connected| connected.connection.clone())
                .expect("the profile is connected")
        });

        let (in_memory, _version) =
            download_whole_object(connection.as_ref(), BUCKET, OBJECT_KEY, u64::MAX)
                .expect("the download succeeds");
        assert!(matches!(in_memory, LocationSource::Memory(_)));

        let (on_disk, _version) = download_whole_object(connection.as_ref(), BUCKET, OBJECT_KEY, 0)
            .expect("the download succeeds");
        let LocationSource::Downloaded(file) = &on_disk else {
            panic!("an object over the limit is downloaded to a file");
        };
        let path = file.path().to_path_buf();

        assert!(path.exists());
        assert_eq!(std::fs::read(&path).expect("the copy reads"), bytes);
        assert_eq!(store.full_reads(), 2);

        drop(on_disk);

        assert!(!path.exists(), "the temporary copy is removed");
    }
}

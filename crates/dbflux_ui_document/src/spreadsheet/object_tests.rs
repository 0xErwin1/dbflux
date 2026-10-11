//! Spreadsheets in object storage: opening by range or after a download
//! prompt, saving with a version check and an audit record, and quitting.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dbflux_audit::query::AuditQueryFilter;
use dbflux_byte_source::MemorySource;
use dbflux_components::components::data_table::model::CellValue as TableValue;
use dbflux_core::{
    BucketCreateOptions, BucketCreateOutcome, BucketDetails, BucketInfo, BucketSizeEstimate,
    Connection, DatabaseCategory, DbError, DbKind, DeletePrefixOutcome, DriverMetadata,
    DriverMetadataBuilder, ObjectListingPage, ObjectMetadata, ObjectStoreConnection,
    ObjectVersionSummary, PresignMethod, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
};
use dbflux_spreadsheet::CellValue;
use dbflux_test_support::fake_driver::FakeDriver;
use dbflux_ui_base::AppStateEntity;
use dbflux_ui_base::keyboard_coverage::{Coverage, FrameCapture};
use dbflux_ui_base::toast::ToastGlobal;
use gpui::{AppContext as _, Entity, Subscription, TestAppContext, VisualTestContext};

use super::document::SpreadsheetDocument;
use super::tests::{fixture, table_state};
use crate::file_source::ObjectReads;
use crate::handle::DocumentEvent;
use crate::keyboard_coverage::SPREADSHEET;
use crate::keyboard_test_support::{connected_app_state, host_document, init_keyboard_runtime};
use crate::pane::QuitDisposition;

const BUCKET: &str = "reports";
const KEY: &str = "2026/budget.ods";

// -- The object store ---------------------------------------------------------

struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
}

/// An object store that keeps one object in memory, gives it a new etag on
/// every write, and counts whole reads and uploads. It reads a byte range
/// only when it declares ranged reads; otherwise a range read costs a whole
/// read, as the trait's default does.
struct FakeStore {
    object: Mutex<Option<StoredObject>>,
    range_reads: bool,
    generation: AtomicUsize,
    full_reads: AtomicUsize,
    uploads: AtomicUsize,
}

impl FakeStore {
    fn replace(&self, bytes: &[u8]) {
        let etag = format!("etag-{}", self.generation.fetch_add(1, Ordering::SeqCst));

        *self.object.lock().expect("the object") = Some(StoredObject {
            bytes: bytes.to_vec(),
            etag,
        });
    }

    fn bytes(&self) -> Vec<u8> {
        self.with_object(BUCKET, KEY, |object| object.bytes.clone())
            .expect("the object exists")
    }

    fn full_reads(&self) -> usize {
        self.full_reads.load(Ordering::SeqCst)
    }

    fn uploads(&self) -> usize {
        self.uploads.load(Ordering::SeqCst)
    }

    #[allow(clippy::result_large_err)]
    #[expect(
        clippy::unwrap_in_result,
        reason = "test fixture: the existing panic-on-poison policy is retained for this mutex"
    )]
    fn with_object<T>(
        &self,
        bucket: &str,
        key: &str,
        read: impl FnOnce(&StoredObject) -> T,
    ) -> Result<T, DbError> {
        if bucket != BUCKET || key != KEY {
            return Err(DbError::object_not_found(format!(
                "NoSuchKey: {bucket}/{key}"
            )));
        }

        self.object
            .lock()
            .expect("the object")
            .as_ref()
            .map(read)
            .ok_or_else(|| DbError::object_not_found(format!("NoSuchKey: {bucket}/{key}")))
    }
}

#[allow(clippy::result_large_err)]
fn not_used<T>() -> Result<T, DbError> {
    Err(DbError::NotSupported("not used by these tests".to_string()))
}

impl ObjectStoreConnection for FakeStore {
    fn list_buckets(&self) -> Result<Vec<BucketInfo>, DbError> {
        not_used()
    }

    fn list_objects(
        &self,
        _bucket: &str,
        _prefix: &str,
        _continuation_token: Option<&str>,
    ) -> Result<ObjectListingPage, DbError> {
        not_used()
    }

    fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata, DbError> {
        self.with_object(bucket, key, |object| ObjectMetadata {
            key: key.to_string(),
            size_bytes: u64::try_from(object.bytes.len()).unwrap_or(u64::MAX),
            content_type: None,
            last_modified: None,
            etag: Some(object.etag.clone()),
            storage_class: None,
            encryption: None,
            version_count: None,
        })
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, DbError> {
        self.full_reads.fetch_add(1, Ordering::SeqCst);

        self.with_object(bucket, key, |object| object.bytes.clone())
    }

    fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, DbError> {
        let bytes = if self.range_reads {
            self.with_object(bucket, key, |object| object.bytes.clone())?
        } else {
            self.get_object(bucket, key)?
        };

        let start = usize::try_from(range.start).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end).unwrap_or(usize::MAX);

        bytes
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| DbError::query_failed("InvalidRange"))
    }

    fn supports_range_reads(&self) -> bool {
        self.range_reads
    }

    fn download_object(&self, bucket: &str, key: &str, dest: &Path) -> Result<u64, DbError> {
        let bytes = self.get_object(bucket, key)?;

        std::fs::write(dest, &bytes).map_err(|error| DbError::query_failed(error.to_string()))?;

        Ok(u64::try_from(bytes.len()).unwrap_or(u64::MAX))
    }

    fn put_object(
        &self,
        _bucket: &str,
        _key: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
    ) -> Result<(), DbError> {
        not_used()
    }

    fn upload_object(
        &self,
        bucket: &str,
        key: &str,
        source_path: &Path,
        _content_type: Option<&str>,
    ) -> Result<(), DbError> {
        self.with_object(bucket, key, |_| ())?;
        self.uploads.fetch_add(1, Ordering::SeqCst);

        let bytes = std::fs::read(source_path)
            .map_err(|error| DbError::query_failed(format!("cannot read the upload: {error}")))?;

        self.replace(&bytes);

        Ok(())
    }

    fn delete_object(&self, _bucket: &str, _key: &str) -> Result<(), DbError> {
        not_used()
    }

    fn delete_prefix(&self, _bucket: &str, _prefix: &str) -> Result<DeletePrefixOutcome, DbError> {
        not_used()
    }

    fn copy_object(&self, _bucket: &str, _src_key: &str, _dest_key: &str) -> Result<(), DbError> {
        not_used()
    }

    fn presign(
        &self,
        _bucket: &str,
        _key: &str,
        _method: PresignMethod,
        _expiry: std::time::Duration,
    ) -> Result<String, DbError> {
        not_used()
    }

    fn get_bucket_details(&self, _bucket: &str) -> Result<BucketDetails, DbError> {
        not_used()
    }

    fn estimate_bucket_size(
        &self,
        _bucket: &str,
        _object_cap: u64,
    ) -> Result<BucketSizeEstimate, DbError> {
        not_used()
    }

    fn list_object_versions(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<Vec<ObjectVersionSummary>, DbError> {
        not_used()
    }

    fn create_bucket(
        &self,
        _bucket: &str,
        _options: BucketCreateOptions,
    ) -> Result<BucketCreateOutcome, DbError> {
        not_used()
    }

    fn delete_bucket(&self, _bucket: &str) -> Result<(), DbError> {
        not_used()
    }
}

/// A connection whose only working part is its object store.
struct FakeConnection {
    store: FakeStore,
}

impl FakeConnection {
    fn new(bytes: &[u8], range_reads: bool) -> Arc<Self> {
        let store = FakeStore {
            object: Mutex::new(None),
            range_reads,
            generation: AtomicUsize::new(0),
            full_reads: AtomicUsize::new(0),
            uploads: AtomicUsize::new(0),
        };
        store.replace(bytes);

        Arc::new(Self { store })
    }
}

impl Connection for FakeConnection {
    fn metadata(&self) -> &DriverMetadata {
        static METADATA: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();

        METADATA.get_or_init(|| {
            DriverMetadataBuilder::new(
                "spreadsheet-storage-test",
                "Spreadsheet Storage Test",
                DatabaseCategory::Relational,
                QueryLanguage::Sql,
            )
            .build()
        })
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, _request: &QueryRequest) -> Result<QueryResult, DbError> {
        not_used()
    }

    fn cancel(&self, _handle: &QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        not_used()
    }

    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn object_store_api(&self) -> Option<&dyn ObjectStoreConnection> {
        Some(&self.store)
    }
}

// -- Helpers ------------------------------------------------------------------

/// Opens the `in.ods` fixture stored as an object of a store that reads by
/// range when `range_reads` is set. Returns the document, its connection and
/// the application state whose audit log records its saves.
fn open_object(
    cx: &mut TestAppContext,
    range_reads: bool,
) -> (
    Entity<SpreadsheetDocument>,
    Arc<FakeConnection>,
    Entity<AppStateEntity>,
    &mut VisualTestContext,
) {
    init_keyboard_runtime(cx);

    let connection = FakeConnection::new(&fixture("in.ods"), range_reads);
    let driver = FakeDriver::new(DbKind::SQLite);
    let (app_state, profile_id) = connected_app_state(cx, &driver, "objects");

    cx.update(|cx| {
        app_state.update(cx, |state, _| {
            state
                .connections_mut()
                .get_mut(&profile_id)
                .expect("the profile is connected")
                .connection = connection.clone();
        });
    });

    let (host, window) = host_document(
        cx,
        {
            let app_state = app_state.clone();
            let connection = connection.clone();

            move |_window, cx| {
                cx.new(|cx| {
                    SpreadsheetDocument::open_object(
                        app_state,
                        profile_id,
                        connection,
                        BUCKET.to_string(),
                        KEY.to_string(),
                        cx,
                    )
                })
            }
        },
        |document, _cx| document.active_context(),
        |document, command, window, cx| document.dispatch_command(command, window, cx),
    );

    let document = window.update(|_, cx| host.read(cx).document.clone());
    window.run_until_parked();

    (document, connection, app_state, window)
}

fn prompt_message(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Option<String> {
    window.update(|_, cx| document.read(cx).download_prompt_message())
}

fn confirm_download(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.confirm_download(cx)));
    window.run_until_parked();
}

fn sheet_names(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Vec<String> {
    window.update(|_, cx| {
        document
            .read(cx)
            .sheets()
            .iter()
            .map(|sheet| sheet.name.clone())
            .collect()
    })
}

/// Stages `text` for the cell shown at `row`, `column`, as a typed value is.
fn stage(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
    row: usize,
    column: usize,
    text: &str,
) {
    let table_state = table_state(document, window);

    window.update(|_, cx| {
        table_state.update(cx, |state, cx| {
            state.stage_cell_value(row, column, TableValue::text(text));
            cx.notify();
        });
    });
    window.run_until_parked();
}

fn save(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) {
    window.update(|_, cx| document.update(cx, |document, cx| document.save(cx)));
    window.run_until_parked();
}

fn is_dirty(document: &Entity<SpreadsheetDocument>, window: &mut VisualTestContext) -> bool {
    window.update(|_, cx| document.read(cx).is_dirty())
}

fn toast_count(window: &mut VisualTestContext) -> usize {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).toast_count())
}

fn last_toast_title(window: &mut VisualTestContext) -> Option<String> {
    window.update(|_, cx| cx.global::<ToastGlobal>().host.read(cx).last_toast_title())
}

/// Records every event the document emits from now on.
fn record_events(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> (Rc<RefCell<Vec<DocumentEvent>>>, Subscription) {
    let events: Rc<RefCell<Vec<DocumentEvent>>> = Rc::default();

    let subscription = window.update(|_, cx| {
        let events = events.clone();

        cx.subscribe(document, move |_, event: &DocumentEvent, _| {
            events.borrow_mut().push(event.clone());
        })
    });

    (events, subscription)
}

/// The keys the opener of `document` is told were saved, from now on.
fn record_saved_keys(
    document: &Entity<SpreadsheetDocument>,
    window: &mut VisualTestContext,
) -> Rc<RefCell<Vec<String>>> {
    let saved_keys: Rc<RefCell<Vec<String>>> = Rc::default();

    window.update(|_, cx| {
        document.update(cx, |document, _cx| {
            let saved_keys = saved_keys.clone();

            document.set_on_object_saved(Rc::new(move |key: &str, _cx: &mut gpui::App| {
                saved_keys.borrow_mut().push(key.to_string());
            }));
        })
    });

    saved_keys
}

fn save_audit_events(
    app_state: &Entity<AppStateEntity>,
    window: &mut VisualTestContext,
    action: &str,
) -> Vec<dbflux_storage::repositories::audit::AuditEventDto> {
    window.update(|_, cx| {
        app_state
            .read(cx)
            .audit_service()
            .query_extended(&AuditQueryFilter {
                action: Some(action.to_string()),
                ..AuditQueryFilter::default()
            })
            .expect("the audit log is readable")
    })
}

/// The value of the cell at `row`, `column` of the first sheet of the
/// object as it is stored now.
fn stored_value(connection: &FakeConnection, row: usize, column: usize) -> CellValue {
    let mut workbook = dbflux_spreadsheet::open(MemorySource::new(connection.store.bytes()))
        .expect("the stored workbook opens");
    let grid = workbook.read_sheet(0).expect("the stored sheet reads");

    grid.cell(row, column)
        .map(|cell| cell.value.clone())
        .unwrap_or(CellValue::Empty)
}

// -- Opening ------------------------------------------------------------------

#[gpui::test]
fn object_with_range_reads_opens_without_asking(cx: &mut TestAppContext) {
    let (document, connection, _app_state, window) = open_object(cx, true);

    assert_eq!(prompt_message(&document, window), None);
    assert_eq!(sheet_names(&document, window), ["Data", "Other"]);
    assert_eq!(
        window.update(|_, cx| document.read(cx).object_reads()),
        Some(ObjectReads::ByRange)
    );
    assert_eq!(
        connection.store.full_reads(),
        0,
        "the package is read by range"
    );
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn object_without_range_reads_asks_before_download(cx: &mut TestAppContext) {
    let (document, connection, _app_state, window) = open_object(cx, false);

    let message = prompt_message(&document, window).expect("the prompt is open");
    let size =
        dbflux_components::components::column_facts::format_bytes(fixture("in.ods").len() as u64);

    assert!(message.contains("budget.ods"), "{message}");
    assert!(message.contains(&size), "{message} names {size}");
    assert!(sheet_names(&document, window).is_empty());
    assert_eq!(connection.store.full_reads(), 0, "nothing is read first");

    confirm_download(&document, window);

    assert_eq!(prompt_message(&document, window), None);
    assert_eq!(sheet_names(&document, window), ["Data", "Other"]);
    assert_eq!(
        window.update(|_, cx| document.read(cx).object_reads()),
        Some(ObjectReads::Downloaded)
    );

    window.update(|window, cx| {
        document.update(cx, |document, cx| document.select_sheet(1, window, cx))
    });
    window.run_until_parked();

    assert_eq!(
        window.update(|_, cx| document.read(cx).active_sheet()),
        Some(1)
    );
    assert_eq!(
        connection.store.full_reads(),
        1,
        "the object is downloaded once and every sheet is read from the copy"
    );
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn declining_the_download_opens_nothing(cx: &mut TestAppContext) {
    let (document, connection, _app_state, window) = open_object(cx, false);
    let (events, _subscription) = record_events(&document, window);

    assert!(prompt_message(&document, window).is_some());

    window.update(|_, cx| document.update(cx, |document, cx| document.dismiss_download_prompt(cx)));
    window.run_until_parked();

    assert_eq!(prompt_message(&document, window), None);
    assert!(
        events
            .borrow()
            .iter()
            .any(|event| matches!(event, DocumentEvent::RequestClose)),
        "declining the download closes the tab"
    );
    assert!(sheet_names(&document, window).is_empty());
    assert_eq!(connection.store.full_reads(), 0);
    assert_eq!(toast_count(window), 0);
}

#[gpui::test]
fn the_download_prompt_is_covered_and_escape_declines_it(cx: &mut TestAppContext) {
    let (document, _connection, _app_state, window) = open_object(cx, false);
    let (events, _subscription) = record_events(&document, window);

    assert!(prompt_message(&document, window).is_some());

    let capture = FrameCapture::observe(window);
    let checked: Vec<String> = Coverage::new(SPREADSHEET)
        .assert_covered(&capture.frame(window))
        .into_iter()
        .map(|id| id.to_string())
        .collect();

    for control in [
        "spreadsheet-download-confirm",
        "spreadsheet-download-cancel",
    ] {
        assert!(checked.iter().any(|id| id == control), "{checked:?}");
    }

    window.simulate_keystrokes("escape");
    window.run_until_parked();

    assert_eq!(prompt_message(&document, window), None);
    assert!(
        events
            .borrow()
            .iter()
            .any(|event| matches!(event, DocumentEvent::RequestClose))
    );
}

// -- Saving -------------------------------------------------------------------

#[gpui::test]
fn saved_object_is_uploaded_and_audited(cx: &mut TestAppContext) {
    for range_reads in [true, false] {
        let (document, connection, app_state, window) = open_object(cx, range_reads);

        if !range_reads {
            confirm_download(&document, window);
        }

        let saved_keys = record_saved_keys(&document, window);
        let (events, _subscription) = record_events(&document, window);
        let full_reads_before = connection.store.full_reads();

        stage(&document, window, 0, 0, "Item");
        save(&document, window);

        let saved: Vec<bool> = events
            .borrow()
            .iter()
            .filter_map(|event| match event {
                DocumentEvent::SaveFinished { succeeded } => Some(*succeeded),
                _ => None,
            })
            .collect();

        assert_eq!(saved, [true], "range reads: {range_reads}");
        assert_eq!(connection.store.uploads(), 1);
        assert!(!is_dirty(&document, window));
        assert_eq!(saved_keys.borrow().as_slice(), [KEY]);
        assert_eq!(toast_count(window), 0, "range reads: {range_reads}");

        if !range_reads {
            assert_eq!(
                connection.store.full_reads(),
                full_reads_before + 1,
                "the patch reads the downloaded copy, and only the reopen downloads"
            );
        }

        assert_eq!(
            stored_value(&connection, 0, 0),
            CellValue::Text("Item".into())
        );

        let audited = save_audit_events(&app_state, window, "object_edit_save");
        assert_eq!(audited.len(), 1);

        let event = &audited[0];
        assert_eq!(event.category.as_deref(), Some("object_storage"));
        assert_eq!(event.outcome.as_deref(), Some("success"));
        assert_eq!(event.object_type.as_deref(), Some("object"));
        assert_eq!(event.object_id.as_deref(), Some("reports/2026/budget.ods"));
    }
}

#[gpui::test]
fn saving_an_object_changed_since_opening_is_refused(cx: &mut TestAppContext) {
    let (document, connection, app_state, window) = open_object(cx, true);
    let saved_keys = record_saved_keys(&document, window);

    stage(&document, window, 0, 0, "Item");

    let foreign = fixture("in.ods");
    connection.store.replace(&foreign);

    save(&document, window);

    assert_eq!(connection.store.uploads(), 0);
    assert_eq!(connection.store.bytes(), foreign);
    assert!(is_dirty(&document, window), "the edits are kept");
    assert!(saved_keys.borrow().is_empty());
    assert_eq!(toast_count(window), 1);
    assert_eq!(
        last_toast_title(window),
        Some(crate::labels::spreadsheet_save_failed_message("budget.ods"))
    );
    assert!(save_audit_events(&app_state, window, "object_edit_save").is_empty());
    assert_eq!(
        save_audit_events(&app_state, window, "object_edit_save_failed").len(),
        1
    );
}

// -- Quitting -----------------------------------------------------------------

#[gpui::test]
fn quit_with_an_edited_object_asks(cx: &mut TestAppContext) {
    let (document, connection, _app_state, window) = open_object(cx, true);
    let pane = window.update(|_, cx| SpreadsheetDocument::into_pane(document.clone(), cx));

    assert_eq!(
        window.update(|_, cx| pane.quit_disposition(cx)),
        QuitDisposition::Clean
    );

    stage(&document, window, 0, 0, "Item");

    assert_eq!(
        window.update(|_, cx| pane.quit_disposition(cx)),
        QuitDisposition::NeedsDecision
    );

    let outstanding = window.update(|_, cx| pane.flush_for_shutdown(cx));
    window.run_until_parked();

    assert!(!outstanding, "nothing is uploaded while quitting");
    assert_eq!(connection.store.uploads(), 0);
    assert_eq!(
        window.update(|_, cx| document.read(cx).dropped_at_shutdown.clone()),
        ["reports/2026/budget.ods"]
    );
}

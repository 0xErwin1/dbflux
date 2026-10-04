use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dbflux_core::{
    BucketCreateOptions, BucketCreateOutcome, BucketDetails, BucketInfo, BucketSizeEstimate,
    Connection, DatabaseCategory, DbError, DbKind, DeletePrefixOutcome, DriverMetadata,
    DriverMetadataBuilder, ObjectListingPage, ObjectMetadata, ObjectStoreConnection,
    ObjectVersionSummary, PresignMethod, QueryHandle, QueryLanguage, QueryRequest, QueryResult,
    SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
};
use dbflux_delimited::{
    AppendedColumn, ByteSource, Dialect, EditSet, Encoding, MemorySource, PagedReader,
    ReaderOptions, Record, Replacement, WriteError,
};

use super::save::{SaveRequest, save_staging_objects_in, verify_version, write_staged};
use super::{SaveOutcome, save_edited};
use crate::file_source::{
    FileLocation, SourceVersion, StorageError, has_changed_since, open_source, read_version,
};

pub(super) const BUCKET: &str = "reports";
pub(super) const KEY: &str = "2026/cities.csv";

const CITIES: &[u8] = b"name,city\r\nAna,Lima\r\n\"Bo, Jr\",Quito\nCy,Rome";

fn window() -> NonZeroU64 {
    NonZeroU64::new(8).expect("a non-zero window")
}

fn dialect(encoding_label: &str) -> Dialect {
    Dialect {
        delimiter: b',',
        quote: Some(b'"'),
        has_header: true,
        encoding: Encoding::for_label(encoding_label.as_bytes()).expect("a known encoding label"),
    }
}

fn utf8() -> Dialect {
    dialect("utf-8")
}

/// Reads every data record of `location` with the ranges the reader reports.
fn records_of(location: &FileLocation, dialect: Dialect) -> (Vec<Record>, u64) {
    let (source, _version) = open_source(location).expect("the source opens");

    let options = ReaderOptions {
        page_size: NonZeroUsize::new(100).expect("a non-zero page size"),
        window_size: window(),
    };

    let mut reader = PagedReader::open(source, dialect, options).expect("the reader opens");
    let page = reader.read_page(0).expect("the first page reads");

    (page.records, reader.source_length())
}

/// An edit set that replaces the data record at `index` with `fields`.
fn replace_record(
    location: &FileLocation,
    dialect: Dialect,
    index: usize,
    fields: &[&str],
) -> EditSet {
    let (records, source_length) = records_of(location, dialect);
    let record = records.get(index).expect("the record exists");

    let mut edits = EditSet::new(source_length);

    edits.replacements.push(Replacement {
        byte_range: record.byte_range.clone(),
        fields: fields.iter().map(|field| (*field).to_string()).collect(),
    });

    edits
}

/// The version of a save whose new version could be read.
fn saved_version(outcome: SaveOutcome) -> SourceVersion {
    match outcome {
        SaveOutcome::Saved(version) => version,
        SaveOutcome::SavedVersionUnknown(error) => {
            panic!("the new version must be readable: {error}")
        }
    }
}

/// Saves an object with its temporary file created inside `staging`, so the
/// test can see whether one was left behind.
fn save_object_staging_in(
    staging: &TestDirectory,
    location: &FileLocation,
    captured: &SourceVersion,
    dialect: &Dialect,
    edits: &EditSet,
) -> Result<SaveOutcome, StorageError> {
    let request = SaveRequest {
        captured,
        dialect,
        edits,
        window_size: window(),
    };

    save_staging_objects_in(location, &request, &staging.path)
}

/// A private directory that is removed when the test ends.
pub(super) struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    pub(super) fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("dbflux-delimited-{name}-{}", uuid::Uuid::new_v4()));

        std::fs::create_dir_all(&path).expect("the test directory must be creatable");

        Self { path }
    }

    /// Writes `bytes` to a file named `name` and returns its location.
    pub(super) fn file(&self, name: &str, bytes: &[u8]) -> (PathBuf, FileLocation) {
        let path = self.path.join(name);
        std::fs::write(&path, bytes).expect("the test file must be writable");

        let location = FileLocation::Local { path: path.clone() };

        (path, location)
    }

    fn entry_names(&self) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(&self.path)
            .expect("the test directory must be readable")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();

        names.sort();
        names
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).ok();
    }
}

struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
    content_type: Option<String>,
}

/// An object store that keeps its objects in memory, counts `head_object`
/// calls, remembers every file it was asked to upload, and fails reads or
/// uploads with a given message when told to.
///
/// Like a store that answers an unsatisfiable range with an error, it refuses
/// a range read that reaches past the end of the object instead of clamping
/// it.
#[derive(Default)]
pub(super) struct FakeObjectStore {
    objects: Mutex<HashMap<(String, String), StoredObject>>,
    head_calls: AtomicUsize,
    generation: AtomicUsize,
    uploaded_from: Mutex<Vec<PathBuf>>,
    read_failure: Mutex<Option<String>>,
    upload_failure: Mutex<Option<String>>,
    head_failure_after_upload: Mutex<Option<String>>,
    one_head_failure_after_upload: Mutex<Option<String>>,
    range_reads: AtomicUsize,
    change_etag_at_range_read: AtomicUsize,
    omit_identity: AtomicBool,
    uploaded_modes: Mutex<Vec<u32>>,
}

impl FakeObjectStore {
    fn next_etag(&self) -> String {
        format!("etag-{}", self.generation.fetch_add(1, Ordering::SeqCst))
    }

    pub(super) fn store(&self, bytes: &[u8], content_type: Option<&str>) {
        let object = StoredObject {
            bytes: bytes.to_vec(),
            etag: self.next_etag(),
            content_type: content_type.map(str::to_string),
        };

        self.objects
            .lock()
            .expect("the object map")
            .insert((BUCKET.to_string(), KEY.to_string()), object);
    }

    /// Gives the object a new etag without changing its bytes, which is what
    /// another writer storing the same number of bytes looks like.
    pub(super) fn change_etag(&self) {
        let etag = self.next_etag();

        if let Some(object) = self
            .objects
            .lock()
            .expect("the object map")
            .get_mut(&(BUCKET.to_string(), KEY.to_string()))
        {
            object.etag = etag;
        }
    }

    pub(super) fn bytes(&self) -> Vec<u8> {
        self.with_object(BUCKET, KEY, |object| object.bytes.clone())
            .expect("the object exists")
    }

    fn content_type(&self) -> Option<String> {
        self.with_object(BUCKET, KEY, |object| object.content_type.clone())
            .expect("the object exists")
    }

    pub(super) fn head_calls(&self) -> usize {
        self.head_calls.load(Ordering::SeqCst)
    }

    fn uploaded_from(&self) -> Vec<PathBuf> {
        self.uploaded_from.lock().expect("the upload log").clone()
    }

    pub(super) fn fail_reads_with(&self, message: &str) {
        *self.read_failure.lock().expect("the read failure") = Some(message.to_string());
    }

    pub(super) fn stop_failing_reads(&self) {
        *self.read_failure.lock().expect("the read failure") = None;
    }

    /// How many range reads the store answered.
    pub(super) fn range_reads(&self) -> usize {
        self.range_reads.load(Ordering::SeqCst)
    }

    pub(super) fn fail_uploads_with(&self, message: &str) {
        *self.upload_failure.lock().expect("the upload failure") = Some(message.to_string());
    }

    /// Makes every `head_object` fail once an upload has happened.
    pub(super) fn fail_heads_after_an_upload_with(&self, message: &str) {
        *self
            .head_failure_after_upload
            .lock()
            .expect("the head failure") = Some(message.to_string());
    }

    /// Makes the first `head_object` after an upload fail, and only that
    /// one, which is a store that cannot report the version of an object it
    /// just stored and answers again right after.
    pub(super) fn fail_one_head_after_an_upload_with(&self, message: &str) {
        *self
            .one_head_failure_after_upload
            .lock()
            .expect("the head failure") = Some(message.to_string());
    }

    /// Gives the object a new etag when the range read with this one-based
    /// number arrives, which is another writer replacing the object while it
    /// is being read.
    fn change_etag_at_range_read(&self, number: usize) {
        self.change_etag_at_range_read
            .store(number, Ordering::SeqCst);
    }

    /// Reports neither an etag nor a modification time from `head_object`.
    pub(super) fn omit_identity(&self) {
        self.omit_identity.store(true, Ordering::SeqCst);
    }

    fn uploaded_modes(&self) -> Vec<u32> {
        self.uploaded_modes.lock().expect("the mode log").clone()
    }

    #[allow(clippy::result_large_err)]
    fn with_object<T>(
        &self,
        bucket: &str,
        key: &str,
        read: impl FnOnce(&StoredObject) -> T,
    ) -> Result<T, DbError> {
        self.objects
            .lock()
            .expect("the object map")
            .get(&(bucket.to_string(), key.to_string()))
            .map(read)
            .ok_or_else(|| DbError::object_not_found(format!("NoSuchKey: {bucket}/{key}")))
    }
}

#[allow(clippy::result_large_err)]
fn not_used<T>() -> Result<T, DbError> {
    Err(DbError::NotSupported("not used by these tests".to_string()))
}

impl ObjectStoreConnection for FakeObjectStore {
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
        self.head_calls.fetch_add(1, Ordering::SeqCst);

        let failure = self
            .head_failure_after_upload
            .lock()
            .expect("the head failure")
            .clone();

        if let Some(message) = failure
            && !self.uploaded_from().is_empty()
        {
            return Err(DbError::query_failed(message));
        }

        if !self.uploaded_from().is_empty() {
            let one_failure = self
                .one_head_failure_after_upload
                .lock()
                .expect("the head failure")
                .take();

            if let Some(message) = one_failure {
                return Err(DbError::query_failed(message));
            }
        }

        let omit_identity = self.omit_identity.load(Ordering::SeqCst);

        self.with_object(bucket, key, |object| ObjectMetadata {
            key: key.to_string(),
            size_bytes: u64::try_from(object.bytes.len()).unwrap_or(u64::MAX),
            content_type: object.content_type.clone(),
            last_modified: None,
            etag: (!omit_identity).then(|| object.etag.clone()),
            storage_class: None,
            encryption: None,
            version_count: None,
        })
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Vec<u8>, DbError> {
        if let Some(message) = self.read_failure.lock().expect("the read failure").clone() {
            return Err(DbError::query_failed(message));
        }

        self.with_object(bucket, key, |object| object.bytes.clone())
    }

    fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, DbError> {
        if let Some(message) = self.read_failure.lock().expect("the read failure").clone() {
            return Err(DbError::query_failed(message));
        }

        let read_number = self.range_reads.fetch_add(1, Ordering::SeqCst) + 1;

        if read_number == self.change_etag_at_range_read.load(Ordering::SeqCst) {
            self.change_etag();
        }

        let start = usize::try_from(range.start).unwrap_or(usize::MAX);
        let end = usize::try_from(range.end).unwrap_or(usize::MAX);

        self.with_object(bucket, key, |object| {
            object.bytes.get(start..end).map(<[u8]>::to_vec)
        })?
        .ok_or_else(|| {
            DbError::query_failed(format!(
                "InvalidRange: bytes {}..{} are not inside the object",
                range.start, range.end
            ))
        })
    }

    fn download_object(&self, _bucket: &str, _key: &str, _dest: &Path) -> Result<u64, DbError> {
        not_used()
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
        content_type: Option<&str>,
    ) -> Result<(), DbError> {
        self.uploaded_from
            .lock()
            .expect("the upload log")
            .push(source_path.to_path_buf());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mode = std::fs::metadata(source_path)
                .map(|metadata| metadata.permissions().mode() & 0o777)
                .map_err(|error| {
                    DbError::query_failed(format!("cannot stat the upload: {error}"))
                })?;

            self.uploaded_modes.lock().expect("the mode log").push(mode);
        }

        if let Some(message) = self
            .upload_failure
            .lock()
            .expect("the upload failure")
            .clone()
        {
            return Err(DbError::query_failed(message));
        }

        let bytes = std::fs::read(source_path)
            .map_err(|error| DbError::query_failed(format!("cannot read the upload: {error}")))?;

        let object = StoredObject {
            bytes,
            etag: self.next_etag(),
            content_type: content_type.map(str::to_string),
        };

        self.objects
            .lock()
            .expect("the object map")
            .insert((bucket.to_string(), key.to_string()), object);

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
#[derive(Default)]
pub(super) struct FakeConnection {
    pub(super) store: FakeObjectStore,
}

impl FakeConnection {
    pub(super) fn with_object(bytes: &[u8]) -> Arc<Self> {
        let connection = Self::default();
        connection.store.store(bytes, Some("text/csv"));

        Arc::new(connection)
    }

    fn location(self: &Arc<Self>) -> FileLocation {
        FileLocation::Object {
            connection: self.clone(),
            bucket: BUCKET.to_string(),
            key: KEY.to_string(),
        }
    }
}

impl Connection for FakeConnection {
    fn metadata(&self) -> &DriverMetadata {
        static METADATA: std::sync::OnceLock<DriverMetadata> = std::sync::OnceLock::new();

        METADATA.get_or_init(|| {
            DriverMetadataBuilder::new(
                "delimited-storage-test",
                "Delimited Storage Test",
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

// -- Local files ---------------------------------------------------------------

#[test]
fn a_local_save_without_edits_leaves_the_file_byte_identical() {
    let directory = TestDirectory::new("no-edits");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = EditSet::new(u64::try_from(CITIES.len()).expect("a small file"));

    save_edited(&location, &version, &utf8(), &edits, window()).expect("the save lands");

    assert_eq!(std::fs::read(&path).expect("the file reads"), CITIES);
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[test]
fn a_local_save_with_one_replaced_record_changes_only_that_record() {
    let directory = TestDirectory::new("one-edit");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    save_edited(&location, &version, &utf8(), &edits, window()).expect("the save lands");

    assert_eq!(
        std::fs::read(&path).expect("the file reads"),
        b"name,city\r\nAna,Cusco\r\n\"Bo, Jr\",Quito\nCy,Rome"
    );
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[test]
fn a_local_save_refuses_a_file_whose_content_changed_after_the_version_was_captured() {
    let directory = TestDirectory::new("changed-content");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    let foreign = b"name,city\r\nSomeone,Else\r\n";
    std::fs::write(&path, foreign).expect("the foreign write lands");

    let error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("a changed file must be refused");

    assert!(matches!(error, StorageError::SourceChanged), "{error}");
    assert_eq!(std::fs::read(&path).expect("the file reads"), foreign);
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[test]
fn a_local_save_refuses_a_file_that_changed_while_the_save_was_staging() {
    let directory = TestDirectory::new("changed-while-staging");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    let foreign = b"name,city\r\nSomeone,Else\r\n";

    super::save::while_next_local_save_stages({
        let path = path.clone();
        move || std::fs::write(&path, foreign).expect("the foreign write lands")
    });

    let error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("a file changed while staging must be refused");

    assert!(matches!(error, StorageError::SourceChanged), "{error}");
    assert_eq!(std::fs::read(&path).expect("the file reads"), foreign);
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[test]
fn a_local_save_refuses_a_file_of_the_same_length_with_a_newer_modification_time() {
    let directory = TestDirectory::new("changed-time");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    let foreign = CITIES.to_ascii_uppercase();
    std::fs::write(&path, &foreign).expect("the foreign write lands");

    let later = std::time::SystemTime::now() + std::time::Duration::from_secs(60);
    std::fs::File::options()
        .write(true)
        .open(&path)
        .and_then(|file| file.set_modified(later))
        .expect("the modification time must be settable");

    let error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("a changed file must be refused");

    assert!(matches!(error, StorageError::SourceChanged), "{error}");
    assert_eq!(std::fs::read(&path).expect("the file reads"), foreign);
}

#[test]
fn has_changed_since_follows_the_local_file() {
    let directory = TestDirectory::new("has-changed");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");

    assert!(!has_changed_since(&location, &version).expect("the version reads"));

    std::fs::write(&path, b"name\n").expect("the foreign write lands");

    assert!(has_changed_since(&location, &version).expect("the version reads"));
}

#[test]
fn a_failing_local_write_leaves_the_target_untouched_and_no_temporary_file() {
    let directory = TestDirectory::new("failing-write");
    let (path, location) = directory.file("cities.csv", CITIES);

    let windows_1252 = dialect("windows-1252");
    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, windows_1252, 0, &["Ana", "\u{6771}\u{4eac}"]);

    let error = save_edited(&location, &version, &windows_1252, &edits, window())
        .expect_err("an unencodable character must fail the save");

    assert!(
        matches!(
            error,
            StorageError::Write(WriteError::UnencodableCharacter { .. })
        ),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).expect("the file reads"), CITIES);
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[test]
fn a_local_save_returns_the_version_a_fresh_read_reports() {
    let directory = TestDirectory::new("new-version");
    let (_path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco, Peru"]);

    let saved = saved_version(
        save_edited(&location, &version, &utf8(), &edits, window()).expect("the save lands"),
    );

    assert_ne!(saved, version, "a longer file is another version");
    assert_eq!(saved, read_version(&location).expect("the version reads"));

    let (source, opened) = open_source(&location).expect("the saved file opens");

    assert_eq!(opened, saved);
    assert_eq!(
        source.byte_length().expect("the length reads"),
        u64::try_from(CITIES.len() - "Lima".len() + "\"Cusco, Peru\"".len()).expect("a small file")
    );
}

#[test]
fn a_missing_local_file_names_its_path() {
    let directory = TestDirectory::new("missing");
    let path = directory.path.join("absent.csv");
    let location = FileLocation::Local { path: path.clone() };

    let error = read_version(&location).expect_err("a missing file has no version");

    assert!(matches!(error, StorageError::LocalIo { .. }), "{error}");
    assert!(
        error.to_string().contains(&path.display().to_string()),
        "{error}"
    );
}

#[cfg(unix)]
#[test]
fn a_local_save_keeps_the_permissions_of_the_original_file() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("permissions");
    let (path, location) = directory.file("cities.csv", CITIES);

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))
        .expect("the seeded permissions must apply");

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    save_edited(&location, &version, &utf8(), &edits, window()).expect("the save lands");

    let mode = std::fs::metadata(&path)
        .expect("the saved file exists")
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(mode, 0o640);
}

#[cfg(unix)]
#[test]
fn a_local_save_refuses_a_read_only_file() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("read-only");
    let (path, location) = directory.file("cities.csv", CITIES);

    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))
        .expect("the seeded permissions must apply");

    // A privileged process can write read-only files, and the refusal this
    // test asserts does not apply to it.
    if std::fs::File::options().write(true).open(&path).is_ok() {
        return;
    }

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    let error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("a read-only file must be refused");

    assert!(
        matches!(
            &error,
            StorageError::LocalIo { source, .. }
                if source.kind() == std::io::ErrorKind::PermissionDenied
        ),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).expect("the file reads"), CITIES);
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[cfg(unix)]
#[test]
fn a_local_save_refuses_a_file_the_user_cannot_write_even_when_others_can() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("not-writable-by-owner");
    let (path, location) = directory.file("cities.csv", CITIES);

    // The group and others may write, so the permission bits alone do not
    // read as read-only, but the owner, who runs the test, may not.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o466))
        .expect("the seeded permissions must apply");

    // A privileged process can write the file, and the refusal this test
    // asserts does not apply to it.
    if std::fs::File::options().write(true).open(&path).is_ok() {
        return;
    }

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    let error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("a file the user cannot write must be refused");

    assert!(
        matches!(
            &error,
            StorageError::LocalIo { source, .. }
                if source.kind() == std::io::ErrorKind::PermissionDenied
        ),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).expect("the file reads"), CITIES);
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[cfg(unix)]
#[test]
fn a_local_save_through_a_symlink_writes_the_file_the_link_points_at() {
    let directory = TestDirectory::new("symlink");
    let (target, _target_location) = directory.file("real.csv", CITIES);

    let link = directory.path.join("link.csv");
    std::os::unix::fs::symlink(&target, &link).expect("the symlink must be creatable");

    let location = FileLocation::Local { path: link.clone() };
    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    save_edited(&location, &version, &utf8(), &edits, window()).expect("the save lands");

    assert_eq!(
        std::fs::read(&target).expect("the target reads"),
        b"name,city\r\nAna,Cusco\r\n\"Bo, Jr\",Quito\nCy,Rome"
    );
    assert!(
        std::fs::symlink_metadata(&link)
            .expect("the link exists")
            .file_type()
            .is_symlink(),
        "the save must not turn the link into a regular file"
    );
    assert_eq!(directory.entry_names(), ["link.csv", "real.csv"]);
}

// -- Object storage ------------------------------------------------------------

#[test]
fn the_object_source_reads_the_bytes_a_slice_of_the_full_object_holds() {
    let connection = FakeConnection::with_object(CITIES);
    let (source, _version) = open_source(&connection.location()).expect("the object opens");

    let window = source.read_range(11..19).expect("the range reads");

    assert_eq!(Some(window.as_slice()), CITIES.get(11..19));
    assert_eq!(window, b"Ana,Lima");
}

#[test]
fn the_object_source_clamps_a_range_before_asking_a_store_that_refuses_one_past_the_end() {
    let connection = FakeConnection::with_object(CITIES);
    let (source, _version) = open_source(&connection.location()).expect("the object opens");

    let tail = source.read_range(36..10_000).expect("the tail reads");
    let past_end = source.read_range(500..600).expect("an empty range reads");

    assert_eq!(tail, b"Cy,Rome");
    assert!(past_end.is_empty());
}

#[test]
fn the_object_source_asks_for_the_object_metadata_at_most_once() {
    let connection = FakeConnection::with_object(CITIES);
    let (source, version) = open_source(&connection.location()).expect("the object opens");

    let expected = u64::try_from(CITIES.len()).expect("a small object");

    for _ in 0..5 {
        assert_eq!(source.byte_length().expect("the length reads"), expected);
    }

    assert_eq!(connection.store.head_calls(), 1);
    assert_eq!(
        version,
        SourceVersion::Object {
            etag: Some("etag-0".to_string()),
            last_modified: None,
            length: expected,
        }
    );
}

#[test]
fn an_object_save_with_one_replaced_record_uploads_the_edited_bytes() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    let saved = saved_version(
        save_edited(&location, &version, &utf8(), &edits, window()).expect("the save lands"),
    );

    assert_eq!(
        connection.store.bytes(),
        b"name,city\r\nAna,Cusco\r\n\"Bo, Jr\",Quito\nCy,Rome"
    );
    assert_eq!(
        connection.store.content_type().as_deref(),
        Some("text/csv"),
        "the upload keeps the object's content type"
    );
    assert_ne!(saved, version);
    assert_eq!(saved, read_version(&location).expect("the version reads"));

    let uploads = connection.store.uploaded_from();

    assert_eq!(uploads.len(), 1);
    assert!(
        uploads.iter().all(|path| !path.exists()),
        "the temporary file must be removed after the upload"
    );

    #[cfg(unix)]
    assert_eq!(
        connection.store.uploaded_modes(),
        [0o600],
        "the temporary file must be private to its owner"
    );
}

#[test]
fn an_object_save_refuses_a_changed_etag_before_any_upload() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    connection.store.change_etag();

    let error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("a changed object must be refused");

    assert!(matches!(error, StorageError::SourceChanged), "{error}");
    assert!(connection.store.uploaded_from().is_empty());
    assert_eq!(connection.store.bytes(), CITIES);
}

#[test]
fn an_object_read_failure_surfaces_the_driver_message() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);
    let (source, _version) = open_source(&location).expect("the object opens");

    connection
        .store
        .fail_reads_with("AccessDenied: no s3:GetObject on reports");

    let read_error = source.read_range(0..4).expect_err("the read must fail");

    assert!(
        read_error
            .to_string()
            .contains("AccessDenied: no s3:GetObject on reports"),
        "{read_error}"
    );

    let save_error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("a save that cannot read must fail");

    assert!(matches!(save_error, StorageError::Read(_)), "{save_error}");
    assert!(
        save_error
            .to_string()
            .contains("AccessDenied: no s3:GetObject on reports"),
        "{save_error}"
    );
    assert!(connection.store.uploaded_from().is_empty());
}

#[test]
fn an_object_upload_failure_surfaces_the_driver_message_and_removes_the_temporary_file() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    connection
        .store
        .fail_uploads_with("SlowDown: reduce your request rate");

    let staging = TestDirectory::new("upload-failure");

    let error = save_object_staging_in(&staging, &location, &version, &utf8(), &edits)
        .expect_err("a failed upload must fail the save");

    assert!(matches!(error, StorageError::ObjectStore { .. }), "{error}");
    assert!(
        error
            .to_string()
            .contains("SlowDown: reduce your request rate"),
        "{error}"
    );
    assert_eq!(connection.store.bytes(), CITIES);

    let uploads = connection.store.uploaded_from();

    assert_eq!(uploads.len(), 1);
    assert!(uploads.iter().all(|path| path.starts_with(&staging.path)));
    assert!(staging.entry_names().is_empty());
}

#[test]
fn a_failing_object_write_uploads_nothing_and_leaves_no_temporary_file() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    let windows_1252 = dialect("windows-1252");
    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, windows_1252, 0, &["Ana", "\u{6771}\u{4eac}"]);

    let staging = TestDirectory::new("failing-object-write");

    let error = save_object_staging_in(&staging, &location, &version, &windows_1252, &edits)
        .expect_err("an unencodable character must fail the save");

    assert!(matches!(error, StorageError::Write(_)), "{error}");
    assert!(connection.store.uploaded_from().is_empty());
    assert_eq!(connection.store.bytes(), CITIES);
    assert!(staging.entry_names().is_empty());
}

#[test]
fn an_object_save_whose_new_version_cannot_be_read_is_reported_as_saved() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    connection
        .store
        .fail_heads_after_an_upload_with("ServiceUnavailable: try again");

    let outcome = save_edited(&location, &version, &utf8(), &edits, window())
        .expect("a save that replaced the object is not a failed save");

    assert!(
        matches!(
            &outcome,
            SaveOutcome::SavedVersionUnknown(error)
                if error.to_string().contains("ServiceUnavailable: try again")
        ),
        "{outcome:?}"
    );
    assert_eq!(
        connection.store.bytes(),
        b"name,city\r\nAna,Cusco\r\n\"Bo, Jr\",Quito\nCy,Rome"
    );
}

#[test]
fn an_object_that_reports_no_etag_and_no_time_is_refused_as_unverifiable() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    connection.store.omit_identity();

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);
    let staging = TestDirectory::new("unverifiable");

    assert!(!version.detects_same_length_change());

    let error = save_object_staging_in(&staging, &location, &version, &utf8(), &edits)
        .expect_err("a version that is only a length must be refused");

    assert!(
        matches!(error, StorageError::VersionUnverifiable),
        "{error}"
    );
    assert!(connection.store.uploaded_from().is_empty());
    assert_eq!(connection.store.bytes(), CITIES);
    assert!(staging.entry_names().is_empty());
}

#[test]
fn a_version_detects_a_same_length_change_only_with_a_time_or_an_etag() {
    let local = |modified| SourceVersion::Local {
        modified,
        length: 10,
    };

    let object = |etag: Option<&str>, last_modified| SourceVersion::Object {
        etag: etag.map(str::to_string),
        last_modified,
        length: 10,
    };

    let now = dbflux_core::chrono::Utc::now();

    assert!(local(Some(std::time::SystemTime::now())).detects_same_length_change());
    assert!(!local(None).detects_same_length_change());

    assert!(object(Some("etag"), None).detects_same_length_change());
    assert!(object(None, Some(now)).detects_same_length_change());
    assert!(!object(None, None).detects_same_length_change());
}

#[test]
fn a_local_version_without_a_modification_time_is_refused_as_unverifiable() {
    let without_time = SourceVersion::Local {
        modified: None,
        length: 10,
    };

    let error = verify_version(&without_time, &without_time)
        .expect_err("a version that is only a length must be refused");

    assert!(
        matches!(error, StorageError::VersionUnverifiable),
        "{error}"
    );

    let longer = SourceVersion::Local {
        modified: None,
        length: 11,
    };

    let error = verify_version(&without_time, &longer).expect_err("a changed length is a change");

    assert!(matches!(error, StorageError::SourceChanged), "{error}");
}

#[test]
fn an_object_replaced_while_it_was_being_read_is_refused_before_the_upload() {
    let connection = FakeConnection::with_object(CITIES);
    let location = connection.location();

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);
    let staging = TestDirectory::new("replaced-while-read");

    let reads_so_far = connection.store.range_reads.load(Ordering::SeqCst);
    connection.store.change_etag_at_range_read(reads_so_far + 2);

    let error = save_object_staging_in(&staging, &location, &version, &utf8(), &edits)
        .expect_err("an object replaced during the read must be refused");

    assert!(matches!(error, StorageError::SourceChanged), "{error}");
    assert!(connection.store.uploaded_from().is_empty());
    assert_eq!(connection.store.bytes(), CITIES);
    assert!(staging.entry_names().is_empty());
}

// -- Failures after partial output, and what they name ---------------------------

/// A file larger than the writer's buffer whose last record ends inside a
/// quoted field that is never closed. Appending a column to it fails at that
/// record, after every record before it was written out.
fn file_with_an_unclosed_final_quote() -> Vec<u8> {
    let mut bytes = b"name,city\n".to_vec();

    for _ in 0..4_000 {
        bytes.extend_from_slice(b"Ana,Lima\n");
    }

    bytes.extend_from_slice(b"Bo,\"Quito");
    bytes
}

fn append_a_column(source_length: usize) -> EditSet {
    let mut edits = EditSet::new(u64::try_from(source_length).expect("a small file"));

    edits.appended_columns.push(AppendedColumn {
        header: "zip".to_string(),
        default_value: "0".to_string(),
        values: Vec::new(),
    });

    edits
}

#[test]
fn a_local_write_that_fails_after_partial_output_leaves_the_target_untouched_and_no_temporary_file()
{
    let bytes = file_with_an_unclosed_final_quote();

    let directory = TestDirectory::new("late-failure");
    let (path, location) = directory.file("cities.csv", &bytes);

    let version = read_version(&location).expect("the version reads");
    let edits = append_a_column(bytes.len());

    let error = save_edited(&location, &version, &utf8(), &edits, window())
        .expect_err("an unclosed quote must fail a column append");

    assert!(
        matches!(error, StorageError::Write(WriteError::UnclosedQuote { .. })),
        "{error}"
    );
    assert_eq!(std::fs::read(&path).expect("the file reads"), bytes);
    assert_eq!(directory.entry_names(), ["cities.csv"]);
}

#[test]
fn an_object_write_that_fails_after_partial_output_uploads_nothing_and_leaves_no_temporary_file() {
    let bytes = file_with_an_unclosed_final_quote();

    let connection = FakeConnection::with_object(&bytes);
    let location = connection.location();

    let version = read_version(&location).expect("the version reads");
    let edits = append_a_column(bytes.len());
    let staging = TestDirectory::new("late-object-failure");

    let error = save_object_staging_in(&staging, &location, &version, &utf8(), &edits)
        .expect_err("an unclosed quote must fail a column append");

    assert!(
        matches!(error, StorageError::Write(WriteError::UnclosedQuote { .. })),
        "{error}"
    );
    assert!(connection.store.uploaded_from().is_empty());
    assert_eq!(connection.store.bytes(), bytes);
    assert!(staging.entry_names().is_empty());
}

#[cfg(target_os = "linux")]
#[test]
fn a_temporary_file_that_cannot_take_the_bytes_names_the_file_being_saved() {
    let source = MemorySource::new(CITIES.to_vec());
    let edits = EditSet::new(u64::try_from(CITIES.len()).expect("a small file"));
    let captured = SourceVersion::Local {
        modified: None,
        length: 0,
    };

    let request = SaveRequest {
        captured: &captured,
        dialect: &utf8(),
        edits: &edits,
        window_size: window(),
    };

    let full_device = std::fs::File::options()
        .write(true)
        .open("/dev/full")
        .expect("the full device opens for writing");

    let error = write_staged(
        &source,
        &request,
        &full_device,
        "/data/cities.csv",
        Path::new("/data"),
    )
    .expect_err("a device without space must refuse the bytes");

    assert!(
        matches!(error, StorageError::TemporaryFile { .. }),
        "{error}"
    );

    let message = error.to_string();

    assert!(message.contains("/data/cities.csv"), "{message}");
    assert!(message.contains("No space left"), "{message}");
}

#[cfg(unix)]
#[test]
fn a_save_into_a_directory_that_refuses_new_files_names_the_file_being_saved() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDirectory::new("read-only-directory");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    std::fs::set_permissions(&directory.path, std::fs::Permissions::from_mode(0o555))
        .expect("the directory permissions must apply");

    let result = save_edited(&location, &version, &utf8(), &edits, window());

    std::fs::set_permissions(&directory.path, std::fs::Permissions::from_mode(0o755))
        .expect("the directory permissions must be restorable");

    // A privileged process can create files in a read-only directory, and the
    // refusal this test asserts does not apply to it.
    let Err(error) = result else {
        return;
    };

    assert!(
        matches!(error, StorageError::TemporaryFile { .. }),
        "{error}"
    );

    let message = error.to_string();

    assert!(message.contains(&path.display().to_string()), "{message}");
    assert!(
        message.contains(&directory.path.display().to_string()),
        "{message}"
    );
    assert!(!message.contains(".dbflux-stage-"), "{message}");
    assert_eq!(std::fs::read(&path).expect("the file reads"), CITIES);
}

#[cfg(unix)]
#[test]
fn a_local_save_replaces_the_target_with_a_complete_new_file_in_one_step() {
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;

    let directory = TestDirectory::new("one-step");
    let (path, location) = directory.file("cities.csv", CITIES);

    let version = read_version(&location).expect("the version reads");
    let edits = replace_record(&location, utf8(), 0, &["Ana", "Cusco"]);

    let mut held_open = std::fs::File::open(&path).expect("the old file opens");
    let old_inode = held_open.metadata().expect("the old file stats").ino();

    save_edited(&location, &version, &utf8(), &edits, window()).expect("the save lands");

    let mut seen_through_the_old_handle = Vec::new();
    held_open
        .read_to_end(&mut seen_through_the_old_handle)
        .expect("the old handle reads");

    assert_eq!(
        seen_through_the_old_handle, CITIES,
        "a reader that opened the file before the save must never see it change"
    );
    assert_ne!(
        std::fs::metadata(&path).expect("the new file stats").ino(),
        old_inode,
        "the path must name a new file, not the old one rewritten"
    );
    assert_eq!(
        std::fs::read(&path).expect("the file reads"),
        b"name,city\r\nAna,Cusco\r\n\"Bo, Jr\",Quito\nCy,Rome"
    );
}

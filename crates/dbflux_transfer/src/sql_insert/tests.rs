use std::sync::{Arc, Mutex};

use dbflux_core::{
    Connection, DbError, DbKind, DefaultSqlDialect, DriverCapabilities, DriverLimits,
    DriverMetadata, GeneratedQuery, GeneratorError, MutationCategory, QueryLanguage, QueryRequest,
    QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, SqlMutationGenerator,
};

use super::*;
use crate::file_sink::{FileFormat, FileSink};
use crate::pipeline::{RowChunk, RowSink, TableMappingMode};

static DIALECT: DefaultSqlDialect = DefaultSqlDialect;
static REAL_GENERATOR: SqlMutationGenerator = SqlMutationGenerator::new(&DIALECT);

/// Records the row count of every batch it is asked to render.
struct RecordingGenerator {
    batch_sizes: Mutex<Vec<usize>>,
    /// The column names and each row's width of every script batch.
    shapes: Mutex<Vec<(Vec<String>, Vec<usize>)>>,
    answers_none: bool,
}

impl RecordingGenerator {
    fn new(answers_none: bool) -> Self {
        Self {
            batch_sizes: Mutex::new(Vec::new()),
            shapes: Mutex::new(Vec::new()),
            answers_none,
        }
    }

    fn batch_sizes(&self) -> Vec<usize> {
        self.batch_sizes
            .lock()
            .map(|sizes| sizes.clone())
            .unwrap_or_default()
    }
}

impl QueryGenerator for RecordingGenerator {
    fn supported_categories(&self) -> &'static [MutationCategory] {
        &[MutationCategory::Sql]
    }

    fn generate_mutation(
        &self,
        _mutation: &dbflux_core::MutationRequest,
    ) -> Option<GeneratedQuery> {
        None
    }

    fn generate_bulk_insert(
        &self,
        schema: Option<&str>,
        table: &str,
        _columns: &[String],
        _column_types: &[Option<String>],
        rows: &[&[Value]],
    ) -> Result<Option<GeneratedQuery>, GeneratorError> {
        if let Ok(mut sizes) = self.batch_sizes.lock() {
            sizes.push(rows.len());
        }
        if self.answers_none {
            return Ok(None);
        }

        Ok(Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: format!("INSERT {schema:?}.{table} rows={}", rows.len()),
        }))
    }

    fn generate_bulk_insert_for_script(
        &self,
        schema: Option<&str>,
        table: &str,
        columns: &[String],
        _column_types: &[Option<String>],
        identity_columns: &[String],
        rows: &[&[Value]],
    ) -> Result<Option<GeneratedQuery>, GeneratorError> {
        if let Ok(mut sizes) = self.batch_sizes.lock() {
            sizes.push(rows.len());
        }
        if let Ok(mut shapes) = self.shapes.lock() {
            shapes.push((columns.to_vec(), rows.iter().map(|row| row.len()).collect()));
        }
        if self.answers_none {
            return Ok(None);
        }

        let identity = if identity_columns.is_empty() {
            String::new()
        } else {
            format!(" identity={identity_columns:?}")
        };
        Ok(Some(GeneratedQuery {
            language: QueryLanguage::Sql,
            text: format!("INSERT {schema:?}.{table} rows={}{identity}", rows.len()),
        }))
    }

    fn script_table_prologue(
        &self,
        _schema: Option<&str>,
        table: &str,
        identity_columns: &[String],
    ) -> Option<String> {
        (!identity_columns.is_empty()).then(|| format!("-- {table} ON"))
    }

    fn script_table_epilogue(
        &self,
        _schema: Option<&str>,
        table: &str,
        identity_columns: &[String],
    ) -> Option<String> {
        (!identity_columns.is_empty()).then(|| format!("-- {table} OFF"))
    }

    fn script_table_sequence_resync(
        &self,
        _schema: Option<&str>,
        table: &str,
        sequence_columns: &[String],
    ) -> Option<String> {
        (!sequence_columns.is_empty()).then(|| format!("-- {table} RESYNC {sequence_columns:?}"))
    }
}

fn columns() -> Vec<TransferColumn> {
    vec![
        TransferColumn {
            name: "id".to_string(),
            type_name: Some("integer".to_string()),
            nullable: false,
            is_primary_key: true,
            kind: dbflux_core::ColumnKind::Unknown,
        },
        TransferColumn {
            name: "note".to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
            kind: dbflux_core::ColumnKind::Unknown,
        },
    ]
}

fn row(id: i64) -> Vec<Value> {
    vec![Value::Int(id), Value::Text(format!("it's {id}"))]
}

fn write_rows(generator: &dyn QueryGenerator, batch_size: usize, count: i64) -> String {
    let mut out = Vec::new();
    let mut writer = SqlInsertWriter::new(
        &mut out,
        Some("public".to_string()),
        "notes".to_string(),
        &columns(),
        Vec::new(),
        Vec::new(),
        batch_size,
    );
    for id in 0..count {
        writer.write_row(generator, &row(id)).unwrap();
    }
    writer.finish(generator).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn rows_are_split_into_one_statement_per_batch() {
    let generator = RecordingGenerator::new(false);
    let text = write_rows(&generator, 2, 5);

    assert_eq!(generator.batch_sizes(), vec![2, 2, 1]);
    assert_eq!(
        text,
        "INSERT Some(\"public\").notes rows=2;\n\
         INSERT Some(\"public\").notes rows=2;\n\
         INSERT Some(\"public\").notes rows=1;\n"
    );
}

#[test]
fn the_statement_is_what_the_dialect_generator_produces_for_the_same_rows() {
    let text = write_rows(&REAL_GENERATOR, 2, 3);

    let names: Vec<String> = columns().iter().map(|c| c.name.clone()).collect();
    let types: Vec<Option<String>> = columns().iter().map(|c| c.type_name.clone()).collect();
    let rows: Vec<Vec<Value>> = (0..3).map(row).collect();
    let expected: String = rows
        .chunks(2)
        .map(|batch| {
            let refs: Vec<&[Value]> = batch.iter().map(Vec::as_slice).collect();
            let query = REAL_GENERATOR
                .generate_bulk_insert(Some("public"), "notes", &names, &types, &refs)
                .unwrap()
                .unwrap();
            format!("{};\n", query.text)
        })
        .collect();

    assert_eq!(text, expected);
    assert!(text.starts_with("INSERT INTO \"public\".\"notes\" (\"id\", \"note\") VALUES (0, "));
    assert!(text.contains("'it''s 1'"), "{text}");
    assert_eq!(text.matches(";\n").count(), 2);
}

#[test]
fn a_table_with_no_rows_writes_an_empty_file() {
    let generator = RecordingGenerator::new(false);
    let text = write_rows(&generator, 500, 0);

    assert_eq!(text, "");
    assert!(generator.batch_sizes().is_empty());
}

#[test]
fn a_generator_that_cannot_render_inserts_fails_the_export() {
    let generator = RecordingGenerator::new(true);
    let mut out = Vec::new();
    let mut writer = SqlInsertWriter::new(
        &mut out,
        None,
        "notes".to_string(),
        &columns(),
        Vec::new(),
        Vec::new(),
        1,
    );

    let error = writer.write_row(&generator, &row(1)).unwrap_err();
    assert_eq!(
        error.to_string(),
        "sink error: the driver cannot generate INSERT statements for 'notes'"
    );
}

#[test]
fn the_batch_size_is_at_least_one_and_capped_by_the_driver() {
    assert_eq!(effective_batch_size(500, None), 500);
    assert_eq!(effective_batch_size(500, Some(1000)), 500);
    assert_eq!(effective_batch_size(5000, Some(1000)), 1000);
    assert_eq!(effective_batch_size(0, None), 1);
    assert_eq!(effective_batch_size(10, Some(0)), 1);
}

struct FakeConnection {
    metadata: DriverMetadata,
    generator: Option<RecordingGenerator>,
    identity: Vec<String>,
    sequences: Vec<String>,
    sequence_lookups: Mutex<Vec<(Option<String>, String)>>,
    generated: Vec<String>,
    generated_lookups: Mutex<Vec<(Option<String>, String)>>,
    identity_lookups: Mutex<Vec<(Option<String>, String)>>,
    pages: Mutex<std::collections::VecDeque<Vec<Vec<Value>>>>,
}

impl FakeConnection {
    fn new(
        capabilities: DriverCapabilities,
        cap: u32,
        generator: Option<RecordingGenerator>,
    ) -> Self {
        let metadata = dbflux_core::DriverMetadataBuilder::new(
            "fake",
            "Fake",
            dbflux_core::DatabaseCategory::Relational,
            QueryLanguage::Sql,
        )
        .capabilities(capabilities)
        .limits(DriverLimits {
            max_bulk_insert_rows: cap,
            ..DriverLimits::default()
        })
        .build();

        Self {
            metadata,
            generator,
            identity: Vec::new(),
            sequences: Vec::new(),
            sequence_lookups: Mutex::new(Vec::new()),
            generated: Vec::new(),
            generated_lookups: Mutex::new(Vec::new()),
            identity_lookups: Mutex::new(Vec::new()),
            pages: Mutex::new(std::collections::VecDeque::new()),
        }
    }

    fn with_identity(mut self, identity: &[&str]) -> Self {
        self.identity = identity.iter().map(|name| name.to_string()).collect();
        self
    }

    fn with_sequences(mut self, sequences: &[&str]) -> Self {
        self.sequences = sequences.iter().map(|name| name.to_string()).collect();
        self
    }

    fn with_generated(mut self, generated: &[&str]) -> Self {
        self.generated = generated.iter().map(|name| name.to_string()).collect();
        self
    }

    fn with_page(self, rows: Vec<Vec<Value>>) -> Self {
        if let Ok(mut pages) = self.pages.lock() {
            pages.push_back(rows);
        }
        self
    }
}

impl Connection for FakeConnection {
    fn metadata(&self) -> &DriverMetadata {
        &self.metadata
    }

    fn capabilities(&self) -> DriverCapabilities {
        self.metadata.capabilities
    }

    fn ping(&self) -> Result<(), DbError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        if req.sql.contains("COUNT(*)") {
            return Ok(QueryResult::table(
                Vec::new(),
                vec![vec![Value::Int(0)]],
                None,
                std::time::Duration::ZERO,
            ));
        }

        let rows = self
            .pages
            .lock()
            .ok()
            .and_then(|mut pages| pages.pop_front())
            .unwrap_or_default();
        Ok(QueryResult::table(
            Vec::new(),
            rows,
            None,
            std::time::Duration::ZERO,
        ))
    }

    fn generated_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<String>, DbError> {
        if let Ok(mut lookups) = self.generated_lookups.lock() {
            lookups.push((schema.map(str::to_string), table.to_string()));
        }
        Ok(self.generated.clone())
    }

    fn sequence_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<String>, DbError> {
        if let Ok(mut lookups) = self.sequence_lookups.lock() {
            lookups.push((schema.map(str::to_string), table.to_string()));
        }
        Ok(self.sequences.clone())
    }

    fn identity_columns(&self, schema: Option<&str>, table: &str) -> Result<Vec<String>, DbError> {
        if let Ok(mut lookups) = self.identity_lookups.lock() {
            lookups.push((schema.map(str::to_string), table.to_string()));
        }
        Ok(self.identity.clone())
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Ok(())
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        Err(DbError::NotSupported("stub".to_string()))
    }

    fn kind(&self) -> DbKind {
        DbKind::SQLite
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &DIALECT
    }

    fn query_generator(&self) -> Option<&dyn QueryGenerator> {
        self.generator.as_ref().map(|g| g as &dyn QueryGenerator)
    }
}

#[test]
fn sql_export_is_offered_only_with_the_capability_and_a_generator_that_renders_inserts() {
    let supported = FakeConnection::new(
        DriverCapabilities::BULK_INSERT,
        0,
        Some(RecordingGenerator::new(false)),
    );
    assert!(supports_sql_export(&supported));

    let no_capability = FakeConnection::new(
        DriverCapabilities::empty(),
        0,
        Some(RecordingGenerator::new(false)),
    );
    assert!(!supports_sql_export(&no_capability));

    let no_generator = FakeConnection::new(DriverCapabilities::BULK_INSERT, 0, None);
    assert!(!supports_sql_export(&no_generator));

    let generator_answers_none = FakeConnection::new(
        DriverCapabilities::BULK_INSERT,
        0,
        Some(RecordingGenerator::new(true)),
    );
    assert!(!supports_sql_export(&generator_answers_none));
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "dbflux_transfer_sql_insert_{label}_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn the_file_sink_batches_by_the_option_and_the_driver_cap() {
    let dir = temp_dir("sink");

    for (option, cap, expected) in [
        (500u32, 0u32, vec![5]),
        (2, 0, vec![2, 2, 1]),
        (500, 3, vec![3, 2]),
    ] {
        let connection = Arc::new(FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            cap,
            Some(RecordingGenerator::new(false)),
        ));
        let dyn_connection: Arc<dyn Connection> = connection.clone();
        let mut sink = FileSink::new(&dir, Some("public".to_string()), "notes", FileFormat::Sql)
            .with_options(dbflux_export::FormatOptions {
                sql_batch_size: option,
                ..Default::default()
            })
            .with_connection(dyn_connection);

        sink.begin(&columns(), TableMappingMode::Existing).unwrap();
        sink.write_chunk(&RowChunk((0..5).map(row).collect()))
            .unwrap();
        let report = sink.finish().unwrap();

        assert_eq!(report.rows_transferred, 5);
        let generator = connection.generator.as_ref().unwrap();
        assert_eq!(
            generator.batch_sizes(),
            expected,
            "option {option}, cap {cap}"
        );
    }

    let written = std::fs::read_to_string(dir.join("public.notes.sql")).unwrap();
    assert_eq!(written.lines().count(), 2);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_sql_sink_without_a_connection_refuses_to_begin() {
    let dir = temp_dir("no_connection");
    let mut sink = FileSink::new(&dir, None, "notes", FileFormat::Sql);

    let error = sink
        .begin(&columns(), TableMappingMode::Existing)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "sink error: SQL export needs the source connection"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn an_export_run_hands_the_connection_to_the_sql_sink_and_records_the_format() {
    use crate::export::{ExportOptions, ExportTable, run_export};

    let dir = temp_dir("run_export");
    let connection: Arc<dyn Connection> = Arc::new(FakeConnection::new(
        DriverCapabilities::BULK_INSERT,
        0,
        Some(RecordingGenerator::new(false)),
    ));
    let tables = [ExportTable {
        schema: Some("public".to_string()),
        name: "notes".to_string(),
        columns: columns(),
        estimated_total: None,
    }];
    let options = ExportOptions {
        driver_id: "fake",
        database: "app",
        format: FileFormat::Sql,
        segment_size: 10,
        format_options: dbflux_export::FormatOptions {
            sql_batch_size: 7,
            null_text: "ignored".to_string(),
            ..Default::default()
        },
    };

    let outcome = run_export(
        &connection,
        &tables,
        &dir,
        &options,
        &dbflux_core::CancelToken::new(),
        |_, _, _| {},
    )
    .unwrap();

    let manifest = outcome.manifest.unwrap();
    assert_eq!(manifest.tables[0].format, "sql");
    assert_eq!(manifest.tables[0].file, "public.notes.sql");
    assert_eq!(manifest.tables[0].options.sql_batch_size, 7);
    assert_eq!(manifest.tables[0].options.null_text, "");
    assert_eq!(
        std::fs::read_to_string(dir.join("public.notes.sql")).unwrap(),
        ""
    );
    std::fs::remove_dir_all(&dir).ok();
}

fn write_with_identity(generator: &dyn QueryGenerator, identity: &[&str], count: i64) -> String {
    let mut out = Vec::new();
    let mut writer = SqlInsertWriter::new(
        &mut out,
        Some("public".to_string()),
        "notes".to_string(),
        &columns(),
        identity.iter().map(|name| name.to_string()).collect(),
        Vec::new(),
        2,
    );
    for id in 0..count {
        writer.write_row(generator, &row(id)).unwrap();
    }
    writer.finish(generator).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn identity_columns_wrap_the_tables_statements_and_use_the_script_variant() {
    let generator = RecordingGenerator::new(false);
    let text = write_with_identity(&generator, &["id"], 3);

    assert_eq!(
        text,
        "-- notes ON\n\
         INSERT Some(\"public\").notes rows=2 identity=[\"id\"];\n\
         INSERT Some(\"public\").notes rows=1 identity=[\"id\"];\n\
         -- notes OFF\n"
    );
}

#[test]
fn a_table_without_identity_columns_gets_no_wrapper() {
    let generator = RecordingGenerator::new(false);
    let text = write_with_identity(&generator, &[], 1);

    assert_eq!(text, "INSERT Some(\"public\").notes rows=1;\n");
}

#[test]
fn an_identity_table_with_no_rows_is_still_an_empty_file() {
    let generator = RecordingGenerator::new(false);
    assert_eq!(write_with_identity(&generator, &["id"], 0), "");
}

#[test]
fn identity_columns_the_export_does_not_include_are_ignored() {
    let generator = RecordingGenerator::new(false);
    let text = write_with_identity(&generator, &["other"], 1);

    assert_eq!(text, "INSERT Some(\"public\").notes rows=1;\n");
}

fn export_one_table(connection: Arc<FakeConnection>, format: FileFormat, label: &str) -> String {
    use crate::export::{ExportOptions, ExportTable, run_export};

    let dir = temp_dir(label);
    let dyn_connection: Arc<dyn Connection> = connection;
    let tables = [ExportTable {
        schema: Some("public".to_string()),
        name: "notes".to_string(),
        columns: columns(),
        estimated_total: None,
    }];
    let options = ExportOptions {
        driver_id: "fake",
        database: "app",
        format,
        segment_size: 10,
        format_options: Default::default(),
    };
    let outcome = run_export(
        &dyn_connection,
        &tables,
        &dir,
        &options,
        &dbflux_core::CancelToken::new(),
        |_, _, _| {},
    )
    .unwrap();
    assert!(outcome.manifest.is_some(), "{:?}", outcome.warnings);

    let written =
        std::fs::read_to_string(dir.join(format!("public.notes.{}", format.extension()))).unwrap();
    std::fs::remove_dir_all(&dir).ok();
    written
}

#[test]
fn an_export_run_looks_up_the_identity_columns_and_wraps_the_script() {
    let connection = Arc::new(
        FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            0,
            Some(RecordingGenerator::new(false)),
        )
        .with_identity(&["id"])
        .with_page(vec![row(1), row(2)]),
    );

    let written = export_one_table(connection.clone(), FileFormat::Sql, "identity_run");

    assert_eq!(
        written,
        "-- notes ON\n\
         INSERT Some(\"public\").notes rows=2 identity=[\"id\"];\n\
         -- notes OFF\n"
    );
    assert_eq!(
        *connection.identity_lookups.lock().unwrap(),
        vec![(Some("public".to_string()), "notes".to_string())]
    );
}

#[test]
fn other_formats_never_look_up_identity_columns() {
    let connection = Arc::new(
        FakeConnection::new(DriverCapabilities::BULK_INSERT, 0, None)
            .with_identity(&["id"])
            .with_page(vec![row(1)]),
    );

    let written = export_one_table(connection.clone(), FileFormat::Csv, "identity_csv");

    assert!(written.starts_with("id,note\n1,"), "{written}");
    assert!(connection.identity_lookups.lock().unwrap().is_empty());
}

fn write_omitting(generator: &dyn QueryGenerator, identity: &[&str], generated: &[&str]) -> String {
    let mut out = Vec::new();
    let names = |list: &[&str]| list.iter().map(|name| name.to_string()).collect();
    let mut writer = SqlInsertWriter::new(
        &mut out,
        Some("public".to_string()),
        "notes".to_string(),
        &columns(),
        names(identity),
        names(generated),
        10,
    );
    writer.write_row(generator, &row(1)).unwrap();
    writer.write_row(generator, &row(2)).unwrap();
    writer.finish(generator).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn generated_columns_are_left_out_of_the_column_list_and_every_row() {
    let generator = RecordingGenerator::new(false);
    write_omitting(&generator, &[], &["note"]);

    let shapes = generator.shapes.lock().unwrap();
    assert_eq!(*shapes, vec![(vec!["id".to_string()], vec![1, 1])]);
}

#[test]
fn a_generated_column_that_is_also_identity_is_not_wrapped() {
    let generator = RecordingGenerator::new(false);
    let text = write_omitting(&generator, &["note"], &["note"]);

    assert_eq!(text, "INSERT Some(\"public\").notes rows=2;\n");
}

#[test]
fn a_table_with_only_generated_columns_cannot_be_exported() {
    let generator = RecordingGenerator::new(false);
    let mut out = Vec::new();
    let mut writer = SqlInsertWriter::new(
        &mut out,
        None,
        "notes".to_string(),
        &columns(),
        Vec::new(),
        vec!["id".to_string(), "note".to_string()],
        10,
    );

    let error = writer.write_row(&generator, &row(1)).unwrap_err();
    assert_eq!(
        error.to_string(),
        "sink error: every column of 'notes' is generated by the database, so there is nothing to insert"
    );
}

#[test]
fn an_export_run_omits_the_generated_columns_for_sql_only() {
    let connection = Arc::new(
        FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            0,
            Some(RecordingGenerator::new(false)),
        )
        .with_generated(&["note"])
        .with_page(vec![row(1), row(2)]),
    );

    let written = export_one_table(connection.clone(), FileFormat::Sql, "generated_run");

    assert_eq!(written, "INSERT Some(\"public\").notes rows=2;\n");
    let generator = connection.generator.as_ref().unwrap();
    assert_eq!(
        *generator.shapes.lock().unwrap(),
        vec![(vec!["id".to_string()], vec![1, 1])]
    );
    assert_eq!(
        *connection.generated_lookups.lock().unwrap(),
        vec![(Some("public".to_string()), "notes".to_string())]
    );

    let csv_connection = Arc::new(
        FakeConnection::new(DriverCapabilities::BULK_INSERT, 0, None)
            .with_generated(&["note"])
            .with_page(vec![row(1)]),
    );
    let csv = export_one_table(csv_connection.clone(), FileFormat::Csv, "generated_csv");
    assert!(csv.starts_with("id,note\n1,"), "{csv}");
    assert!(csv_connection.generated_lookups.lock().unwrap().is_empty());
}

fn write_with_sequences(
    generator: &dyn QueryGenerator,
    identity: &[&str],
    sequences: &[&str],
    generated: &[&str],
    count: i64,
) -> String {
    let names = |list: &[&str]| list.iter().map(|name| name.to_string()).collect();
    let mut out = Vec::new();
    let mut writer = SqlInsertWriter::new(
        &mut out,
        Some("public".to_string()),
        "notes".to_string(),
        &columns(),
        names(identity),
        names(generated),
        2,
    )
    .with_sequence_columns(names(sequences));
    for id in 0..count {
        writer.write_row(generator, &row(id)).unwrap();
    }
    writer.finish(generator).unwrap();
    String::from_utf8(out).unwrap()
}

#[test]
fn sequence_columns_are_resynced_after_the_last_batch() {
    let generator = RecordingGenerator::new(false);
    let text = write_with_sequences(&generator, &[], &["id"], &[], 3);

    assert_eq!(
        text,
        "INSERT Some(\"public\").notes rows=2;\n\
         INSERT Some(\"public\").notes rows=1;\n\
         -- notes RESYNC [\"id\"]\n"
    );
}

#[test]
fn the_resync_follows_the_identity_epilogue() {
    let generator = RecordingGenerator::new(false);
    let text = write_with_sequences(&generator, &["id"], &["id"], &[], 1);

    assert_eq!(
        text,
        "-- notes ON\n\
         INSERT Some(\"public\").notes rows=1 identity=[\"id\"];\n\
         -- notes OFF\n\
         -- notes RESYNC [\"id\"]\n"
    );
}

#[test]
fn a_table_with_no_rows_gets_no_resync() {
    let generator = RecordingGenerator::new(false);
    assert_eq!(write_with_sequences(&generator, &[], &["id"], &[], 0), "");
}

#[test]
fn sequence_columns_the_export_does_not_write_are_not_resynced() {
    let generator = RecordingGenerator::new(false);
    let text = write_with_sequences(&generator, &[], &["other", "note"], &["note"], 1);

    assert_eq!(text, "INSERT Some(\"public\").notes rows=1;\n");
}

#[test]
fn an_export_run_looks_up_the_sequence_columns_for_sql_only() {
    let connection = Arc::new(
        FakeConnection::new(
            DriverCapabilities::BULK_INSERT,
            0,
            Some(RecordingGenerator::new(false)),
        )
        .with_sequences(&["id"])
        .with_page(vec![row(1), row(2)]),
    );

    let written = export_one_table(connection.clone(), FileFormat::Sql, "sequence_run");

    assert_eq!(
        written,
        "INSERT Some(\"public\").notes rows=2;\n\
         -- notes RESYNC [\"id\"]\n"
    );
    assert_eq!(
        *connection.sequence_lookups.lock().unwrap(),
        vec![(Some("public".to_string()), "notes".to_string())]
    );

    let csv_connection = Arc::new(
        FakeConnection::new(DriverCapabilities::BULK_INSERT, 0, None)
            .with_sequences(&["id"])
            .with_page(vec![row(1)]),
    );
    let csv = export_one_table(csv_connection.clone(), FileFormat::Csv, "sequence_csv");
    assert!(csv.starts_with("id,note\n1,"), "{csv}");
    assert!(csv_connection.sequence_lookups.lock().unwrap().is_empty());
}

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
    answers_none: bool,
}

impl RecordingGenerator {
    fn new(answers_none: bool) -> Self {
        Self {
            batch_sizes: Mutex::new(Vec::new()),
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
}

fn columns() -> Vec<TransferColumn> {
    vec![
        TransferColumn {
            name: "id".to_string(),
            type_name: Some("integer".to_string()),
            nullable: false,
            is_primary_key: true,
        },
        TransferColumn {
            name: "note".to_string(),
            type_name: Some("text".to_string()),
            nullable: true,
            is_primary_key: false,
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
    let mut writer = SqlInsertWriter::new(&mut out, None, "notes".to_string(), &columns(), 1);

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
        }
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

    fn execute(&self, _req: &QueryRequest) -> Result<QueryResult, DbError> {
        Ok(QueryResult::empty())
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

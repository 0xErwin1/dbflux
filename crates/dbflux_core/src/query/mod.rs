pub(crate) mod generator;
pub(crate) mod language_service;
pub(crate) mod safety;
pub(crate) mod table_browser;
pub(crate) mod types;

pub use generator::{GeneratedQuery, MutationCategory, QueryGenerator, SqlMutationGenerator};
pub use language_service::{
    classify_query_for_language, detect_dangerous_mongo, detect_dangerous_query,
    detect_dangerous_redis, detect_dangerous_sql, strip_leading_comments, DangerousQueryKind,
    Diagnostic, DiagnosticSeverity, EditorDiagnostic, LanguageService, SqlLanguageService,
    TextPosition, TextPositionRange, TextRange, ValidationResult,
};
pub use safety::{classify_query_for_governance, classify_sql_execution, is_safe_read_query};
pub use table_browser::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ColumnRef, DescribeRequest,
    ExplainRequest, OrderByColumn, Pagination, SortDirection, TableBrowseRequest,
    TableCountRequest, TableRef,
};
pub use types::{ColumnMeta, QueryHandle, QueryRequest, QueryResult, QueryResultShape, Row};

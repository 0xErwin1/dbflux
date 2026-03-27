pub(crate) mod generator;
pub(crate) mod language_service;
pub(crate) mod safety;
pub(crate) mod semantic;
pub(crate) mod table_browser;
pub(crate) mod types;

pub use generator::{
    GeneratedQuery, MutationCategory, MutationTemplateOperation, MutationTemplateRequest,
    QueryGenerator, ReadTemplateOperation, ReadTemplateRequest, SqlMutationGenerator,
};
pub use language_service::{
    DangerousQueryKind, Diagnostic, DiagnosticSeverity, EditorDiagnostic, LanguageService,
    SqlLanguageService, TextPosition, TextPositionRange, TextRange, ValidationResult,
    classify_query_for_language, detect_dangerous_mongo, detect_dangerous_query,
    detect_dangerous_redis, detect_dangerous_sql, strip_leading_comments,
};
pub use safety::{classify_query_for_governance, classify_sql_execution, is_safe_read_query};
pub use semantic::{
    AggregateFunction, AggregateRequest, AggregateSpec, PlannedQuery, SemanticFieldRef,
    SemanticFilter, SemanticPlan, SemanticPlanKind, SemanticPlanner, SemanticPredicate,
    SemanticRequest, SemanticRequestKind, parse_semantic_filter_json, render_semantic_filter_sql,
};
pub use table_browser::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionRef, ColumnRef, DescribeRequest,
    ExplainRequest, OrderByColumn, Pagination, SortDirection, TableBrowseRequest,
    TableCountRequest, TableRef,
};
pub use types::{ColumnMeta, QueryHandle, QueryRequest, QueryResult, QueryResultShape, Row};

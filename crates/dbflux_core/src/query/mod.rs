pub(crate) mod column_kind;
pub(crate) mod generator;
pub(crate) mod keyset;
pub(crate) mod language_service;
pub mod relational_filter;
pub(crate) mod safety;
pub(crate) mod semantic;
pub(crate) mod table_browser;
pub(crate) mod time_macros;
pub(crate) mod tx_vocab;
pub(crate) mod types;
pub(crate) mod visual_query;

pub use column_kind::infer_column_kind;
pub use generator::{
    CollectionTemplateRequest, GeneratedMutation, GeneratedQuery, GeneratorError, MutationCategory,
    MutationTemplateOperation, MutationTemplateRequest, QueryGenError, QueryGenerator,
    ReadTemplateOperation, ReadTemplateRequest, SelectQuery, SqlMutationGenerator,
};
pub use keyset::lower_keyset_predicate;
pub use language_service::{
    ClassifiedMutation, DangerousQueryKind, Diagnostic, DiagnosticSeverity, EditorDiagnostic,
    LanguageService, SqlLanguageService, TextPosition, TextPositionRange, TextRange,
    ValidationResult, classify_query_for_language, classify_visual_mutation,
    detect_dangerous_query, detect_dangerous_sql, strip_leading_comments,
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
pub use time_macros::{contains_time_macros, substitute_time_macros};
pub use tx_vocab::TransactionVocab;
pub use types::{
    ColumnKind, ColumnMeta, QueryHandle, QueryRequest, QueryResult, QueryResultShape,
    ResolvedWindow, Row,
};
pub use visual_query::SortDirection as VisualSortDirection;
pub use visual_query::{
    AliasOrigin, Assignment, AssignmentValue, BoolOp, Comparator, CountSpec, FilterNode,
    JoinFilterNode, JoinKind, JoinOn, JoinPredicate, JoinStep, LiteralValue, MutationKind,
    Predicate, PredicateValue, ProjectedColumn, Projection, ScalarLiteral, SortEntry, SourceTable,
    SpecError, VisualMutationSpec, VisualQuerySpec,
};

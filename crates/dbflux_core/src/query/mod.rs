pub(crate) mod column_kind;
pub(crate) mod document_query;
pub(crate) mod generator;
pub(crate) mod join_condition;
pub(crate) mod keyset;
pub(crate) mod language_service;
pub(crate) mod name_suggestion;
pub mod relational_filter;
pub(crate) mod relaxed_json;
pub(crate) mod safety;
pub(crate) mod script_operation;
pub(crate) mod semantic;
pub(crate) mod sql_context;
pub(crate) mod table_browser;
pub(crate) mod time_macros;
pub(crate) mod transfer;
pub(crate) mod tx_vocab;
pub(crate) mod types;
pub(crate) mod visual_query;

pub use column_kind::{infer_column_kind, project_aggregate_kinds};
pub use document_query::{
    DocumentAccumulator, DocumentAccumulatorKind, DocumentCombinator, DocumentCondition,
    DocumentFieldType, DocumentFilterGroup, DocumentFilterNode, DocumentFindSlots,
    DocumentGroupStage, DocumentOperator, DocumentProjection, DocumentProjectionMode,
    DocumentQueryCodec, DocumentQueryMode, DocumentQuerySpec, DocumentSlot, DocumentSlotParse,
    DocumentSortDirection, DocumentSortKey, DocumentSpecProblem, DocumentValue,
    UnrepresentableClause,
};
pub use generator::{
    CollectionTemplateRequest, CreateTableSpec, GeneratedMutation, GeneratedQuery, GeneratorError,
    MutationCategory, MutationTemplateOperation, MutationTemplateRequest, QueryGenError,
    QueryGenerator, ReadTemplateOperation, ReadTemplateRequest, SelectQuery, SqlMutationGenerator,
    inline_params, render_filter_node_sql,
};
pub use join_condition::{
    JOIN_CONDITION_FORM, JoinColumnRef, JoinComparison, JoinConditionError,
    is_plain_sql_identifier, join_on_conditions, parse_join_condition,
};
pub use keyset::lower_keyset_predicate;
pub use language_service::{
    ClassifiedMutation, CodeAction, CodeActionEdit, DangerousQueryKind, Diagnostic,
    DiagnosticSeverity, EditorDiagnostic, LanguageService, SchemaColumns, SqlLanguageService,
    TextPosition, TextPositionRange, TextRange, ValidationResult, aggregate_writes_output,
    classify_query_for_language, classify_query_for_language_with_service,
    classify_visual_mutation, detect_dangerous_query, detect_dangerous_sql, sql_statement_keywords,
    strip_leading_comments,
};
pub use name_suggestion::{MAX_NAME_SUGGESTIONS, MAX_SUGGESTED_NAME_LENGTH, suggest_names};
pub use relaxed_json::{normalize_relaxed_json, parse_relaxed_json};
pub use safety::{classify_query_for_governance, classify_sql_execution, is_safe_read_query};
pub use script_operation::{
    ScriptMethod, ScriptOperation, ScriptOperationCounts, ScriptOperationHost,
    ScriptOperationOutcome, ScriptTarget, ceiling_permits,
};
pub use semantic::{
    AggregateFunction, AggregateRequest, AggregateSpec, PlannedQuery, SemanticFieldRef,
    SemanticFilter, SemanticPlan, SemanticPlanKind, SemanticPlanner, SemanticPredicate,
    SemanticRequest, SemanticRequestKind, parse_semantic_filter_json, render_semantic_filter_sql,
    semantic_filter_to_filter_node,
};
pub use sql_context::{
    ScopeRelation, SqlClause, SqlCompletionContext, SqlContextEngine, SqlCursorAnalysis,
    StatementScope,
};
pub use table_browser::{
    CollectionBrowseRequest, CollectionCountEstimate, CollectionCountRequest, CollectionRef,
    ColumnRef, DescribeRequest, ExplainRequest, OrderByColumn, Pagination, SortDirection,
    TableBrowseRequest, TableCountRequest, TableRef,
};
pub use time_macros::{contains_time_macros, substitute_time_macros};
pub use transfer::TransferColumn;
pub use tx_vocab::TransactionVocab;
pub use types::{
    ColumnKind, ColumnMeta, QueryHandle, QueryRequest, QueryResult, QueryResultShape,
    ResolvedWindow, Row,
};
pub use visual_query::AggregateSpec as VisualAggregateSpec;
pub use visual_query::SortDirection as VisualSortDirection;
pub use visual_query::{
    AggFn, AliasOrigin, Assignment, AssignmentValue, BoolOp, ColumnOrigin, Comparator, CountSpec,
    EditableBinding, FilterNode, GroupByEntry, JoinFilterNode, JoinKind, JoinOn, JoinPredicate,
    JoinStep, LiteralValue, MutationKind, Predicate, PredicateValue, ProjectedColumn, Projection,
    ScalarLiteral, SortEntry, SourceTable, SpecError, VisualMutationSpec, VisualQuerySpec,
};

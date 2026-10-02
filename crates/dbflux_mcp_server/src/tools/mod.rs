//! Granular MCP tools for database operations.
//!
//! This module provides type-safe parameter structs for granular database operations
//! organized by operation type:
//! - `read`: SELECT, COUNT, AGGREGATE operations
//! - `write`: INSERT, UPDATE, UPSERT operations
//! - `destructive`: DELETE, TRUNCATE operations
//! - `ddl`: CREATE, ALTER, DROP operations
//! - `approval`: Approval flow for pending executions
//! - `audit`: Audit log querying and export

pub mod approval;
pub mod audit;
pub mod connection;
pub mod ddl;
pub mod destructive;
pub(crate) mod join_select;
pub(crate) mod not_found;
pub mod query;
pub mod read;
pub mod schema;
pub mod scripts;
pub mod write;

pub use approval::{
    ApproveExecutionParams, GetPendingExecutionParams, ListPendingExecutionsParams,
    RejectExecutionParams, RequestExecutionParams,
};
pub use audit::{ExportAuditLogsParams, GetAuditEntryParams, QueryAuditLogsParams};
pub use ddl::{
    AlterOperation, AlterTableParams, ColumnDef, CreateIndexParams, CreateTableParams,
    CreateTypeParams, DropDatabaseParams, DropIndexParams, DropTableParams, ForeignKeyRef,
    TypeAttribute, validate_drop_database_params, validate_drop_table_params,
};
pub use destructive::{
    DELETE_WHERE_REQUIRED_ERROR, DeleteRecordsParams, TRUNCATE_CONFIRMATION_ERROR,
    TruncateTableParams, validate_delete_params, validate_truncate_params,
};
pub use read::{
    AggregateDataParams, AggregationSpec, CountRecordsParams, JoinSpec, OrderByItem,
    SelectDataParams,
};
pub use scripts::{
    CreateScriptParams, DELETE_CONFIRMATION_ERROR, DeleteScriptParams, ExecuteScriptParams,
    GetScriptParams, ListScriptsParams, UpdateScriptParams,
    validate_delete_params as validate_delete_script_params,
};
pub use write::{InsertRecordParams, UpdateRecordsParams, UpsertRecordParams};

/// Expands to the operator reference shared by every `where` description, as
/// a literal so `concat!` can prefix it.
macro_rules! where_filter_reference {
    () => {
        "Filter as a JSON object keyed by column; several keys are ANDed. \
         A bare value means equality and null means IS NULL. \
         Operators: $eq $ne $gt $gte $lt $lte; $in $nin (array of values); \
         $like $ilike $regex (patterns); $contains $overlap $all $size (array columns); \
         $exists (boolean). \
         Logic: $and and $or take a non-empty array of filters, $not takes a filter. \
         Example: {\"status\": {\"$in\": [\"active\", \"pending\"]}, \"age\": {\"$gte\": 18}}. \
         Not every driver supports every operator."
    };
}

/// Schema description of the optional `where` parameter.
///
/// It lists only what `dbflux_core::parse_semantic_filter_json` accepts, and it
/// is sent on every tool listing, so it stays compact.
pub const WHERE_FILTER_DESCRIPTION: &str = where_filter_reference!();

/// Schema description of a `where` parameter that a mutation requires.
pub const REQUIRED_WHERE_FILTER_DESCRIPTION: &str =
    concat!("REQUIRED - cannot be empty. ", where_filter_reference!());

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
pub mod query;
pub mod read;
pub mod schema;
pub mod scripts;
pub mod write;

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
pub use write::{InsertRecordParams, UpdateRecordsParams, UpsertRecordParams};
pub use scripts::{
    CreateScriptParams, DeleteScriptParams, ExecuteScriptParams, GetScriptParams,
    ListScriptsParams, UpdateScriptParams, DELETE_CONFIRMATION_ERROR,
    validate_delete_params as validate_delete_script_params,
};
pub use approval::{
    ApproveExecutionParams, GetPendingExecutionParams, ListPendingExecutionsParams,
    RejectExecutionParams, RequestExecutionParams,
};
pub use audit::{
    ExportAuditLogsParams, GetAuditEntryParams, QueryAuditLogsParams,
};

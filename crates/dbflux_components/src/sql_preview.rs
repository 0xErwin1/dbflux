//! SQL preview types shared between the SQL preview modal and other UI surfaces.

use dbflux_core::{MutationRequest, TableInfo, Value};
use uuid::Uuid;

/// Type of SQL statement to generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlGenerationType {
    SelectAll,
    SelectWhere,
    Insert,
    Update,
    Delete,
    CreateTable,
    Truncate,
    DropTable,
}

impl SqlGenerationType {
    pub fn label(&self) -> &'static str {
        match self {
            SqlGenerationType::SelectAll => "SELECT *",
            SqlGenerationType::SelectWhere => "SELECT WHERE",
            SqlGenerationType::Insert => "INSERT",
            SqlGenerationType::Update => "UPDATE",
            SqlGenerationType::Delete => "DELETE",
            SqlGenerationType::CreateTable => "CREATE TABLE",
            SqlGenerationType::Truncate => "TRUNCATE",
            SqlGenerationType::DropTable => "DROP TABLE",
        }
    }

    /// Convert from driver generator_id to SqlGenerationType.
    /// Returns None for generator types we don't support in the preview modal.
    pub fn from_generator_id(id: &str) -> Option<Self> {
        match id {
            "select_star" => Some(SqlGenerationType::SelectAll),
            "select_where" => Some(SqlGenerationType::SelectWhere),
            "insert" => Some(SqlGenerationType::Insert),
            "update" => Some(SqlGenerationType::Update),
            "delete" => Some(SqlGenerationType::Delete),
            "create_table" => Some(SqlGenerationType::CreateTable),
            "truncate" => Some(SqlGenerationType::Truncate),
            "drop_table" => Some(SqlGenerationType::DropTable),
            _ => None,
        }
    }

    /// DDL operations don't support column selection or value options.
    pub fn is_ddl(&self) -> bool {
        matches!(
            self,
            SqlGenerationType::CreateTable
                | SqlGenerationType::Truncate
                | SqlGenerationType::DropTable
        )
    }

    /// Returns the driver generator_id for DDL operations.
    pub fn driver_generator_id(&self) -> Option<&'static str> {
        match self {
            SqlGenerationType::CreateTable => Some("create_table"),
            SqlGenerationType::Truncate => Some("truncate"),
            SqlGenerationType::DropTable => Some("drop_table"),
            _ => None,
        }
    }
}

/// Context for SQL generation — where the request came from.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum SqlPreviewContext {
    /// From data table: row data with values.
    DataTableRow {
        profile_id: Uuid,
        schema_name: Option<String>,
        table_name: String,
        column_names: Vec<String>,
        row_values: Vec<Value>,
        pk_indices: Vec<usize>,
    },
    /// From data table: a concrete mutation rendered by the driver.
    DataMutation {
        profile_id: Uuid,
        mutation: MutationRequest,
    },
    /// From sidebar: table metadata.
    SidebarTable {
        profile_id: Uuid,
        table_info: TableInfo,
    },
}

#[allow(dead_code)]
impl SqlPreviewContext {
    pub fn profile_id(&self) -> Uuid {
        match self {
            SqlPreviewContext::DataTableRow { profile_id, .. } => *profile_id,
            SqlPreviewContext::DataMutation { profile_id, .. } => *profile_id,
            SqlPreviewContext::SidebarTable { profile_id, .. } => *profile_id,
        }
    }

    pub fn table_name(&self) -> &str {
        match self {
            SqlPreviewContext::DataTableRow { table_name, .. } => table_name,
            SqlPreviewContext::DataMutation { mutation, .. } => match mutation {
                MutationRequest::SqlInsert(insert) => &insert.table,
                MutationRequest::SqlUpdate(patch) => &patch.table,
                MutationRequest::SqlDelete(delete) => &delete.table,
                _ => "",
            },
            SqlPreviewContext::SidebarTable { table_info, .. } => &table_info.name,
        }
    }

    pub fn schema_name(&self) -> Option<&str> {
        match self {
            SqlPreviewContext::DataTableRow { schema_name, .. } => schema_name.as_deref(),
            SqlPreviewContext::DataMutation { mutation, .. } => match mutation {
                MutationRequest::SqlInsert(insert) => insert.schema.as_deref(),
                MutationRequest::SqlUpdate(patch) => patch.schema.as_deref(),
                MutationRequest::SqlDelete(delete) => delete.schema.as_deref(),
                _ => None,
            },
            SqlPreviewContext::SidebarTable { table_info, .. } => table_info.schema.as_deref(),
        }
    }
}

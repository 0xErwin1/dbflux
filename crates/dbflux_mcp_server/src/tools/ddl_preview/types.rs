use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Result of a DDL preview operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlPreviewResult {
    /// SQL statements that would be executed.
    pub sql_statements: Vec<String>,

    /// Schema differences that would result from the DDL.
    pub schema_diff: SchemaDiff,

    /// Cascade impact analysis (objects affected by CASCADE operations).
    pub cascade_impact: CascadeImpact,

    /// Validation warnings and errors.
    pub validation: ValidationResult,

    /// Whether the DDL is safe to execute (no critical errors).
    pub is_safe: bool,

    /// Human-readable summary of the operation.
    pub summary: String,
}

/// Schema differences before and after DDL execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Tables that would be created.
    pub tables_created: Vec<TableDiffEntry>,

    /// Tables that would be dropped.
    pub tables_dropped: Vec<TableDiffEntry>,

    /// Tables that would be altered.
    pub tables_altered: Vec<TableAlterationEntry>,

    /// Columns that would be added.
    pub columns_added: Vec<ColumnDiffEntry>,

    /// Columns that would be dropped.
    pub columns_dropped: Vec<ColumnDiffEntry>,

    /// Columns that would be modified.
    pub columns_modified: Vec<ColumnModificationEntry>,

    /// Indexes that would be created.
    pub indexes_created: Vec<IndexDiffEntry>,

    /// Indexes that would be dropped.
    pub indexes_dropped: Vec<IndexDiffEntry>,

    /// Foreign keys that would be added.
    pub foreign_keys_added: Vec<ForeignKeyDiffEntry>,

    /// Foreign keys that would be dropped.
    pub foreign_keys_dropped: Vec<ForeignKeyDiffEntry>,

    /// Constraints that would be added.
    pub constraints_added: Vec<ConstraintDiffEntry>,

    /// Constraints that would be dropped.
    pub constraints_dropped: Vec<ConstraintDiffEntry>,
}

impl SchemaDiff {
    /// Create an empty schema diff.
    pub fn empty() -> Self {
        Self {
            tables_created: Vec::new(),
            tables_dropped: Vec::new(),
            tables_altered: Vec::new(),
            columns_added: Vec::new(),
            columns_dropped: Vec::new(),
            columns_modified: Vec::new(),
            indexes_created: Vec::new(),
            indexes_dropped: Vec::new(),
            foreign_keys_added: Vec::new(),
            foreign_keys_dropped: Vec::new(),
            constraints_added: Vec::new(),
            constraints_dropped: Vec::new(),
        }
    }

    /// Check if the diff is empty (no changes).
    pub fn is_empty(&self) -> bool {
        self.tables_created.is_empty()
            && self.tables_dropped.is_empty()
            && self.tables_altered.is_empty()
            && self.columns_added.is_empty()
            && self.columns_dropped.is_empty()
            && self.columns_modified.is_empty()
            && self.indexes_created.is_empty()
            && self.indexes_dropped.is_empty()
            && self.foreign_keys_added.is_empty()
            && self.foreign_keys_dropped.is_empty()
            && self.constraints_added.is_empty()
            && self.constraints_dropped.is_empty()
    }
}

/// Table entry in schema diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDiffEntry {
    pub schema: Option<String>,
    pub name: String,
    pub column_count: usize,
}

/// Table alteration entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableAlterationEntry {
    pub schema: Option<String>,
    pub name: String,
    pub changes: Vec<String>,
}

/// Column entry in schema diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDiffEntry {
    pub table_schema: Option<String>,
    pub table_name: String,
    pub column_name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// Column modification entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnModificationEntry {
    pub table_schema: Option<String>,
    pub table_name: String,
    pub column_name: String,
    pub old_type: String,
    pub new_type: String,
    pub old_nullable: bool,
    pub new_nullable: bool,
}

/// Index entry in schema diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDiffEntry {
    pub schema: Option<String>,
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

/// Foreign key entry in schema diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyDiffEntry {
    pub schema: Option<String>,
    pub table_name: String,
    pub constraint_name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

/// Constraint entry in schema diff.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintDiffEntry {
    pub schema: Option<String>,
    pub table_name: String,
    pub constraint_name: String,
    pub constraint_type: String,
    pub definition: String,
}

/// Cascade impact analysis result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CascadeImpact {
    /// Objects that would be affected by CASCADE operations.
    pub affected_objects: Vec<AffectedObject>,

    /// Whether CASCADE operations are present.
    pub has_cascade: bool,

    /// Estimated number of rows affected (if available).
    pub estimated_row_impact: Option<usize>,
}

impl CascadeImpact {
    /// Create an empty cascade impact.
    pub fn empty() -> Self {
        Self {
            affected_objects: Vec::new(),
            has_cascade: false,
            estimated_row_impact: None,
        }
    }
}

/// Object affected by CASCADE operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffectedObject {
    pub object_type: String,
    pub schema: Option<String>,
    pub name: String,
    pub reason: String,
}

/// Validation result with warnings and errors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Critical errors that prevent execution.
    pub errors: Vec<ValidationMessage>,

    /// Warnings that should be reviewed.
    pub warnings: Vec<ValidationMessage>,

    /// Informational messages.
    pub info: Vec<ValidationMessage>,
}

impl ValidationResult {
    /// Create an empty validation result.
    pub fn empty() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            info: Vec::new(),
        }
    }

    /// Check if there are any errors.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Add an error message.
    pub fn add_error(&mut self, message: impl Into<String>) {
        self.errors.push(ValidationMessage {
            message: message.into(),
            location: None,
        });
    }

    /// Add a warning message.
    pub fn add_warning(&mut self, message: impl Into<String>) {
        self.warnings.push(ValidationMessage {
            message: message.into(),
            location: None,
        });
    }

    /// Add an info message.
    pub fn add_info(&mut self, message: impl Into<String>) {
        self.info.push(ValidationMessage {
            message: message.into(),
            location: None,
        });
    }
}

/// A validation message with optional location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationMessage {
    pub message: String,
    pub location: Option<String>,
}

/// Snapshot of schema state before and after DDL.
#[derive(Debug, Clone)]
pub struct SchemaStateSnapshot {
    pub tables: HashMap<String, TableSnapshot>,
    pub indexes: HashMap<String, IndexSnapshot>,
    pub foreign_keys: HashMap<String, ForeignKeySnapshot>,
}

impl SchemaStateSnapshot {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            foreign_keys: HashMap::new(),
        }
    }
}

/// Snapshot of a table's structure.
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    pub schema: Option<String>,
    pub name: String,
    pub columns: Vec<ColumnSnapshot>,
}

/// Snapshot of a column's structure.
#[derive(Debug, Clone)]
pub struct ColumnSnapshot {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
}

/// Snapshot of an index.
#[derive(Debug, Clone)]
pub struct IndexSnapshot {
    pub schema: Option<String>,
    pub table_name: String,
    pub index_name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
}

/// Snapshot of a foreign key.
#[derive(Debug, Clone)]
pub struct ForeignKeySnapshot {
    pub schema: Option<String>,
    pub table_name: String,
    pub constraint_name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_diff_empty() {
        let diff = SchemaDiff::empty();
        assert!(diff.is_empty());
    }

    #[test]
    fn test_schema_diff_not_empty() {
        let mut diff = SchemaDiff::empty();
        diff.tables_created.push(TableDiffEntry {
            schema: None,
            name: "users".to_string(),
            column_count: 3,
        });
        assert!(!diff.is_empty());
    }

    #[test]
    fn test_validation_result_empty() {
        let validation = ValidationResult::empty();
        assert!(!validation.has_errors());
        assert_eq!(validation.errors.len(), 0);
        assert_eq!(validation.warnings.len(), 0);
    }

    #[test]
    fn test_validation_result_with_error() {
        let mut validation = ValidationResult::empty();
        validation.add_error("Test error");
        assert!(validation.has_errors());
        assert_eq!(validation.errors.len(), 1);
        assert_eq!(validation.errors[0].message, "Test error");
    }

    #[test]
    fn test_cascade_impact_empty() {
        let impact = CascadeImpact::empty();
        assert!(!impact.has_cascade);
        assert_eq!(impact.affected_objects.len(), 0);
    }
}

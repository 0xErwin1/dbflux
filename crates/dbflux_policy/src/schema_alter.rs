use crate::classification::ExecutionClassification;

/// Transport/representation-neutral description of an ALTER-family schema
/// change. Both `dbflux_mcp_server`'s `AlterOperation` and `dbflux_core`'s
/// diff `SchemaChange` map onto this enum before classification, guaranteeing
/// both sides agree on risk for equivalent operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaAlterKind {
    AddColumn { safe: bool },
    DropColumn,
    RenameColumn,
    AlterColumn,
    AddConstraint,
    DropConstraint,
    AddIndex,
    DropIndex,
    AddTable,
    DropTable,
}

/// Classify a schema-alter operation kind into its governance risk level.
///
/// This is the single source of truth for the ALTER TABLE risk ladder,
/// previously duplicated as MCP-local logic in `dbflux_mcp_server`.
pub fn classify_schema_alter(kind: SchemaAlterKind) -> ExecutionClassification {
    match kind {
        SchemaAlterKind::AddColumn { safe: true } => ExecutionClassification::AdminSafe,
        SchemaAlterKind::AddColumn { safe: false } => ExecutionClassification::Admin,
        SchemaAlterKind::DropColumn => ExecutionClassification::AdminDestructive,
        SchemaAlterKind::RenameColumn => ExecutionClassification::AdminSafe,
        SchemaAlterKind::AlterColumn => ExecutionClassification::Admin,
        SchemaAlterKind::AddConstraint => ExecutionClassification::Admin,
        SchemaAlterKind::DropConstraint => ExecutionClassification::AdminDestructive,
        SchemaAlterKind::AddIndex => ExecutionClassification::AdminSafe,
        SchemaAlterKind::DropIndex => ExecutionClassification::Admin,
        SchemaAlterKind::AddTable => ExecutionClassification::AdminSafe,
        SchemaAlterKind::DropTable => ExecutionClassification::AdminDestructive,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_column_safe_is_admin_safe() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::AddColumn { safe: true }),
            ExecutionClassification::AdminSafe
        );
    }

    #[test]
    fn add_column_unsafe_is_admin() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::AddColumn { safe: false }),
            ExecutionClassification::Admin
        );
    }

    #[test]
    fn drop_column_is_admin_destructive() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::DropColumn),
            ExecutionClassification::AdminDestructive
        );
    }

    #[test]
    fn rename_column_is_admin_safe() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::RenameColumn),
            ExecutionClassification::AdminSafe
        );
    }

    #[test]
    fn alter_column_is_admin() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::AlterColumn),
            ExecutionClassification::Admin
        );
    }

    #[test]
    fn add_constraint_is_admin() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::AddConstraint),
            ExecutionClassification::Admin
        );
    }

    #[test]
    fn drop_constraint_is_admin_destructive() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::DropConstraint),
            ExecutionClassification::AdminDestructive
        );
    }

    #[test]
    fn add_index_is_admin_safe() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::AddIndex),
            ExecutionClassification::AdminSafe
        );
    }

    #[test]
    fn drop_index_is_admin() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::DropIndex),
            ExecutionClassification::Admin
        );
    }

    #[test]
    fn add_table_is_admin_safe() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::AddTable),
            ExecutionClassification::AdminSafe
        );
    }

    #[test]
    fn drop_table_is_admin_destructive() {
        assert_eq!(
            classify_schema_alter(SchemaAlterKind::DropTable),
            ExecutionClassification::AdminDestructive
        );
    }
}

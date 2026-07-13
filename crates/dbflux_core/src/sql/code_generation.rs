use bitflags::bitflags;
use serde::{Deserialize, Serialize};

bitflags! {
    /// DDL operations supported by a driver.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct CodeGenCapabilities: u32 {
        // Index operations
        const CREATE_INDEX = 1 << 0;
        const DROP_INDEX = 1 << 1;
        const REINDEX = 1 << 2;

        // Foreign key operations
        const ADD_FOREIGN_KEY = 1 << 3;
        const DROP_FOREIGN_KEY = 1 << 4;

        // Custom type operations (PostgreSQL)
        const CREATE_TYPE = 1 << 5;
        const DROP_TYPE = 1 << 6;
        const ALTER_TYPE = 1 << 7;

        // Basic CRUD
        const SELECT = 1 << 8;
        const INSERT = 1 << 9;
        const UPDATE = 1 << 10;
        const DELETE = 1 << 11;

        // Table operations
        const CREATE_TABLE = 1 << 12;
        const DROP_TABLE = 1 << 13;
        const ALTER_TABLE = 1 << 14;

        // Column-level ALTER operations
        const ADD_COLUMN = 1 << 15;
        const DROP_COLUMN = 1 << 16;
        const ALTER_COLUMN = 1 << 17;

        // Common combinations
        const INDEXES = Self::CREATE_INDEX.bits() | Self::DROP_INDEX.bits();
        const FOREIGN_KEYS = Self::ADD_FOREIGN_KEY.bits() | Self::DROP_FOREIGN_KEY.bits();
        const CRUD = Self::SELECT.bits() | Self::INSERT.bits() | Self::UPDATE.bits() | Self::DELETE.bits();

        // SQL databases typically support all of these
        const SQL_FULL = Self::CRUD.bits()
            | Self::INDEXES.bits()
            | Self::REINDEX.bits()
            | Self::FOREIGN_KEYS.bits()
            | Self::CREATE_TABLE.bits()
            | Self::DROP_TABLE.bits()
            | Self::ALTER_TABLE.bits();

        // PostgreSQL adds custom types
        const POSTGRES_FULL = Self::SQL_FULL.bits()
            | Self::CREATE_TYPE.bits()
            | Self::DROP_TYPE.bits()
            | Self::ALTER_TYPE.bits();
    }
}

impl Serialize for CodeGenCapabilities {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.bits().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CodeGenCapabilities {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bits = u32::deserialize(deserializer)?;
        Ok(Self::from_bits(bits).unwrap_or_else(Self::empty))
    }
}

// =============================================================================
// Request Types
// =============================================================================

#[derive(Debug, Clone)]
pub struct CreateIndexRequest<'a> {
    pub index_name: &'a str,
    pub table_name: &'a str,
    pub schema_name: Option<&'a str>,
    pub columns: &'a [String],
    pub unique: bool,
}

#[derive(Debug, Clone)]
pub struct DropIndexRequest<'a> {
    pub index_name: &'a str,
    pub table_name: Option<&'a str>,
    pub schema_name: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct ReindexRequest<'a> {
    pub index_name: &'a str,
    pub schema_name: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct AddForeignKeyRequest<'a> {
    pub constraint_name: &'a str,
    pub table_name: &'a str,
    pub schema_name: Option<&'a str>,
    pub columns: &'a [String],
    pub ref_table: &'a str,
    pub ref_schema: Option<&'a str>,
    pub ref_columns: &'a [String],
    pub on_delete: Option<&'a str>,
    pub on_update: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct DropForeignKeyRequest<'a> {
    pub constraint_name: &'a str,
    pub table_name: &'a str,
    pub schema_name: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeAttributeDefinition {
    pub name: String,
    pub type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeDefinition {
    Enum {
        values: Vec<String>,
    },
    Domain {
        base_type: String,
    },
    Composite {
        attributes: Vec<TypeAttributeDefinition>,
    },
}

#[derive(Debug, Clone)]
pub struct CreateTypeRequest<'a> {
    pub type_name: &'a str,
    pub schema_name: Option<&'a str>,
    pub definition: TypeDefinition,
}

#[derive(Debug, Clone)]
pub struct DropTypeRequest<'a> {
    pub type_name: &'a str,
    pub schema_name: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct AddEnumValueRequest<'a> {
    pub type_name: &'a str,
    pub schema_name: Option<&'a str>,
    pub new_value: &'a str,
}

// =============================================================================
// Column ALTER Request Types
// =============================================================================

#[derive(Debug, Clone)]
pub struct AddColumnRequest<'a> {
    pub table_name: &'a str,
    pub schema_name: Option<&'a str>,
    pub column_name: &'a str,
    pub type_name: &'a str,
    pub nullable: bool,
    /// Raw SQL default expression/literal (already formatted by the caller).
    pub default: Option<&'a str>,
}

#[derive(Debug, Clone)]
pub struct DropColumnRequest<'a> {
    pub table_name: &'a str,
    pub schema_name: Option<&'a str>,
    pub column_name: &'a str,
}

/// The desired state of a column's default when altering it.
///
/// Distinct from a plain `Option<&str>` because a column-default alter has
/// three states: leave the default untouched (represented by
/// `AlterColumnRequest.default` itself being `None`), drop it, or set it to
/// a new raw SQL expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultSpec<'a> {
    Drop,
    Set(&'a str),
}

#[derive(Debug, Clone)]
pub struct AlterColumnRequest<'a> {
    pub table_name: &'a str,
    pub schema_name: Option<&'a str>,
    pub column_name: &'a str,
    pub new_type: Option<&'a str>,
    pub nullable: Option<bool>,
    pub default: Option<DefaultSpec<'a>>,
}

/// A driver's explanation for why it cannot generate DDL for a requested
/// column change, carried instead of a silent `None` so the UI can surface
/// the exact limitation to the user (see DBF-24 decision: column DDL reject
/// semantics).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlRejection {
    pub reason: String,
    pub followup: Option<&'static str>,
}

impl DdlRejection {
    pub fn unsupported() -> Self {
        Self {
            reason: "Operation not supported by this driver".to_string(),
            followup: None,
        }
    }
}

/// SQL fragments (a column type or a raw default expression) that a driver
/// interpolates verbatim into generated DDL cannot be blanket-quoted —
/// `VARCHAR(255)`, `now()` and `nextval('seq')` are all legitimate. To keep
/// that seam from becoming a DDL-injection vector, a fragment must not carry a
/// statement terminator or comment sequence that would let it smuggle a second
/// statement past the generator. Any of `;`, `--`, `/*` or `*/` is rejected.
///
/// This is the single shared gate every driver and the MCP DDL re-route call,
/// so a crafted source-schema default such as `0; DROP TABLE x; --` cannot be
/// emitted through any per-driver path.
pub fn validate_ddl_fragment(value: &str, field: &str) -> Result<(), DdlRejection> {
    const FORBIDDEN: [&str; 4] = [";", "--", "/*", "*/"];

    if let Some(marker) = FORBIDDEN.iter().find(|marker| value.contains(*marker)) {
        return Err(DdlRejection {
            reason: format!(
                "{field} contains a disallowed SQL sequence ({marker:?}); \
                 statement terminators and comment markers are rejected to prevent DDL injection"
            ),
            followup: None,
        });
    }

    Ok(())
}

// =============================================================================
// CodeGenerator Trait
// =============================================================================

/// Trait for generating database-specific DDL.
pub trait CodeGenerator: Send + Sync {
    fn capabilities(&self) -> CodeGenCapabilities;

    fn supports(&self, cap: CodeGenCapabilities) -> bool {
        self.capabilities().contains(cap)
    }

    // =========================================================================
    // Index Operations
    // =========================================================================

    fn generate_create_index(&self, _request: &CreateIndexRequest) -> Option<String> {
        None
    }

    fn generate_drop_index(&self, _request: &DropIndexRequest) -> Option<String> {
        None
    }

    fn generate_reindex(&self, _request: &ReindexRequest) -> Option<String> {
        None
    }

    // =========================================================================
    // Foreign Key Operations
    // =========================================================================

    fn generate_add_foreign_key(&self, _request: &AddForeignKeyRequest) -> Option<String> {
        None
    }

    fn generate_drop_foreign_key(&self, _request: &DropForeignKeyRequest) -> Option<String> {
        None
    }

    // =========================================================================
    // Custom Type Operations
    // =========================================================================

    fn generate_create_type(&self, _request: &CreateTypeRequest) -> Option<String> {
        None
    }

    fn generate_drop_type(&self, _request: &DropTypeRequest) -> Option<String> {
        None
    }

    fn generate_add_enum_value(&self, _request: &AddEnumValueRequest) -> Option<String> {
        None
    }

    // =========================================================================
    // Column ALTER Operations
    // =========================================================================

    fn generate_add_column(
        &self,
        _request: &AddColumnRequest,
    ) -> Result<Vec<String>, DdlRejection> {
        Err(DdlRejection::unsupported())
    }

    fn generate_drop_column(
        &self,
        _request: &DropColumnRequest,
    ) -> Result<Vec<String>, DdlRejection> {
        Err(DdlRejection::unsupported())
    }

    fn generate_alter_column(
        &self,
        _request: &AlterColumnRequest,
    ) -> Result<Vec<String>, DdlRejection> {
        Err(DdlRejection::unsupported())
    }
}

/// Code generator that returns `None` for all operations.
pub struct NoOpCodeGenerator;

impl CodeGenerator for NoOpCodeGenerator {
    fn capabilities(&self) -> CodeGenCapabilities {
        CodeGenCapabilities::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_include_the_three_new_column_alter_bits() {
        let bits = CodeGenCapabilities::ADD_COLUMN
            | CodeGenCapabilities::DROP_COLUMN
            | CodeGenCapabilities::ALTER_COLUMN;

        assert!(bits.contains(CodeGenCapabilities::ADD_COLUMN));
        assert!(bits.contains(CodeGenCapabilities::DROP_COLUMN));
        assert!(bits.contains(CodeGenCapabilities::ALTER_COLUMN));
        assert_ne!(
            CodeGenCapabilities::ADD_COLUMN,
            CodeGenCapabilities::ALTER_TABLE
        );
    }

    #[test]
    fn ddl_rejection_unsupported_has_no_followup() {
        let rejection = DdlRejection::unsupported();

        assert_eq!(rejection.reason, "Operation not supported by this driver");
        assert_eq!(rejection.followup, None);
    }

    #[test]
    fn ddl_rejection_can_carry_a_named_followup() {
        let rejection = DdlRejection {
            reason: "SQLite requires a table rebuild".to_string(),
            followup: Some("DBF-158"),
        };

        assert_eq!(rejection.followup, Some("DBF-158"));
    }

    #[test]
    fn default_generate_add_column_rejects_as_unsupported() {
        let generator = NoOpCodeGenerator;
        let request = AddColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "age",
            type_name: "INTEGER",
            nullable: true,
            default: None,
        };

        let result = generator.generate_add_column(&request);

        assert_eq!(result, Err(DdlRejection::unsupported()));
    }

    #[test]
    fn default_generate_drop_column_rejects_as_unsupported() {
        let generator = NoOpCodeGenerator;
        let request = DropColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "age",
        };

        let result = generator.generate_drop_column(&request);

        assert_eq!(result, Err(DdlRejection::unsupported()));
    }

    #[test]
    fn validate_ddl_fragment_accepts_legitimate_types_and_defaults() {
        for value in [
            "VARCHAR(255)",
            "now()",
            "nextval('users_id_seq')",
            "BIGINT",
            "0",
        ] {
            assert!(
                validate_ddl_fragment(value, "type").is_ok(),
                "expected {value:?} to pass validation"
            );
        }
    }

    #[test]
    fn validate_ddl_fragment_rejects_statement_terminator() {
        let result = validate_ddl_fragment("0; DROP TABLE x", "default");
        assert!(
            result.is_err(),
            "expected a stacked statement to be rejected"
        );
    }

    #[test]
    fn validate_ddl_fragment_rejects_comment_sequences() {
        assert!(validate_ddl_fragment("TEXT -- inject", "type").is_err());
        assert!(validate_ddl_fragment("TEXT /* inject", "type").is_err());
        assert!(validate_ddl_fragment("TEXT */ inject", "type").is_err());
    }

    #[test]
    fn default_generate_alter_column_rejects_as_unsupported() {
        let generator = NoOpCodeGenerator;
        let request = AlterColumnRequest {
            table_name: "users",
            schema_name: None,
            column_name: "age",
            new_type: Some("BIGINT"),
            nullable: None,
            default: Some(DefaultSpec::Set("0")),
        };

        let result = generator.generate_alter_column(&request);

        assert_eq!(result, Err(DdlRejection::unsupported()));
    }
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TypeDefinition {
    Enum { values: Vec<String> },
    Domain { base_type: String },
    Composite,
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
}

/// Code generator that returns `None` for all operations.
pub struct NoOpCodeGenerator;

impl CodeGenerator for NoOpCodeGenerator {
    fn capabilities(&self) -> CodeGenCapabilities {
        CodeGenCapabilities::empty()
    }
}

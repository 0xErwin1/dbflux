pub(crate) mod code_generation;
pub(crate) mod dialect;
pub(crate) mod generation;
pub(crate) mod query_builder;

pub use code_generation::{
    AddEnumValueRequest, AddForeignKeyRequest, CodeGenCapabilities, CodeGenerator,
    CreateIndexRequest, CreateTypeRequest, DropForeignKeyRequest, DropIndexRequest,
    DropTypeRequest, NoOpCodeGenerator, ReindexRequest, TypeAttributeDefinition, TypeDefinition,
};
pub use dialect::{DefaultSqlDialect, PlaceholderStyle, SqlDialect};
pub use generation::{
    SqlGenerationOptions, SqlGenerationRequest, SqlOperation, SqlValueMode, generate_create_table,
    generate_delete_template, generate_drop_table, generate_insert_template, generate_select_star,
    generate_sql, generate_truncate, generate_update_template,
};
pub use query_builder::SqlQueryBuilder;

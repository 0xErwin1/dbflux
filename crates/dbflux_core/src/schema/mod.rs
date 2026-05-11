pub(crate) mod builder;
pub mod dependents;
pub mod drift_check;
pub mod fingerprint;
pub mod node_id;
pub mod query_parser;
pub mod schema_drift;
pub(crate) mod types;

pub use builder::{ForeignKeyBuilder, IndexBuilder, SchemaForeignKeyBuilder, SchemaIndexBuilder};
pub use dependents::{RelationKind, RelationRef};
pub use drift_check::{DriftOutcome, check_drift_sync, check_schema_drift};
pub use fingerprint::SchemaFingerprint;
pub use node_id::{ParseSchemaNodeIdError, SchemaNodeId, SchemaNodeKind};
pub use query_parser::{QueryTableRef, extract_referenced_tables};
pub use schema_drift::{
    ColumnDiff, ColumnSnapshot, SchemaChange, SchemaDiff, SchemaDriftDetected, diff_table_info,
};
pub use types::{
    CollectionChildInfo, CollectionChildrenCache, CollectionChildrenPage,
    CollectionChildrenRequest, CollectionIndexInfo, CollectionInfo, CollectionPresentation,
    ColumnFamilyInfo, ColumnInfo, ConstraintInfo, ConstraintKind, ContainerInfo, CustomTypeInfo,
    CustomTypeKind, DataStructure, DatabaseInfo, DbSchemaInfo, DocumentSchema, FieldInfo,
    ForeignKeyInfo, GraphInfo, GraphSchema, IndexData, IndexDirection, IndexInfo, KeyInfo,
    KeySpaceInfo, KeyValueSchema, MeasurementInfo, MultiModelCapabilities, MultiModelSchema,
    NodeLabelInfo, PropertyInfo, RelationalSchema, RelationshipTypeInfo, RetentionPolicyInfo,
    SchemaForeignKeyInfo, SchemaIndexInfo, SchemaSnapshot, SearchIndexInfo, SearchMappingInfo,
    SearchSchema, TableInfo, TimeSeriesFieldInfo, TimeSeriesSchema, VectorCollectionInfo,
    VectorMetadataField, VectorMetric, VectorSchema, ViewInfo, WideColumnInfo,
    WideColumnKeyspaceInfo, WideColumnSchema,
};

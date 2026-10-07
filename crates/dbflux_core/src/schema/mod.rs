pub(crate) mod builder;
pub(crate) mod dependency_order;
pub(crate) mod dependents;
pub(crate) mod drift_check;
pub(crate) mod fingerprint;
pub mod node_id;
pub(crate) mod query_parser;
pub(crate) mod schema_drift;
pub(crate) mod snapshot_record;
pub(crate) mod types;

pub use builder::{ForeignKeyBuilder, IndexBuilder, SchemaForeignKeyBuilder, SchemaIndexBuilder};
pub use dependency_order::{OrderResult, topological_order};
pub use dependents::{RelationKind, RelationRef};
pub use drift_check::{DriftOutcome, TableKey, check_drift_sync, check_schema_drift};
pub use fingerprint::SchemaFingerprint;
pub use node_id::{ParseSchemaNodeIdError, SchemaNodeId, SchemaNodeKind};
pub use query_parser::{QueryTableRef, extract_referenced_tables};
pub use schema_drift::{
    ColumnDiff, ColumnSnapshot, IndexSnapshot, RiskedChange, SchemaChange, SchemaDiff,
    SchemaDriftDetected, TableAlterNormalizationError, TableChange, classify_table_added,
    classify_table_removed, diff_schema, diff_table_info, normalize_selected_table_alter,
};
pub use snapshot_record::{SchemaSnapshotRecord, SnapshotDepth};
pub use types::{
    CollectionChildInfo, CollectionChildrenCache, CollectionChildrenPage,
    CollectionChildrenRequest, CollectionIndexInfo, CollectionInfo, CollectionPresentation,
    ColumnFamilyInfo, ColumnInfo, ConstraintInfo, ConstraintKind, ContainerInfo, CreationBlocker,
    CustomTypeInfo, CustomTypeKind, DataStructure, DatabaseInfo, DbSchemaInfo, DocumentSchema,
    FieldInfo, ForeignKeyInfo, GraphInfo, GraphSchema, IdentitySpec, IndexData, IndexDirection,
    IndexInfo, KeyInfo, KeySpaceInfo, KeyValueSchema, MeasurementInfo, MetadataCompleteness,
    MissingCreationMetadata, MultiModelCapabilities, MultiModelSchema, NodeLabelInfo,
    PrimaryKeySpec, PropertyInfo, RelationalSchema, RelationshipTypeInfo, RetentionPolicyInfo,
    RoutineInfo, RoutineKind, SchemaColumnInfo, SchemaForeignKeyInfo, SchemaIndexInfo,
    SchemaSnapshot, SearchIndexInfo, SearchMappingInfo, SearchSchema, TableCreationMetadata,
    TableInfo, TableStorageHint, TimeSeriesFieldInfo, TimeSeriesSchema, VectorCollectionInfo,
    VectorMetadataField, VectorMetric, VectorSchema, ViewInfo, WideColumnInfo,
    WideColumnKeyspaceInfo, WideColumnSchema,
};

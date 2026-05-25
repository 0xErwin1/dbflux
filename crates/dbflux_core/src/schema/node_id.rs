use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

/// Typed representation of a sidebar tree node ID.
///
/// Every node in the sidebar schema tree has a string ID that encodes its type
/// and parentage. `SchemaNodeId` replaces the fragile prefix-based string parsing
/// with a typed enum that can be constructed, matched, and round-tripped via
/// `Display`/`FromStr`.
///
/// Encoding uses pipe (`|`) as the separator since it cannot appear in database
/// identifiers, unlike underscore which is common in table/schema names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchemaNodeId {
    ConnectionFolder {
        node_id: Uuid,
    },
    Profile {
        profile_id: Uuid,
    },
    Database {
        profile_id: Uuid,
        name: String,
    },
    Loading {
        profile_id: Uuid,
        database: String,
    },
    Schema {
        profile_id: Uuid,
        name: String,
    },

    // Folder variants
    TablesFolder {
        profile_id: Uuid,
        schema: String,
    },
    ViewsFolder {
        profile_id: Uuid,
        schema: String,
    },
    TypesFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    TypesLoadingFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    SchemaIndexesFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    SchemaIndexesLoadingFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    SchemaForeignKeysFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    SchemaForeignKeysLoadingFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    RoutinesFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    RoutinesLoadingFolder {
        profile_id: Uuid,
        database: String,
        schema: String,
    },
    CollectionsFolder {
        profile_id: Uuid,
        database: String,
    },

    /// Root folder for the metric catalog under a CloudWatch connection.
    /// Rendered as a sibling of "Collections" when `METRIC_CATALOG` capability is set.
    MetricsFolder {
        profile_id: Uuid,
        database: String,
    },
    /// Expandable folder for a single CloudWatch namespace (e.g. `AWS/EC2`).
    MetricNamespaceFolder {
        profile_id: Uuid,
        database: String,
        namespace: String,
    },
    /// Clickable leaf for a single CloudWatch metric.
    /// Clicking opens a ChartDocument pre-populated with this metric's defaults.
    MetricLeaf {
        profile_id: Uuid,
        database: String,
        namespace: String,
        metric_name: String,
    },

    /// Root folder for dashboards under a connection profile.
    /// Always visible, regardless of driver capabilities.
    DashboardsFolder {
        profile_id: Uuid,
    },
    /// Clickable item for a single saved dashboard.
    DashboardItem {
        profile_id: Uuid,
        dashboard_id: Uuid,
    },
    /// Root folder for saved charts under a connection profile.
    /// Always visible, regardless of driver capabilities.
    SavedChartsFolder {
        profile_id: Uuid,
    },
    /// Clickable item for a single saved chart.
    SavedChartItem {
        profile_id: Uuid,
        chart_id: Uuid,
    },

    // Object variants
    Table {
        profile_id: Uuid,
        database: Option<String>,
        schema: String,
        name: String,
    },
    View {
        profile_id: Uuid,
        database: Option<String>,
        schema: String,
        name: String,
    },
    Collection {
        profile_id: Uuid,
        database: String,
        name: String,
    },
    CollectionChild {
        profile_id: Uuid,
        database: String,
        collection: String,
        child_id: String,
        name: String,
    },
    CollectionChildrenMore {
        profile_id: Uuid,
        database: String,
        collection: String,
    },
    CustomType {
        profile_id: Uuid,
        schema: String,
        name: String,
    },

    // Table detail folder variants
    ColumnsFolder {
        profile_id: Uuid,
        schema: String,
        table: String,
    },
    IndexesFolder {
        profile_id: Uuid,
        schema: String,
        table: String,
    },
    ForeignKeysFolder {
        profile_id: Uuid,
        schema: String,
        table: String,
    },
    ConstraintsFolder {
        profile_id: Uuid,
        schema: String,
        table: String,
    },

    // Detail variants
    Column {
        profile_id: Uuid,
        table: String,
        name: String,
    },
    Index {
        profile_id: Uuid,
        table: String,
        name: String,
    },
    ForeignKey {
        profile_id: Uuid,
        table: String,
        name: String,
    },
    Constraint {
        profile_id: Uuid,
        table: String,
        name: String,
    },
    SchemaIndex {
        profile_id: Uuid,
        schema: String,
        name: String,
    },
    SchemaForeignKey {
        profile_id: Uuid,
        schema: String,
        name: String,
    },
    Routine {
        profile_id: Uuid,
        schema: String,
        /// Engine-specific unique identity (name + argument signature).
        /// Uses `specific_name` from `RoutineInfo` to distinguish overloads.
        specific_name: String,
    },

    // Collection detail variants
    DatabaseIndexesFolder {
        profile_id: Uuid,
        database: String,
    },
    CollectionFieldsFolder {
        profile_id: Uuid,
        database: String,
        collection: String,
    },
    CollectionField {
        profile_id: Uuid,
        collection: String,
        name: String,
    },
    CollectionIndexesFolder {
        profile_id: Uuid,
        database: String,
        collection: String,
    },
    CollectionIndex {
        profile_id: Uuid,
        collection: String,
        name: String,
    },

    // Type detail variants
    EnumValue {
        profile_id: Uuid,
        schema: String,
        type_name: String,
        value: String,
    },
    BaseType {
        profile_id: Uuid,
        schema: String,
        type_name: String,
    },

    // Placeholder
    Placeholder {
        profile_id: Uuid,
        schema: String,
        table: String,
    },

    // Dependents disclosure (views, FK children, triggers that use a table)
    DependentsFolder {
        profile_id: Uuid,
        schema: String,
        table: String,
    },
    DependentItem {
        profile_id: Uuid,
        schema: String,
        table: String,
        /// The `qualified_name` of the `RelationRef`.
        name: String,
    },

    // Scripts section (not connection-bound)
    ScriptsFolder {
        path: Option<String>,
    },
    ScriptFile {
        path: String,
    },
}

/// Simple kind enum for cheap matching without data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaNodeKind {
    ConnectionFolder,
    Profile,
    Database,
    Loading,
    Schema,
    TablesFolder,
    ViewsFolder,
    TypesFolder,
    TypesLoadingFolder,
    SchemaIndexesFolder,
    SchemaIndexesLoadingFolder,
    SchemaForeignKeysFolder,
    SchemaForeignKeysLoadingFolder,
    RoutinesFolder,
    RoutinesLoadingFolder,
    CollectionsFolder,
    MetricsFolder,
    MetricNamespaceFolder,
    MetricLeaf,
    DashboardsFolder,
    DashboardItem,
    SavedChartsFolder,
    SavedChartItem,
    Table,
    View,
    Collection,
    CollectionChild,
    CollectionChildrenMore,
    CustomType,
    ColumnsFolder,
    IndexesFolder,
    ForeignKeysFolder,
    ConstraintsFolder,
    Column,
    Index,
    ForeignKey,
    Constraint,
    SchemaIndex,
    SchemaForeignKey,
    Routine,
    DatabaseIndexesFolder,
    CollectionFieldsFolder,
    CollectionField,
    CollectionIndexesFolder,
    CollectionIndex,
    EnumValue,
    BaseType,
    Placeholder,
    DependentsFolder,
    DependentItem,
    ScriptsFolder,
    ScriptFile,
}

impl SchemaNodeId {
    pub fn kind(&self) -> SchemaNodeKind {
        match self {
            Self::ConnectionFolder { .. } => SchemaNodeKind::ConnectionFolder,
            Self::Profile { .. } => SchemaNodeKind::Profile,
            Self::Database { .. } => SchemaNodeKind::Database,
            Self::Loading { .. } => SchemaNodeKind::Loading,
            Self::Schema { .. } => SchemaNodeKind::Schema,
            Self::TablesFolder { .. } => SchemaNodeKind::TablesFolder,
            Self::ViewsFolder { .. } => SchemaNodeKind::ViewsFolder,
            Self::TypesFolder { .. } => SchemaNodeKind::TypesFolder,
            Self::TypesLoadingFolder { .. } => SchemaNodeKind::TypesLoadingFolder,
            Self::SchemaIndexesFolder { .. } => SchemaNodeKind::SchemaIndexesFolder,
            Self::SchemaIndexesLoadingFolder { .. } => SchemaNodeKind::SchemaIndexesLoadingFolder,
            Self::SchemaForeignKeysFolder { .. } => SchemaNodeKind::SchemaForeignKeysFolder,
            Self::SchemaForeignKeysLoadingFolder { .. } => {
                SchemaNodeKind::SchemaForeignKeysLoadingFolder
            }
            Self::RoutinesFolder { .. } => SchemaNodeKind::RoutinesFolder,
            Self::RoutinesLoadingFolder { .. } => SchemaNodeKind::RoutinesLoadingFolder,
            Self::CollectionsFolder { .. } => SchemaNodeKind::CollectionsFolder,
            Self::MetricsFolder { .. } => SchemaNodeKind::MetricsFolder,
            Self::MetricNamespaceFolder { .. } => SchemaNodeKind::MetricNamespaceFolder,
            Self::MetricLeaf { .. } => SchemaNodeKind::MetricLeaf,
            Self::DashboardsFolder { .. } => SchemaNodeKind::DashboardsFolder,
            Self::DashboardItem { .. } => SchemaNodeKind::DashboardItem,
            Self::SavedChartsFolder { .. } => SchemaNodeKind::SavedChartsFolder,
            Self::SavedChartItem { .. } => SchemaNodeKind::SavedChartItem,
            Self::Table { .. } => SchemaNodeKind::Table,
            Self::View { .. } => SchemaNodeKind::View,
            Self::Collection { .. } => SchemaNodeKind::Collection,
            Self::CollectionChild { .. } => SchemaNodeKind::CollectionChild,
            Self::CollectionChildrenMore { .. } => SchemaNodeKind::CollectionChildrenMore,
            Self::CustomType { .. } => SchemaNodeKind::CustomType,
            Self::ColumnsFolder { .. } => SchemaNodeKind::ColumnsFolder,
            Self::IndexesFolder { .. } => SchemaNodeKind::IndexesFolder,
            Self::ForeignKeysFolder { .. } => SchemaNodeKind::ForeignKeysFolder,
            Self::ConstraintsFolder { .. } => SchemaNodeKind::ConstraintsFolder,
            Self::Column { .. } => SchemaNodeKind::Column,
            Self::Index { .. } => SchemaNodeKind::Index,
            Self::ForeignKey { .. } => SchemaNodeKind::ForeignKey,
            Self::Constraint { .. } => SchemaNodeKind::Constraint,
            Self::SchemaIndex { .. } => SchemaNodeKind::SchemaIndex,
            Self::SchemaForeignKey { .. } => SchemaNodeKind::SchemaForeignKey,
            Self::Routine { .. } => SchemaNodeKind::Routine,
            Self::DatabaseIndexesFolder { .. } => SchemaNodeKind::DatabaseIndexesFolder,
            Self::CollectionFieldsFolder { .. } => SchemaNodeKind::CollectionFieldsFolder,
            Self::CollectionField { .. } => SchemaNodeKind::CollectionField,
            Self::CollectionIndexesFolder { .. } => SchemaNodeKind::CollectionIndexesFolder,
            Self::CollectionIndex { .. } => SchemaNodeKind::CollectionIndex,
            Self::EnumValue { .. } => SchemaNodeKind::EnumValue,
            Self::BaseType { .. } => SchemaNodeKind::BaseType,
            Self::Placeholder { .. } => SchemaNodeKind::Placeholder,
            Self::DependentsFolder { .. } => SchemaNodeKind::DependentsFolder,
            Self::DependentItem { .. } => SchemaNodeKind::DependentItem,
            Self::ScriptsFolder { .. } => SchemaNodeKind::ScriptsFolder,
            Self::ScriptFile { .. } => SchemaNodeKind::ScriptFile,
        }
    }

    pub fn profile_id(&self) -> Option<Uuid> {
        match self {
            Self::ConnectionFolder { .. }
            | Self::ScriptsFolder { .. }
            | Self::ScriptFile { .. } => None,
            Self::Profile { profile_id, .. }
            | Self::Database { profile_id, .. }
            | Self::Loading { profile_id, .. }
            | Self::Schema { profile_id, .. }
            | Self::TablesFolder { profile_id, .. }
            | Self::ViewsFolder { profile_id, .. }
            | Self::TypesFolder { profile_id, .. }
            | Self::TypesLoadingFolder { profile_id, .. }
            | Self::SchemaIndexesFolder { profile_id, .. }
            | Self::SchemaIndexesLoadingFolder { profile_id, .. }
            | Self::SchemaForeignKeysFolder { profile_id, .. }
            | Self::SchemaForeignKeysLoadingFolder { profile_id, .. }
            | Self::RoutinesFolder { profile_id, .. }
            | Self::RoutinesLoadingFolder { profile_id, .. }
            | Self::CollectionsFolder { profile_id, .. }
            | Self::MetricsFolder { profile_id, .. }
            | Self::MetricNamespaceFolder { profile_id, .. }
            | Self::MetricLeaf { profile_id, .. }
            | Self::Table { profile_id, .. }
            | Self::View { profile_id, .. }
            | Self::Collection { profile_id, .. }
            | Self::CollectionChild { profile_id, .. }
            | Self::CollectionChildrenMore { profile_id, .. }
            | Self::CustomType { profile_id, .. }
            | Self::ColumnsFolder { profile_id, .. }
            | Self::IndexesFolder { profile_id, .. }
            | Self::ForeignKeysFolder { profile_id, .. }
            | Self::ConstraintsFolder { profile_id, .. }
            | Self::Column { profile_id, .. }
            | Self::Index { profile_id, .. }
            | Self::ForeignKey { profile_id, .. }
            | Self::Constraint { profile_id, .. }
            | Self::SchemaIndex { profile_id, .. }
            | Self::SchemaForeignKey { profile_id, .. }
            | Self::Routine { profile_id, .. }
            | Self::DatabaseIndexesFolder { profile_id, .. }
            | Self::CollectionFieldsFolder { profile_id, .. }
            | Self::CollectionField { profile_id, .. }
            | Self::CollectionIndexesFolder { profile_id, .. }
            | Self::CollectionIndex { profile_id, .. }
            | Self::EnumValue { profile_id, .. }
            | Self::BaseType { profile_id, .. }
            | Self::Placeholder { profile_id, .. }
            | Self::DependentsFolder { profile_id, .. }
            | Self::DependentItem { profile_id, .. }
            | Self::DashboardsFolder { profile_id, .. }
            | Self::DashboardItem { profile_id, .. }
            | Self::SavedChartsFolder { profile_id, .. }
            | Self::SavedChartItem { profile_id, .. } => Some(*profile_id),
        }
    }
}

// Prefix tags used in the pipe-delimited encoding.
// Keep them short to minimize string allocation overhead.
const P_CONN_FOLDER: &str = "CF";
const P_PROFILE: &str = "P";
const P_DATABASE: &str = "DB";
const P_LOADING: &str = "LD";
const P_SCHEMA: &str = "S";
const P_TABLES_FOLDER: &str = "TF";
const P_VIEWS_FOLDER: &str = "VF";
const P_TYPES_FOLDER: &str = "YF";
const P_TYPES_LOADING: &str = "YL";
const P_SCHEMA_IDX_FOLDER: &str = "XF";
const P_SCHEMA_IDX_LOADING: &str = "XL";
const P_SCHEMA_FK_FOLDER: &str = "KF";
const P_SCHEMA_FK_LOADING: &str = "KL";
const P_COLLECTIONS_FOLDER: &str = "CF2";
const P_TABLE: &str = "T";
const P_VIEW: &str = "V";
const P_COLLECTION: &str = "C";
const P_COLLECTION_CHILD: &str = "CCH";
const P_COLLECTION_CHILDREN_MORE: &str = "CCM";
const P_CUSTOM_TYPE: &str = "Y";
const P_COLUMNS_FOLDER: &str = "CLF";
const P_INDEXES_FOLDER: &str = "IXF";
const P_FK_FOLDER: &str = "FKF";
const P_CONSTRAINTS_FOLDER: &str = "CSF";
const P_COLUMN: &str = "CL";
const P_INDEX: &str = "IX";
const P_FK: &str = "FK";
const P_CONSTRAINT: &str = "CS";
const P_SCHEMA_INDEX: &str = "SX";
const P_SCHEMA_FK: &str = "SK";
const P_DB_IDX_FOLDER: &str = "DIF";
const P_COLL_FIELDS_FOLDER: &str = "CFF";
const P_COLL_FIELD: &str = "CFD";
const P_COLL_IDX_FOLDER: &str = "CIF";
const P_COLL_INDEX: &str = "CI";
const P_ENUM_VALUE: &str = "EV";
const P_BASE_TYPE: &str = "BT";
const P_PLACEHOLDER: &str = "PH";
const P_SCRIPTS_FOLDER: &str = "SCF";
const P_SCRIPT_FILE: &str = "SCR";
const P_DEPENDENTS_FOLDER: &str = "DEPF";
const P_DEPENDENT_ITEM: &str = "DEP";
const P_ROUTINES_FOLDER: &str = "RTF";
const P_ROUTINES_LOADING: &str = "RTL";
const P_ROUTINE: &str = "RT";
// Metric catalog node prefixes (CloudWatch sidebar tree)
const P_METRICS_FOLDER: &str = "MF";
const P_METRIC_NS_FOLDER: &str = "MNF";
const P_METRIC_LEAF: &str = "ML";
// Dashboard and saved-chart sidebar node prefixes.
// Note: P_SCRIPTS_FOLDER already uses "SCF", so we use distinct tags here.
const P_DASHBOARDS_FOLDER: &str = "DBF";
const P_DASHBOARD_ITEM: &str = "DBI";
const P_SAVED_CHARTS_FOLDER: &str = "SCRF";
const P_SAVED_CHART_ITEM: &str = "SCRI";

impl fmt::Display for SchemaNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionFolder { node_id } => {
                write!(f, "{}|{}", P_CONN_FOLDER, node_id)
            }
            Self::Profile { profile_id } => {
                write!(f, "{}|{}", P_PROFILE, profile_id)
            }
            Self::Database { profile_id, name } => {
                write!(f, "{}|{}|{}", P_DATABASE, profile_id, name)
            }
            Self::Loading {
                profile_id,
                database,
            } => {
                write!(f, "{}|{}|{}", P_LOADING, profile_id, database)
            }
            Self::Schema { profile_id, name } => {
                write!(f, "{}|{}|{}", P_SCHEMA, profile_id, name)
            }
            Self::TablesFolder { profile_id, schema } => {
                write!(f, "{}|{}|{}", P_TABLES_FOLDER, profile_id, schema)
            }
            Self::ViewsFolder { profile_id, schema } => {
                write!(f, "{}|{}|{}", P_VIEWS_FOLDER, profile_id, schema)
            }
            Self::TypesFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_TYPES_FOLDER, profile_id, database, schema
                )
            }
            Self::TypesLoadingFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_TYPES_LOADING, profile_id, database, schema
                )
            }
            Self::SchemaIndexesFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_SCHEMA_IDX_FOLDER, profile_id, database, schema
                )
            }
            Self::SchemaIndexesLoadingFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_SCHEMA_IDX_LOADING, profile_id, database, schema
                )
            }
            Self::SchemaForeignKeysFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_SCHEMA_FK_FOLDER, profile_id, database, schema
                )
            }
            Self::SchemaForeignKeysLoadingFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_SCHEMA_FK_LOADING, profile_id, database, schema
                )
            }
            Self::CollectionsFolder {
                profile_id,
                database,
            } => {
                write!(f, "{}|{}|{}", P_COLLECTIONS_FOLDER, profile_id, database)
            }
            Self::Table {
                profile_id,
                database,
                schema,
                name,
            } => match database {
                Some(db) => write!(f, "{}|{}|{}|{}|{}", P_TABLE, profile_id, schema, name, db),
                None => write!(f, "{}|{}|{}|{}", P_TABLE, profile_id, schema, name),
            },
            Self::View {
                profile_id,
                database,
                schema,
                name,
            } => match database {
                Some(db) => write!(f, "{}|{}|{}|{}|{}", P_VIEW, profile_id, schema, name, db),
                None => write!(f, "{}|{}|{}|{}", P_VIEW, profile_id, schema, name),
            },
            Self::Collection {
                profile_id,
                database,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_COLLECTION, profile_id, database, name)
            }
            Self::CollectionChild {
                profile_id,
                database,
                collection,
                child_id,
                name,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}|{}|{}",
                    P_COLLECTION_CHILD, profile_id, database, collection, child_id, name
                )
            }
            Self::CollectionChildrenMore {
                profile_id,
                database,
                collection,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_COLLECTION_CHILDREN_MORE, profile_id, database, collection
                )
            }
            Self::CustomType {
                profile_id,
                schema,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_CUSTOM_TYPE, profile_id, schema, name)
            }
            Self::ColumnsFolder {
                profile_id,
                schema,
                table,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_COLUMNS_FOLDER, profile_id, schema, table
                )
            }
            Self::IndexesFolder {
                profile_id,
                schema,
                table,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_INDEXES_FOLDER, profile_id, schema, table
                )
            }
            Self::ForeignKeysFolder {
                profile_id,
                schema,
                table,
            } => {
                write!(f, "{}|{}|{}|{}", P_FK_FOLDER, profile_id, schema, table)
            }
            Self::ConstraintsFolder {
                profile_id,
                schema,
                table,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_CONSTRAINTS_FOLDER, profile_id, schema, table
                )
            }
            Self::Column {
                profile_id,
                table,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_COLUMN, profile_id, table, name)
            }
            Self::Index {
                profile_id,
                table,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_INDEX, profile_id, table, name)
            }
            Self::ForeignKey {
                profile_id,
                table,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_FK, profile_id, table, name)
            }
            Self::Constraint {
                profile_id,
                table,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_CONSTRAINT, profile_id, table, name)
            }
            Self::SchemaIndex {
                profile_id,
                schema,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_SCHEMA_INDEX, profile_id, schema, name)
            }
            Self::SchemaForeignKey {
                profile_id,
                schema,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_SCHEMA_FK, profile_id, schema, name)
            }
            Self::DatabaseIndexesFolder {
                profile_id,
                database,
            } => {
                write!(f, "{}|{}|{}", P_DB_IDX_FOLDER, profile_id, database)
            }
            Self::CollectionFieldsFolder {
                profile_id,
                database,
                collection,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_COLL_FIELDS_FOLDER, profile_id, database, collection
                )
            }
            Self::CollectionField {
                profile_id,
                collection,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_COLL_FIELD, profile_id, collection, name)
            }
            Self::CollectionIndexesFolder {
                profile_id,
                database,
                collection,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_COLL_IDX_FOLDER, profile_id, database, collection
                )
            }
            Self::CollectionIndex {
                profile_id,
                collection,
                name,
            } => {
                write!(f, "{}|{}|{}|{}", P_COLL_INDEX, profile_id, collection, name)
            }
            Self::EnumValue {
                profile_id,
                schema,
                type_name,
                value,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}|{}",
                    P_ENUM_VALUE, profile_id, schema, type_name, value
                )
            }
            Self::BaseType {
                profile_id,
                schema,
                type_name,
            } => {
                write!(f, "{}|{}|{}|{}", P_BASE_TYPE, profile_id, schema, type_name)
            }
            Self::Placeholder {
                profile_id,
                schema,
                table,
            } => {
                write!(f, "{}|{}|{}|{}", P_PLACEHOLDER, profile_id, schema, table)
            }
            Self::DependentsFolder {
                profile_id,
                schema,
                table,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_DEPENDENTS_FOLDER, profile_id, schema, table
                )
            }
            Self::DependentItem {
                profile_id,
                schema,
                table,
                name,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}|{}",
                    P_DEPENDENT_ITEM, profile_id, schema, table, name
                )
            }
            Self::RoutinesFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_ROUTINES_FOLDER, profile_id, database, schema
                )
            }
            Self::RoutinesLoadingFolder {
                profile_id,
                database,
                schema,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_ROUTINES_LOADING, profile_id, database, schema
                )
            }
            Self::Routine {
                profile_id,
                schema,
                specific_name,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_ROUTINE, profile_id, schema, specific_name
                )
            }
            Self::MetricsFolder {
                profile_id,
                database,
            } => {
                write!(f, "{}|{}|{}", P_METRICS_FOLDER, profile_id, database)
            }
            Self::MetricNamespaceFolder {
                profile_id,
                database,
                namespace,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}",
                    P_METRIC_NS_FOLDER, profile_id, database, namespace
                )
            }
            Self::MetricLeaf {
                profile_id,
                database,
                namespace,
                metric_name,
            } => {
                write!(
                    f,
                    "{}|{}|{}|{}|{}",
                    P_METRIC_LEAF, profile_id, database, namespace, metric_name
                )
            }
            Self::ScriptsFolder { path } => match path {
                Some(p) => write!(f, "{}|{}", P_SCRIPTS_FOLDER, p),
                None => write!(f, "{}", P_SCRIPTS_FOLDER),
            },
            Self::ScriptFile { path } => {
                write!(f, "{}|{}", P_SCRIPT_FILE, path)
            }
            Self::DashboardsFolder { profile_id } => {
                write!(f, "{}|{}", P_DASHBOARDS_FOLDER, profile_id)
            }
            Self::DashboardItem {
                profile_id,
                dashboard_id,
            } => {
                write!(f, "{}|{}|{}", P_DASHBOARD_ITEM, profile_id, dashboard_id)
            }
            Self::SavedChartsFolder { profile_id } => {
                write!(f, "{}|{}", P_SAVED_CHARTS_FOLDER, profile_id)
            }
            Self::SavedChartItem {
                profile_id,
                chart_id,
            } => {
                write!(f, "{}|{}|{}", P_SAVED_CHART_ITEM, profile_id, chart_id)
            }
        }
    }
}

/// Error returned when parsing a `SchemaNodeId` from a string fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseSchemaNodeIdError {
    pub input: String,
}

impl fmt::Display for ParseSchemaNodeIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid schema node id: {:?}", self.input)
    }
}

impl std::error::Error for ParseSchemaNodeIdError {}

impl FromStr for SchemaNodeId {
    type Err = ParseSchemaNodeIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseSchemaNodeIdError {
            input: s.to_string(),
        };

        let parts: Vec<&str> = s.splitn(6, '|').collect();
        if parts.is_empty() {
            return Err(err());
        }

        let prefix = parts[0];

        // Handle single-token variants first
        if prefix == P_SCRIPTS_FOLDER && parts.len() == 1 {
            return Ok(Self::ScriptsFolder { path: None });
        }

        // ScriptsFolder with path — path may contain pipes, rejoin everything after prefix
        if prefix == P_SCRIPTS_FOLDER && parts.len() >= 2 {
            let path = parts[1..].join("|");
            return Ok(Self::ScriptsFolder { path: Some(path) });
        }

        if parts.len() < 2 {
            return Err(err());
        }

        match prefix {
            P_CONN_FOLDER => {
                let node_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                Ok(Self::ConnectionFolder { node_id })
            }

            P_PROFILE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                Ok(Self::Profile { profile_id })
            }

            P_DATABASE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let name = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::Database { profile_id, name })
            }

            P_LOADING => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::Loading {
                    profile_id,
                    database,
                })
            }

            P_SCHEMA => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let name = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::Schema { profile_id, name })
            }

            P_TABLES_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::TablesFolder { profile_id, schema })
            }

            P_VIEWS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::ViewsFolder { profile_id, schema })
            }

            P_TYPES_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::TypesFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_TYPES_LOADING => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::TypesLoadingFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_SCHEMA_IDX_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::SchemaIndexesFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_SCHEMA_IDX_LOADING => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::SchemaIndexesLoadingFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_SCHEMA_FK_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::SchemaForeignKeysFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_SCHEMA_FK_LOADING => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::SchemaForeignKeysLoadingFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_COLLECTIONS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::CollectionsFolder {
                    profile_id,
                    database,
                })
            }

            P_TABLE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                let database = parts.get(4).map(|s| s.to_string());
                Ok(Self::Table {
                    profile_id,
                    database,
                    schema,
                    name,
                })
            }

            P_VIEW => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                let database = parts.get(4).map(|s| s.to_string());
                Ok(Self::View {
                    profile_id,
                    database,
                    schema,
                    name,
                })
            }

            P_COLLECTION => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::Collection {
                    profile_id,
                    database,
                    name,
                })
            }

            P_COLLECTION_CHILD => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let collection = parts.get(3).ok_or_else(err)?.to_string();
                let child_id = parts.get(4).ok_or_else(err)?.to_string();
                let name = parts.get(5).ok_or_else(err)?.to_string();
                Ok(Self::CollectionChild {
                    profile_id,
                    database,
                    collection,
                    child_id,
                    name,
                })
            }

            P_COLLECTION_CHILDREN_MORE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let collection = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::CollectionChildrenMore {
                    profile_id,
                    database,
                    collection,
                })
            }

            P_CUSTOM_TYPE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::CustomType {
                    profile_id,
                    schema,
                    name,
                })
            }

            P_COLUMNS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let table = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::ColumnsFolder {
                    profile_id,
                    schema,
                    table,
                })
            }

            P_INDEXES_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let table = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::IndexesFolder {
                    profile_id,
                    schema,
                    table,
                })
            }

            P_FK_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let table = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::ForeignKeysFolder {
                    profile_id,
                    schema,
                    table,
                })
            }

            P_CONSTRAINTS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let table = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::ConstraintsFolder {
                    profile_id,
                    schema,
                    table,
                })
            }

            P_COLUMN => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let table = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::Column {
                    profile_id,
                    table,
                    name,
                })
            }

            P_INDEX => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let table = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::Index {
                    profile_id,
                    table,
                    name,
                })
            }

            P_FK => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let table = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::ForeignKey {
                    profile_id,
                    table,
                    name,
                })
            }

            P_CONSTRAINT => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let table = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::Constraint {
                    profile_id,
                    table,
                    name,
                })
            }

            P_SCHEMA_INDEX => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::SchemaIndex {
                    profile_id,
                    schema,
                    name,
                })
            }

            P_SCHEMA_FK => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::SchemaForeignKey {
                    profile_id,
                    schema,
                    name,
                })
            }

            P_DB_IDX_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::DatabaseIndexesFolder {
                    profile_id,
                    database,
                })
            }

            P_COLL_FIELDS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let collection = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::CollectionFieldsFolder {
                    profile_id,
                    database,
                    collection,
                })
            }

            P_COLL_FIELD => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let collection = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::CollectionField {
                    profile_id,
                    collection,
                    name,
                })
            }

            P_COLL_IDX_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let collection = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::CollectionIndexesFolder {
                    profile_id,
                    database,
                    collection,
                })
            }

            P_COLL_INDEX => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let collection = parts.get(2).ok_or_else(err)?.to_string();
                let name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::CollectionIndex {
                    profile_id,
                    collection,
                    name,
                })
            }

            P_ENUM_VALUE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let type_name = parts.get(3).ok_or_else(err)?.to_string();
                let value = parts.get(4).ok_or_else(err)?.to_string();
                Ok(Self::EnumValue {
                    profile_id,
                    schema,
                    type_name,
                    value,
                })
            }

            P_BASE_TYPE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let type_name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::BaseType {
                    profile_id,
                    schema,
                    type_name,
                })
            }

            P_PLACEHOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let table = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::Placeholder {
                    profile_id,
                    schema,
                    table,
                })
            }

            P_DEPENDENTS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let table = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::DependentsFolder {
                    profile_id,
                    schema,
                    table,
                })
            }

            P_DEPENDENT_ITEM => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                let table = parts.get(3).ok_or_else(err)?.to_string();
                let name = parts.get(4).ok_or_else(err)?.to_string();
                Ok(Self::DependentItem {
                    profile_id,
                    schema,
                    table,
                    name,
                })
            }

            P_SCRIPT_FILE => {
                // Path may contain pipe characters, so rejoin everything after the prefix
                let path = parts[1..].join("|");
                if path.is_empty() {
                    return Err(err());
                }
                Ok(Self::ScriptFile { path })
            }

            P_ROUTINES_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::RoutinesFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_ROUTINES_LOADING => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let schema = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::RoutinesLoadingFolder {
                    profile_id,
                    database,
                    schema,
                })
            }

            P_ROUTINE => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let schema = parts.get(2).ok_or_else(err)?.to_string();
                // specific_name may contain commas and parens but no pipes;
                // with splitn(6) the 4th field absorbs any remaining content.
                let specific_name = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::Routine {
                    profile_id,
                    schema,
                    specific_name,
                })
            }

            P_METRICS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                Ok(Self::MetricsFolder {
                    profile_id,
                    database,
                })
            }

            P_METRIC_NS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let namespace = parts.get(3).ok_or_else(err)?.to_string();
                Ok(Self::MetricNamespaceFolder {
                    profile_id,
                    database,
                    namespace,
                })
            }

            P_METRIC_LEAF => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let database = parts.get(2).ok_or_else(err)?.to_string();
                let namespace = parts.get(3).ok_or_else(err)?.to_string();
                // metric_name may contain slashes; splitn(6) gives us all remaining content
                let metric_name = parts.get(4).ok_or_else(err)?.to_string();
                Ok(Self::MetricLeaf {
                    profile_id,
                    database,
                    namespace,
                    metric_name,
                })
            }

            P_DASHBOARDS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                Ok(Self::DashboardsFolder { profile_id })
            }

            P_DASHBOARD_ITEM => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let dashboard_id =
                    Uuid::parse_str(parts.get(2).ok_or_else(err)?).map_err(|_| err())?;
                Ok(Self::DashboardItem {
                    profile_id,
                    dashboard_id,
                })
            }

            P_SAVED_CHARTS_FOLDER => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                Ok(Self::SavedChartsFolder { profile_id })
            }

            P_SAVED_CHART_ITEM => {
                let profile_id =
                    Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
                let chart_id =
                    Uuid::parse_str(parts.get(2).ok_or_else(err)?).map_err(|_| err())?;
                Ok(Self::SavedChartItem {
                    profile_id,
                    chart_id,
                })
            }

            _ => Err(err()),
        }
    }
}

impl SchemaNodeKind {
    pub fn needs_click_handler(&self) -> bool {
        matches!(
            self,
            Self::Profile
                | Self::Database
                | Self::Table
                | Self::View
                | Self::Collection
                | Self::CollectionChild
                | Self::CollectionChildrenMore
                | Self::ConnectionFolder
                | Self::Schema
                | Self::TablesFolder
                | Self::ViewsFolder
                | Self::TypesFolder
                | Self::ColumnsFolder
                | Self::IndexesFolder
                | Self::ForeignKeysFolder
                | Self::ConstraintsFolder
                | Self::SchemaIndexesFolder
                | Self::SchemaForeignKeysFolder
                | Self::RoutinesFolder
                | Self::CollectionsFolder
                | Self::CollectionFieldsFolder
                | Self::CustomType
                | Self::ScriptsFolder
                | Self::ScriptFile
                | Self::DependentsFolder
                | Self::Routine
                | Self::MetricsFolder
                | Self::MetricNamespaceFolder
                | Self::MetricLeaf
                | Self::DashboardsFolder
                | Self::DashboardItem
                | Self::SavedChartsFolder
                | Self::SavedChartItem
        )
    }

    pub fn is_expandable_folder(&self) -> bool {
        matches!(
            self,
            Self::ConnectionFolder
                | Self::Schema
                | Self::TablesFolder
                | Self::ViewsFolder
                | Self::TypesFolder
                | Self::ColumnsFolder
                | Self::IndexesFolder
                | Self::ForeignKeysFolder
                | Self::ConstraintsFolder
                | Self::SchemaIndexesFolder
                | Self::SchemaForeignKeysFolder
                | Self::RoutinesFolder
                | Self::CollectionsFolder
                | Self::CollectionFieldsFolder
                | Self::Database
                | Self::CustomType
                | Self::ScriptsFolder
                | Self::DependentsFolder
                | Self::MetricsFolder
                | Self::MetricNamespaceFolder
                | Self::DashboardsFolder
                | Self::SavedChartsFolder
        )
    }

    pub fn shows_pointer_cursor(&self) -> bool {
        matches!(
            self,
            Self::Profile
                | Self::Database
                | Self::ConnectionFolder
                | Self::CollectionChild
                | Self::CollectionChildrenMore
                | Self::ScriptFile
                | Self::Routine
                | Self::MetricLeaf
                | Self::DashboardItem
                | Self::SavedChartItem
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(id: SchemaNodeId) {
        let s = id.to_string();
        let parsed: SchemaNodeId = s.parse().unwrap_or_else(|e| {
            panic!("Failed to parse {:?}: {}", s, e);
        });
        assert_eq!(id, parsed, "Roundtrip failed for: {}", s);
    }

    #[test]
    fn test_roundtrip_all_variants() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();

        roundtrip(SchemaNodeId::ConnectionFolder { node_id: uuid });
        roundtrip(SchemaNodeId::Profile { profile_id: uuid });
        roundtrip(SchemaNodeId::Database {
            profile_id: uuid,
            name: "mydb".into(),
        });
        roundtrip(SchemaNodeId::Loading {
            profile_id: uuid,
            database: "mydb".into(),
        });
        roundtrip(SchemaNodeId::Schema {
            profile_id: uuid,
            name: "public".into(),
        });
        roundtrip(SchemaNodeId::TablesFolder {
            profile_id: uuid,
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::ViewsFolder {
            profile_id: uuid,
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::TypesFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::TypesLoadingFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::SchemaIndexesFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::SchemaIndexesLoadingFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::SchemaForeignKeysFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::SchemaForeignKeysLoadingFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::CollectionsFolder {
            profile_id: uuid,
            database: "mydb".into(),
        });
        roundtrip(SchemaNodeId::Table {
            profile_id: uuid,
            database: None,
            schema: "public".into(),
            name: "users".into(),
        });
        roundtrip(SchemaNodeId::Table {
            profile_id: uuid,
            database: Some("miniflux".into()),
            schema: "public".into(),
            name: "entries".into(),
        });
        roundtrip(SchemaNodeId::View {
            profile_id: uuid,
            database: None,
            schema: "public".into(),
            name: "active_users".into(),
        });
        roundtrip(SchemaNodeId::View {
            profile_id: uuid,
            database: Some("miniflux".into()),
            schema: "public".into(),
            name: "active_users".into(),
        });
        roundtrip(SchemaNodeId::Collection {
            profile_id: uuid,
            database: "mydb".into(),
            name: "orders".into(),
        });
        roundtrip(SchemaNodeId::CollectionChild {
            profile_id: uuid,
            database: "logs".into(),
            collection: "/aws/lambda/app".into(),
            child_id: "stream-2026-04-25".into(),
            name: "2026/04/25/[$LATEST]abc".into(),
        });
        roundtrip(SchemaNodeId::CollectionChildrenMore {
            profile_id: uuid,
            database: "logs".into(),
            collection: "/aws/lambda/app".into(),
        });
        roundtrip(SchemaNodeId::CustomType {
            profile_id: uuid,
            schema: "public".into(),
            name: "mood".into(),
        });
        roundtrip(SchemaNodeId::ColumnsFolder {
            profile_id: uuid,
            schema: "public".into(),
            table: "users".into(),
        });
        roundtrip(SchemaNodeId::IndexesFolder {
            profile_id: uuid,
            schema: "public".into(),
            table: "users".into(),
        });
        roundtrip(SchemaNodeId::ForeignKeysFolder {
            profile_id: uuid,
            schema: "public".into(),
            table: "users".into(),
        });
        roundtrip(SchemaNodeId::ConstraintsFolder {
            profile_id: uuid,
            schema: "public".into(),
            table: "users".into(),
        });
        roundtrip(SchemaNodeId::Column {
            profile_id: uuid,
            table: "users".into(),
            name: "email".into(),
        });
        roundtrip(SchemaNodeId::Index {
            profile_id: uuid,
            table: "users".into(),
            name: "idx_email".into(),
        });
        roundtrip(SchemaNodeId::ForeignKey {
            profile_id: uuid,
            table: "orders".into(),
            name: "fk_user_id".into(),
        });
        roundtrip(SchemaNodeId::Constraint {
            profile_id: uuid,
            table: "users".into(),
            name: "users_pkey".into(),
        });
        roundtrip(SchemaNodeId::SchemaIndex {
            profile_id: uuid,
            schema: "public".into(),
            name: "idx_users_email".into(),
        });
        roundtrip(SchemaNodeId::SchemaForeignKey {
            profile_id: uuid,
            schema: "public".into(),
            name: "fk_orders_user".into(),
        });
        roundtrip(SchemaNodeId::DatabaseIndexesFolder {
            profile_id: uuid,
            database: "mydb".into(),
        });
        roundtrip(SchemaNodeId::CollectionFieldsFolder {
            profile_id: uuid,
            database: "mydb".into(),
            collection: "orders".into(),
        });
        roundtrip(SchemaNodeId::CollectionField {
            profile_id: uuid,
            collection: "orders".into(),
            name: "email".into(),
        });
        roundtrip(SchemaNodeId::CollectionIndexesFolder {
            profile_id: uuid,
            database: "mydb".into(),
            collection: "orders".into(),
        });
        roundtrip(SchemaNodeId::CollectionIndex {
            profile_id: uuid,
            collection: "orders".into(),
            name: "_id_".into(),
        });
        roundtrip(SchemaNodeId::EnumValue {
            profile_id: uuid,
            schema: "public".into(),
            type_name: "mood".into(),
            value: "happy".into(),
        });
        roundtrip(SchemaNodeId::BaseType {
            profile_id: uuid,
            schema: "public".into(),
            type_name: "positive_int".into(),
        });
        roundtrip(SchemaNodeId::Placeholder {
            profile_id: uuid,
            schema: "public".into(),
            table: "users".into(),
        });

        roundtrip(SchemaNodeId::ScriptsFolder { path: None });
        roundtrip(SchemaNodeId::ScriptsFolder {
            path: Some("/home/user/scripts/migrations".into()),
        });
        roundtrip(SchemaNodeId::ScriptFile {
            path: "/home/user/scripts/query.sql".into(),
        });

        // Routines variants
        roundtrip(SchemaNodeId::RoutinesFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::RoutinesLoadingFolder {
            profile_id: uuid,
            database: "mydb".into(),
            schema: "public".into(),
        });
        roundtrip(SchemaNodeId::Routine {
            profile_id: uuid,
            schema: "public".into(),
            specific_name: "add(integer, integer)".into(),
        });
    }

    #[test]
    fn metric_nodes_round_trip_via_display_and_from_str() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();

        roundtrip(SchemaNodeId::MetricsFolder {
            profile_id: uuid,
            database: "default".into(),
        });
        roundtrip(SchemaNodeId::MetricNamespaceFolder {
            profile_id: uuid,
            database: "default".into(),
            namespace: "AWS/EC2".into(),
        });
        roundtrip(SchemaNodeId::MetricLeaf {
            profile_id: uuid,
            database: "default".into(),
            namespace: "AWS/EC2".into(),
            metric_name: "CPUUtilization".into(),
        });
    }

    #[test]
    fn test_routine_display_format() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let id = SchemaNodeId::Routine {
            profile_id: uuid,
            schema: "public".into(),
            specific_name: "add(integer, integer)".into(),
        };
        assert_eq!(
            id.to_string(),
            "RT|12345678-1234-1234-1234-123456789abc|public|add(integer, integer)"
        );
    }

    #[test]
    fn test_script_file_path_with_pipes() {
        roundtrip(SchemaNodeId::ScriptFile {
            path: "/home/user/dir|with|pipes/query.sql".into(),
        });
    }

    #[test]
    fn test_special_characters_in_names() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();

        roundtrip(SchemaNodeId::Table {
            profile_id: uuid,
            database: None,
            schema: "my_schema".into(),
            name: "user_accounts".into(),
        });

        roundtrip(SchemaNodeId::Database {
            profile_id: uuid,
            name: "my-database-name".into(),
        });
    }

    #[test]
    fn test_kind() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let id = SchemaNodeId::Table {
            profile_id: uuid,
            database: None,
            schema: "public".into(),
            name: "users".into(),
        };
        assert_eq!(id.kind(), SchemaNodeKind::Table);
    }

    #[test]
    fn test_profile_id() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();

        assert_eq!(
            SchemaNodeId::Table {
                profile_id: uuid,
                database: None,
                schema: "public".into(),
                name: "users".into()
            }
            .profile_id(),
            Some(uuid)
        );

        assert_eq!(
            SchemaNodeId::ConnectionFolder { node_id: uuid }.profile_id(),
            None
        );
    }

    #[test]
    fn test_invalid_parse() {
        assert!("".parse::<SchemaNodeId>().is_err());
        assert!("UNKNOWN|foo".parse::<SchemaNodeId>().is_err());
        assert!("T|not-a-uuid|public|users".parse::<SchemaNodeId>().is_err());
        assert!("T|".parse::<SchemaNodeId>().is_err());
    }

    #[test]
    fn test_display_format() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();

        let id = SchemaNodeId::Table {
            profile_id: uuid,
            database: None,
            schema: "public".into(),
            name: "users".into(),
        };
        assert_eq!(
            id.to_string(),
            "T|12345678-1234-1234-1234-123456789abc|public|users"
        );

        let id_with_db = SchemaNodeId::Table {
            profile_id: uuid,
            database: Some("miniflux".into()),
            schema: "public".into(),
            name: "entries".into(),
        };
        assert_eq!(
            id_with_db.to_string(),
            "T|12345678-1234-1234-1234-123456789abc|public|entries|miniflux"
        );
    }

    // --- REV 4: Dashboard and SavedChart sidebar node tests ---

    #[test]
    fn test_node_id_dashboards_folder_roundtrip() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        roundtrip(SchemaNodeId::DashboardsFolder { profile_id: uuid });
    }

    #[test]
    fn test_node_id_dashboard_item_roundtrip() {
        let profile_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let dashboard_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        roundtrip(SchemaNodeId::DashboardItem {
            profile_id,
            dashboard_id,
        });
    }

    #[test]
    fn test_node_id_saved_charts_folder_roundtrip() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        roundtrip(SchemaNodeId::SavedChartsFolder { profile_id: uuid });
    }

    #[test]
    fn test_node_id_saved_chart_item_roundtrip() {
        let profile_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let chart_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        roundtrip(SchemaNodeId::SavedChartItem {
            profile_id,
            chart_id,
        });
    }

    #[test]
    fn test_node_id_dashboards_folder_profile_id_accessor() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        assert_eq!(
            SchemaNodeId::DashboardsFolder { profile_id: uuid }.profile_id(),
            Some(uuid)
        );
    }

    #[test]
    fn test_node_id_dashboard_item_profile_id_accessor() {
        let profile_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let dashboard_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        assert_eq!(
            SchemaNodeId::DashboardItem {
                profile_id,
                dashboard_id
            }
            .profile_id(),
            Some(profile_id)
        );
    }

    #[test]
    fn test_node_id_saved_charts_folder_profile_id_accessor() {
        let uuid = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        assert_eq!(
            SchemaNodeId::SavedChartsFolder { profile_id: uuid }.profile_id(),
            Some(uuid)
        );
    }

    #[test]
    fn test_node_id_saved_chart_item_profile_id_accessor() {
        let profile_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let chart_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        assert_eq!(
            SchemaNodeId::SavedChartItem {
                profile_id,
                chart_id
            }
            .profile_id(),
            Some(profile_id)
        );
    }

    #[test]
    fn test_node_id_dashboard_item_display_format() {
        let profile_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let dashboard_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let id = SchemaNodeId::DashboardItem {
            profile_id,
            dashboard_id,
        };
        assert_eq!(
            id.to_string(),
            "DBI|12345678-1234-1234-1234-123456789abc|aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        );
    }

    #[test]
    fn test_node_id_saved_chart_item_display_format() {
        let profile_id = Uuid::parse_str("12345678-1234-1234-1234-123456789abc").unwrap();
        let chart_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let id = SchemaNodeId::SavedChartItem {
            profile_id,
            chart_id,
        };
        assert_eq!(
            id.to_string(),
            "SCRI|12345678-1234-1234-1234-123456789abc|aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        );
    }
}

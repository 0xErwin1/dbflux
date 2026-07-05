use std::fmt;

use super::SchemaNodeId;
use super::{
    P_BASE_TYPE, P_COLL_FIELD, P_COLL_FIELDS_FOLDER, P_COLL_IDX_FOLDER, P_COLL_INDEX, P_COLLECTION,
    P_COLLECTION_CHILD, P_COLLECTION_CHILDREN_MORE, P_COLLECTIONS_FOLDER, P_COLUMN,
    P_COLUMNS_FOLDER, P_CONN_FOLDER, P_CONSTRAINT, P_CONSTRAINTS_FOLDER, P_CUSTOM_TYPE,
    P_DASHBOARD_ITEM, P_DASHBOARDS_FOLDER, P_DATABASE, P_DATABASES_FOLDER, P_DB_IDX_FOLDER,
    P_DEPENDENT_ITEM, P_DEPENDENTS_FOLDER, P_ENUM_VALUE, P_FK, P_FK_FOLDER, P_INDEX,
    P_INDEXES_FOLDER, P_INST_INSPECTOR_LEAF, P_INST_INSPECTORS_FOLDER, P_INST_METRIC_LEAF,
    P_INST_METRICS_FOLDER, P_INST_OVERVIEW_LEAF, P_LOADING, P_METRIC_LEAF, P_METRIC_NS_FOLDER,
    P_METRICS_FOLDER, P_PLACEHOLDER, P_PROFILE, P_REMOTE_DASHBOARD_ITEM,
    P_REMOTE_DASHBOARDS_FOLDER, P_ROUTINE, P_ROUTINES_FOLDER, P_ROUTINES_LOADING,
    P_SAVED_CHART_ITEM, P_SAVED_CHARTS_FOLDER, P_SCHEMA, P_SCHEMA_FK, P_SCHEMA_FK_FOLDER,
    P_SCHEMA_FK_LOADING, P_SCHEMA_IDX_FOLDER, P_SCHEMA_IDX_LOADING, P_SCHEMA_INDEX, P_SCRIPT_FILE,
    P_SCRIPTS_FOLDER, P_TABLE, P_TABLES_FOLDER, P_TYPES_FOLDER, P_TYPES_LOADING, P_VIEW,
    P_VIEWS_FOLDER,
};

impl fmt::Display for SchemaNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionFolder { .. }
            | Self::Profile { .. }
            | Self::DatabasesFolder { .. }
            | Self::Database { .. }
            | Self::Loading { .. }
            | Self::Schema { .. } => fmt_connection_scope(self, f),

            Self::TablesFolder { .. }
            | Self::ViewsFolder { .. }
            | Self::TypesFolder { .. }
            | Self::TypesLoadingFolder { .. }
            | Self::SchemaIndexesFolder { .. }
            | Self::SchemaIndexesLoadingFolder { .. }
            | Self::SchemaForeignKeysFolder { .. }
            | Self::SchemaForeignKeysLoadingFolder { .. }
            | Self::CollectionsFolder { .. } => fmt_folder_variants(self, f),

            Self::Table { .. }
            | Self::View { .. }
            | Self::Collection { .. }
            | Self::CollectionChild { .. }
            | Self::CollectionChildrenMore { .. }
            | Self::CustomType { .. } => fmt_object_variants(self, f),

            Self::ColumnsFolder { .. }
            | Self::IndexesFolder { .. }
            | Self::ForeignKeysFolder { .. }
            | Self::ConstraintsFolder { .. } => fmt_table_detail_folders(self, f),

            Self::Column { .. }
            | Self::Index { .. }
            | Self::ForeignKey { .. }
            | Self::Constraint { .. }
            | Self::SchemaIndex { .. }
            | Self::SchemaForeignKey { .. } => fmt_detail_variants(self, f),

            Self::DatabaseIndexesFolder { .. }
            | Self::CollectionFieldsFolder { .. }
            | Self::CollectionField { .. }
            | Self::CollectionIndexesFolder { .. }
            | Self::CollectionIndex { .. } => fmt_collection_detail_variants(self, f),

            Self::EnumValue { .. } | Self::BaseType { .. } => fmt_type_detail_variants(self, f),

            Self::Placeholder { .. }
            | Self::DependentsFolder { .. }
            | Self::DependentItem { .. } => fmt_placeholder_and_dependents(self, f),

            Self::RoutinesFolder { .. }
            | Self::RoutinesLoadingFolder { .. }
            | Self::Routine { .. } => fmt_routine_variants(self, f),

            Self::MetricsFolder { .. }
            | Self::MetricNamespaceFolder { .. }
            | Self::MetricLeaf { .. } => fmt_metric_variants(self, f),

            Self::ScriptsFolder { .. } | Self::ScriptFile { .. } => fmt_scripts_variants(self, f),

            Self::DashboardsFolder { .. }
            | Self::DashboardItem { .. }
            | Self::RemoteDashboardsFolder { .. }
            | Self::RemoteDashboardItem { .. }
            | Self::SavedChartsFolder { .. }
            | Self::SavedChartItem { .. } => fmt_dashboard_and_chart_variants(self, f),

            Self::InstanceMetricsFolder { .. }
            | Self::InstanceMetricLeaf { .. }
            | Self::InstanceInspectorsFolder { .. }
            | Self::InstanceInspectorLeaf { .. }
            | Self::InstanceOverviewLeaf { .. } => fmt_instance_variants(self, f),
        }
    }
}

fn fmt_connection_scope(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::ConnectionFolder { node_id } => {
            write!(f, "{}|{}", P_CONN_FOLDER, node_id)
        }
        SchemaNodeId::Profile { profile_id } => {
            write!(f, "{}|{}", P_PROFILE, profile_id)
        }
        SchemaNodeId::DatabasesFolder { profile_id } => {
            write!(f, "{}|{}", P_DATABASES_FOLDER, profile_id)
        }
        SchemaNodeId::Database { profile_id, name } => {
            write!(f, "{}|{}|{}", P_DATABASE, profile_id, name)
        }
        SchemaNodeId::Loading {
            profile_id,
            database,
        } => {
            write!(f, "{}|{}|{}", P_LOADING, profile_id, database)
        }
        SchemaNodeId::Schema { profile_id, name } => {
            write!(f, "{}|{}|{}", P_SCHEMA, profile_id, name)
        }
        _ => unreachable!("fmt_connection_scope called with an unexpected variant"),
    }
}

fn fmt_folder_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::TablesFolder { profile_id, schema } => {
            write!(f, "{}|{}|{}", P_TABLES_FOLDER, profile_id, schema)
        }
        SchemaNodeId::ViewsFolder { profile_id, schema } => {
            write!(f, "{}|{}|{}", P_VIEWS_FOLDER, profile_id, schema)
        }
        SchemaNodeId::TypesFolder {
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
        SchemaNodeId::TypesLoadingFolder {
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
        SchemaNodeId::SchemaIndexesFolder {
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
        SchemaNodeId::SchemaIndexesLoadingFolder {
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
        SchemaNodeId::SchemaForeignKeysFolder {
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
        SchemaNodeId::SchemaForeignKeysLoadingFolder {
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
        SchemaNodeId::CollectionsFolder {
            profile_id,
            database,
        } => {
            write!(f, "{}|{}|{}", P_COLLECTIONS_FOLDER, profile_id, database)
        }
        _ => unreachable!("fmt_folder_variants called with an unexpected variant"),
    }
}

fn fmt_object_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::Table {
            profile_id,
            database,
            schema,
            name,
        } => match database {
            Some(db) => write!(f, "{}|{}|{}|{}|{}", P_TABLE, profile_id, schema, name, db),
            None => write!(f, "{}|{}|{}|{}", P_TABLE, profile_id, schema, name),
        },
        SchemaNodeId::View {
            profile_id,
            database,
            schema,
            name,
        } => match database {
            Some(db) => write!(f, "{}|{}|{}|{}|{}", P_VIEW, profile_id, schema, name, db),
            None => write!(f, "{}|{}|{}|{}", P_VIEW, profile_id, schema, name),
        },
        SchemaNodeId::Collection {
            profile_id,
            database,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_COLLECTION, profile_id, database, name)
        }
        SchemaNodeId::CollectionChild {
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
        SchemaNodeId::CollectionChildrenMore {
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
        SchemaNodeId::CustomType {
            profile_id,
            schema,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_CUSTOM_TYPE, profile_id, schema, name)
        }
        _ => unreachable!("fmt_object_variants called with an unexpected variant"),
    }
}

fn fmt_table_detail_folders(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::ColumnsFolder {
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
        SchemaNodeId::IndexesFolder {
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
        SchemaNodeId::ForeignKeysFolder {
            profile_id,
            schema,
            table,
        } => {
            write!(f, "{}|{}|{}|{}", P_FK_FOLDER, profile_id, schema, table)
        }
        SchemaNodeId::ConstraintsFolder {
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
        _ => unreachable!("fmt_table_detail_folders called with an unexpected variant"),
    }
}

fn fmt_detail_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::Column {
            profile_id,
            table,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_COLUMN, profile_id, table, name)
        }
        SchemaNodeId::Index {
            profile_id,
            table,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_INDEX, profile_id, table, name)
        }
        SchemaNodeId::ForeignKey {
            profile_id,
            table,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_FK, profile_id, table, name)
        }
        SchemaNodeId::Constraint {
            profile_id,
            table,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_CONSTRAINT, profile_id, table, name)
        }
        SchemaNodeId::SchemaIndex {
            profile_id,
            schema,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_SCHEMA_INDEX, profile_id, schema, name)
        }
        SchemaNodeId::SchemaForeignKey {
            profile_id,
            schema,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_SCHEMA_FK, profile_id, schema, name)
        }
        _ => unreachable!("fmt_detail_variants called with an unexpected variant"),
    }
}

fn fmt_collection_detail_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::DatabaseIndexesFolder {
            profile_id,
            database,
        } => {
            write!(f, "{}|{}|{}", P_DB_IDX_FOLDER, profile_id, database)
        }
        SchemaNodeId::CollectionFieldsFolder {
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
        SchemaNodeId::CollectionField {
            profile_id,
            collection,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_COLL_FIELD, profile_id, collection, name)
        }
        SchemaNodeId::CollectionIndexesFolder {
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
        SchemaNodeId::CollectionIndex {
            profile_id,
            collection,
            name,
        } => {
            write!(f, "{}|{}|{}|{}", P_COLL_INDEX, profile_id, collection, name)
        }
        _ => unreachable!("fmt_collection_detail_variants called with an unexpected variant"),
    }
}

fn fmt_type_detail_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::EnumValue {
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
        SchemaNodeId::BaseType {
            profile_id,
            schema,
            type_name,
        } => {
            write!(f, "{}|{}|{}|{}", P_BASE_TYPE, profile_id, schema, type_name)
        }
        _ => unreachable!("fmt_type_detail_variants called with an unexpected variant"),
    }
}

fn fmt_placeholder_and_dependents(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::Placeholder {
            profile_id,
            schema,
            table,
        } => {
            write!(f, "{}|{}|{}|{}", P_PLACEHOLDER, profile_id, schema, table)
        }
        SchemaNodeId::DependentsFolder {
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
        SchemaNodeId::DependentItem {
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
        _ => unreachable!("fmt_placeholder_and_dependents called with an unexpected variant"),
    }
}

fn fmt_routine_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::RoutinesFolder {
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
        SchemaNodeId::RoutinesLoadingFolder {
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
        SchemaNodeId::Routine {
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
        _ => unreachable!("fmt_routine_variants called with an unexpected variant"),
    }
}

fn fmt_metric_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::MetricsFolder {
            profile_id,
            database,
        } => {
            write!(f, "{}|{}|{}", P_METRICS_FOLDER, profile_id, database)
        }
        SchemaNodeId::MetricNamespaceFolder {
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
        SchemaNodeId::MetricLeaf {
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
        _ => unreachable!("fmt_metric_variants called with an unexpected variant"),
    }
}

fn fmt_scripts_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::ScriptsFolder { path } => match path {
            Some(p) => write!(f, "{}|{}", P_SCRIPTS_FOLDER, p),
            None => write!(f, "{}", P_SCRIPTS_FOLDER),
        },
        SchemaNodeId::ScriptFile { path } => {
            write!(f, "{}|{}", P_SCRIPT_FILE, path)
        }
        _ => unreachable!("fmt_scripts_variants called with an unexpected variant"),
    }
}

fn fmt_dashboard_and_chart_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::DashboardsFolder { profile_id } => {
            write!(f, "{}|{}", P_DASHBOARDS_FOLDER, profile_id)
        }
        SchemaNodeId::DashboardItem {
            profile_id,
            dashboard_id,
        } => {
            write!(f, "{}|{}|{}", P_DASHBOARD_ITEM, profile_id, dashboard_id)
        }
        SchemaNodeId::RemoteDashboardsFolder { profile_id } => {
            write!(f, "{}|{}", P_REMOTE_DASHBOARDS_FOLDER, profile_id)
        }
        SchemaNodeId::RemoteDashboardItem { profile_id, name } => {
            write!(f, "{}|{}|{}", P_REMOTE_DASHBOARD_ITEM, profile_id, name)
        }
        SchemaNodeId::SavedChartsFolder { profile_id } => {
            write!(f, "{}|{}", P_SAVED_CHARTS_FOLDER, profile_id)
        }
        SchemaNodeId::SavedChartItem {
            profile_id,
            chart_id,
        } => {
            write!(f, "{}|{}|{}", P_SAVED_CHART_ITEM, profile_id, chart_id)
        }
        _ => unreachable!("fmt_dashboard_and_chart_variants called with an unexpected variant"),
    }
}

fn fmt_instance_variants(id: &SchemaNodeId, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match id {
        SchemaNodeId::InstanceMetricsFolder { profile_id } => {
            write!(f, "{}|{}", P_INST_METRICS_FOLDER, profile_id)
        }
        SchemaNodeId::InstanceMetricLeaf {
            profile_id,
            metric_id,
        } => {
            write!(f, "{}|{}|{}", P_INST_METRIC_LEAF, profile_id, metric_id)
        }
        SchemaNodeId::InstanceInspectorsFolder { profile_id } => {
            write!(f, "{}|{}", P_INST_INSPECTORS_FOLDER, profile_id)
        }
        SchemaNodeId::InstanceInspectorLeaf {
            profile_id,
            metric_id,
        } => {
            write!(f, "{}|{}|{}", P_INST_INSPECTOR_LEAF, profile_id, metric_id)
        }
        SchemaNodeId::InstanceOverviewLeaf { profile_id } => {
            write!(f, "{}|{}", P_INST_OVERVIEW_LEAF, profile_id)
        }
        _ => unreachable!("fmt_instance_variants called with an unexpected variant"),
    }
}

use std::str::FromStr;
use uuid::Uuid;

use super::ParseSchemaNodeIdError;
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
    P_SCRIPTS_FOLDER, P_STORAGE_HINT_ITEM, P_STORAGE_HINTS_FOLDER, P_TABLE, P_TABLES_FOLDER,
    P_TYPES_FOLDER, P_TYPES_LOADING, P_VIEW, P_VIEWS_FOLDER,
};

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
            P_CONN_FOLDER | P_PROFILE | P_DATABASES_FOLDER | P_DATABASE | P_LOADING | P_SCHEMA => {
                parse_connection_scope(prefix, &parts, err)
            }

            P_TABLES_FOLDER | P_VIEWS_FOLDER | P_TYPES_FOLDER | P_TYPES_LOADING
            | P_SCHEMA_IDX_FOLDER | P_SCHEMA_IDX_LOADING | P_SCHEMA_FK_FOLDER
            | P_SCHEMA_FK_LOADING | P_COLLECTIONS_FOLDER => {
                parse_folder_variants(prefix, &parts, err)
            }

            P_TABLE
            | P_VIEW
            | P_COLLECTION
            | P_COLLECTION_CHILD
            | P_COLLECTION_CHILDREN_MORE
            | P_CUSTOM_TYPE => parse_object_variants(prefix, &parts, err),

            P_COLUMNS_FOLDER
            | P_INDEXES_FOLDER
            | P_FK_FOLDER
            | P_CONSTRAINTS_FOLDER
            | P_STORAGE_HINTS_FOLDER => parse_table_detail_folders(prefix, &parts, err),

            P_COLUMN | P_INDEX | P_FK | P_CONSTRAINT | P_STORAGE_HINT_ITEM | P_SCHEMA_INDEX
            | P_SCHEMA_FK => parse_detail_variants(prefix, &parts, err),

            P_DB_IDX_FOLDER | P_COLL_FIELDS_FOLDER | P_COLL_FIELD | P_COLL_IDX_FOLDER
            | P_COLL_INDEX => parse_collection_detail_variants(prefix, &parts, err),

            P_ENUM_VALUE | P_BASE_TYPE => parse_type_detail_variants(prefix, &parts, err),

            P_PLACEHOLDER | P_DEPENDENTS_FOLDER | P_DEPENDENT_ITEM => {
                parse_placeholder_and_dependents(prefix, &parts, err)
            }

            P_SCRIPT_FILE => parse_script_file(&parts, err),

            P_ROUTINES_FOLDER | P_ROUTINES_LOADING | P_ROUTINE => {
                parse_routine_variants(prefix, &parts, err)
            }

            P_METRICS_FOLDER | P_METRIC_NS_FOLDER | P_METRIC_LEAF => {
                parse_metric_variants(prefix, &parts, err)
            }

            P_DASHBOARDS_FOLDER
            | P_DASHBOARD_ITEM
            | P_REMOTE_DASHBOARDS_FOLDER
            | P_REMOTE_DASHBOARD_ITEM
            | P_SAVED_CHARTS_FOLDER
            | P_SAVED_CHART_ITEM => parse_dashboard_and_chart_variants(prefix, &parts, err),

            P_INST_METRICS_FOLDER
            | P_INST_METRIC_LEAF
            | P_INST_INSPECTORS_FOLDER
            | P_INST_INSPECTOR_LEAF
            | P_INST_OVERVIEW_LEAF => parse_instance_variants(prefix, &parts, err),

            _ => Err(err()),
        }
    }
}

fn parse_connection_scope(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_CONN_FOLDER => {
            let node_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::ConnectionFolder { node_id })
        }

        P_PROFILE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::Profile { profile_id })
        }

        P_DATABASES_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::DatabasesFolder { profile_id })
        }

        P_DATABASE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let name = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Database { profile_id, name })
        }

        P_LOADING => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Loading {
                profile_id,
                database,
            })
        }

        P_SCHEMA => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let name = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Schema { profile_id, name })
        }

        _ => Err(err()),
    }
}

fn parse_folder_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_TABLES_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::TablesFolder { profile_id, schema })
        }

        P_VIEWS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::ViewsFolder { profile_id, schema })
        }

        P_TYPES_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::TypesFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_TYPES_LOADING => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::TypesLoadingFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_SCHEMA_IDX_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::SchemaIndexesFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_SCHEMA_IDX_LOADING => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::SchemaIndexesLoadingFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_SCHEMA_FK_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::SchemaForeignKeysFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_SCHEMA_FK_LOADING => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::SchemaForeignKeysLoadingFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_COLLECTIONS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CollectionsFolder {
                profile_id,
                database,
            })
        }

        _ => Err(err()),
    }
}

fn parse_object_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_TABLE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            let database = parts.get(4).map(|s| s.to_string());
            Ok(SchemaNodeId::Table {
                profile_id,
                database,
                schema,
                name,
            })
        }

        P_VIEW => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            let database = parts.get(4).map(|s| s.to_string());
            Ok(SchemaNodeId::View {
                profile_id,
                database,
                schema,
                name,
            })
        }

        P_COLLECTION => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Collection {
                profile_id,
                database,
                name,
            })
        }

        P_COLLECTION_CHILD => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let collection = parts.get(3).ok_or_else(err)?.to_string();
            let child_id = parts.get(4).ok_or_else(err)?.to_string();
            let name = parts.get(5).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CollectionChild {
                profile_id,
                database,
                collection,
                child_id,
                name,
            })
        }

        P_COLLECTION_CHILDREN_MORE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let collection = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CollectionChildrenMore {
                profile_id,
                database,
                collection,
            })
        }

        P_CUSTOM_TYPE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CustomType {
                profile_id,
                schema,
                name,
            })
        }

        _ => Err(err()),
    }
}

fn parse_table_detail_folders(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_COLUMNS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::ColumnsFolder {
                profile_id,
                schema,
                table,
            })
        }

        P_INDEXES_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::IndexesFolder {
                profile_id,
                schema,
                table,
            })
        }

        P_FK_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::ForeignKeysFolder {
                profile_id,
                schema,
                table,
            })
        }

        P_CONSTRAINTS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::ConstraintsFolder {
                profile_id,
                schema,
                table,
            })
        }

        P_STORAGE_HINTS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::StorageHintsFolder {
                profile_id,
                schema,
                table,
            })
        }

        _ => Err(err()),
    }
}

fn parse_detail_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_COLUMN => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let table = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Column {
                profile_id,
                table,
                name,
            })
        }

        P_INDEX => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let table = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Index {
                profile_id,
                table,
                name,
            })
        }

        P_FK => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let table = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::ForeignKey {
                profile_id,
                table,
                name,
            })
        }

        P_CONSTRAINT => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let table = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Constraint {
                profile_id,
                table,
                name,
            })
        }

        P_STORAGE_HINT_ITEM => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let table = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::StorageHintItem {
                profile_id,
                table,
                name,
            })
        }

        P_SCHEMA_INDEX => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::SchemaIndex {
                profile_id,
                schema,
                name,
            })
        }

        P_SCHEMA_FK => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::SchemaForeignKey {
                profile_id,
                schema,
                name,
            })
        }

        _ => Err(err()),
    }
}

fn parse_collection_detail_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_DB_IDX_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::DatabaseIndexesFolder {
                profile_id,
                database,
            })
        }

        P_COLL_FIELDS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let collection = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CollectionFieldsFolder {
                profile_id,
                database,
                collection,
            })
        }

        P_COLL_FIELD => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let collection = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CollectionField {
                profile_id,
                collection,
                name,
            })
        }

        P_COLL_IDX_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let collection = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CollectionIndexesFolder {
                profile_id,
                database,
                collection,
            })
        }

        P_COLL_INDEX => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let collection = parts.get(2).ok_or_else(err)?.to_string();
            let name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::CollectionIndex {
                profile_id,
                collection,
                name,
            })
        }

        _ => Err(err()),
    }
}

fn parse_type_detail_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_ENUM_VALUE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let type_name = parts.get(3).ok_or_else(err)?.to_string();
            let value = parts.get(4).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::EnumValue {
                profile_id,
                schema,
                type_name,
                value,
            })
        }

        P_BASE_TYPE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let type_name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::BaseType {
                profile_id,
                schema,
                type_name,
            })
        }

        _ => Err(err()),
    }
}

fn parse_placeholder_and_dependents(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_PLACEHOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Placeholder {
                profile_id,
                schema,
                table,
            })
        }

        P_DEPENDENTS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::DependentsFolder {
                profile_id,
                schema,
                table,
            })
        }

        P_DEPENDENT_ITEM => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            let table = parts.get(3).ok_or_else(err)?.to_string();
            let name = parts.get(4).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::DependentItem {
                profile_id,
                schema,
                table,
                name,
            })
        }

        _ => Err(err()),
    }
}

fn parse_script_file(
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    // Path may contain pipe characters, so rejoin everything after the prefix
    let path = parts[1..].join("|");
    if path.is_empty() {
        return Err(err());
    }
    Ok(SchemaNodeId::ScriptFile { path })
}

fn parse_routine_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_ROUTINES_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::RoutinesFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_ROUTINES_LOADING => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let schema = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::RoutinesLoadingFolder {
                profile_id,
                database,
                schema,
            })
        }

        P_ROUTINE => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let schema = parts.get(2).ok_or_else(err)?.to_string();
            // specific_name may contain commas and parens but no pipes;
            // with splitn(6) the 4th field absorbs any remaining content.
            let specific_name = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::Routine {
                profile_id,
                schema,
                specific_name,
            })
        }

        _ => Err(err()),
    }
}

fn parse_metric_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_METRICS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::MetricsFolder {
                profile_id,
                database,
            })
        }

        P_METRIC_NS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let namespace = parts.get(3).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::MetricNamespaceFolder {
                profile_id,
                database,
                namespace,
            })
        }

        P_METRIC_LEAF => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let database = parts.get(2).ok_or_else(err)?.to_string();
            let namespace = parts.get(3).ok_or_else(err)?.to_string();
            // metric_name may contain slashes; splitn(6) gives us all remaining content
            let metric_name = parts.get(4).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::MetricLeaf {
                profile_id,
                database,
                namespace,
                metric_name,
            })
        }

        _ => Err(err()),
    }
}

fn parse_dashboard_and_chart_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_DASHBOARDS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::DashboardsFolder { profile_id })
        }

        P_DASHBOARD_ITEM => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let dashboard_id = Uuid::parse_str(parts.get(2).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::DashboardItem {
                profile_id,
                dashboard_id,
            })
        }

        P_REMOTE_DASHBOARDS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::RemoteDashboardsFolder { profile_id })
        }

        P_REMOTE_DASHBOARD_ITEM => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let name = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::RemoteDashboardItem { profile_id, name })
        }

        P_SAVED_CHARTS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::SavedChartsFolder { profile_id })
        }

        P_SAVED_CHART_ITEM => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let chart_id = Uuid::parse_str(parts.get(2).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::SavedChartItem {
                profile_id,
                chart_id,
            })
        }

        _ => Err(err()),
    }
}

fn parse_instance_variants(
    prefix: &str,
    parts: &[&str],
    err: impl Fn() -> ParseSchemaNodeIdError + Copy,
) -> Result<SchemaNodeId, ParseSchemaNodeIdError> {
    match prefix {
        P_INST_METRICS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::InstanceMetricsFolder { profile_id })
        }

        P_INST_METRIC_LEAF => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let metric_id = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::InstanceMetricLeaf {
                profile_id,
                metric_id,
            })
        }

        P_INST_INSPECTORS_FOLDER => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::InstanceInspectorsFolder { profile_id })
        }

        P_INST_INSPECTOR_LEAF => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            let metric_id = parts.get(2).ok_or_else(err)?.to_string();
            Ok(SchemaNodeId::InstanceInspectorLeaf {
                profile_id,
                metric_id,
            })
        }

        P_INST_OVERVIEW_LEAF => {
            let profile_id = Uuid::parse_str(parts.get(1).ok_or_else(err)?).map_err(|_| err())?;
            Ok(SchemaNodeId::InstanceOverviewLeaf { profile_id })
        }

        _ => Err(err()),
    }
}

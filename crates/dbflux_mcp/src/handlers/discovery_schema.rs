use std::collections::HashMap;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionInfo {
    pub id: String,
    pub name: String,
    pub mcp_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionMetadata {
    pub connection_id: String,
    pub database_kind: String,
    pub supports_collections: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDescription {
    pub connection_id: String,
    pub database: String,
    pub schema: Option<String>,
    pub object_name: String,
    pub object_kind: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeObjectRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: Option<String>,
    pub object_name: String,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum DiscoverySchemaError {
    #[error("connection not found: {0}")]
    ConnectionNotFound(String),
    #[error("object not found: {0}")]
    ObjectNotFound(String),
}

#[derive(Debug, Clone, Default)]
pub struct DiscoverySchemaCatalog {
    connections: HashMap<String, ConnectionInfo>,
    metadata_by_connection: HashMap<String, ConnectionMetadata>,
    databases_by_connection: HashMap<String, Vec<String>>,
    schemas_by_database: HashMap<(String, String), Vec<String>>,
    tables_by_scope: HashMap<(String, String, Option<String>), Vec<String>>,
    collections_by_database: HashMap<(String, String), Vec<String>>,
    object_descriptions: HashMap<(String, String, Option<String>, String), ObjectDescription>,
}

impl DiscoverySchemaCatalog {
    pub fn insert_connection(
        &mut self,
        connection: ConnectionInfo,
        metadata: ConnectionMetadata,
        databases: Vec<String>,
    ) {
        self.databases_by_connection
            .insert(connection.id.clone(), sorted(databases));

        self.metadata_by_connection
            .insert(connection.id.clone(), metadata);

        self.connections.insert(connection.id.clone(), connection);
    }

    pub fn insert_schemas(
        &mut self,
        connection_id: impl Into<String>,
        database: impl Into<String>,
        schemas: Vec<String>,
    ) {
        self.schemas_by_database
            .insert((connection_id.into(), database.into()), sorted(schemas));
    }

    pub fn insert_tables(
        &mut self,
        connection_id: impl Into<String>,
        database: impl Into<String>,
        schema: Option<String>,
        tables: Vec<String>,
    ) {
        self.tables_by_scope.insert(
            (connection_id.into(), database.into(), schema),
            sorted(tables),
        );
    }

    pub fn insert_collections(
        &mut self,
        connection_id: impl Into<String>,
        database: impl Into<String>,
        collections: Vec<String>,
    ) {
        self.collections_by_database
            .insert((connection_id.into(), database.into()), sorted(collections));
    }

    pub fn insert_object_description(&mut self, object: ObjectDescription) {
        self.object_descriptions.insert(
            (
                object.connection_id.clone(),
                object.database.clone(),
                object.schema.clone(),
                object.object_name.clone(),
            ),
            object,
        );
    }

    pub fn list_connections(&self) -> Vec<ConnectionInfo> {
        let mut connections: Vec<_> = self.connections.values().cloned().collect();
        connections.sort_by(|left, right| left.id.cmp(&right.id));
        connections
    }

    pub fn get_connection(
        &self,
        connection_id: &str,
    ) -> Result<ConnectionInfo, DiscoverySchemaError> {
        self.connections
            .get(connection_id)
            .cloned()
            .ok_or_else(|| DiscoverySchemaError::ConnectionNotFound(connection_id.to_string()))
    }

    pub fn get_connection_metadata(
        &self,
        connection_id: &str,
    ) -> Result<ConnectionMetadata, DiscoverySchemaError> {
        self.metadata_by_connection
            .get(connection_id)
            .cloned()
            .ok_or_else(|| DiscoverySchemaError::ConnectionNotFound(connection_id.to_string()))
    }

    pub fn list_databases(&self, connection_id: &str) -> Result<Vec<String>, DiscoverySchemaError> {
        let Some(databases) = self.databases_by_connection.get(connection_id) else {
            return Err(DiscoverySchemaError::ConnectionNotFound(
                connection_id.to_string(),
            ));
        };

        Ok(databases.clone())
    }

    pub fn list_schemas(
        &self,
        connection_id: &str,
        database: &str,
    ) -> Result<Vec<String>, DiscoverySchemaError> {
        self.ensure_connection_exists(connection_id)?;

        Ok(self
            .schemas_by_database
            .get(&(connection_id.to_string(), database.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    pub fn list_tables(
        &self,
        connection_id: &str,
        database: &str,
        schema: Option<&str>,
    ) -> Result<Vec<String>, DiscoverySchemaError> {
        self.ensure_connection_exists(connection_id)?;

        Ok(self
            .tables_by_scope
            .get(&(
                connection_id.to_string(),
                database.to_string(),
                schema.map(ToString::to_string),
            ))
            .cloned()
            .unwrap_or_default())
    }

    pub fn list_collections(
        &self,
        connection_id: &str,
        database: &str,
    ) -> Result<Vec<String>, DiscoverySchemaError> {
        self.ensure_connection_exists(connection_id)?;

        Ok(self
            .collections_by_database
            .get(&(connection_id.to_string(), database.to_string()))
            .cloned()
            .unwrap_or_default())
    }

    pub fn describe_object(
        &self,
        request: &DescribeObjectRequest,
    ) -> Result<ObjectDescription, DiscoverySchemaError> {
        self.object_descriptions
            .get(&(
                request.connection_id.clone(),
                request.database.clone(),
                request.schema.clone(),
                request.object_name.clone(),
            ))
            .cloned()
            .ok_or_else(|| {
                DiscoverySchemaError::ObjectNotFound(format!(
                    "{}:{}:{}",
                    request.connection_id, request.database, request.object_name
                ))
            })
    }

    fn ensure_connection_exists(&self, connection_id: &str) -> Result<(), DiscoverySchemaError> {
        if self.connections.contains_key(connection_id) {
            return Ok(());
        }

        Err(DiscoverySchemaError::ConnectionNotFound(
            connection_id.to_string(),
        ))
    }
}

fn sorted(mut items: Vec<String>) -> Vec<String> {
    items.sort();
    items
}

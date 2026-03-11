use std::collections::HashMap;
use std::sync::LazyLock;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::config::{Builder as DynamoConfigBuilder, Credentials};
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodb::operation::list_tables::ListTablesError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::scan::ScanError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType, Select,
};
use aws_sdk_dynamodb::{Client, types::TableDescription};
use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionIndexInfo, CollectionInfo,
    CollectionRef, ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionProfile,
    DYNAMODB_FORM, DangerousQueryKind, DatabaseCategory, DatabaseInfo, DbConfig, DbDriver, DbError,
    DbKind, DbSchemaInfo, DocumentDelete, DocumentInsert, DocumentSchema, DocumentUpdate,
    DriverCapabilities, DriverFormDef, DriverMetadata, FieldInfo, FormValues, FormattedError, Icon,
    IndexData, IndexDirection, LanguageService, Pagination, QueryErrorFormatter, QueryLanguage,
    QueryRequest, QueryResult, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect, TableInfo,
    ValidationResult, Value,
};

use crate::query_generator::DynamoQueryGenerator;
use crate::query_parser::{DynamoCommandEnvelope, parse_command_envelope};

const DYNAMODB_DEFAULT_DATABASE: &str = "dynamodb";

pub static DYNAMODB_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "dynamodb".into(),
    display_name: "DynamoDB".into(),
    description: "AWS managed NoSQL key-value and document database".into(),
    category: DatabaseCategory::Document,
    query_language: QueryLanguage::Custom("DynamoDB".into()),
    capabilities: DriverCapabilities::from_bits_truncate(
        DriverCapabilities::AUTHENTICATION.bits()
            | DriverCapabilities::PAGINATION.bits()
            | DriverCapabilities::FILTERING.bits()
            | DriverCapabilities::INSERT.bits()
            | DriverCapabilities::UPDATE.bits()
            | DriverCapabilities::DELETE.bits(),
    ),
    default_port: None,
    uri_scheme: "dynamodb".into(),
    icon: Icon::Dynamodb,
});

pub const DYNAMODB_MVP_SUPPORTED_FLOWS: &[&str] = &[
    "connect",
    "test_connection",
    "list_tables",
    "table_details",
    "browse_collection_scan",
    "browse_collection_query_when_key_predicate_is_valid",
    "count_collection",
    "insert_document_single_item_put",
    "update_document_single_item_update",
    "delete_document_single_item_delete",
    "execute_scan",
    "execute_query",
    "execute_put",
    "execute_update",
    "execute_delete",
];

pub const DYNAMODB_MVP_UNSUPPORTED_FLOWS: &[&str] = &[
    "multi_item_transactions",
    "advanced_partiql_workflows",
    "streams_changefeeds",
    "dax",
    "global_tables_controls",
    "bulk_many_update",
    "bulk_many_delete",
    "specialized_dynamodb_ui_panels",
];

pub struct DynamoDriver;

impl DynamoDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DynamoDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for DynamoDriver {
    fn kind(&self) -> DbKind {
        DbKind::DynamoDB
    }

    fn metadata(&self) -> &DriverMetadata {
        &DYNAMODB_METADATA
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:dynamodb".into()
    }

    fn form_definition(&self) -> &DriverFormDef {
        &DYNAMODB_FORM
    }

    fn build_config(&self, values: &FormValues) -> Result<DbConfig, DbError> {
        let region = values
            .get("region")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| DbError::InvalidProfile("AWS Region is required".to_string()))?
            .to_string();

        let profile = values
            .get("profile")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());

        let endpoint = values
            .get("endpoint")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());

        let table = values
            .get("table")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string());

        Ok(DbConfig::DynamoDB {
            region,
            profile,
            endpoint,
            table,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let DbConfig::DynamoDB {
            region,
            profile,
            endpoint,
            table,
        } = config
        else {
            return HashMap::new();
        };

        let mut values = HashMap::new();
        values.insert("region".to_string(), region.clone());
        values.insert("profile".to_string(), profile.clone().unwrap_or_default());
        values.insert("endpoint".to_string(), endpoint.clone().unwrap_or_default());
        values.insert("table".to_string(), table.clone().unwrap_or_default());

        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        _password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = profile_config(&profile.config)?;
        let client = build_client(&config)?;

        probe_connection(&client, &config)?;

        Ok(Box::new(DynamoConnection {
            client,
            default_region: config.region,
            default_table: config.table,
        }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let config = profile_config(&profile.config)?;
        let client = build_client(&config)?;

        probe_connection(&client, &config)
    }
}

struct DynamoConnection {
    client: Client,
    default_region: String,
    default_table: Option<String>,
}

impl Connection for DynamoConnection {
    fn metadata(&self) -> &DriverMetadata {
        &DYNAMODB_METADATA
    }

    fn ping(&self) -> Result<(), DbError> {
        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        probe_connection(&self.client, &config)
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let started = std::time::Instant::now();
        let envelope = parse_command_envelope(&req.sql)?;

        let mut result = match envelope {
            DynamoCommandEnvelope::Scan {
                database,
                table,
                filter,
                limit,
                offset,
            }
            | DynamoCommandEnvelope::Query {
                database,
                table,
                filter,
                limit,
                offset,
            } => {
                let resolved_database = database
                    .or_else(|| req.database.clone())
                    .unwrap_or_else(|| DYNAMODB_DEFAULT_DATABASE.to_string());

                let pagination = Pagination::Offset {
                    limit: limit.or(req.limit).unwrap_or(100),
                    offset: offset.or(req.offset.map(u64::from)).unwrap_or(0),
                };

                let request = CollectionBrowseRequest {
                    collection: CollectionRef::new(resolved_database, table),
                    pagination,
                    filter,
                };

                self.browse_collection(&request)?
            }
            DynamoCommandEnvelope::Put {
                database,
                table,
                item,
            } => {
                let insert = DocumentInsert {
                    collection: table,
                    database: Some(
                        database
                            .or_else(|| req.database.clone())
                            .unwrap_or_else(|| DYNAMODB_DEFAULT_DATABASE.to_string()),
                    ),
                    documents: vec![item],
                };

                crud_result_to_query_result(self.insert_document(&insert)?)
            }
            DynamoCommandEnvelope::Update {
                database,
                table,
                key,
                update,
                many,
                upsert,
            } => {
                let update_request = DocumentUpdate {
                    collection: table,
                    database: Some(
                        database
                            .or_else(|| req.database.clone())
                            .unwrap_or_else(|| DYNAMODB_DEFAULT_DATABASE.to_string()),
                    ),
                    filter: dbflux_core::DocumentFilter { filter: key },
                    update,
                    many,
                    upsert,
                };

                crud_result_to_query_result(self.update_document(&update_request)?)
            }
            DynamoCommandEnvelope::Delete {
                database,
                table,
                key,
                many,
            } => {
                let delete_request = DocumentDelete {
                    collection: table,
                    database: Some(
                        database
                            .or_else(|| req.database.clone())
                            .unwrap_or_else(|| DYNAMODB_DEFAULT_DATABASE.to_string()),
                    ),
                    filter: dbflux_core::DocumentFilter { filter: key },
                    many,
                };

                crud_result_to_query_result(self.delete_document(&delete_request)?)
            }
        };

        result.execution_time = started.elapsed();
        Ok(result)
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation is not supported for DynamoDB in Phase 4".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let table_names = self.fetch_table_names()?;
        let collections = table_names
            .iter()
            .map(|table_name| CollectionInfo {
                name: table_name.clone(),
                database: Some(DYNAMODB_DEFAULT_DATABASE.to_string()),
                document_count: None,
                avg_document_size: None,
                sample_fields: None,
                indexes: None,
                validator: None,
                is_capped: false,
            })
            .collect();

        Ok(SchemaSnapshot::document(DocumentSchema {
            databases: vec![DatabaseInfo {
                name: DYNAMODB_DEFAULT_DATABASE.to_string(),
                is_current: true,
            }],
            current_database: Some(DYNAMODB_DEFAULT_DATABASE.to_string()),
            collections,
        }))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        Ok(vec![DatabaseInfo {
            name: DYNAMODB_DEFAULT_DATABASE.to_string(),
            is_current: true,
        }])
    }

    fn schema_for_database(&self, database: &str) -> Result<DbSchemaInfo, DbError> {
        if database != DYNAMODB_DEFAULT_DATABASE {
            return Err(DbError::object_not_found(format!(
                "Database '{}' is not available for DynamoDB",
                database
            )));
        }

        let table_names = self.fetch_table_names()?;
        let tables = table_names
            .into_iter()
            .map(|name| TableInfo {
                name,
                schema: Some(DYNAMODB_DEFAULT_DATABASE.to_string()),
                columns: None,
                indexes: None,
                foreign_keys: None,
                constraints: None,
                sample_fields: None,
            })
            .collect();

        Ok(DbSchemaInfo {
            name: DYNAMODB_DEFAULT_DATABASE.to_string(),
            tables,
            views: Vec::new(),
            custom_types: None,
        })
    }

    fn table_details(
        &self,
        database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        if database != DYNAMODB_DEFAULT_DATABASE {
            return Err(DbError::object_not_found(format!(
                "Database '{}' is not available for DynamoDB",
                database
            )));
        }

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;
        let output = runtime
            .block_on(self.client.describe_table().table_name(table).send())
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_describe_error(&error, &config);
                classify_connection_error(formatted)
            })?;

        let description = output
            .table()
            .ok_or_else(|| DbError::object_not_found(format!("Table '{}' was not found", table)))?;

        Ok(build_table_info_from_description(
            table,
            DYNAMODB_DEFAULT_DATABASE,
            description,
        ))
    }

    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        if request.collection.database != DYNAMODB_DEFAULT_DATABASE {
            return Err(DbError::object_not_found(format!(
                "Database '{}' is not available for DynamoDB",
                request.collection.database
            )));
        }

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let key_schema = self.fetch_table_key_schema(&request.collection.name)?;
        let read_strategy = decide_read_strategy(request.filter.as_ref(), &key_schema)?;

        let page = self.read_items_page(
            &request.collection.name,
            &read_strategy,
            request.pagination.offset(),
            request.pagination.limit() as u64,
            &config,
        )?;

        let _ = page.has_more;
        Ok(items_to_query_result(&page.items))
    }

    fn count_collection(&self, request: &CollectionCountRequest) -> Result<u64, DbError> {
        if request.collection.database != DYNAMODB_DEFAULT_DATABASE {
            return Err(DbError::object_not_found(format!(
                "Database '{}' is not available for DynamoDB",
                request.collection.database
            )));
        }

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let key_schema = self.fetch_table_key_schema(&request.collection.name)?;
        let read_strategy = decide_read_strategy(request.filter.as_ref(), &key_schema)?;

        self.count_items(&request.collection.name, &read_strategy, &config)
    }

    fn insert_document(&self, insert: &DocumentInsert) -> Result<dbflux_core::CrudResult, DbError> {
        if insert.documents.len() != 1 {
            return Err(unsupported_non_mvp_operation(
                "insert_many_documents",
                "DynamoDB MVP supports only single-item put operations.",
            ));
        }

        let database = insert
            .database
            .as_deref()
            .unwrap_or(DYNAMODB_DEFAULT_DATABASE);
        if database != DYNAMODB_DEFAULT_DATABASE {
            return Err(DbError::object_not_found(format!(
                "Database '{}' is not available for DynamoDB",
                database
            )));
        }

        let item_json = insert
            .documents
            .first()
            .ok_or_else(|| DbError::query_failed("Document payload is required"))?;

        let item_map = json_object_to_attribute_map(item_json)?;

        let key_schema = self.fetch_table_key_schema(&insert.collection)?;
        ensure_item_contains_required_keys(&item_map, &key_schema)?;

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;
        runtime
            .block_on(
                self.client
                    .put_item()
                    .table_name(&insert.collection)
                    .set_item(Some(item_map))
                    .send(),
            )
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_put_error(&error, &config);
                classify_query_error(formatted)
            })?;

        Ok(dbflux_core::CrudResult::new(1, None))
    }

    fn update_document(&self, update: &DocumentUpdate) -> Result<dbflux_core::CrudResult, DbError> {
        if update.many {
            return Err(unsupported_non_mvp_operation(
                "update_many_documents",
                "DynamoDB MVP does not support many-item update semantics.",
            ));
        }

        if update.upsert {
            return Err(unsupported_non_mvp_operation(
                "upsert_document",
                "DynamoDB MVP does not support upsert semantics through document update.",
            ));
        }

        let database = update
            .database
            .as_deref()
            .unwrap_or(DYNAMODB_DEFAULT_DATABASE);
        if database != DYNAMODB_DEFAULT_DATABASE {
            return Err(DbError::object_not_found(format!(
                "Database '{}' is not available for DynamoDB",
                database
            )));
        }

        let key_schema = self.fetch_table_key_schema(&update.collection)?;
        let key_map = extract_key_map_from_filter(&update.filter.filter, &key_schema)?;

        let (update_expression, names, values) =
            build_update_expression_from_json(&update.update, &key_schema)?;

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;
        runtime
            .block_on(
                self.client
                    .update_item()
                    .table_name(&update.collection)
                    .set_key(Some(key_map))
                    .update_expression(update_expression)
                    .set_expression_attribute_names(Some(names))
                    .set_expression_attribute_values(Some(values))
                    .send(),
            )
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_update_error(&error, &config);
                classify_query_error(formatted)
            })?;

        Ok(dbflux_core::CrudResult::new(1, None))
    }

    fn delete_document(&self, delete: &DocumentDelete) -> Result<dbflux_core::CrudResult, DbError> {
        if delete.many {
            return Err(unsupported_non_mvp_operation(
                "delete_many_documents",
                "DynamoDB MVP does not support many-item delete semantics.",
            ));
        }

        let database = delete
            .database
            .as_deref()
            .unwrap_or(DYNAMODB_DEFAULT_DATABASE);
        if database != DYNAMODB_DEFAULT_DATABASE {
            return Err(DbError::object_not_found(format!(
                "Database '{}' is not available for DynamoDB",
                database
            )));
        }

        let key_schema = self.fetch_table_key_schema(&delete.collection)?;
        let key_map = extract_key_map_from_filter(&delete.filter.filter, &key_schema)?;

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;
        runtime
            .block_on(
                self.client
                    .delete_item()
                    .table_name(&delete.collection)
                    .set_key(Some(key_map))
                    .send(),
            )
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_delete_error(&error, &config);
                classify_query_error(formatted)
            })?;

        Ok(dbflux_core::CrudResult::new(1, None))
    }

    fn language_service(&self) -> &dyn LanguageService {
        &DYNAMO_LANGUAGE_SERVICE
    }

    fn query_generator(&self) -> Option<&dyn dbflux_core::QueryGenerator> {
        static GENERATOR: DynamoQueryGenerator = DynamoQueryGenerator;
        Some(&GENERATOR)
    }

    fn kind(&self) -> DbKind {
        DbKind::DynamoDB
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn dialect(&self) -> &dyn SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }
}

impl DynamoConnection {
    fn fetch_table_names(&self) -> Result<Vec<String>, DbError> {
        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;
        let mut names = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let request = match &cursor {
                Some(start) => self
                    .client
                    .list_tables()
                    .exclusive_start_table_name(start)
                    .limit(100),
                None => self.client.list_tables().limit(100),
            };

            let output = runtime.block_on(request.send()).map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_probe_error(&error, &config);
                classify_connection_error(formatted)
            })?;

            for name in output.table_names() {
                names.push(name.clone());
            }

            cursor = output
                .last_evaluated_table_name()
                .map(|value| value.to_string());
            if cursor.is_none() {
                break;
            }
        }

        normalize_table_names(names)
    }

    fn fetch_table_key_schema(&self, table: &str) -> Result<DynamoTableKeySchema, DbError> {
        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;
        let output = runtime
            .block_on(self.client.describe_table().table_name(table).send())
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_describe_error(&error, &config);
                classify_connection_error(formatted)
            })?;

        let description = output
            .table()
            .ok_or_else(|| DbError::object_not_found(format!("Table '{}' was not found", table)))?;

        let keys = extract_key_components(
            description.key_schema(),
            description.attribute_definitions(),
        );

        let partition_key = keys
            .iter()
            .find(|component| component.role == DynamoKeyRole::Partition)
            .map(|component| component.name.clone());

        let sort_key = keys
            .iter()
            .find(|component| component.role == DynamoKeyRole::Sort)
            .map(|component| component.name.clone());

        Ok(DynamoTableKeySchema {
            partition_key,
            sort_key,
        })
    }

    fn read_items_page(
        &self,
        table: &str,
        strategy: &DynamoReadStrategy,
        offset: u64,
        limit: u64,
        config: &DynamoProfileConfig,
    ) -> Result<DynamoReadPage, DbError> {
        if limit == 0 {
            return Ok(DynamoReadPage {
                items: Vec::new(),
                has_more: false,
            });
        }

        let runtime = runtime()?;
        let mut remaining_skip = offset;
        let mut collected = Vec::new();
        let mut cursor: Option<HashMap<String, AttributeValue>> = None;
        let mut has_more = false;

        loop {
            if collected.len() >= limit as usize {
                break;
            }

            let request_limit = std::cmp::max(
                1,
                std::cmp::min(
                    100,
                    remaining_skip.saturating_add((limit as usize - collected.len()) as u64),
                ) as i32,
            );

            let page = fetch_read_page(
                &self.client,
                table,
                strategy,
                request_limit,
                cursor.clone(),
                &runtime,
                config,
            )?;

            if page.items.is_empty() {
                if page.last_evaluated_key.is_none() {
                    break;
                }

                cursor = page.last_evaluated_key;
                continue;
            }

            let page_has_more = append_window_items(
                &page.items,
                &mut remaining_skip,
                &mut collected,
                limit as usize,
            );
            has_more = has_more || page_has_more;

            if collected.len() >= limit as usize {
                has_more = has_more || page.last_evaluated_key.is_some();
                break;
            }

            cursor = page.last_evaluated_key;
            if cursor.is_none() {
                break;
            }
        }

        Ok(DynamoReadPage {
            items: collected,
            has_more,
        })
    }

    fn count_items(
        &self,
        table: &str,
        strategy: &DynamoReadStrategy,
        config: &DynamoProfileConfig,
    ) -> Result<u64, DbError> {
        let runtime = runtime()?;
        let mut total: u64 = 0;
        let mut cursor: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let page = fetch_count_page(
                &self.client,
                table,
                strategy,
                cursor.clone(),
                &runtime,
                config,
            )?;

            total = total.saturating_add(page.count as u64);
            cursor = page.last_evaluated_key;

            if cursor.is_none() {
                break;
            }
        }

        Ok(total)
    }
}

fn append_window_items<T: Clone>(
    page_items: &[T],
    remaining_skip: &mut u64,
    collected: &mut Vec<T>,
    limit: usize,
) -> bool {
    let start_index = std::cmp::min(*remaining_skip as usize, page_items.len());
    *remaining_skip = (*remaining_skip).saturating_sub(start_index as u64);

    let mut has_more = false;
    for item in page_items.iter().skip(start_index) {
        if collected.len() >= limit {
            has_more = true;
            break;
        }

        collected.push(item.clone());
    }

    has_more
}

#[derive(Debug, Clone)]
struct DynamoProfileConfig {
    region: String,
    profile: Option<String>,
    endpoint: Option<String>,
    table: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DynamoKeyRole {
    Partition,
    Sort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DynamoKeyComponent {
    name: String,
    role: DynamoKeyRole,
    attribute_type: String,
}

#[derive(Debug, Clone, Default)]
struct DynamoTableKeySchema {
    partition_key: Option<String>,
    sort_key: Option<String>,
}

#[derive(Debug, Clone)]
enum DynamoReadStrategy {
    Scan,
    Query(DynamoQueryPlan),
}

#[derive(Debug, Clone)]
struct DynamoQueryPlan {
    key_condition_expression: String,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
}

type DynamoUpdateExpressionParts = (
    String,
    HashMap<String, String>,
    HashMap<String, AttributeValue>,
);

#[derive(Debug, Clone)]
struct DynamoFetchedPage {
    items: Vec<HashMap<String, AttributeValue>>,
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone)]
struct DynamoCountPage {
    count: i32,
    last_evaluated_key: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone)]
struct DynamoReadPage {
    items: Vec<HashMap<String, AttributeValue>>,
    has_more: bool,
}

fn profile_config(config: &DbConfig) -> Result<DynamoProfileConfig, DbError> {
    let DbConfig::DynamoDB {
        region,
        profile,
        endpoint,
        table,
    } = config
    else {
        return Err(DbError::InvalidProfile(
            "Expected DynamoDB configuration".to_string(),
        ));
    };

    let trimmed_region = region.trim();
    if trimmed_region.is_empty() {
        return Err(DbError::InvalidProfile(
            "AWS Region is required".to_string(),
        ));
    }

    Ok(DynamoProfileConfig {
        region: trimmed_region.to_string(),
        profile: profile
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string()),
        endpoint: endpoint
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string()),
        table: table
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string()),
    })
}

fn decide_read_strategy(
    filter: Option<&serde_json::Value>,
    key_schema: &DynamoTableKeySchema,
) -> Result<DynamoReadStrategy, DbError> {
    let Some(partition_key) = key_schema.partition_key.as_ref() else {
        return Ok(DynamoReadStrategy::Scan);
    };

    let Some(filter_obj) = filter.and_then(serde_json::Value::as_object) else {
        return Ok(DynamoReadStrategy::Scan);
    };

    let Some(partition_value_json) = filter_obj.get(partition_key) else {
        return Ok(DynamoReadStrategy::Scan);
    };

    let mut expression_attribute_names = HashMap::new();
    let mut expression_attribute_values = HashMap::new();

    expression_attribute_names.insert("#pk".to_string(), partition_key.clone());
    expression_attribute_values.insert(
        ":pk".to_string(),
        json_value_to_attribute_value(partition_value_json)?,
    );

    let mut key_condition_expression = "#pk = :pk".to_string();

    if let Some(sort_key) = key_schema.sort_key.as_ref()
        && let Some(sort_value_json) = filter_obj.get(sort_key)
    {
        expression_attribute_names.insert("#sk".to_string(), sort_key.clone());
        expression_attribute_values.insert(
            ":sk".to_string(),
            json_value_to_attribute_value(sort_value_json)?,
        );

        key_condition_expression.push_str(" AND #sk = :sk");
    }

    Ok(DynamoReadStrategy::Query(DynamoQueryPlan {
        key_condition_expression,
        expression_attribute_names,
        expression_attribute_values,
    }))
}

fn fetch_read_page(
    client: &Client,
    table: &str,
    strategy: &DynamoReadStrategy,
    limit: i32,
    start_key: Option<HashMap<String, AttributeValue>>,
    runtime: &tokio::runtime::Runtime,
    config: &DynamoProfileConfig,
) -> Result<DynamoFetchedPage, DbError> {
    match strategy {
        DynamoReadStrategy::Scan => {
            let output = runtime
                .block_on(
                    client
                        .scan()
                        .table_name(table)
                        .limit(limit)
                        .set_exclusive_start_key(start_key)
                        .send(),
                )
                .map_err(|error| {
                    let formatted = DYNAMO_ERROR_FORMATTER.format_scan_error(&error, config);
                    classify_query_error(formatted)
                })?;

            Ok(DynamoFetchedPage {
                items: output.items().to_vec(),
                last_evaluated_key: output.last_evaluated_key().cloned(),
            })
        }
        DynamoReadStrategy::Query(plan) => {
            let output = runtime
                .block_on(
                    client
                        .query()
                        .table_name(table)
                        .key_condition_expression(&plan.key_condition_expression)
                        .set_expression_attribute_names(Some(
                            plan.expression_attribute_names.clone(),
                        ))
                        .set_expression_attribute_values(Some(
                            plan.expression_attribute_values.clone(),
                        ))
                        .limit(limit)
                        .set_exclusive_start_key(start_key)
                        .send(),
                )
                .map_err(|error| {
                    let formatted = DYNAMO_ERROR_FORMATTER.format_query_op_error(&error, config);
                    classify_query_error(formatted)
                })?;

            Ok(DynamoFetchedPage {
                items: output.items().to_vec(),
                last_evaluated_key: output.last_evaluated_key().cloned(),
            })
        }
    }
}

fn fetch_count_page(
    client: &Client,
    table: &str,
    strategy: &DynamoReadStrategy,
    start_key: Option<HashMap<String, AttributeValue>>,
    runtime: &tokio::runtime::Runtime,
    config: &DynamoProfileConfig,
) -> Result<DynamoCountPage, DbError> {
    match strategy {
        DynamoReadStrategy::Scan => {
            let output = runtime
                .block_on(
                    client
                        .scan()
                        .table_name(table)
                        .select(Select::Count)
                        .set_exclusive_start_key(start_key)
                        .send(),
                )
                .map_err(|error| {
                    let formatted = DYNAMO_ERROR_FORMATTER.format_scan_error(&error, config);
                    classify_query_error(formatted)
                })?;

            Ok(DynamoCountPage {
                count: output.count(),
                last_evaluated_key: output.last_evaluated_key().cloned(),
            })
        }
        DynamoReadStrategy::Query(plan) => {
            let output = runtime
                .block_on(
                    client
                        .query()
                        .table_name(table)
                        .key_condition_expression(&plan.key_condition_expression)
                        .set_expression_attribute_names(Some(
                            plan.expression_attribute_names.clone(),
                        ))
                        .set_expression_attribute_values(Some(
                            plan.expression_attribute_values.clone(),
                        ))
                        .select(Select::Count)
                        .set_exclusive_start_key(start_key)
                        .send(),
                )
                .map_err(|error| {
                    let formatted = DYNAMO_ERROR_FORMATTER.format_query_op_error(&error, config);
                    classify_query_error(formatted)
                })?;

            Ok(DynamoCountPage {
                count: output.count(),
                last_evaluated_key: output.last_evaluated_key().cloned(),
            })
        }
    }
}

fn items_to_query_result(items: &[HashMap<String, AttributeValue>]) -> QueryResult {
    if items.is_empty() {
        return QueryResult::json(Vec::new(), Vec::new(), std::time::Duration::ZERO);
    }

    let mut field_names = Vec::new();
    let mut seen = std::collections::BTreeSet::new();

    for item in items {
        let mut keys: Vec<&String> = item.keys().collect();
        keys.sort();

        for key in keys {
            if seen.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }
    }

    if let Some(position) = field_names.iter().position(|name| name == "_id") {
        let key = field_names.remove(position);
        field_names.insert(0, key);
    }

    let columns = field_names
        .iter()
        .map(|name| ColumnMeta {
            name: name.clone(),
            type_name: "DynamoDB".to_string(),
            nullable: true,
        })
        .collect();

    let rows = items
        .iter()
        .map(|item| {
            field_names
                .iter()
                .map(|field| {
                    item.get(field)
                        .map(attribute_value_to_value)
                        .unwrap_or(Value::Null)
                })
                .collect()
        })
        .collect();

    QueryResult::json(columns, rows, std::time::Duration::ZERO)
}

fn crud_result_to_query_result(result: dbflux_core::CrudResult) -> QueryResult {
    let mut query_result = QueryResult::json(Vec::new(), Vec::new(), std::time::Duration::ZERO);
    query_result.affected_rows = Some(result.affected_rows);
    query_result
}

fn attribute_value_to_value(value: &AttributeValue) -> Value {
    if let Ok(string) = value.as_s() {
        return Value::Text(string.clone());
    }

    if let Ok(number) = value.as_n() {
        if let Ok(int_value) = number.parse::<i64>() {
            return Value::Int(int_value);
        }
        return Value::Decimal(number.clone());
    }

    if let Ok(boolean) = value.as_bool() {
        return Value::Bool(*boolean);
    }

    if let Ok(is_null) = value.as_null()
        && *is_null
    {
        return Value::Null;
    }

    if let Ok(map) = value.as_m() {
        let mut out = std::collections::BTreeMap::new();
        for (key, nested) in map {
            out.insert(key.clone(), attribute_value_to_value(nested));
        }
        return Value::Document(out);
    }

    if let Ok(list) = value.as_l() {
        return Value::Array(list.iter().map(attribute_value_to_value).collect());
    }

    if let Ok(blob) = value.as_b() {
        return Value::Bytes(blob.as_ref().to_vec());
    }

    if let Ok(strings) = value.as_ss() {
        return Value::Array(
            strings
                .iter()
                .map(|item| Value::Text(item.clone()))
                .collect(),
        );
    }

    if let Ok(numbers) = value.as_ns() {
        return Value::Array(
            numbers
                .iter()
                .map(|item| {
                    item.parse::<i64>()
                        .map(Value::Int)
                        .unwrap_or_else(|_| Value::Decimal(item.clone()))
                })
                .collect(),
        );
    }

    if let Ok(blobs) = value.as_bs() {
        return Value::Array(
            blobs
                .iter()
                .map(|item| Value::Bytes(item.as_ref().to_vec()))
                .collect(),
        );
    }

    Value::Unsupported(format!("{value:?}"))
}

fn json_value_to_attribute_value(value: &serde_json::Value) -> Result<AttributeValue, DbError> {
    match value {
        serde_json::Value::Null => Ok(AttributeValue::Null(true)),
        serde_json::Value::Bool(boolean) => Ok(AttributeValue::Bool(*boolean)),
        serde_json::Value::Number(number) => Ok(AttributeValue::N(number.to_string())),
        serde_json::Value::String(string) => Ok(AttributeValue::S(string.clone())),
        serde_json::Value::Array(items) => {
            let converted = items
                .iter()
                .map(json_value_to_attribute_value)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AttributeValue::L(converted))
        }
        serde_json::Value::Object(map) => {
            let converted = map
                .iter()
                .map(|(key, nested)| {
                    json_value_to_attribute_value(nested).map(|converted| (key.clone(), converted))
                })
                .collect::<Result<HashMap<_, _>, _>>()?;
            Ok(AttributeValue::M(converted))
        }
    }
}

fn json_object_to_attribute_map(
    value: &serde_json::Value,
) -> Result<HashMap<String, AttributeValue>, DbError> {
    let object = value
        .as_object()
        .ok_or_else(|| DbError::query_failed("DynamoDB item payload must be a JSON object"))?;

    object
        .iter()
        .map(|(key, nested)| {
            json_value_to_attribute_value(nested).map(|converted| (key.clone(), converted))
        })
        .collect()
}

fn ensure_item_contains_required_keys(
    item: &HashMap<String, AttributeValue>,
    key_schema: &DynamoTableKeySchema,
) -> Result<(), DbError> {
    if let Some(partition_key) = key_schema.partition_key.as_ref()
        && !item.contains_key(partition_key)
    {
        return Err(DbError::query_failed(format!(
            "Missing required partition key '{}' for PutItem",
            partition_key
        )));
    }

    if let Some(sort_key) = key_schema.sort_key.as_ref()
        && !item.contains_key(sort_key)
    {
        return Err(DbError::query_failed(format!(
            "Missing required sort key '{}' for PutItem",
            sort_key
        )));
    }

    Ok(())
}

fn extract_key_map_from_filter(
    filter: &serde_json::Value,
    key_schema: &DynamoTableKeySchema,
) -> Result<HashMap<String, AttributeValue>, DbError> {
    let filter_object = filter
        .as_object()
        .ok_or_else(|| DbError::query_failed("DynamoDB key filter must be a JSON object"))?;

    let mut key_map = HashMap::new();

    let partition_key = key_schema.partition_key.as_ref().ok_or_else(|| {
        DbError::query_failed(
            "Table metadata is missing a partition key; cannot resolve item identity",
        )
    })?;

    let partition_value = filter_object.get(partition_key).ok_or_else(|| {
        DbError::query_failed(format!(
            "DynamoDB mutation requires partition key '{}' in filter",
            partition_key
        ))
    })?;
    key_map.insert(
        partition_key.clone(),
        json_value_to_attribute_value(partition_value)?,
    );

    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        let sort_value = filter_object.get(sort_key).ok_or_else(|| {
            DbError::query_failed(format!(
                "DynamoDB mutation requires sort key '{}' in filter",
                sort_key
            ))
        })?;
        key_map.insert(sort_key.clone(), json_value_to_attribute_value(sort_value)?);
    }

    Ok(key_map)
}

fn build_update_expression_from_json(
    update: &serde_json::Value,
    key_schema: &DynamoTableKeySchema,
) -> Result<DynamoUpdateExpressionParts, DbError> {
    let root = update
        .as_object()
        .ok_or_else(|| DbError::query_failed("DynamoDB update payload must be a JSON object"))?;

    let set_object = if let Some(explicit_set) = root.get("$set") {
        explicit_set
            .as_object()
            .ok_or_else(|| DbError::query_failed("$set must be a JSON object"))?
    } else {
        root
    };

    if set_object.is_empty() {
        return Err(DbError::query_failed(
            "DynamoDB update payload must include at least one field",
        ));
    }

    let mut key_names = std::collections::HashSet::new();
    if let Some(partition_key) = key_schema.partition_key.as_ref() {
        key_names.insert(partition_key.as_str());
    }
    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        key_names.insert(sort_key.as_str());
    }

    let mut names = HashMap::new();
    let mut values = HashMap::new();
    let mut assignments = Vec::new();

    for (index, (field, field_value)) in set_object.iter().enumerate() {
        if field.starts_with('$') {
            return Err(DbError::NotSupported(format!(
                "DynamoDB MVP supports only plain field updates and optional '$set'; operator '{}' is not supported",
                field
            )));
        }

        if key_names.contains(field.as_str()) {
            return Err(DbError::query_failed(format!(
                "DynamoDB key field '{}' cannot be updated; provide it in the filter instead",
                field
            )));
        }

        let name_token = format!("#u{index}");
        let value_token = format!(":v{index}");

        names.insert(name_token.clone(), field.clone());
        values.insert(
            value_token.clone(),
            json_value_to_attribute_value(field_value)?,
        );
        assignments.push(format!("{name_token} = {value_token}"));
    }

    if assignments.is_empty() {
        return Err(DbError::query_failed(
            "DynamoDB update payload must include at least one field",
        ));
    }

    Ok((format!("SET {}", assignments.join(", ")), names, values))
}

fn unsupported_non_mvp_operation(operation: &str, message: &str) -> DbError {
    DbError::NotSupported(format!("{message} (operation={operation})"))
}

fn build_client(config: &DynamoProfileConfig) -> Result<Client, DbError> {
    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(config.region.clone()));

    if let Some(profile) = &config.profile {
        loader = loader.profile_name(profile);
    }

    let runtime = runtime()?;
    let sdk_config = runtime.block_on(loader.load());

    let mut builder = DynamoConfigBuilder::from(&sdk_config);
    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint_url(endpoint);

        if endpoint_looks_local(endpoint)
            && config.profile.is_none()
            && !has_environment_credentials()
        {
            builder = builder.credentials_provider(Credentials::new(
                "test",
                "test",
                None,
                None,
                "dbflux-dynamodb-local",
            ));
        }
    }

    Ok(Client::from_conf(builder.build()))
}

fn has_environment_credentials() -> bool {
    std::env::var_os("AWS_ACCESS_KEY_ID").is_some()
        && std::env::var_os("AWS_SECRET_ACCESS_KEY").is_some()
}

fn endpoint_looks_local(endpoint: &str) -> bool {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);

    let host_with_port = without_scheme.split('/').next().unwrap_or_default();
    let host = host_with_port.split(':').next().unwrap_or_default();

    host.eq_ignore_ascii_case("localhost")
        || host == "127.0.0.1"
        || host == "::1"
        || host == "[::1]"
}

fn probe_connection(client: &Client, config: &DynamoProfileConfig) -> Result<(), DbError> {
    let runtime = runtime()?;
    runtime
        .block_on(client.list_tables().limit(1).send())
        .map_err(|error| {
            let formatted = DYNAMO_ERROR_FORMATTER.format_probe_error(&error, config);
            classify_connection_error(formatted)
        })?;

    Ok(())
}

fn runtime() -> Result<tokio::runtime::Runtime, DbError> {
    tokio::runtime::Runtime::new()
        .map_err(|error| DbError::connection_failed(format!("Tokio runtime setup failed: {error}")))
}

fn normalize_table_names(mut table_names: Vec<String>) -> Result<Vec<String>, DbError> {
    table_names.sort();
    table_names.dedup();

    Ok(table_names)
}

fn build_table_info_from_description(
    table_name: &str,
    database: &str,
    description: &TableDescription,
) -> TableInfo {
    let key_components = extract_key_components(
        description.key_schema(),
        description.attribute_definitions(),
    );

    let sample_fields = key_components_to_fields(&key_components);
    let indexes = key_components_to_indexes(&key_components);

    TableInfo {
        name: table_name.to_string(),
        schema: Some(database.to_string()),
        columns: None,
        indexes,
        foreign_keys: None,
        constraints: None,
        sample_fields,
    }
}

fn extract_key_components(
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
) -> Vec<DynamoKeyComponent> {
    let mut type_by_name: HashMap<&str, &ScalarAttributeType> = HashMap::new();

    for attribute in attribute_definitions {
        let name = attribute.attribute_name();
        let attribute_type = attribute.attribute_type();
        type_by_name.insert(name, attribute_type);
    }

    let mut components = Vec::new();

    for key in key_schema {
        let name = key.attribute_name();
        let key_type = key.key_type();

        let role = match key_type {
            KeyType::Hash => DynamoKeyRole::Partition,
            KeyType::Range => DynamoKeyRole::Sort,
            _ => continue,
        };

        let attribute_type = type_by_name
            .get(name)
            .map(|value| value.as_str().to_string())
            .unwrap_or_else(|| "unknown".to_string());

        components.push(DynamoKeyComponent {
            name: name.to_string(),
            role,
            attribute_type,
        });
    }

    components
}

fn key_components_to_fields(key_components: &[DynamoKeyComponent]) -> Option<Vec<FieldInfo>> {
    if key_components.is_empty() {
        return None;
    }

    let fields = key_components
        .iter()
        .map(|component| {
            let key_role = match component.role {
                DynamoKeyRole::Partition => "partition_key",
                DynamoKeyRole::Sort => "sort_key",
            };

            FieldInfo {
                name: component.name.clone(),
                common_type: format!("{} ({})", component.attribute_type, key_role),
                occurrence_rate: Some(1.0),
                nested_fields: None,
            }
        })
        .collect();

    Some(fields)
}

fn key_components_to_indexes(key_components: &[DynamoKeyComponent]) -> Option<IndexData> {
    if key_components.is_empty() {
        return None;
    }

    let keys = key_components
        .iter()
        .map(|component| (component.name.clone(), IndexDirection::Ascending))
        .collect();

    Some(IndexData::Document(vec![CollectionIndexInfo {
        name: "PRIMARY".to_string(),
        keys,
        is_unique: true,
        is_sparse: false,
        expire_after_seconds: None,
    }]))
}

struct DynamoErrorFormatter;

impl DynamoErrorFormatter {
    fn format_from_code(
        &self,
        code: Option<&str>,
        message: &str,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        let mut formatted = FormattedError::new(message.to_string());

        if let Some(code_value) = code {
            formatted = formatted.with_code(code_value.to_string());
        }

        let hint = match code {
            Some("UnrecognizedClientException")
            | Some("InvalidSignatureException")
            | Some("ExpiredTokenException")
            | Some("IncompleteSignatureException")
            | Some("MissingAuthenticationToken") => {
                Some("Check AWS credentials (environment, profile, or SSO session) and retry.")
            }
            Some("AccessDeniedException") => {
                Some("Check IAM permissions for dynamodb:ListTables in the selected region.")
            }
            Some("ResourceNotFoundException") => Some(
                "Check resource names and ensure you are using the intended AWS region/account.",
            ),
            Some("ValidationException") => {
                Some("Review request fields (region, endpoint, table/key names) and try again.")
            }
            Some("ProvisionedThroughputExceededException")
            | Some("ThrottlingException")
            | Some("RequestLimitExceeded") => {
                Some("Request was throttled. Retry with backoff or reduce request rate.")
            }
            _ => None,
        };

        if let Some(hint_value) = hint {
            formatted = formatted.with_hint(hint_value);
        }

        if code.is_some_and(|value| {
            matches!(
                value,
                "ProvisionedThroughputExceededException"
                    | "ThrottlingException"
                    | "RequestLimitExceeded"
            )
        }) {
            formatted = formatted.with_retriable(true);
        }

        if let Some(endpoint) = &config.endpoint {
            formatted = formatted.with_detail(format!(
                "region={}, endpoint_override={}",
                config.region, endpoint
            ));
        } else {
            formatted = formatted.with_detail(format!("region={}", config.region));
        }

        formatted
    }

    fn format_sdk_message(&self, message: &str, config: &DynamoProfileConfig) -> FormattedError {
        let lower = message.to_lowercase();

        let formatted = if lower.contains("credential") || lower.contains("token") {
            FormattedError::new("AWS credentials were not found or are invalid.")
                .with_hint("Configure credentials via AWS profile, environment, or SSO login.")
        } else if lower.contains("timed out") || lower.contains("timeout") {
            FormattedError::new("Connection to DynamoDB timed out.")
                .with_hint("Check network connectivity, endpoint reachability, and region.")
                .with_retriable(true)
        } else if lower.contains("dns")
            || lower.contains("resolve")
            || lower.contains("endpoint")
            || lower.contains("connection refused")
        {
            FormattedError::new("Unable to reach DynamoDB endpoint.")
                .with_hint("Check endpoint override and region configuration.")
        } else {
            FormattedError::new(message.to_string())
        };

        if let Some(endpoint) = &config.endpoint {
            formatted.with_detail(format!(
                "region={}, endpoint_override={}",
                config.region, endpoint
            ))
        } else {
            formatted.with_detail(format!("region={}", config.region))
        }
    }

    fn format_probe_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<ListTablesError>,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("DynamoDB service error");
            return self.format_from_code(code, message, config);
        }

        self.format_sdk_message(&error.to_string(), config)
    }

    fn format_describe_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<DescribeTableError>,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("DynamoDB service error");
            return self.format_from_code(code, message, config);
        }

        self.format_sdk_message(&error.to_string(), config)
    }

    fn format_scan_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<ScanError>,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("DynamoDB service error");
            return self.format_from_code(code, message, config);
        }

        self.format_sdk_message(&error.to_string(), config)
    }

    fn format_query_op_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<QueryError>,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("DynamoDB service error");
            return self.format_from_code(code, message, config);
        }

        self.format_sdk_message(&error.to_string(), config)
    }

    fn format_put_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<PutItemError>,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("DynamoDB service error");
            return self.format_from_code(code, message, config);
        }

        self.format_sdk_message(&error.to_string(), config)
    }

    fn format_update_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<UpdateItemError>,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("DynamoDB service error");
            return self.format_from_code(code, message, config);
        }

        self.format_sdk_message(&error.to_string(), config)
    }

    fn format_delete_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<DeleteItemError>,
        config: &DynamoProfileConfig,
    ) -> FormattedError {
        if let Some(service_error) = error.as_service_error() {
            let code = service_error.code();
            let message = service_error.message().unwrap_or("DynamoDB service error");
            return self.format_from_code(code, message, config);
        }

        self.format_sdk_message(&error.to_string(), config)
    }
}

impl QueryErrorFormatter for DynamoErrorFormatter {
    fn format_query_error(&self, error: &(dyn std::error::Error + 'static)) -> FormattedError {
        FormattedError::new(error.to_string())
    }
}

impl ConnectionErrorFormatter for DynamoErrorFormatter {
    fn format_connection_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        _host: &str,
        _port: u16,
    ) -> FormattedError {
        FormattedError::new(error.to_string())
    }

    fn format_uri_error(
        &self,
        error: &(dyn std::error::Error + 'static),
        sanitized_uri: &str,
    ) -> FormattedError {
        FormattedError::new(error.to_string())
            .with_detail(format!("sanitized_endpoint={sanitized_uri}"))
    }
}

static DYNAMO_ERROR_FORMATTER: DynamoErrorFormatter = DynamoErrorFormatter;

struct DynamoLanguageService;

impl LanguageService for DynamoLanguageService {
    fn validate(&self, query: &str) -> ValidationResult {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return ValidationResult::Valid;
        }

        let lower = trimmed.to_ascii_lowercase();
        if lower.starts_with("select ")
            || lower.starts_with("insert ")
            || lower.starts_with("update ")
            || lower.starts_with("delete ")
        {
            return ValidationResult::WrongLanguage {
                expected: QueryLanguage::Custom("DynamoDB".to_string()),
                message: "SQL syntax not supported for DynamoDB. Use DynamoDB command envelopes or mutation tools."
                    .to_string(),
            };
        }

        ValidationResult::Valid
    }

    fn detect_dangerous(&self, query: &str) -> Option<DangerousQueryKind> {
        let normalized = query.trim().to_ascii_lowercase();

        if normalized.contains("\"op\":\"delete\"") {
            return Some(DangerousQueryKind::DeleteNoWhere);
        }

        if normalized.contains("\"op\":\"update\"") {
            return Some(DangerousQueryKind::UpdateNoWhere);
        }

        None
    }
}

static DYNAMO_LANGUAGE_SERVICE: DynamoLanguageService = DynamoLanguageService;

fn classify_connection_error(formatted: FormattedError) -> DbError {
    match formatted.code.as_deref() {
        Some(
            "UnrecognizedClientException"
            | "InvalidSignatureException"
            | "ExpiredTokenException"
            | "IncompleteSignatureException"
            | "MissingAuthenticationToken",
        ) => DbError::AuthFailed(formatted),
        Some("AccessDeniedException") => DbError::PermissionDenied(formatted),
        Some("ResourceNotFoundException") => DbError::ObjectNotFound(formatted),
        Some("ValidationException") => DbError::ConnectionFailed(formatted),
        Some(
            "ProvisionedThroughputExceededException"
            | "ThrottlingException"
            | "RequestLimitExceeded",
        ) => DbError::ConnectionFailed(formatted),
        _ => DbError::ConnectionFailed(formatted),
    }
}

fn classify_query_error(formatted: FormattedError) -> DbError {
    match formatted.code.as_deref() {
        Some(
            "UnrecognizedClientException"
            | "InvalidSignatureException"
            | "ExpiredTokenException"
            | "IncompleteSignatureException"
            | "MissingAuthenticationToken",
        ) => DbError::AuthFailed(formatted),
        Some("AccessDeniedException") => DbError::PermissionDenied(formatted),
        Some("ResourceNotFoundException") => DbError::ObjectNotFound(formatted),
        Some("ValidationException") => DbError::QueryFailed(formatted),
        Some(
            "ProvisionedThroughputExceededException"
            | "ThrottlingException"
            | "RequestLimitExceeded",
        ) => DbError::QueryFailed(formatted.with_retriable(true)),
        _ => DbError::QueryFailed(formatted),
    }
}

pub fn is_supported_mvp_flow(flow: &str) -> bool {
    DYNAMODB_MVP_SUPPORTED_FLOWS.contains(&flow)
}

pub fn is_unsupported_mvp_flow(flow: &str) -> bool {
    DYNAMODB_MVP_UNSUPPORTED_FLOWS.contains(&flow)
}

pub fn unsupported_mvp_message(flow: &str) -> String {
    format!(
        "Operation '{flow}' is not supported in DynamoDB MVP. This workflow is outside the current MVP scope."
    )
}

#[cfg(test)]
mod tests {
    use super::{
        DYNAMODB_DEFAULT_DATABASE, DYNAMODB_METADATA, DynamoDriver, DynamoErrorFormatter,
        DynamoKeyComponent, DynamoKeyRole, DynamoLanguageService, DynamoProfileConfig,
        DynamoReadStrategy, DynamoTableKeySchema, append_window_items, attribute_value_to_value,
        build_table_info_from_description, build_update_expression_from_json,
        classify_connection_error, classify_query_error, decide_read_strategy,
        ensure_item_contains_required_keys, extract_key_map_from_filter,
        json_value_to_attribute_value, key_components_to_fields, key_components_to_indexes,
        normalize_table_names, unsupported_non_mvp_operation,
    };
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
        TableDescription,
    };
    use dbflux_core::{
        ConnectionProfile, DangerousQueryKind, DatabaseCategory, DbConfig, DbDriver, DbError,
        DriverCapabilities, FormValues, IndexData, LanguageService,
    };
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn metadata_uses_document_semantics_with_truthful_phase3_caps() {
        assert_eq!(DYNAMODB_METADATA.category, DatabaseCategory::Document);
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::AUTHENTICATION)
        );
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::INSERT)
        );
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::UPDATE)
        );
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::DELETE)
        );
    }

    #[test]
    fn build_config_requires_region() {
        let driver = DynamoDriver::new();
        let values = FormValues::new();

        let error = driver
            .build_config(&values)
            .expect_err("region should be required");

        match error {
            DbError::InvalidProfile(message) => {
                assert!(message.to_lowercase().contains("region"));
            }
            other => panic!("expected InvalidProfile error, got {other:?}"),
        }
    }

    #[test]
    fn missing_credentials_error_is_actionable() {
        let formatter = DynamoErrorFormatter;
        let config = DynamoProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: None,
            table: None,
        };

        let formatted =
            formatter.format_sdk_message("No credentials found in credential chain", &config);
        let mapped = classify_connection_error(formatted);

        match mapped {
            DbError::ConnectionFailed(details) => {
                let hint = details.hint.unwrap_or_default();
                assert!(hint.to_lowercase().contains("credentials"));
            }
            other => panic!("expected ConnectionFailed, got {other:?}"),
        }
    }

    #[test]
    fn invalid_region_validation_error_is_actionable() {
        let formatter = DynamoErrorFormatter;
        let config = DynamoProfileConfig {
            region: "invalid-region-1".to_string(),
            profile: None,
            endpoint: None,
            table: None,
        };

        let formatted = formatter.format_from_code(
            Some("ValidationException"),
            "Region must be a valid AWS region",
            &config,
        );

        let mapped = classify_connection_error(formatted);

        match mapped {
            DbError::ConnectionFailed(details) => {
                assert_eq!(details.code.as_deref(), Some("ValidationException"));
                let hint = details.hint.unwrap_or_default();
                assert!(hint.to_lowercase().contains("region"));
            }
            other => panic!("expected ConnectionFailed, got {other:?}"),
        }
    }

    #[test]
    fn auth_failure_codes_map_to_auth_failed_for_connection_flows() {
        let formatter = DynamoErrorFormatter;
        let config = DynamoProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: None,
            table: None,
        };

        let formatted = formatter.format_from_code(
            Some("UnrecognizedClientException"),
            "The security token included in the request is invalid",
            &config,
        );

        let mapped = classify_connection_error(formatted);

        match mapped {
            DbError::AuthFailed(details) => {
                assert_eq!(details.code.as_deref(), Some("UnrecognizedClientException"));
            }
            other => panic!("expected AuthFailed, got {other:?}"),
        }
    }

    #[test]
    fn auth_failure_codes_map_to_auth_failed_for_query_flows() {
        let formatter = DynamoErrorFormatter;
        let config = DynamoProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: None,
            table: None,
        };

        let formatted = formatter.format_from_code(
            Some("ExpiredTokenException"),
            "The security token included in the request is expired",
            &config,
        );

        let mapped = classify_query_error(formatted);

        match mapped {
            DbError::AuthFailed(details) => {
                assert_eq!(details.code.as_deref(), Some("ExpiredTokenException"));
            }
            other => panic!("expected AuthFailed, got {other:?}"),
        }
    }

    #[test]
    fn connect_and_test_connection_succeed_against_local_endpoint() {
        let endpoint = match std::env::var("DBFLUX_DYNAMODB_TEST_ENDPOINT") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => return,
        };

        let profile = ConnectionProfile::new_with_driver(
            "dynamo-local",
            dbflux_core::DbKind::DynamoDB,
            "builtin:dynamodb",
            DbConfig::DynamoDB {
                region: "us-east-1".to_string(),
                profile: None,
                endpoint: Some(endpoint),
                table: None,
            },
        );

        let driver = DynamoDriver::new();

        driver
            .test_connection(&profile)
            .expect("test_connection should succeed against local endpoint");

        driver
            .connect(&profile)
            .expect("connect should succeed against local endpoint");
    }

    #[test]
    fn empty_endpoint_schema_returns_valid_empty_state() {
        let endpoint = match std::env::var("DBFLUX_DYNAMODB_TEST_ENDPOINT") {
            Ok(value) if !value.trim().is_empty() => value,
            _ => return,
        };

        let profile = ConnectionProfile::new_with_driver(
            "dynamo-local-empty",
            dbflux_core::DbKind::DynamoDB,
            "builtin:dynamodb",
            DbConfig::DynamoDB {
                region: "us-east-1".to_string(),
                profile: None,
                endpoint: Some(endpoint),
                table: None,
            },
        );

        let driver = DynamoDriver::new();
        let connection = driver
            .connect(&profile)
            .expect("connect should succeed against local endpoint");

        let schema = connection
            .schema()
            .expect("schema should resolve even when there are no tables");

        assert_eq!(schema.databases().len(), 1);
        assert_eq!(schema.collections().len(), 0);
    }

    #[test]
    fn table_discovery_is_sorted_and_deduplicated() {
        let input = vec![
            "z".to_string(),
            "a".to_string(),
            "m".to_string(),
            "a".to_string(),
        ];
        let output = normalize_table_names(input).expect("normalization should succeed");
        assert_eq!(
            output,
            vec!["a".to_string(), "m".to_string(), "z".to_string()]
        );
    }

    #[test]
    fn key_metadata_supports_partition_only_and_partition_sort_tables() {
        let partition_only = vec![DynamoKeyComponent {
            name: "pk".to_string(),
            role: DynamoKeyRole::Partition,
            attribute_type: "S".to_string(),
        }];

        let fields = key_components_to_fields(&partition_only).expect("fields should be present");
        assert_eq!(fields.len(), 1);
        assert!(fields[0].common_type.contains("partition_key"));

        let partition_sort = vec![
            DynamoKeyComponent {
                name: "pk".to_string(),
                role: DynamoKeyRole::Partition,
                attribute_type: "S".to_string(),
            },
            DynamoKeyComponent {
                name: "sk".to_string(),
                role: DynamoKeyRole::Sort,
                attribute_type: "N".to_string(),
            },
        ];

        let indexes =
            key_components_to_indexes(&partition_sort).expect("indexes should be present");
        match indexes {
            IndexData::Document(doc_indexes) => {
                assert_eq!(doc_indexes.len(), 1);
                assert_eq!(doc_indexes[0].keys.len(), 2);
            }
            other => panic!("expected document indexes, got {other:?}"),
        }
    }

    #[test]
    fn empty_discovery_path_returns_empty_collection_semantics() {
        let description = TableDescription::builder().table_name("unused").build();
        let table_info =
            build_table_info_from_description("users", DYNAMODB_DEFAULT_DATABASE, &description);

        assert_eq!(table_info.name, "users");
        assert!(table_info.sample_fields.is_none());
        assert!(table_info.indexes.is_none());
    }

    #[test]
    fn describe_mapping_includes_partition_and_sort_key_metadata() {
        let pk = KeySchemaElement::builder()
            .attribute_name("pk")
            .key_type(KeyType::Hash)
            .build()
            .expect("pk element should build");
        let sk = KeySchemaElement::builder()
            .attribute_name("sk")
            .key_type(KeyType::Range)
            .build()
            .expect("sk element should build");

        let pk_attr = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .expect("pk attr should build");
        let sk_attr = AttributeDefinition::builder()
            .attribute_name("sk")
            .attribute_type(ScalarAttributeType::N)
            .build()
            .expect("sk attr should build");

        let description = TableDescription::builder()
            .table_name("users")
            .set_key_schema(Some(vec![pk, sk]))
            .set_attribute_definitions(Some(vec![pk_attr, sk_attr]))
            .build();

        let table_info =
            build_table_info_from_description("users", DYNAMODB_DEFAULT_DATABASE, &description);

        let fields = table_info
            .sample_fields
            .expect("sample fields should include key metadata");
        assert_eq!(fields.len(), 2);

        let indexes = table_info
            .indexes
            .expect("indexes should include primary key metadata");
        match indexes {
            IndexData::Document(doc_indexes) => {
                assert_eq!(doc_indexes[0].name, "PRIMARY");
                assert_eq!(doc_indexes[0].keys.len(), 2);
            }
            other => panic!("expected document indexes, got {other:?}"),
        }
    }

    #[test]
    fn browse_window_reports_continuation_on_partial_page() {
        let page_items = vec![1, 2, 3, 4, 5];
        let mut skip = 0;
        let mut collected = vec![1, 2];

        let has_more = append_window_items(&page_items, &mut skip, &mut collected, 4);

        assert!(has_more);
        assert_eq!(collected, vec![1, 2, 1, 2]);
    }

    #[test]
    fn browse_window_final_page_has_no_continuation() {
        let page_items = vec![10, 11, 12];
        let mut skip = 1;
        let mut collected = Vec::new();

        let has_more = append_window_items(&page_items, &mut skip, &mut collected, 10);

        assert!(!has_more);
        assert_eq!(collected, vec![11, 12]);
        assert_eq!(skip, 0);
    }

    #[test]
    fn key_filter_selects_query_strategy_and_missing_key_falls_back_to_scan() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: Some("sk".to_string()),
        };

        let query_strategy = decide_read_strategy(Some(&json!({"pk":"A","sk":1})), &key_schema)
            .expect("strategy decision should succeed");
        assert!(matches!(query_strategy, DynamoReadStrategy::Query(_)));

        let scan_strategy = decide_read_strategy(Some(&json!({"other":"A"})), &key_schema)
            .expect("strategy decision should succeed");
        assert!(matches!(scan_strategy, DynamoReadStrategy::Scan));
    }

    #[test]
    fn attribute_value_conversion_round_trips_nested_json_shapes() {
        let original = json!({
            "pk": "USER#1",
            "count": 42,
            "active": true,
            "tags": ["a", "b"],
            "meta": {
                "score": 9.5,
                "flags": [true, false, null]
            }
        });

        let attribute_value = json_value_to_attribute_value(&original)
            .expect("json to attribute conversion should work");
        let converted = attribute_value_to_value(&attribute_value);

        match converted {
            dbflux_core::Value::Document(map) => {
                assert!(map.contains_key("pk"));
                assert!(map.contains_key("count"));
                assert!(map.contains_key("meta"));
            }
            other => panic!("expected document value, got {other:?}"),
        }

        let av_map = AttributeValue::M(
            [
                ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
                ("count".to_string(), AttributeValue::N("42".to_string())),
                (
                    "meta".to_string(),
                    AttributeValue::M(HashMap::from([(
                        "flag".to_string(),
                        AttributeValue::Bool(true),
                    )])),
                ),
            ]
            .into_iter()
            .collect(),
        );

        let converted_map = attribute_value_to_value(&av_map);
        match converted_map {
            dbflux_core::Value::Document(map) => {
                assert_eq!(
                    map.get("pk"),
                    Some(&dbflux_core::Value::Text("USER#1".to_string()))
                );
                assert_eq!(map.get("count"), Some(&dbflux_core::Value::Int(42)));
            }
            other => panic!("expected document value, got {other:?}"),
        }
    }

    #[test]
    fn put_requires_partition_key() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
        };

        let item = HashMap::from([("other".to_string(), AttributeValue::S("x".to_string()))]);
        let error = ensure_item_contains_required_keys(&item, &key_schema)
            .expect_err("missing partition key should fail");

        assert!(
            error
                .to_string()
                .to_ascii_lowercase()
                .contains("partition key")
        );
    }

    #[test]
    fn update_filter_requires_full_key_identity() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: Some("sk".to_string()),
        };

        let error = extract_key_map_from_filter(&json!({"pk":"A"}), &key_schema)
            .expect_err("missing sort key should fail");

        assert!(error.to_string().contains("sort key 'sk'"));
    }

    #[test]
    fn update_expression_rejects_key_mutation() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
        };

        let error = build_update_expression_from_json(&json!({"$set":{"pk":"new"}}), &key_schema)
            .expect_err("updating partition key should fail");

        assert!(error.to_string().contains("cannot be updated"));
    }

    #[test]
    fn unsupported_many_operations_are_not_supported_errors() {
        let error = unsupported_non_mvp_operation(
            "update_many_documents",
            "DynamoDB MVP does not support many-item update semantics.",
        );

        match error {
            DbError::NotSupported(message) => {
                assert!(message.contains("update_many_documents"));
            }
            other => panic!("expected NotSupported, got {other:?}"),
        }
    }

    #[test]
    fn dangerous_detection_flags_update_and_delete_envelopes() {
        let service = DynamoLanguageService;

        let delete =
            service.detect_dangerous(r#"{"op":"delete","table":"users","key":{"pk":"1"}}"#);
        let update = service.detect_dangerous(
            r#"{"op":"update","table":"users","key":{"pk":"1"},"update":{"name":"A"}}"#,
        );
        let put = service.detect_dangerous(r#"{"op":"put","table":"users","item":{"pk":"1"}}"#);

        assert_eq!(delete, Some(DangerousQueryKind::DeleteNoWhere));
        assert_eq!(update, Some(DangerousQueryKind::UpdateNoWhere));
        assert_eq!(put, None);
    }
}

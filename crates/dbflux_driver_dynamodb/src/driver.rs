use std::collections::HashMap;
use std::sync::LazyLock;

use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::config::{Builder as DynamoConfigBuilder, Credentials};
use aws_sdk_dynamodb::error::ProvideErrorMetadata;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodb::operation::list_tables::ListTablesError;
use aws_sdk_dynamodb::operation::put_item::PutItemError;
use aws_sdk_dynamodb::operation::query::QueryError;
use aws_sdk_dynamodb::operation::scan::ScanError;
use aws_sdk_dynamodb::operation::update_item::UpdateItemError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, PutRequest,
    ScalarAttributeType, Select, WriteRequest,
};
use aws_sdk_dynamodb::{Client, types::TableDescription};
use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    CollectionBrowseRequest, CollectionCountRequest, CollectionIndexInfo, CollectionInfo,
    CollectionRef, ColumnMeta, Connection, ConnectionErrorFormatter, ConnectionExt,
    ConnectionProfile, DYNAMODB_FORM, DangerousQueryKind, DatabaseCategory, DatabaseInfo, DbConfig,
    DbDriver, DbError, DbKind, DbSchemaInfo, DdlCapabilities, DocumentConnection, DocumentDelete,
    DocumentInsert, DocumentSchema, DocumentUpdate, DriverCapabilities, DriverFormDef,
    DriverLimits, DriverMetadata, FieldInfo, FormValues, FormattedError, Icon, IndexData,
    IndexDirection, KeyValueConnection, LanguageService, MutationCapabilities, Pagination,
    PaginationStyle, QueryCapabilities, QueryErrorFormatter, QueryLanguage, QueryRequest,
    QueryResult, RelationalConnection, SchemaLoadingStrategy, SchemaSnapshot, SqlDialect,
    TableInfo, TransactionCapabilities, ValidationResult, Value, WhereOperator,
};

use crate::query_generator::DynamoQueryGenerator;
use crate::query_parser::{
    DynamoCommandEnvelope, DynamoFilterFallback, DynamoReadOptions, parse_command_envelope,
};

const DYNAMODB_DEFAULT_DATABASE: &str = "dynamodb";
const DYNAMODB_BATCH_WRITE_WINDOW: usize = 25;
const DYNAMODB_BATCH_WRITE_MAX_ATTEMPTS: u32 = 5;
const DYNAMODB_BATCH_WRITE_BASE_BACKOFF_MS: u64 = 50;

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
            | DriverCapabilities::DELETE.bits()
            | DriverCapabilities::NESTED_DOCUMENTS.bits()
            | DriverCapabilities::ARRAYS.bits(),
    ),
    default_port: None,
    uri_scheme: "dynamodb".into(),
    icon: Icon::Dynamodb,
    syntax: None,
    query: Some(QueryCapabilities {
        pagination: vec![PaginationStyle::PageToken],
        where_operators: vec![
            WhereOperator::Eq,
            WhereOperator::Ne,
            WhereOperator::Gt,
            WhereOperator::Gte,
            WhereOperator::Lt,
            WhereOperator::Lte,
            WhereOperator::In,
            WhereOperator::NotIn,
            WhereOperator::And,
            WhereOperator::Or,
            WhereOperator::Not,
        ],
        supports_order_by: true,
        supports_group_by: false,
        supports_having: false,
        supports_distinct: false,
        supports_limit: true,
        supports_offset: false,
        supports_joins: false,
        supports_subqueries: false,
        supports_union: false,
        supports_intersect: false,
        supports_except: false,
        supports_case_expressions: false,
        supports_window_functions: false,
        supports_ctes: false,
        supports_explain: false,
        max_query_parameters: 0,
        max_order_by_columns: 0,
        max_group_by_columns: 0,
    }),
    mutation: Some(MutationCapabilities {
        supports_insert: true,
        supports_update: true,
        supports_delete: true,
        supports_upsert: false,
        supports_returning: false,
        supports_batch: true,
        supports_bulk_update: false,
        supports_bulk_delete: false,
        max_insert_values: 25,
    }),
    ddl: Some(DdlCapabilities {
        supports_create_database: false,
        supports_drop_database: false,
        supports_create_table: false,
        supports_drop_table: false,
        supports_alter_table: false,
        supports_create_index: false,
        supports_drop_index: false,
        supports_create_view: false,
        supports_drop_view: false,
        supports_create_trigger: false,
        supports_drop_trigger: false,
        transactional_ddl: false,
        supports_add_column: false,
        supports_drop_column: false,
        supports_rename_column: false,
        supports_alter_column: false,
        supports_add_constraint: false,
        supports_drop_constraint: false,
    }),
    transactions: Some(TransactionCapabilities {
        supports_transactions: false,
        supported_isolation_levels: vec![],
        default_isolation_level: None,
        supports_savepoints: false,
        supports_nested_transactions: false,
        supports_read_only: false,
        supports_deferrable: false,
    }),
    limits: Some(DriverLimits {
        max_query_length: 0,
        max_parameters: 0,
        max_result_rows: 0,
        max_connections: 0,
        max_nested_subqueries: 0,
        max_identifier_length: 0,
        max_columns: 0,
        max_indexes_per_table: 0,
    }),
    classification_override: None,
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
                read_options,
            }
            | DynamoCommandEnvelope::Query {
                database,
                table,
                filter,
                limit,
                offset,
                read_options,
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

                self.browse_collection_with_read_options(&request, &read_options)?
            }
            DynamoCommandEnvelope::Put {
                database,
                table,
                items,
            } => {
                let insert = DocumentInsert {
                    collection: table,
                    database: Some(
                        database
                            .or_else(|| req.database.clone())
                            .unwrap_or_else(|| DYNAMODB_DEFAULT_DATABASE.to_string()),
                    ),
                    documents: items,
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
        let read_options = DynamoReadOptions::default();
        self.browse_collection_with_read_options(request, &read_options)
    }

    fn count_collection(&self, request: &CollectionCountRequest) -> Result<u64, DbError> {
        let read_options = DynamoReadOptions::default();
        self.count_collection_with_read_options(request, &read_options)
    }

    fn insert_document(&self, insert: &DocumentInsert) -> Result<dbflux_core::CrudResult, DbError> {
        let plan = self.plan_insert_operation(insert)?;

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;

        if plan.total_items == 0 {
            return Err(DbError::query_failed("Document payload is required"));
        }

        if plan.total_items == 1 {
            let item_map = plan
                .item_batches
                .first()
                .and_then(|batch| batch.first())
                .cloned()
                .ok_or_else(|| DbError::query_failed("Document payload is required"))?;

            runtime
                .block_on(
                    self.client
                        .put_item()
                        .table_name(&plan.table)
                        .set_item(Some(item_map))
                        .send(),
                )
                .map_err(|error| {
                    let formatted = DYNAMO_ERROR_FORMATTER.format_put_error(&error, &config);
                    classify_query_error(formatted)
                })?;

            return Ok(crud_result_with_affected_rows(plan.total_items));
        }

        for item_batch in &plan.item_batches {
            let write_requests = build_batch_put_write_requests(item_batch)?;
            execute_batch_write_with_retries(
                &self.client,
                &plan.table,
                write_requests,
                &runtime,
                &config,
            )?;
        }

        Ok(crud_result_with_affected_rows(plan.total_items))
    }

    fn update_document(&self, update: &DocumentUpdate) -> Result<dbflux_core::CrudResult, DbError> {
        if update.many && update.upsert {
            return Err(unsupported_operation(
                "update_many_with_upsert",
                "DynamoDB upsert is supported only for single-item updates.",
            ));
        }

        if update.upsert {
            let plan = self.plan_upsert_operation(update)?;

            let config = DynamoProfileConfig {
                region: self.default_region.clone(),
                profile: None,
                endpoint: None,
                table: self.default_table.clone(),
            };

            let runtime = runtime()?;

            let mut condition_attribute_names = HashMap::new();
            condition_attribute_names.insert("#ck0".to_string(), plan.partition_key_name.clone());

            let mut condition_expression = "attribute_exists(#ck0)".to_string();
            if let Some(sort_key_name) = plan.sort_key_name.as_ref() {
                condition_attribute_names.insert("#ck1".to_string(), sort_key_name.clone());
                condition_expression.push_str(" AND attribute_exists(#ck1)");
            }

            let mut update_names = plan.expression_attribute_names.clone();
            for (token, key_name) in condition_attribute_names {
                update_names.entry(token).or_insert(key_name);
            }

            let update_result = runtime.block_on(
                self.client
                    .update_item()
                    .table_name(&plan.table)
                    .set_key(Some(plan.key_map.clone()))
                    .update_expression(plan.update_expression)
                    .condition_expression(condition_expression)
                    .set_expression_attribute_names(Some(update_names))
                    .set_expression_attribute_values(Some(plan.expression_attribute_values.clone()))
                    .send(),
            );

            match update_result {
                Ok(_) => return Ok(crud_result_with_affected_rows(1)),
                Err(error) => {
                    let is_conditional_miss = error
                        .as_service_error()
                        .and_then(|service_error| service_error.code())
                        .is_some_and(|code| code == "ConditionalCheckFailedException");

                    if is_conditional_miss {
                        let upsert_item =
                            build_upsert_item_map(&plan.key_map, &plan.insert_attributes);

                        runtime
                            .block_on(
                                self.client
                                    .put_item()
                                    .table_name(&plan.table)
                                    .set_item(Some(upsert_item))
                                    .send(),
                            )
                            .map_err(|put_error| {
                                let formatted =
                                    DYNAMO_ERROR_FORMATTER.format_put_error(&put_error, &config);
                                classify_query_error(formatted)
                            })?;

                        return Ok(crud_result_with_affected_rows(1));
                    }

                    let formatted = DYNAMO_ERROR_FORMATTER.format_update_error(&error, &config);
                    return Err(classify_query_error(formatted));
                }
            }
        }

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        if update.many {
            let many_plan = self.plan_update_many_operation(update)?;
            let runtime = runtime()?;

            let mut updated_count: usize = 0;

            for key_map in many_plan.key_maps {
                runtime
                    .block_on(
                        self.client
                            .update_item()
                            .table_name(&many_plan.table)
                            .set_key(Some(key_map))
                            .update_expression(many_plan.update_expression.clone())
                            .set_expression_attribute_names(Some(
                                many_plan.expression_attribute_names.clone(),
                            ))
                            .set_expression_attribute_values(Some(
                                many_plan.expression_attribute_values.clone(),
                            ))
                            .send(),
                    )
                    .map_err(|error| {
                        let formatted = DYNAMO_ERROR_FORMATTER.format_update_error(&error, &config);
                        classify_query_error(formatted)
                    })?;

                updated_count = updated_count.saturating_add(1);
            }

            return Ok(crud_result_with_affected_rows(updated_count));
        }

        let plan = self.plan_update_operation(update)?;
        let runtime = runtime()?;
        runtime
            .block_on(
                self.client
                    .update_item()
                    .table_name(&plan.table)
                    .set_key(Some(plan.key_map))
                    .update_expression(plan.update_expression)
                    .set_expression_attribute_names(Some(plan.expression_attribute_names))
                    .set_expression_attribute_values(Some(plan.expression_attribute_values))
                    .send(),
            )
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_update_error(&error, &config);
                classify_query_error(formatted)
            })?;

        Ok(crud_result_with_affected_rows(1))
    }

    fn delete_document(&self, delete: &DocumentDelete) -> Result<dbflux_core::CrudResult, DbError> {
        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        if delete.many {
            let many_plan = self.plan_delete_many_operation(delete)?;
            let runtime = runtime()?;

            let mut deleted_count: usize = 0;

            for key_map in many_plan.key_maps {
                runtime
                    .block_on(
                        self.client
                            .delete_item()
                            .table_name(&many_plan.table)
                            .set_key(Some(key_map))
                            .send(),
                    )
                    .map_err(|error| {
                        let formatted = DYNAMO_ERROR_FORMATTER.format_delete_error(&error, &config);
                        classify_query_error(formatted)
                    })?;

                deleted_count = deleted_count.saturating_add(1);
            }

            return Ok(crud_result_with_affected_rows(deleted_count));
        }

        let plan = self.plan_delete_operation(delete)?;

        let runtime = runtime()?;
        runtime
            .block_on(
                self.client
                    .delete_item()
                    .table_name(&plan.table)
                    .set_key(Some(plan.key_map))
                    .send(),
            )
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_delete_error(&error, &config);
                classify_query_error(formatted)
            })?;

        Ok(crud_result_with_affected_rows(1))
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

impl DocumentConnection for DynamoConnection {}

impl ConnectionExt for DynamoConnection {
    fn as_relational(&self) -> Option<&dyn RelationalConnection> {
        None
    }

    fn as_document(&self) -> Option<&dyn DocumentConnection> {
        Some(self)
    }

    fn as_keyvalue(&self) -> Option<&dyn KeyValueConnection> {
        None
    }
}

impl DynamoConnection {
    fn browse_collection_with_read_options(
        &self,
        request: &CollectionBrowseRequest,
        read_options: &DynamoReadOptions,
    ) -> Result<QueryResult, DbError> {
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
        validate_read_options(&request.collection.name, &key_schema, read_options)?;
        let read_strategy =
            decide_read_strategy(request.filter.as_ref(), &key_schema, read_options)?;

        let page = self.read_items_page(
            &request.collection.name,
            &read_strategy,
            request.pagination.offset(),
            request.pagination.limit() as u64,
            &config,
        )?;

        let _ = page.has_more;
        Ok(items_to_query_result(&page.items, &key_schema))
    }

    fn count_collection_with_read_options(
        &self,
        request: &CollectionCountRequest,
        read_options: &DynamoReadOptions,
    ) -> Result<u64, DbError> {
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
        validate_read_options(&request.collection.name, &key_schema, read_options)?;
        let read_strategy =
            decide_read_strategy(request.filter.as_ref(), &key_schema, read_options)?;

        self.count_items(&request.collection.name, &read_strategy, &config)
    }
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

        // Retry up to 2 times on dispatch failure (transient SDK errors)
        let mut last_error = None;
        for attempt in 0..2 {
            if attempt > 0 {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            match runtime.block_on(self.client.describe_table().table_name(table).send()) {
                Ok(output) => {
                    let description = output.table().ok_or_else(|| {
                        DbError::object_not_found(format!("Table '{}' was not found", table))
                    })?;

                    let keys = extract_key_components(
                        description.key_schema(),
                        description.attribute_definitions(),
                    );
                    let index_kinds = extract_table_index_kinds(description);

                    let partition_key = keys
                        .iter()
                        .find(|component| component.role == DynamoKeyRole::Partition)
                        .map(|component| component.name.clone());

                    let sort_key = keys
                        .iter()
                        .find(|component| component.role == DynamoKeyRole::Sort)
                        .map(|component| component.name.clone());

                    return Ok(DynamoTableKeySchema {
                        partition_key,
                        sort_key,
                        index_kinds,
                    });
                }
                Err(error) => {
                    let error_str = error.to_string();
                    let is_dispatch_failure = error_str.to_lowercase().contains("dispatch");

                    if is_dispatch_failure && attempt < 1 {
                        // Retry on dispatch failure
                        last_error = Some(error);
                        continue;
                    }

                    // Non-retriable error or final attempt
                    let formatted = DYNAMO_ERROR_FORMATTER.format_describe_error(&error, &config);
                    return Err(classify_connection_error(formatted));
                }
            }
        }

        // Should never reach here, but handle it just in case
        if let Some(error) = last_error {
            let formatted = DYNAMO_ERROR_FORMATTER.format_describe_error(&error, &config);
            Err(classify_connection_error(formatted))
        } else {
            Err(DbError::query_failed(
                "Failed to fetch table key schema after retries",
            ))
        }
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

        if strategy.client_filter().is_some()
            && strategy.filter_fallback_policy() == DynamoFilterFallbackPolicy::Reject
        {
            return Err(DbError::query_failed(
                "DynamoDB read strategy is invalid: client filter cannot be used when fallback is disabled",
            ));
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

            let filtered_page_items = apply_item_filter(&page.items, strategy.client_filter())?;

            let page_has_more = append_window_items(
                &filtered_page_items,
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
        if strategy.client_filter().is_some()
            && strategy.filter_fallback_policy() == DynamoFilterFallbackPolicy::Reject
        {
            return Err(DbError::query_failed(
                "DynamoDB count strategy is invalid: client filter cannot be used when fallback is disabled",
            ));
        }

        let runtime = runtime()?;
        let mut total: u64 = 0;
        let mut cursor: Option<HashMap<String, AttributeValue>> = None;

        if strategy.client_filter().is_none() {
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

            return Ok(total);
        }

        loop {
            let page = fetch_read_page(
                &self.client,
                table,
                strategy,
                100,
                cursor.clone(),
                &runtime,
                config,
            )?;

            let matching_items = apply_item_filter(&page.items, strategy.client_filter())?;
            total = total.saturating_add(matching_items.len() as u64);
            cursor = page.last_evaluated_key;

            if cursor.is_none() {
                break;
            }
        }

        Ok(total)
    }

    fn plan_insert_operation(&self, insert: &DocumentInsert) -> Result<DynamoInsertPlan, DbError> {
        ensure_default_database(insert.database.as_deref())?;

        let key_schema = self.fetch_table_key_schema(&insert.collection)?;
        let mut items = Vec::with_capacity(insert.documents.len());

        for document in &insert.documents {
            let item_map = json_object_to_attribute_map(document)?;
            ensure_item_contains_required_keys(&item_map, &key_schema)?;
            items.push(item_map);
        }

        let total_items = items.len();
        let item_batches = build_item_batches(items, DYNAMODB_BATCH_WRITE_WINDOW)?;

        Ok(DynamoInsertPlan {
            table: insert.collection.clone(),
            item_batches,
            total_items,
        })
    }

    fn plan_update_operation(&self, update: &DocumentUpdate) -> Result<DynamoUpdatePlan, DbError> {
        ensure_default_database(update.database.as_deref())?;

        let key_schema = self.fetch_table_key_schema(&update.collection)?;
        let key_map = extract_key_map_from_filter(&update.filter.filter, &key_schema)?;
        let (update_expression, expression_attribute_names, expression_attribute_values) =
            build_update_expression_from_json(&update.update, &key_schema)?;

        Ok(DynamoUpdatePlan {
            table: update.collection.clone(),
            key_map,
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
        })
    }

    fn plan_update_many_operation(
        &self,
        update: &DocumentUpdate,
    ) -> Result<DynamoUpdateManyPlan, DbError> {
        ensure_default_database(update.database.as_deref())?;

        let key_schema = self.fetch_table_key_schema(&update.collection)?;
        let read_options = DynamoReadOptions::default();
        let read_strategy =
            decide_read_strategy(Some(&update.filter.filter), &key_schema, &read_options)?;
        let key_maps =
            self.collect_matching_key_maps(&update.collection, &read_strategy, &key_schema)?;

        let (update_expression, expression_attribute_names, expression_attribute_values) =
            build_update_expression_from_json(&update.update, &key_schema)?;

        Ok(DynamoUpdateManyPlan {
            table: update.collection.clone(),
            key_maps,
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
        })
    }

    fn plan_upsert_operation(&self, update: &DocumentUpdate) -> Result<DynamoUpsertPlan, DbError> {
        ensure_default_database(update.database.as_deref())?;

        let key_schema = self.fetch_table_key_schema(&update.collection)?;
        let key_map = resolve_upsert_key_map(update, &key_schema)?;

        let sanitized_update = strip_key_fields_from_update_payload(&update.update, &key_schema)?;
        let (update_expression, expression_attribute_names, expression_attribute_values) =
            build_update_expression_from_json(&sanitized_update, &key_schema)?;

        let insert_attributes = extract_non_key_update_attributes(&update.update, &key_schema)?;

        Ok(DynamoUpsertPlan {
            table: update.collection.clone(),
            key_map,
            partition_key_name: key_schema.partition_key.ok_or_else(|| {
                DbError::query_failed(
                    "Table metadata is missing a partition key; cannot resolve item identity",
                )
            })?,
            sort_key_name: key_schema.sort_key,
            update_expression,
            expression_attribute_names,
            expression_attribute_values,
            insert_attributes,
        })
    }

    fn plan_delete_operation(&self, delete: &DocumentDelete) -> Result<DynamoDeletePlan, DbError> {
        ensure_default_database(delete.database.as_deref())?;

        let key_schema = self.fetch_table_key_schema(&delete.collection)?;
        let key_map = extract_key_map_from_filter(&delete.filter.filter, &key_schema)?;

        Ok(DynamoDeletePlan {
            table: delete.collection.clone(),
            key_map,
        })
    }

    fn plan_delete_many_operation(
        &self,
        delete: &DocumentDelete,
    ) -> Result<DynamoDeleteManyPlan, DbError> {
        ensure_default_database(delete.database.as_deref())?;

        let key_schema = self.fetch_table_key_schema(&delete.collection)?;
        let read_options = DynamoReadOptions::default();
        let read_strategy =
            decide_read_strategy(Some(&delete.filter.filter), &key_schema, &read_options)?;
        let key_maps =
            self.collect_matching_key_maps(&delete.collection, &read_strategy, &key_schema)?;

        Ok(DynamoDeleteManyPlan {
            table: delete.collection.clone(),
            key_maps,
        })
    }

    fn collect_matching_key_maps(
        &self,
        table: &str,
        strategy: &DynamoReadStrategy,
        key_schema: &DynamoTableKeySchema,
    ) -> Result<Vec<HashMap<String, AttributeValue>>, DbError> {
        const READ_PAGE_LIMIT: i32 = 100;

        let config = DynamoProfileConfig {
            region: self.default_region.clone(),
            profile: None,
            endpoint: None,
            table: self.default_table.clone(),
        };

        let runtime = runtime()?;
        let mut cursor: Option<HashMap<String, AttributeValue>> = None;
        let mut key_maps = Vec::new();

        loop {
            let page = fetch_read_page(
                &self.client,
                table,
                strategy,
                READ_PAGE_LIMIT,
                cursor.clone(),
                &runtime,
                &config,
            )?;

            let filtered_items = apply_item_filter(&page.items, strategy.client_filter())?;

            for item in &filtered_items {
                let key_map = extract_key_map_from_item(item, key_schema)?;
                key_maps.push(key_map);
            }

            cursor = page.last_evaluated_key;
            if cursor.is_none() {
                break;
            }
        }

        Ok(key_maps)
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

fn apply_item_filter(
    items: &[HashMap<String, AttributeValue>],
    filter: Option<&serde_json::Value>,
) -> Result<Vec<HashMap<String, AttributeValue>>, DbError> {
    let Some(filter_value) = filter else {
        return Ok(items.to_vec());
    };

    let mut filtered = Vec::new();

    for item in items {
        if item_matches_filter(item, filter_value)? {
            filtered.push(item.clone());
        }
    }

    Ok(filtered)
}

fn item_matches_filter(
    item: &HashMap<String, AttributeValue>,
    filter: &serde_json::Value,
) -> Result<bool, DbError> {
    let filter_object = filter
        .as_object()
        .ok_or_else(|| DbError::query_failed("DynamoDB filter must be a JSON object"))?;

    evaluate_filter_object(item, filter_object)
}

fn evaluate_filter_object(
    item: &HashMap<String, AttributeValue>,
    filter_object: &serde_json::Map<String, serde_json::Value>,
) -> Result<bool, DbError> {
    for (field, expected_value) in filter_object {
        if field == "$and" {
            let clauses = expected_value
                .as_array()
                .ok_or_else(|| DbError::query_failed("$and requires an array of filter objects"))?;

            for clause in clauses {
                let clause_object = clause.as_object().ok_or_else(|| {
                    DbError::query_failed("$and requires an array of filter objects")
                })?;

                if !evaluate_filter_object(item, clause_object)? {
                    return Ok(false);
                }
            }

            continue;
        }

        if field == "$or" {
            let clauses = expected_value
                .as_array()
                .ok_or_else(|| DbError::query_failed("$or requires an array of filter objects"))?;

            let mut any_match = false;
            for clause in clauses {
                let clause_object = clause.as_object().ok_or_else(|| {
                    DbError::query_failed("$or requires an array of filter objects")
                })?;

                if evaluate_filter_object(item, clause_object)? {
                    any_match = true;
                    break;
                }
            }

            if !any_match {
                return Ok(false);
            }

            continue;
        }

        if field.starts_with('$') {
            return Err(DbError::query_failed(format!(
                "Unsupported DynamoDB top-level filter operator '{field}'"
            )));
        }

        let Some(actual_value) = item.get(field) else {
            return Ok(false);
        };

        let actual_json = attribute_value_to_json(actual_value)?;

        let matches = if let Some(expected_object) = expected_value.as_object() {
            if expected_object.keys().any(|key| key.starts_with('$')) {
                evaluate_filter_operators(&actual_json, expected_object)?
            } else {
                actual_json == *expected_value
            }
        } else {
            actual_json == *expected_value
        };

        if !matches {
            return Ok(false);
        }
    }

    Ok(true)
}

fn evaluate_filter_operators(
    actual_value: &serde_json::Value,
    expected_object: &serde_json::Map<String, serde_json::Value>,
) -> Result<bool, DbError> {
    for (operator, expected_value) in expected_object {
        let matches = match operator.as_str() {
            "$eq" => actual_value == expected_value,
            "$ne" => actual_value != expected_value,
            "$gt" => compare_json_order(actual_value, expected_value, std::cmp::Ordering::Greater)?,
            "$gte" => {
                let ordering = compare_json_partial_order(actual_value, expected_value)?;
                matches!(
                    ordering,
                    Some(std::cmp::Ordering::Greater) | Some(std::cmp::Ordering::Equal)
                )
            }
            "$lt" => compare_json_order(actual_value, expected_value, std::cmp::Ordering::Less)?,
            "$lte" => {
                let ordering = compare_json_partial_order(actual_value, expected_value)?;
                matches!(
                    ordering,
                    Some(std::cmp::Ordering::Less) | Some(std::cmp::Ordering::Equal)
                )
            }
            "$begins_with" => {
                let actual_text = actual_value.as_str().ok_or_else(|| {
                    DbError::query_failed(
                        "$begins_with requires a string field value for DynamoDB filtering",
                    )
                })?;

                let expected_text = expected_value.as_str().ok_or_else(|| {
                    DbError::query_failed(
                        "$begins_with requires a string comparison value for DynamoDB filtering",
                    )
                })?;

                actual_text.starts_with(expected_text)
            }
            _ => {
                return Err(DbError::query_failed(format!(
                    "Unsupported DynamoDB filter operator '{operator}'"
                )));
            }
        };

        if !matches {
            return Ok(false);
        }
    }

    Ok(true)
}

fn compare_json_order(
    actual_value: &serde_json::Value,
    expected_value: &serde_json::Value,
    expected_ordering: std::cmp::Ordering,
) -> Result<bool, DbError> {
    let ordering = compare_json_partial_order(actual_value, expected_value)?;
    Ok(ordering == Some(expected_ordering))
}

fn compare_json_partial_order(
    actual_value: &serde_json::Value,
    expected_value: &serde_json::Value,
) -> Result<Option<std::cmp::Ordering>, DbError> {
    if let (Some(left), Some(right)) = (actual_value.as_f64(), expected_value.as_f64()) {
        return Ok(left.partial_cmp(&right));
    }

    if let (Some(left), Some(right)) = (actual_value.as_str(), expected_value.as_str()) {
        return Ok(Some(left.cmp(right)));
    }

    Err(DbError::query_failed(
        "DynamoDB comparison operators require comparable string or numeric values",
    ))
}

fn attribute_value_to_json(value: &AttributeValue) -> Result<serde_json::Value, DbError> {
    if let Ok(string) = value.as_s() {
        return Ok(serde_json::Value::String(string.clone()));
    }

    if let Ok(number) = value.as_n() {
        if let Ok(integer_value) = number.parse::<i64>() {
            return Ok(serde_json::Value::Number(serde_json::Number::from(
                integer_value,
            )));
        }

        let float_value = number.parse::<f64>().map_err(|error| {
            DbError::query_failed(format!(
                "Invalid DynamoDB numeric value '{number}': {error}"
            ))
        })?;

        let float_number = serde_json::Number::from_f64(float_value).ok_or_else(|| {
            DbError::query_failed(format!("Invalid DynamoDB numeric value '{number}'"))
        })?;

        return Ok(serde_json::Value::Number(float_number));
    }

    if let Ok(boolean) = value.as_bool() {
        return Ok(serde_json::Value::Bool(*boolean));
    }

    if let Ok(is_null) = value.as_null()
        && *is_null
    {
        return Ok(serde_json::Value::Null);
    }

    if let Ok(map) = value.as_m() {
        let mut json_map = serde_json::Map::new();

        for (key, nested_value) in map {
            json_map.insert(key.clone(), attribute_value_to_json(nested_value)?);
        }

        return Ok(serde_json::Value::Object(json_map));
    }

    if let Ok(list) = value.as_l() {
        let mut json_list = Vec::with_capacity(list.len());

        for nested_value in list {
            json_list.push(attribute_value_to_json(nested_value)?);
        }

        return Ok(serde_json::Value::Array(json_list));
    }

    if let Ok(blob) = value.as_b() {
        return Ok(serde_json::Value::Array(
            blob.as_ref()
                .iter()
                .map(|byte| serde_json::Value::Number(serde_json::Number::from(*byte)))
                .collect(),
        ));
    }

    if let Ok(strings) = value.as_ss() {
        return Ok(serde_json::Value::Array(
            strings
                .iter()
                .map(|item| serde_json::Value::String(item.clone()))
                .collect(),
        ));
    }

    if let Ok(numbers) = value.as_ns() {
        let mut out = Vec::with_capacity(numbers.len());

        for item in numbers {
            if let Ok(integer_value) = item.parse::<i64>() {
                out.push(serde_json::Value::Number(serde_json::Number::from(
                    integer_value,
                )));
                continue;
            }

            let float_value = item.parse::<f64>().map_err(|error| {
                DbError::query_failed(format!("Invalid DynamoDB numeric value '{item}': {error}"))
            })?;
            let float_number = serde_json::Number::from_f64(float_value).ok_or_else(|| {
                DbError::query_failed(format!("Invalid DynamoDB numeric value '{item}'"))
            })?;

            out.push(serde_json::Value::Number(float_number));
        }

        return Ok(serde_json::Value::Array(out));
    }

    if let Ok(blobs) = value.as_bs() {
        return Ok(serde_json::Value::Array(
            blobs
                .iter()
                .map(|item| {
                    serde_json::Value::Array(
                        item.as_ref()
                            .iter()
                            .map(|byte| serde_json::Value::Number(serde_json::Number::from(*byte)))
                            .collect(),
                    )
                })
                .collect(),
        ));
    }

    Err(DbError::query_failed(
        "Unsupported DynamoDB attribute value in filter evaluation",
    ))
}

fn ensure_default_database(database: Option<&str>) -> Result<(), DbError> {
    let selected_database = database.unwrap_or(DYNAMODB_DEFAULT_DATABASE);

    if selected_database != DYNAMODB_DEFAULT_DATABASE {
        return Err(DbError::object_not_found(format!(
            "Database '{}' is not available for DynamoDB",
            selected_database
        )));
    }

    Ok(())
}

fn build_item_batches<T>(items: Vec<T>, batch_size: usize) -> Result<Vec<Vec<T>>, DbError> {
    if batch_size == 0 {
        return Err(DbError::query_failed(
            "Batch size must be greater than zero",
        ));
    }

    if items.is_empty() {
        return Ok(Vec::new());
    }

    let mut batches = Vec::new();
    let mut current_batch = Vec::with_capacity(batch_size);

    for item in items {
        current_batch.push(item);
        if current_batch.len() == batch_size {
            batches.push(current_batch);
            current_batch = Vec::with_capacity(batch_size);
        }
    }

    if !current_batch.is_empty() {
        batches.push(current_batch);
    }

    Ok(batches)
}

fn build_batch_put_write_requests(
    items: &[HashMap<String, AttributeValue>],
) -> Result<Vec<WriteRequest>, DbError> {
    let mut requests = Vec::with_capacity(items.len());

    for item in items {
        let put_request = PutRequest::builder()
            .set_item(Some(item.clone()))
            .build()
            .map_err(|error| {
                DbError::query_failed(format!(
                    "Failed to build batch put request payload: {error}"
                ))
            })?;

        let write_request = WriteRequest::builder().put_request(put_request).build();
        requests.push(write_request);
    }

    Ok(requests)
}

fn execute_batch_write_with_retries(
    client: &Client,
    table: &str,
    write_requests: Vec<WriteRequest>,
    runtime: &tokio::runtime::Runtime,
    config: &DynamoProfileConfig,
) -> Result<(), DbError> {
    let mut pending = write_requests;

    for attempt in 1..=DYNAMODB_BATCH_WRITE_MAX_ATTEMPTS {
        let request_items = HashMap::from([(table.to_string(), pending)]);

        let output = runtime
            .block_on(
                client
                    .batch_write_item()
                    .set_request_items(Some(request_items))
                    .send(),
            )
            .map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_batch_write_error(&error, config);
                classify_query_error(formatted)
            })?;

        let mut next_pending = output
            .unprocessed_items()
            .and_then(|items| items.get(table))
            .cloned()
            .unwrap_or_default();

        if next_pending.is_empty() {
            return Ok(());
        }

        if attempt == DYNAMODB_BATCH_WRITE_MAX_ATTEMPTS {
            return Err(DbError::query_failed(format!(
                "DynamoDB batch write left {} unprocessed item(s) for table '{}' after {} attempt(s)",
                next_pending.len(),
                table,
                DYNAMODB_BATCH_WRITE_MAX_ATTEMPTS
            )));
        }

        let backoff_ms = DYNAMODB_BATCH_WRITE_BASE_BACKOFF_MS
            .saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1)));
        std::thread::sleep(std::time::Duration::from_millis(backoff_ms));

        pending = std::mem::take(&mut next_pending);
    }

    Err(DbError::query_failed(
        "DynamoDB batch write retry loop exhausted unexpectedly",
    ))
}

fn crud_result_with_affected_rows(affected_rows: usize) -> dbflux_core::CrudResult {
    dbflux_core::CrudResult::new(affected_rows as u64, None)
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
    index_kinds: HashMap<String, DynamoIndexKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DynamoIndexKind {
    LocalSecondary,
    GlobalSecondary,
}

#[derive(Debug, Clone)]
enum DynamoReadStrategy {
    Scan(DynamoScanPlan),
    Query(DynamoQueryPlan),
}

#[derive(Debug, Clone)]
struct DynamoScanPlan {
    index_name: Option<String>,
    consistent_read: bool,
    server_filter_expression: Option<String>,
    server_filter_attribute_names: HashMap<String, String>,
    server_filter_attribute_values: HashMap<String, AttributeValue>,
    client_filter: Option<serde_json::Value>,
    filter_fallback_policy: DynamoFilterFallbackPolicy,
}

#[derive(Debug, Clone)]
struct DynamoQueryPlan {
    index_name: Option<String>,
    consistent_read: bool,
    key_condition_expression: String,
    key_expression_attribute_names: HashMap<String, String>,
    key_expression_attribute_values: HashMap<String, AttributeValue>,
    server_filter_expression: Option<String>,
    server_filter_attribute_names: HashMap<String, String>,
    server_filter_attribute_values: HashMap<String, AttributeValue>,
    client_filter: Option<serde_json::Value>,
    filter_fallback_policy: DynamoFilterFallbackPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DynamoFilterFallbackPolicy {
    ClientSide,
    Reject,
}

impl DynamoReadStrategy {
    fn filter_fallback_policy(&self) -> DynamoFilterFallbackPolicy {
        match self {
            Self::Scan(plan) => plan.filter_fallback_policy,
            Self::Query(plan) => plan.filter_fallback_policy,
        }
    }

    fn client_filter(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Scan(plan) => plan.client_filter.as_ref(),
            Self::Query(plan) => plan.client_filter.as_ref(),
        }
    }
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

#[derive(Debug, Clone)]
struct DynamoInsertPlan {
    table: String,
    item_batches: Vec<Vec<HashMap<String, AttributeValue>>>,
    total_items: usize,
}

#[derive(Debug, Clone)]
struct DynamoUpdatePlan {
    table: String,
    key_map: HashMap<String, AttributeValue>,
    update_expression: String,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
}

#[derive(Debug, Clone)]
struct DynamoUpdateManyPlan {
    table: String,
    key_maps: Vec<HashMap<String, AttributeValue>>,
    update_expression: String,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
}

#[derive(Debug, Clone)]
struct DynamoUpsertPlan {
    table: String,
    key_map: HashMap<String, AttributeValue>,
    partition_key_name: String,
    sort_key_name: Option<String>,
    update_expression: String,
    expression_attribute_names: HashMap<String, String>,
    expression_attribute_values: HashMap<String, AttributeValue>,
    insert_attributes: HashMap<String, AttributeValue>,
}

#[derive(Debug, Clone)]
struct DynamoDeletePlan {
    table: String,
    key_map: HashMap<String, AttributeValue>,
}

#[derive(Debug, Clone)]
struct DynamoDeleteManyPlan {
    table: String,
    key_maps: Vec<HashMap<String, AttributeValue>>,
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
    read_options: &DynamoReadOptions,
) -> Result<DynamoReadStrategy, DbError> {
    let filter_fallback_policy = map_filter_fallback_policy(read_options.filter_fallback);

    if let Some(partition_key) = key_schema.partition_key.as_ref()
        && let Some(filter_obj) = filter.and_then(serde_json::Value::as_object)
        && let Some(partition_filter_value) = filter_obj.get(partition_key)
        && let Some(partition_value_json) = extract_eq_filter_value(partition_filter_value)
    {
        let mut key_expression_attribute_names = HashMap::new();
        let mut key_expression_attribute_values = HashMap::new();

        key_expression_attribute_names.insert("#pk".to_string(), partition_key.clone());
        key_expression_attribute_values.insert(
            ":pk".to_string(),
            json_value_to_attribute_value(partition_value_json)?,
        );

        let mut key_condition_expression = "#pk = :pk".to_string();
        let mut consumed_filter_fields = vec![partition_key.clone()];

        if let Some(sort_key) = key_schema.sort_key.as_ref()
            && let Some(sort_filter_value) = filter_obj.get(sort_key)
            && let Some((condition_suffix, token, sort_value_json)) =
                build_sort_key_condition(sort_filter_value)
        {
            key_expression_attribute_names.insert("#sk".to_string(), sort_key.clone());
            key_expression_attribute_values
                .insert(token, json_value_to_attribute_value(sort_value_json)?);
            key_condition_expression.push_str(&condition_suffix);
            consumed_filter_fields.push(sort_key.clone());
        }

        let residual_filter = residual_filter(filter, &consumed_filter_fields)?;
        let filter_execution =
            resolve_filter_execution(residual_filter.as_ref(), filter_fallback_policy, "query")?;

        return Ok(DynamoReadStrategy::Query(DynamoQueryPlan {
            index_name: read_options.index_name.clone(),
            consistent_read: read_options.consistent_read,
            key_condition_expression,
            key_expression_attribute_names,
            key_expression_attribute_values,
            server_filter_expression: filter_execution.server_filter_expression,
            server_filter_attribute_names: filter_execution.server_filter_attribute_names,
            server_filter_attribute_values: filter_execution.server_filter_attribute_values,
            client_filter: filter_execution.client_filter,
            filter_fallback_policy,
        }));
    }

    let residual_filter = residual_filter(filter, &[])?;
    let filter_execution =
        resolve_filter_execution(residual_filter.as_ref(), filter_fallback_policy, "scan")?;

    Ok(DynamoReadStrategy::Scan(DynamoScanPlan {
        index_name: read_options.index_name.clone(),
        consistent_read: read_options.consistent_read,
        server_filter_expression: filter_execution.server_filter_expression,
        server_filter_attribute_names: filter_execution.server_filter_attribute_names,
        server_filter_attribute_values: filter_execution.server_filter_attribute_values,
        client_filter: filter_execution.client_filter,
        filter_fallback_policy,
    }))
}

fn map_filter_fallback_policy(fallback: DynamoFilterFallback) -> DynamoFilterFallbackPolicy {
    match fallback {
        DynamoFilterFallback::ClientSide => DynamoFilterFallbackPolicy::ClientSide,
        DynamoFilterFallback::Reject => DynamoFilterFallbackPolicy::Reject,
    }
}

#[derive(Debug)]
struct DynamoFilterExecution {
    server_filter_expression: Option<String>,
    server_filter_attribute_names: HashMap<String, String>,
    server_filter_attribute_values: HashMap<String, AttributeValue>,
    client_filter: Option<serde_json::Value>,
}

#[derive(Debug)]
enum DynamoFilterTranslationError {
    Unsupported(String),
    Invalid(String),
}

#[derive(Debug, Default)]
struct DynamoFilterTranslationState {
    next_name_token: usize,
    next_value_token: usize,
    name_token_by_segment: HashMap<String, String>,
    attribute_names: HashMap<String, String>,
    attribute_values: HashMap<String, AttributeValue>,
}

fn residual_filter(
    filter: Option<&serde_json::Value>,
    consumed_fields: &[String],
) -> Result<Option<serde_json::Value>, DbError> {
    let Some(filter_value) = filter else {
        return Ok(None);
    };

    let mut filter_object = filter_value
        .as_object()
        .ok_or_else(|| DbError::query_failed("DynamoDB filter must be a JSON object"))?
        .clone();

    for field in consumed_fields {
        filter_object.remove(field);
    }

    if filter_object.is_empty() {
        return Ok(None);
    }

    Ok(Some(serde_json::Value::Object(filter_object)))
}

fn resolve_filter_execution(
    filter: Option<&serde_json::Value>,
    fallback_policy: DynamoFilterFallbackPolicy,
    operation: &str,
) -> Result<DynamoFilterExecution, DbError> {
    let Some(filter_value) = filter else {
        return Ok(DynamoFilterExecution {
            server_filter_expression: None,
            server_filter_attribute_names: HashMap::new(),
            server_filter_attribute_values: HashMap::new(),
            client_filter: None,
        });
    };

    let mut state = DynamoFilterTranslationState::default();

    match translate_filter_object(filter_value, &mut state) {
        Ok(server_filter_expression) => Ok(DynamoFilterExecution {
            server_filter_expression: Some(server_filter_expression),
            server_filter_attribute_names: state.attribute_names,
            server_filter_attribute_values: state.attribute_values,
            client_filter: None,
        }),
        Err(DynamoFilterTranslationError::Unsupported(reason)) => match fallback_policy {
            DynamoFilterFallbackPolicy::ClientSide => Ok(DynamoFilterExecution {
                server_filter_expression: None,
                server_filter_attribute_names: HashMap::new(),
                server_filter_attribute_values: HashMap::new(),
                client_filter: Some(filter_value.clone()),
            }),
            DynamoFilterFallbackPolicy::Reject => Err(DbError::query_failed(format!(
                "DynamoDB {operation} filter translation failed: {reason}"
            ))),
        },
        Err(DynamoFilterTranslationError::Invalid(reason)) => Err(DbError::query_failed(reason)),
    }
}

fn translate_filter_object(
    filter: &serde_json::Value,
    state: &mut DynamoFilterTranslationState,
) -> Result<String, DynamoFilterTranslationError> {
    let filter_object = filter.as_object().ok_or_else(|| {
        DynamoFilterTranslationError::Invalid("DynamoDB filter must be a JSON object".to_string())
    })?;

    if filter_object.is_empty() {
        return Err(DynamoFilterTranslationError::Unsupported(
            "empty filter object is not translatable".to_string(),
        ));
    }

    let mut clauses = Vec::new();

    for (field, expected_value) in filter_object {
        if field == "$and" {
            let and_clause = translate_logical_filter("AND", expected_value, state)?;
            clauses.push(and_clause);
            continue;
        }

        if field == "$or" {
            let or_clause = translate_logical_filter("OR", expected_value, state)?;
            clauses.push(or_clause);
            continue;
        }

        if field.starts_with('$') {
            return Err(DynamoFilterTranslationError::Unsupported(format!(
                "top-level operator '{field}' is not supported for server-side translation"
            )));
        }

        clauses.push(translate_field_filter(field, expected_value, state)?);
    }

    Ok(clauses.join(" AND "))
}

fn translate_logical_filter(
    join_operator: &str,
    value: &serde_json::Value,
    state: &mut DynamoFilterTranslationState,
) -> Result<String, DynamoFilterTranslationError> {
    let clauses = value.as_array().ok_or_else(|| {
        DynamoFilterTranslationError::Invalid(format!(
            "${} requires an array of filter objects",
            join_operator.to_ascii_lowercase()
        ))
    })?;

    if clauses.is_empty() {
        return Err(DynamoFilterTranslationError::Invalid(format!(
            "${} requires at least one filter object",
            join_operator.to_ascii_lowercase()
        )));
    }

    let mut translated = Vec::with_capacity(clauses.len());

    for clause in clauses {
        translated.push(translate_filter_object(clause, state)?);
    }

    if translated.len() == 1 {
        return Ok(translated.remove(0));
    }

    Ok(format!(
        "({})",
        translated.join(&format!(" {join_operator} "))
    ))
}

fn translate_field_filter(
    field: &str,
    expected_value: &serde_json::Value,
    state: &mut DynamoFilterTranslationState,
) -> Result<String, DynamoFilterTranslationError> {
    let path_expression = bind_attribute_path(field, state)?;

    if let Some(expected_object) = expected_value.as_object()
        && expected_object.keys().any(|key| key.starts_with('$'))
    {
        let mut clauses = Vec::new();

        for (operator, value) in expected_object {
            clauses.push(translate_field_operator(
                &path_expression,
                operator,
                value,
                state,
            )?);
        }

        return Ok(clauses.join(" AND "));
    }

    let value_token = bind_attribute_value(expected_value, state)?;
    Ok(format!("{path_expression} = {value_token}"))
}

fn translate_field_operator(
    path_expression: &str,
    operator: &str,
    value: &serde_json::Value,
    state: &mut DynamoFilterTranslationState,
) -> Result<String, DynamoFilterTranslationError> {
    match operator {
        "$eq" => {
            let value_token = bind_attribute_value(value, state)?;
            Ok(format!("{path_expression} = {value_token}"))
        }
        "$ne" => {
            let value_token = bind_attribute_value(value, state)?;
            Ok(format!("{path_expression} <> {value_token}"))
        }
        "$gt" => {
            let value_token = bind_attribute_value(value, state)?;
            Ok(format!("{path_expression} > {value_token}"))
        }
        "$gte" => {
            let value_token = bind_attribute_value(value, state)?;
            Ok(format!("{path_expression} >= {value_token}"))
        }
        "$lt" => {
            let value_token = bind_attribute_value(value, state)?;
            Ok(format!("{path_expression} < {value_token}"))
        }
        "$lte" => {
            let value_token = bind_attribute_value(value, state)?;
            Ok(format!("{path_expression} <= {value_token}"))
        }
        "$begins_with" => {
            if !value.is_string() {
                return Err(DynamoFilterTranslationError::Invalid(
                    "$begins_with requires a string comparison value".to_string(),
                ));
            }

            let value_token = bind_attribute_value(value, state)?;
            Ok(format!("begins_with({path_expression}, {value_token})"))
        }
        _ => Err(DynamoFilterTranslationError::Unsupported(format!(
            "operator '{operator}' is not translatable"
        ))),
    }
}

fn bind_attribute_path(
    path: &str,
    state: &mut DynamoFilterTranslationState,
) -> Result<String, DynamoFilterTranslationError> {
    let mut tokens = Vec::new();

    for segment in path.split('.') {
        if segment.trim().is_empty() {
            return Err(DynamoFilterTranslationError::Unsupported(format!(
                "attribute path '{path}' is invalid"
            )));
        }

        if let Some(existing_token) = state.name_token_by_segment.get(segment) {
            tokens.push(existing_token.clone());
            continue;
        }

        let token = format!("#n{}", state.next_name_token);
        state.next_name_token += 1;

        state
            .name_token_by_segment
            .insert(segment.to_string(), token.clone());
        state
            .attribute_names
            .insert(token.clone(), segment.to_string());
        tokens.push(token);
    }

    Ok(tokens.join("."))
}

fn bind_attribute_value(
    value: &serde_json::Value,
    state: &mut DynamoFilterTranslationState,
) -> Result<String, DynamoFilterTranslationError> {
    let token = format!(":v{}", state.next_value_token);
    state.next_value_token += 1;

    let attribute_value = json_value_to_attribute_value(value).map_err(|error| {
        DynamoFilterTranslationError::Invalid(format!(
            "DynamoDB filter value cannot be represented as AttributeValue: {error}"
        ))
    })?;

    state
        .attribute_values
        .insert(token.clone(), attribute_value);
    Ok(token)
}

fn extract_eq_filter_value(filter_value: &serde_json::Value) -> Option<&serde_json::Value> {
    if !filter_value.is_object() {
        return Some(filter_value);
    }

    filter_value.as_object()?.get("$eq")
}

fn build_sort_key_condition(
    filter_value: &serde_json::Value,
) -> Option<(String, String, &serde_json::Value)> {
    if !filter_value.is_object() {
        return Some((
            " AND #sk = :sk".to_string(),
            ":sk".to_string(),
            filter_value,
        ));
    }

    let filter_object = filter_value.as_object()?;

    if let Some(value) = filter_object.get("$eq") {
        return Some((" AND #sk = :sk".to_string(), ":sk".to_string(), value));
    }

    if let Some(value) = filter_object.get("$gt") {
        return Some((" AND #sk > :sk_gt".to_string(), ":sk_gt".to_string(), value));
    }

    if let Some(value) = filter_object.get("$gte") {
        return Some((
            " AND #sk >= :sk_gte".to_string(),
            ":sk_gte".to_string(),
            value,
        ));
    }

    if let Some(value) = filter_object.get("$lt") {
        return Some((" AND #sk < :sk_lt".to_string(), ":sk_lt".to_string(), value));
    }

    if let Some(value) = filter_object.get("$lte") {
        return Some((
            " AND #sk <= :sk_lte".to_string(),
            ":sk_lte".to_string(),
            value,
        ));
    }

    if let Some(value) = filter_object.get("$begins_with") {
        return Some((
            " AND begins_with(#sk, :sk_prefix)".to_string(),
            ":sk_prefix".to_string(),
            value,
        ));
    }

    None
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
        DynamoReadStrategy::Scan(plan) => {
            let mut request = client
                .scan()
                .table_name(table)
                .set_index_name(plan.index_name.clone())
                .consistent_read(plan.consistent_read)
                .limit(limit)
                .set_exclusive_start_key(start_key)
                .set_expression_attribute_names(if plan.server_filter_attribute_names.is_empty() {
                    None
                } else {
                    Some(plan.server_filter_attribute_names.clone())
                })
                .set_expression_attribute_values(
                    if plan.server_filter_attribute_values.is_empty() {
                        None
                    } else {
                        Some(plan.server_filter_attribute_values.clone())
                    },
                );

            if let Some(filter_expression) = plan.server_filter_expression.as_ref() {
                request = request.filter_expression(filter_expression);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_scan_error(&error, config);
                classify_query_error(formatted)
            })?;

            Ok(DynamoFetchedPage {
                items: output.items().to_vec(),
                last_evaluated_key: output.last_evaluated_key().cloned(),
            })
        }
        DynamoReadStrategy::Query(plan) => {
            let mut expression_attribute_names = plan.key_expression_attribute_names.clone();
            expression_attribute_names.extend(plan.server_filter_attribute_names.clone());

            let mut expression_attribute_values = plan.key_expression_attribute_values.clone();
            expression_attribute_values.extend(plan.server_filter_attribute_values.clone());

            let mut request = client
                .query()
                .table_name(table)
                .set_index_name(plan.index_name.clone())
                .consistent_read(plan.consistent_read)
                .key_condition_expression(&plan.key_condition_expression)
                .set_expression_attribute_names(Some(expression_attribute_names))
                .set_expression_attribute_values(Some(expression_attribute_values))
                .limit(limit)
                .set_exclusive_start_key(start_key);

            if let Some(filter_expression) = plan.server_filter_expression.as_ref() {
                request = request.filter_expression(filter_expression);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
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
        DynamoReadStrategy::Scan(plan) => {
            let mut request = client
                .scan()
                .table_name(table)
                .set_index_name(plan.index_name.clone())
                .consistent_read(plan.consistent_read)
                .select(Select::Count)
                .set_exclusive_start_key(start_key)
                .set_expression_attribute_names(if plan.server_filter_attribute_names.is_empty() {
                    None
                } else {
                    Some(plan.server_filter_attribute_names.clone())
                })
                .set_expression_attribute_values(
                    if plan.server_filter_attribute_values.is_empty() {
                        None
                    } else {
                        Some(plan.server_filter_attribute_values.clone())
                    },
                );

            if let Some(filter_expression) = plan.server_filter_expression.as_ref() {
                request = request.filter_expression(filter_expression);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
                let formatted = DYNAMO_ERROR_FORMATTER.format_scan_error(&error, config);
                classify_query_error(formatted)
            })?;

            Ok(DynamoCountPage {
                count: output.count(),
                last_evaluated_key: output.last_evaluated_key().cloned(),
            })
        }
        DynamoReadStrategy::Query(plan) => {
            let mut expression_attribute_names = plan.key_expression_attribute_names.clone();
            expression_attribute_names.extend(plan.server_filter_attribute_names.clone());

            let mut expression_attribute_values = plan.key_expression_attribute_values.clone();
            expression_attribute_values.extend(plan.server_filter_attribute_values.clone());

            let mut request = client
                .query()
                .table_name(table)
                .set_index_name(plan.index_name.clone())
                .consistent_read(plan.consistent_read)
                .key_condition_expression(&plan.key_condition_expression)
                .set_expression_attribute_names(Some(expression_attribute_names))
                .set_expression_attribute_values(Some(expression_attribute_values))
                .select(Select::Count)
                .set_exclusive_start_key(start_key);

            if let Some(filter_expression) = plan.server_filter_expression.as_ref() {
                request = request.filter_expression(filter_expression);
            }

            let output = runtime.block_on(request.send()).map_err(|error| {
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

fn items_to_query_result(
    items: &[HashMap<String, AttributeValue>],
    key_schema: &DynamoTableKeySchema,
) -> QueryResult {
    // Insert PK columns first (partition key, then sort key), then discovered attributes.
    // This ensures the UI can detect PKs even when the table is empty.
    let mut field_names: Vec<String> = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    if let Some(pk) = &key_schema.partition_key
        && seen.insert(pk.clone())
    {
        field_names.push(pk.clone());
    }
    if let Some(sk) = &key_schema.sort_key
        && seen.insert(sk.clone())
    {
        field_names.push(sk.clone());
    }

    for item in items {
        let mut keys: Vec<&String> = item.keys().collect();
        keys.sort();

        for key in keys {
            if seen.insert(key.clone()) {
                field_names.push(key.clone());
            }
        }
    }

    if items.is_empty() {
        // Emit PK columns even with no rows so UI can show CRUD actions.
        let columns = field_names
            .iter()
            .map(|name| {
                let is_primary_key = key_schema.partition_key.as_deref() == Some(name.as_str())
                    || key_schema.sort_key.as_deref() == Some(name.as_str());
                ColumnMeta {
                    name: name.clone(),
                    type_name: "DynamoDB".to_string(),
                    nullable: true,
                    is_primary_key,
                }
            })
            .collect();
        return QueryResult::json(columns, Vec::new(), std::time::Duration::ZERO);
    }

    let columns = field_names
        .iter()
        .map(|name| {
            let is_primary_key = key_schema.partition_key.as_deref() == Some(name.as_str())
                || key_schema.sort_key.as_deref() == Some(name.as_str());

            ColumnMeta {
                name: name.clone(),
                type_name: "DynamoDB".to_string(),
                nullable: true,
                is_primary_key,
            }
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

fn extract_key_map_from_item(
    item: &HashMap<String, AttributeValue>,
    key_schema: &DynamoTableKeySchema,
) -> Result<HashMap<String, AttributeValue>, DbError> {
    let mut key_map = HashMap::new();

    let partition_key = key_schema.partition_key.as_ref().ok_or_else(|| {
        DbError::query_failed(
            "Table metadata is missing a partition key; cannot resolve item identity",
        )
    })?;

    let partition_value = item.get(partition_key).ok_or_else(|| {
        DbError::query_failed(format!(
            "DynamoDB item is missing required partition key '{}' while planning many-item mutation",
            partition_key
        ))
    })?;
    key_map.insert(partition_key.clone(), partition_value.clone());

    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        let sort_value = item.get(sort_key).ok_or_else(|| {
            DbError::query_failed(format!(
                "DynamoDB item is missing required sort key '{}' while planning many-item mutation",
                sort_key
            ))
        })?;
        key_map.insert(sort_key.clone(), sort_value.clone());
    }

    Ok(key_map)
}

fn resolve_upsert_key_map(
    update: &DocumentUpdate,
    key_schema: &DynamoTableKeySchema,
) -> Result<HashMap<String, AttributeValue>, DbError> {
    let filter_object = update.filter.filter.as_object().ok_or_else(|| {
        DbError::query_failed("DynamoDB key filter must be a JSON object for upsert")
    })?;

    let update_object = update_set_object(&update.update)?;

    let mut key_map = HashMap::new();

    let partition_key = key_schema.partition_key.as_ref().ok_or_else(|| {
        DbError::query_failed(
            "Table metadata is missing a partition key; cannot resolve item identity",
        )
    })?;

    let partition_value = filter_object
        .get(partition_key)
        .or_else(|| update_object.get(partition_key))
        .ok_or_else(|| {
            DbError::query_failed(format!(
                "DynamoDB upsert requires partition key '{}' in filter or update payload",
                partition_key
            ))
        })?;
    key_map.insert(
        partition_key.clone(),
        json_value_to_attribute_value(partition_value)?,
    );

    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        let sort_value = filter_object
            .get(sort_key)
            .or_else(|| update_object.get(sort_key))
            .ok_or_else(|| {
                DbError::query_failed(format!(
                    "DynamoDB upsert requires sort key '{}' in filter or update payload",
                    sort_key
                ))
            })?;
        key_map.insert(sort_key.clone(), json_value_to_attribute_value(sort_value)?);
    }

    Ok(key_map)
}

fn strip_key_fields_from_update_payload(
    update: &serde_json::Value,
    key_schema: &DynamoTableKeySchema,
) -> Result<serde_json::Value, DbError> {
    let key_names = key_field_names(key_schema);

    let root = update
        .as_object()
        .ok_or_else(|| DbError::query_failed("DynamoDB update payload must be a JSON object"))?;

    if let Some(explicit_set) = root.get("$set") {
        let set_object = explicit_set
            .as_object()
            .ok_or_else(|| DbError::query_failed("$set must be a JSON object"))?;

        let filtered_set = set_object
            .iter()
            .filter(|(field, _)| !key_names.contains(field.as_str()))
            .map(|(field, value)| (field.clone(), value.clone()))
            .collect();

        return Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
            "$set".to_string(),
            serde_json::Value::Object(filtered_set),
        )])));
    }

    let filtered_root = root
        .iter()
        .filter(|(field, _)| !key_names.contains(field.as_str()))
        .map(|(field, value)| (field.clone(), value.clone()))
        .collect();

    Ok(serde_json::Value::Object(filtered_root))
}

fn extract_non_key_update_attributes(
    update: &serde_json::Value,
    key_schema: &DynamoTableKeySchema,
) -> Result<HashMap<String, AttributeValue>, DbError> {
    let set_object = update_set_object(update)?;
    let key_names = key_field_names(key_schema);

    let mut attributes = HashMap::new();

    for (field, value) in set_object {
        if field.starts_with('$') {
            return Err(DbError::NotSupported(format!(
                "DynamoDB update supports only plain field updates and optional '$set'; operator '{}' is not supported",
                field
            )));
        }

        if key_names.contains(field.as_str()) {
            continue;
        }

        attributes.insert(field.clone(), json_value_to_attribute_value(value)?);
    }

    Ok(attributes)
}

fn update_set_object(
    update: &serde_json::Value,
) -> Result<&serde_json::Map<String, serde_json::Value>, DbError> {
    let root = update
        .as_object()
        .ok_or_else(|| DbError::query_failed("DynamoDB update payload must be a JSON object"))?;

    if let Some(explicit_set) = root.get("$set") {
        return explicit_set
            .as_object()
            .ok_or_else(|| DbError::query_failed("$set must be a JSON object"));
    }

    Ok(root)
}

fn key_field_names(key_schema: &DynamoTableKeySchema) -> std::collections::HashSet<&str> {
    let mut key_names = std::collections::HashSet::new();

    if let Some(partition_key) = key_schema.partition_key.as_ref() {
        key_names.insert(partition_key.as_str());
    }

    if let Some(sort_key) = key_schema.sort_key.as_ref() {
        key_names.insert(sort_key.as_str());
    }

    key_names
}

fn build_upsert_item_map(
    key_map: &HashMap<String, AttributeValue>,
    insert_attributes: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    let mut item = key_map.clone();

    for (field, value) in insert_attributes {
        item.insert(field.clone(), value.clone());
    }

    item
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
                "DynamoDB update supports only plain field updates and optional '$set'; operator '{}' is not supported",
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

fn unsupported_operation(operation: &str, message: &str) -> DbError {
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
    let indexes = table_description_to_indexes(description, &key_components);

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

fn table_description_to_indexes(
    description: &TableDescription,
    primary_components: &[DynamoKeyComponent],
) -> Option<IndexData> {
    let mut indexes = Vec::new();

    if let Some(primary_index) = key_components_to_index_info("PRIMARY", primary_components, true) {
        indexes.push(primary_index);
    }

    for index in description.local_secondary_indexes() {
        let Some(index_name) = index.index_name() else {
            continue;
        };

        let components =
            extract_key_components(index.key_schema(), description.attribute_definitions());
        if let Some(index_info) = key_components_to_index_info(index_name, &components, false) {
            indexes.push(index_info);
        }
    }

    for index in description.global_secondary_indexes() {
        let Some(index_name) = index.index_name() else {
            continue;
        };

        let components =
            extract_key_components(index.key_schema(), description.attribute_definitions());
        if let Some(index_info) = key_components_to_index_info(index_name, &components, false) {
            indexes.push(index_info);
        }
    }

    if indexes.is_empty() {
        None
    } else {
        Some(IndexData::Document(indexes))
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

fn extract_table_index_kinds(description: &TableDescription) -> HashMap<String, DynamoIndexKind> {
    let mut index_kinds = HashMap::new();

    for index in description.local_secondary_indexes() {
        if let Some(index_name) = index.index_name() {
            index_kinds.insert(index_name.to_string(), DynamoIndexKind::LocalSecondary);
        }
    }

    for index in description.global_secondary_indexes() {
        if let Some(index_name) = index.index_name() {
            index_kinds.insert(index_name.to_string(), DynamoIndexKind::GlobalSecondary);
        }
    }

    index_kinds
}

fn validate_read_options(
    table: &str,
    key_schema: &DynamoTableKeySchema,
    read_options: &DynamoReadOptions,
) -> Result<(), DbError> {
    let Some(index_name) = read_options.index_name.as_ref() else {
        return Ok(());
    };

    let Some(index_kind) = key_schema.index_kinds.get(index_name) else {
        let mut available_indexes: Vec<&str> =
            key_schema.index_kinds.keys().map(String::as_str).collect();
        available_indexes.sort();

        let available_text = if available_indexes.is_empty() {
            "none".to_string()
        } else {
            available_indexes.join(", ")
        };

        return Err(DbError::query_failed(format!(
            "DynamoDB table '{table}' does not define index '{index_name}' (available indexes: {available_text})"
        )));
    };

    if *index_kind == DynamoIndexKind::GlobalSecondary && read_options.consistent_read {
        return Err(DbError::query_failed(format!(
            "DynamoDB global secondary index '{index_name}' on table '{table}' cannot be used with 'consistent_read=true'"
        )));
    }

    Ok(())
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

#[cfg(test)]
fn key_components_to_indexes(key_components: &[DynamoKeyComponent]) -> Option<IndexData> {
    key_components_to_index_info("PRIMARY", key_components, true)
        .map(|index| IndexData::Document(vec![index]))
}

fn key_components_to_index_info(
    name: &str,
    key_components: &[DynamoKeyComponent],
    is_primary: bool,
) -> Option<CollectionIndexInfo> {
    if key_components.is_empty() {
        return None;
    }

    let keys = key_components
        .iter()
        .map(|component| (component.name.clone(), IndexDirection::Ascending))
        .collect();

    Some(CollectionIndexInfo {
        name: name.to_string(),
        keys,
        is_unique: is_primary,
        is_sparse: false,
        expire_after_seconds: None,
    })
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
            Some("ConditionalCheckFailedException") => Some(
                "Conditional update check failed. Verify key identity and condition assumptions before retrying.",
            ),
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
        } else if lower.contains("dispatch failure") || lower.contains("dispatch") {
            FormattedError::new("AWS SDK dispatch failure (transient error).")
                .with_hint("This is usually a temporary issue. Try the operation again, or refresh AWS credentials if using SSO.")
                .with_retriable(true)
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

    fn format_batch_write_error(
        &self,
        error: &aws_sdk_dynamodb::error::SdkError<BatchWriteItemError>,
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
        Some("ConditionalCheckFailedException") => DbError::QueryFailed(formatted),
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
        DynamoFilterFallbackPolicy, DynamoIndexKind, DynamoKeyComponent, DynamoKeyRole,
        DynamoLanguageService, DynamoProfileConfig, DynamoReadStrategy, DynamoTableKeySchema,
        append_window_items, apply_item_filter, attribute_value_to_value, build_item_batches,
        build_table_info_from_description, build_update_expression_from_json,
        classify_connection_error, classify_query_error, decide_read_strategy,
        ensure_default_database, ensure_item_contains_required_keys, extract_key_map_from_filter,
        extract_non_key_update_attributes, json_value_to_attribute_value, key_components_to_fields,
        key_components_to_indexes, normalize_table_names, resolve_upsert_key_map,
        strip_key_fields_from_update_payload, unsupported_operation, validate_read_options,
    };
    use crate::query_parser::{DynamoFilterFallback, DynamoReadOptions};
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
        TableDescription,
    };
    use dbflux_core::{
        ConnectionProfile, DangerousQueryKind, DatabaseCategory, DbConfig, DbDriver, DbError,
        DocumentFilter, DocumentUpdate, DriverCapabilities, FormValues, IndexData, LanguageService,
    };
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn metadata_uses_document_semantics_with_truthful_phase4_caps() {
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
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::PAGINATION)
        );
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::FILTERING)
        );
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::NESTED_DOCUMENTS)
        );
        assert!(
            DYNAMODB_METADATA
                .capabilities
                .contains(DriverCapabilities::ARRAYS)
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
    fn conditional_check_failure_hint_is_actionable() {
        let formatter = DynamoErrorFormatter;
        let config = DynamoProfileConfig {
            region: "us-east-1".to_string(),
            profile: None,
            endpoint: None,
            table: None,
        };

        let formatted = formatter.format_from_code(
            Some("ConditionalCheckFailedException"),
            "The conditional request failed",
            &config,
        );

        let mapped = classify_query_error(formatted);

        match mapped {
            DbError::QueryFailed(details) => {
                let hint = details.hint.unwrap_or_default().to_ascii_lowercase();
                assert!(hint.contains("conditional") || hint.contains("key identity"));
            }
            other => panic!("expected QueryFailed, got {other:?}"),
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
            ..Default::default()
        };

        let read_options = DynamoReadOptions::default();

        let query_strategy =
            decide_read_strategy(Some(&json!({"pk":"A","sk":1})), &key_schema, &read_options)
                .expect("strategy decision should succeed");
        assert!(matches!(query_strategy, DynamoReadStrategy::Query(_)));

        let scan_strategy =
            decide_read_strategy(Some(&json!({"other":"A"})), &key_schema, &read_options)
                .expect("strategy decision should succeed");
        assert!(matches!(scan_strategy, DynamoReadStrategy::Scan(_)));
    }

    #[test]
    fn read_strategy_carries_read_options_and_fallback_policy() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            ..Default::default()
        };

        let read_options = DynamoReadOptions {
            index_name: Some("gsi_users_by_status".to_string()),
            consistent_read: true,
            filter_fallback: DynamoFilterFallback::Reject,
        };

        let strategy =
            decide_read_strategy(Some(&json!({"pk":"USER#1"})), &key_schema, &read_options)
                .expect("strategy decision should succeed");

        match strategy {
            DynamoReadStrategy::Query(plan) => {
                assert_eq!(plan.index_name.as_deref(), Some("gsi_users_by_status"));
                assert!(plan.consistent_read);
                assert_eq!(
                    plan.filter_fallback_policy,
                    DynamoFilterFallbackPolicy::Reject
                );
                assert!(plan.server_filter_expression.is_none());
                assert!(plan.server_filter_attribute_names.is_empty());
                assert!(plan.server_filter_attribute_values.is_empty());
            }
            DynamoReadStrategy::Scan(_) => panic!("expected query strategy"),
        }
    }

    #[test]
    fn scan_strategy_carries_fallback_policy_without_key_plan() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            ..Default::default()
        };

        let read_options = DynamoReadOptions {
            index_name: Some("gsi_users_by_status".to_string()),
            consistent_read: false,
            filter_fallback: DynamoFilterFallback::Reject,
        };

        let strategy = decide_read_strategy(
            Some(&json!({"status":"active"})),
            &key_schema,
            &read_options,
        )
        .expect("strategy decision should succeed");

        match strategy {
            DynamoReadStrategy::Scan(plan) => {
                assert_eq!(plan.index_name.as_deref(), Some("gsi_users_by_status"));
                assert!(!plan.consistent_read);
                assert_eq!(
                    plan.filter_fallback_policy,
                    DynamoFilterFallbackPolicy::Reject
                );
            }
            DynamoReadStrategy::Query(_) => panic!("expected scan strategy"),
        }
    }

    #[test]
    fn query_strategy_pushes_non_key_filter_server_side() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: Some("sk".to_string()),
            ..Default::default()
        };

        let read_options = DynamoReadOptions::default();
        let strategy = decide_read_strategy(
            Some(&json!({"pk":"USER#1","status":"active","score":{"$gte":10}})),
            &key_schema,
            &read_options,
        )
        .expect("strategy decision should succeed");

        match strategy {
            DynamoReadStrategy::Query(plan) => {
                assert!(plan.server_filter_expression.is_some());
                assert!(plan.client_filter.is_none());

                let filter_expression = plan.server_filter_expression.unwrap_or_default();
                assert!(filter_expression.contains("#n0"));
                assert!(filter_expression.contains("#n1"));
            }
            DynamoReadStrategy::Scan(_) => panic!("expected query strategy"),
        }
    }

    #[test]
    fn query_strategy_rejects_untranslatable_filter_when_fallback_disabled() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            ..Default::default()
        };

        let read_options = DynamoReadOptions {
            index_name: None,
            consistent_read: false,
            filter_fallback: DynamoFilterFallback::Reject,
        };

        let error = decide_read_strategy(
            Some(&json!({"pk":"USER#1","meta..flag":true})),
            &key_schema,
            &read_options,
        )
        .expect_err("untranslatable filter should fail when fallback is disabled");

        assert!(error.to_string().contains("filter translation failed"));
    }

    #[test]
    fn query_strategy_fallback_uses_client_filter_without_reapplying_key_predicate() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            ..Default::default()
        };

        let read_options = DynamoReadOptions::default();
        let strategy = decide_read_strategy(
            Some(&json!({"pk":"USER#1","meta..flag":true})),
            &key_schema,
            &read_options,
        )
        .expect("fallback-enabled strategy should be constructed");

        match strategy {
            DynamoReadStrategy::Query(plan) => {
                assert!(plan.server_filter_expression.is_none());

                let client_filter = plan
                    .client_filter
                    .expect("client fallback filter must exist");
                let client_filter_object = client_filter
                    .as_object()
                    .expect("client fallback filter should be an object");

                assert!(!client_filter_object.contains_key("pk"));
                assert!(client_filter_object.contains_key("meta..flag"));
            }
            DynamoReadStrategy::Scan(_) => panic!("expected query strategy"),
        }
    }

    #[test]
    fn validate_read_options_rejects_unknown_index_name() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            index_kinds: HashMap::from([(
                "gsi_users_by_status".to_string(),
                DynamoIndexKind::GlobalSecondary,
            )]),
        };

        let read_options = DynamoReadOptions {
            index_name: Some("missing_index".to_string()),
            consistent_read: false,
            filter_fallback: DynamoFilterFallback::ClientSide,
        };

        let error = validate_read_options("users", &key_schema, &read_options)
            .expect_err("unknown index should fail validation");

        assert!(error.to_string().contains("does not define index"));
    }

    #[test]
    fn validate_read_options_rejects_consistent_reads_on_gsi() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            index_kinds: HashMap::from([(
                "gsi_users_by_status".to_string(),
                DynamoIndexKind::GlobalSecondary,
            )]),
        };

        let read_options = DynamoReadOptions {
            index_name: Some("gsi_users_by_status".to_string()),
            consistent_read: true,
            filter_fallback: DynamoFilterFallback::ClientSide,
        };

        let error = validate_read_options("users", &key_schema, &read_options)
            .expect_err("consistent reads against GSI should fail validation");

        assert!(
            error
                .to_string()
                .contains("cannot be used with 'consistent_read=true'")
        );
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
            ..Default::default()
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
            ..Default::default()
        };

        let error = extract_key_map_from_filter(&json!({"pk":"A"}), &key_schema)
            .expect_err("missing sort key should fail");

        assert!(error.to_string().contains("sort key 'sk'"));
    }

    #[test]
    fn upsert_key_resolution_accepts_key_from_update_payload() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            ..Default::default()
        };

        let update = DocumentUpdate {
            collection: "users".to_string(),
            database: None,
            filter: DocumentFilter::new(json!({"status":"active"})),
            update: json!({"$set":{"pk":"USER#1","name":"Alice"}}),
            many: false,
            upsert: true,
        };

        let key_map = resolve_upsert_key_map(&update, &key_schema)
            .expect("upsert should resolve key from filter or update payload");

        assert_eq!(
            key_map.get("pk"),
            Some(&AttributeValue::S("USER#1".to_string()))
        );
    }

    #[test]
    fn upsert_payload_strips_key_fields_from_update_expression_source() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            ..Default::default()
        };

        let sanitized = strip_key_fields_from_update_payload(
            &json!({"$set":{"pk":"USER#1","name":"Alice"}}),
            &key_schema,
        )
        .expect("sanitizing update payload should succeed");

        assert_eq!(sanitized, json!({"$set":{"name":"Alice"}}));
    }

    #[test]
    fn upsert_insert_attributes_exclude_key_fields() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: Some("sk".to_string()),
            ..Default::default()
        };

        let attributes = extract_non_key_update_attributes(
            &json!({"$set":{"pk":"A","sk":"B","name":"Alice"}}),
            &key_schema,
        )
        .expect("extracting insert attributes should succeed");

        assert_eq!(attributes.len(), 1);
        assert_eq!(
            attributes.get("name"),
            Some(&AttributeValue::S("Alice".to_string()))
        );
    }

    #[test]
    fn apply_item_filter_matches_exact_fields() {
        let items = vec![
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
                (
                    "status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
            ]),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#2".to_string())),
                (
                    "status".to_string(),
                    AttributeValue::S("inactive".to_string()),
                ),
            ]),
        ];

        let filtered = apply_item_filter(&items, Some(&json!({"status":"active"})))
            .expect("filtering should succeed");

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].get("pk"),
            Some(&AttributeValue::S("USER#1".to_string()))
        );
    }

    #[test]
    fn apply_item_filter_returns_error_for_non_object_filter() {
        let items = vec![HashMap::from([(
            "pk".to_string(),
            AttributeValue::S("USER#1".to_string()),
        )])];

        let error = apply_item_filter(&items, Some(&json!("invalid")))
            .expect_err("non-object filter should fail");

        assert!(error.to_string().contains("JSON object"));
    }

    #[test]
    fn apply_item_filter_supports_comparison_operators() {
        let items = vec![
            HashMap::from([("score".to_string(), AttributeValue::N("12".to_string()))]),
            HashMap::from([("score".to_string(), AttributeValue::N("3".to_string()))]),
        ];

        let filtered = apply_item_filter(&items, Some(&json!({"score":{"$gt":10}})))
            .expect("comparison operator filter should succeed");

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].get("score"),
            Some(&AttributeValue::N("12".to_string()))
        );
    }

    #[test]
    fn key_strategy_uses_sort_operator_when_present() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: Some("sk".to_string()),
            ..Default::default()
        };

        let read_options = DynamoReadOptions::default();

        let strategy = decide_read_strategy(
            Some(&json!({"pk":"A","sk":{"$gte":5}})),
            &key_schema,
            &read_options,
        )
        .expect("strategy resolution should succeed");

        match strategy {
            DynamoReadStrategy::Query(plan) => {
                assert!(plan.key_condition_expression.contains("#sk >= :sk_gte"));
            }
            DynamoReadStrategy::Scan(_) => panic!("expected query strategy"),
        }
    }

    #[test]
    fn apply_item_filter_supports_and_or_composition() {
        let items = vec![
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#1".to_string())),
                (
                    "status".to_string(),
                    AttributeValue::S("active".to_string()),
                ),
                ("score".to_string(), AttributeValue::N("12".to_string())),
            ]),
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("USER#2".to_string())),
                (
                    "status".to_string(),
                    AttributeValue::S("inactive".to_string()),
                ),
                ("score".to_string(), AttributeValue::N("6".to_string())),
            ]),
        ];

        let filtered = apply_item_filter(
            &items,
            Some(&json!({
                "$and": [
                    {"score": {"$gte": 10}},
                    {"$or": [{"status": "active"}, {"status": "pending"}]}
                ]
            })),
        )
        .expect("logical composition filter should succeed");

        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered[0].get("pk"),
            Some(&AttributeValue::S("USER#1".to_string()))
        );
    }

    #[test]
    fn apply_item_filter_rejects_unknown_top_level_operator() {
        let items = vec![HashMap::from([(
            "pk".to_string(),
            AttributeValue::S("USER#1".to_string()),
        )])];

        let error = apply_item_filter(&items, Some(&json!({"$nor": [{"pk": "USER#1"}]})))
            .expect_err("unknown top-level operator should fail");

        assert!(error.to_string().contains("top-level filter operator"));
    }

    #[test]
    fn update_expression_rejects_key_mutation() {
        let key_schema = DynamoTableKeySchema {
            partition_key: Some("pk".to_string()),
            sort_key: None,
            ..Default::default()
        };

        let error = build_update_expression_from_json(&json!({"$set":{"pk":"new"}}), &key_schema)
            .expect_err("updating partition key should fail");

        assert!(error.to_string().contains("cannot be updated"));
    }

    #[test]
    fn unsupported_operations_are_not_supported_errors() {
        let error = unsupported_operation(
            "update_many_documents",
            "DynamoDB upsert is supported only for single-item updates.",
        );

        match error {
            DbError::NotSupported(message) => {
                assert!(message.contains("update_many_documents"));
            }
            other => panic!("expected NotSupported, got {other:?}"),
        }
    }

    #[test]
    fn insert_planner_batches_items_with_bounded_window() {
        let items = vec![1, 2, 3, 4, 5];
        let batches = build_item_batches(items, 2).expect("batching should succeed");

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0], vec![1, 2]);
        assert_eq!(batches[1], vec![3, 4]);
        assert_eq!(batches[2], vec![5]);
    }

    #[test]
    fn non_default_database_is_rejected_early() {
        let error = ensure_default_database(Some("analytics"))
            .expect_err("non-default database should be rejected");

        assert!(matches!(error, DbError::ObjectNotFound(_)));
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

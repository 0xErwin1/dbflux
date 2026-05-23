use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_cloudwatch::config::Builder as CloudWatchMetricsConfigBuilder;
use aws_sdk_cloudwatch::primitives::DateTime as MetricsDateTime;
use aws_sdk_cloudwatch::types::{Dimension, Metric, MetricDataQuery, MetricStat};
use aws_sdk_cloudwatchlogs::Client;
use aws_sdk_cloudwatchlogs::config::Builder as CloudWatchConfigBuilder;
use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    CLOUDWATCH_FORM, CollectionBrowseRequest, CollectionChildInfo, CollectionChildrenPage,
    CollectionChildrenRequest, CollectionCountRequest, CollectionInfo, CollectionPresentation,
    ColumnKind, ColumnMeta, Connection, ConnectionProfile, DatabaseCategory, DatabaseInfo,
    DbConfig, DbDriver, DbError, DbKind, DeploymentClass, DocumentSchema, DriverCapabilities,
    DriverFormDef, DriverMetadata, EventActorType, EventCategory, EventPage, EventQuery,
    EventRecord, EventSeverity, EventSourceId, EventStreamTarget, ExecutionSourceContext,
    FormValues, Icon, MetricCatalog, QueryLanguage, QueryRequest, QueryResult, SchemaFeatures,
    SchemaLoadingStrategy, SchemaSnapshot, SourceContextSpec, SourceQueryMode, TableInfo,
    ValidationResult, Value,
};

use crate::metric_catalog::{CloudWatchMetricCatalog, RealCloudWatchClient};

pub static CLOUDWATCH_METADATA: LazyLock<DriverMetadata> = LazyLock::new(|| DriverMetadata {
    id: "cloudwatch".into(),
    display_name: "CloudWatch Logs".into(),
    description: "AWS CloudWatch Logs Insights queries with editor-managed source context".into(),
    category: DatabaseCategory::LogStream,
    deployment_class: Some(DeploymentClass::CloudManaged),
    query_language: QueryLanguage::Sql,
    capabilities: DriverCapabilities::AUTHENTICATION
        .union(DriverCapabilities::METRIC_SERIES)
        .union(DriverCapabilities::METRIC_CATALOG),
    default_port: None,
    uri_scheme: "cloudwatch".into(),
    icon: Icon::Logs,
    syntax: None,
    query: None,
    mutation: None,
    ddl: None,
    transactions: None,
    limits: None,
    ssl_modes: None,
    ssl_cert_fields: None,
    classification_override: None,
});

const CLOUDWATCH_DEFAULT_DATABASE: &str = "logs";
const DEFAULT_BROWSE_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;
const MAX_QUERY_WAIT_ATTEMPTS: usize = 120;
const QUERY_POLL_INTERVAL: Duration = Duration::from_millis(500);
const CLOUDWATCH_QUERY_MODE_CWLI: &str = "cwli";
const CLOUDWATCH_QUERY_MODE_PPL: &str = "ppl";
const CLOUDWATCH_QUERY_MODE_SQL: &str = "sql";

static CLOUDWATCH_LANGUAGE_SERVICE: CloudWatchLanguageService = CloudWatchLanguageService;

pub struct CloudWatchDriver;

#[derive(Clone, Debug)]
struct CloudWatchProfileConfig {
    region: String,
    profile: Option<String>,
    endpoint: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct CloudWatchCollectionFilter {
    filter_pattern: Option<String>,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    log_stream_name_prefix: Option<String>,
    log_stream_names: Option<Vec<String>>,
    most_recent: bool,
}

struct CloudWatchConnection {
    client: Client,
    metrics_client: aws_sdk_cloudwatch::Client,
    config: CloudWatchProfileConfig,
    /// Metric catalog implementation backed by the same AWS metrics client.
    metric_catalog_impl: CloudWatchMetricCatalog,
}

struct CloudWatchLanguageService;

impl CloudWatchDriver {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CloudWatchDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl DbDriver for CloudWatchDriver {
    fn kind(&self) -> DbKind {
        DbKind::CloudWatchLogs
    }

    fn metadata(&self) -> &DriverMetadata {
        &CLOUDWATCH_METADATA
    }

    fn form_definition(&self) -> &DriverFormDef {
        &CLOUDWATCH_FORM
    }

    fn driver_key(&self) -> dbflux_core::DriverKey {
        "builtin:cloudwatch".into()
    }

    fn requires_password(&self) -> bool {
        false
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

        Ok(DbConfig::CloudWatchLogs {
            region,
            profile,
            endpoint,
        })
    }

    fn extract_values(&self, config: &DbConfig) -> FormValues {
        let DbConfig::CloudWatchLogs {
            region,
            profile,
            endpoint,
        } = config
        else {
            return HashMap::new();
        };

        let mut values = HashMap::new();
        values.insert("region".to_string(), region.clone());
        values.insert("profile".to_string(), profile.clone().unwrap_or_default());
        values.insert("endpoint".to_string(), endpoint.clone().unwrap_or_default());
        values
    }

    fn connect_with_secrets(
        &self,
        profile: &ConnectionProfile,
        _password: Option<&SecretString>,
        _ssh_secret: Option<&SecretString>,
    ) -> Result<Box<dyn Connection>, DbError> {
        let config = profile_config(&profile.config)?;
        let (client, metrics_client) = build_clients(&config)?;

        probe_connection(&client, &config)?;

        let metric_catalog_impl = CloudWatchMetricCatalog::new(Box::new(
            RealCloudWatchClient::new(metrics_client.clone())?,
        ));

        Ok(Box::new(CloudWatchConnection {
            client,
            metrics_client,
            config,
            metric_catalog_impl,
        }))
    }

    fn test_connection(&self, profile: &ConnectionProfile) -> Result<(), DbError> {
        let config = profile_config(&profile.config)?;
        let (client, _) = build_clients(&config)?;

        probe_connection(&client, &config)
    }
}

impl Connection for CloudWatchConnection {
    fn metadata(&self) -> &DriverMetadata {
        &CLOUDWATCH_METADATA
    }

    fn metric_catalog(&self) -> Option<&dyn MetricCatalog> {
        Some(&self.metric_catalog_impl)
    }

    fn ping(&self) -> Result<(), DbError> {
        probe_connection(&self.client, &self.config)
    }

    fn close(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn execute(&self, req: &QueryRequest) -> Result<QueryResult, DbError> {
        let started = Instant::now();

        let source = req
            .execution_context
            .as_ref()
            .and_then(|context| context.source.as_ref())
            .ok_or_else(|| {
                DbError::query_failed("CloudWatch execution requires structured source context")
            })?;

        // Route on source context: existing logs path for CollectionWindow; the
        // MetricQuery path (GetMetricData) is implemented in the next slice.
        let (log_groups, start_ms, end_ms, query_mode) = match source {
            ExecutionSourceContext::CollectionWindow {
                targets: log_groups,
                start_ms,
                end_ms,
                query_mode,
            } => (log_groups, start_ms, end_ms, query_mode),
            ExecutionSourceContext::MetricQuery {
                namespace,
                metric_name,
                dimensions,
                period_s,
                statistic,
                start_ms,
                end_ms,
            } => {
                return execute_metric_query(
                    &self.metrics_client,
                    namespace,
                    metric_name,
                    dimensions,
                    *period_s,
                    statistic,
                    *start_ms,
                    *end_ms,
                    started,
                );
            }
        };

        let query_mode = query_mode.as_deref().unwrap_or(CLOUDWATCH_QUERY_MODE_CWLI);

        if query_mode != CLOUDWATCH_QUERY_MODE_SQL && log_groups.is_empty() {
            return Err(DbError::query_failed(
                "Select at least one CloudWatch log group before running a query".to_string(),
            ));
        }

        let query_limit = req.limit.unwrap_or(1000).clamp(1, 10_000);
        let start_seconds = start_ms.div_euclid(1000);
        let end_seconds = end_ms.div_euclid(1000);

        let mut start_request = self
            .client
            .start_query()
            .query_string(req.sql.clone())
            .start_time(start_seconds)
            .end_time(end_seconds)
            .limit(query_limit as i32)
            .query_language(cloudwatch_sdk_query_language(query_mode));

        if query_mode != CLOUDWATCH_QUERY_MODE_SQL {
            start_request = start_request.set_log_group_names(Some(log_groups.clone()));
        }

        let start_output = runtime()?.block_on(start_request.send()).map_err(|error| {
            DbError::query_failed(format!("CloudWatch StartQuery failed: {error}"))
        })?;

        let query_id = start_output
            .query_id()
            .map(ToOwned::to_owned)
            .ok_or_else(|| DbError::query_failed("CloudWatch StartQuery returned no query id"))?;

        let mut attempts = 0;
        loop {
            attempts += 1;

            let output = runtime()?
                .block_on(
                    self.client
                        .get_query_results()
                        .query_id(query_id.clone())
                        .send(),
                )
                .map_err(|error| {
                    DbError::query_failed(format!("CloudWatch GetQueryResults failed: {error}"))
                })?;

            let status = output
                .status()
                .map(|value| value.as_str())
                .unwrap_or("Unknown");

            match status {
                "Complete" => {
                    let mut column_order = Vec::new();
                    let mut seen = HashSet::new();
                    let mut row_maps = Vec::new();

                    for result_row in output.results() {
                        let mut row_map = HashMap::new();

                        for field in result_row {
                            let field_name = field.field().unwrap_or("").to_string();
                            if field_name.is_empty() {
                                continue;
                            }

                            if seen.insert(field_name.clone()) {
                                column_order.push(field_name.clone());
                            }

                            let value = field
                                .value()
                                .map(|value| Value::Text(value.to_string()))
                                .unwrap_or(Value::Null);
                            row_map.insert(field_name, value);
                        }

                        row_maps.push(row_map);
                    }

                    let columns = column_order
                        .iter()
                        .map(|name| ColumnMeta {
                            name: name.clone(),
                            type_name: "text".to_string(),
                            kind: cloudwatch_column_kind(name),
                            nullable: true,
                            is_primary_key: false,
                        })
                        .collect::<Vec<_>>();

                    let rows = row_maps
                        .into_iter()
                        .map(|mut row_map| {
                            column_order
                                .iter()
                                .map(|name| row_map.remove(name).unwrap_or(Value::Null))
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();

                    return Ok(QueryResult::table(columns, rows, None, started.elapsed()));
                }
                "Scheduled" | "Running" => {
                    if attempts >= MAX_QUERY_WAIT_ATTEMPTS {
                        return Err(DbError::query_failed(format!(
                            "CloudWatch query did not finish within {} polling attempts",
                            MAX_QUERY_WAIT_ATTEMPTS
                        )));
                    }

                    std::thread::sleep(QUERY_POLL_INTERVAL);
                }
                other => {
                    return Err(DbError::query_failed(format!(
                        "CloudWatch query ended with status {other}"
                    )));
                }
            }
        }
    }

    fn cancel(&self, _handle: &dbflux_core::QueryHandle) -> Result<(), DbError> {
        Err(DbError::NotSupported(
            "Query cancellation not supported for CloudWatch Logs yet".to_string(),
        ))
    }

    fn schema(&self) -> Result<SchemaSnapshot, DbError> {
        let collections = fetch_log_groups(&self.client)?;

        Ok(SchemaSnapshot::document(DocumentSchema {
            databases: vec![DatabaseInfo {
                name: CLOUDWATCH_DEFAULT_DATABASE.to_string(),
                is_current: true,
            }],
            current_database: Some(CLOUDWATCH_DEFAULT_DATABASE.to_string()),
            collections,
        }))
    }

    fn list_databases(&self) -> Result<Vec<DatabaseInfo>, DbError> {
        Ok(vec![DatabaseInfo {
            name: CLOUDWATCH_DEFAULT_DATABASE.to_string(),
            is_current: true,
        }])
    }

    fn browse_collection(&self, request: &CollectionBrowseRequest) -> Result<QueryResult, DbError> {
        let started = Instant::now();
        let filter = CloudWatchCollectionFilter::from_json(request.filter.as_ref())?;

        let limit = request.pagination.limit().clamp(1, 10_000) as usize;
        let offset = request.pagination.offset() as usize;

        if filter.most_recent
            && filter.filter_pattern.is_none()
            && let Some(stream_names) = filter.log_stream_names.as_ref()
            && let [single_stream] = stream_names.as_slice()
        {
            return self.fetch_recent_stream_events(
                request.collection.name.as_str(),
                single_stream.as_str(),
                &filter,
                limit,
                offset,
                started,
            );
        }

        let default_end = current_time_ms()?;
        let default_start = default_end.saturating_sub(DEFAULT_BROWSE_WINDOW_MS);

        let mut next_token: Option<String> = None;
        let mut skipped = 0usize;
        let mut rows = Vec::new();

        loop {
            let mut operation = self
                .client
                .filter_log_events()
                .log_group_name(request.collection.name.clone())
                .limit(limit as i32)
                .start_time(filter.start_ms.unwrap_or(default_start))
                .end_time(filter.end_ms.unwrap_or(default_end));

            if let Some(pattern) = filter.filter_pattern.clone() {
                operation = operation.filter_pattern(pattern);
            }

            if let Some(prefix) = filter.log_stream_name_prefix.clone() {
                operation = operation.log_stream_name_prefix(prefix);
            }

            if let Some(stream_names) = filter.log_stream_names.clone() {
                operation = operation.set_log_stream_names(Some(stream_names));
            }

            if let Some(token) = next_token.clone() {
                operation = operation.next_token(token);
            }

            let output = runtime()?.block_on(operation.send()).map_err(|error| {
                DbError::query_failed(format!("CloudWatch FilterLogEvents failed: {error}"))
            })?;

            for event in output.events() {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if rows.len() >= limit {
                    break;
                }

                rows.push(vec![
                    event.timestamp().map(Value::Int).unwrap_or(Value::Null),
                    event
                        .ingestion_time()
                        .map(Value::Int)
                        .unwrap_or(Value::Null),
                    event
                        .log_stream_name()
                        .map(|value| Value::Text(value.to_string()))
                        .unwrap_or(Value::Null),
                    event
                        .message()
                        .map(|value| Value::Text(value.to_string()))
                        .unwrap_or(Value::Null),
                    event
                        .event_id()
                        .map(|value| Value::Text(value.to_string()))
                        .unwrap_or(Value::Null),
                ]);
            }

            next_token = output.next_token().map(ToOwned::to_owned);

            if rows.len() >= limit || next_token.is_none() {
                break;
            }
        }

        let columns = vec![
            ColumnMeta {
                name: "timestamp_ms".to_string(),
                type_name: "bigint".to_string(),
                kind: ColumnKind::Timestamp,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "ingestion_time_ms".to_string(),
                type_name: "bigint".to_string(),
                kind: ColumnKind::Timestamp,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "log_stream_name".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "message".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "event_id".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
        ];

        let mut result = QueryResult::table(columns, rows, None, started.elapsed());
        result.next_page_token = next_token;
        Ok(result)
    }

    fn count_collection(&self, _request: &CollectionCountRequest) -> Result<u64, DbError> {
        Err(DbError::NotSupported(
            "CloudWatch event counts are not available as a cheap collection count".to_string(),
        ))
    }

    fn browse_event_stream(
        &self,
        target: &EventStreamTarget,
        query: &EventQuery,
    ) -> Result<EventPage, DbError> {
        let request = Self::event_stream_request(target, query);
        let result = self.browse_collection(&request)?;

        Ok(Self::event_query_result_to_page(
            &target.collection,
            target.child_id.as_deref(),
            query,
            result,
        ))
    }

    fn source_context_spec(&self) -> Option<SourceContextSpec> {
        Some(SourceContextSpec {
            targets_label: "Log groups".to_string(),
            targets_placeholder: "Log groups".to_string(),
            default_target: None,
            start_label: "Start".to_string(),
            end_label: "End".to_string(),
            query_mode_label: Some("Syntax".to_string()),
            query_modes: cloudwatch_query_modes(),
            default_query_mode: Some(CLOUDWATCH_QUERY_MODE_CWLI.to_string()),
        })
    }

    fn language_service(&self) -> &dyn dbflux_core::LanguageService {
        &CLOUDWATCH_LANGUAGE_SERVICE
    }

    fn table_details(
        &self,
        _database: &str,
        _schema: Option<&str>,
        table: &str,
    ) -> Result<TableInfo, DbError> {
        Ok(TableInfo {
            name: table.to_string(),
            schema: Some(CLOUDWATCH_DEFAULT_DATABASE.to_string()),
            columns: None,
            indexes: None,
            foreign_keys: None,
            constraints: None,
            sample_fields: None,
            presentation: CollectionPresentation::EventStream,
            child_items: None,
        })
    }

    fn collection_children(
        &self,
        request: &CollectionChildrenRequest,
    ) -> Result<CollectionChildrenPage, DbError> {
        fetch_log_stream_page(
            &self.client,
            &request.collection.name,
            request.limit,
            request.page_token.as_deref(),
        )
    }

    fn kind(&self) -> DbKind {
        DbKind::CloudWatchLogs
    }

    fn schema_loading_strategy(&self) -> SchemaLoadingStrategy {
        SchemaLoadingStrategy::SingleDatabase
    }

    fn schema_features(&self) -> SchemaFeatures {
        SchemaFeatures::empty()
    }

    fn dialect(&self) -> &dyn dbflux_core::SqlDialect {
        &dbflux_core::DefaultSqlDialect
    }

    fn version_query(&self) -> &'static str {
        "SELECT 'cloudwatch'"
    }
}

impl CloudWatchConnection {
    fn event_stream_request(
        target: &EventStreamTarget,
        query: &EventQuery,
    ) -> CollectionBrowseRequest {
        let mut filter = serde_json::Map::new();
        let mut from_ts_ms = query.from_ts_ms;
        let mut to_ts_ms = query.to_ts_ms;

        if target.child_id.is_some()
            && from_ts_ms.is_none()
            && to_ts_ms.is_none()
            && let Ok(default_end) = current_time_ms()
        {
            to_ts_ms = Some(default_end);
            from_ts_ms = Some(default_end.saturating_sub(DEFAULT_BROWSE_WINDOW_MS));
        }

        if let Some(pattern) = query.free_text.as_ref().filter(|value| !value.is_empty()) {
            filter.insert(
                "filter_pattern".to_string(),
                serde_json::Value::String(pattern.clone()),
            );
        }

        if let Some(start_ms) = from_ts_ms {
            filter.insert("start_ms".to_string(), serde_json::Value::from(start_ms));
        }

        if let Some(end_ms) = to_ts_ms {
            filter.insert("end_ms".to_string(), serde_json::Value::from(end_ms));
        }

        if let Some(child_id) = target.child_id.as_ref() {
            filter.insert(
                "log_stream_names".to_string(),
                serde_json::Value::Array(vec![serde_json::Value::String(child_id.clone())]),
            );
            filter.insert("most_recent".to_string(), serde_json::Value::Bool(true));
        }

        CollectionBrowseRequest {
            collection: target.collection.clone(),
            filter: (!filter.is_empty()).then_some(serde_json::Value::Object(filter)),
            semantic_filter: None,
            pagination: dbflux_core::Pagination::Offset {
                limit: query.limit.unwrap_or(100) as u32,
                offset: query.offset.unwrap_or(0) as u64,
            },
        }
    }

    fn event_query_result_to_page(
        collection: &dbflux_core::CollectionRef,
        child_id: Option<&str>,
        query: &EventQuery,
        result: QueryResult,
    ) -> EventPage {
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(100);
        let records = result
            .rows
            .iter()
            .enumerate()
            .map(|(index, row)| Self::row_to_event_record(row, collection, child_id, offset, index))
            .collect();

        EventPage::new(
            records,
            None,
            result.next_page_token.is_some(),
            offset,
            limit,
        )
    }

    fn row_to_event_record(
        row: &[Value],
        collection: &dbflux_core::CollectionRef,
        child_id: Option<&str>,
        pagination_offset: usize,
        index: usize,
    ) -> EventRecord {
        let timestamp_ms = match row.first() {
            Some(Value::Int(value)) => *value,
            Some(Value::Text(value)) => value.parse().unwrap_or_default(),
            _ => 0,
        };
        let ingestion_time_ms = match row.get(1) {
            Some(Value::Int(value)) => Some(*value),
            Some(Value::Text(value)) => value.parse().ok(),
            _ => None,
        };
        let stream_name = row
            .get(2)
            .and_then(|value| match value {
                Value::Text(text) if !text.is_empty() => Some(text.clone()),
                Value::Int(value) => Some(value.to_string()),
                _ => None,
            })
            .or_else(|| child_id.map(ToOwned::to_owned));
        let message = row
            .get(3)
            .and_then(|value| match value {
                Value::Text(text) => Some(text.clone()),
                Value::Int(value) => Some(value.to_string()),
                _ => None,
            })
            .unwrap_or_default();
        let event_id = row.get(4).and_then(|value| match value {
            Value::Text(text) if !text.is_empty() => Some(text.clone()),
            Value::Int(value) => Some(value.to_string()),
            _ => None,
        });

        EventRecord {
            id: Some((pagination_offset + index + 1) as i64),
            ts_ms: timestamp_ms,
            level: EventSeverity::Info,
            category: EventCategory::System,
            action: stream_name
                .clone()
                .unwrap_or_else(|| collection.name.clone()),
            outcome: dbflux_core::EventOutcome::Success,
            actor_type: EventActorType::System,
            actor_id: None,
            source_id: EventSourceId::System,
            connection_id: Some(collection.name.clone()),
            database_name: Some(collection.database.clone()),
            driver_id: Some(CLOUDWATCH_METADATA.id.clone()),
            object_type: Some("event_stream".to_string()),
            object_id: event_id,
            summary: message.clone(),
            details_json: Some(build_message_details(Some(message.as_str())).to_string()),
            error_code: None,
            error_message: ingestion_time_ms.map(|value| value.to_string()),
            duration_ms: None,
            session_id: None,
            correlation_id: None,
        }
    }

    fn fetch_recent_stream_events(
        &self,
        log_group_name: &str,
        log_stream_name: &str,
        filter: &CloudWatchCollectionFilter,
        limit: usize,
        offset: usize,
        started: Instant,
    ) -> Result<QueryResult, DbError> {
        let mut next_token: Option<String> = None;
        let mut rows = Vec::new();
        let mut skipped = 0usize;

        loop {
            let mut operation = self
                .client
                .get_log_events()
                .log_group_name(log_group_name)
                .log_stream_name(log_stream_name)
                .start_from_head(false)
                .limit(limit as i32);

            if let Some(start_ms) = filter.start_ms {
                operation = operation.start_time(start_ms);
            }

            if let Some(end_ms) = filter.end_ms {
                operation = operation.end_time(end_ms);
            }

            if let Some(token) = next_token.clone() {
                operation = operation.next_token(token);
            }

            let output = runtime()?.block_on(operation.send()).map_err(|error| {
                DbError::query_failed(format!("CloudWatch GetLogEvents failed: {error}"))
            })?;

            let mut page_rows = output
                .events()
                .iter()
                .enumerate()
                .map(|(index, event)| {
                    let message = event.message().unwrap_or_default().to_string();

                    let timestamp_ms = event.timestamp().unwrap_or_default();
                    let ingestion_time_ms = event.ingestion_time();
                    let synthetic_event_id = format!(
                        "{}:{}:{}:{}",
                        log_stream_name,
                        timestamp_ms,
                        ingestion_time_ms.unwrap_or_default(),
                        index
                    );

                    vec![
                        Value::Int(timestamp_ms),
                        ingestion_time_ms.map(Value::Int).unwrap_or(Value::Null),
                        Value::Text(log_stream_name.to_string()),
                        Value::Text(message),
                        Value::Text(synthetic_event_id),
                    ]
                })
                .collect::<Vec<_>>();

            page_rows.sort_by(|left, right| {
                let left_ts = match left.first() {
                    Some(Value::Int(value)) => *value,
                    _ => 0,
                };
                let right_ts = match right.first() {
                    Some(Value::Int(value)) => *value,
                    _ => 0,
                };

                right_ts.cmp(&left_ts)
            });

            for row in page_rows {
                if skipped < offset {
                    skipped += 1;
                    continue;
                }

                if rows.len() >= limit {
                    break;
                }

                rows.push(row);
            }

            if rows.len() >= limit {
                break;
            }

            let new_token = output.next_backward_token().map(ToOwned::to_owned);
            if new_token.is_none() || new_token == next_token {
                break;
            }

            next_token = new_token;
        }

        let columns = vec![
            ColumnMeta {
                name: "timestamp_ms".to_string(),
                type_name: "bigint".to_string(),
                kind: ColumnKind::Timestamp,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "ingestion_time_ms".to_string(),
                type_name: "bigint".to_string(),
                kind: ColumnKind::Timestamp,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "log_stream_name".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "message".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
            ColumnMeta {
                name: "event_id".to_string(),
                type_name: "text".to_string(),
                kind: ColumnKind::Text,
                nullable: true,
                is_primary_key: false,
            },
        ];

        Ok(QueryResult::table(columns, rows, None, started.elapsed()))
    }
}

impl dbflux_core::LanguageService for CloudWatchLanguageService {
    fn validate(&self, _query: &str) -> ValidationResult {
        ValidationResult::Valid
    }

    fn detect_dangerous(&self, _query: &str) -> Option<dbflux_core::DangerousQueryKind> {
        None
    }
}

/// Classify a CloudWatch Insights column name to a semantic `ColumnKind`.
///
/// CloudWatch Insights uses known annotation fields (@timestamp, @ingestionTime)
/// and dynamic stats fields. This function maps the driver's own canonical field
/// names — not a generic heuristic.
fn cloudwatch_column_kind(name: &str) -> ColumnKind {
    match name {
        "@timestamp" | "@ingestionTime" => ColumnKind::Timestamp,
        "@message" | "@logStream" | "@log" => ColumnKind::Text,
        _ => {
            // stats query numeric fields: treat as Float (safest choice for chart)
            ColumnKind::Float
        }
    }
}

fn cloudwatch_query_modes() -> Vec<SourceQueryMode> {
    vec![
        SourceQueryMode {
            value: CLOUDWATCH_QUERY_MODE_CWLI.to_string(),
            label: "Logs Insights QL".to_string(),
            query_language: QueryLanguage::CloudWatchLogsInsightsQl,
        },
        SourceQueryMode {
            value: CLOUDWATCH_QUERY_MODE_PPL.to_string(),
            label: "OpenSearch PPL".to_string(),
            query_language: QueryLanguage::OpenSearchPpl,
        },
        SourceQueryMode {
            value: CLOUDWATCH_QUERY_MODE_SQL.to_string(),
            label: "OpenSearch SQL".to_string(),
            query_language: QueryLanguage::OpenSearchSql,
        },
    ]
}

fn cloudwatch_sdk_query_language(query_mode: &str) -> aws_sdk_cloudwatchlogs::types::QueryLanguage {
    match query_mode {
        CLOUDWATCH_QUERY_MODE_PPL => aws_sdk_cloudwatchlogs::types::QueryLanguage::Ppl,
        CLOUDWATCH_QUERY_MODE_SQL => aws_sdk_cloudwatchlogs::types::QueryLanguage::Sql,
        _ => aws_sdk_cloudwatchlogs::types::QueryLanguage::Cwli,
    }
}

impl CloudWatchCollectionFilter {
    fn from_json(filter: Option<&serde_json::Value>) -> Result<Self, DbError> {
        let Some(filter) = filter else {
            return Ok(Self::default());
        };

        let object = filter.as_object().ok_or_else(|| {
            DbError::query_failed("CloudWatch collection filter must be a JSON object")
        })?;

        let filter_pattern = string_field(object, &["filter_pattern", "filterPattern"]);
        let start_ms = i64_field(object, &["start_ms", "startTime", "start_time"])?;
        let end_ms = i64_field(object, &["end_ms", "endTime", "end_time"])?;
        let log_stream_name_prefix =
            string_field(object, &["log_stream_name_prefix", "logStreamNamePrefix"]);
        let log_stream_names = string_array_field(object, &["log_stream_names", "logStreamNames"])?;
        let most_recent = bool_field(object, &["most_recent", "mostRecent"])?;

        Ok(Self {
            filter_pattern,
            start_ms,
            end_ms,
            log_stream_name_prefix,
            log_stream_names,
            most_recent,
        })
    }
}

fn profile_config(config: &DbConfig) -> Result<CloudWatchProfileConfig, DbError> {
    let DbConfig::CloudWatchLogs {
        region,
        profile,
        endpoint,
    } = config
    else {
        return Err(DbError::InvalidProfile(
            "Expected CloudWatch Logs configuration".to_string(),
        ));
    };

    let region = region.trim();
    if region.is_empty() {
        return Err(DbError::InvalidProfile(
            "AWS Region is required".to_string(),
        ));
    }

    Ok(CloudWatchProfileConfig {
        region: region.to_string(),
        profile: profile
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        endpoint: endpoint
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
    })
}

/// Build both the Logs and Metrics CloudWatch clients from a shared `sdk_config`.
///
/// Both clients share the same region, credentials, profile, and optional
/// custom endpoint. Loading the SDK config once and building two clients avoids
/// a redundant async config-load.
fn build_clients(
    config: &CloudWatchProfileConfig,
) -> Result<(Client, aws_sdk_cloudwatch::Client), DbError> {
    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(config.region.clone()));

    if let Some(profile) = &config.profile {
        loader = loader.profile_name(profile);
    }

    let sdk_config = runtime()?.block_on(loader.load());

    if config.endpoint.is_none() {
        let logs_client = Client::new(&sdk_config);
        let metrics_client = aws_sdk_cloudwatch::Client::new(&sdk_config);
        return Ok((logs_client, metrics_client));
    }

    let mut logs_builder = CloudWatchConfigBuilder::from(&sdk_config);
    let mut metrics_builder = CloudWatchMetricsConfigBuilder::from(&sdk_config);

    if let Some(endpoint) = &config.endpoint {
        logs_builder = logs_builder.endpoint_url(endpoint);
        metrics_builder = metrics_builder.endpoint_url(endpoint);
    }

    let logs_client = Client::from_conf(logs_builder.build());
    let metrics_client = aws_sdk_cloudwatch::Client::from_conf(metrics_builder.build());

    Ok((logs_client, metrics_client))
}

/// Guard: reject period_s == 0 before any network I/O.
///
/// CloudWatch GetMetricData requires a positive period. A period of zero
/// would produce an AWS API error, but we catch it locally for a cleaner
/// error message and to avoid an unnecessary network round-trip.
pub(crate) fn check_period_nonzero(period_s: u32) -> Result<(), DbError> {
    if period_s == 0 {
        return Err(DbError::query_failed(
            "MetricQuery period_s must be greater than 0",
        ));
    }
    Ok(())
}

/// Execute a CloudWatch GetMetricData call and return a `QueryResult`.
///
/// Builds a single-metric `MetricDataQuery` from the provided parameters,
/// calls GetMetricData via the blocking runtime pattern used elsewhere in
/// this driver, then maps the output to a `QueryResult` via the pure mapper.
#[allow(clippy::too_many_arguments)]
fn execute_metric_query(
    client: &aws_sdk_cloudwatch::Client,
    namespace: &str,
    metric_name: &str,
    dimensions: &[(String, String)],
    period_s: u32,
    statistic: &str,
    start_ms: i64,
    end_ms: i64,
    started: Instant,
) -> Result<QueryResult, DbError> {
    check_period_nonzero(period_s)?;

    let sdk_dimensions = dimensions
        .iter()
        .map(|(name, value)| Dimension::builder().name(name).value(value).build())
        .collect::<Vec<_>>();

    let metric = Metric::builder()
        .namespace(namespace)
        .metric_name(metric_name)
        .set_dimensions(Some(sdk_dimensions))
        .build();

    let metric_stat = MetricStat::builder()
        .metric(metric)
        .period(period_s as i32)
        .stat(statistic)
        .build();

    let query = MetricDataQuery::builder()
        .id("m0")
        .metric_stat(metric_stat)
        .return_data(true)
        .build();

    let start_time = MetricsDateTime::from_millis(start_ms);
    let end_time = MetricsDateTime::from_millis(end_ms);

    let output = runtime()?
        .block_on(
            client
                .get_metric_data()
                .start_time(start_time)
                .end_time(end_time)
                .metric_data_queries(query)
                .send(),
        )
        .map_err(|error| {
            DbError::query_failed(format!("CloudWatch GetMetricData failed: {error}"))
        })?;

    let mut result = metric_data_output_to_query_result(&output, "m0");
    result.execution_time = started.elapsed();
    Ok(result)
}

/// Map a `GetMetricDataOutput` to a `QueryResult`.
///
/// For a single `MetricDataResult`: two columns (`timestamp`, `<result_id>`)
/// ordered ascending by timestamp. For multiple results: wide pivot keyed by
/// timestamp with one Float column per result id; timestamps present in some
/// but not all results yield `Value::Null` for the missing series.
///
/// Timestamps from the SDK are second-precision; this function converts them
/// to milliseconds (×1000) and stores them as `Value::Int`.
pub(crate) fn metric_data_output_to_query_result(
    output: &aws_sdk_cloudwatch::operation::get_metric_data::GetMetricDataOutput,
    metric_id: &str,
) -> QueryResult {
    let results = output.metric_data_results();

    match results {
        [] => build_empty_metric_result(metric_id),
        [single] => build_single_metric_result(single, metric_id),
        multiple => build_wide_metric_result(multiple),
    }
}

/// Return an empty `QueryResult` with the standard metric column schema.
fn build_empty_metric_result(metric_id: &str) -> QueryResult {
    let columns = vec![
        ColumnMeta {
            name: "timestamp".to_string(),
            type_name: "bigint".to_string(),
            kind: ColumnKind::Timestamp,
            nullable: false,
            is_primary_key: false,
        },
        ColumnMeta {
            name: metric_id.to_string(),
            type_name: "double".to_string(),
            kind: ColumnKind::Float,
            nullable: true,
            is_primary_key: false,
        },
    ];

    QueryResult::table(columns, vec![], None, Duration::ZERO)
}

/// Build a two-column ascending `QueryResult` from a single `MetricDataResult`.
fn build_single_metric_result(
    result: &aws_sdk_cloudwatch::types::MetricDataResult,
    metric_id: &str,
) -> QueryResult {
    let id = result.id().unwrap_or(metric_id);

    let timestamps = result.timestamps();
    let values = result.values();

    // Pair timestamps and values, then sort ascending by timestamp.
    let mut pairs: Vec<(i64, f64)> = timestamps
        .iter()
        .zip(values.iter())
        .map(|(ts, val)| (ts.secs() * 1000, *val))
        .collect();

    pairs.sort_unstable_by_key(|(ts, _)| *ts);

    let columns = vec![
        ColumnMeta {
            name: "timestamp".to_string(),
            type_name: "bigint".to_string(),
            kind: ColumnKind::Timestamp,
            nullable: false,
            is_primary_key: false,
        },
        ColumnMeta {
            name: id.to_string(),
            type_name: "double".to_string(),
            kind: ColumnKind::Float,
            nullable: true,
            is_primary_key: false,
        },
    ];

    let rows = pairs
        .into_iter()
        .map(|(ts, val)| vec![Value::Int(ts), Value::Float(val)])
        .collect();

    QueryResult::table(columns, rows, None, Duration::ZERO)
}

/// Build a wide-format `QueryResult` from multiple `MetricDataResult`s.
///
/// Pivots on timestamp: one row per unique timestamp, one Float column per
/// result id. Missing values for a given series at a given timestamp are
/// represented as `Value::Null`.
fn build_wide_metric_result(
    results: &[aws_sdk_cloudwatch::types::MetricDataResult],
) -> QueryResult {
    // Collect all unique timestamps (in seconds, sorted ascending).
    let mut all_timestamps: Vec<i64> = results
        .iter()
        .flat_map(|r| r.timestamps().iter().map(|ts| ts.secs()))
        .collect();

    all_timestamps.sort_unstable();
    all_timestamps.dedup();

    // Build an index from timestamp_s → column value for each result.
    let series: Vec<(String, HashMap<i64, f64>)> = results
        .iter()
        .map(|r| {
            let id = r.id().unwrap_or("").to_string();
            let map: HashMap<i64, f64> = r
                .timestamps()
                .iter()
                .zip(r.values().iter())
                .map(|(ts, val)| (ts.secs(), *val))
                .collect();
            (id, map)
        })
        .collect();

    let mut columns = vec![ColumnMeta {
        name: "timestamp".to_string(),
        type_name: "bigint".to_string(),
        kind: ColumnKind::Timestamp,
        nullable: false,
        is_primary_key: false,
    }];

    for (id, _) in &series {
        columns.push(ColumnMeta {
            name: id.clone(),
            type_name: "double".to_string(),
            kind: ColumnKind::Float,
            nullable: true,
            is_primary_key: false,
        });
    }

    let rows = all_timestamps
        .into_iter()
        .map(|ts_s| {
            let mut row = vec![Value::Int(ts_s * 1000)];
            for (_, map) in &series {
                let value = map
                    .get(&ts_s)
                    .copied()
                    .map(Value::Float)
                    .unwrap_or(Value::Null);
                row.push(value);
            }
            row
        })
        .collect();

    QueryResult::table(columns, rows, None, Duration::ZERO)
}

fn probe_connection(client: &Client, config: &CloudWatchProfileConfig) -> Result<(), DbError> {
    runtime()?
        .block_on(client.describe_log_groups().limit(1).send())
        .map_err(|error| {
            DbError::connection_failed(format!(
                "CloudWatch probe failed (region={}, profile={}): {} | debug={:?}",
                config.region,
                config.profile.as_deref().unwrap_or("<default>"),
                error,
                error
            ))
        })?;

    Ok(())
}

fn fetch_log_groups(client: &Client) -> Result<Vec<CollectionInfo>, DbError> {
    let mut collections = Vec::new();
    let mut next_token: Option<String> = None;

    loop {
        let mut operation = client.describe_log_groups().limit(50);
        if let Some(token) = next_token.clone() {
            operation = operation.next_token(token);
        }

        let output = runtime()?.block_on(operation.send()).map_err(|error| {
            DbError::query_failed(format!("CloudWatch DescribeLogGroups failed: {error}"))
        })?;

        for group in output.log_groups() {
            if let Some(name) = group.log_group_name() {
                collections.push(CollectionInfo {
                    name: name.to_string(),
                    database: Some(CLOUDWATCH_DEFAULT_DATABASE.to_string()),
                    document_count: None,
                    avg_document_size: None,
                    sample_fields: None,
                    indexes: None,
                    validator: None,
                    is_capped: false,
                    presentation: CollectionPresentation::EventStream,
                    child_items: None,
                });
            }
        }

        next_token = output.next_token().map(ToOwned::to_owned);
        if next_token.is_none() {
            break;
        }
    }

    collections.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(collections)
}

fn current_time_ms() -> Result<i64, DbError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| DbError::query_failed(format!("System clock error: {error}")))?;

    i64::try_from(duration.as_millis())
        .map_err(|_| DbError::query_failed("Current time does not fit in i64".to_string()))
}

fn runtime() -> Result<tokio::runtime::Runtime, DbError> {
    tokio::runtime::Runtime::new()
        .map_err(|error| DbError::connection_failed(format!("Tokio runtime setup failed: {error}")))
}

fn build_message_details(message: Option<&str>) -> serde_json::Value {
    message
        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
        .unwrap_or_else(|| serde_json::json!(message))
}

fn string_field(
    object: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<String> {
    keys.iter()
        .find_map(|key| object.get(*key))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn i64_field(
    object: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Result<Option<i64>, DbError> {
    let Some(value) = keys.iter().find_map(|key| object.get(*key)) else {
        return Ok(None);
    };

    value.as_i64().map(Some).ok_or_else(|| {
        DbError::query_failed(format!(
            "CloudWatch collection filter field '{}' must be an integer",
            keys.first().copied().unwrap_or("?")
        ))
    })
}

fn string_array_field(
    object: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Result<Option<Vec<String>>, DbError> {
    let Some(value) = keys.iter().find_map(|key| object.get(*key)) else {
        return Ok(None);
    };

    let array = value.as_array().ok_or_else(|| {
        DbError::query_failed(format!(
            "CloudWatch collection filter field '{}' must be an array of strings",
            keys.first().copied().unwrap_or("?")
        ))
    })?;

    let mut values = Vec::with_capacity(array.len());
    for item in array {
        let item = item.as_str().ok_or_else(|| {
            DbError::query_failed(format!(
                "CloudWatch collection filter field '{}' must contain only strings",
                keys.first().copied().unwrap_or("?")
            ))
        })?;

        let trimmed = item.trim();
        if !trimmed.is_empty() {
            values.push(trimmed.to_string());
        }
    }

    Ok((!values.is_empty()).then_some(values))
}

fn bool_field(
    object: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Result<bool, DbError> {
    let Some(value) = keys.iter().find_map(|key| object.get(*key)) else {
        return Ok(false);
    };

    value.as_bool().ok_or_else(|| {
        DbError::query_failed(format!(
            "CloudWatch collection filter field '{}' must be a boolean",
            keys.first().copied().unwrap_or("?")
        ))
    })
}

fn fetch_log_stream_page(
    client: &Client,
    log_group_name: &str,
    limit: u32,
    page_token: Option<&str>,
) -> Result<CollectionChildrenPage, DbError> {
    let mut streams = Vec::new();
    let limit = limit.clamp(1, 50) as i32;

    let mut operation = client
        .describe_log_streams()
        .log_group_name(log_group_name)
        .order_by(aws_sdk_cloudwatchlogs::types::OrderBy::LastEventTime)
        .descending(true)
        .limit(limit);

    if let Some(token) = page_token {
        operation = operation.next_token(token.to_string());
    }

    let output = runtime()?.block_on(operation.send()).map_err(|error| {
        DbError::query_failed(format!("CloudWatch DescribeLogStreams failed: {error}"))
    })?;

    for stream in output.log_streams() {
        if let Some(stream_name) = stream.log_stream_name() {
            streams.push(CollectionChildInfo {
                id: stream_name.to_string(),
                label: stream_name.to_string(),
                last_event_ts_ms: stream.last_event_timestamp(),
                presentation: CollectionPresentation::EventStream,
            });
        }
    }

    Ok(CollectionChildrenPage {
        items: streams,
        next_page_token: output.next_token().map(ToOwned::to_owned),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CLOUDWATCH_METADATA, CloudWatchCollectionFilter, CloudWatchDriver,
        metric_data_output_to_query_result,
    };
    use aws_sdk_cloudwatch::operation::get_metric_data::GetMetricDataOutput;
    use aws_sdk_cloudwatch::primitives::DateTime;
    use aws_sdk_cloudwatch::types::MetricDataResult;
    use dbflux_core::{ColumnKind, DbConfig, DbDriver, DriverCapabilities, Value};

    #[test]
    fn cloudwatch_driver_uses_builtin_form_and_key() {
        let driver = CloudWatchDriver::new();

        assert_eq!(driver.driver_key(), "builtin:cloudwatch");
        assert!(!driver.requires_password());
        assert_eq!(driver.form_definition().main_tab().unwrap().label, "Main");
    }

    #[test]
    fn cloudwatch_collection_filter_accepts_supported_fields() {
        let filter = serde_json::json!({
            "filter_pattern": "ERROR",
            "start_ms": 10,
            "end_ms": 20,
            "log_stream_names": ["stream-a", "stream-b"],
            "most_recent": true
        });

        let parsed = CloudWatchCollectionFilter::from_json(Some(&filter)).expect("parse filter");

        assert_eq!(parsed.filter_pattern.as_deref(), Some("ERROR"));
        assert_eq!(parsed.start_ms, Some(10));
        assert_eq!(parsed.end_ms, Some(20));
        assert_eq!(
            parsed.log_stream_names,
            Some(vec!["stream-a".to_string(), "stream-b".to_string()])
        );
        assert!(parsed.most_recent);
    }

    #[test]
    fn cloudwatch_default_config_has_logs_database_kind() {
        assert!(matches!(
            DbConfig::default_cloudwatch_logs(),
            DbConfig::CloudWatchLogs { .. }
        ));
    }

    // T-6: single MetricDataResult → two-column ascending QueryResult
    // Verifies: column names (timestamp / result id), ColumnKind assignments,
    // row ordering (ascending), and second→ms conversion (×1000).
    #[test]
    fn get_metric_data_output_single_metric() {
        let t1 = DateTime::from_secs(1000);
        let t2 = DateTime::from_secs(2000);
        let t3 = DateTime::from_secs(3000);

        // AWS returns data descending — supply in descending order to exercise the sort.
        let result = MetricDataResult::builder()
            .id("m0")
            .set_timestamps(Some(vec![t3, t2, t1]))
            .set_values(Some(vec![3.0_f64, 2.0_f64, 1.0_f64]))
            .build();

        let output = GetMetricDataOutput::builder()
            .metric_data_results(result)
            .build();

        let qr = metric_data_output_to_query_result(&output, "m0");

        assert_eq!(qr.columns.len(), 2);
        assert_eq!(qr.columns[0].name, "timestamp");
        assert_eq!(qr.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(qr.columns[1].name, "m0");
        assert_eq!(qr.columns[1].kind, ColumnKind::Float);

        assert_eq!(qr.rows.len(), 3);

        // Row 0 corresponds to t1 (ascending order after sort)
        assert_eq!(qr.rows[0][0], Value::Int(1000 * 1000));
        assert_eq!(qr.rows[0][1], Value::Float(1.0));

        assert_eq!(qr.rows[1][0], Value::Int(2000 * 1000));
        assert_eq!(qr.rows[1][1], Value::Float(2.0));

        assert_eq!(qr.rows[2][0], Value::Int(3000 * 1000));
        assert_eq!(qr.rows[2][1], Value::Float(3.0));
    }

    // T-7: empty GetMetricDataOutput → empty rows, no panic
    #[test]
    fn get_metric_data_output_empty() {
        let output = GetMetricDataOutput::builder().build();
        let qr = metric_data_output_to_query_result(&output, "m0");

        assert_eq!(qr.rows.len(), 0, "expected zero rows for empty output");
    }

    // T-8: period_s == 0 must return Err, never panic.
    //
    // The guard fires before any network I/O, so this test is credential-free.
    // We exercise it by calling `check_period` (the extracted guard fn) directly.
    #[test]
    fn execute_metric_query_period_zero_errors() {
        use super::check_period_nonzero;

        let result = check_period_nonzero(0);
        assert!(result.is_err(), "period_s == 0 must return Err");

        let ok = check_period_nonzero(60);
        assert!(ok.is_ok(), "period_s == 60 must be Ok");
    }

    // T-9: live integration test — requires real AWS credentials
    #[test]
    #[ignore]
    fn live_execute_cloudwatch_metric() {
        use dbflux_core::{
            ColumnKind, DbConfig, DbDriver, ExecutionContext, ExecutionSourceContext, QueryRequest,
        };

        // Requires: AWS credentials in environment / ~/.aws, region set,
        // and a metric that has data in the given window.
        let profile = dbflux_core::ConnectionProfile::new(
            "test",
            DbConfig::CloudWatchLogs {
                region: std::env::var("AWS_DEFAULT_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                profile: std::env::var("AWS_PROFILE").ok(),
                endpoint: None,
            },
        );

        let driver = CloudWatchDriver::new();
        let conn = driver
            .connect_with_secrets(&profile, None, None)
            .expect("connection failed — check AWS credentials");

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let req = QueryRequest::new(String::new()).with_execution_context(Some(ExecutionContext {
            source: Some(ExecutionSourceContext::MetricQuery {
                namespace: "AWS/Lambda".to_string(),
                metric_name: "Invocations".to_string(),
                dimensions: vec![],
                period_s: 300,
                statistic: "Sum".to_string(),
                start_ms: now_ms - 24 * 3600 * 1000,
                end_ms: now_ms,
            }),
            ..ExecutionContext::default()
        }));

        let result = conn.execute(&req).expect("execute failed");

        assert!(result.columns.len() >= 2);
        assert_eq!(result.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(result.columns[1].kind, ColumnKind::Float);

        // Rows may be zero if no invocations in the window — that is valid.
        // Just assert no panic and correct column shape.
        for row in &result.rows {
            assert_eq!(row.len(), result.columns.len());
        }
    }

    // T-5: CLOUDWATCH_METADATA must advertise METRIC_SERIES.
    //
    // This test is RED until TASK-3.2 adds the flag to CLOUDWATCH_METADATA.
    #[test]
    fn cloudwatch_metadata_has_metric_series() {
        assert!(
            CLOUDWATCH_METADATA
                .capabilities
                .contains(DriverCapabilities::METRIC_SERIES),
            "CLOUDWATCH_METADATA must advertise METRIC_SERIES capability"
        );
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_cloudwatch::config::Builder as CloudWatchMetricsConfigBuilder;
use aws_sdk_cloudwatch::error::ProvideErrorMetadata as CloudWatchProvideErrorMetadata;
use aws_sdk_cloudwatch::primitives::DateTime as MetricsDateTime;
use aws_sdk_cloudwatch::types::{Dimension, Metric, MetricDataQuery, MetricStat};
use aws_sdk_cloudwatchlogs::Client;
use aws_sdk_cloudwatchlogs::config::Builder as CloudWatchConfigBuilder;
use aws_sdk_cloudwatchlogs::error::ProvideErrorMetadata as CloudWatchLogsProvideErrorMetadata;
use dbflux_core::secrecy::SecretString;
use dbflux_core::{
    CollectionBrowseRequest, CollectionChildInfo, CollectionChildrenPage,
    CollectionChildrenRequest, CollectionCountRequest, CollectionInfo, CollectionPresentation,
    ColumnKind, ColumnMeta, Connection, ConnectionProfile, DatabaseCategory, DatabaseInfo,
    DbConfig, DbDriver, DbError, DbKind, DeploymentClass, DocumentSchema, DriverCapabilities,
    DriverFormDef, DriverMetadata, EventActorType, EventCategory, EventPage, EventQuery,
    EventRecord, EventSeverity, EventSourceId, EventStreamTarget, ExecutionSourceContext,
    FormFieldKind, FormSection, FormTab, FormValues, FormattedError, Icon, MetricCatalog,
    MetricQuerySeries, QueryLanguage, QueryRequest, QueryResult, SchemaFeatures,
    SchemaLoadingStrategy, SchemaSnapshot, SourceContextSpec, SourceQueryMode, TableInfo,
    ValidationResult, Value, field, field_required,
};

use crate::dashboard_import::CloudWatchDashboardImporter;
use crate::dashboard_source::{CloudWatchDashboardSource, RealCloudWatchDashboardApi};
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
        .union(DriverCapabilities::METRIC_CATALOG)
        .union(DriverCapabilities::DASHBOARD_IMPORT)
        .union(DriverCapabilities::DASHBOARD_SYNC)
        .union(DriverCapabilities::CHART_AUTHORING),
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
    default_chunk_size: None,
    supports_lock_timeout: false,
    editor_profile: None,
});

pub static CLOUDWATCH_FORM: LazyLock<DriverFormDef> = LazyLock::new(|| DriverFormDef {
    tabs: vec![FormTab {
        id: "main".into(),
        label: "Main".into(),
        sections: vec![FormSection {
            title: "AWS".into(),
            fields: vec![
                field_required("region", "Region", FormFieldKind::Text, "us-east-1"),
                field(
                    "profile",
                    "Profile",
                    FormFieldKind::AuthProfileRef { provider_id: None },
                    "",
                ),
                field(
                    "endpoint",
                    "Endpoint Override",
                    FormFieldKind::Text,
                    "http://localhost:4566",
                ),
            ],
        }],
    }],
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
pub(crate) struct CloudWatchProfileConfig {
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
    /// Dashboard JSON importer — always present; returns `Some` from `dashboard_importer()`.
    dashboard_importer_impl: CloudWatchDashboardImporter,
    /// Dashboard source — always present; returns `Some` from `dashboard_source()`.
    dashboard_source_impl: CloudWatchDashboardSource,
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
            RealCloudWatchClient::new(metrics_client.clone()),
        ));

        let dashboard_source_impl = CloudWatchDashboardSource::new(Box::new(
            RealCloudWatchDashboardApi::new(metrics_client.clone()),
        ));

        Ok(Box::new(CloudWatchConnection {
            client,
            metrics_client,
            config,
            metric_catalog_impl,
            dashboard_importer_impl: CloudWatchDashboardImporter,
            dashboard_source_impl,
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

    fn dashboard_importer(&self) -> Option<&dyn dbflux_core::DashboardImporter> {
        Some(&self.dashboard_importer_impl)
    }

    fn dashboard_source(&self) -> Option<&dyn dbflux_core::DashboardSource> {
        Some(&self.dashboard_source_impl)
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
                series,
                start_ms,
                end_ms,
            } => {
                return execute_metric_query(
                    &self.metrics_client,
                    series,
                    *start_ms,
                    *end_ms,
                    started,
                    Some(&self.config),
                );
            }
            ExecutionSourceContext::InstanceMetricQuery { metric_id, .. } => {
                return Err(DbError::NotSupported(format!(
                    "CloudWatch does not support instance metrics (metric_id: {metric_id})"
                )));
            }
            ExecutionSourceContext::InstanceInspectorQuery { metric_id } => {
                return Err(DbError::NotSupported(format!(
                    "CloudWatch does not support instance inspector (metric_id: {metric_id})"
                )));
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

        let start_output = runtime()
            .block_on(start_request.send())
            .map_err(|error| from_logs_err(&error, Some(&self.config)).into_query_error())?;

        let query_id = start_output
            .query_id()
            .map(ToOwned::to_owned)
            .ok_or_else(|| DbError::query_failed("CloudWatch StartQuery returned no query id"))?;

        let mut attempts = 0;
        loop {
            attempts += 1;

            let output = runtime()
                .block_on(
                    self.client
                        .get_query_results()
                        .query_id(query_id.clone())
                        .send(),
                )
                .map_err(|error| from_logs_err(&error, Some(&self.config)).into_query_error())?;

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
                                .map(|raw| cwli_field_value(&field_name, raw))
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
                            kind: cwli_column_kind(name, &row_maps),
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

            let output = runtime()
                .block_on(operation.send())
                .map_err(|error| from_logs_err(&error, Some(&self.config)).into_query_error())?;

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

            let output = runtime()
                .block_on(operation.send())
                .map_err(|error| from_logs_err(&error, Some(&self.config)).into_query_error())?;

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

/// Classify a CloudWatch Insights column name to a semantic `ColumnKind` using
/// name-based rules only. Known annotation fields are authoritative: timestamp
/// fields map to Timestamp, message/stream/log fields map to Text. All other
/// names return Unknown; callers that have row data should use
/// `cwli_column_kind` instead.
fn cloudwatch_column_kind(name: &str) -> ColumnKind {
    match name {
        "@timestamp" | "@ingestionTime" => ColumnKind::Timestamp,
        "@message" | "@logStream" | "@log" => ColumnKind::Text,
        _ => ColumnKind::Unknown,
    }
}

/// Produce a typed `Value` for a CWLI result field.
///
/// `@timestamp` and `@ingestionTime` arrive from CWLI in the format
/// `"YYYY-MM-DD HH:MM:SS.mmm"` (space separator, no timezone, UTC). They are
/// normalised to RFC3339 (`"...T...+00:00"`) so the chart engine can parse
/// them as time-axis values. On parse failure the raw string is preserved
/// unchanged.
///
/// `@message`, `@logStream`, and `@log` stay as `Value::Text`. For all other
/// fields the raw string is parsed: integer strings become `Value::Int`,
/// finite floating-point strings become `Value::Float`, and everything else
/// stays `Value::Text`.
fn cwli_field_value(name: &str, raw: &str) -> Value {
    use dbflux_core::chrono::{NaiveDateTime, TimeZone, Utc};

    match name {
        "@timestamp" | "@ingestionTime" => {
            let parsed = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.3f")
                .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"));
            match parsed {
                Ok(naive) => Value::Text(Utc.from_utc_datetime(&naive).to_rfc3339()),
                Err(_) => Value::Text(raw.to_string()),
            }
        }
        "@message" | "@logStream" | "@log" => Value::Text(raw.to_string()),
        _ => {
            if let Ok(i) = raw.parse::<i64>() {
                return Value::Int(i);
            }
            if let Ok(f) = raw.parse::<f64>()
                && f.is_finite()
            {
                return Value::Float(f);
            }
            Value::Text(raw.to_string())
        }
    }
}

/// Determine the `ColumnKind` for a CWLI result column using name-based rules
/// first, falling back to value sampling across `row_maps` for unknown names.
///
/// Because `cwli_field_value` already emits typed values, this function
/// scans rows for the first `Value::Int` (→ Integer) or `Value::Float` (→ Float),
/// skipping `Null` and `Text` samples. Returns `Unknown` only when no numeric
/// value is found across all rows.
fn cwli_column_kind(name: &str, row_maps: &[HashMap<String, Value>]) -> ColumnKind {
    let name_kind = cloudwatch_column_kind(name);
    if name_kind != ColumnKind::Unknown {
        return name_kind;
    }

    for row in row_maps {
        match row.get(name) {
            Some(Value::Int(_)) => return ColumnKind::Integer,
            Some(Value::Float(_)) => return ColumnKind::Float,
            _ => continue,
        }
    }

    ColumnKind::Unknown
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

    let sdk_config = runtime().block_on(loader.load());

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

/// Execute a CloudWatch GetMetricData call batching every requested series
/// and return a `QueryResult` with one timestamp column plus one numeric
/// column per series.
///
/// All series share the supplied time window. The resulting columns are
/// labelled (in order): the series' explicit `label`, the series'
/// `metric_name` when no label is set and metric names are unique, or
/// `metric_name (dim1=value1, ...)` otherwise. Every column name is made
/// unique by suffixing `#<index>` when a collision would otherwise occur.
fn execute_metric_query(
    client: &aws_sdk_cloudwatch::Client,
    series: &[MetricQuerySeries],
    start_ms: i64,
    end_ms: i64,
    started: Instant,
    config: Option<&CloudWatchProfileConfig>,
) -> Result<QueryResult, DbError> {
    if series.is_empty() {
        return Err(DbError::query_failed(
            "MetricQuery must carry at least one series",
        ));
    }

    for s in series {
        check_period_nonzero(s.period_s)?;
    }

    // GetMetricData identifies each query by an id matching `^[a-z][a-zA-Z0-9_]*$`
    // and at most 255 chars. We use sequential `mN` ids that map 1:1 onto the
    // series order so the response can be re-aligned by id below.
    let mut queries = Vec::with_capacity(series.len());

    for (i, s) in series.iter().enumerate() {
        let sdk_dimensions = s
            .dimensions
            .iter()
            .map(|(name, value)| Dimension::builder().name(name).value(value).build())
            .collect::<Vec<_>>();

        let metric = Metric::builder()
            .namespace(&s.namespace)
            .metric_name(&s.metric_name)
            .set_dimensions(Some(sdk_dimensions))
            .build();

        let metric_stat = MetricStat::builder()
            .metric(metric)
            .period(s.period_s as i32)
            .stat(&s.statistic)
            .build();

        queries.push(
            MetricDataQuery::builder()
                .id(format!("m{i}"))
                .metric_stat(metric_stat)
                .return_data(true)
                .build(),
        );
    }

    let start_time = MetricsDateTime::from_millis(start_ms);
    let end_time = MetricsDateTime::from_millis(end_ms);

    let output = runtime()
        .block_on(
            client
                .get_metric_data()
                .start_time(start_time)
                .end_time(end_time)
                .set_metric_data_queries(Some(queries))
                .send(),
        )
        .map_err(|error| from_metrics_err(&error, config).into_query_error())?;

    let mut result = metric_data_output_to_multi_series_result(&output, series);
    result.execution_time = started.elapsed();
    Ok(result)
}

/// Build a wide-format `QueryResult` from a multi-series GetMetricData response.
///
/// One column per series in the same order as the request; the column name is
/// chosen from `series[i].label` (when set), else the series' `metric_name`,
/// with disambiguation suffixes appended when two series would otherwise
/// produce the same column name.
fn metric_data_output_to_multi_series_result(
    output: &aws_sdk_cloudwatch::operation::get_metric_data::GetMetricDataOutput,
    series: &[MetricQuerySeries],
) -> QueryResult {
    let results = output.metric_data_results();

    // Build per-series timestamp_s -> value maps, indexed by the response id
    // (e.g. "m0", "m1"). Series that returned no data still produce an empty
    // column so the output column count matches `series.len()` exactly.
    let mut series_maps: Vec<HashMap<i64, f64>> =
        (0..series.len()).map(|_| HashMap::new()).collect();

    for result in results {
        let id = result.id().unwrap_or("");
        let Some(idx) = id
            .strip_prefix('m')
            .and_then(|n| n.parse::<usize>().ok())
            .filter(|n| *n < series.len())
        else {
            continue;
        };

        let Some(map) = series_maps.get_mut(idx) else {
            continue;
        };
        for (ts, val) in result.timestamps().iter().zip(result.values().iter()) {
            map.insert(ts.secs(), *val);
        }
    }

    let mut all_timestamps: Vec<i64> = series_maps.iter().flat_map(|m| m.keys().copied()).collect();
    all_timestamps.sort_unstable();
    all_timestamps.dedup();

    let column_names = unique_series_column_names(series);

    let mut columns = vec![ColumnMeta {
        name: "timestamp".to_string(),
        type_name: "bigint".to_string(),
        kind: ColumnKind::Timestamp,
        nullable: false,
        is_primary_key: false,
    }];

    for name in &column_names {
        columns.push(ColumnMeta {
            name: name.clone(),
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
            for map in &series_maps {
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

/// Pick a unique display label for every series so column names never collide.
fn unique_series_column_names(series: &[MetricQuerySeries]) -> Vec<String> {
    let mut base: Vec<String> = series
        .iter()
        .map(|s| match s.label.as_ref() {
            Some(l) if !l.is_empty() => l.clone(),
            _ => s.metric_name.clone(),
        })
        .collect();

    // Detect any group of names that collide and disambiguate every member of
    // the group by appending its first non-empty dimension value. Doing the
    // detection over the ORIGINAL names (`originals`) avoids the bug where
    // mutating an earlier collider leaves later siblings looking unique
    // against the already-renamed value.
    let originals = base.clone();
    for (i, name_slot) in base.iter_mut().enumerate() {
        let Some(original) = originals.get(i) else {
            continue;
        };
        let collides_in_originals = originals
            .iter()
            .enumerate()
            .any(|(j, name)| j != i && name == original);

        if collides_in_originals
            && let Some(s) = series.get(i)
            && let Some((_, dim_val)) = s.dimensions.iter().find(|(_, v)| !v.is_empty())
        {
            *name_slot = format!("{original} ({dim_val})");
        }
    }

    // Final pass: any remaining duplicate gets a `#N` suffix matching its index.
    let mut seen = HashMap::<String, usize>::new();
    for name in base.iter_mut() {
        let count = *seen.entry(name.clone()).or_insert(0);
        if count > 0 {
            *name = format!("{name}#{count}");
        }
        *seen.entry(name.clone()).or_insert(0) += 1;
    }

    base
}

// ---------------------------------------------------------------------------
// CloudWatch error formatter
// ---------------------------------------------------------------------------

/// Classify a raw AWS error code + message into a structured `FormattedError`.
///
/// This is the single place that knows about CloudWatch / CloudWatch Logs error
/// codes and maps them to user-facing hints and retriable flags.  Both SDK
/// error families (`aws_sdk_cloudwatchlogs` and `aws_sdk_cloudwatch`) reduce
/// their typed errors to `(code, message)` before arriving here, so the
/// credential/throttle/syntax logic is never duplicated between the two.
///
/// `config` is optional so sites that lack a `CloudWatchProfileConfig` in
/// scope (e.g. `RealCloudWatchClient::list_metrics`) can still produce
/// structured errors without threading config state through the seam.
pub(crate) fn classify_cw(
    code: Option<&str>,
    message: &str,
    config: Option<&CloudWatchProfileConfig>,
) -> FormattedError {
    let mut formatted = FormattedError::new(message.to_string());

    if let Some(code_value) = code {
        formatted = formatted.with_code(code_value.to_string());
    }

    let (hint, retriable, override_message): (Option<&str>, bool, Option<&str>) = match code {
        Some(
            "ExpiredTokenException"
            | "UnrecognizedClientException"
            | "InvalidSignatureException"
            | "IncompleteSignatureException"
            | "MissingAuthenticationToken",
        ) => (
            Some("Re-authenticate or refresh your AWS SSO / credential session and retry."),
            false,
            Some("AWS session expired — please re-login"),
        ),
        Some("AccessDeniedException") => (
            Some(
                "Check IAM permissions for the requested CloudWatch/CloudWatchLogs action in the selected region.",
            ),
            false,
            None,
        ),
        Some("ResourceNotFoundException") => (
            Some("Verify the log group, metric, or resource name and the AWS region."),
            false,
            None,
        ),
        Some("ThrottlingException" | "ServiceUnavailableException") => (
            Some("Request was throttled. Retry with exponential back-off or reduce request rate."),
            true,
            None,
        ),
        Some("InvalidParameterException" | "MalformedQueryException") => (
            Some(
                "Check the query syntax and parameter values. Refer to the CloudWatch Logs Insights query syntax documentation.",
            ),
            false,
            None,
        ),
        None => {
            let lower = message.to_lowercase();
            if lower.contains("expired") || lower.contains("token") || lower.contains("credential")
            {
                (
                    Some("Re-authenticate or refresh your AWS SSO / credential session and retry."),
                    false,
                    Some("AWS session expired — please re-login"),
                )
            } else if lower.contains("throttl") {
                (
                    Some(
                        "Request was throttled. Retry with exponential back-off or reduce request rate.",
                    ),
                    true,
                    None,
                )
            } else {
                (None, false, None)
            }
        }
        _ => (None, false, None),
    };

    if let Some(msg) = override_message {
        formatted = FormattedError::new(msg.to_string());
        if let Some(code_value) = code {
            formatted = formatted.with_code(code_value.to_string());
        }
    }

    if let Some(hint_value) = hint {
        formatted = formatted.with_hint(hint_value);
    }

    if retriable {
        formatted = formatted.with_retriable(true);
    }

    if let Some(cfg) = config {
        let detail = match &cfg.endpoint {
            Some(ep) => format!("region={}, endpoint_override={}", cfg.region, ep),
            None => match &cfg.profile {
                Some(p) => format!("region={}, profile={}", cfg.region, p),
                None => format!("region={}", cfg.region),
            },
        };
        formatted = formatted.with_detail(detail);
    }

    formatted
}

/// Walk the `std::error::Error::source` chain of a transport-level SDK error
/// and join every link's `Display` into one string.
///
/// `SdkError::to_string()` is terse (e.g. "dispatch failure"), which drops the
/// root cause for DNS / TLS / connection failures. Walking the source chain
/// surfaces the underlying message ("dns error: failed to lookup …", "tcp
/// connect error", certificate failures) so transport faults stay diagnosable.
fn transport_error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    const MAX_SOURCE_DEPTH: usize = 16;

    let mut parts = vec![error.to_string()];
    let mut source = error.source();

    let mut depth = 0;
    while let Some(cause) = source {
        if depth >= MAX_SOURCE_DEPTH {
            break;
        }
        parts.push(cause.to_string());
        source = cause.source();
        depth += 1;
    }

    parts.join(": ")
}

/// Augment a transport-error `FormattedError` with the SDK error's debug
/// representation without clobbering the config-derived detail set by
/// `classify_cw`. The AWS SDK debug output does not contain secrets.
fn with_transport_debug(mut formatted: FormattedError, debug: String) -> FormattedError {
    let detail = match formatted.detail.take() {
        Some(existing) => format!("{existing}; debug={debug}"),
        None => format!("debug={debug}"),
    };
    formatted.with_detail(detail)
}

/// Convert a `aws_sdk_cloudwatchlogs::error::SdkError<E>` into a `FormattedError`
/// by extracting the service error code and message via `ProvideErrorMetadata`,
/// then routing both through the shared `classify_cw` classifier.
fn from_logs_err<E>(
    error: &aws_sdk_cloudwatchlogs::error::SdkError<E>,
    config: Option<&CloudWatchProfileConfig>,
) -> FormattedError
where
    E: CloudWatchLogsProvideErrorMetadata + std::error::Error + std::fmt::Debug + 'static,
{
    if let Some(svc) = error.as_service_error() {
        classify_cw(
            svc.code(),
            svc.message().unwrap_or("CloudWatch Logs service error"),
            config,
        )
    } else {
        let formatted = classify_cw(None, &transport_error_chain(error), config);
        with_transport_debug(formatted, format!("{error:?}"))
    }
}

/// Convert a `aws_sdk_cloudwatch::error::SdkError<E>` into a `FormattedError`
/// by extracting the service error code and message via `ProvideErrorMetadata`,
/// then routing both through the shared `classify_cw` classifier.
pub(crate) fn from_metrics_err<E>(
    error: &aws_sdk_cloudwatch::error::SdkError<E>,
    config: Option<&CloudWatchProfileConfig>,
) -> FormattedError
where
    E: CloudWatchProvideErrorMetadata + std::error::Error + std::fmt::Debug + 'static,
{
    if let Some(svc) = error.as_service_error() {
        classify_cw(
            svc.code(),
            svc.message().unwrap_or("CloudWatch service error"),
            config,
        )
    } else {
        let formatted = classify_cw(None, &transport_error_chain(error), config);
        with_transport_debug(formatted, format!("{error:?}"))
    }
}

// ---------------------------------------------------------------------------

fn probe_connection(client: &Client, config: &CloudWatchProfileConfig) -> Result<(), DbError> {
    runtime()
        .block_on(client.describe_log_groups().limit(1).send())
        .map_err(|error| from_logs_err(&error, Some(config)).into_connection_error())?;

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

        let output = runtime()
            .block_on(operation.send())
            .map_err(|error| from_logs_err(&error, None).into_query_error())?;

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

/// Process-wide tokio runtime shared across every CloudWatch SDK call.
///
/// Building a fresh runtime per call (the previous behavior) was expensive in
/// file descriptors and broke connection pooling — hyper's pool is keyed to the
/// runtime that issued the request, so per-call runtimes defeated keep-alive.
/// A `LazyLock<Runtime>` lives for the lifetime of the process and is never
/// dropped, which also sidesteps any Runtime-in-async-context panic risk.
///
/// The runtime is shared with `RealCloudWatchClient::list_metrics` via the
/// public `runtime()` accessor so SDK clients built here and exercised in
/// metric_catalog.rs share one reactor.
#[allow(clippy::expect_used)]
pub(crate) static CLOUDWATCH_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    // Fatal at process scope: the driver cannot operate without a tokio
    // runtime, and there is no recoverable path. A panic here surfaces the
    // OS-level reason (typically EMFILE / out-of-memory).
    tokio::runtime::Runtime::new().expect("CloudWatch driver failed to construct tokio runtime")
});

/// Accessor for the shared runtime. Returns `&'static Runtime` so callers
/// chain `.block_on(...)` without an intermediate `?`.
pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    &CLOUDWATCH_RUNTIME
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

    let output = runtime()
        .block_on(operation.send())
        .map_err(|error| from_logs_err(&error, None).into_query_error())?;

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
        CLOUDWATCH_FORM, CLOUDWATCH_METADATA, CloudWatchCollectionFilter, CloudWatchDriver,
        CloudWatchProfileConfig, classify_cw, cloudwatch_column_kind, cwli_column_kind,
        cwli_field_value, metric_data_output_to_multi_series_result,
    };
    use aws_sdk_cloudwatch::operation::get_metric_data::GetMetricDataOutput;
    use aws_sdk_cloudwatch::primitives::DateTime;
    use aws_sdk_cloudwatch::types::MetricDataResult;
    use dbflux_core::{
        ColumnKind, DbConfig, DbDriver, DriverCapabilities, FormFieldKind, FormValues,
        MetricQuerySeries, Value,
    };
    use std::collections::HashMap;

    #[test]
    fn cloudwatch_column_kind_name_based() {
        assert_eq!(cloudwatch_column_kind("@timestamp"), ColumnKind::Timestamp);
        assert_eq!(
            cloudwatch_column_kind("@ingestionTime"),
            ColumnKind::Timestamp
        );
        assert_eq!(cloudwatch_column_kind("@message"), ColumnKind::Text);
        assert_eq!(cloudwatch_column_kind("@logStream"), ColumnKind::Text);
        assert_eq!(cloudwatch_column_kind("@log"), ColumnKind::Text);
        assert_eq!(cloudwatch_column_kind("avg_latency"), ColumnKind::Unknown);
        assert_eq!(cloudwatch_column_kind("p99"), ColumnKind::Unknown);
        assert_eq!(cloudwatch_column_kind("count"), ColumnKind::Unknown);
    }

    fn typed_row_maps_for(field: &str, typed_values: &[Value]) -> Vec<HashMap<String, Value>> {
        typed_values
            .iter()
            .map(|v| {
                let mut m = HashMap::new();
                m.insert(field.to_string(), v.clone());
                m
            })
            .collect()
    }

    #[test]
    fn cwli_field_value_typed_output() {
        assert_eq!(cwli_field_value("count", "123"), Value::Int(123));
        assert_eq!(cwli_field_value("latency", "1.5"), Value::Float(1.5));
        assert_eq!(
            cwli_field_value("errorCode", "BadRequest"),
            Value::Text("BadRequest".to_string())
        );

        // Annotation names always stay Text regardless of numeric content.
        assert_eq!(
            cwli_field_value("@timestamp", "1234567890"),
            Value::Text("1234567890".to_string())
        );
        assert_eq!(
            cwli_field_value("@message", "42"),
            Value::Text("42".to_string())
        );

        // Non-finite floats must not produce Value::Float.
        assert_eq!(
            cwli_field_value("ratio", "NaN"),
            Value::Text("NaN".to_string())
        );
        assert_eq!(
            cwli_field_value("ratio", "inf"),
            Value::Text("inf".to_string())
        );
        assert_eq!(
            cwli_field_value("ratio", "-inf"),
            Value::Text("-inf".to_string())
        );
    }

    #[test]
    fn cwli_field_value_timestamp_to_rfc3339() {
        use dbflux_core::chrono::DateTime;

        // CWLI format with milliseconds → must round-trip through parse_from_rfc3339.
        let v = cwli_field_value("@timestamp", "2023-01-15 12:34:56.789");
        match &v {
            Value::Text(s) => {
                DateTime::parse_from_rfc3339(s)
                    .unwrap_or_else(|e| panic!("RFC3339 parse failed: {e} — got: {s}"));
            }
            other => panic!("expected Value::Text, got {other:?}"),
        }

        // Same for @ingestionTime.
        let v = cwli_field_value("@ingestionTime", "2023-01-15 12:34:56.789");
        match &v {
            Value::Text(s) => {
                DateTime::parse_from_rfc3339(s)
                    .unwrap_or_else(|e| panic!("RFC3339 parse failed: {e} — got: {s}"));
            }
            other => panic!("expected Value::Text, got {other:?}"),
        }

        // Without milliseconds must also parse as RFC3339.
        let v = cwli_field_value("@timestamp", "2023-01-15 12:34:56");
        match &v {
            Value::Text(s) => {
                DateTime::parse_from_rfc3339(s)
                    .unwrap_or_else(|e| panic!("RFC3339 parse failed: {e} — got: {s}"));
            }
            other => panic!("expected Value::Text, got {other:?}"),
        }

        // Unparseable raw value falls back to unchanged raw text.
        let raw = "not-a-timestamp";
        assert_eq!(
            cwli_field_value("@timestamp", raw),
            Value::Text(raw.to_string())
        );
    }

    #[test]
    fn cwli_column_kind_name_authoritative() {
        let empty: Vec<HashMap<String, Value>> = vec![];
        assert_eq!(
            cwli_column_kind("@timestamp", &empty),
            ColumnKind::Timestamp
        );
        assert_eq!(cwli_column_kind("@message", &empty), ColumnKind::Text);
    }

    #[test]
    fn cwli_column_kind_value_based_numeric() {
        let int_rows = typed_row_maps_for("count", &[Value::Int(123), Value::Int(456)]);
        assert_eq!(cwli_column_kind("count", &int_rows), ColumnKind::Integer);

        let float_rows = typed_row_maps_for("latency", &[Value::Float(1.5), Value::Float(2.3)]);
        assert_eq!(cwli_column_kind("latency", &float_rows), ColumnKind::Float);

        let string_rows = typed_row_maps_for(
            "errorCode",
            &[
                Value::Text("500".to_string()),
                Value::Text("BadRequest".to_string()),
            ],
        );
        assert_eq!(
            cwli_column_kind("errorCode", &string_rows),
            ColumnKind::Unknown
        );
    }

    #[test]
    fn cwli_column_kind_empty_or_missing_values() {
        let empty: Vec<HashMap<String, Value>> = vec![];
        assert_eq!(cwli_column_kind("count", &empty), ColumnKind::Unknown);

        let null_rows: Vec<HashMap<String, Value>> = vec![{
            let mut m = HashMap::new();
            m.insert("count".to_string(), Value::Null);
            m
        }];
        assert_eq!(cwli_column_kind("count", &null_rows), ColumnKind::Unknown);
    }

    #[test]
    fn cwli_column_kind_skips_text_to_find_numeric() {
        // Text samples are skipped; a later Int sample resolves Integer.
        let rows = typed_row_maps_for("field", &[Value::Text("x".to_string()), Value::Int(42)]);
        assert_eq!(cwli_column_kind("field", &rows), ColumnKind::Integer);

        // Null is skipped; a later Float sample resolves Float.
        let rows = typed_row_maps_for("field", &[Value::Null, Value::Float(1.5)]);
        assert_eq!(cwli_column_kind("field", &rows), ColumnKind::Float);

        // All Text samples → Unknown (no numeric found).
        let rows = typed_row_maps_for(
            "field",
            &[Value::Text("a".to_string()), Value::Text("b".to_string())],
        );
        assert_eq!(cwli_column_kind("field", &rows), ColumnKind::Unknown);
    }

    fn series(namespace: &str, metric_name: &str) -> MetricQuerySeries {
        MetricQuerySeries {
            namespace: namespace.to_string(),
            metric_name: metric_name.to_string(),
            dimensions: vec![],
            period_s: 60,
            statistic: "Average".to_string(),
            label: None,
        }
    }

    #[test]
    fn cloudwatch_form_exposes_aws_region_profile_and_endpoint_fields() {
        let main_tab = CLOUDWATCH_FORM.main_tab().expect("main tab");

        assert!(
            main_tab
                .sections
                .iter()
                .flat_map(|section| section.fields.iter())
                .any(|field| field.id == "region" && field.required)
        );
        assert!(CLOUDWATCH_FORM.field("profile").is_some());
        assert!(CLOUDWATCH_FORM.field("endpoint").is_some());
    }

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

    // T-6: single-series GetMetricData → two-column ascending QueryResult.
    // Verifies: column name (metric_name), ColumnKind assignments,
    // ascending row ordering, and second→ms conversion (×1000).
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

        let qr = metric_data_output_to_multi_series_result(
            &output,
            &[series("AWS/EC2", "CPUUtilization")],
        );

        assert_eq!(qr.columns.len(), 2);
        assert_eq!(qr.columns[0].name, "timestamp");
        assert_eq!(qr.columns[0].kind, ColumnKind::Timestamp);
        assert_eq!(qr.columns[1].name, "CPUUtilization");
        assert_eq!(qr.columns[1].kind, ColumnKind::Float);

        assert_eq!(qr.rows.len(), 3);

        // Row 0 corresponds to t1 (ascending order after sort).
        assert_eq!(qr.rows[0][0], Value::Int(1000 * 1000));
        assert_eq!(qr.rows[0][1], Value::Float(1.0));

        assert_eq!(qr.rows[1][0], Value::Int(2000 * 1000));
        assert_eq!(qr.rows[1][1], Value::Float(2.0));

        assert_eq!(qr.rows[2][0], Value::Int(3000 * 1000));
        assert_eq!(qr.rows[2][1], Value::Float(3.0));
    }

    // T-7: empty GetMetricData → one-row-per-series zero columns, no panic.
    #[test]
    fn get_metric_data_output_empty() {
        let output = GetMetricDataOutput::builder().build();
        let qr = metric_data_output_to_multi_series_result(
            &output,
            &[series("AWS/EC2", "CPUUtilization")],
        );

        assert_eq!(qr.columns.len(), 2);
        assert_eq!(qr.rows.len(), 0, "expected zero rows for empty output");
    }

    /// Multi-series GetMetricData → one column per series in request order,
    /// values aligned by timestamp, missing samples become Null.
    #[test]
    fn get_metric_data_output_multi_series() {
        let t1 = DateTime::from_secs(1000);
        let t2 = DateTime::from_secs(2000);

        // m0 returns (t1, 1.0) and (t2, 2.0); m1 returns only (t1, 10.0).
        let r0 = MetricDataResult::builder()
            .id("m0")
            .set_timestamps(Some(vec![t1, t2]))
            .set_values(Some(vec![1.0_f64, 2.0_f64]))
            .build();
        let r1 = MetricDataResult::builder()
            .id("m1")
            .set_timestamps(Some(vec![t1]))
            .set_values(Some(vec![10.0_f64]))
            .build();

        let output = GetMetricDataOutput::builder()
            .metric_data_results(r0)
            .metric_data_results(r1)
            .build();

        let qr = metric_data_output_to_multi_series_result(
            &output,
            &[
                series("AWS/EC2", "CPUUtilization"),
                series("AWS/EC2", "NetworkIn"),
            ],
        );

        assert_eq!(qr.columns.len(), 3);
        assert_eq!(qr.columns[1].name, "CPUUtilization");
        assert_eq!(qr.columns[2].name, "NetworkIn");
        assert_eq!(qr.rows.len(), 2);
        assert_eq!(qr.rows[0][1], Value::Float(1.0));
        assert_eq!(qr.rows[0][2], Value::Float(10.0));
        assert_eq!(qr.rows[1][1], Value::Float(2.0));
        assert_eq!(qr.rows[1][2], Value::Null);
    }

    /// Two series with the same metric name disambiguate by their first
    /// non-empty dimension value.
    #[test]
    fn multi_series_disambiguates_by_dimension() {
        let mut s_primary = series("AWS/RDS", "CPUUtilization");
        s_primary.dimensions = vec![("DBInstanceIdentifier".to_string(), "primary-db".to_string())];
        let mut s_replica = series("AWS/RDS", "CPUUtilization");
        s_replica.dimensions = vec![("DBInstanceIdentifier".to_string(), "replica-db".to_string())];

        let output = GetMetricDataOutput::builder().build();
        let qr = metric_data_output_to_multi_series_result(&output, &[s_primary, s_replica]);

        assert_eq!(qr.columns[1].name, "CPUUtilization (primary-db)");
        assert_eq!(qr.columns[2].name, "CPUUtilization (replica-db)");
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
                series: vec![MetricQuerySeries {
                    namespace: "AWS/Lambda".to_string(),
                    metric_name: "Invocations".to_string(),
                    dimensions: vec![],
                    period_s: 300,
                    statistic: "Sum".to_string(),
                    label: None,
                }],
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

    #[test]
    fn cloudwatch_metadata_advertises_dashboard_import_and_sync() {
        let caps = CLOUDWATCH_METADATA.capabilities;
        assert!(
            caps.contains(DriverCapabilities::DASHBOARD_IMPORT),
            "DASHBOARD_IMPORT must remain on the CW driver"
        );
        assert!(
            caps.contains(DriverCapabilities::DASHBOARD_SYNC),
            "DASHBOARD_SYNC must be advertised so the UI surfaces sync affordances"
        );
    }

    #[test]
    fn cloudwatch_metadata_advertises_chart_authoring() {
        assert!(
            CLOUDWATCH_METADATA
                .capabilities
                .contains(DriverCapabilities::CHART_AUTHORING),
            "CHART_AUTHORING must be advertised so the sidebar surfaces Dashboards / Saved Charts folders for CW connections"
        );
    }

    #[test]
    fn cloudwatch_profile_field_is_auth_profile_ref_with_none_provider_id() {
        let profile_field = CLOUDWATCH_FORM
            .tabs
            .iter()
            .flat_map(|t| t.sections.iter())
            .flat_map(|s| s.fields.iter())
            .find(|f| f.id == "profile")
            .expect("CloudWatch form must have a 'profile' field");

        assert!(
            matches!(
                &profile_field.kind,
                FormFieldKind::AuthProfileRef { provider_id: None }
            ),
            "CloudWatch 'profile' field must be AuthProfileRef {{ provider_id: None }}, got {:?}",
            profile_field.kind
        );
    }

    #[test]
    fn cloudwatch_export_hint_profile_is_required_on_import() {
        let driver = CloudWatchDriver::new();
        let values = FormValues::default();
        assert_eq!(
            driver.export_field_hint("profile", &values),
            dbflux_core::ExportFieldHint::RequiredOnImport,
            "CloudWatch 'profile' field must be RequiredOnImport on export"
        );
    }

    fn test_config() -> CloudWatchProfileConfig {
        CloudWatchProfileConfig {
            region: "us-east-1".to_string(),
            profile: Some("test-profile".to_string()),
            endpoint: None,
        }
    }

    #[test]
    fn cloudwatch_error_formatter_session_expired() {
        let cfg = test_config();

        let expired_by_code = classify_cw(
            Some("ExpiredTokenException"),
            "The security token included in the request is expired",
            Some(&cfg),
        );
        assert!(
            expired_by_code
                .message
                .to_lowercase()
                .contains("session expired")
                || expired_by_code.message.to_lowercase().contains("re-login"),
            "ExpiredTokenException must yield a session-expired message, got: {}",
            expired_by_code.message
        );

        let expired_by_message = classify_cw(None, "token is expired or invalid", Some(&cfg));
        assert!(
            expired_by_message
                .message
                .to_lowercase()
                .contains("session expired")
                || expired_by_message
                    .message
                    .to_lowercase()
                    .contains("re-login"),
            "Message containing 'expired' must yield a session-expired message, got: {}",
            expired_by_message.message
        );
    }

    #[test]
    fn cloudwatch_error_formatter_throttle_is_retriable() {
        let cfg = test_config();

        let throttled = classify_cw(Some("ThrottlingException"), "Rate exceeded", Some(&cfg));
        assert!(
            throttled.retriable,
            "ThrottlingException must produce retriable=true"
        );

        let unavailable = classify_cw(
            Some("ServiceUnavailableException"),
            "Service unavailable",
            Some(&cfg),
        );
        assert!(
            unavailable.retriable,
            "ServiceUnavailableException must produce retriable=true"
        );

        let access_denied = classify_cw(Some("AccessDeniedException"), "Access denied", Some(&cfg));
        let hint = access_denied.hint.as_deref().unwrap_or("");
        assert!(
            hint.to_lowercase().contains("iam") || hint.to_lowercase().contains("permission"),
            "AccessDeniedException hint must reference IAM/permissions, got: {hint}"
        );
    }

    #[test]
    fn cloudwatch_error_formatter_malformed_query_hint() {
        let cfg = test_config();

        let malformed = classify_cw(
            Some("MalformedQueryException"),
            "Syntax error in query",
            Some(&cfg),
        );
        let hint = malformed.hint.as_deref().unwrap_or("");
        assert!(
            hint.to_lowercase().contains("query") || hint.to_lowercase().contains("syntax"),
            "MalformedQueryException hint must reference query/syntax, got: {hint}"
        );

        let invalid_param = classify_cw(
            Some("InvalidParameterException"),
            "Invalid parameter value",
            Some(&cfg),
        );
        let hint = invalid_param.hint.as_deref().unwrap_or("");
        assert!(
            hint.to_lowercase().contains("query") || hint.to_lowercase().contains("syntax"),
            "InvalidParameterException hint must reference query/syntax, got: {hint}"
        );
    }

    #[test]
    fn cloudwatch_error_formatter_unknown_degrades() {
        let cfg = test_config();

        let unknown = classify_cw(None, "some completely unknown error occurred", Some(&cfg));
        assert!(
            !unknown.message.is_empty(),
            "Unknown error must produce a non-empty message"
        );

        let no_config_case = classify_cw(None, "error with no config context", None);
        assert!(
            !no_config_case.message.is_empty(),
            "Unknown error without config must produce a non-empty message"
        );
    }
}
